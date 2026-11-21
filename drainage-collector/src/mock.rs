//! 本地模拟站点：分页 HTML 站与 JSON 站各一个。
//!
//! 同一个 [`MockWorld`] 有两种用法：
//! - 测试中用 [`MockPageClient`] 进程内直连，不起网络；
//! - 演示时用 [`router`] 起 axum HTTP 服务，行为与生产站点一致
//!   （分页变更清单、按修订取页面、临时失败与 Retry-After 注入）。
//!
//! 所有请求（状态 / 变更清单 / 页面）都会写入 [`MockWorld`] 的请求日志，
//! 便于用真实 HTTP 链路检查发起时间线与落库结果。
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json as AxumJson, Response},
    routing::{get, post},
    Router,
};
use serde::Serialize;
use serde_json::json;

use crate::fetch::{FetchedPage, PageClient, PageFetchError};
use crate::model::{ChangeLogEntry, FacilityInfo, LinkRef, StationConfig};

/// 站点被请求的端点类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RequestKind {
    State,
    Changes,
    Page,
}

impl RequestKind {
    pub fn as_str(self) -> &'static str {
        match self {
            RequestKind::State => "state",
            RequestKind::Changes => "changes",
            RequestKind::Page => "page",
        }
    }
}

/// 故障注入行为。
#[derive(Debug, Clone)]
pub enum InjectionBehavior {
    /// 临时失败（HTTP 500 / 可重试错误）。
    Transient,
    /// 要求等待指定秒数（HTTP 429 + Retry-After）。
    RetryAfter(u64),
}

/// 一条故障注入：`kind` 为 `None` 表示作用于该站任意端点。
#[derive(Debug, Clone)]
struct Injection {
    kind: Option<RequestKind>,
    behavior: InjectionBehavior,
    remaining: u32,
}

/// 一次请求的观测记录（时间线用）。
#[derive(Debug, Clone, Serialize)]
pub struct RequestRecord {
    pub station_id: String,
    pub kind: String,
    pub page_key: Option<String>,
    /// Unix 毫秒时间戳（请求开始时刻）。
    pub at_ms: i64,
    /// Unix 毫秒时间戳（响应结束时刻）。
    pub finished_ms: i64,
    /// 响应状态：200 / 404 / 429 / 500。
    pub status: u16,
    /// 本次响应携带的 Retry-After 秒数。
    pub retry_after_secs: Option<u64>,
}

/// 页面的一个历史修订。
#[derive(Debug, Clone)]
pub struct PageVersion {
    pub revision: i64,
    /// 该修订自哪个登记序号起生效。
    pub effective_seq: i64,
    pub facility: FacilityInfo,
    pub links: Vec<LinkRef>,
    pub removed: bool,
}

#[derive(Debug, Clone)]
pub struct MockStation {
    pub id: String,
    pub pages: HashMap<String, Vec<PageVersion>>,
    pub change_log: Vec<ChangeLogEntry>,
    pub latest_seq: i64,
    /// 剩余临时失败次数（每次页面请求消耗一次）。
    pub transient_failures: u32,
    /// 剩余 Retry-After 注入次数（每次页面请求消耗一次）。
    pub retry_after_hits: u32,
    pub retry_after_secs: u64,
    /// 可针对状态 / 变更清单 / 任意端点的故障注入。
    injections: Vec<Injection>,
}

impl MockStation {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            pages: HashMap::new(),
            change_log: Vec::new(),
            latest_seq: 0,
            transient_failures: 0,
            retry_after_hits: 0,
            retry_after_secs: 1,
            injections: Vec::new(),
        }
    }

    /// 为指定端点排入一次故障注入（`kind=None` 表示任意端点）。
    pub fn inject(&mut self, kind: Option<RequestKind>, behavior: InjectionBehavior) {
        self.injections.push(Injection {
            kind,
            behavior,
            remaining: 1,
        });
    }

    /// 排入连续 N 次故障注入。
    pub fn inject_repeated(
        &mut self,
        kind: Option<RequestKind>,
        behavior: InjectionBehavior,
        times: u32,
    ) {
        if times > 0 {
            self.injections.push(Injection {
                kind,
                behavior,
                remaining: times,
            });
        }
    }

    /// 消耗一次匹配的注入。
    fn take_injection(&mut self, kind: RequestKind) -> Option<InjectionBehavior> {
        let idx = self
            .injections
            .iter()
            .position(|i| i.remaining > 0 && i.kind.map_or(true, |k| k == kind))?;
        let behavior = self.injections[idx].behavior.clone();
        self.injections[idx].remaining -= 1;
        if self.injections[idx].remaining == 0 {
            self.injections.remove(idx);
        }
        Some(behavior)
    }

    fn register(&mut self, seq: i64, page_key: &str, revision: i64, kind: &str) {
        self.change_log.push(ChangeLogEntry {
            seq,
            station_id: self.id.clone(),
            page_key: page_key.to_string(),
            revision,
            kind: kind.to_string(),
        });
        self.latest_seq = self.latest_seq.max(seq);
    }

    /// 登记/修订一个页面版本，并写入变更清单。
    pub fn put_version(
        &mut self,
        seq: i64,
        page_key: &str,
        revision: i64,
        facility: FacilityInfo,
        links: Vec<LinkRef>,
        kind: &str,
    ) {
        self.register(seq, page_key, revision, kind);
        self.pages
            .entry(page_key.to_string())
            .or_default()
            .push(PageVersion {
                revision,
                effective_seq: seq,
                facility,
                links,
                removed: false,
            });
    }

    pub fn remove_version(&mut self, seq: i64, page_key: &str, revision: i64) {
        self.register(seq, page_key, revision, "removed");
        if let Some(vers) = self.pages.get_mut(page_key) {
            if let Some(v) = vers.last_mut() {
                let mut v = v.clone();
                v.revision = revision;
                v.effective_seq = seq;
                v.removed = true;
                vers.push(v);
            }
        }
    }

    /// 按修订取页面；revision 为 None 时取当前最新版本。
    fn version_at(&self, page_key: &str, revision: Option<i64>) -> Option<&PageVersion> {
        let vers = self.pages.get(page_key)?;
        match revision {
            Some(rev) => vers.iter().find(|v| v.revision == rev),
            None => vers.last(),
        }
    }

    /// 按登记截止点取当时最新版本（业务时间可能早于登记时间，故只看 effective_seq）。
    fn version_as_of(&self, page_key: &str, as_of_seq: i64) -> Option<&PageVersion> {
        let vers = self.pages.get(page_key)?;
        vers.iter()
            .filter(|v| v.effective_seq <= as_of_seq)
            .max_by_key(|v| v.effective_seq)
    }
}

#[derive(Clone, Default)]
pub struct MockWorld {
    inner: Arc<RwLock<HashMap<String, MockStation>>>,
    /// 单调请求序号（同时充当请求日志的排序键）。
    request_seq: Arc<AtomicU64>,
    requests: Arc<RwLock<Vec<RequestRecord>>>,
    /// 进程内客户端发出的页面请求数（缓存命中不计数）。
    page_requests: Arc<AtomicU64>,
}

impl MockWorld {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_station(&self, station: MockStation) {
        self.inner
            .write()
            .unwrap()
            .insert(station.id.clone(), station);
    }

    pub fn get_station(&self, id: &str) -> Option<MockStation> {
        self.inner.read().unwrap().get(id).cloned()
    }

    pub fn with_station(&self, id: &str, f: impl FnOnce(&mut MockStation)) {
        let mut g = self.inner.write().unwrap();
        if let Some(s) = g.get_mut(id) {
            f(s);
        }
    }

    /// 全部请求日志（按发起顺序）。
    pub fn request_log(&self) -> Vec<RequestRecord> {
        self.requests.read().unwrap().clone()
    }

    /// 仅某站的请求日志。
    pub fn request_log_for(&self, station_id: &str) -> Vec<RequestRecord> {
        self.request_log()
            .into_iter()
            .filter(|r| r.station_id == station_id)
            .collect()
    }

    /// 清空请求日志（不影响故障注入计数）。
    pub fn clear_request_log(&self) {
        self.requests.write().unwrap().clear();
    }

    /// 进程内客户端累计页面请求数。
    pub fn page_request_count(&self) -> u64 {
        self.page_requests.load(Ordering::SeqCst)
    }

    /// 记录一次请求并分配序号。
    fn record_request(&self, rec: RequestRecord) -> u64 {
        let seq = self.request_seq.fetch_add(1, Ordering::SeqCst);
        self.requests.write().unwrap().push(rec);
        seq
    }

    /// 处理一次端点请求：按注入规则返回成功 / 临时失败 / 等待要求。
    ///
    /// 返回 `Ok(())` 表示成功；否则返回注入的故障。无论结果如何都会记入请求日志。
    fn dispatch<T>(
        &self,
        station_id: &str,
        kind: RequestKind,
        page_key: Option<&str>,
        on_success: impl FnOnce(&MockStation) -> std::result::Result<T, PageFetchError>,
    ) -> std::result::Result<T, PageFetchError> {
        let at_ms = crate::model::now_millis();
        let mut g = self.inner.write().unwrap();
        let st = match g.get_mut(station_id) {
            Some(s) => s,
            None => {
                drop(g);
                self.record_request(RequestRecord {
                    station_id: station_id.to_string(),
                    kind: kind.as_str().to_string(),
                    page_key: page_key.map(str::to_string),
                    at_ms,
                    finished_ms: crate::model::now_millis(),
                    status: 404,
                    retry_after_secs: None,
                });
                return Err(PageFetchError::Fatal(format!("未知站点 {station_id}")));
            }
        };
        // 通用注入优先（可按端点限定）。
        if let Some(behavior) = st.take_injection(kind) {
            let (err, status, retry) = match behavior {
                InjectionBehavior::Transient => (
                    PageFetchError::Transient("模拟临时故障 500".into()),
                    500,
                    None,
                ),
                InjectionBehavior::RetryAfter(secs) => (
                    PageFetchError::RetryAfter(Duration::from_secs(secs.max(1))),
                    429,
                    Some(secs.max(1)),
                ),
            };
            drop(g);
            self.record_request(RequestRecord {
                station_id: station_id.to_string(),
                kind: kind.as_str().to_string(),
                page_key: page_key.map(str::to_string),
                at_ms,
                finished_ms: crate::model::now_millis(),
                status,
                retry_after_secs: retry,
            });
            return Err(err);
        }
        // 历史演示注入：页面端点上的瞬态失败 / 等待。
        if kind == RequestKind::Page {
            if st.transient_failures > 0 {
                st.transient_failures -= 1;
                drop(g);
                self.record_request(RequestRecord {
                    station_id: station_id.to_string(),
                    kind: kind.as_str().to_string(),
                    page_key: page_key.map(str::to_string),
                    at_ms,
                    finished_ms: crate::model::now_millis(),
                    status: 500,
                    retry_after_secs: None,
                });
                return Err(PageFetchError::Transient("模拟临时故障 500".into()));
            }
            if st.retry_after_hits > 0 {
                st.retry_after_hits -= 1;
                let secs = st.retry_after_secs.max(1);
                drop(g);
                self.record_request(RequestRecord {
                    station_id: station_id.to_string(),
                    kind: kind.as_str().to_string(),
                    page_key: page_key.map(str::to_string),
                    at_ms,
                    finished_ms: crate::model::now_millis(),
                    status: 429,
                    retry_after_secs: Some(secs),
                });
                return Err(PageFetchError::RetryAfter(Duration::from_secs(secs)));
            }
        }
        let result = on_success(st);
        drop(g);
        let (status, retry) = match &result {
            Ok(_) => (200, None),
            Err(PageFetchError::Gone) => (404, None),
            Err(PageFetchError::RetryAfter(d)) => (429, Some(d.as_secs().max(1))),
            Err(_) => (500, None),
        };
        self.record_request(RequestRecord {
            station_id: station_id.to_string(),
            kind: kind.as_str().to_string(),
            page_key: page_key.map(str::to_string),
            at_ms,
            finished_ms: crate::model::now_millis(),
            status,
            retry_after_secs: retry,
        });
        result
    }

    /// 构造标准演示图：跨站路线、汇合与回流成环，外加一轮后续修订。
    pub fn demo() -> Self {
        let world = MockWorld::new();

        // ---- 站 A（HTML） ----
        let mut a = MockStation::new("station-a");
        let l = |page: &str| LinkRef {
            station_id: "station-a".into(),
            page_key: page.into(),
        };
        let lb = |page: &str| LinkRef {
            station_id: "station-b".into(),
            page_key: page.into(),
        };
        let fac = |name: &str, kind: &str| FacilityInfo {
            name: Some(name.into()),
            kind: Some(kind.into()),
        };
        let mut seq = 0;
        let mut next = || {
            seq += 1;
            seq
        };
        a.put_version(
            next(),
            "a0",
            1,
            fac("A站入流口", "inlet"),
            vec![l("a1")],
            "registered",
        );
        a.put_version(
            next(),
            "a1",
            1,
            fac("A1号检查井", "manhole"),
            vec![l("a2")],
            "registered",
        );
        a.put_version(
            next(),
            "a2",
            1,
            fac("A2号提升泵站", "pump"),
            vec![l("a3"), lb("b1")],
            "registered",
        );
        a.put_version(
            next(),
            "a3",
            1,
            fac("A3号截流井", "interceptor"),
            vec![lb("b3")],
            "registered",
        );

        // ---- 站 B（JSON） ----
        let mut b = MockStation::new("station-b");
        b.put_version(
            next(),
            "b1",
            1,
            fac("B1号调蓄池", "tank"),
            vec![lb_self("b2"), lb_self("b3")],
            "registered",
        );
        b.put_version(
            next(),
            "b2",
            1,
            fac("B2号检查井", "manhole"),
            vec![LinkRef {
                station_id: "station-a".into(),
                page_key: "a1".into(),
            }],
            "registered",
        ); // 回流指向 A 站，形成跨站环
        b.put_version(
            next(),
            "b3",
            1,
            fac("B3号出水口", "outfall"),
            vec![],
            "registered",
        );
        world.add_station(a);
        world.add_station(b);

        // ---- 后续登记（增量轮次要追的变化） ----
        let world2 = world.clone();
        let mut a = world2.get_station("station-a").unwrap();
        let mut b = world2.get_station("station-b").unwrap();
        // a1 修订：新增通往下游新设施 a4 的路线
        a.put_version(
            next(),
            "a1",
            2,
            fac("A1号检查井(扩建)", "manhole"),
            vec![l("a2"), l("a4")],
            "revised",
        );
        a.put_version(
            next(),
            "a4",
            1,
            fac("A4号应急闸门", "gate"),
            vec![lb("b3")],
            "registered",
        );
        // b2 修订：不再回流 a1，改走 b3（环被拆除）
        b.put_version(
            next(),
            "b2",
            2,
            fac("B2号检查井(改造)", "manhole"),
            vec![lb_self("b3")],
            "revised",
        );
        world.add_station(a);
        world.add_station(b);
        world
    }
}

fn lb_self(page: &str) -> LinkRef {
    LinkRef {
        station_id: "station-b".into(),
        page_key: page.into(),
    }
}

// ---------- 进程内客户端（测试用） ----------

#[derive(Clone)]
pub struct MockPageClient {
    world: MockWorld,
}

impl MockPageClient {
    pub fn new(world: MockWorld) -> Self {
        Self { world }
    }

    /// 累计页面请求次数（缓存命中不计数）。
    pub fn request_count(&self) -> u64 {
        self.world.page_request_count()
    }

    fn serve_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: crate::fetch::FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        self.world.page_requests.fetch_add(1, Ordering::SeqCst);
        self.world.dispatch(
            &station.id,
            RequestKind::Page,
            Some(page_key),
            |st| {
                use crate::fetch::FetchTarget;
                let version = match target {
                    FetchTarget::AtRevision(rev) => st.version_at(page_key, Some(rev)),
                    FetchTarget::AtSeq(seq) => st.version_as_of(page_key, seq),
                }
                .ok_or(PageFetchError::Gone)?;
                if version.removed {
                    return Err(PageFetchError::Gone);
                }
                let (content_type, body) = render(station, page_key, version);
                Ok(FetchedPage { body, content_type })
            },
        )
    }
}

#[async_trait]
impl PageClient for MockPageClient {
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: crate::fetch::FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        self.serve_page(station, page_key, target)
    }

    async fn fetch_state(
        &self,
        station: &StationConfig,
    ) -> std::result::Result<i64, PageFetchError> {
        self.world
            .dispatch(&station.id, RequestKind::State, None, |st| Ok(st.latest_seq))
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> std::result::Result<(Vec<ChangeLogEntry>, Option<i64>), PageFetchError> {
        self.world.dispatch(&station.id, RequestKind::Changes, None, |st| {
            let page_size = 3;
            let entries: Vec<_> = st
                .change_log
                .iter()
                .filter(|e| e.seq > after_seq)
                .take(page_size)
                .cloned()
                .collect();
            let next = if entries.len() == page_size {
                entries.last().map(|e| e.seq)
            } else {
                None
            };
            Ok((entries, next))
        })
    }
}

/// 按站点格式渲染页面正文。
pub fn render(station: &StationConfig, page_key: &str, v: &PageVersion) -> (String, String) {
    match station.format {
        crate::model::SiteFormat::Json => {
            let body = json!({
                "page_key": page_key,
                "revision": v.revision,
                "facility": v.facility,
                "links": v.links.iter().map(|l| json!({"station_id": l.station_id, "page_key": l.page_key})).collect::<Vec<_>>(),
            })
            .to_string();
            ("application/json".into(), body)
        }
        crate::model::SiteFormat::Html => {
            let links = v
                .links
                .iter()
                .map(|l| {
                    format!(
                        r#"<a class="downstream" data-station="{}" data-page="{}" href="/pages/{}">下游 {}</a>"#,
                        l.station_id, l.page_key, l.page_key, l.page_key
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            let body = format!(
                r#"<!doctype html><html><body>
<article class="facility-page" data-revision="{rev}">
  <h1 class="facility-name">{name}</h1>
  <span class="facility-kind">{kind}</span>
  <nav>{links}</nav>
</article></body></html>"#,
                rev = v.revision,
                name = v.facility.name.clone().unwrap_or_default(),
                kind = v.facility.kind.clone().unwrap_or_default(),
                links = links
            );
            ("text/html".into(), body)
        }
    }
}

// ---------- HTTP 形态（演示用） ----------

#[derive(Serialize)]
struct ChangesResp {
    changes: Vec<ChangeLogEntry>,
    next_after: Option<i64>,
}

#[derive(serde::Deserialize)]
struct ChangesQuery {
    after: Option<i64>,
}

#[derive(serde::Deserialize)]
struct PageQuery {
    revision: Option<i64>,
    as_of: Option<i64>,
}

#[derive(serde::Deserialize)]
struct InjectQuery {
    times: Option<u32>,
    secs: Option<u64>,
    /// 限定端点：state / changes / page；缺省为任意端点。
    kind: Option<String>,
}

fn parse_inject_kind(s: Option<&str>) -> Option<RequestKind> {
    match s {
        Some("state") => Some(RequestKind::State),
        Some("changes") => Some(RequestKind::Changes),
        Some("page") => Some(RequestKind::Page),
        _ => None,
    }
}

pub fn router(world: MockWorld, station_id: &'static str) -> Router {
    Router::new()
        .route("/state", get(state_handler))
        .route("/changes", get(changes_handler))
        .route("/pages/:key", get(page_handler))
        .route("/inject/fail", post(inject_fail_handler))
        .route("/inject/retry-after", post(inject_retry_handler))
        .route("/requests", get(requests_handler))
        .with_state((world, station_id))
}

type HttpState = (MockWorld, &'static str);

/// 把注入的故障转为 HTTP 响应（保持与生产站点一致的 429 + Retry-After / 5xx）。
fn failure_response(err: PageFetchError) -> Response {
    match err {
        PageFetchError::RetryAfter(d) => (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", d.as_secs().max(1).to_string())],
            "rate limited",
        )
            .into_response(),
        PageFetchError::Gone => StatusCode::NOT_FOUND.into_response(),
        PageFetchError::Transient(m) => (StatusCode::INTERNAL_SERVER_ERROR, m).into_response(),
        PageFetchError::Fatal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m).into_response(),
    }
}

async fn state_handler(State((world, sid)): State<HttpState>) -> Response {
    match world.dispatch(sid, RequestKind::State, None, |st| Ok(st.latest_seq)) {
        Ok(latest) => AxumJson(json!({ "station_id": sid, "latest_seq": latest })).into_response(),
        Err(e) => failure_response(e),
    }
}

async fn changes_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<ChangesQuery>,
) -> Response {
    let after = q.after.unwrap_or(0);
    let result = world.dispatch(sid, RequestKind::Changes, None, |st| {
        let page_size = 3;
        let entries: Vec<ChangeLogEntry> = st
            .change_log
            .iter()
            .filter(|e| e.seq > after)
            .take(page_size)
            .cloned()
            .collect();
        let next_after = if entries.len() == page_size {
            entries.last().map(|e| e.seq)
        } else {
            None
        };
        Ok(ChangesResp {
            changes: entries,
            next_after,
        })
    });
    match result {
        Ok(resp) => AxumJson(resp).into_response(),
        Err(e) => failure_response(e),
    }
}

async fn page_handler(
    State((world, sid)): State<HttpState>,
    Path(key): Path<String>,
    Query(q): Query<PageQuery>,
) -> Response {
    let cfg = StationConfig {
        id: sid.to_string(),
        name: sid.to_string(),
        base_url: String::new(),
        format: if sid.ends_with('a') {
            crate::model::SiteFormat::Html
        } else {
            crate::model::SiteFormat::Json
        },
        min_interval_ms: 0,
        max_inflight: 1,
        watermark_seq: 0,
    };
    let result = world.dispatch(sid, RequestKind::Page, Some(&key), |st| {
        let v = match (q.revision, q.as_of) {
            (Some(rev), _) => st.version_at(&key, Some(rev)),
            (None, Some(seq)) => st.version_as_of(&key, seq),
            (None, None) => st.version_at(&key, None),
        }
        .ok_or(PageFetchError::Gone)?;
        if v.removed {
            return Err(PageFetchError::Gone);
        }
        let (ct, body) = render(&cfg, &key, v);
        Ok((ct, body))
    });
    match result {
        Ok((ct, body)) => match ct.as_str() {
            "text/html" => Html(body).into_response(),
            _ => AxumJson(serde_json::from_str::<serde_json::Value>(&body).unwrap())
                .into_response(),
        },
        Err(e) => failure_response(e),
    }
}

/// 请求日志（测试用真实 HTTP 链路检查发起时间线）。
async fn requests_handler(State((world, sid)): State<HttpState>) -> AxumJson<Vec<RequestRecord>> {
    AxumJson(world.request_log_for(sid))
}

async fn inject_fail_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<InjectQuery>,
) -> AxumJson<serde_json::Value> {
    let times = q.times.unwrap_or(1);
    let kind = parse_inject_kind(q.kind.as_deref());
    world.with_station(sid, |s| {
        s.inject_repeated(kind, InjectionBehavior::Transient, times)
    });
    AxumJson(json!({
        "ok": true,
        "transient_failures_queued": times,
        "kind": q.kind.unwrap_or_else(|| "any".into()),
    }))
}

async fn inject_retry_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<InjectQuery>,
) -> AxumJson<serde_json::Value> {
    let secs = q.secs.unwrap_or(1);
    let times = q.times.unwrap_or(1);
    let kind = parse_inject_kind(q.kind.as_deref());
    world.with_station(sid, |s| {
        s.inject_repeated(kind, InjectionBehavior::RetryAfter(secs), times)
    });
    AxumJson(json!({
        "ok": true,
        "retry_after_secs": secs,
        "times": times,
        "kind": q.kind.unwrap_or_else(|| "any".into()),
    }))
}
