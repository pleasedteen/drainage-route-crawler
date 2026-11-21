//! 本地模拟站点：分页 HTML 站与 JSON 站各一个。
//!
//! 同一个 [`MockWorld`] 有两种用法：
//! - 测试中用 [`MockPageClient`] 进程内直连，不起网络；
//! - 演示时用 [`router`] 起 axum HTTP 服务，行为与生产站点一致
//!   （分页变更清单、按修订取页面、临时失败与 Retry-After 注入）。
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use async_trait::async_trait;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Json as AxumJson},
    routing::{get, post},
    Router,
};
use serde::Serialize;
use serde_json::json;

use crate::fetch::{FetchedPage, PageClient, PageFetchError};
use crate::model::{ChangeLogEntry, FacilityInfo, LinkRef, StationConfig};

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
    /// 剩余 Retry-After 注入次数。
    pub retry_after_hits: u32,
    pub retry_after_secs: u64,
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
        }
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
    /// 每站收到的变更清单请求游标 `(after, until)`，按到达顺序记录，供测试核对。
    change_requests: Arc<RwLock<Vec<(String, i64, i64)>>>,
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

    /// 记录一次变更清单请求（站点侧真实收到的 `after` / `until`）。
    fn record_change_request(&self, station_id: &str, after: i64, until: i64) {
        self.change_requests
            .write()
            .unwrap()
            .push((station_id.to_string(), after, until));
    }

    /// 全部变更清单请求游标快照。
    pub fn change_requests(&self) -> Vec<(String, i64, i64)> {
        self.change_requests.read().unwrap().clone()
    }

    /// 清空请求记录（便于分段核对）。
    pub fn clear_change_requests(&self) {
        self.change_requests.write().unwrap().clear();
    }

    /// 按站裁剪后的一页变更清单：`after < seq <= until`，每页至多 `page_size` 条。
    ///
    /// `next_after` 为页内最后一条的 seq（无更多不超过 `until` 的条目时为 None）。
    /// 登记 seq 允许有空洞：游标跳到页内最后一条即可，不必连续。
    fn changes_page(
        &self,
        station_id: &str,
        after: i64,
        until: i64,
        page_size: usize,
    ) -> (Vec<ChangeLogEntry>, Option<i64>) {
        let st = match self.inner.read().unwrap().get(station_id) {
            Some(s) => s.clone(),
            None => return (vec![], None),
        };
        let mut entries: Vec<ChangeLogEntry> = st
            .change_log
            .iter()
            .filter(|e| e.seq > after && e.seq <= until)
            .cloned()
            .collect();
        entries.sort_by_key(|e| e.seq);
        let full = entries.len() == page_size;
        entries.truncate(page_size);
        let next = if full {
            entries.last().map(|e| e.seq)
        } else {
            None
        };
        (entries, next)
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
    /// 实际发出的页面请求数（缓存命中不计数），用于验证增量复用。
    requests: Arc<std::sync::atomic::AtomicU64>,
}

impl MockPageClient {
    pub fn new(world: MockWorld) -> Self {
        Self {
            world,
            requests: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// 累计页面请求次数。
    pub fn request_count(&self) -> u64 {
        self.requests.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn serve_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: crate::fetch::FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        self.requests
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut st = self
            .world
            .get_station(&station.id)
            .ok_or_else(|| PageFetchError::Fatal(format!("未知站点 {}", station.id)))?;
        if st.transient_failures > 0 {
            st.transient_failures -= 1;
            self.world.with_station(&station.id, |s| {
                s.transient_failures = st.transient_failures
            });
            return Err(PageFetchError::Transient("模拟临时故障 500".into()));
        }
        if st.retry_after_hits > 0 {
            st.retry_after_hits -= 1;
            let secs = st.retry_after_secs;
            self.world
                .with_station(&station.id, |s| s.retry_after_hits = st.retry_after_hits);
            return Err(PageFetchError::RetryAfter(std::time::Duration::from_secs(
                secs,
            )));
        }
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

    async fn fetch_state(&self, station: &StationConfig) -> Result<i64> {
        Ok(self
            .world
            .get_station(&station.id)
            .map(|s| s.latest_seq)
            .unwrap_or(0))
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
        until_seq: i64,
    ) -> Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        self.world
            .record_change_request(&station.id, after_seq, until_seq);
        Ok(self
            .world
            .changes_page(&station.id, after_seq, until_seq, 3))
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
    until: Option<i64>,
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
}

pub fn router(world: MockWorld, station_id: &'static str) -> Router {
    Router::new()
        .route("/state", get(state_handler))
        .route("/changes", get(changes_handler))
        .route("/pages/:key", get(page_handler))
        .route("/inject/fail", post(inject_fail_handler))
        .route("/inject/retry-after", post(inject_retry_handler))
        .with_state((world, station_id))
}

type HttpState = (MockWorld, &'static str);

async fn state_handler(State((world, sid)): State<HttpState>) -> AxumJson<serde_json::Value> {
    let latest = world.get_station(sid).map(|s| s.latest_seq).unwrap_or(0);
    AxumJson(json!({ "station_id": sid, "latest_seq": latest }))
}

async fn changes_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<ChangesQuery>,
) -> AxumJson<ChangesResp> {
    let after = q.after.unwrap_or(0);
    let until = q.until.unwrap_or(i64::MAX);
    world.record_change_request(sid, after, until);
    let (entries, next_after) = world.changes_page(sid, after, until, 3);
    AxumJson(ChangesResp {
        changes: entries,
        next_after,
    })
}

async fn page_handler(
    State((world, sid)): State<HttpState>,
    Path(key): Path<String>,
    Query(q): Query<PageQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    let st = world.get_station(sid).ok_or(StatusCode::NOT_FOUND)?;
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
    if st.transient_failures > 0 {
        world.with_station(sid, |s| {
            s.transient_failures = s.transient_failures.saturating_sub(1)
        });
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    if st.retry_after_hits > 0 {
        world.with_station(sid, |s| {
            s.retry_after_hits = s.retry_after_hits.saturating_sub(1)
        });
        return Ok((
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", st.retry_after_secs.to_string())],
            "rate limited",
        )
            .into_response());
    }
    let v = match (q.revision, q.as_of) {
        (Some(rev), _) => st.version_at(&key, Some(rev)),
        (None, Some(seq)) => st.version_as_of(&key, seq),
        (None, None) => st.version_at(&key, None),
    }
    .ok_or(StatusCode::NOT_FOUND)?;
    if v.removed {
        return Err(StatusCode::GONE);
    }
    let (ct, body) = render(&cfg, &key, v);
    Ok(match ct.as_str() {
        "text/html" => Html(body).into_response(),
        _ => AxumJson(serde_json::from_str::<serde_json::Value>(&body).unwrap()).into_response(),
    })
}

async fn inject_fail_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<InjectQuery>,
) -> AxumJson<serde_json::Value> {
    let times = q.times.unwrap_or(1);
    world.with_station(sid, |s| s.transient_failures += times);
    AxumJson(json!({"ok": true, "transient_failures_queued": times}))
}

async fn inject_retry_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<InjectQuery>,
) -> AxumJson<serde_json::Value> {
    let secs = q.secs.unwrap_or(1);
    world.with_station(sid, |s| {
        s.retry_after_hits += 1;
        s.retry_after_secs = secs;
    });
    AxumJson(json!({"ok": true, "retry_after_next_request_secs": secs}))
}
