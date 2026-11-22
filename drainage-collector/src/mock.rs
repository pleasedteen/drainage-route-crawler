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

/// 请求端点种类：三类请求共用同一套故障注入与时间线记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointKind {
    State,
    Changes,
    Page,
}

impl EndpointKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EndpointKind::State => "state",
            EndpointKind::Changes => "changes",
            EndpointKind::Page => "page",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "state" => Some(EndpointKind::State),
            "changes" => Some(EndpointKind::Changes),
            "page" => Some(EndpointKind::Page),
            _ => None,
        }
    }
}

/// 一次请求的时间线记录（时间为相对世界创建时刻的毫秒偏移，便于脚本核对）。
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct RequestRecord {
    /// 相对 MockWorld 创建时刻的毫秒时间戳。
    pub t_ms: u128,
    pub station_id: String,
    pub endpoint: String,
    pub detail: String,
    pub outcome: String,
}

/// 某类端点上排队的故障注入。
#[derive(Debug, Clone)]
pub struct FaultSpec {
    pub kind: EndpointKind,
    /// 剩余触发次数。
    pub hits: u32,
    /// 以 Retry-After 形式返回（true）还是普通 500 暂时失败（false）。
    pub retry_after: bool,
    pub secs: u64,
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
    /// 剩余临时失败次数（每次页面请求消耗一次）。保留以兼容既有测试。
    pub transient_failures: u32,
    /// 剩余 Retry-After 注入次数（页面请求）。保留以兼容既有测试。
    pub retry_after_hits: u32,
    pub retry_after_secs: u64,
    /// 按端点种类排队的故障注入（state/changes/page 均可注入）。
    pub faults: Vec<FaultSpec>,
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
            faults: Vec::new(),
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

#[derive(Clone)]
pub struct MockWorld {
    inner: Arc<RwLock<HashMap<String, MockStation>>>,
    /// 全局请求时间线（进程内客户端与 HTTP 模拟站共用同一份记录）。
    log: Arc<RwLock<Vec<RequestRecord>>>,
    start: Arc<std::time::Instant>,
}

impl Default for MockWorld {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            log: Arc::new(RwLock::new(Vec::new())),
            start: Arc::new(std::time::Instant::now()),
        }
    }
}

impl std::fmt::Debug for MockWorld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockWorld")
            .field("stations", &self.inner.read().unwrap().keys())
            .finish()
    }
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

    /// 注入故障：对指定站的指定端点（state/changes/page）返回临时失败或 Retry-After。
    pub fn inject_fault(
        &self,
        station_id: &str,
        kind: EndpointKind,
        hits: u32,
        retry_after: bool,
        secs: u64,
    ) {
        self.with_station(station_id, |s| {
            s.faults.push(FaultSpec {
                kind,
                hits,
                retry_after,
                secs,
            });
        });
    }

    /// 清空故障队列与请求时间线（每个验证场景开始时调用，保证可重复）。
    pub fn reset_faults_and_log(&self) {
        {
            let mut g = self.inner.write().unwrap();
            for s in g.values_mut() {
                s.faults.clear();
                s.transient_failures = 0;
                s.retry_after_hits = 0;
            }
        }
        self.log.write().unwrap().clear();
    }

    /// 记录一次请求。
    pub fn record(&self, station_id: &str, kind: EndpointKind, detail: &str, outcome: &str) {
        let rec = RequestRecord {
            t_ms: self.start.elapsed().as_millis(),
            station_id: station_id.to_string(),
            endpoint: kind.as_str().to_string(),
            detail: detail.to_string(),
            outcome: outcome.to_string(),
        };
        self.log.write().unwrap().push(rec);
    }

    /// 读取请求时间线快照。
    pub fn request_log(&self) -> Vec<RequestRecord> {
        self.log.read().unwrap().clone()
    }

    /// 取下一条命中指定端点的故障（命中一次消耗一次）。
    fn take_fault(&self, station_id: &str, kind: EndpointKind) -> Option<FaultSpec> {
        let mut g = self.inner.write().unwrap();
        let s = g.get_mut(station_id)?;
        let pos = s.faults.iter().position(|f| f.kind == kind && f.hits > 0)?;
        s.faults[pos].hits -= 1;
        let spec = s.faults[pos].clone();
        if s.faults[pos].hits == 0 {
            s.faults.remove(pos);
        }
        Some(spec)
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
        // 新版统一故障注入（page 端点）。
        if let Some(fault) = self.world.take_fault(&station.id, EndpointKind::Page) {
            self.world.record(
                &station.id,
                EndpointKind::Page,
                page_key,
                if fault.retry_after { "retry-after" } else { "fail" },
            );
            if fault.retry_after {
                return Err(PageFetchError::RetryAfter(std::time::Duration::from_secs(
                    fault.secs,
                )));
            }
            return Err(PageFetchError::Transient("模拟临时故障 500".into()));
        }
        // 兼容旧版字段的临时失败注入。
        if st.transient_failures > 0 {
            st.transient_failures -= 1;
            self.world.with_station(&station.id, |s| {
                s.transient_failures = st.transient_failures
            });
            self.world
                .record(&station.id, EndpointKind::Page, page_key, "fail");
            return Err(PageFetchError::Transient("模拟临时故障 500".into()));
        }
        if st.retry_after_hits > 0 {
            st.retry_after_hits -= 1;
            let secs = st.retry_after_secs;
            self.world
                .with_station(&station.id, |s| s.retry_after_hits = st.retry_after_hits);
            self.world
                .record(&station.id, EndpointKind::Page, page_key, "retry-after");
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
            self.world
                .record(&station.id, EndpointKind::Page, page_key, "gone");
            return Err(PageFetchError::Gone);
        }
        let (content_type, body) = render(station, page_key, version);
        self.world
            .record(&station.id, EndpointKind::Page, page_key, "ok");
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
        // 状态端点同样支持 Retry-After / 临时失败注入，并写入统一时间线。
        if let Some(fault) = self.world.take_fault(&station.id, EndpointKind::State) {
            self.world.record(
                &station.id,
                EndpointKind::State,
                "latest_seq",
                if fault.retry_after { "retry-after" } else { "fail" },
            );
            if fault.retry_after {
                return Err(PageFetchError::RetryAfter(std::time::Duration::from_secs(
                    fault.secs,
                ))
                .into());
            }
            return Err(anyhow::anyhow!("模拟状态端点临时故障 500"));
        }
        let seq = self
            .world
            .get_station(&station.id)
            .map(|s| s.latest_seq)
            .ok_or_else(|| anyhow::anyhow!("未知站点 {}", station.id))?;
        self.world
            .record(&station.id, EndpointKind::State, "latest_seq", "ok");
        Ok(seq)
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        // 清单端点同样支持 Retry-After / 临时失败注入，并写入统一时间线。
        if let Some(fault) = self.world.take_fault(&station.id, EndpointKind::Changes) {
            let detail = format!("after={after_seq}");
            self.world.record(
                &station.id,
                EndpointKind::Changes,
                &detail,
                if fault.retry_after { "retry-after" } else { "fail" },
            );
            if fault.retry_after {
                return Err(PageFetchError::RetryAfter(std::time::Duration::from_secs(
                    fault.secs,
                ))
                .into());
            }
            return Err(anyhow::anyhow!("模拟清单端点临时故障 500"));
        }
        let st = match self.world.get_station(&station.id) {
            Some(s) => s,
            None => return Ok((vec![], None)),
        };
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
        self.world.record(
            &station.id,
            EndpointKind::Changes,
            &format!("after={after_seq}"),
            "ok",
        );
        Ok((entries, next))
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
    /// 注入到哪个端点：state / changes / page（缺省 page，兼容旧接口）。
    endpoint: Option<String>,
}

pub fn router(world: MockWorld, station_id: &'static str) -> Router {
    Router::new()
        .route("/state", get(state_handler))
        .route("/changes", get(changes_handler))
        .route("/pages/:key", get(page_handler))
        .route("/inject/fail", post(inject_fail_handler))
        .route("/inject/retry-after", post(inject_retry_handler))
        .route("/inject/fault", post(inject_fault_handler))
        .route("/requests", get(requests_handler))
        .route("/reset", post(reset_handler))
        .with_state((world, station_id))
}

type HttpState = (MockWorld, &'static str);

/// 把故障规格翻译成 HTTP 响应；无故障返回 None。
fn fault_response(
    world: &MockWorld,
    sid: &str,
    kind: EndpointKind,
) -> Option<axum::response::Response> {
    let fault = world.take_fault(sid, kind)?;
    if fault.retry_after {
        Some((
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", fault.secs.to_string())],
            "rate limited",
        )
            .into_response())
    } else {
        Some(StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }
}

async fn state_handler(
    State((world, sid)): State<HttpState>,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(resp) = fault_response(&world, sid, EndpointKind::State) {
        world.record(sid, EndpointKind::State, "latest_seq", fault_label(&resp));
        return Ok(resp);
    }
    let latest = world.get_station(sid).map(|s| s.latest_seq).unwrap_or(0);
    world.record(sid, EndpointKind::State, "latest_seq", "ok");
    Ok(AxumJson(json!({ "station_id": sid, "latest_seq": latest })).into_response())
}

async fn changes_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<ChangesQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    if let Some(resp) = fault_response(&world, sid, EndpointKind::Changes) {
        world.record(
            sid,
            EndpointKind::Changes,
            &format!("after={}", q.after.unwrap_or(0)),
            fault_label(&resp),
        );
        return Ok(resp);
    }
    let st = world.get_station(sid);
    let after = q.after.unwrap_or(0);
    let page_size = 3;
    let entries: Vec<ChangeLogEntry> = st
        .iter()
        .flat_map(|s| s.change_log.iter())
        .filter(|e| e.seq > after)
        .take(page_size)
        .cloned()
        .collect();
    let next_after = if entries.len() == page_size {
        entries.last().map(|e| e.seq)
    } else {
        None
    };
    world.record(
        sid,
        EndpointKind::Changes,
        &format!("after={after}"),
        "ok",
    );
    Ok(AxumJson(ChangesResp {
        changes: entries,
        next_after,
    })
    .into_response())
}

fn fault_label(resp: &axum::response::Response) -> &'static str {
    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
        "retry-after"
    } else {
        "fail"
    }
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
    // 新版统一故障注入优先。
    if let Some(resp) = fault_response(&world, sid, EndpointKind::Page) {
        world.record(sid, EndpointKind::Page, &key, fault_label(&resp));
        return Ok(resp);
    }
    // 兼容旧版字段。
    if st.transient_failures > 0 {
        world.with_station(sid, |s| {
            s.transient_failures = s.transient_failures.saturating_sub(1)
        });
        world.record(sid, EndpointKind::Page, &key, "fail");
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    if st.retry_after_hits > 0 {
        world.with_station(sid, |s| {
            s.retry_after_hits = s.retry_after_hits.saturating_sub(1)
        });
        world.record(sid, EndpointKind::Page, &key, "retry-after");
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
        world.record(sid, EndpointKind::Page, &key, "gone");
        return Err(StatusCode::GONE);
    }
    let (ct, body) = render(&cfg, &key, v);
    world.record(sid, EndpointKind::Page, &key, "ok");
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
    // 旧接口默认注入 page 端点；可通过 endpoint 参数指向 state/changes。
    let kind = q
        .endpoint
        .as_deref()
        .and_then(EndpointKind::parse)
        .unwrap_or(EndpointKind::Page);
    world.inject_fault(sid, kind, times, false, 0);
    AxumJson(json!({"ok": true, "endpoint": kind.as_str(), "failures_queued": times}))
}

async fn inject_retry_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<InjectQuery>,
) -> AxumJson<serde_json::Value> {
    let secs = q.secs.unwrap_or(1);
    let times = q.times.unwrap_or(1);
    let kind = q
        .endpoint
        .as_deref()
        .and_then(EndpointKind::parse)
        .unwrap_or(EndpointKind::Page);
    world.inject_fault(sid, kind, times, true, secs);
    AxumJson(json!({"ok": true, "endpoint": kind.as_str(), "retry_after_next_request_secs": secs, "times": times}))
}

/// 统一故障注入：POST /inject/fault?endpoint=state|changes|page&kind=retry-after|fail&secs=N&times=N
async fn inject_fault_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<FaultInjectQuery>,
) -> Result<AxumJson<serde_json::Value>, (StatusCode, String)> {
    let kind = EndpointKind::parse(&q.endpoint)
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "endpoint 必须是 state/changes/page".into()))?;
    let retry_after = q.kind.as_deref().unwrap_or("retry-after") == "retry-after";
    let times = q.times.unwrap_or(1);
    let secs = q.secs.unwrap_or(1);
    world.inject_fault(sid, kind, times, retry_after, secs);
    Ok(AxumJson(json!({
        "ok": true,
        "station": sid,
        "endpoint": kind.as_str(),
        "mode": if retry_after { "retry-after" } else { "fail" },
        "times": times,
        "secs": secs,
    })))
}

#[derive(serde::Deserialize)]
struct FaultInjectQuery {
    endpoint: String,
    kind: Option<String>,
    times: Option<u32>,
    secs: Option<u64>,
}

/// 读取请求时间线：GET /requests
async fn requests_handler(State((world, _sid)): State<HttpState>) -> AxumJson<serde_json::Value> {
    AxumJson(json!({ "requests": world.request_log() }))
}

/// 清空故障与时间线：POST /reset
async fn reset_handler(State((world, _sid)): State<HttpState>) -> AxumJson<serde_json::Value> {
    world.reset_faults_and_log();
    AxumJson(json!({"ok": true}))
}
