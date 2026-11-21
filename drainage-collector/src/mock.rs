//! 本地模拟站点：分页 HTML 站与 JSON 站各一个。
//!
//! 同一个 [`MockWorld`] 有两种用法：
//! - 测试中用 [`MockPageClient`] 进程内直连，不起网络；
//! - 演示时用 [`router`] 起 axum HTTP 服务，行为与生产站点一致
//!   （分页变更清单、按修订取页面、临时失败与 Retry-After 注入）。
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

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

/// 页面阻塞闸门：前 `waiters` 个到达该页的请求在此等待，`release` 后全部放行。
/// 用于在真实 HTTP 链路里制造“响应很慢”的在途窗口，验证跨进程单飞。
#[derive(Debug)]
pub struct HoldGate {
    waiters: AtomicU64,
    sem: tokio::sync::Semaphore,
}

impl HoldGate {
    pub fn new(waiters: u64) -> Self {
        Self {
            waiters: AtomicU64::new(waiters),
            sem: tokio::sync::Semaphore::new(0),
        }
    }

    /// 前 waiters 次调用挂起；release 之后（或名额耗尽后）直接通过。
    pub async fn pass(&self) {
        if self.waiters.fetch_saturating_sub() > 0 {
            // 关闭信号量会让所有 acquire 立即返回 Err，达到一次性广播放行。
            let _ = self.sem.acquire().await;
        }
    }

    pub fn release(&self) {
        self.sem.close();
    }
}

trait AtomicSaturating {
    fn fetch_saturating_sub(&self) -> u64;
}

impl AtomicSaturating for AtomicU64 {
    fn fetch_saturating_sub(&self) -> u64 {
        let mut cur = self.load(Ordering::SeqCst);
        loop {
            let next = cur.saturating_sub(1);
            match self.compare_exchange(cur, next, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(v) => return v,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// 单页命中计数 +1。一次性取得写锁后在同一守卫内查改，避免重入死锁。
fn bump_per_page_hit(counters: &Arc<RwLock<HashMap<String, u64>>>, page_key: &str) {
    let mut g = counters.write().unwrap();
    *g.entry(page_key.to_string()).or_insert(0) += 1;
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
    /// 剩余 Retry-After 注入次数。
    pub retry_after_hits: u32,
    pub retry_after_secs: u64,
    /// 页面请求计数（实际到达模拟站的请求，缓存命中不会到达这里）。
    pub page_requests: Arc<AtomicU64>,
    /// 当前在途页面请求数与历史峰值，用来验证站点在途上限。
    pub inflight: Arc<AtomicU64>,
    pub inflight_peak: Arc<AtomicU64>,
    /// 每页固定延迟（毫秒），制造慢响应。
    pub delays_ms: Arc<RwLock<HashMap<String, u64>>>,
    /// 每页阻塞闸门，配合 release 制造可控在途窗口。
    pub holds: Arc<RwLock<HashMap<String, Arc<HoldGate>>>>,
    /// 单页请求计数。
    pub per_page_hits: Arc<RwLock<HashMap<String, u64>>>,
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
            page_requests: Arc::new(AtomicU64::new(0)),
            inflight: Arc::new(AtomicU64::new(0)),
            inflight_peak: Arc::new(AtomicU64::new(0)),
            delays_ms: Arc::new(RwLock::new(HashMap::new())),
            holds: Arc::new(RwLock::new(HashMap::new())),
            per_page_hits: Arc::new(RwLock::new(HashMap::new())),
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

    /// 让某页面的每次请求固定延迟，制造“响应很慢”的在途窗口。
    pub fn set_page_delay_ms(&self, station_id: &str, page_key: &str, delay_ms: u64) {
        self.with_station(station_id, |s| {
            s.delays_ms
                .write()
                .unwrap()
                .insert(page_key.to_string(), delay_ms);
        });
    }

    /// 让某页面前 `waiters` 个请求阻塞，直到调用 release_page_hold。
    pub fn hold_page(&self, station_id: &str, page_key: &str, waiters: u64) {
        let gate = Arc::new(HoldGate::new(waiters));
        self.with_station(station_id, |s| {
            s.holds.write().unwrap().insert(page_key.to_string(), gate);
        });
    }

    /// 放行某页面上所有阻塞中的请求。
    pub fn release_page_hold(&self, station_id: &str, page_key: &str) {
        if let Some(s) = self.get_station(station_id) {
            if let Some(gate) = s.holds.read().unwrap().get(page_key).cloned() {
                gate.release();
            }
        }
    }

    /// 某站实际收到的页面请求总数（缓存/单飞命中不会产生请求）。
    pub fn page_request_count(&self, station_id: &str) -> u64 {
        self.get_station(station_id)
            .map(|s| s.page_requests.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// 全站页面请求总数。
    pub fn total_page_requests(&self) -> u64 {
        self.inner
            .read()
            .unwrap()
            .values()
            .map(|s| s.page_requests.load(Ordering::SeqCst))
            .sum()
    }

    /// 某站在途请求历史峰值，用来核对 max_inflight 限速。
    pub fn inflight_peak(&self, station_id: &str) -> u64 {
        self.get_station(station_id)
            .map(|s| s.inflight_peak.load(Ordering::SeqCst))
            .unwrap_or(0)
    }

    /// 单页实际收到的请求次数（验证同页同修订只发一次）。
    pub fn page_hits(&self, station_id: &str, page_key: &str) -> u64 {
        // 页面计数统一存在按页计数器里（serve 时写入）。
        self.get_station(station_id)
            .and_then(|s| s.per_page_hits.read().unwrap().get(page_key).copied())
            .unwrap_or(0)
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

    async fn serve_page(
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

        // 在途计数：先于故障注入，反映真实占用的在途额度。
        let inflight = st.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        st.inflight_peak.fetch_max(inflight, Ordering::SeqCst);
        st.page_requests.fetch_add(1, Ordering::SeqCst);
        bump_per_page_hit(&st.per_page_hits, page_key);
        // 慢响应与阻塞闸门：用于验证“响应很慢也只发一次请求”。
        let delay = st
            .delays_ms
            .read()
            .unwrap()
            .get(page_key)
            .copied()
            .unwrap_or(0);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        let gate = st.holds.read().unwrap().get(page_key).cloned();
        if let Some(gate) = gate {
            gate.pass().await;
        }
        let result = self
            .serve_page_inner(&mut st, station, page_key, target)
            .await;
        st.inflight.fetch_sub(1, Ordering::SeqCst);
        result
    }

    async fn serve_page_inner(
        &self,
        st: &mut MockStation,
        station: &StationConfig,
        page_key: &str,
        target: crate::fetch::FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
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
        self.serve_page(station, page_key, target).await
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
    ) -> Result<(Vec<ChangeLogEntry>, Option<i64>)> {
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
}

pub fn router(world: MockWorld, station_id: &'static str) -> Router {
    Router::new()
        .route("/state", get(state_handler))
        .route("/changes", get(changes_handler))
        .route("/pages/:key", get(page_handler))
        .route("/inject/fail", post(inject_fail_handler))
        .route("/inject/retry-after", post(inject_retry_handler))
        .route("/inject/delay", post(inject_delay_handler))
        .route("/inject/hold", post(inject_hold_handler))
        .route("/inject/release", post(release_hold_handler))
        .route("/requests", get(requests_handler))
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

    // 在途与请求计数（真实到达模拟站的请求）。
    let inflight = st.inflight.fetch_add(1, Ordering::SeqCst) + 1;
    st.inflight_peak.fetch_max(inflight, Ordering::SeqCst);
    st.page_requests.fetch_add(1, Ordering::SeqCst);
    bump_per_page_hit(&st.per_page_hits, &key);
    // 慢响应/阻塞闸门，制造可观测的在途窗口。
    let delay = st.delays_ms.read().unwrap().get(&key).copied().unwrap_or(0);
    if delay > 0 {
        tokio::time::sleep(Duration::from_millis(delay)).await;
    }
    let gate = st.holds.read().unwrap().get(&key).cloned();
    if let Some(gate) = gate {
        gate.pass().await;
    }

    let result = page_handler_inner(&world, sid, &st, &cfg, &key, &q).await;
    st.inflight.fetch_sub(1, Ordering::SeqCst);
    result
}

async fn page_handler_inner(
    world: &MockWorld,
    sid: &str,
    st: &MockStation,
    cfg: &StationConfig,
    key: &str,
    q: &PageQuery,
) -> Result<axum::response::Response, StatusCode> {
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
        (Some(rev), _) => st.version_at(key, Some(rev)),
        (None, Some(seq)) => st.version_as_of(key, seq),
        (None, None) => st.version_at(key, None),
    }
    .ok_or(StatusCode::NOT_FOUND)?;
    if v.removed {
        return Err(StatusCode::GONE);
    }
    let (ct, body) = render(cfg, key, v);
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

#[derive(serde::Deserialize)]
struct PageInjectQuery {
    page: String,
    #[serde(default)]
    delay_ms: Option<u64>,
    #[serde(default)]
    waiters: Option<u64>,
}

async fn inject_delay_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<PageInjectQuery>,
) -> AxumJson<serde_json::Value> {
    let delay = q.delay_ms.unwrap_or(0);
    world.set_page_delay_ms(sid, &q.page, delay);
    AxumJson(json!({"ok": true, "station_id": sid, "page": q.page, "delay_ms": delay}))
}

async fn inject_hold_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<PageInjectQuery>,
) -> AxumJson<serde_json::Value> {
    let waiters = q.waiters.unwrap_or(1);
    world.hold_page(sid, &q.page, waiters);
    AxumJson(json!({"ok": true, "station_id": sid, "page": q.page, "waiters": waiters}))
}

async fn release_hold_handler(
    State((world, sid)): State<HttpState>,
    Query(q): Query<PageInjectQuery>,
) -> AxumJson<serde_json::Value> {
    world.release_page_hold(sid, &q.page);
    AxumJson(json!({"ok": true, "station_id": sid, "page": q.page, "released": true}))
}

async fn requests_handler(State((world, sid)): State<HttpState>) -> AxumJson<serde_json::Value> {
    let st = match world.get_station(sid) {
        Some(s) => s,
        None => {
            return AxumJson(
                json!({"station_id": sid, "page_requests": 0, "inflight_peak": 0, "per_page": {}}),
            )
        }
    };
    AxumJson(json!({
        "station_id": sid,
        "page_requests": st.page_requests.load(Ordering::SeqCst),
        "inflight_peak": st.inflight_peak.load(Ordering::SeqCst),
        "per_page": st.per_page_hits.read().unwrap().clone(),
    }))
}
