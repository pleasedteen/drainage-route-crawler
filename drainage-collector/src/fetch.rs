//! 抓取层：每站独立的发起频率 / 在途并发闸门、失败退避重试、
//! 收到站点等待要求（Retry-After）后暂停该站、其他站不受影响。
//!
//! 暂停是**站点级**的：状态读取、变更清单、页面抓取共用同一个
//! [`StationController`]，因此任一类请求收到 `Retry-After` 后，本站后续的
//! 所有请求都必须等到等待期结束；即使触发等待的那次请求已经用尽重试，
//! 等待期也照样生效（先登记暂停，再返回失败）。
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use reqwest::{Client, Response, StatusCode};
use tokio::sync::{Mutex, Notify, Semaphore};

use crate::model::{ChangeLogEntry, StationConfig};
use crate::parse::parse_json_changes;

/// 一次页面抓取的结果。
pub struct FetchedPage {
    pub body: String,
    pub content_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageFetchError {
    /// 暂时失败：可退避重试。
    Transient(String),
    /// 站点要求等待到指定时长后再请求（Retry-After）。
    RetryAfter(Duration),
    /// 永久失败（如 404/410 页面已移除）：本轮不再重试。
    Gone,
    Fatal(String),
}

impl std::fmt::Display for PageFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageFetchError::Transient(s) => write!(f, "暂时失败: {s}"),
            PageFetchError::RetryAfter(d) => write!(f, "站点要求等待 {}ms", d.as_millis()),
            PageFetchError::Gone => write!(f, "页面已移除 (404/410)"),
            PageFetchError::Fatal(s) => write!(f, "永久失败: {s}"),
        }
    }
}

impl std::error::Error for PageFetchError {}

/// 抓取目标：明确修订号，或按登记截止点取当时最新版本。
#[derive(Debug, Clone, Copy)]
pub enum FetchTarget {
    AtRevision(i64),
    AtSeq(i64),
}

/// 站点客户端抽象：生产为 HTTP，测试可用内存实现。
///
/// 三类请求（状态 / 变更清单 / 页面）都以 [`PageFetchError`] 表达失败，
/// 其中非成功 HTTP 响应一律按失败处理，绝不把错误响应当作正常数据。
#[async_trait]
pub trait PageClient: Send + Sync {
    /// 抓取页面。
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError>;

    /// 读取某站当前登记截止序号。
    async fn fetch_state(
        &self,
        station: &StationConfig,
    ) -> std::result::Result<i64, PageFetchError>;

    /// 续读变更清单：返回严格大于 after_seq 的一页及下一页游标。
    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> std::result::Result<(Vec<ChangeLogEntry>, Option<i64>), PageFetchError>;
}

// ---------- 每站节奏控制器 ----------

struct StationController {
    cfg: StationConfig,
    last_request: Mutex<Option<Instant>>,
    inflight: Semaphore,
    paused_until: Mutex<Option<Instant>>,
    wake: Notify,
}

impl StationController {
    fn new(cfg: StationConfig) -> Self {
        Self {
            inflight: Semaphore::new(cfg.max_inflight.max(1) as usize),
            cfg,
            last_request: Mutex::new(None),
            paused_until: Mutex::new(None),
            wake: Notify::new(),
        }
    }

    /// 取得在途许可并满足最小发起间隔；若站点被要求暂停则等待到恢复时刻。
    async fn acquire(self: &Arc<Self>) -> tokio::sync::SemaphorePermit<'_> {
        loop {
            // 先排队拿在途额度。
            let permit = self
                .inflight
                .acquire()
                .await
                .expect("semaphore never closed");
            // 站点暂停期间不占在途额度。
            let pause = *self.paused_until.lock().await;
            if let Some(until) = pause {
                let now = Instant::now();
                if until > now {
                    drop(permit);
                    tokio::select! {
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(until)) => {}
                        _ = self.wake.notified() => {}
                    }
                    continue;
                }
            }
            // 满足最小发起间隔。
            let wait = {
                let mut last = self.last_request.lock().await;
                let min_gap = Duration::from_millis(self.cfg.min_interval_ms);
                match *last {
                    Some(t) if t.elapsed() < min_gap => Some(min_gap - t.elapsed()),
                    _ => {
                        *last = Some(Instant::now());
                        None
                    }
                }
            };
            if let Some(w) = wait {
                drop(permit);
                tokio::time::sleep(w).await;
                continue;
            }
            return permit;
        }
    }

    async fn pause_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut g = self.paused_until.lock().await;
        *g = Some(match *g {
            Some(old) if old > until => old,
            _ => until,
        });
        drop(g);
        self.wake.notify_waiters();
    }
}

// ---------- HTTP 客户端 ----------

pub struct HttpPageClient {
    http: Client,
}

impl HttpPageClient {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(15))
            .user_agent("drainage-collector/0.1")
            .build()?;
        Ok(Self { http })
    }

    async fn get(&self, url: &str) -> std::result::Result<Response, PageFetchError> {
        self.http
            .get(url)
            .send()
            .await
            .map_err(|e| PageFetchError::Transient(format!("连接失败: {e}")))
    }
}

/// 把非成功 HTTP 响应归类为可重试 / 等待 / 永久错误。
fn classify_response(resp: &Response) -> PageFetchError {
    let status = resp.status();
    if matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE) {
        return PageFetchError::Gone;
    }
    if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
        if let Some(d) = retry_after_header(resp) {
            return PageFetchError::RetryAfter(d);
        }
    }
    PageFetchError::Transient(format!("HTTP {status}"))
}

fn retry_after_header(resp: &Response) -> Option<Duration> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_retry_after)
}

#[async_trait]
impl PageClient for HttpPageClient {
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        let mut url = format!(
            "{}/pages/{}",
            station.base_url.trim_end_matches('/'),
            page_key
        );
        match target {
            FetchTarget::AtRevision(rev) => url.push_str(&format!("?revision={rev}")),
            FetchTarget::AtSeq(seq) => url.push_str(&format!("?as_of={seq}")),
        }
        let resp = self.get(&url).await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(classify_response(&resp));
        }
        let content_type = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let body = resp
            .text()
            .await
            .map_err(|e| PageFetchError::Transient(format!("读取响应失败: {e}")))?;
        Ok(FetchedPage { body, content_type })
    }

    async fn fetch_state(
        &self,
        station: &StationConfig,
    ) -> std::result::Result<i64, PageFetchError> {
        let url = format!("{}/state", station.base_url.trim_end_matches('/'));
        let resp = self.get(&url).await?;
        if !resp.status().is_success() {
            return Err(classify_response(&resp));
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| PageFetchError::Transient(format!("状态响应解析失败: {e}")))?;
        v.get("latest_seq")
            .and_then(|n| n.as_i64())
            .ok_or_else(|| PageFetchError::Fatal("状态响应缺少 latest_seq".into()))
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> std::result::Result<(Vec<ChangeLogEntry>, Option<i64>), PageFetchError> {
        let url = format!(
            "{}/changes?after={}",
            station.base_url.trim_end_matches('/'),
            after_seq
        );
        let resp = self.get(&url).await?;
        if !resp.status().is_success() {
            return Err(classify_response(&resp));
        }
        let body = resp
            .text()
            .await
            .map_err(|e| PageFetchError::Transient(format!("读取变更清单失败: {e}")))?;
        let parsed = parse_json_changes(&body)
            .map_err(|e| PageFetchError::Transient(format!("变更清单解析失败: {e}")))?;
        let entries = parsed
            .changes
            .into_iter()
            .map(|c| ChangeLogEntry {
                seq: c.seq,
                station_id: station.id.clone(),
                page_key: c.page_key,
                revision: c.revision,
                kind: c.kind,
            })
            .collect();
        Ok((entries, parsed.next_after))
    }
}

// ---------- 带退避与暂停的抓取门面 ----------

pub struct Fetcher {
    client: Arc<dyn PageClient>,
    controllers: Mutex<HashMap<String, Arc<StationController>>>,
}

/// 单页最多尝试次数。
const MAX_ATTEMPTS: u32 = 5;
/// 退避上限。
const MAX_BACKOFF: Duration = Duration::from_secs(8);

/// 在站点控制器约束下重试一次操作。
///
/// `Retry-After` 的处理顺序很关键：**先**把等待期登记到站点控制器，
/// **再**判断是否还能重试。这样即使本次请求已经是最后一次尝试，
/// 本站后续的状态 / 清单 / 页面请求也会继续遵守该等待期。
async fn with_retries<T, F, Fut>(
    ctl: &Arc<StationController>,
    mut op: F,
) -> std::result::Result<T, PageFetchError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, PageFetchError>> + Send,
{
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let permit = ctl.acquire().await;
        match op().await {
            Ok(v) => return Ok(v),
            Err(PageFetchError::Gone) => return Err(PageFetchError::Gone),
            Err(PageFetchError::Fatal(m)) => return Err(PageFetchError::Fatal(m)),
            Err(PageFetchError::RetryAfter(d)) => {
                drop(permit);
                // 无论是否继续重试，都必须让本站后续请求遵守等待期。
                ctl.pause_for(d).await;
                if attempt >= MAX_ATTEMPTS {
                    return Err(PageFetchError::RetryAfter(d));
                }
            }
            Err(PageFetchError::Transient(msg)) => {
                drop(permit);
                if attempt >= MAX_ATTEMPTS {
                    return Err(PageFetchError::Transient(format!(
                        "{msg}（已重试 {MAX_ATTEMPTS} 次）"
                    )));
                }
                // 退避：指数 + 抖动。
                let base = (Duration::from_millis(80) * 2u32.pow(attempt - 1)).min(MAX_BACKOFF);
                let jitter =
                    Duration::from_millis((rand::random::<u64>() % 40).min(base.as_millis() as u64));
                tokio::time::sleep(base + jitter).await;
            }
        }
    }
}

impl Fetcher {
    pub fn new(client: Arc<dyn PageClient>) -> Self {
        Self {
            client,
            controllers: Mutex::new(HashMap::new()),
        }
    }

    async fn controller(&self, cfg: &StationConfig) -> Arc<StationController> {
        let mut g = self.controllers.lock().await;
        g.entry(cfg.id.clone())
            .or_insert_with(|| Arc::new(StationController::new(cfg.clone())))
            .clone()
    }

    /// 带退避重试地抓页面。收到 Retry-After 时只暂停对应站点。
    pub async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        let ctl = self.controller(station).await;
        with_retries(&ctl, || self.client.fetch_page(station, page_key, target)).await
    }

    /// 站点要求等待：暂停该站，其它站继续。
    pub async fn pause_station(&self, station_id: &str, d: Duration) {
        if let Some(ctl) = self.controllers.lock().await.get(station_id).cloned() {
            ctl.pause_for(d).await;
        }
    }

    /// 该站当前是否可立即发起请求（不在等待期内）。
    pub async fn is_available(&self, station_id: &str) -> bool {
        let ctl = self.controllers.lock().await.get(station_id).cloned();
        match ctl {
            None => true,
            Some(ctl) => {
                let pause = *ctl.paused_until.lock().await;
                pause.map_or(true, |until| until <= Instant::now())
            }
        }
    }

    /// 带重试地读取状态截止点；非成功响应按失败处理。
    pub async fn fetch_state(
        &self,
        station: &StationConfig,
    ) -> std::result::Result<i64, PageFetchError> {
        let ctl = self.controller(station).await;
        with_retries(&ctl, || self.client.fetch_state(station)).await
    }

    /// 续读变更清单直到无更多页，且只保留不超过截止点的条目。
    /// 非成功响应按失败处理；Retry-After 与页面请求共用本站等待期。
    pub async fn fetch_changes_until(
        &self,
        station: &StationConfig,
        after_seq: i64,
        cutoff: i64,
    ) -> std::result::Result<Vec<ChangeLogEntry>, PageFetchError> {
        let ctl = self.controller(station).await;
        let mut cursor = after_seq;
        let mut out = Vec::new();
        loop {
            let (entries, next) =
                with_retries(&ctl, || self.client.fetch_changes(station, cursor)).await?;
            let mut stop = false;
            for e in entries {
                if e.seq > cutoff {
                    stop = true;
                    break;
                }
                out.push(e);
            }
            match next {
                Some(n) if !stop && n > cursor => cursor = n,
                _ => break,
            }
        }
        Ok(out)
    }
}

/// 解析 Retry-After：支持秒数与 HTTP-date（取与当前时刻的差值）。
fn parse_retry_after(value: &str) -> Option<Duration> {
    let v = value.trim();
    if let Ok(secs) = v.parse::<u64>() {
        return Some(Duration::from_secs(secs.max(1)));
    }
    // HTTP-date 形如 "Wed, 21 Oct 2026 07:28:00 GMT"
    let ts = parse_http_date(v)?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    Some(Duration::from_secs(ts.saturating_sub(now).max(1)))
}

/// 不引入额外依赖地解析 RFC 7231 IMF-fixdate。
fn parse_http_date(s: &str) -> Option<u64> {
    let mut parts = s.splitn(2, ',');
    let rest = parts.nth(1)?.trim();
    let mut it = rest.split_whitespace();
    let day: u64 = it.next()?.parse().ok()?;
    let mon = match it.next()? {
        "Jan" => 1u64,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let year: u64 = it.next()?.parse().ok()?;
    let mut hms = it.next()?.split(':');
    let hour: u64 = hms.next()?.parse().ok()?;
    let min: u64 = hms.next()?.parse().ok()?;
    let sec: u64 = hms.next()?.parse().ok()?;
    Some(days_from_civil(year, mon, day) * 86400 + hour * 3600 + min * 60 + sec)
}

/// Howard Hinnant  civil-from-days 逆算法。
fn days_from_civil(y: u64, m: u64, d: u64) -> u64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
