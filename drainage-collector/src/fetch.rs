//! 抓取层：每站独立的发起频率 / 在途并发闸门、失败退避重试、
//! 收到站点等待要求（Retry-After）后暂停该站、其他站不受影响。
//!
//! 值班员确认暂停某个轮次时，通过 [`PauseToken`] 只中断该轮次的抓取，
//! 站点控制器（频率、在途、Retry-After 暂停）仍按原节奏服务其他轮次。
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use tokio::sync::{Mutex, Notify, Semaphore};

use crate::model::{ChangeLogEntry, StationConfig};
use crate::parse::parse_json_changes;

/// 一次页面抓取的结果。
pub struct FetchedPage {
    pub body: String,
    pub content_type: String,
}

#[derive(Debug)]
pub enum PageFetchError {
    /// 暂时失败：可退避重试。
    Transient(String),
    /// 站点要求等待到指定时长后再请求（Retry-After）。
    RetryAfter(Duration),
    /// 永久失败（如 404/410 页面已移除）：本轮不再重试。
    Gone,
    Fatal(String),
    /// 该轮次已被值班员确认暂停：不再发起请求，页面留待继续时处理。
    Paused,
}

impl std::fmt::Display for PageFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageFetchError::Transient(s) => write!(f, "暂时失败: {s}"),
            PageFetchError::RetryAfter(d) => write!(f, "站点要求等待 {}ms", d.as_millis()),
            PageFetchError::Gone => write!(f, "页面已移除 (404/410)"),
            PageFetchError::Fatal(s) => write!(f, "永久失败: {s}"),
            PageFetchError::Paused => write!(f, "轮次已暂停"),
        }
    }
}

impl std::error::Error for PageFetchError {}

/// 某个轮次的暂停信号。每个轮次一个，与站点节奏控制器完全独立：
/// 暂停轮次 A 不会让共用站点的轮次 B 停下来。
#[derive(Clone, Default)]
pub struct PauseToken {
    paused: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl PauseToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// 确认暂停：置位并唤醒所有在退避/排队/请求中的等待者。
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// 等到该轮次被暂停为止；未暂停时永久挂起。可安全地放进 `select!`。
    pub async fn wait_paused(&self) {
        loop {
            // 先登记通知再检查标志，避免错过置位与唤醒之间的竞态。
            let notified = self.notify.notified();
            if self.is_paused() {
                return;
            }
            notified.await;
            if self.is_paused() {
                return;
            }
        }
    }
}

/// 抓取目标：明确修订号，或按登记截止点取当时最新版本。
#[derive(Debug, Clone, Copy)]
pub enum FetchTarget {
    AtRevision(i64),
    AtSeq(i64),
}

/// 站点客户端抽象：生产为 HTTP，测试可用内存实现。
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
    async fn fetch_state(&self, station: &StationConfig) -> Result<i64>;

    /// 续读变更清单：返回严格大于 after_seq 的一页及下一页游标。
    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> Result<(Vec<ChangeLogEntry>, Option<i64>)>;
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
            inflight: Semaphore::new(cfg.max_inflight as usize),
            cfg,
            last_request: Mutex::new(None),
            paused_until: Mutex::new(None),
            wake: Notify::new(),
        }
    }

    /// 取得在途许可并满足最小发起间隔；若站点被要求暂停则等待到恢复时刻。
    /// 返回 `None` 表示该轮次已被值班员确认暂停，应立即放弃本次抓取
    /// （站点控制器自身的节奏与暂停状态不受影响）。
    async fn acquire(
        self: &Arc<Self>,
        pause: &PauseToken,
    ) -> Option<tokio::sync::SemaphorePermit<'_>> {
        loop {
            if pause.is_paused() {
                return None;
            }
            // 先排队拿在途额度；等待期间可被轮次暂停打断。
            let permit = tokio::select! {
                p = self.inflight.acquire() => p.expect("semaphore never closed"),
                _ = pause.wait_paused() => return None,
            };
            // 站点暂停期间不占在途额度（这是站点的节奏，不是轮次的暂停）。
            let pause_until = *self.paused_until.lock().await;
            if let Some(until) = pause_until {
                let now = Instant::now();
                if until > now {
                    drop(permit);
                    tokio::select! {
                        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(until)) => {}
                        _ = self.wake.notified() => {}
                        _ = pause.wait_paused() => return None,
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
                tokio::select! {
                    _ = tokio::time::sleep(w) => {}
                    _ = pause.wait_paused() => return None,
                }
                continue;
            }
            return Some(permit);
        }
    }

    async fn pause_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut g = self.paused_until.lock().await;
        *g = Some(match *g {
            Some(old) if old > until => old,
            _ => until,
        });
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
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| PageFetchError::Transient(format!("连接失败: {e}")))?;
        let status = resp.status();
        if matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE) {
            return Err(PageFetchError::Gone);
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
            if let Some(h) = resp.headers().get(reqwest::header::RETRY_AFTER) {
                if let Some(d) = parse_retry_after(h.to_str().unwrap_or("")) {
                    return Err(PageFetchError::RetryAfter(d));
                }
            }
            return Err(PageFetchError::Transient(format!("HTTP {status}")));
        }
        if !status.is_success() {
            return Err(PageFetchError::Transient(format!("HTTP {status}")));
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

    async fn fetch_state(&self, station: &StationConfig) -> Result<i64> {
        let url = format!("{}/state", station.base_url.trim_end_matches('/'));
        let v: serde_json::Value = self.http.get(&url).send().await?.json().await?;
        Ok(v.get("latest_seq").and_then(|n| n.as_i64()).unwrap_or(0))
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        let url = format!(
            "{}/changes?after={}",
            station.base_url.trim_end_matches('/'),
            after_seq
        );
        let body = self.http.get(&url).send().await?.text().await?;
        let parsed = parse_json_changes(&body)?;
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

    /// 带退避重试地抓页面。收到 Retry-After 时只暂停对应站点；
    /// 该轮次被值班员确认暂停时立刻返回 [`PageFetchError::Paused`]，不再发新请求。
    pub async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
        pause: &PauseToken,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        let ctl = self.controller(station).await;
        let mut attempt: u32 = 0;
        loop {
            if pause.is_paused() {
                return Err(PageFetchError::Paused);
            }
            attempt += 1;
            let _permit = match ctl.acquire(pause).await {
                Some(p) => p,
                None => return Err(PageFetchError::Paused),
            };
            // 等待在途额度期间可能已被暂停，发请求前最后确认一次。
            if pause.is_paused() {
                return Err(PageFetchError::Paused);
            }
            match self.client.fetch_page(station, page_key, target).await {
                Ok(page) => return Ok(page),
                Err(PageFetchError::Gone) => return Err(PageFetchError::Gone),
                Err(PageFetchError::Fatal(m)) => return Err(PageFetchError::Fatal(m)),
                Err(PageFetchError::RetryAfter(d)) => {
                    if attempt >= MAX_ATTEMPTS {
                        return Err(PageFetchError::Transient(format!(
                            "站点持续要求等待（已重试 {MAX_ATTEMPTS} 次）"
                        )));
                    }
                    // 只暂停该站；其他站的控制器不受影响。
                    drop(_permit);
                    ctl.pause_for(d).await;
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = pause.wait_paused() => return Err(PageFetchError::Paused),
                    }
                }
                Err(PageFetchError::Transient(msg)) => {
                    if attempt >= MAX_ATTEMPTS {
                        return Err(PageFetchError::Transient(format!(
                            "{msg}（已重试 {MAX_ATTEMPTS} 次）"
                        )));
                    }
                    // 退避：指数 + 抖动。
                    let base = Duration::from_millis(80) * 2u32.pow(attempt - 1);
                    let base = base.min(MAX_BACKOFF);
                    let jitter = Duration::from_millis(
                        (rand::random::<u64>() % 40).min(base.as_millis() as u64),
                    );
                    drop(_permit);
                    // 站点正在退避时同样允许确认暂停。
                    tokio::select! {
                        _ = tokio::time::sleep(base + jitter) => {}
                        _ = pause.wait_paused() => return Err(PageFetchError::Paused),
                    }
                }
                Err(PageFetchError::Paused) => return Err(PageFetchError::Paused),
            }
        }
    }

    /// 站点要求等待：暂停该站，其它站继续。
    pub async fn pause_station(&self, station_id: &str, d: Duration) {
        if let Some(ctl) = self.controllers.lock().await.get(station_id).cloned() {
            ctl.pause_for(d).await;
        }
    }

    /// 带重试地读取状态截止点。
    pub async fn fetch_state(
        &self,
        station: &StationConfig,
        pause: &PauseToken,
    ) -> Result<i64> {
        let ctl = self.controller(station).await;
        let mut attempt = 0;
        loop {
            if pause.is_paused() {
                return Err(anyhow::anyhow!(PageFetchError::Paused));
            }
            attempt += 1;
            let _permit = match ctl.acquire(pause).await {
                Some(p) => p,
                None => return Err(anyhow::anyhow!(PageFetchError::Paused)),
            };
            if pause.is_paused() {
                return Err(anyhow::anyhow!(PageFetchError::Paused));
            }
            match self.client.fetch_state(station).await {
                Ok(v) => return Ok(v),
                Err(e) if attempt >= MAX_ATTEMPTS => return Err(e),
                Err(_) => {
                    drop(_permit);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(100) * attempt) => {}
                        _ = pause.wait_paused() => return Err(anyhow::anyhow!(PageFetchError::Paused)),
                    }
                }
            }
        }
    }

    /// 续读变更清单直到无更多页，且只保留不超过截止点的条目。
    pub async fn fetch_changes_until(
        &self,
        station: &StationConfig,
        after_seq: i64,
        cutoff: i64,
        pause: &PauseToken,
    ) -> Result<Vec<ChangeLogEntry>> {
        let ctl = self.controller(station).await;
        let mut cursor = after_seq;
        let mut out = Vec::new();
        loop {
            if pause.is_paused() {
                return Err(anyhow::anyhow!(PageFetchError::Paused));
            }
            let mut attempt = 0;
            let (entries, next) = loop {
                attempt += 1;
                let _permit = match ctl.acquire(pause).await {
                    Some(p) => p,
                    None => return Err(anyhow::anyhow!(PageFetchError::Paused)),
                };
                if pause.is_paused() {
                    return Err(anyhow::anyhow!(PageFetchError::Paused));
                }
                match self.client.fetch_changes(station, cursor).await {
                    Ok(v) => break v,
                    Err(e) if attempt >= MAX_ATTEMPTS => return Err(e),
                    Err(_) => {
                        drop(_permit);
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(100) * attempt) => {}
                            _ = pause.wait_paused() => return Err(anyhow::anyhow!(PageFetchError::Paused)),
                        }
                    }
                }
            };
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
