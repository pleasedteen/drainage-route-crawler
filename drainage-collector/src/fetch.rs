//! 抓取层：每站独立的发起频率 / 在途并发闸门、失败退避重试、
//! 收到站点等待要求（Retry-After）后暂停该站、其他站不受影响。
//!
//! ## 统一等待期（关键约束）
//! 任一**请求类型**（状态截止点 / 变更清单 / 页面）收到 `Retry-After`，
//! 都写入同一个 [`StationController`] 的暂停时刻：该站的读截止点、翻清单、
//! 抓页面在等待结束前都不会再发起新请求；其他站的控制器完全独立。
//! 即使重试次数已耗尽，最后一次命中的等待期依然生效（先暂停再返回错误）。
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use tokio::sync::{Mutex, Semaphore};

use crate::model::{ChangeLogEntry, StationConfig};
use crate::parse::parse_json_changes;

/// 一次页面抓取的结果。
#[derive(Debug)]
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
    /// 发起前发现该站正处于等待期：本次没有真正发请求，调用方应让出任务稍后再来。
    StationPaused(Duration),
    /// 永久失败（如 404/410 页面已移除）：本轮不再重试。
    Gone,
    Fatal(String),
}

impl std::fmt::Display for PageFetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageFetchError::Transient(s) => write!(f, "暂时失败: {s}"),
            PageFetchError::RetryAfter(d) => write!(f, "站点要求等待 {}ms", d.as_millis()),
            PageFetchError::StationPaused(d) => write!(f, "站点等待中，剩余 {}ms", d.as_millis()),
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
    ///
    /// 实现必须把“请求失败 / 响应无法解析”与“合法的 seq=0”区分开：
    /// 失败时返回 `Err`，绝不能用 0 冒充截止点（否则会把整站变更误判为越界）。
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
}

impl StationController {
    fn new(cfg: StationConfig) -> Self {
        Self {
            inflight: Semaphore::new(cfg.max_inflight as usize),
            cfg,
            last_request: Mutex::new(None),
            paused_until: Mutex::new(None),
        }
    }

    /// 取得在途许可并满足最小发起间隔；若站点正处于等待期则**立即返回错误**，
    /// 绝不持有许可/任务睡眠——这样故障站等待期间页面工作槽可让给其他站。
    async fn acquire(self: &Arc<Self>) -> std::result::Result<tokio::sync::SemaphorePermit<'_>, PageFetchError> {
        loop {
            // 先排队拿在途额度（信号量本身按站独立，不影响其他站）。
            let permit = self
                .inflight
                .acquire()
                .await
                .expect("semaphore never closed");
            // 站点暂停期间不占在途额度，直接把许可还回去并报错，由上层让出任务。
            if let Some(remain) = self.pause_remaining().await {
                drop(permit);
                return Err(PageFetchError::StationPaused(remain));
            }
            // 满足最小发起间隔。间隔等待只在本控制器内，且不涉及跨站任务让位。
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
            return Ok(permit);
        }
    }

    /// 记录站点等待期。三类请求共用同一个暂停时刻，取更长者。
    async fn pause_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut g = self.paused_until.lock().await;
        *g = Some(match *g {
            Some(old) if old > until => old,
            _ => until,
        });
    }

    /// 距等待期结束还剩多久；未暂停或已过期返回 None。
    async fn pause_remaining(&self) -> Option<Duration> {
        match *self.paused_until.lock().await {
            Some(until) => {
                let now = Instant::now();
                if until > now {
                    Some(until - now)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// 站点当前是否处于等待期（不阻塞，供页面 worker 让出工作槽前判断）。
    async fn is_paused(&self) -> bool {
        self.pause_remaining().await.is_some()
    }

    /// 某站等待期剩余时长的公开查询入口（未暂停返回 None）。
    async fn paused_remaining(&self) -> Option<Duration> {
        self.pause_remaining().await
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

    /// 把 429/503 上的 Retry-After 头翻译成统一错误；其余非成功状态按暂时失败处理。
    fn classify_status(status: StatusCode, headers: &reqwest::header::HeaderMap) -> PageFetchError {
        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::SERVICE_UNAVAILABLE {
            if let Some(h) = headers.get(reqwest::header::RETRY_AFTER) {
                if let Some(d) = parse_retry_after(h.to_str().unwrap_or("")) {
                    return PageFetchError::RetryAfter(d);
                }
            }
        }
        PageFetchError::Transient(format!("HTTP {status}"))
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
        if !status.is_success() {
            return Err(Self::classify_status(status, resp.headers()));
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
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow!("状态请求连接失败: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            // 状态请求同样识别 Retry-After，交给上层进入站点统一等待期。
            return Err(Self::classify_status(status, resp.headers()).into());
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| anyhow!("状态响应解析失败: {e}"))?;
        // 字段缺失/类型错误必须报错：不能把失败伪装成截止点 0。
        v.get("latest_seq")
            .and_then(|n| n.as_i64())
            .ok_or_else(|| anyhow!("状态响应缺少合法的 latest_seq 字段: {v}"))
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
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| anyhow!("清单请求连接失败: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            // 清单请求同样识别 Retry-After，等待期内该站不再翻页。
            return Err(Self::classify_status(status, resp.headers()).into());
        }
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("清单响应读取失败: {e}"))?;
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

/// 单请求最多尝试次数（页面 / 状态 / 清单三类共用同一预算）。
pub const MAX_ATTEMPTS: u32 = 5;
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

    /// 某站当前是否处于 Retry-After 等待期。
    pub async fn station_paused(&self, station_id: &str) -> bool {
        match self.controllers.lock().await.get(station_id).cloned() {
            Some(ctl) => ctl.is_paused().await,
            None => false,
        }
    }

    /// 某站等待期剩余时长（未暂停返回 None），供测试与编排层精确判断。
    pub async fn station_pause_remaining(&self, station_id: &str) -> Option<Duration> {
        match self.controllers.lock().await.get(station_id).cloned() {
            Some(ctl) => ctl.paused_remaining().await,
            None => None,
        }
    }

    /// 计算第 attempt 次（从 1 开始）暂时失败后的退避时长：指数 + 抖动。
    fn backoff(attempt: u32) -> Duration {
        let base = Duration::from_millis(80) * 2u32.pow(attempt - 1);
        let base = base.min(MAX_BACKOFF);
        let jitter = Duration::from_millis(
            (rand::random::<u64>() % 40).min(base.as_millis() as u64),
        );
        base + jitter
    }

    /// 抓一次页面：按站节奏拿许可，真正发出**至多一次**请求。
    ///
    /// 本方法**不做睡眠式重试**，从而保证故障站等待期间没有 worker 抱着任务睡死：
    /// - 发起前发现站点在等待期 → 直接返回 [`PageFetchError::StationPaused`]，不发请求；
    /// - 响应带 Retry-After → 先把等待期写入该站控制器（即使这是最后一次允许尝试，
    ///   等待也照样生效），再返回 [`PageFetchError::RetryAfter`]，由编排层让出工作槽；
    /// - 普通暂时失败 → 在本方法内做有限的退避重试（退避期间不持有在途许可）。
    pub async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> std::result::Result<FetchedPage, PageFetchError> {
        let ctl = self.controller(station).await;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            // 非阻塞：暂停中立刻报错返回，调用方（worker）让出任务去服务其他站。
            let permit = match ctl.acquire().await {
                Ok(p) => p,
                Err(PageFetchError::StationPaused(d)) => {
                    return Err(PageFetchError::StationPaused(d));
                }
                Err(other) => return Err(other),
            };
            let result = self.client.fetch_page(station, page_key, target).await;
            drop(permit);
            match result {
                Ok(page) => return Ok(page),
                Err(PageFetchError::Gone) => return Err(PageFetchError::Gone),
                Err(PageFetchError::Fatal(m)) => return Err(PageFetchError::Fatal(m)),
                Err(PageFetchError::StationPaused(d)) => {
                    return Err(PageFetchError::StationPaused(d))
                }
                Err(PageFetchError::RetryAfter(d)) => {
                    // 关键：无论重试预算是否耗尽，等待期先落到该站控制器，
                    // 该站三类请求在等待结束前都不会再新发。
                    ctl.pause_for(d).await;
                    return Err(PageFetchError::RetryAfter(d));
                }
                Err(PageFetchError::Transient(msg)) => {
                    if attempt >= MAX_ATTEMPTS {
                        return Err(PageFetchError::Transient(format!(
                            "{msg}（已重试 {MAX_ATTEMPTS} 次）"
                        )));
                    }
                    tokio::time::sleep(Self::backoff(attempt)).await;
                }
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
    ///
    /// Retry-After 与页面/清单请求走**同一个**站点等待期；请求彻底失败时返回 `Err`，
    /// 调用方绝不能把截止点填成 0。
    pub async fn fetch_state(&self, station: &StationConfig) -> Result<i64> {
        let ctl = self.controller(station).await;
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let permit = ctl.acquire().await.map_err(|e| anyhow!(e))?;
            let result = self.client.fetch_state(station).await;
            drop(permit);
            match result {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // anyhow 下钻：HTTP 层可能携带 PageFetchError（Retry-After 等）。
                    match e.downcast_ref::<PageFetchError>() {
                        Some(PageFetchError::RetryAfter(d)) => {
                            // 耗尽与否等待期都生效。
                            ctl.pause_for(*d).await;
                            if attempt >= MAX_ATTEMPTS {
                                return Err(anyhow!(
                                    "状态请求遭遇持续 Retry-After（已重试 {MAX_ATTEMPTS} 次）"
                                ));
                            }
                            tokio::time::sleep(*d).await;
                        }
                        Some(PageFetchError::StationPaused(d)) => {
                            if attempt >= MAX_ATTEMPTS {
                                return Err(anyhow!("状态请求时站点持续处于等待期"));
                            }
                            tokio::time::sleep(*d).await;
                        }
                        Some(other) => {
                            if attempt >= MAX_ATTEMPTS {
                                return Err(anyhow!(
                                    "状态请求失败: {other}（已重试 {MAX_ATTEMPTS} 次）"
                                ));
                            }
                            tokio::time::sleep(Self::backoff(attempt)).await;
                        }
                        None => {
                            if attempt >= MAX_ATTEMPTS {
                                return Err(e.context(format!(
                                    "状态请求连续失败（已重试 {MAX_ATTEMPTS} 次）"
                                )));
                            }
                            tokio::time::sleep(Duration::from_millis(100) * attempt).await;
                        }
                    }
                }
            }
        }
    }

    /// 续读变更清单直到无更多页，且只保留不超过截止点的条目。
    ///
    /// 翻页期间收到 Retry-After 同样进入该站统一等待期，等待结束前不再翻页；
    /// 重试预算耗尽时等待期也已生效，随后返回错误。
    pub async fn fetch_changes_until(
        &self,
        station: &StationConfig,
        after_seq: i64,
        cutoff: i64,
    ) -> Result<Vec<ChangeLogEntry>> {
        let ctl = self.controller(station).await;
        let mut cursor = after_seq;
        let mut out = Vec::new();
        loop {
            let mut attempt: u32 = 0;
            let (entries, next) = loop {
                attempt += 1;
                let permit = ctl.acquire().await.map_err(|e| anyhow!(e))?;
                let result = self.client.fetch_changes(station, cursor).await;
                drop(permit);
                match result {
                    Ok(v) => break v,
                    Err(e) => match e.downcast_ref::<PageFetchError>() {
                        Some(PageFetchError::RetryAfter(d)) => {
                            ctl.pause_for(*d).await;
                            if attempt >= MAX_ATTEMPTS {
                                return Err(anyhow!(
                                    "变更清单遭遇持续 Retry-After（已重试 {MAX_ATTEMPTS} 次）"
                                ));
                            }
                            tokio::time::sleep(*d).await;
                        }
                        Some(PageFetchError::StationPaused(d)) => {
                            if attempt >= MAX_ATTEMPTS {
                                return Err(anyhow!("变更清单请求时站点持续处于等待期"));
                            }
                            tokio::time::sleep(*d).await;
                        }
                        Some(other) => {
                            if attempt >= MAX_ATTEMPTS {
                                return Err(anyhow!(
                                    "变更清单请求失败: {other}（已重试 {MAX_ATTEMPTS} 次）"
                                ));
                            }
                            tokio::time::sleep(Self::backoff(attempt)).await;
                        }
                        None => {
                            if attempt >= MAX_ATTEMPTS {
                                return Err(e.context(format!(
                                    "变更清单连续失败（已重试 {MAX_ATTEMPTS} 次）"
                                )));
                            }
                            tokio::time::sleep(Duration::from_millis(100) * attempt).await;
                        }
                    },
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
