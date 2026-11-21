//! 领域模型：站点、入流口、轮次、页面、变更清单等。
use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

/// 站点页面格式：分页 HTML 或 JSON。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SiteFormat {
    Html,
    Json,
}

impl SiteFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            SiteFormat::Html => "html",
            SiteFormat::Json => "json",
        }
    }
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "html" => Ok(SiteFormat::Html),
            "json" => Ok(SiteFormat::Json),
            other => Err(anyhow::anyhow!("未知站点格式: {other}")),
        }
    }
}

/// 养护站配置（含本站点的发起频率与在途请求上限）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StationConfig {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub format: SiteFormat,
    /// 两次请求之间的最小间隔（毫秒）。
    pub min_interval_ms: u64,
    /// 同时在途请求数上限。
    pub max_inflight: u32,
    /// 最近一次已登记变更序号（水位），由采集过程推进。
    #[serde(default)]
    pub watermark_seq: i64,
}

/// 值班员选定的入流口。
#[derive(Debug, Clone, Serialize)]
pub struct EntryPoint {
    pub id: i64,
    pub station_id: String,
    pub page_key: String,
    pub label: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunMode {
    /// 全量：从入流口重新走一遍当前图。
    Full,
    /// 增量：按变更清单补齐修订与新路线，未修订页面直接复用缓存。
    Incremental,
}

impl RunMode {
    pub fn as_str(self) -> &'static str {
        match self {
            RunMode::Full => "full",
            RunMode::Incremental => "incremental",
        }
    }
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s {
            "full" => Ok(RunMode::Full),
            "incremental" => Ok(RunMode::Incremental),
            other => Err(anyhow::anyhow!("未知采集模式: {other}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    /// 值班员确认暂停：停在当前进度，等待继续。
    Paused,
    Succeeded,
    Failed,
}

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Running => "running",
            RunStatus::Paused => "paused",
            RunStatus::Succeeded => "succeeded",
            RunStatus::Failed => "failed",
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "succeeded" => RunStatus::Succeeded,
            "failed" => RunStatus::Failed,
            "paused" => RunStatus::Paused,
            _ => RunStatus::Running,
        }
    }

    /// 未走完、会占用原入口的工作状态（同入口不得另起轮次）。
    pub fn is_open(self) -> bool {
        matches!(self, RunStatus::Running | RunStatus::Paused)
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 一个采集轮次。每轮开始时固化各站登记截止点（watermarks）。
#[derive(Debug, Clone)]
pub struct Run {
    pub id: i64,
    pub entry_id: i64,
    pub mode: RunMode,
    pub status: RunStatus,
    /// site_id -> 本轮截止登记序号
    pub watermarks: BTreeMap<String, i64>,
    pub error: Option<String>,
    /// 值班员确认暂停的时刻（仅 status = paused 时有值，重启后仍保留）。
    pub paused_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// 页面的某个修订版本（同一页面同一修订只抓取一次，跨入口/跨轮次共享）。
#[derive(Debug, Clone)]
pub struct PageRevision {
    pub id: i64,
    pub station_id: String,
    pub page_key: String,
    pub revision: i64,
    pub content_type: String,
    pub body: String,
    pub fetched_at: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LinkRef {
    pub station_id: String,
    pub page_key: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct FacilityInfo {
    pub name: Option<String>,
    pub kind: Option<String>,
}

/// 解析后的页面。
#[derive(Debug, Clone, Serialize)]
pub struct ParsedPage {
    pub revision: i64,
    pub facility: FacilityInfo,
    pub links: Vec<LinkRef>,
}

/// 变更清单条目（按站登记顺序，序号单调递增）。
#[derive(Debug, Clone, Serialize)]
pub struct ChangeLogEntry {
    pub seq: i64,
    pub station_id: String,
    pub page_key: String,
    pub revision: i64,
    /// registered / revised / removed
    pub kind: String,
}

/// 可达设施查询结果。
#[derive(Debug, Clone, Serialize)]
pub struct ReachableFacility {
    pub station_id: String,
    pub station_name: String,
    pub page_key: String,
    pub revision: Option<i64>,
    pub name: Option<String>,
    pub kind: Option<String>,
    pub depth: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunProgress {
    pub run_id: i64,
    pub status: String,
    pub mode: String,
    pub entry_id: i64,
    pub watermarks: BTreeMap<String, i64>,
    pub total: i64,
    pub done: i64,
    pub pending: i64,
    pub fetching: i64,
    pub failed: i64,
    pub reachable: i64,
    pub error: Option<String>,
    /// 已确认暂停的时刻（status = "paused" 时非空），便于查询接口看出暂停。
    pub paused_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
