//! 配置文件加载（TOML）。
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::model::StationConfig;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub mock: Option<MockConfig>,
    #[serde(default)]
    pub stations: Vec<StationConfig>,
    #[serde(default)]
    pub entry_points: Vec<EntrySeed>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    /// postgres:// 用于生产；sqlite:// 用于本地测试。
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MockConfig {
    /// 本地模拟站点监听起始端口（HTML 站用 base_port，JSON 站用 base_port+1）。
    pub base_port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct EntrySeed {
    pub station_id: String,
    pub page_key: String,
    pub label: String,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
        let cfg: AppConfig = toml::from_str(&text).context("解析配置文件失败")?;
        Ok(cfg)
    }
}
