//! 页面解析：分页 HTML 与 JSON 两种站点格式。
//!
//! 下游链接允许跨站：HTML 中用 `data-station` 指向外站，缺省即本站；JSON 同理。

use anyhow::{anyhow, Result};
use scraper::{Html, Selector};
use serde::Deserialize;

use crate::model::{FacilityInfo, LinkRef, ParsedPage, SiteFormat};

#[derive(Debug, Deserialize)]
struct JsonPage {
    #[allow(dead_code)]
    page_key: String,
    revision: i64,
    facility: Option<JsonFacility>,
    #[serde(default)]
    links: Vec<JsonLink>,
}

#[derive(Debug, Deserialize)]
struct JsonFacility {
    name: Option<String>,
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonLink {
    station_id: Option<String>,
    page_key: String,
}

/// 解析页面。`own_station_id` 用于补全未显式标注站点的相对链接。
pub fn parse_page(format: SiteFormat, own_station_id: &str, body: &str) -> Result<ParsedPage> {
    match format {
        SiteFormat::Json => parse_json(own_station_id, body),
        SiteFormat::Html => parse_html(own_station_id, body),
    }
}

fn parse_json(own_station_id: &str, body: &str) -> Result<ParsedPage> {
    let jp: JsonPage = serde_json::from_str(body).map_err(|e| anyhow!("JSON 页面解析失败: {e}"))?;
    let facility = jp
        .facility
        .map(|f| FacilityInfo {
            name: f.name,
            kind: f.kind,
        })
        .unwrap_or_default();
    let links = jp
        .links
        .into_iter()
        .map(|l| LinkRef {
            station_id: l.station_id.unwrap_or_else(|| own_station_id.to_string()),
            page_key: l.page_key,
        })
        .collect();
    Ok(ParsedPage {
        revision: jp.revision,
        facility,
        links,
    })
}

fn parse_html(own_station_id: &str, body: &str) -> Result<ParsedPage> {
    let doc = Html::parse_document(body);

    let revision = doc
        .select(&Selector::parse("[data-revision]").unwrap())
        .next()
        .and_then(|el| el.value().attr("data-revision"))
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or_else(|| anyhow!("HTML 页面缺少 data-revision"))?;

    let name = doc
        .select(&Selector::parse(".facility-name").unwrap())
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
        .filter(|s| !s.is_empty());
    let kind = doc
        .select(&Selector::parse(".facility-kind").unwrap())
        .next()
        .map(|el| el.text().collect::<String>().trim().to_string())
        .filter(|s| !s.is_empty());

    let links = doc
        .select(&Selector::parse("a.downstream").unwrap())
        .filter_map(|el| {
            let page_key = el.value().attr("data-page")?.to_string();
            let station_id = el
                .value()
                .attr("data-station")
                .unwrap_or(own_station_id)
                .to_string();
            Some(LinkRef {
                station_id,
                page_key,
            })
        })
        .collect();

    Ok(ParsedPage {
        revision,
        facility: FacilityInfo { name, kind },
        links,
    })
}

/// 解析 JSON 变更清单响应。
#[derive(Debug, Deserialize)]
pub struct JsonChanges {
    pub changes: Vec<JsonChange>,
    pub next_after: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct JsonChange {
    pub seq: i64,
    pub page_key: String,
    pub revision: i64,
    pub kind: String,
}

pub fn parse_json_changes(body: &str) -> Result<JsonChanges> {
    Ok(serde_json::from_str(body)?)
}
