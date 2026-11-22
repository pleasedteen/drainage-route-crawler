//! 站点访问边界：由站点配置（`base_url`）推导台账范围，
//! 下游页键定位与自动跳转都不得越出该范围。
//!
//! 边界规则：
//! - 页键只能是安全的单段路径键（白名单字符，不含 `.`/`..` 段），
//!   且经 URL 规范化后仍落在 `{base_url}/pages/` 台账根之内；
//! - 自动跳转（HTTP 3xx）只允许留在发起站点的同源范围内；
//! - 页面里显式标注的跨站链接不受影响：它们以目标站自己的边界发起全新请求。

use reqwest::Url;

use crate::fetch::PageFetchError;
use crate::model::StationConfig;

/// 页键最大长度。
const MAX_PAGE_KEY_LEN: usize = 128;

/// 页键字符白名单：RFC 3986 unreserved 子集（单段路径安全字符）。
/// 拒绝 `/`、`\`、`?`、`#`、`%` 等可注入路径或查询的字符。
fn is_safe_page_key(page_key: &str) -> bool {
    !page_key.is_empty()
        && page_key.len() <= MAX_PAGE_KEY_LEN
        && page_key != "."
        && page_key != ".."
        && page_key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// 站点台账根（ledger root）：站点配置 `base_url` 下的 `/pages/` 集合。
pub fn ledger_root(station: &StationConfig) -> String {
    format!("{}/pages/", station.base_url.trim_end_matches('/'))
}

/// 校验下游页键并给出它在目标站台账内的定位 URL（不含查询参数）。
///
/// 两道防线：
/// 1. 页键字符白名单（拒绝路径逃逸所需的字符）；
/// 2. URL 规范化（点段消除、百分号解码）后仍须落在台账根之内。
///
/// 越界返回 [`PageFetchError::OutOfBoundary`]。
pub fn page_locator(station: &StationConfig, page_key: &str) -> Result<String, PageFetchError> {
    if !is_safe_page_key(page_key) {
        return Err(PageFetchError::OutOfBoundary(format!(
            "页键 {page_key:?} 不是合法的台账单段键（仅允许字母数字与 . _ -）"
        )));
    }
    let root = ledger_root(station);
    let root_url = Url::parse(&root).map_err(|e| {
        PageFetchError::OutOfBoundary(format!("站点 {} 的台账根 {root:?} 无效: {e}", station.id))
    })?;
    let raw = format!("{root}{page_key}");
    let normalized = Url::parse(&raw).map_err(|e| {
        PageFetchError::OutOfBoundary(format!(
            "页键 {page_key:?} 在站点 {} 台账内定位失败: {e}",
            station.id
        ))
    })?;
    if normalized.as_str().starts_with(root_url.as_str()) {
        Ok(normalized.into())
    } else {
        Err(PageFetchError::OutOfBoundary(format!(
            "页键 {page_key:?} 越出站点 {} 的台账范围 {root}",
            station.id
        )))
    }
}

/// 自动跳转边界：目标 URL 必须与发起请求同源（scheme / host / 端口一致）。
pub fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{SiteFormat, StationConfig};

    fn station(base: &str) -> StationConfig {
        StationConfig {
            id: "s".into(),
            name: "s".into(),
            base_url: base.into(),
            format: SiteFormat::Html,
            min_interval_ms: 0,
            max_inflight: 1,
            watermark_seq: 0,
        }
    }

    #[test]
    fn safe_keys_locate_inside_ledger() {
        let st = station("http://127.0.0.1:9100");
        assert_eq!(
            page_locator(&st, "a1").unwrap(),
            "http://127.0.0.1:9100/pages/a1"
        );
        assert!(page_locator(&st, "MH-1024.2_北").is_err()); // 非 ASCII 不在白名单
        assert!(page_locator(&st, "MH-1024.2_1").is_ok());
    }

    #[test]
    fn traversal_keys_are_rejected() {
        let st = station("http://127.0.0.1:9100");
        for bad in [
            "../state",
            "..",
            ".",
            "a/b",
            "a\\b",
            "a?x=1",
            "a#f",
            "a%2e%2e",
            "",
            "http://evil/x",
        ] {
            assert!(page_locator(&st, bad).is_err(), "应拒绝 {bad:?}");
        }
    }

    #[test]
    fn mock_scheme_base_url_works() {
        let st = station("mock://station-a");
        assert_eq!(
            page_locator(&st, "a0").unwrap(),
            "mock://station-a/pages/a0"
        );
    }

    #[test]
    fn base_url_with_path_prefix() {
        let st = station("https://stations.example.org/a/");
        assert_eq!(
            page_locator(&st, "a1").unwrap(),
            "https://stations.example.org/a/pages/a1"
        );
    }

    #[test]
    fn origin_comparison() {
        let a = Url::parse("http://127.0.0.1:9100/pages/a0").unwrap();
        assert!(same_origin(
            &a,
            &Url::parse("http://127.0.0.1:9100/pages/a1").unwrap()
        ));
        assert!(!same_origin(
            &a,
            &Url::parse("http://127.0.0.1:9101/pages/b1").unwrap()
        ));
        assert!(!same_origin(
            &a,
            &Url::parse("https://127.0.0.1:9100/pages/a0").unwrap()
        ));
        // 默认端口归一化。
        let b = Url::parse("http://example.org/x").unwrap();
        assert!(same_origin(
            &b,
            &Url::parse("http://example.org:80/y").unwrap()
        ));
    }
}
