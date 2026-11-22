//! 站点访问边界：
//! - 越界下游页键（路径逃逸、多段路径、未配置站点）被拦截，不得入队；
//! - 合法的站内跳转（同源 302）继续可采；
//! - 跨站自动跳转（跨源 302）被拒绝；
//! - 显式跨站链接（data-station）继续可采。
mod common;

use std::sync::Arc;

use common::wait_run;
use drainage_collector::engine::Engine;
use drainage_collector::fetch::{FetchTarget, Fetcher, HttpPageClient, PageFetchError};
use drainage_collector::mock::{router, MockPageClient, MockStation, MockWorld};
use drainage_collector::model::{FacilityInfo, LinkRef, RunMode, SiteFormat, StationConfig};
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;

fn fac(name: &str) -> FacilityInfo {
    FacilityInfo {
        name: Some(name.into()),
        kind: Some("manhole".into()),
    }
}

fn link(station: &str, page: &str) -> LinkRef {
    LinkRef {
        station_id: station.into(),
        page_key: page.into(),
    }
}

/// 构造边界演示世界：a0 的下游同时包含合法站内链接、显式跨站链接与各种越界页键。
fn boundary_world() -> MockWorld {
    let world = MockWorld::new();
    let mut a = MockStation::new("station-a");
    a.put_version(
        1,
        "a0",
        1,
        fac("A站入流口"),
        vec![
            link("station-a", "a1"),       // 合法站内链接
            link("station-b", "b1"),       // 显式跨站链接（合法）
            link("station-a", "../state"), // 路径逃逸：会定位到台账外的 /state
            link("station-a", ".."),       // 上级目录
            link("station-a", "a/b"),      // 多段路径
            link("station-c", "x1"),       // 未配置站点
        ],
        "registered",
    );
    a.put_version(2, "a1", 1, fac("A1号检查井"), vec![], "registered");
    world.add_station(a);
    let mut b = MockStation::new("station-b");
    b.put_version(3, "b1", 1, fac("B1号调蓄池"), vec![], "registered");
    world.add_station(b);
    world
}

#[tokio::test]
async fn out_of_ledger_links_are_blocked_legit_links_still_collected() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("boundary.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = boundary_world();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client.clone()));
    let engine = Engine::new(store.clone(), fetcher);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);

    // 只有合法页面被采集：a0、a1、b1；越界页键从未入队。
    let keys = engine.reachable_keys_for_equivalence(run_id).await.unwrap();
    let expected: std::collections::HashSet<_> = [
        ("station-a".to_string(), "a0".to_string(), 1),
        ("station-a".to_string(), "a1".to_string(), 1),
        ("station-b".to_string(), "b1".to_string(), 1),
    ]
    .into_iter()
    .collect();
    assert_eq!(keys, expected);

    // 工作队列里也只有这三个页面（越界页键在入队前被拦截）。
    let queued: Vec<(String, String)> = sqlx::query_as(
        "SELECT station_id, page_key FROM run_pages WHERE run_id = ? \
         ORDER BY station_id, page_key",
    )
    .bind(run_id)
    .fetch_all(&store.pool)
    .await
    .unwrap();
    assert_eq!(
        queued,
        vec![
            ("station-a".to_string(), "a0".to_string()),
            ("station-a".to_string(), "a1".to_string()),
            ("station-b".to_string(), "b1".to_string()),
        ]
    );

    // 越界页键连一次抓取请求都没有产生。
    assert_eq!(client.request_count(), 3);
}

/// 在本地监听端口上起一个 HTML 模拟站，返回其 base_url 与站点配置。
async fn spawn_html_station(world: MockWorld) -> (String, StationConfig) {
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(world, "station-a"))
            .await
            .unwrap();
    });
    let base = format!("http://{addr}");
    let cfg = StationConfig {
        id: "station-a".into(),
        name: "城东养护站".into(),
        base_url: base.clone(),
        format: SiteFormat::Html,
        min_interval_ms: 0,
        max_inflight: 2,
        watermark_seq: 0,
    };
    (base, cfg)
}

#[tokio::test]
async fn in_site_redirect_is_followed() {
    let world = MockWorld::new();
    let mut a = MockStation::new("station-a");
    a.put_version(1, "a0", 1, fac("旧址页面"), vec![], "registered");
    a.put_version(2, "a1", 1, fac("新址页面"), vec![], "registered");
    world.add_station(a);
    let (base, cfg) = spawn_html_station(world.clone()).await;

    // 请求 a0 时站点 302 到同站的 a1（合法站内跳转）。
    world.with_station("station-a", |s| {
        s.redirect_hits = 1;
        s.redirect_to = format!("{base}/pages/a1");
    });

    let fetcher = Fetcher::new(Arc::new(HttpPageClient::new().unwrap()));
    let page = fetcher
        .fetch_page(&cfg, "a0", FetchTarget::AtRevision(1))
        .await
        .unwrap();
    assert!(
        page.body.contains("新址页面"),
        "应跟随站内跳转取到 a1 的正文: {}",
        page.body
    );
    // 来源证明记录跳转后的最终地址。
    assert!(
        page.source.starts_with(&format!("{base}/pages/a1")),
        "来源应为最终地址: {}",
        page.source
    );
}

#[tokio::test]
async fn cross_site_redirect_is_refused() {
    let world = MockWorld::new();
    let mut a = MockStation::new("station-a");
    a.put_version(1, "a0", 1, fac("A站入流口"), vec![], "registered");
    world.add_station(a);
    let (_base, cfg) = spawn_html_station(world.clone()).await;

    // 请求 a0 时站点试图 302 到另一个源（不同端口即不同站点范围；
    // 目标无监听也没关系——边界策略应在发起连接前拦截）。
    world.with_station("station-a", |s| {
        s.redirect_hits = 1;
        s.redirect_to = "http://127.0.0.1:9/pages/x".into();
    });

    let fetcher = Fetcher::new(Arc::new(HttpPageClient::new().unwrap()));
    let err = fetcher
        .fetch_page(&cfg, "a0", FetchTarget::AtRevision(1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, PageFetchError::OutOfBoundary(_)),
        "跨站自动跳转应被拒绝，实际: {err}"
    );
}
