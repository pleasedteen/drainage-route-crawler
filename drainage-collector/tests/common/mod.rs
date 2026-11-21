//! 公共测试夹具：内存模拟站（进程内客户端 / 真实 HTTP 监听两种形态）
//! + 文件型 SQLite（验证嵌入式持久化替身）。
#![allow(dead_code)]
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{router, MockPageClient, MockWorld};
use drainage_collector::model::{SiteFormat, StationConfig};
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;

pub async fn harness() -> (Arc<Engine>, Store, MockWorld, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let url = format!("sqlite://{}", db_path.display());
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = MockWorld::demo();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client));
    let engine = Engine::new(store.clone(), fetcher);
    (engine, store, world, dir)
}

pub async fn wait_run(engine: &Engine, run_id: i64) -> drainage_collector::model::RunProgress {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let p = engine.progress(run_id).await.unwrap();
        if p.status != "running" {
            return p;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "轮次 {run_id} 超时未结束"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

pub async fn fresh_engine_for_store(store: Store, world: MockWorld) -> Arc<Engine> {
    let client = Arc::new(MockPageClient::new(world));
    let fetcher = Arc::new(Fetcher::new(client));
    Engine::new(store, fetcher)
}

pub fn stations() -> Vec<StationConfig> {
    Vec::new()
}

/// 本地真实 HTTP 模拟站：HTML 站与 JSON 站各监听一个随机端口，共享同一世界。
pub struct HttpStations {
    pub world: MockWorld,
    pub base_url_a: String,
    pub base_url_b: String,
    pub http: reqwest::Client,
}

impl HttpStations {
    pub async fn spawn(world: MockWorld) -> Self {
        let (base_url_a, _h1) = serve(world.clone(), "station-a").await;
        let (base_url_b, _h2) = serve(world.clone(), "station-b").await;
        Self {
            http: reqwest::Client::new(),
            world,
            base_url_a,
            base_url_b,
        }
    }

    /// 两站各自的默认配置（测试会按需覆盖频率 / 并发）。
    pub fn configs(&self) -> Vec<StationConfig> {
        vec![
            StationConfig {
                id: "station-a".into(),
                name: "城东养护站".into(),
                base_url: self.base_url_a.clone(),
                format: SiteFormat::Html,
                min_interval_ms: 5,
                max_inflight: 2,
                watermark_seq: 0,
            },
            StationConfig {
                id: "station-b".into(),
                name: "河西养护站".into(),
                base_url: self.base_url_b.clone(),
                format: SiteFormat::Json,
                min_interval_ms: 5,
                max_inflight: 2,
                watermark_seq: 0,
            },
        ]
    }

    pub async fn inject(
        &self,
        station: &str,
        path: &str,
        query: &str,
    ) -> serde_json::Value {
        self.http
            .post(format!("{}{path}?{query}", self.base(station)))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    pub fn base(&self, station: &str) -> &str {
        if station == "station-a" {
            &self.base_url_a
        } else {
            &self.base_url_b
        }
    }
}

async fn serve(world: MockWorld, station_id: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let app = router(world, station_id);
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), handle)
}

/// 建立使用真实 HTTP 模拟站的引擎与数据库。
pub async fn http_harness(
    station_overrides: Option<BTreeMap<String, (u64, u32)>>,
) -> (Arc<Engine>, Store, HttpStations, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("http.db");
    let url = format!("sqlite://{}", db_path.display());
    let store = Store::connect(&url).await.unwrap();
    let stations = HttpStations::spawn(MockWorld::demo()).await;
    for mut cfg in stations.configs() {
        if let Some((gap, inflight)) = station_overrides
            .as_ref()
            .and_then(|m| m.get(&cfg.id))
            .copied()
        {
            cfg.min_interval_ms = gap;
            cfg.max_inflight = inflight;
        }
        store.upsert_station(&cfg).await.unwrap();
    }
    store.add_entry("station-a", "a0", "城东入流口").await.unwrap();
    let client = Arc::new(drainage_collector::fetch::HttpPageClient::new().unwrap());
    let fetcher = Arc::new(Fetcher::new(client));
    let engine = Engine::new(store.clone(), fetcher);
    (engine, store, stations, dir)
}

/// 轮询某入口轮次的进度，直到满足断言或超时。
pub async fn wait_until<F>(engine: &Engine, run_id: i64, mut pred: F) -> bool
where
    F: FnMut(&drainage_collector::model::RunProgress) -> bool,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let p = engine.progress(run_id).await.unwrap();
        if pred(&p) {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

