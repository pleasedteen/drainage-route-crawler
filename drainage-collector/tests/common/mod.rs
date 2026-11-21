//! 公共测试夹具：内存模拟站 + 文件型 SQLite（验证嵌入式持久化替身）。
#![allow(dead_code)]
use std::sync::Arc;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{MockPageClient, MockWorld};
use drainage_collector::model::StationConfig;
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
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

/// 等到出现满足条件的一次进度快照（带超时）。
pub async fn wait_progress(
    engine: &Engine,
    run_id: i64,
    mut pred: impl FnMut(&drainage_collector::model::RunProgress) -> bool,
) -> drainage_collector::model::RunProgress {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let p = engine.progress(run_id).await.unwrap();
        if pred(&p) {
            return p;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待轮次 {run_id} 进度条件超时: {p:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
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
