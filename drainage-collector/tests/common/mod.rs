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

pub async fn fresh_engine_for_store(store: Store, world: MockWorld) -> Arc<Engine> {
    let client = Arc::new(MockPageClient::new(world));
    let fetcher = Arc::new(Fetcher::new(client));
    Engine::new(store, fetcher)
}

/// 在同一份持久化库上构造两个独立引擎，模拟“常驻服务”与“独立 CLI”两个进程
/// 共用采集库：各自持有独立的 Fetcher（独立限速/在途闸门），但底层 SQLite 与
/// 模拟站完全共享，从而可以用真实请求计数验证跨进程单飞与接手。
///
/// 租约 TTL/心跳通过 [`Engine::with_lease_timing`] 按引擎显式传入，
/// 不使用进程级环境变量，避免并行测试之间互相干扰。
pub async fn two_engines_shared_store(
    lease_ttl_ms: u64,
    heartbeat_ms: u64,
) -> (
    Arc<Engine>,
    Arc<Engine>,
    Store,
    MockWorld,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("shared.db");
    let url = format!("sqlite://{}", db_path.display());
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = MockWorld::demo();

    let store2 = Store::connect(&url).await.unwrap();
    let client1 = Arc::new(MockPageClient::new(world.clone()));
    let client2 = Arc::new(MockPageClient::new(world.clone()));
    let engine1 = Engine::with_lease_timing(
        store.clone(),
        Arc::new(Fetcher::new(client1)),
        lease_ttl_ms,
        heartbeat_ms,
    );
    let engine2 = Engine::with_lease_timing(
        store2,
        Arc::new(Fetcher::new(client2)),
        lease_ttl_ms,
        heartbeat_ms,
    );
    (engine1, engine2, store, world, dir)
}

/// 阻塞等待直到某轮次离开 running（带超时）。
pub async fn wait_run_id(engine: &Engine, run_id: i64) -> drainage_collector::model::RunProgress {
    wait_run(engine, run_id).await
}

pub fn stations() -> Vec<StationConfig> {
    Vec::new()
}
