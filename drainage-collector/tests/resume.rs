//! 进程中途退出后重启续跑：已确认进度与抓取结果保留，原轮次最终完成。
mod common;

use std::sync::Arc;
use std::time::Duration;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::MockPageClient;
use drainage_collector::model::RunMode;
use drainage_collector::store::Store;

#[tokio::test]
async fn restart_resumes_interrupted_run_and_keeps_confirmed_progress() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("resume.db").display());

    // 第一次“进程”：起轮次后立即杀掉运行时（模拟中途退出）。
    let (run_id, cached_before, world) = {
        let store = Store::connect(&db_url).await.unwrap();
        drainage_collector::seed::seed_demo_stations(&store)
            .await
            .unwrap();
        // 放慢两站节奏，保证中断窗口内轮次仍在进行（全图约需 1-2 秒）。
        for mut st in store.list_stations().await.unwrap() {
            st.min_interval_ms = 40;
            st.max_inflight = 1;
            store.upsert_station(&st).await.unwrap();
        }
        let world = drainage_collector::mock::MockWorld::demo();
        let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone()))));
        let engine = Engine::new(store.clone(), fetcher);

        let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
        // 让它跑一会儿，保证部分页面已确认落库、但轮次尚未结束。
        tokio::time::sleep(Duration::from_millis(300)).await;

        let cached = count_cached(&store).await;
        assert!(cached >= 1, "中断前应至少确认了一个页面");
        // 不落任何“完成”标记，直接丢弃整个引擎/连接，模拟进程退出。
        drop(engine);
        drop(store);
        (run_id, cached, world)
    };

    // 第二次“进程”：重新打开同一个持久化文件。
    let store2 = Store::connect(&db_url).await.unwrap();
    let engine2 = common::fresh_engine_for_store(store2.clone(), world).await;

    // 轮次仍处于 running；崩溃残留的 fetching 被收回 pending。
    let run = store2.get_run(run_id).await.unwrap();
    assert_eq!(run.status.as_str(), "running");

    let resumed = engine2.resume_on_boot().await.unwrap();
    assert!(resumed.contains(&run_id));

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let p = engine2.progress(run_id).await.unwrap();
        if p.status != "running" {
            assert_eq!(p.status, "succeeded", "{:?}", p.error);
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // 已确认的抓取结果没有丢，且续跑不会重复写修订。
    let cached_after = count_cached(&store2).await;
    assert!(cached_after >= cached_before);
    let dup: i64 = sqlx::query(
        "SELECT COUNT(*) AS c FROM (SELECT 1 FROM page_revisions GROUP BY station_id, page_key, revision HAVING COUNT(*) > 1)",
    )
    .fetch_one(&store2.pool)
    .await
    .unwrap()
    .try_get::<i64, _>("c")
    .unwrap();
    assert_eq!(dup, 0);

    // 可达设施完整（演示图 8 个页面全部可达）。
    let reach = engine2.reachable(run_id).await.unwrap();
    assert_eq!(reach.len(), 8);
}

#[tokio::test]
async fn retry_after_pauses_only_that_station() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("retry.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    drainage_collector::seed::seed_demo_stations(&store)
        .await
        .unwrap();
    let world = drainage_collector::mock::MockWorld::demo();
    // B 站要求先等 1 秒；A 站应不受影响继续完成。
    world.with_station("station-b", |s| {
        s.retry_after_hits = 1;
        s.retry_after_secs = 1;
    });
    let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone()))));
    let engine = Engine::new(store.clone(), fetcher);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    // 暂停后重试成功，两站设施都可达。
    assert_eq!(p.reachable, 8);
}

#[tokio::test]
async fn transient_failures_are_retried_with_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("retry2.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    drainage_collector::seed::seed_demo_stations(&store)
        .await
        .unwrap();
    let world = drainage_collector::mock::MockWorld::demo();
    // 前两次请求失败，退避重试后成功。
    world.with_station("station-a", |s| s.transient_failures = 2);
    let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone()))));
    let engine = Engine::new(store.clone(), fetcher);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);
}

async fn count_cached(store: &Store) -> i64 {
    sqlx::query("SELECT COUNT(*) AS c FROM page_revisions")
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get::<i64, _>("c")
        .unwrap()
}

use sqlx::Row;
