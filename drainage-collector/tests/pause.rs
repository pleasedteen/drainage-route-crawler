//! 值班员暂停 / 重启 / 继续：请求与结果全链路验证（全部走模拟站）。
mod common;

use std::sync::Arc;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{MockPageClient, MockWorld};
use drainage_collector::model::RunMode;
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;

const ENTRY_PAGES: usize = 8;

fn db_url(dir: &tempfile::TempDir, name: &str) -> String {
    format!("sqlite://{}", dir.path().join(name).display())
}

/// 给两站打上请求来源标签；同一张图、同一个站点 id，仅 base_url 前缀不同。
async fn tag_stations(store: &Store, tag: &str) {
    let mut stations = store.list_stations().await.unwrap();
    for st in &mut stations {
        let bare = st
            .base_url
            .rsplit('@')
            .next()
            .unwrap_or(&st.base_url)
            .to_string();
        st.base_url = format!("mock://{tag}@{bare}");
        st.min_interval_ms = 25;
        st.max_inflight = 1;
        store.upsert_station(st).await.unwrap();
    }
}

fn engine_for(store: Store, world: &MockWorld) -> (Arc<Engine>, Arc<MockPageClient>) {
    let client = Arc::new(MockPageClient::new(world.clone()));
    (
        Engine::new(store, Arc::new(Fetcher::new(client.clone()))),
        client,
    )
}

// ---------- 核心：暂停停住请求与结果 -> 重启仍暂停 -> 继续复用原轮次 ----------

#[tokio::test]
async fn pause_then_restart_then_resume_keeps_run_and_confirmed_results() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(&dir, "pause.db");
    let world = MockWorld::demo();

    // ---- 第一次“进程”：起轮次，跑到一半时确认暂停 ----
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    tag_stations(&store, "run1").await;
    let (engine, client) = engine_for(store.clone(), &world);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    common::wait_progress(&engine, run_id, |p| p.done >= 2).await;
    let before = engine.progress(run_id).await.unwrap();
    assert_eq!(before.status, "running");
    assert!(
        before.pending + before.fetching > 0,
        "暂停前应仍有未完成页面: {before:?}"
    );
    let reach_before = engine.reachable_keys_for_equivalence(run_id).await.unwrap();
    let requests_before = client.request_count();

    engine.pause_run(run_id).await.unwrap();
    let paused = engine.progress(run_id).await.unwrap();
    assert_eq!(paused.status, "paused");
    assert!(paused.paused_at.is_some(), "进度查询应能看出已确认暂停");
    assert_eq!(paused.done, before.done, "应停在暂停确认时的进度");
    assert_eq!(paused.fetching, 0, "在途页面应已放回队列");
    assert!(!engine.is_executing(run_id).await, "执行任务应已停下");

    // 暂停后：不再发请求，已核实结果纹丝不动。
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(
        client.request_count(),
        requests_before,
        "暂停后不再发出页面请求"
    );
    assert_eq!(
        engine.reachable_keys_for_equivalence(run_id).await.unwrap(),
        reach_before,
        "暂停不改已核实结果"
    );

    // 同入口不能另起轮次（暂停也算未走完）。
    assert!(
        engine.start_run(1, RunMode::Full).await.is_err(),
        "暂停中的入口不得另起轮次"
    );
    let requests_at_pause = world.ledger().count("run1");
    let pages_at_pause: std::collections::BTreeSet<String> = world.ledger().pages("run1");

    // 模拟进程退出：丢弃引擎与数据库连接，模拟站数据保留。
    drop(engine);
    drop(store);

    // ---- 第二次“进程”：重启 ----
    let store2 = Store::connect(&url).await.unwrap();
    let (engine2, client2) = engine_for(store2.clone(), &world);

    let booted = engine2.resume_on_boot().await.unwrap();
    assert!(
        !booted.contains(&run_id),
        "重启不得自动续跑已暂停轮次"
    );
    let after_boot = engine2.progress(run_id).await.unwrap();
    assert_eq!(after_boot.status, "paused", "重启后暂停状态仍在");
    assert!(after_boot.paused_at.is_some());
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(client2.request_count(), 0, "重启后暂停轮次不得有请求");
    assert!(
        engine2.start_run(1, RunMode::Full).await.is_err(),
        "重启后同入口仍不得另起轮次"
    );

    // ---- 继续：沿用原轮次 ID 与各站截止点 ----
    let wm_before = store2.get_run(run_id).await.unwrap().watermarks;
    assert_eq!(engine2.resume_run(run_id).await.unwrap(), Some(run_id));
    assert_eq!(
        store2.get_run(run_id).await.unwrap().status.as_str(),
        "running"
    );
    let wm_after = store2.get_run(run_id).await.unwrap().watermarks;
    assert_eq!(wm_after, wm_before, "继续沿用原各站截止点");

    let p = common::wait_run(&engine2, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, ENTRY_PAGES as i64);

    // 已核实结果保留且补齐：暂停前确认的页面一个不少。
    let reach_after = engine2
        .reachable_keys_for_equivalence(run_id)
        .await
        .unwrap();
    assert!(
        reach_after.is_superset(&reach_before),
        "继续后已核实结果不得丢失"
    );

    // 同修订已确认的抓取被复用：继续后新增请求不超过剩余页面数。
    let remaining = ENTRY_PAGES.saturating_sub(reach_before.len());
    assert!(
        client2.request_count() as usize <= remaining,
        "继续应复用已确认抓取：新增请求 {}，剩余页面 {}",
        client2.request_count(),
        remaining
    );
    // 暂停前抓过的页面在继续后没有被重新抓取。
    // 暂停前抓过的页面在继续后没有被重新抓取：继续只补抓此前未请求过的页面。
    let pages_after = world.ledger().pages("run1");
    assert_eq!(
        requests_at_pause as usize,
        pages_at_pause.len(),
        "暂停前每次页面请求都对应一个页面（无重试）"
    );
    assert_eq!(
        pages_after.len(),
        pages_at_pause.len() + client2.request_count() as usize,
        "继续只补抓了此前未抓过的页面"
    );

    // 每个 (station, page, revision) 只有一行，无重复抓取。
    let dup: i64 = sqlx::query_as::<_, (i64,)>(
        "SELECT COUNT(*) FROM (SELECT 1 FROM page_revisions \
         GROUP BY station_id, page_key, revision HAVING COUNT(*) > 1)",
    )
    .fetch_one(&store2.pool)
    .await
    .unwrap()
    .0;
    assert_eq!(dup, 0);
}

// ---------- 站点正在退避时也能确认暂停 ----------

#[tokio::test]
async fn pause_is_confirmed_immediately_while_station_is_backing_off() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(&dir, "backoff.db");
    let world = MockWorld::demo();
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    tag_stations(&store, "run1").await;
    let (engine, _client) = engine_for(store.clone(), &world);

    // A 站要求等待 3 秒：轮次会停在站点的等待里。
    world.with_station("station-a", |s| {
        s.retry_after_hits = 1;
        s.retry_after_secs = 3;
    });

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    // 等页面抓取遇阻、进入站点等待。
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let started = std::time::Instant::now();
    engine.pause_run(run_id).await.unwrap();
    let elapsed = started.elapsed();

    // 若暂停没有打断站点等待，这里至少要等 3 秒。
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "退避期间确认暂停应立即生效，实际耗时 {:?}",
        elapsed
    );
    let p = engine.progress(run_id).await.unwrap();
    assert_eq!(p.status, "paused");
    assert!(p.paused_at.is_some());

    // 继续后照常完成。
    assert_eq!(engine.resume_run(run_id).await.unwrap(), Some(run_id));
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, ENTRY_PAGES as i64);
}

// ---------- 暂停只拦本入口，另一入口在同一共用站点上照常采集 ----------

#[tokio::test]
async fn pausing_one_entry_leaves_shared_station_run_going() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(&dir, "shared.db");
    let world = MockWorld::demo();
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let entry2 = store
        .add_entry("station-b", "b1", "河西调蓄池入口")
        .await
        .unwrap();

    // 入口 1 用 run1 标签配置起跑。
    tag_stations(&store, "run1").await;
    let (engine, _client) = engine_for(store.clone(), &world);
    let run1 = engine.start_run(1, RunMode::Full).await.unwrap();
    common::wait_progress(&engine, run1, |p| p.done >= 1).await;

    // 入口 2 在同一对共用站点上另起轮次（站点 id 不变，仅来源标签变化）。
    tag_stations(&store, "run2").await;
    let run2 = engine.start_run(entry2, RunMode::Full).await.unwrap();

    // 让共用站点暂时报错（两轮共享同一站点控制器，节奏照旧）。
    world.with_station("station-a", |s| s.transient_failures = 1);

    // 确认暂停入口 1。
    engine.pause_run(run1).await.unwrap();
    assert_eq!(engine.progress(run1).await.unwrap().status, "paused");
    let n1 = world.ledger().count("run1");

    // 入口 2 继续推进直到完成；共用站点没有被入口 1 的暂停拦住。
    let p2 = common::wait_run(&engine, run2).await;
    assert_eq!(p2.status, "succeeded", "{:?}", p2.error);
    // 从 b1 出发的可达子图：b1、b2、b3。
    assert_eq!(p2.reachable, 3);
    assert!(
        world.ledger().count("run2") > 0,
        "另一入口应照常采集"
    );

    // 暂停期间入口 1 没有新请求。
    assert_eq!(
        world.ledger().count("run1"),
        n1,
        "暂停后此入口来源不应再有请求"
    );

    // 继续入口 1，共用站点按原节奏补齐剩余页面。
    assert_eq!(engine.resume_run(run1).await.unwrap(), Some(run1));
    let p1 = common::wait_run(&engine, run1).await;
    assert_eq!(p1.status, "succeeded", "{:?}", p1.error);
    assert_eq!(p1.reachable, ENTRY_PAGES as i64);

    // 共用站点节奏由站点控制器统一维护：两轮（含暂停/继续前后）对该站的
    // 每一次请求都满足站点最小发起间隔（25ms，留调度误差）。
    let min_gap = std::time::Duration::from_millis(20);
    for station in ["station-a", "station-b"] {
        let times = world.ledger().station_times(station);
        assert!(times.len() >= 3, "{station} 应有多轮请求");
        for pair in times.windows(2) {
            assert!(
                pair[1] >= pair[0] + min_gap,
                "{station} 请求间隔过短: {:?}",
                pair[1] - pair[0]
            );
        }
    }
}

// ---------- 暂停/继续的边界 ----------

#[tokio::test]
async fn pause_rejects_finished_and_resume_rejects_running() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(&dir, "edges.db");
    let world = MockWorld::demo();
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let (engine, _client) = engine_for(store, &world);

    let done = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, done).await;
    assert_eq!(p.status, "succeeded");
    assert!(
        engine.pause_run(done).await.is_err(),
        "已结束轮次不能暂停"
    );

    let running = engine.start_run(1, RunMode::Full).await.unwrap();
    assert!(
        engine.resume_run(running).await.is_err(),
        "进行中的轮次不能继续"
    );
    // 重复暂停是幂等的。
    engine.pause_run(running).await.unwrap();
    engine.pause_run(running).await.unwrap();
    assert_eq!(engine.progress(running).await.unwrap().status, "paused");
    // 继续两次：第二次应被拒绝（状态已不是 paused）。
    assert_eq!(engine.resume_run(running).await.unwrap(), Some(running));
    assert!(engine.resume_run(running).await.is_err());
    let p = common::wait_run(&engine, running).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
}

// ---------- 跨进程确认暂停：另一个进程落库后，采集中进程应尽快停下 ----------

#[tokio::test]
async fn pause_confirmed_by_another_process_stops_worker() {
    let dir = tempfile::tempdir().unwrap();
    let url = db_url(&dir, "cross.db");
    let world = MockWorld::demo();

    // 进程 1：发起并执行轮次。
    let store1 = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store1).await.unwrap();
    tag_stations(&store1, "run1").await;
    let (engine1, client1) = engine_for(store1.clone(), &world);
    let run_id = engine1.start_run(1, RunMode::Full).await.unwrap();
    common::wait_progress(&engine1, run_id, |p| p.done >= 1).await;

    // 进程 2：只连同一个库确认暂停（没有该轮的执行任务）。
    let store2 = Store::connect(&url).await.unwrap();
    let (engine2, _c2) = engine_for(store2.clone(), &world);
    assert!(!engine2.is_executing(run_id).await);
    engine2.pause_run(run_id).await.unwrap();
    assert_eq!(engine2.progress(run_id).await.unwrap().status, "paused");

    // 进程 1 应在看门狗的一个轮询周期内停下，且不再发请求。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let p = engine1.progress(run_id).await.unwrap();
        if p.fetching == 0 && !engine1.is_executing(run_id).await {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "另一进程确认暂停后执行任务未停下: {p:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let n = client1.request_count();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(client1.request_count(), n, "跨进程暂停后不得再发请求");
    assert_eq!(engine1.progress(run_id).await.unwrap().status, "paused");

    // 任一进程继续，轮次都应跑完。
    assert_eq!(engine2.resume_run(run_id).await.unwrap(), Some(run_id));
    let p = common::wait_run(&engine1, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, ENTRY_PAGES as i64);
}
