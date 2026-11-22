//! 历史轮次修复：用原登记截止点重新核实路线、一次切换、失败/中断不影响已发布结果。
mod common;

use std::sync::Arc;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{MockPageClient, MockStation, MockWorld};
use drainage_collector::model::{FacilityInfo, LinkRef, RunMode};
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;
use sqlx::Row;

/// 构造“旧版本遗留的污染轮次”：
/// 1. 先在最新截止点跑一轮全量（缓存各修订、写满变更清单）；
/// 2. 把该轮次固化的截止点改回较早的 `{station-a:4, station-b:7}`。
/// 于是一个**声称**在早期截止点成功、结果却包含截止点之后登记设施
/// （a1 rev2、a4、b2 rev2）的历史轮次就复现出来了。
async fn polluted_setup() -> (Arc<Engine>, Store, MockWorld, tempfile::TempDir, i64) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("repair.db").display());
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = MockWorld::demo();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client));
    let engine = Engine::new(store.clone(), fetcher);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run(&engine, run_id).await.status, "succeeded");

    // 回拨截止点：轮次编号、创建时间不变，只改它声称的截止点。
    sqlx::query("UPDATE runs SET watermarks = ? WHERE id = ?")
        .bind(r#"{"station-a":4,"station-b":7}"#)
        .bind(run_id)
        .execute(&store.pool)
        .await
        .unwrap();

    (engine, store, world, dir, run_id)
}

fn keys(items: &[drainage_collector::model::ReachableFacility]) -> Vec<(String, String, i64)> {
    let mut v: Vec<_> = items
        .iter()
        .map(|f| (f.station_id.clone(), f.page_key.clone(), f.revision.unwrap()))
        .collect();
    v.sort();
    v
}

async fn gen_row_count(store: &Store, run_id: i64, generation: i64) -> i64 {
    sqlx::query("SELECT COUNT(*) AS c FROM reachable WHERE run_id = ? AND generation = ?")
        .bind(run_id)
        .bind(generation)
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get::<i64, _>("c")
        .unwrap()
}

#[tokio::test]
async fn repair_recomputes_at_original_cutoff_and_switches_once() {
    let (engine, store, _world, _dir, run_id) = polluted_setup().await;

    // 切前：已发布结果就是那份被污染的 8 项（含 a4、a1 rev2、b2 rev2）。
    let before = engine.reachable(run_id).await.unwrap();
    assert_eq!(before.len(), 8, "修复前应是被污染的旧结果");
    assert!(keys(&before).contains(&("station-a".into(), "a4".into(), 1)));
    assert!(keys(&before).contains(&("station-a".into(), "a1".into(), 2)));
    assert!(keys(&before).contains(&("station-b".into(), "b2".into(), 2)));
    let run_before = store.get_run(run_id).await.unwrap();
    assert_eq!(run_before.published_generation, 1);

    let out = engine.repair_run(run_id).await.unwrap();
    assert_eq!(out.generation, 2);
    assert_eq!(out.reachable, 7);
    assert!(!out.already_repaired);

    // 切后：只剩截止点内 7 项，a4 不再出现，a1/b2 回到 rev1。
    let after = engine.reachable(run_id).await.unwrap();
    let k = keys(&after);
    assert_eq!(k.len(), 7, "截止点内应恰好 7 个设施: {k:?}");
    assert!(!k.iter().any(|(_, p, _)| p == "a4"), "a4 登记于截止点后，不应可达");
    assert!(k.contains(&("station-a".into(), "a1".into(), 1)));
    assert!(k.contains(&("station-b".into(), "b2".into(), 1)));

    // 按入口查询同步切到新结果。
    let (erun, eitems) = engine.entry_reachable(1).await.unwrap().unwrap();
    assert_eq!(erun, run_id);
    assert_eq!(eitems.len(), 7);

    // 轮次编号 / 创建时间 / 状态 / 固化截止点均保持不变（历史顺序不受影响）。
    let run_after = store.get_run(run_id).await.unwrap();
    assert_eq!(run_after.id, run_before.id);
    assert_eq!(run_after.created_at, run_before.created_at);
    assert_eq!(run_after.mode.as_str(), run_before.mode.as_str());
    assert_eq!(run_after.status.as_str(), "succeeded");
    assert_eq!(run_after.watermarks, run_before.watermarks);
    assert_eq!(run_after.published_generation, 2);
    assert_eq!(store.list_runs(100).await.unwrap().len(), 1, "修复不应新增轮次");

    // 旧代次结果原样保留在库里（切换是“指针翻转”，不是删除重建）。
    assert_eq!(gen_row_count(&store, run_id, 1).await, 8);
    assert_eq!(gen_row_count(&store, run_id, 2).await, 7);

    // 幂等：再次修复是空操作，不新增写入、不再抓取。
    let again = engine.repair_run(run_id).await.unwrap();
    assert!(again.already_repaired);
    assert_eq!(again.generation, 2);
    assert_eq!(gen_row_count(&store, run_id, 1).await, 8);
    assert_eq!(gen_row_count(&store, run_id, 2).await, 7);
}

#[tokio::test]
async fn repair_fetches_only_missing_target_revisions() {
    // 全量轮次只缓存了最新修订（a1 rev2、b2 rev2），早期截止点的目标修订
    // （a1 rev1、b2 rev1）未缓存；其余 5 页命中缓存。修复应恰好只抓这 2 页。
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("repair2.db").display());
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = MockWorld::demo();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client.clone()));
    let engine = Engine::new(store.clone(), fetcher);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run(&engine, run_id).await.status, "succeeded");
    sqlx::query("UPDATE runs SET watermarks = ? WHERE id = ?")
        .bind(r#"{"station-a":4,"station-b":7}"#)
        .bind(run_id)
        .execute(&store.pool)
        .await
        .unwrap();

    let before = client.request_count();
    engine.repair_run(run_id).await.unwrap();
    let delta = client.request_count() - before;
    assert_eq!(delta, 2, "只应抓取缺失的 a1 rev1 与 b2 rev1，实际 {delta}");
}

#[tokio::test]
async fn failed_repair_keeps_published_results_then_resumes() {
    let (engine, store, world, _dir, run_id) = polluted_setup().await;

    // 修复前快照。
    let published_before = keys(&engine.reachable(run_id).await.unwrap());
    let wm_before = store.get_run(run_id).await.unwrap().watermarks;

    // station-a 连续失败：a1 rev1 抓取重试耗尽 -> 修复失败。
    world.with_station("station-a", |s| s.transient_failures = 5);

    let err = engine.repair_run(run_id).await.unwrap_err();
    assert!(err.to_string().contains("未切换"), "{err}");

    // 失败后：按轮次、按入口仍返回原已发布结果，指针未动。
    assert_eq!(keys(&engine.reachable(run_id).await.unwrap()), published_before);
    assert_eq!(engine.reachable(run_id).await.unwrap().len(), 8);
    assert_eq!(engine.entry_reachable(1).await.unwrap().unwrap().1.len(), 8);
    assert_eq!(store.get_run(run_id).await.unwrap().published_generation, 1);
    assert_eq!(store.get_run(run_id).await.unwrap().watermarks, wm_before);
    let st = engine.repair_status(run_id).await.unwrap().unwrap();
    assert_eq!(st.status, "failed");
    assert_eq!(st.published_generation, 1);
    assert!(st.done >= 1, "a0 应已核实完成，续修时无需重抓");

    // 模拟进程重启：换一个引擎复用同一持久库，从断点接着修。
    let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone()))));
    let engine2 = Engine::new(store.clone(), fetcher);
    world.with_station("station-a", |s| s.transient_failures = 0);

    let out = engine2.repair_run(run_id).await.unwrap();
    assert_eq!(out.generation, 2);
    assert_eq!(out.reachable, 7);
    assert_eq!(engine2.reachable(run_id).await.unwrap().len(), 7);
    assert_eq!(store.get_run(run_id).await.unwrap().published_generation, 2);
}

/// 构造可复现“误收截止点后设施”的场景：
/// - 先按最新截止点跑一轮全量，缓存里因此含有**截止点之后**才登记的 a9；
/// - 再手工建一个截止点更早的轮次：其可达页 a1（截止点内修订）已链接 a9，
///   但 a9 的登记发生在截止点之后。
/// 旧引擎在 a9 没有截止点内变更条目时会退回“最新缓存修订”，于是把 a9 误收；
/// 正确实现必须按截止点判定 a9 在此时不存在（Gone）。
async fn cutoff_before_new_route() -> (
    Arc<Engine>,
    Store,
    MockWorld,
    tempfile::TempDir,
    Arc<MockPageClient>,
) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("cutoff.db").display());
    let store = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();

    let mut a = MockStation::new("station-a");
    let l = |page: &str| LinkRef {
        station_id: "station-a".into(),
        page_key: page.into(),
    };
    let fac = |name: &str| FacilityInfo {
        name: Some(name.into()),
        kind: Some("inlet".into()),
    };
    // seq 1..3：截止点内，a0 -> a1 -> a2；a1 已链接尚未登记的 a9。
    a.put_version(1, "a0", 1, fac("入口"), vec![l("a1")], "registered");
    a.put_version(2, "a1", 1, fac("井1"), vec![l("a2"), l("a9")], "registered");
    a.put_version(3, "a2", 1, fac("井2"), vec![], "registered");
    // seq 4：a9 才登记（截止点之后）。
    a.put_version(4, "a9", 1, fac("新设施"), vec![], "registered");
    let world = MockWorld::new();
    world.add_station(a);
    world.add_station(MockStation::new("station-b"));
    let client = Arc::new(MockPageClient::new(world.clone()));
    let engine = Engine::new(store.clone(), Arc::new(Fetcher::new(client.clone())));

    // 轮次 1：最新截止点（4）全量，a9 合法可达并进入缓存。
    let warm = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run(&engine, warm).await.status, "succeeded");
    assert!(keys(&engine.reachable(warm).await.unwrap())
        .iter()
        .any(|(_, p, _)| p == "a9"));

    // 轮次 2：手工建轮次，截止点固定在 seq=3（a9 登记前），走公开续跑入口执行。
    let mut wm = std::collections::BTreeMap::new();
    wm.insert("station-a".to_string(), 3i64);
    wm.insert("station-b".to_string(), 0i64);
    let run_id = store.create_run(1, RunMode::Full, &wm).await.unwrap();
    store
        .enqueue_run_page(run_id, "station-a", "a0", 0)
        .await
        .unwrap();
    engine.resume_on_boot().await.unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if store.get_run(run_id).await.unwrap().status.as_str() != "running" {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    (engine, store, world, dir, client)
}

#[tokio::test]
async fn new_run_does_not_pull_facilities_registered_after_cutoff() {
    // 核心回归：截止点之后登记的设施绝不能进入该轮次的可达结果，
    // 即便 a9 的修订就在缓存里、且它被截止点内的 a1 链接到。
    let (engine, store, _world, _dir, _client) = cutoff_before_new_route().await;
    let run_id = 2;

    let items = engine.reachable(run_id).await.unwrap();
    let k = keys(&items);
    assert!(
        !k.iter().any(|(_, p, _)| p == "a9"),
        "a9 登记于截止点后，不应可达: {k:?}"
    );
    assert!(k.contains(&("station-a".into(), "a0".into(), 1)));
    assert_eq!(k.len(), 3, "截止点内应恰好 a0/a1/a2: {k:?}");
    assert_eq!(store.get_run(run_id).await.unwrap().status.as_str(), "succeeded");
}

#[tokio::test]
async fn repair_is_crash_safe_and_resumes_without_refetch() {
    // 修复中途“崩溃”（只跑到部分页面就返回），随后重启续修：
    // - 中断期间已发布结果不变；
    // - 续修不重抓已核实的修订（用请求计数证明）。
    let (engine, store, world, _dir, client) = cutoff_before_new_route().await;
    let run_id = 2;

    // 这轮已是正确结果（3 项）；为了制造“需要修复”的场景，回拨不再需要，
    // 直接验证修复自身在中断-重启下的行为即可。
    let published_before = keys(&engine.reachable(run_id).await.unwrap());

    // 第一次修复：人为注入 station-a 失败，使其在完成部分页面后失败。
    // a0/a1 已缓存，a2 需从缓存命中（截止点轮次已抓），因此改成先清掉
    // a2 的缓存逼它重抓并失败。
    sqlx::query("DELETE FROM page_revisions WHERE station_id='station-a' AND page_key='a2'")
        .execute(&store.pool)
        .await
        .unwrap();
    world.with_station("station-a", |s| s.transient_failures = 5);

    assert!(engine.repair_run(run_id).await.is_err());
    // 中断/失败后已发布结果原封不动。
    assert_eq!(keys(&engine.reachable(run_id).await.unwrap()), published_before);
    assert_eq!(store.get_run(run_id).await.unwrap().published_generation, 1);

    // 计数续修请求：a2 仍未缓存需重抓；a0/a1 不应再抓。
    world.with_station("station-a", |s| s.transient_failures = 0);
    let before = client.request_count();
    let out = engine.repair_run(run_id).await.unwrap();
    let delta = client.request_count() - before;
    assert_eq!(delta, 1, "续修只应重抓缺失的 a2，实际 {delta}");
    assert_eq!(out.generation, 2);
    assert_eq!(engine.reachable(run_id).await.unwrap().len(), 3);
}

#[tokio::test]
async fn legacy_v1_database_migrates_and_keeps_published_results() {
    // 造一个真正的“旧库”：只有 v1 结构（reachable 无 generation 列、
    // runs 无 published_generation 列、无修复/记忆表），其中已有历史结果。
    // 用不带迁移的裸连接写入，模拟升级前的库文件。
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    std::fs::File::create(&path).unwrap();
    let ddl = include_str!("../schema/schema.sql")
        .replace("__PK_AUTO__", "INTEGER PRIMARY KEY AUTOINCREMENT");
    {
        use sqlx::any::AnyPoolOptions;
        sqlx::any::install_default_drivers();
        let pool = AnyPoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap();
        for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        for stmt in [
            "CREATE TABLE schema_versions (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)",
            "INSERT INTO schema_versions (version, applied_at) VALUES (1, 0)",
            "INSERT INTO runs (entry_id, mode, status, watermarks, error, created_at, updated_at) \
               VALUES (1, 'full', 'succeeded', '{\"station-a\":4,\"station-b\":7}', NULL, 100, 100)",
            "INSERT INTO reachable (run_id, entry_id, station_id, page_key, revision, depth) \
               VALUES (1, 1, 'station-a', 'a0', 1, 0)",
            "INSERT INTO reachable (run_id, entry_id, station_id, page_key, revision, depth) \
               VALUES (1, 1, 'station-a', 'a4', 1, 1)",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        pool.close().await;
    }

    // 打开即触发 v2 迁移。
    let url = format!("sqlite://{}", path.display());
    let store = Store::connect(&url).await.unwrap();

    // 旧结果归入第 1 代并保持可见，published_generation 默认 1。
    let run = store.get_run(1).await.unwrap();
    assert_eq!(run.published_generation, 1);
    assert_eq!(run.created_at, 100);
    let items = store.list_reachable(1).await.unwrap();
    assert_eq!(items.len(), 2, "旧库历史结果必须保留");
    let ver: i64 = sqlx::query("SELECT version FROM schema_versions ORDER BY version DESC LIMIT 1")
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get("version")
        .unwrap();
    assert_eq!(ver, 2, "应升级到 schema v2");

    // 迁移后的旧库能立即用于修复：未修复时状态为空。
    let engine = Engine::new(
        store.clone(),
        Arc::new(Fetcher::new(Arc::new(MockPageClient::new(MockWorld::new())))),
    );
    assert!(engine.repair_status(1).await.unwrap().is_none());
}

#[tokio::test]
async fn boot_resumes_unfinished_repair_and_switches() {
    // 模拟“修复刚建好任务就崩溃”：库里留下 running 的修复记录与待处理队列，
    let (engine, store, world, _dir, run_id) = polluted_setup().await;
    drop(engine);

    // 手工造出崩溃现场：第 2 代修复任务在跑，入口页尚未处理。
    store.begin_repair(run_id, 2).await.unwrap();
    store
        .enqueue_repair_page(run_id, "station-a", "a0", 0)
        .await
        .unwrap();
    // 混入一个“上一进程”留下的 fetching 残留，验证会被收回。
    sqlx::query("UPDATE repair_pages SET state='fetching' WHERE run_id = ?")
        .bind(run_id)
        .execute(&store.pool)
        .await
        .unwrap();

    // 新进程：复用同一持久库。
    let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone()))));
    let engine2 = Engine::new(store.clone(), fetcher);
    engine2.resume_on_boot().await.unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let run = store.get_run(run_id).await.unwrap();
        if run.published_generation == 2 {
            if let Some(st) = engine2.repair_status(run_id).await.unwrap() {
                if st.status == "succeeded" {
                    break;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "重启后修复未完成：{:?}",
            engine2.repair_status(run_id).await.unwrap()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    assert_eq!(engine2.reachable(run_id).await.unwrap().len(), 7);
    assert_eq!(store.list_runs(100).await.unwrap().len(), 1);
}

#[tokio::test]
async fn resume_repairs_page_marked_done_but_missing_reachable_row() {
    // 模拟“done 已落库、可达行却没写成”的崩溃窗口：续跑必须补回该行，
    // 且不重抓（修订已缓存）。
    let (engine, store, world, _dir, run_id) = polluted_setup().await;

    // 先正常修复一轮，得到确定结果。
    engine.repair_run(run_id).await.unwrap();
    let baseline = keys(&engine.reachable(run_id).await.unwrap());
    assert_eq!(baseline.len(), 7);

    // 人为删掉 a2 的可达行，制造“done 但漏写可达”的现场，并造一个待续修任务。
    let gen = store.get_run(run_id).await.unwrap().published_generation;
    sqlx::query("DELETE FROM reachable WHERE run_id = ? AND generation = ? AND station_id='station-a' AND page_key='a2'")
        .bind(run_id)
        .bind(gen)
        .execute(&store.pool)
        .await
        .unwrap();
    // 强制再修一轮：把发布代次保持，但把修复记录重置为 running 的新代次。
    let next_gen = gen + 1;
    store.begin_repair(run_id, next_gen).await.unwrap();

    let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone()))));
    let engine2 = Engine::new(store.clone(), fetcher);
    engine2.resume_on_boot().await.unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if store.get_run(run_id).await.unwrap().published_generation == next_gen {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let after = keys(&engine2.reachable(run_id).await.unwrap());
    assert_eq!(after, baseline, "续修应补回漏写的可达行，结果与基线一致");
}

#[tokio::test]
async fn publish_switch_is_one_row_regardless_of_facility_count() {
    // 切换写入量必须与设施数无关：对比“可达 7 项”与“可达更多项”两种库，
    // 执行发布语句后 SQLite 的 total_changes 增量恒为 1。
    async fn total_changes(store: &Store) -> i64 {
        sqlx::query("SELECT total_changes() AS c")
            .fetch_one(&store.pool)
            .await
            .unwrap()
            .try_get::<i64, _>("c")
            .unwrap()
    }

    // 小库：7 项可达。
    let (engine_small, store_small, _w1, _d1, small_run) = polluted_setup().await;
    // 发布语句只用一条 UPDATE；不需要真跑完修复就能度量语句本身的写入量。
    let before = total_changes(&store_small).await;
    store_small.publish_generation(small_run, 2).await.unwrap();
    let small_delta = total_changes(&store_small).await - before;
    drop(engine_small);

    // 大库：同一切换语句，作用在更多可达行上，写入量仍应为 1。
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("big.db").display());
    let store_big = Store::connect(&url).await.unwrap();
    seed_demo_stations(&store_big).await.unwrap();
    let mut wm = std::collections::BTreeMap::new();
    wm.insert("station-a".to_string(), 0i64);
    let big_run = store_big
        .create_run(1, RunMode::Full, &wm)
        .await
        .unwrap();
    // 灌入 500 行可达关系（远超小库的 7 行）。
    let mut items = Vec::new();
    for i in 0..500 {
        items.push((
            LinkRef {
                station_id: "station-a".into(),
                page_key: format!("p{i}"),
            },
            1i64,
            i as i64,
        ));
    }
    store_big
        .replace_generation_reachable(big_run, 2, 1, &items)
        .await
        .unwrap();
    let before_big = total_changes(&store_big).await;
    store_big.publish_generation(big_run, 2).await.unwrap();
    let big_delta = total_changes(&store_big).await - before_big;

    assert_eq!(small_delta, 1, "小库切换应只写 1 行");
    assert_eq!(big_delta, 1, "大库切换也应只写 1 行，与设施数无关");
    assert_eq!(store_big.count_reachable(big_run).await.unwrap(), 500);
}

