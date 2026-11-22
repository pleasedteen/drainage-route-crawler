//! 缓存来源证明与旧库迁移：
//! 旧库（schema v1）的缓存没有来源证明，后续轮次首次用到必须重新核实；
//! 核实后的修订跨入口、跨重启复用；历史轮次仍能原样查询。
//!
//! 夹具：手工建一座 v1 结构的 SQLite 旧库（缓存行没有来源证明列），
//! 数据与本地模拟站当前登记一致，另含一个已完成的历史轮次。
mod common;

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

use common::wait_run;
use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{render, MockPageClient, MockWorld};
use drainage_collector::model::{RunMode, SiteFormat, StationConfig};
use drainage_collector::store::Store;
use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use sqlx::Row;

/// v1 基线 schema（保持无来源证明列；v2 迁移由 Store::connect 完成）。
const V1_SCHEMA: &str = include_str!("../schema/schema.sql");

/// 与 seed_demo_stations 相同的两站配置（夹具直接写 stations 表）。
fn demo_station_configs() -> Vec<StationConfig> {
    vec![
        StationConfig {
            id: "station-a".into(),
            name: "城东养护站".into(),
            base_url: "mock://station-a".into(),
            format: SiteFormat::Html,
            min_interval_ms: 5,
            max_inflight: 2,
            watermark_seq: 0,
        },
        StationConfig {
            id: "station-b".into(),
            name: "河西养护站".into(),
            base_url: "mock://station-b".into(),
            format: SiteFormat::Json,
            min_interval_ms: 5,
            max_inflight: 2,
            watermark_seq: 0,
        },
    ]
}

/// 从模拟站当前版本算 BFS 最小深度，得到 (station, page, revision, depth) 可达集。
fn bfs_reachable(
    world: &MockWorld,
    start_station: &str,
    start_page: &str,
) -> Vec<(String, String, i64, i64)> {
    let mut depth_of: HashMap<(String, String), i64> = HashMap::new();
    let mut queue = VecDeque::from([(start_station.to_string(), start_page.to_string(), 0i64)]);
    while let Some((sid, key, depth)) = queue.pop_front() {
        if depth_of.contains_key(&(sid.clone(), key.clone())) {
            continue;
        }
        depth_of.insert((sid.clone(), key.clone()), depth);
        let st = world.get_station(&sid).unwrap();
        let v = st.pages.get(&key).unwrap().last().unwrap();
        for l in &v.links {
            queue.push_back((l.station_id.clone(), l.page_key.clone(), depth + 1));
        }
    }
    depth_of
        .into_iter()
        .map(|((sid, key), depth)| {
            let st = world.get_station(&sid).unwrap();
            let rev = st.pages.get(&key).unwrap().last().unwrap().revision;
            (sid, key, rev, depth)
        })
        .collect()
}

/// 在指定 SQLite 文件里手工建一座“旧库”：v1 表结构 + 旧系统数据。
/// 页面缓存、设施、链接、变更清单、历史轮次 run 1 及其可达关系，
/// 全部按 v1 列写入（page_revisions 没有来源证明列）。
async fn build_legacy_fixture(db_url: &str, world: &MockWorld) {
    drainage_collector::store::install_drivers();
    // sqlx Any 的 SQLite 不会自动建文件，先预建。
    let path = db_url.trim_start_matches("sqlite://");
    std::fs::File::create(path).unwrap();
    let opts = AnyConnectOptions::from_str(db_url).unwrap();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect_lazy_with(opts);

    let ddl = V1_SCHEMA.replace("__PK_AUTO__", "INTEGER PRIMARY KEY AUTOINCREMENT");
    for stmt in ddl.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(stmt).execute(&pool).await.unwrap();
    }
    // schema_versions 由连接初始化维护（不在 schema.sql 内），旧库同样从 v1 起步。
    sqlx::query("CREATE TABLE IF NOT EXISTS schema_versions (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO schema_versions (version, applied_at) VALUES (1, 1000)")
        .execute(&pool)
        .await
        .unwrap();

    // 站点与入流口（v1 表结构与新版本一致）。
    let cfgs = demo_station_configs();
    for st in &cfgs {
        sqlx::query(
            "INSERT INTO stations (id, name, base_url, format, min_interval_ms, max_inflight, watermark_seq) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&st.id)
        .bind(&st.name)
        .bind(&st.base_url)
        .bind(st.format.as_str())
        .bind(st.min_interval_ms as i64)
        .bind(st.max_inflight as i64)
        .bind(st.watermark_seq)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("INSERT INTO entry_points (id, station_id, page_key, label) VALUES (1, 'station-a', 'a0', '城东入流口')")
        .execute(&pool)
        .await
        .unwrap();

    // 变更清单：与模拟站当前登记一致（旧系统已续读到截止点）。
    for sid in ["station-a", "station-b"] {
        let st = world.get_station(sid).unwrap();
        for e in &st.change_log {
            sqlx::query(
                "INSERT INTO change_log (seq, station_id, page_key, revision, kind) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(e.seq)
            .bind(&e.station_id)
            .bind(&e.page_key)
            .bind(e.revision)
            .bind(&e.kind)
            .execute(&pool)
            .await
            .unwrap();
        }
    }

    // 页面缓存：旧系统抓取的当前修订（v1 无来源证明列），连同设施与链接。
    for sid in ["station-a", "station-b"] {
        let st = world.get_station(sid).unwrap();
        let cfg = cfgs.iter().find(|c| c.id == sid).unwrap();
        for (page_key, versions) in &st.pages {
            let v = versions.last().unwrap();
            let (ct, body) = render(cfg, page_key, v);
            sqlx::query(
                "INSERT INTO page_revisions (station_id, page_key, revision, content_type, body, fetched_at) \
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(sid)
            .bind(page_key)
            .bind(v.revision)
            .bind(&ct)
            .bind(&body)
            .bind(1_600_000_000_000i64)
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO facilities (station_id, page_key, revision, name, kind) \
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(sid)
            .bind(page_key)
            .bind(v.revision)
            .bind(&v.facility.name)
            .bind(&v.facility.kind)
            .execute(&pool)
            .await
            .unwrap();
            for (i, l) in v.links.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO links (from_station_id, from_page_key, from_revision, position, to_station_id, to_page_key) \
                     VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(sid)
                .bind(page_key)
                .bind(v.revision)
                .bind(i as i64)
                .bind(&l.station_id)
                .bind(&l.page_key)
                .execute(&pool)
                .await
                .unwrap();
            }
        }
    }

    // 历史轮次 run 1（succeeded）及其已核实可达关系。
    let watermarks = serde_json::json!({
        "station-a": world.get_station("station-a").unwrap().latest_seq,
        "station-b": world.get_station("station-b").unwrap().latest_seq,
    })
    .to_string();
    sqlx::query(
        "INSERT INTO runs (id, entry_id, mode, status, watermarks, error, created_at, updated_at) \
         VALUES (1, 1, 'full', 'succeeded', ?, NULL, 1000, 1000)",
    )
    .bind(&watermarks)
    .execute(&pool)
    .await
    .unwrap();
    for (sid, key, rev, depth) in bfs_reachable(world, "station-a", "a0") {
        sqlx::query(
            "INSERT INTO reachable (run_id, entry_id, station_id, page_key, revision, depth) \
             VALUES (1, 1, ?, ?, ?, ?)",
        )
        .bind(sid)
        .bind(key)
        .bind(rev)
        .bind(depth)
        .execute(&pool)
        .await
        .unwrap();
    }
    pool.close().await;
}

#[tokio::test]
async fn legacy_cache_is_reverified_once_then_reused_across_entries_and_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("legacy.db").display());
    let world = MockWorld::demo();
    build_legacy_fixture(&db_url, &world).await;

    // 新代码打开旧库：自动迁移到 v2；旧缓存行来源证明为 NULL。
    let store = Store::connect(&db_url).await.unwrap();
    let schema_v: i64 = sqlx::query("SELECT MAX(version) AS v FROM schema_versions")
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get("v")
        .unwrap();
    assert_eq!(schema_v, 2, "旧库应自动迁移到 v2");
    let legacy_rows: i64 = sqlx::query(
        "SELECT COUNT(*) AS c FROM page_revisions WHERE verified_run_id IS NULL OR source_url IS NULL",
    )
    .fetch_one(&store.pool)
    .await
    .unwrap()
    .try_get("c")
    .unwrap();
    assert_eq!(legacy_rows, 8, "旧库 8 条缓存全部缺少来源证明");

    let client = Arc::new(MockPageClient::new(world.clone()));
    let engine = Engine::new(store.clone(), Arc::new(Fetcher::new(client.clone())));

    // 历史轮次（run 1）迁移后原样可查。
    let hist_before = engine.reachable(1).await.unwrap();
    assert_eq!(hist_before.len(), 8);

    // 后续轮次首次用到旧缓存：8 页逐页重新核实（每页恰好一次请求）。
    let run2 = engine.start_run(1, RunMode::Full).await.unwrap();
    let p2 = wait_run(&engine, run2).await;
    assert_eq!(p2.status, "succeeded", "{:?}", p2.error);
    assert_eq!(
        client.request_count(),
        8,
        "旧缓存缺少来源证明，首次用到须逐页重新核实"
    );
    // 核实结果与历史轮次同图（同截止点、同修订）。
    assert_eq!(
        engine.reachable_keys_for_equivalence(1).await.unwrap(),
        engine.reachable_keys_for_equivalence(run2).await.unwrap(),
    );
    // 来源证明已写回：8 条缓存的核实轮次都是 run2。
    let verified: i64 =
        sqlx::query("SELECT COUNT(*) AS c FROM page_revisions WHERE verified_run_id = ?")
            .bind(run2)
            .fetch_one(&store.pool)
            .await
            .unwrap()
            .try_get("c")
            .unwrap();
    assert_eq!(verified, 8);
    let missing_proof: i64 =
        sqlx::query("SELECT COUNT(*) AS c FROM page_revisions WHERE verified_run_id IS NULL")
            .fetch_one(&store.pool)
            .await
            .unwrap()
            .try_get("c")
            .unwrap();
    assert_eq!(missing_proof, 0, "核实后来源证明应全部落库");

    // 跨入口复用：另一入口的全量轮次零请求。
    let entry2 = store
        .add_entry("station-b", "b1", "河西调蓄池入口")
        .await
        .unwrap();
    let run3 = engine.start_run(entry2, RunMode::Full).await.unwrap();
    assert_eq!(wait_run(&engine, run3).await.status, "succeeded");
    assert_eq!(client.request_count(), 8, "核实后的修订跨入口零请求复用");

    // 跨重启复用：全新 Store/Engine/Fetcher（模拟进程重启）再跑增量，零请求。
    drop(engine);
    drop(store);
    let store2 = Store::connect(&db_url).await.unwrap();
    let client2 = Arc::new(MockPageClient::new(world.clone()));
    let engine2 = Engine::new(store2.clone(), Arc::new(Fetcher::new(client2.clone())));
    let run4 = engine2.start_run(1, RunMode::Incremental).await.unwrap();
    assert_eq!(wait_run(&engine2, run4).await.status, "succeeded");
    assert_eq!(client2.request_count(), 0, "核实后的修订跨重启零请求复用");

    // 历史轮次仍能原样查询（逐字段一致，未被后续轮次改变）。
    let hist_after = engine2.reachable(1).await.unwrap();
    assert_eq!(
        serde_json::to_string(&hist_before).unwrap(),
        serde_json::to_string(&hist_after).unwrap(),
        "历史轮次的可达关系不应被后续轮次改变"
    );
}
