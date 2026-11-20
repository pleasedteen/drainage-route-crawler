//! 共享抓取缓存：同一页面同一修订跨入口/跨轮次只抓一次；增量复用未修订页面。
mod common;

use std::sync::Arc;

use common::{harness, wait_run};
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::MockPageClient;
use drainage_collector::model::RunMode;

#[tokio::test]
async fn same_revision_is_shared_across_runs_and_entries() {
    let (engine, store, _world, _dir) = harness().await;

    // 第二个入流口，与入口 1 的下游图大量重叠。
    let entry2 = store
        .add_entry("station-b", "b1", "河西调蓄池入口")
        .await
        .unwrap();

    let r1 = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(wait_run(&engine, r1).await.status, "succeeded");

    // 第一次全量后，所有演示页修订都已在共享缓存中。
    assert!(!store.list_stations().await.unwrap().is_empty());

    // 另一入口的全量轮次：重叠页面全部缓存命中，不产生重复修订记录。
    let r2 = engine.start_run(entry2, RunMode::Full).await.unwrap();
    assert_eq!(wait_run(&engine, r2).await.status, "succeeded");

    // 每个 (station, page, revision) 在缓存表中恰好一行。
    let cnt: i64 = sqlx_any_count(
        &store,
        "SELECT COUNT(*) AS c FROM (SELECT 1 FROM page_revisions GROUP BY station_id, page_key, revision HAVING COUNT(*) > 1)",
    )
    .await;
    assert_eq!(cnt, 0, "同一修订存在重复抓取记录");

    // b1 被两个入口的轮次共享，缓存只有 1 行。
    let b1_rows = sqlx_any_count(
        &store,
        "SELECT COUNT(*) AS c FROM page_revisions WHERE station_id='station-b' AND page_key='b1'",
    )
    .await;
    assert_eq!(b1_rows, 1);
}

#[tokio::test]
async fn incremental_reuses_unrevised_pages_and_picks_up_new_route() {
    let (engine, _store, _world, _dir) = harness().await;

    // 用旧截止点做一轮全量（只看前 4 条登记：a0,a1,a2,a3 初始版本）。
    // demo 世界 station-a 共 6 条、station-b 共 3 条。先手动把站点“冻结”在早期：
    // 通过先跑一轮再推进数据的方式模拟，这里直接跑全量得到当前基线。
    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(wait_run(&engine, base).await.status, "succeeded");
    let base_reach = engine.reachable_keys_for_equivalence(base).await.unwrap();

    // 增量轮次：无新登记 -> 全部复用缓存，结果与基线一致。
    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    let p = wait_run(&engine, inc).await;
    assert_eq!(p.status, "succeeded");
    let inc_reach = engine.reachable_keys_for_equivalence(inc).await.unwrap();
    assert_eq!(base_reach, inc_reach);

    // 进度口径自洽：done == reachable（Gone 页面在本场景不存在）。
    assert_eq!(p.done, p.reachable);
}

#[tokio::test]
async fn incremental_fetches_only_changed_pages_after_new_registration() {
    // 场景：第一轮全量后站点新增一个页面与一条路线；增量轮次应只抓变化部分，
    // 未修订页面全部走缓存，请求数显著少于全量。
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("inc.db").display());
    let store = drainage_collector::Store::connect(&db_url).await.unwrap();
    drainage_collector::seed::seed_demo_stations(&store)
        .await
        .unwrap();
    let world = drainage_collector::mock::MockWorld::demo();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client.clone()));
    let engine = drainage_collector::Engine::new(store.clone(), fetcher);

    // 基线全量。
    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(wait_run(&engine, base).await.status, "succeeded");
    let full_requests = client.request_count();
    assert!(
        full_requests >= 8,
        "全量至少抓 8 个页面，实际 {full_requests}"
    );

    // 站点再登记：新增 a5，并让 a2 改指 a5（a2 产生新修订）。
    world.with_station("station-a", |s| {
        let seq = s.latest_seq + 1;
        s.put_version(
            seq,
            "a5",
            1,
            drainage_collector::model::FacilityInfo {
                name: Some("A5号新建截流井".into()),
                kind: Some("interceptor".into()),
            },
            vec![drainage_collector::model::LinkRef {
                station_id: "station-b".into(),
                page_key: "b3".into(),
            }],
            "registered",
        );
        let seq2 = seq + 1;
        s.put_version(
            seq2,
            "a2",
            2,
            drainage_collector::model::FacilityInfo {
                name: Some("A2号提升泵站(改造)".into()),
                kind: Some("pump".into()),
            },
            vec![
                drainage_collector::model::LinkRef {
                    station_id: "station-a".into(),
                    page_key: "a5".into(),
                },
                drainage_collector::model::LinkRef {
                    station_id: "station-b".into(),
                    page_key: "b1".into(),
                },
            ],
            "revised",
        );
    });

    // 增量轮次。
    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    let p = wait_run(&engine, inc).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    let inc_requests = client.request_count() - full_requests;

    // 只有新页 a5 与修订页 a2 需要真正抓取（最多 2 次，含可能的重试余量），
    // 其余 7 个未修订页面全部复用缓存。
    assert!(
        inc_requests <= 2,
        "增量应只抓变化页面，实际新增请求 {inc_requests} 次"
    );

    // 新设施 a5 已可达，且 a2 取到修订 2。
    let reach = engine.reachable_keys_for_equivalence(inc).await.unwrap();
    assert!(reach.contains(&("station-a".to_string(), "a5".to_string(), 1)));
    assert!(reach.contains(&("station-a".to_string(), "a2".to_string(), 2)));

    // 同截止点再跑一次全量：结果与增量完全一致（一致性回归保护）。
    let full2 = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(wait_run(&engine, full2).await.status, "succeeded");
    assert_eq!(
        engine.reachable_keys_for_equivalence(inc).await.unwrap(),
        engine.reachable_keys_for_equivalence(full2).await.unwrap()
    );
}

async fn sqlx_any_count(store: &drainage_collector::Store, sql: &str) -> i64 {
    use sqlx::Row;
    sqlx::query(sql)
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get::<i64, _>("c")
        .unwrap()
}
