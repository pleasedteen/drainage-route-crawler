//! 全量 vs 增量在相同截止点下的可达关系一致性（核心验收点）。
mod common;

use common::{harness, wait_run};
use drainage_collector::model::RunMode;

#[tokio::test]
async fn full_and_incremental_reach_same_facilities_at_same_cutoff() {
    let (engine, _store, _world, _dir) = harness().await;
    let entry_id = 1;

    // 第一轮：全量。
    let full_id = engine.start_run(entry_id, RunMode::Full).await.unwrap();
    let p = wait_run(&engine, full_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);

    // 第二轮：增量。站点数据未再变化 -> 截止点相同。
    let inc_id = engine
        .start_run(entry_id, RunMode::Incremental)
        .await
        .unwrap();
    let p2 = wait_run(&engine, inc_id).await;
    assert_eq!(p2.status, "succeeded", "{:?}", p2.error);

    // 固化的截止点必须逐站相同。
    let r1 = engine.store.get_run(full_id).await.unwrap();
    let r2 = engine.store.get_run(inc_id).await.unwrap();
    assert_eq!(r1.watermarks, r2.watermarks);

    let a = engine
        .reachable_keys_for_equivalence(full_id)
        .await
        .unwrap();
    let b = engine.reachable_keys_for_equivalence(inc_id).await.unwrap();
    assert_eq!(a, b, "同截止点下全量/增量可达关系必须一致");

    // 演示图包含跨站路线（station-b）与汇合/回流成环，且每个页面都可达。
    let keys: std::collections::BTreeSet<_> =
        a.iter().map(|(s, p, _)| (s.as_str(), p.as_str())).collect();
    for expected in [
        ("station-a", "a0"),
        ("station-a", "a1"),
        ("station-a", "a2"),
        ("station-a", "a3"),
        ("station-a", "a4"),
        ("station-b", "b1"),
        ("station-b", "b2"),
        ("station-b", "b3"),
    ] {
        assert!(keys.contains(&expected), "缺少可达设施 {expected:?}");
    }
}

#[tokio::test]
async fn cutoff_pins_earlier_graph_for_full_run() {
    // 在“旧截止点”采集后，即便站点继续登记，旧轮次结果保持不变。
    let (engine, store, world, _dir) = harness().await;
    let entry_id = 1;

    let run1 = engine.start_run(entry_id, RunMode::Full).await.unwrap();
    let p1 = wait_run(&engine, run1).await;
    assert_eq!(p1.status, "succeeded");
    let before = engine.reachable_keys_for_equivalence(run1).await.unwrap();
    let cutoff_before = store.get_run(run1).await.unwrap().watermarks;

    // 站点再登记一轮新修订（a0 增加一条链接）。
    world.with_station("station-a", |s| {
        let seq = s.latest_seq + 1;
        s.put_version(
            seq,
            "a0",
            2,
            drainage_collector::model::FacilityInfo {
                name: Some("A站入流口(改)".into()),
                kind: Some("inlet".into()),
            },
            vec![
                drainage_collector::model::LinkRef {
                    station_id: "station-a".into(),
                    page_key: "a1".into(),
                },
                drainage_collector::model::LinkRef {
                    station_id: "station-b".into(),
                    page_key: "b1".into(),
                },
            ],
            "revised",
        );
    });

    // 旧轮次查询结果不受后续登记影响。
    assert_eq!(
        engine.reachable_keys_for_equivalence(run1).await.unwrap(),
        before
    );
    assert_eq!(store.get_run(run1).await.unwrap().watermarks, cutoff_before);

    // 新轮次截止点推进，且拿到新修订（a0 revision=2）。
    let run2 = engine.start_run(entry_id, RunMode::Full).await.unwrap();
    let p2 = wait_run(&engine, run2).await;
    assert_eq!(p2.status, "succeeded");
    let after = engine.reachable_keys_for_equivalence(run2).await.unwrap();
    assert!(after.contains(&("station-a".to_string(), "a0".to_string(), 2)));
    let wm2 = store.get_run(run2).await.unwrap().watermarks;
    assert!(wm2["station-a"] > cutoff_before["station-a"]);
}
