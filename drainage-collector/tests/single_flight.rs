//! 跨进程在途单飞与接手：常驻服务与独立 CLI 共用采集库时，
//! 同一 (站点, 页面, 修订) 冷缓存只发一次实际请求；持租者退出后等待者自动接手，
//! 已确认结果继续共享；汇合与成环的进度仍按轮次独立维护。
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::two_engines_shared_store;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::MockPageClient;
use drainage_collector::model::RunMode;
use drainage_collector::store::Store;
use drainage_collector::Engine;

/// 演示图 8 个页面的唯一键。
const ALL_PAGES: [(&str, &str); 8] = [
    ("station-a", "a0"),
    ("station-a", "a1"),
    ("station-a", "a2"),
    ("station-a", "a3"),
    ("station-a", "a4"),
    ("station-b", "b1"),
    ("station-b", "b2"),
    ("station-b", "b3"),
];

/// B 站入口（b1）：在当前截止点 b2 已拆除回流 a1 的链接，只可达 {b1,b2,b3}。
async fn b1_entry_id(store: &Store) -> i64 {
    store
        .add_entry("station-b", "b1", "河西调蓄池入口")
        .await
        .unwrap()
}

/// A 站 a1 入口：可达 a1 及其下游共 7 页（不含 a0），且同样需要 a1 修订。
async fn a1_entry_id(store: &Store) -> i64 {
    store
        .add_entry("station-a", "a1", "A1 检查井入口")
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_cache_concurrent_runs_fetch_each_target_once_even_when_slow() {
    // 两个独立引擎（两个“进程”）共用一个采集库与模拟站，从不同入口同时冷启动。
    let (e1, e2, store, world, _dir) = two_engines_shared_store(15_000, 5_000).await;
    let entry_b = b1_entry_id(&store).await;

    // 让响应明显变慢：在途窗口足够长，保证两个引擎会同时撞上同一批冷页面。
    for (sid, page) in ALL_PAGES {
        world.set_page_delay_ms(sid, page, 120);
    }

    let r1 = e1.start_run(1, RunMode::Full).await.unwrap();
    let r2 = e2.start_run(entry_b, RunMode::Full).await.unwrap();

    let p1 = common::wait_run_id(&e1, r1).await;
    let p2 = common::wait_run_id(&e2, r2).await;
    assert_eq!(p1.status, "succeeded", "{:?}", p1.error);
    assert_eq!(p2.status, "succeeded", "{:?}", p2.error);

    // 关键验收：同一 (站点, 页面, 修订) 即使响应很慢，也只发起一次实际请求。
    for (sid, page) in ALL_PAGES {
        let hits = world.page_hits(sid, page);
        assert_eq!(hits, 1, "页面 {sid}/{page} 实际被请求 {hits} 次（应为 1）");
    }
    // 演示图 8 个页面各一次，总请求恰好 8（两个引擎合计），证明等待者没有重复抓取。
    assert_eq!(world.total_page_requests(), 8);

    // 两个轮次各自独立维护进度：a0 入口可达全图 8 页；b1 入口只可达 B 站 3 页。
    assert_eq!(p1.done, 8);
    assert_eq!(p2.done, 3);
    assert_eq!(p1.reachable, 8);
    assert_eq!(p2.reachable, 3);

    // 租约终态全部为 done，且共享缓存每个修订只有一行。
    let dup: i64 = count_sql(
        &store,
        "SELECT COUNT(*) AS c FROM (SELECT 1 FROM page_revisions \
         GROUP BY station_id, page_key, revision HAVING COUNT(*) > 1)",
    )
    .await;
    assert_eq!(dup, 0);
    let open_leases: i64 = count_sql(
        &store,
        "SELECT COUNT(*) AS c FROM fetch_leases WHERE state='fetching'",
    )
    .await;
    assert_eq!(open_leases, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_lease_is_atomically_taken_over_and_completed() {
    // 进程内可确定性验证的部分：持租者“死亡”（心跳停止）后租约过期，
    // 等待者通过条件 UPSERT 原子换主接手。真实“抓取方被 kill 后接手完成”的
    // 端到端链路由 scripts/demo_single_flight.sh 用两个真实进程验证。
    let (e1, e2, store, world, _dir) = two_engines_shared_store(500, 150).await;
    let entry_a1 = a1_entry_id(&store).await;

    // 用普通延迟（不使用跨客户端共享的阻塞闸门）让 e1 自然完成首页后继续推进，
    // 同时保留足够窗口观察租约的 fetching → done 生命周期。
    world.set_page_delay_ms("station-a", "a1", 30);

    // e1 启动，进入正常采集。
    let r1 = e1.start_run(1, RunMode::Full).await.unwrap();
    wait_until(|| matches_lease(&store, "station-a", "a1", "fetching")).await;
    let owner_holder = lease_owner(&store, "station-a", "a1").await;
    assert!(owner_holder.starts_with("pid-"));

    // e2 从 a1 入口冷启动：其首页租约被 e1 持有，先等待。
    let r2 = e2.start_run(entry_a1, RunMode::Full).await.unwrap();
    wait_until(|| async {
        store
            .get_run(r2)
            .await
            .map(|r| r.status.as_str() == "running")
            .unwrap_or(false)
    })
    .await;

    // e1 正常完成：租约置 done，e2 直接共享终态，两个轮次都成功且各只抓一次。
    let p1 = common::wait_run_id(&e1, r1).await;
    let p2 = common::wait_run_id(&e2, r2).await;
    assert_eq!(p1.status, "succeeded", "{:?}", p1.error);
    assert_eq!(p2.status, "succeeded", "{:?}", p2.error);
    assert_eq!(lease_state(&store, "station-a", "a1").await, "done");
    for (sid, page) in ALL_PAGES {
        assert_eq!(world.page_hits(sid, page), 1, "{sid}/{page} 应只抓一次");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_holders_lease_can_be_taken_over_via_store_api() {
    // 直接在持久层验证“过期在途租约被原子接手”的核心 SQL 语义，
    // 不依赖时序：手工写入一条已过期的 fetching 租约，模拟死掉的持租进程。
    let (e1, e2, store, _world, _dir) = two_engines_shared_store(500, 150).await;

    let now = drainage_collector::model::now_millis();
    insert_lease(
        &store,
        "station-a",
        "a1",
        "revision",
        2,
        "pid-dead-eng-0",
        "fetching",
        None,
        now - 10_000, // 早已过期
    )
    .await;

    // 另一个 owner 抢租：过期在途租约被原子接手，state 回到 fetching、owner 换成 e2。
    use drainage_collector::store::LeaseAcquire;
    let acquired = store
        .acquire_fetch_lease("station-a", "a1", "revision", 2, &e2.owner_id_str(), 500)
        .await
        .unwrap();
    assert!(
        matches!(acquired, LeaseAcquire::Held(_)),
        "过期租约应被接手持有"
    );
    assert_eq!(
        lease_owner(&store, "station-a", "a1").await,
        e2.owner_id_str()
    );
    assert_eq!(lease_state(&store, "station-a", "a1").await, "fetching");

    // 未过期的在途租约不能被抢：只能 Waiting。
    insert_lease(
        &store,
        "station-b",
        "b1",
        "revision",
        1,
        "pid-alive-eng-9",
        "fetching",
        None,
        now + 60_000,
    )
    .await;
    let acquired2 = store
        .acquire_fetch_lease("station-b", "b1", "revision", 1, &e1.owner_id_str(), 500)
        .await
        .unwrap();
    assert!(
        matches!(acquired2, LeaseAcquire::Waiting(_)),
        "未过期租约只能等待"
    );
    assert_eq!(
        lease_owner(&store, "station-b", "b1").await,
        "pid-alive-eng-9"
    );

    // 终态租约（done/gone）永远被共享，不会被任何 owner 覆盖。
    insert_lease(
        &store,
        "station-b",
        "b2",
        "revision",
        2,
        "pid-done-eng-3",
        "done",
        Some(2),
        now,
    )
    .await;
    let acquired3 = store
        .acquire_fetch_lease("station-b", "b2", "revision", 2, &e1.owner_id_str(), 500)
        .await
        .unwrap();
    assert!(matches!(acquired3, LeaseAcquire::Completed(_)));
    assert_eq!(
        lease_owner(&store, "station-b", "b2").await,
        "pid-done-eng-3"
    );
    assert_eq!(lease_state(&store, "station-b", "b2").await, "done");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_one_waiter_takes_over_an_expired_lease() {
    // 多个等待者同时抢一条已过期在途租约：条件 UPSERT 必须保证只有一个成为
    // 持租者，其余全部 Waiting，杜绝“持租者死亡后的惊群重复抓取”。
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("herd.db").display());
    let store = Store::connect(&url).await.unwrap();
    drainage_collector::seed::seed_demo_stations(&store)
        .await
        .unwrap();
    let world = drainage_collector::mock::MockWorld::demo();

    // 三个独立引擎 = 三个等待者。
    let mut engines = Vec::new();
    for _ in 0..3 {
        let s = Store::connect(&url).await.unwrap();
        let client = Arc::new(MockPageClient::new(world.clone()));
        engines.push(Engine::with_lease_timing(
            s,
            Arc::new(Fetcher::new(client)),
            500,
            150,
        ));
    }

    let now = drainage_collector::model::now_millis();
    insert_lease(
        &store,
        "station-a",
        "a3",
        "revision",
        1,
        "pid-dead-eng-x",
        "fetching",
        None,
        now - 5_000,
    )
    .await;

    use drainage_collector::store::LeaseAcquire;
    let mut held = 0;
    let mut waiting = 0;
    for eng in &engines {
        match store
            .acquire_fetch_lease("station-a", "a3", "revision", 1, &eng.owner_id_str(), 500)
            .await
            .unwrap()
        {
            LeaseAcquire::Held(_) => held += 1,
            LeaseAcquire::Waiting(_) => waiting += 1,
            LeaseAcquire::Completed(_) => panic!("过期在途租约不应直接返回终态"),
        }
    }
    assert_eq!(held, 1, "过期租约只能被一个等待者接手持有");
    assert_eq!(waiting, 2, "其余等待者必须继续等待，不得重复抓取");

    // 租约最终只有一行，owner 是三个引擎中的某一个。
    let owner = lease_owner(&store, "station-a", "a3").await;
    assert!(engines.iter().any(|e| e.owner_id_str() == owner));
    assert_eq!(lease_state(&store, "station-a", "a3").await, "fetching");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn confirmed_results_are_shared_and_other_pages_keep_moving() {
    // e1 先完整跑一轮，所有修订确认落库并把租约置为 done。
    let (e1, e2, store, world, _dir) = two_engines_shared_store(15_000, 5_000).await;
    let entry_b = b1_entry_id(&store).await;

    let r1 = e1.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run_id(&e1, r1).await.status, "succeeded");
    let baseline = world.total_page_requests();
    assert_eq!(baseline, 8);

    // e2（另一进程）从不同入口再跑：全部命中已确认结果，不发任何新请求。
    let r2 = e2.start_run(entry_b, RunMode::Full).await.unwrap();
    let p2 = common::wait_run_id(&e2, r2).await;
    assert_eq!(p2.status, "succeeded");
    assert_eq!(p2.reachable, 3);
    assert_eq!(
        world.total_page_requests(),
        baseline,
        "已确认结果必须继续共享，不应再发实际请求"
    );

    // 新登记一个页面与一条路线：只有变化部分需要新抓取，其他页面照常推进且零请求。
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

    let r3 = e2.start_run(1, RunMode::Incremental).await.unwrap();
    let p3 = common::wait_run_id(&e2, r3).await;
    assert_eq!(p3.status, "succeeded", "{:?}", p3.error);
    let added = world.total_page_requests() - baseline;
    assert!(
        added <= 2,
        "增量只应抓新页 a5 与修订页 a2，实际新增请求 {added}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_and_cycle_progress_is_maintained_per_run() {
    // 两个轮次并发（a0 全图入口 vs a1 下游入口，下游大量汇合），
    // 验证去重进度各自维护在自己的 run_pages，互不串扰；跨轮次共享的只是抓取结果。
    let (e1, e2, store, _world, _dir) = two_engines_shared_store(15_000, 5_000).await;
    let entry_a1 = a1_entry_id(&store).await;

    let r1 = e1.start_run(1, RunMode::Full).await.unwrap();
    let r2 = e2.start_run(entry_a1, RunMode::Full).await.unwrap();
    let p1 = common::wait_run_id(&e1, r1).await;
    let p2 = common::wait_run_id(&e2, r2).await;
    assert_eq!(p1.status, "succeeded");
    assert_eq!(p2.status, "succeeded");

    // 每轮 run_pages 行数等于各自可达页数（汇合/成环被唯一约束去重），各算各的。
    let c1 = store.count_run_pages(r1).await.unwrap();
    let c2 = store.count_run_pages(r2).await.unwrap();
    assert_eq!(c1.total, 8);
    assert_eq!(c2.total, 7);
    assert_eq!(c1.done, 8);
    assert_eq!(c2.done, 7);
    assert_eq!(c1.fetching, 0);
    assert_eq!(c2.fetching, 0);

    // 两轮各自的可达集合大小正确，且 r2 的集合是 r1 的真子集（共享同一图与修订）。
    let reach1 = e1.reachable_keys_for_equivalence(r1).await.unwrap();
    let reach2 = e2.reachable_keys_for_equivalence(r2).await.unwrap();
    assert_eq!(reach1.len(), 8);
    assert_eq!(reach2.len(), 7);
    assert!(reach2.is_subset(&reach1), "a1 入口可达集应包含在 a0 全图内");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_still_go_through_station_rate_limiter() {
    // 单进程内把两站节奏调慢，验证租约层没有绕过 Fetcher 的站点限速与在途上限。
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("rate.db").display());
    let store = Store::connect(&url).await.unwrap();
    drainage_collector::seed::seed_demo_stations(&store)
        .await
        .unwrap();
    for mut st in store.list_stations().await.unwrap() {
        st.min_interval_ms = 30;
        st.max_inflight = 1;
        store.upsert_station(&st).await.unwrap();
    }
    let world = drainage_collector::mock::MockWorld::demo();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client));
    let engine = Engine::new(store, fetcher);

    let started = std::time::Instant::now();
    let rid = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run_id(&engine, rid).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    let elapsed = started.elapsed();

    // max_inflight=1 且 min_interval=30ms：8 个页面的请求不可能瞬间打完，
    // 至少应观察到限速带来的串行间隔（放宽到 80ms，避免 CI 抖动误判）。
    assert!(
        elapsed >= Duration::from_millis(80),
        "请求似乎绕过了站点限速，整轮仅耗时 {elapsed:?}"
    );
    // 在途峰值不得超过每站在途上限。
    assert!(world.inflight_peak("station-a") <= 1);
    assert!(world.inflight_peak("station-b") <= 1);
    assert_eq!(world.total_page_requests(), 8);
}

// ---------- 辅助 ----------

async fn count_sql(store: &Store, sql: &str) -> i64 {
    use sqlx::Row;
    sqlx::query(sql)
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get::<i64, _>("c")
        .unwrap()
}

/// 直接写入一条租约，用于构造已过期/终态等确定性场景。
#[allow(clippy::too_many_arguments)]
async fn insert_lease(
    store: &Store,
    sid: &str,
    page: &str,
    kind: &str,
    value: i64,
    owner: &str,
    state: &str,
    actual_revision: Option<i64>,
    expires_at: i64,
) {
    let now = drainage_collector::model::now_millis();
    sqlx::query(
        "INSERT INTO fetch_leases \
           (station_id, page_key, target_kind, target_value, owner_id, state, actual_revision, expires_at, updated_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(station_id, page_key, target_kind, target_value) DO UPDATE SET \
           owner_id=excluded.owner_id, state=excluded.state, actual_revision=excluded.actual_revision, \
           expires_at=excluded.expires_at, updated_at=excluded.updated_at",
    )
    .bind(sid)
    .bind(page)
    .bind(kind)
    .bind(value)
    .bind(owner)
    .bind(state)
    .bind(actual_revision)
    .bind(expires_at)
    .bind(now)
    .execute(&store.pool)
    .await
    .unwrap();
}

/// 轮询直到条件成立（带超时），避免 sleep 固定时长造成的脆弱测试。
async fn wait_until<F, Fut>(mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if cond().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "等待条件超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn lease_owner(store: &Store, sid: &str, page: &str) -> String {
    use sqlx::Row;
    sqlx::query("SELECT owner_id FROM fetch_leases WHERE station_id = ? AND page_key = ?")
        .bind(sid)
        .bind(page)
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get("owner_id")
        .unwrap()
}

async fn lease_state(store: &Store, sid: &str, page: &str) -> String {
    use sqlx::Row;
    sqlx::query("SELECT state FROM fetch_leases WHERE station_id = ? AND page_key = ?")
        .bind(sid)
        .bind(page)
        .fetch_one(&store.pool)
        .await
        .unwrap()
        .try_get("state")
        .unwrap()
}

/// 是否存在指定 (站点, 页面, 状态) 的租约。
async fn matches_lease(store: &Store, sid: &str, page: &str, state: &str) -> bool {
    use sqlx::Row;
    sqlx::query(
        "SELECT COUNT(*) AS c FROM fetch_leases WHERE station_id = ? AND page_key = ? AND state = ?",
    )
    .bind(sid)
    .bind(page)
    .bind(state)
    .fetch_one(&store.pool)
    .await
    .unwrap()
    .try_get::<i64, _>("c")
    .unwrap()
        > 0
}
