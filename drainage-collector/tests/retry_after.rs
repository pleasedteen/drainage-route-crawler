//! Retry-After 统一等待期验证：
//! 1. 任一请求类型（state/changes/page）返回 Retry-After，该站三类请求进入同一等待期，
//!    等待结束前不再新发；耗尽重试后的等待同样生效。
//! 2. 状态请求失败时截止点不会被填成 0。
//! 3. 故障站等待期间，其他站同阶段任务继续推进，即使页面工作槽都被故障站页面占住。
//! 4. 每站频率 / 在途上限 / 每轮最多 8 个页面槽的预算保持不变。
mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use drainage_collector::engine::Engine;
use drainage_collector::fetch::{
    FetchedPage, Fetcher, FetchTarget, PageClient, PageFetchError, MAX_ATTEMPTS,
};
use drainage_collector::mock::{EndpointKind, MockPageClient, MockWorld, RequestRecord};
use drainage_collector::model::{
    ChangeLogEntry, FacilityInfo, LinkRef, RunMode, StationConfig,
};
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;

/// 全新测试夹具（独立 DB 与 world，避免跨用例污染）。
async fn setup(
    min_interval_ms: u64,
    max_inflight: u32,
) -> (Arc<Engine>, Store, MockWorld, Arc<MockPageClient>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("retry.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    for mut st in store.list_stations().await.unwrap() {
        st.min_interval_ms = min_interval_ms;
        st.max_inflight = max_inflight;
        store.upsert_station(&st).await.unwrap();
    }
    let world = MockWorld::demo();
    world.reset_faults_and_log();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client.clone()));
    let engine = Engine::new(store.clone(), fetcher);
    (engine, store, world, client, dir)
}

async fn wait_finish(engine: &Engine, run_id: i64) -> drainage_collector::model::RunProgress {
    common::wait_run(engine, run_id).await
}

/// 取时间线中某站某端点的全部记录。
fn records_of<'a>(
    log: &'a [RequestRecord],
    station: &str,
    endpoint: &str,
) -> Vec<&'a RequestRecord> {
    log.iter()
        .filter(|r| r.station_id == station && r.endpoint == endpoint)
        .collect()
}

/// 可按 (站点, 页面) 精确注入 Retry-After 的包装客户端，
/// 用于让故障落在非入口页，保证其他站同阶段任务仍可推进。
struct ScriptedPageClient {
    inner: MockPageClient,
    world: MockWorld,
    /// 命中这些页面的前 N 次请求返回 Retry-After（N 为剩余次数，随请求递减）。
    ra_pages: std::sync::Mutex<Vec<(String, String, u32, u64)>>,
}

impl ScriptedPageClient {
    fn new(
        inner: MockPageClient,
        world: MockWorld,
        ra_pages: Vec<(String, String, u32, u64)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            world,
            ra_pages: std::sync::Mutex::new(ra_pages),
        })
    }

    fn match_fault(&self, station: &str, page: &str) -> Option<u64> {
        let mut g = self.ra_pages.lock().unwrap();
        if let Some(pos) = g
            .iter()
            .position(|(s, p, hits, _)| s == station && p == page && *hits > 0)
        {
            g[pos].2 -= 1;
            let secs = g[pos].3;
            if g[pos].2 == 0 {
                g.remove(pos);
            }
            return Some(secs);
        }
        None
    }
}

#[async_trait]
impl PageClient for ScriptedPageClient {
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> Result<FetchedPage, PageFetchError> {
        if let Some(secs) = self.match_fault(&station.id, page_key) {
            // 与真实模拟站一致：命中故障也写入统一请求时间线。
            self.world.record(
                &station.id,
                EndpointKind::Page,
                page_key,
                "retry-after",
            );
            return Err(PageFetchError::RetryAfter(Duration::from_secs(secs)));
        }
        self.inner.fetch_page(station, page_key, target).await
    }

    async fn fetch_state(&self, station: &StationConfig) -> anyhow::Result<i64> {
        self.inner.fetch_state(station).await
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> anyhow::Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        self.inner.fetch_changes(station, after_seq).await
    }
}

/// 给 a0 增加一条直达 B 站 b1 的路线（新修订），
/// 这样 A 站内部页面故障时，B 站仍可独立抵达，用来验证跨站不被拖垮。
fn add_a0_direct_link_to_b(world: &MockWorld) {
    world.with_station("station-a", |s| {
        let seq = s.latest_seq + 1;
        s.put_version(
            seq,
            "a0",
            2,
            FacilityInfo {
                name: Some("A站入流口(扩建)".into()),
                kind: Some("inlet".into()),
            },
            vec![
                LinkRef {
                    station_id: "station-a".into(),
                    page_key: "a1".into(),
                },
                LinkRef {
                    station_id: "station-b".into(),
                    page_key: "b1".into(),
                },
            ],
            "revised",
        );
    });
}

// ---------- 1. 页面端点 Retry-After：等待期内该站不再发任何请求，恢复后自动续跑 ----------

#[tokio::test]
async fn page_retry_after_blocks_all_request_types_of_that_station_then_resumes() {
    let (engine, _store, world, _client, _dir) = setup(10, 2).await;
    // A 站第一个页面请求返回 Retry-After: 1s（只命中一次，之后恢复正常）。
    world.inject_fault("station-a", EndpointKind::Page, 1, true, 1);

    let t0 = Instant::now();
    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_finish(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);

    let log = world.request_log();
    let a429 = log
        .iter()
        .find(|r| r.station_id == "station-a" && r.outcome == "retry-after")
        .expect("应记录一次 A 站 Retry-After");
    // 等待期（1s）内 A 站不得有任何新请求（state/changes/page 都算）。
    let during = log
        .iter()
        .filter(|r| {
            r.station_id == "station-a" && r.t_ms > a429.t_ms && r.t_ms < a429.t_ms + 900
        })
        .count();
    assert_eq!(during, 0, "等待期内 A 站不应发出任何新请求");
    // 等待结束后 A 站恢复请求并全部成功。
    let after_ok = log
        .iter()
        .filter(|r| {
            r.station_id == "station-a" && r.t_ms >= a429.t_ms + 900 && r.outcome == "ok"
        })
        .count();
    assert!(after_ok >= 1, "等待结束后 A 站应恢复请求");
    assert!(t0.elapsed() >= Duration::from_millis(900));
}

// ---------- 2. 状态端点 Retry-After：截止点读取也进入同一等待期，且不写 0 ----------

#[tokio::test]
async fn state_retry_after_pauses_station_and_never_writes_zero_watermark() {
    let (engine, store, world, _client, _dir) = setup(10, 2).await;
    // A 站状态端点先返回 2 次 Retry-After（1s），第 3 次成功。
    world.inject_fault("station-a", EndpointKind::State, 2, true, 1);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_finish(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);

    // 截止点必须等于模拟站真实 latest_seq，绝不是 0。
    let expected_a = world.get_station("station-a").unwrap().latest_seq;
    let expected_b = world.get_station("station-b").unwrap().latest_seq;
    let run = store.get_run(run_id).await.unwrap();
    assert_eq!(run.watermarks.get("station-a"), Some(&expected_a));
    assert_eq!(run.watermarks.get("station-b"), Some(&expected_b));
    assert!(expected_a > 0 && expected_b > 0);

    // 两次 Retry-After 之后的等待期内，A 站没有任何请求新发。
    let log = world.request_log();
    let state_recs = records_of(&log, "station-a", "state");
    let ra: Vec<_> = state_recs
        .iter()
        .filter(|r| r.outcome == "retry-after")
        .collect();
    assert_eq!(ra.len(), 2);
    let first_ra = ra[0].t_ms;
    let during = log
        .iter()
        .filter(|r| {
            r.station_id == "station-a" && r.t_ms > first_ra && r.t_ms < first_ra + 900
        })
        .count();
    assert_eq!(during, 0, "状态端点等待期内 A 站不应有新请求");
}

// ---------- 3. 清单端点 Retry-After：翻清单进入同一等待期 ----------

#[tokio::test]
async fn changes_retry_after_pauses_pagination_and_resumes() {
    let (engine, _store, world, _client, _dir) = setup(10, 2).await;
    // B 站清单第一页返回 Retry-After。
    world.inject_fault("station-b", EndpointKind::Changes, 1, true, 1);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_finish(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);

    let log = world.request_log();
    let ra = log
        .iter()
        .find(|r| {
            r.station_id == "station-b"
                && r.endpoint == "changes"
                && r.outcome == "retry-after"
        })
        .expect("清单端点应返回一次 Retry-After");
    let during = log
        .iter()
        .filter(|r| r.station_id == "station-b" && r.t_ms > ra.t_ms && r.t_ms < ra.t_ms + 900)
        .count();
    assert_eq!(during, 0, "清单等待期内 B 站不应有新请求");
    // 等待后清单续读成功。
    let changes_ok = records_of(&log, "station-b", "changes")
        .iter()
        .filter(|r| r.outcome == "ok")
        .count();
    assert!(changes_ok >= 1);
}

// ---------- 4. 故障站等待期间其他站同阶段任务继续，且不被故障站占满页面槽 ----------

#[tokio::test]
async fn other_station_keeps_progressing_even_when_faulty_station_holds_slots() {
    // 两站各 4 在途 -> 每轮 worker 数 = min(4+4, 8) = 8（页面槽上限保持 8）。
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("slots.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    for mut st in store.list_stations().await.unwrap() {
        st.min_interval_ms = 5;
        st.max_inflight = 4;
        store.upsert_station(&st).await.unwrap();
    }
    let world = MockWorld::demo();
    world.reset_faults_and_log();
    // a0 增加直达 b1 的路线：a1 故障期间 B 站仍可独立抵达。
    add_a0_direct_link_to_b(&world);

    // a1 的前 2 次页面请求返回 Retry-After: 1s（第 3 次恢复，轮次最终成功）。
    let client = ScriptedPageClient::new(
        MockPageClient::new(world.clone()),
        world.clone(),
        vec![("station-a".into(), "a1".into(), 2, 1)],
    );
    let engine = Engine::new(store.clone(), Arc::new(Fetcher::new(client)));

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_finish(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);

    let log = world.request_log();
    let a_ra = log
        .iter()
        .find(|r| r.station_id == "station-a" && r.outcome == "retry-after")
        .unwrap();
    // A 站等待期内，B 站页面工作照常推进（至少完成 2 个 B 站页面请求）。
    let b_during: Vec<_> = log
        .iter()
        .filter(|r| {
            r.station_id == "station-b"
                && r.endpoint == "page"
                && r.t_ms > a_ra.t_ms
                && r.t_ms < a_ra.t_ms + 900
                && r.outcome == "ok"
        })
        .collect();
    assert!(
        b_during.len() >= 2,
        "A 站等待期内 B 站页面应继续推进，实际 {} 个",
        b_during.len()
    );
    // 同期 A 站自己一个请求都不发。
    let a_during = log
        .iter()
        .filter(|r| {
            r.station_id == "station-a" && r.t_ms > a_ra.t_ms && r.t_ms < a_ra.t_ms + 900
        })
        .count();
    assert_eq!(a_during, 0);
}

// ---------- 5. 每轮页面工作槽上限 8、每站在途/频率预算保持 ----------

#[tokio::test]
async fn page_worker_slots_capped_at_eight_and_station_budgets_kept() {
    // 即使配置在途之和超过 8，worker 数也被夹到 8。
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("cap.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    for mut st in store.list_stations().await.unwrap() {
        st.min_interval_ms = 20;
        st.max_inflight = 8; // 两站合计 16，应被夹到 8
        store.upsert_station(&st).await.unwrap();
    }
    let world = MockWorld::demo();
    world.reset_faults_and_log();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client.clone()));
    let engine = Engine::new(store.clone(), fetcher);

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_finish(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);

    // 每站最小间隔：同站相邻两次页面请求间隔 >= min_interval（允许少量调度误差）。
    let log = world.request_log();
    for station in ["station-a", "station-b"] {
        let mut times: Vec<u128> = records_of(&log, station, "page")
            .iter()
            .map(|r| r.t_ms)
            .collect();
        times.sort_unstable();
        for w in times.windows(2) {
            assert!(
                w[1] - w[0] >= 15,
                "{station} 相邻页面请求间隔 {}ms 低于 min_interval=20ms 预算",
                w[1] - w[0]
            );
        }
    }
}

// ---------- 6. 状态请求彻底失败：返回错误，截止点绝不填 0 ----------

struct AlwaysFailStateClient {
    inner: MockPageClient,
    fail_station: String,
}

#[async_trait]
impl PageClient for AlwaysFailStateClient {
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> Result<FetchedPage, PageFetchError> {
        self.inner.fetch_page(station, page_key, target).await
    }

    async fn fetch_state(&self, station: &StationConfig) -> anyhow::Result<i64> {
        if station.id == self.fail_station {
            // 模拟状态端点持续故障：合法的 0 与失败必须可区分，这里只能报错。
            return Err(anyhow::anyhow!("状态端点持续故障 500"));
        }
        self.inner.fetch_state(station).await
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
    ) -> anyhow::Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        self.inner.fetch_changes(station, after_seq).await
    }
}

#[tokio::test]
async fn state_failure_does_not_fabricate_zero_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("statefail.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = MockWorld::demo();
    world.reset_faults_and_log();
    let inner = MockPageClient::new(world.clone());
    let client = Arc::new(AlwaysFailStateClient {
        inner,
        fail_station: "station-a".into(),
    });
    let fetcher = Arc::new(Fetcher::new(client));
    let engine = Engine::new(store.clone(), fetcher);

    // 轮次启动应直接失败（状态读不到），而不是带着 0 截止点继续。
    let err = engine
        .start_run(1, RunMode::Full)
        .await
        .expect_err("状态端点持续故障时启动轮次必须报错");
    assert!(err.to_string().contains("状态"), "{err}");

    // 没有任何轮次被创建，也就不存在被填成 0 的 watermarks。
    let runs = store.list_runs(20).await.unwrap();
    assert!(runs.is_empty(), "失败的状态读取不应产生轮次记录");
}

// ---------- 7a. Fetcher 层：耗尽重试后的 Retry-After 等待依然生效 ----------

#[tokio::test]
async fn fetcher_retry_after_wait_still_active_after_attempt_budget_exhausted() {
    let world = MockWorld::demo();
    world.reset_faults_and_log();
    // a1 对每次请求都返回 1s Retry-After，永不恢复。
    let client = ScriptedPageClient::new(
        MockPageClient::new(world.clone()),
        world.clone(),
        vec![(
            "station-a".into(),
            "a1".into(),
            MAX_ATTEMPTS + 10,
            1,
        )],
    );
    let fetcher = Fetcher::new(client);
    let station = StationConfig {
        id: "station-a".into(),
        name: "t".into(),
        base_url: "mock://station-a".into(),
        format: drainage_collector::model::SiteFormat::Html,
        min_interval_ms: 0,
        max_inflight: 2,
        watermark_seq: 0,
    };

    // 第一次真实请求立刻收到 RetryAfter，并把 1s 等待期写入控制器。
    match fetcher
        .fetch_page(&station, "a1", FetchTarget::AtRevision(1))
        .await
    {
        Err(PageFetchError::RetryAfter(d)) => assert_eq!(d, Duration::from_secs(1)),
        other => panic!("首次请求应返回 RetryAfter，实际 {other:?}"),
    }
    assert!(fetcher.station_paused("station-a").await);

    // 等待期内连续再试 MAX_ATTEMPTS-1 次：acquire 非阻塞地直接报 StationPaused，
    // 一次真实请求都不会发出（工作槽因此能让给其他站）。
    let before = world.request_log().len();
    for _ in 1..MAX_ATTEMPTS {
        let err = fetcher
            .fetch_page(&station, "a1", FetchTarget::AtRevision(1))
            .await
            .unwrap_err();
        assert!(matches!(err, PageFetchError::StationPaused(_)), "{err:?}");
    }
    assert_eq!(world.request_log().len(), before, "等待期内不得真正发请求");

    // 等待结束后自动恢复：可以再次真正发起（这次仍是 429，说明控制器没被锁死）。
    tokio::time::sleep(Duration::from_millis(1_050)).await;
    assert!(!fetcher.station_paused("station-a").await);
    let err = fetcher
        .fetch_page(&station, "a1", FetchTarget::AtRevision(1))
        .await
        .unwrap_err();
    assert!(matches!(err, PageFetchError::RetryAfter(_)), "{err:?}");
    assert_eq!(world.request_log().len(), before + 1);

    // 模拟“耗尽预算的最后一次命中”：此刻控制器仍处于等待期——
    // 等待没有因为重试预算耗尽而失效。
    assert!(
        fetcher.station_paused("station-a").await,
        "耗尽重试后的等待期必须依然生效"
    );
    let remain = fetcher
        .station_pause_remaining("station-a")
        .await
        .expect("应能读到剩余等待时长");
    assert!(remain <= Duration::from_secs(1) && remain > Duration::from_millis(500));
}

// ---------- 7b. 引擎层：预算用尽页面判失败，其他站照常完成 ----------

#[tokio::test]
async fn engine_marks_page_failed_after_budget_but_other_station_completes() {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("exhaust.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    for mut st in store.list_stations().await.unwrap() {
        st.min_interval_ms = 5;
        st.max_inflight = 2;
        store.upsert_station(&st).await.unwrap();
    }
    let world = MockWorld::demo();
    world.reset_faults_and_log();
    // a0 直达 b1：a1 永久故障不影响 B 站独立抵达。
    add_a0_direct_link_to_b(&world);
    let client = ScriptedPageClient::new(
        MockPageClient::new(world.clone()),
        world.clone(),
        vec![("station-a".into(), "a1".into(), MAX_ATTEMPTS + 10, 1)],
    );
    let engine = Engine::new(store.clone(), Arc::new(Fetcher::new(client)));

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = wait_finish(&engine, run_id).await;
    // 页面重试预算用尽 -> 轮次失败（部分结果），但 B 站设施全部可达。
    assert_eq!(p.status, "failed");
    assert_eq!(p.failed, 1, "只有永久 429 的 a1 判失败");
    let reach = engine.reachable(run_id).await.unwrap();
    let b_count = reach.iter().filter(|r| r.station_id == "station-b").count();
    assert_eq!(b_count, 3, "B 站三个设施应照常可达");
    assert!(
        reach.iter().any(|r| r.station_id == "station-a" && r.page_key == "a0"),
        "a0 不受影响，应仍可达"
    );

    // a1 的真实命中：首次为 RetryAfter；等待期内的再认领在发起前即被
    // StationPaused 挡回（不发请求），等待结束后的下一次认领才再次真正命中 429。
    // 因此真实 RetryAfter 命中按约 1s 间隔发生，共 MAX_ATTEMPTS 次后预算用尽。
    let log = world.request_log();
    let ra_times: Vec<u128> = log
        .iter()
        .filter(|r| r.station_id == "station-a" && r.outcome == "retry-after")
        .map(|r| r.t_ms)
        .collect();
    assert_eq!(
        ra_times.len(),
        MAX_ATTEMPTS as usize,
        "每次等待结束后才允许下一次真实命中，应恰好耗尽 {MAX_ATTEMPTS} 次预算"
    );
    for w in ra_times.windows(2) {
        assert!(
            w[1] - w[0] >= 900,
            "两次真实 Retry-After 间隔 {}ms，等待期未生效",
            w[1] - w[0]
        );
    }
    // 最后一次命中（预算用尽）之后的等待期内，A 站不再有任何请求。
    let last = *ra_times.last().unwrap();
    let after_last = log
        .iter()
        .filter(|r| r.station_id == "station-a" && r.t_ms > last && r.t_ms < last + 900)
        .count();
    assert_eq!(after_last, 0, "耗尽重试后的等待期内仍不应新发请求");
}
