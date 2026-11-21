//! 变更清单采集的游标语义：
//! - 清单请求携带本轮截止点，逐页落库，绝不整段读进内存；
//! - 每站最多保留一页未落库清单；
//! - 重启或另一入口续采时跳过已确认范围，不重读已收到的页；
//! - 登记序号有空洞时也要把本轮变更取齐；
//! - 新轮次卡在后续清单页时，已齐清单、所需页的较早轮次先完成。
mod common;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use drainage_collector::engine::Engine;
use drainage_collector::fetch::{FetchTarget, FetchedPage, Fetcher, PageClient, PageFetchError};
use drainage_collector::mock::{MockPageClient, MockWorld};
use drainage_collector::model::{ChangeLogEntry, FacilityInfo, LinkRef, RunMode, StationConfig};
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;

/// 在第 N 次变更清单请求时放弃当前任务，模拟进程在清单同步中途退出。
struct AbortAfterPages {
    inner: Arc<MockPageClient>,
    seen: std::sync::atomic::AtomicU32,
}

#[async_trait]
impl PageClient for AbortAfterPages {
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> Result<FetchedPage, PageFetchError> {
        self.inner.fetch_page(station, page_key, target).await
    }

    async fn fetch_state(&self, station: &StationConfig) -> anyhow::Result<i64> {
        self.inner.fetch_state(station).await
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
        until_seq: i64,
    ) -> anyhow::Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        // 站 A 的第 2 次清单请求即模拟崩溃点（第一页已确认落库）。
        let n = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n >= 1 && station.id == "station-a" {
            // 放弃当前任务（与进程被 SIGKILL 等效：没有后续写入）。
            futures_abort().await;
        }
        self.inner
            .fetch_changes(station, after_seq, until_seq)
            .await
    }
}

/// 站 A 第二次变更清单请求（第二页）永久挂起，模拟新轮次卡在后续清单页。
struct StallSecondPage {
    inner: Arc<MockPageClient>,
    seen: std::sync::atomic::AtomicU32,
}

#[async_trait]
impl PageClient for StallSecondPage {
    async fn fetch_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: FetchTarget,
    ) -> Result<FetchedPage, PageFetchError> {
        self.inner.fetch_page(station, page_key, target).await
    }

    async fn fetch_state(&self, station: &StationConfig) -> anyhow::Result<i64> {
        self.inner.fetch_state(station).await
    }

    async fn fetch_changes(
        &self,
        station: &StationConfig,
        after_seq: i64,
        until_seq: i64,
    ) -> anyhow::Result<(Vec<ChangeLogEntry>, Option<i64>)> {
        if station.id == "station-a" {
            let n = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n >= 1 {
                std::future::pending::<()>().await;
            }
        }
        self.inner
            .fetch_changes(station, after_seq, until_seq)
            .await
    }
}

/// 在后台任务里永挂起，令当前任务永远等不到结果，然后由测试丢弃运行时。
async fn futures_abort() {
    // 直接 panic 会让轮次被标记 failed；这里挂起，模拟进程不再推进。
    std::future::pending::<()>().await
}

/// 站 A 的初始登记共 6 条（a0,a1,a2,a3 与 a1 修订、a4）；站 B 共 3 条。
fn make_world() -> MockWorld {
    MockWorld::demo()
}

async fn harness_with(
    world: MockWorld,
) -> (Arc<Engine>, Store, Arc<MockPageClient>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("cursor.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let client = Arc::new(MockPageClient::new(world));
    let fetcher = Arc::new(Fetcher::new(client.clone()));
    let engine = Engine::new(store.clone(), fetcher);
    (engine, store, client, dir)
}

#[tokio::test]
async fn change_requests_carry_cutoff_and_advance_monotonically() {
    let world = make_world();
    let (engine, store, _client, _dir) = harness_with(world.clone()).await;
    world.clear_change_requests();

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let cutoff_a = store.get_run(run_id).await.unwrap().watermarks["station-a"].clone();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);

    let reqs = world.change_requests();
    assert!(!reqs.is_empty(), "应实际续读变更清单");
    // 每次请求都携带本轮截止点。
    for (station, after, until) in &reqs {
        assert!(
            *until == cutoff_a || *station == "station-b",
            "站 {station} 的清单请求未携带本轮截止点: after={after} until={until}"
        );
        assert!(after < until || *station == "station-b");
    }
    // 站 A 的游标严格单调前进，页与页之间不重读。
    let a: Vec<(i64, i64)> = reqs
        .iter()
        .filter(|(s, _, _)| s == "station-a")
        .map(|(_, a, u)| (*a, *u))
        .collect();
    for w in a.windows(2) {
        assert!(w[1].0 > w[0].0, "站 A 清单游标重复或倒退: {:?}", a);
    }
}

#[tokio::test]
async fn seq_gaps_are_still_covered_within_the_round() {
    let world = make_world();
    let (engine, store, _client, _dir) = harness_with(world.clone()).await;

    // 站 A 现有最新 seq（demo 的登记序号是全局递增的），在其后跨越空档登记。
    // 让 a1 的新修订接到 a6、a6 接到 a7，确认空档后的新路线也被走通。
    let base = world.get_station("station-a").unwrap().latest_seq;
    // 空档登记三条 (base+4 / +11 / +18)。a1 的修订（revision 3，demo 已用 2）
    // 指向 a6，使空档后的新路线能从入口走到；修订登记 seq 落在截止点内。
    let seqs = [base + 4, base + 11, base + 18];
    let gap_fac = |n: &str| FacilityInfo {
        name: Some(n.into()),
        kind: Some("gate".into()),
    };
    let link = |page: &str| LinkRef {
        station_id: "station-a".into(),
        page_key: page.into(),
    };
    world.with_station("station-a", |s| {
        // a1 修订登记在 base+1（早于空档页，但仍是本轮截止点内）。
        s.put_version(
            base + 1,
            "a1",
            3,
            gap_fac("A1号检查井(接空档页)"),
            vec![link("a2"), link("a6")],
            "revised",
        );
        s.put_version(
            seqs[0],
            "a6",
            1,
            gap_fac("A6号闸门"),
            vec![link("a7")],
            "registered",
        );
        s.put_version(
            seqs[1],
            "a7",
            1,
            gap_fac("A7号闸门"),
            vec![link("a8")],
            "registered",
        );
        s.put_version(seqs[2], "a8", 1, gap_fac("A8号闸门"), vec![], "registered");
    });
    let pages = ["a6", "a7", "a8"];

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(
        store.get_run(run_id).await.unwrap().watermarks["station-a"],
        seqs[2]
    );

    // 空洞后的变更都必须取到并落库。
    for (seq, page) in seqs.iter().zip(pages.iter()) {
        let row = sqlx::query(
            "SELECT COUNT(*) AS c FROM change_log WHERE station_id = 'station-a' AND seq = ? AND page_key = ?",
        )
        .bind(seq)
        .bind(page)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        let c: i64 = sqlx::Row::try_get(&row, "c").unwrap();
        assert_eq!(c, 1, "seq={seq} 的登记应已落库");
    }
    assert_eq!(store.change_cursor("station-a").await.unwrap(), seqs[2]);

    // 新页面均可达（a1 修订接入 a6，a6 接入 a7）。
    let keys = engine.reachable_keys_for_equivalence(run_id).await.unwrap();
    for page in pages {
        assert!(
            keys.iter().any(|(s, p, _)| s == "station-a" && p == page),
            "{page} 应可达"
        );
    }
}

#[tokio::test]
async fn already_confirmed_pages_are_not_reread_by_another_entry() {
    // 第一个轮次把所有清单页收全。随后第二个入口用同一截止点开轮次：
    // 游标已在截止点，不应再发任何清单请求。
    let world = make_world();
    let (engine, store, _client, _dir) = harness_with(world.clone()).await;

    let r1 = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run(&engine, r1).await.status, "succeeded");
    let cursor_a = store.change_cursor("station-a").await.unwrap();
    let cutoff_a = store.get_run(r1).await.unwrap().watermarks["station-a"].clone();
    assert_eq!(cursor_a, cutoff_a, "清单应已收齐到截止点");

    let entry2 = store
        .add_entry("station-b", "b1", "河西调蓄池入口")
        .await
        .unwrap();
    world.clear_change_requests();
    let r2 = engine.start_run(entry2, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run(&engine, r2).await.status, "succeeded");

    // 截止点未推进，两站都不应重读已确认清单。
    assert!(
        world.change_requests().is_empty(),
        "另一入口续采不应重读已确认清单: {:?}",
        world.change_requests()
    );
}

#[tokio::test]
async fn restart_does_not_reread_confirmed_change_pages() {
    // 模拟“进程在清单同步过程中退出”：第一次只同步一页就崩溃式丢弃引擎；
    // 重启续跑时，应从已确认游标之后继续，而不是从头重读。
    let world = make_world();
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("restart.db").display());
    let base = world.get_station("station-a").unwrap().latest_seq;

    // 先让清单足够多页（A 站 6 条 + 追加到 15 条），且放慢站点节奏无关。
    {
        let store = Store::connect(&db_url).await.unwrap();
        seed_demo_stations(&store).await.unwrap();
        let next_seq = base;
        // a1 新修订接到全部追加页（该修订的 seq 略后登记），确保它们在图中可达。
        let links: Vec<LinkRef> = (1..=9)
            .map(|i| LinkRef {
                station_id: "station-a".into(),
                page_key: format!("x{}", next_seq + i),
            })
            .collect();
        for i in 1..=9 {
            let seq = next_seq + i;
            world.with_station("station-a", |s| {
                s.put_version(
                    seq,
                    &format!("x{seq}"),
                    1,
                    FacilityInfo {
                        name: Some(format!("追加{seq}")),
                        kind: Some("manhole".into()),
                    },
                    vec![],
                    "registered",
                )
            });
        }
        world.with_station("station-a", |s| {
            s.put_version(
                next_seq + 10,
                "a1",
                3,
                FacilityInfo {
                    name: Some("A1号检查井(扩建)".into()),
                    kind: Some("manhole".into()),
                },
                links,
                "revised",
            );
        });

        // 包装客户端：第 2 次清单请求时直接放弃当前任务，模拟进程在同步中途退出。
        let inner = Arc::new(MockPageClient::new(world.clone()));
        let aborting = Arc::new(AbortAfterPages {
            inner,
            seen: std::sync::atomic::AtomicU32::new(0),
        });
        let engine = Engine::new(store.clone(), Arc::new(Fetcher::new(aborting)));
        let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
        // 轮次在后台任务里跑；等第一页确认落库、第二页请求挂起。
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let c = store.change_cursor("station-a").await.unwrap();
            if c > 0 {
                // 第二页请求会挂起；再等一会确保它已经发出，游标停在第一页末尾。
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                break;
            }
            assert!(std::time::Instant::now() < deadline, "等待清单同步超时");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let _ = run_id;
        // 丢弃引擎与连接，模拟进程退出（后台任务随连接一起消失）。
        drop(engine);
        drop(store);
    }

    // 重启：复用同一文件库与模拟世界。
    let store2 = Store::connect(&db_url).await.unwrap();
    let cursor_before = store2.change_cursor("station-a").await.unwrap();
    assert!(cursor_before > 0, "崩溃前应至少确认了一页清单");
    // 未走完的轮次仍为 running，且已确认清单保持落库。
    let run_id = store2
        .list_runs(10)
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.status == drainage_collector::model::RunStatus::Running)
        .expect("应留下未完成的轮次")
        .id;
    world.clear_change_requests();
    let engine2 = common::fresh_engine_for_store(store2.clone(), world.clone()).await;
    let resumed = engine2.resume_on_boot().await.unwrap();
    assert!(resumed.contains(&run_id));

    // 等原轮次完成。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let p = engine2.progress(run_id).await.unwrap();
        if p.status != "running" {
            assert_eq!(p.status, "succeeded", "{:?}", p.error);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "续跑超时");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    // 重启后的清单请求必须从已确认游标之后开始，不得重读已收到的页。
    let a_reqs: Vec<(i64, i64)> = world
        .change_requests()
        .into_iter()
        .filter(|(s, _, _)| s == "station-a")
        .map(|(_, af, un)| (af, un))
        .collect();
    assert!(
        a_reqs.iter().all(|(after, _)| *after >= cursor_before),
        "重启后重读了已确认范围: cursor_before={cursor_before}, 请求={a_reqs:?}"
    );

    // 追加登记的页面全部可达，证明多页清单被取齐。
    let keys = engine2
        .reachable_keys_for_equivalence(run_id)
        .await
        .unwrap();
    let pages: HashSet<_> = keys
        .iter()
        .filter(|(s, _, _)| s == "station-a")
        .map(|(_, p, _)| p.as_str())
        .collect();
    for i in 1..=9 {
        let page = format!("x{}", base + i);
        assert!(pages.contains(page.as_str()), "{page} 应可达");
    }
}

#[tokio::test]
async fn newer_round_blocked_on_later_page_does_not_starve_earlier_round() {
    // 场景：新轮次（截止点 base+7）在第二页清单处被站点挂起；
    // 与此同时，一个清单已齐、所需页已可用的较早轮次（另一个入口）
    // 必须能先完成，而不是被迫等待新轮次的后续页。
    let world = make_world();
    let dir = tempfile::tempdir().unwrap();
    let db_url = format!("sqlite://{}", dir.path().join("starve.db").display());
    let store = Store::connect(&db_url).await.unwrap();
    seed_demo_stations(&store).await.unwrap();

    // 基线：正常完成一轮，游标推进到 base。
    let plain = Arc::new(MockPageClient::new(world.clone()));
    let engine = Engine::new(store.clone(), Arc::new(Fetcher::new(plain)));
    let r1 = engine.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(common::wait_run(&engine, r1).await.status, "succeeded");
    let base = world.get_station("station-a").unwrap().latest_seq;
    assert_eq!(store.change_cursor("station-a").await.unwrap(), base);
    drop(engine);

    // 追加两页变更：a1 修订（把后续页接入入口）+ 六条 later* 登记。
    let later_seqs: Vec<i64> = (2..=7).map(|d| base + d).collect();
    let later_links: Vec<LinkRef> = later_seqs
        .iter()
        .map(|seq| LinkRef {
            station_id: "station-a".into(),
            page_key: format!("later{seq}"),
        })
        .collect();
    world.with_station("station-a", |s| {
        s.put_version(
            base + 1,
            "a1",
            3,
            FacilityInfo {
                name: Some("A1号检查井(接后续页)".into()),
                kind: Some("manhole".into()),
            },
            later_links,
            "revised",
        );
    });
    for &seq in &later_seqs {
        let page = format!("later{seq}");
        world.with_station("station-a", |s| {
            s.put_version(
                seq,
                &page,
                1,
                FacilityInfo {
                    name: Some(format!("后页设施{seq}")),
                    kind: Some("manhole".into()),
                },
                vec![],
                "registered",
            )
        });
    }

    // 新轮次使用会卡住站 A 第二页的客户端；它在后台运行。
    let stalling = Arc::new(StallSecondPage {
        inner: Arc::new(MockPageClient::new(world.clone())),
        seen: std::sync::atomic::AtomicU32::new(0),
    });
    let engine_new = Engine::new(store.clone(), Arc::new(Fetcher::new(stalling)));
    let newer = engine_new.start_run(1, RunMode::Full).await.unwrap();
    assert_eq!(
        store.get_run(newer).await.unwrap().watermarks["station-a"],
        base + 7
    );
    // 等第一页落库（base+1..base+3），随后第二页请求挂起。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if store.change_cursor("station-a").await.unwrap() >= base + 3 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "等第一页落库超时");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(
        store.get_run(newer).await.unwrap().status.as_str(),
        "running",
        "新轮次应仍卡在后续清单页"
    );

    // 较早轮次：截止点 = base+3（第一页末尾），其所需清单已随第一页确认落库。
    // 直接固化该截止点入队入口，模拟一个“更早开始、清单已齐”的轮次。
    // 此时新轮次仍卡在第二页，较早轮次应能独立跑完。
    let mut wm = std::collections::BTreeMap::new();
    wm.insert("station-a".to_string(), base + 3);
    wm.insert(
        "station-b".to_string(),
        world.get_station("station-b").unwrap().latest_seq,
    );
    let early = store.create_run(1, RunMode::Full, &wm).await.unwrap();
    store
        .enqueue_run_page(early, "station-a", "a0", 0)
        .await
        .unwrap();

    let before_reqs = world
        .change_requests()
        .iter()
        .filter(|(s, _, _)| s == "station-a")
        .count();
    let plain2 = Arc::new(MockPageClient::new(world.clone()));
    let engine_early = Engine::new(store.clone(), Arc::new(Fetcher::new(plain2)));
    engine_early.resume_run(early).await.unwrap();

    let p_early = engine_early.progress(early).await.unwrap();
    assert_eq!(p_early.status, "succeeded", "{:?}", p_early.error);

    // 新轮次仍卡住，未被较早轮次“带跑”或改写状态。
    assert_eq!(
        store.get_run(newer).await.unwrap().status.as_str(),
        "running"
    );

    // 较早轮次清单已齐，不应再发出站 A 的清单请求（尤其不会为第二页而等待）。
    let a_reqs_after: Vec<(i64, i64)> = world
        .change_requests()
        .into_iter()
        .filter(|(s, _, _)| s == "station-a")
        .map(|(_, a, u)| (a, u))
        .collect();
    assert_eq!(
        a_reqs_after.len(),
        before_reqs,
        "清单已齐的较早轮次不应重读站 A 清单: {:?}",
        &a_reqs_after[before_reqs.min(a_reqs_after.len())..]
    );

    // 较早轮次截止点内的新增页（a1 rev3 与之相连的前两页）已可达。
    let keys = engine_early
        .reachable_keys_for_equivalence(early)
        .await
        .unwrap();
    for seq in [base + 2, base + 3] {
        let page = format!("later{seq}");
        assert!(
            keys.iter().any(|(s, p, _)| s == "station-a" && p == &page),
            "{page} 应可达"
        );
    }

    // 清理：停掉卡住的新轮次，避免遗留后台任务影响其它断言。
    drop(engine_new);
}
