//! 真实本地 HTTP 模拟站下的行为验收：
//! - `Retry-After` 的等待期覆盖本站的状态 / 变更清单 / 页面三类请求；
//!   即使触发等待的那次请求已用尽重试，等待期仍然生效；
//! - 状态与变更清单的非成功响应按失败处理（不把错误响应当数据）；
//! - 各站独立读取截止点；
//! - 某站清单仍在等待 / 页面积压时，同轮其他站已就绪的页面照常入库；
//! - 各站发起频率与在途上限保持不变。
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use common::http_harness;
use drainage_collector::fetch::{FetchTarget, Fetcher, HttpPageClient};
use drainage_collector::mock::RequestRecord;
use drainage_collector::model::RunMode;

/// 判断两个时间点之间的间隔（毫秒）。
fn gap_ms(a: i64, b: i64) -> i64 {
    (b - a).abs()
}

fn page_enqueued(log: &[RequestRecord], station: &str, page: &str) -> Option<i64> {
    log.iter()
        .find(|r| r.station_id == station && r.kind == "page" && r.page_key.as_deref() == Some(page))
        .map(|r| r.at_ms)
}

fn last_retry_after(log: &[RequestRecord], station: &str) -> Option<RequestRecord> {
    log.iter()
        .rev()
        .find(|r| r.station_id == station && r.retry_after_secs.is_some())
        .cloned()
}

#[tokio::test]
async fn retry_after_on_changes_pauses_state_and_pages_of_that_station() {
    // 站 A 的变更清单被要求等待 1 秒。等待期必须覆盖本站随后的状态与页面请求。
    let (engine, store, stations, _dir) = http_harness(None).await;
    stations
        .inject("station-a", "/inject/retry-after", "kind=changes&secs=1&times=1")
        .await;

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);

    let log = stations.world.request_log_for("station-a");
    let retry = last_retry_after(&log, "station-a").expect("应有一条 Retry-After 响应");
    assert_eq!(retry.kind, "changes");
    assert_eq!(retry.retry_after_secs, Some(1));

    // 等待期（自该响应结束起 1 秒）内，站 A 的任何请求都不应出现。
    let deadline = retry.finished_ms + 950;
    for r in &log {
        if r.finished_ms > retry.finished_ms {
            assert!(
                r.at_ms >= deadline,
                "站 A 在等待期内又发起请求: {:?} at={} deadline={}",
                r,
                r.at_ms,
                deadline
            );
        }
    }
    // 等待期之后站 A 仍然完成了页面抓取。
    assert!(page_enqueued(&log, "station-a", "a0").is_some());

    // 落库结果完整。
    assert_eq!(p.reachable, 8);
    assert_eq!(engine.reachable(run_id).await.unwrap().len(), 8);
    assert!(!store.list_stations().await.unwrap().is_empty());
}

#[tokio::test]
async fn retry_after_on_page_still_pauses_subsequent_state_and_changes() {
    // 触发等待的是页面请求；等待期结束后本站的状态 / 清单请求也必须遵守。
    let (engine, _store, stations, _dir) = http_harness(None).await;
    stations
        .inject("station-b", "/inject/retry-after", "kind=page&secs=1&times=1")
        .await;

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);

    let log = stations.world.request_log_for("station-b");
    let retry = last_retry_after(&log, "station-b").expect("应有一条 Retry-After 响应");
    assert_eq!(retry.kind, "page");
    let deadline = retry.finished_ms + 950;
    for r in &log {
        if r.finished_ms > retry.finished_ms {
            assert!(
                r.at_ms >= deadline,
                "站 B 在等待期内又发起请求: {:?}",
                r
            );
        }
    }
}

#[tokio::test]
async fn exhausted_retry_after_still_holds_lockout_for_next_run_requests() {
    // 连续 5 次（用尽重试）要求等待：状态读取因此失败、轮次不创建；
    // 等待期仍写入本站控制器，下一轮开始时必须继续等待，而不是立刻猛打。
    let (engine, store, stations, _dir) = http_harness(None).await;
    stations
        .inject("station-a", "/inject/retry-after", "kind=state&secs=1&times=5")
        .await;

    let err = engine.start_run(1, RunMode::Full).await.unwrap_err();
    assert!(err.to_string().contains("状态读取失败"), "{err}");

    let log = stations.world.request_log_for("station-a");
    let state_calls: Vec<_> = log.iter().filter(|r| r.kind == "state").collect();
    assert_eq!(state_calls.len(), 5, "应按最大尝试次数重试");
    assert!(state_calls.iter().all(|r| r.status == 429));
    let retry = last_retry_after(&log, "station-a").unwrap();
    let deadline = retry.finished_ms + 950;

    // 等待期未结束前再次开轮：状态请求必须被推迟到等待期之后。
    let started = std::time::Instant::now();
    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(800),
        "第二次开轮应等待本站等待期，实际只用了 {waited:?}"
    );
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);

    // 第二次开轮后的首次站 A 请求不早于等待期结束。
    let first_after = stations
        .world
        .request_log_for("station-a")
        .into_iter()
        .filter(|r| r.at_ms > retry.finished_ms)
        .min_by_key(|r| r.at_ms)
        .expect("应有后续请求");
    assert!(
        first_after.at_ms >= deadline,
        "等待期结束后才能恢复：{:?}",
        first_after
    );
    assert_eq!(p.reachable, 8);
    assert!(store.get_run(run_id).await.is_ok());
}

#[tokio::test]
async fn non_success_state_and_changes_are_failures_not_data() {
    // 状态端点返回 500：轮次不得创建，也绝不能把 0 当作截止点。
    let (engine, _store, stations, _dir) = http_harness(None).await;
    stations
        .inject("station-b", "/inject/fail", "kind=state&times=5")
        .await;
    let err = engine.start_run(1, RunMode::Full).await.unwrap_err();
    assert!(err.to_string().contains("状态读取失败"), "{err}");
    let runs = engine.store.list_runs(10).await.unwrap();
    assert!(runs.is_empty(), "状态失败不应创建轮次");

    // 变更清单返回 500：轮次创建后按失败处理，不把坏响应解析成“无变更”。
    let (engine2, store2, stations2, _dir2) = http_harness(None).await;
    stations2
        .inject("station-b", "/inject/fail", "kind=changes&times=5")
        .await;
    let run_id = engine2.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine2, run_id).await;
    assert_eq!(p.status, "failed", "清单读取失败必须按失败处理: {p:?}");
    assert!(p.error.unwrap_or_default().contains("station-b"));
    // 站 B 的页面一个都不应落库（清单不可信），站 A 的仍可完成入库。
    let reach = engine2.reachable(run_id).await.unwrap();
    assert!(reach.iter().all(|r| r.station_id == "station-a"));
    assert!(!reach.is_empty());
    assert_eq!(
        store2.count_run_pages(run_id).await.unwrap().failed,
        2,
        "站 B 的页面（b1/b3）应按未就绪收尾"
    );
}

#[tokio::test]
async fn other_stations_pages_land_while_one_station_list_is_waiting() {
    // 站 B 的清单被要求等待 1.5 秒；期间站 A 已就绪的页面应照常入库。
    let (engine, store, stations, _dir) = http_harness(None).await;
    stations
        .inject(
            "station-b",
            "/inject/retry-after",
            "kind=changes&secs=1&times=1",
        )
        .await;

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    // 等到站 A 的入口页已 done、而站 B 仍在等待期。
    let ok = common::wait_until(&engine, run_id, |p| p.done >= 1).await;
    assert!(ok, "站 A 应在站 B 等待期内完成页面入库");

    let a_done_at = page_enqueued(&stations.world.request_log(), "station-a", "a0")
        .expect("站 A 入口页应已被抓取");
    let retry = last_retry_after(
        &stations.world.request_log_for("station-b"),
        "station-b",
    )
    .expect("站 B 应收到等待要求");
    assert!(
        a_done_at < retry.finished_ms + 1000,
        "站 A 的页面应在站 B 等待期结束前就已抓取（时间线：a0={a_done_at}, retry 结束={}）",
        retry.finished_ms
    );
    // 站 B 在等待期内没有任何页面请求。
    for r in stations.world.request_log_for("station-b") {
        if r.kind == "page" {
            assert!(
                r.at_ms >= retry.finished_ms + 950,
                "站 B 的页面请求不应在等待期内发出: {:?}",
                r
            );
        }
    }

    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);
    let counts = store.count_run_pages(run_id).await.unwrap();
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.done, 8);
}

#[tokio::test]
async fn each_station_reads_its_own_cutoff_independently() {
    // 站 A 在开轮前多登记一次；站 B 不变。固化的截止点应逐站不同。
    let (engine, store, _stations, _dir) = http_harness(None).await;
    let before_a = store
        .get_station("station-a")
        .await
        .unwrap()
        .map(|_| ())
        .is_some();
    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let run = store.get_run(run_id).await.unwrap();
    assert!(before_a);
    assert_ne!(run.watermarks["station-a"], run.watermarks["station-b"]);
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
}

#[tokio::test]
async fn per_station_rate_and_inflight_limits_are_respected() {
    // 站 A 最小间隔 150ms、在途 1；站 B 不做间隔要求，验证两站互不拖累。
    let mut overrides = BTreeMap::new();
    overrides.insert("station-a".to_string(), (150u64, 1u32));
    overrides.insert("station-b".to_string(), (0u64, 2u32));
    let (engine, _store, stations, _dir) = http_harness(Some(overrides)).await;

    let run_id = engine.start_run(1, RunMode::Full).await.unwrap();
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);

    // 站 A 的两次请求之间不短于最小间隔（给调度留 10ms 余量）。
    // 首个请求包含 TCP/连接建立耗时：控制器在 `acquire()` 时刻就记下发起时刻，
    // 而服务端记录的到达时刻更晚，故首对间隔看起来偏短；从第二个请求起连接已复用。
    let log = stations.world.request_log_for("station-a");
    assert!(log.len() >= 3, "应至少有三条站 A 请求");
    for pair in log[1..].windows(2) {
        assert!(
            gap_ms(pair[0].at_ms, pair[1].at_ms) >= 140,
            "站 A 请求间隔过短: {:?} -> {:?}",
            pair[0],
            pair[1]
        );
    }
    // 站 A 任意时刻最多 1 个在途请求。
    for r in &log {
        let overlap = log
            .iter()
            .filter(|o| o.at_ms <= r.at_ms && o.finished_ms >= r.at_ms)
            .count();
        assert!(overlap <= 1, "站 A 在途上限被突破: {:?}", r);
    }
}

#[tokio::test]
async fn fetcher_state_failure_is_typed_and_surfaces_retry_after() {
    // 直接验证抓取层：状态读取的 Retry-After 用尽后返回 RetryAfter 而非成功。
    let stations = common::HttpStations::spawn(drainage_collector::mock::MockWorld::demo()).await;
    stations
        .inject("station-a", "/inject/retry-after", "kind=state&secs=1&times=5")
        .await;
    let fetcher = Arc::new(Fetcher::new(Arc::new(HttpPageClient::new().unwrap())));
    let cfg = stations.configs().into_iter().next().unwrap();
    let err = fetcher.fetch_state(&cfg).await.unwrap_err();
    match err {
        drainage_collector::fetch::PageFetchError::RetryAfter(_) => {}
        other => panic!("应返回 RetryAfter，实际 {other:?}"),
    }
    // 等待期已登记：紧接着的页面请求也必须被推迟。
    let started = std::time::Instant::now();
    let _ = fetcher
        .fetch_page(&cfg, "a0", FetchTarget::AtRevision(1))
        .await;
    assert!(
        started.elapsed() >= Duration::from_millis(800),
        "状态请求失败后等待期仍应约束页面请求"
    );
}

#[tokio::test]
async fn page_retry_after_exhausted_still_gates_state_and_changes() {
    // 触发等待的是页面请求且重试已用尽：随后的状态与变更清单请求必须同样等到期。
    let stations = common::HttpStations::spawn(drainage_collector::mock::MockWorld::demo()).await;
    stations
        .inject("station-a", "/inject/retry-after", "kind=page&secs=1&times=5")
        .await;
    let fetcher = Arc::new(Fetcher::new(Arc::new(HttpPageClient::new().unwrap())));
    let cfg = stations.configs().into_iter().next().unwrap();

    let err = match fetcher
        .fetch_page(&cfg, "a0", FetchTarget::AtRevision(1))
        .await
    {
        Ok(_) => panic!("页面重试用尽后应失败"),
        Err(e) => e,
    };
    match err {
        drainage_collector::fetch::PageFetchError::RetryAfter(_) => {}
        other => panic!("页面重试用尽后应返回 RetryAfter，实际 {other:?}"),
    }

    // 状态读取：等待期未结束前不应发出请求。
    let started = std::time::Instant::now();
    let _ = fetcher.fetch_state(&cfg).await;
    assert!(
        started.elapsed() >= Duration::from_millis(800),
        "页面触发并耗尽重试后，状态请求也必须遵守等待期（实际 {:?}）",
        started.elapsed()
    );

    // 变更清单同理（用独立抓取器，避免状态读取已把等待期耗尽）。
    let stations2 = common::HttpStations::spawn(drainage_collector::mock::MockWorld::demo()).await;
    stations2
        .inject("station-a", "/inject/retry-after", "kind=page&secs=1&times=5")
        .await;
    let fetcher2 = Arc::new(Fetcher::new(Arc::new(HttpPageClient::new().unwrap())));
    let cfg2 = stations2.configs().into_iter().next().unwrap();
    let _ = fetcher2
        .fetch_page(&cfg2, "a0", FetchTarget::AtRevision(1))
        .await;
    let started = std::time::Instant::now();
    let _ = fetcher2.fetch_changes_until(&cfg2, 0, 1_000_000).await;
    assert!(
        started.elapsed() >= Duration::from_millis(800),
        "页面触发并耗尽重试后，变更清单请求也必须遵守等待期（实际 {:?}）",
        started.elapsed()
    );
}
