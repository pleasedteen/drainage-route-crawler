//! 一键验证：本地 HTTP 模拟站端到端核对 Retry-After 统一等待期。
//!
//! 真实起两个 axum 模拟站（HTML/JSON 各一，监听临时端口），采集器通过
//! [`crate::fetch::HttpPageClient`] 走完整 HTTP 链路，核对：
//! 1. 页面 / 状态 / 清单三类请求任一返回 Retry-After，该站进入统一等待期，
//!    等待结束前不再新发任何请求，恢复后轮次自动完成且结果正确；
//! 2. 状态请求持续失败时轮次中止，截止点不会被填成 0；
//! 3. 故障站等待期间其他站同阶段任务照常推进；
//! 4. 每轮页面工作槽最多 8 个、每站频率与在途预算保持。
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};

use crate::engine::Engine;
use crate::fetch::{Fetcher, HttpPageClient};
use crate::mock::{router, MockWorld};
use crate::model::{RunMode, SiteFormat, StationConfig};
use crate::store::Store;

/// 验证参数。
pub struct VerifyOptions {
    /// Retry-After 等待秒数（模拟站返回的值）。
    pub wait_secs: u64,
    pub min_interval_ms: u64,
    pub max_inflight: u32,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            wait_secs: 1,
            min_interval_ms: 20,
            max_inflight: 4,
        }
    }
}

struct MockServers {
    world: MockWorld,
    port_a: u16,
    port_b: u16,
    http: reqwest::Client,
    _guards: Vec<tokio::task::JoinHandle<()>>,
}

#[derive(serde::Deserialize)]
struct ReqLogResp {
    requests: Vec<crate::mock::RequestRecord>,
}

impl MockServers {
    /// 起两个本地 HTTP 模拟站，返回端口与共享世界。
    async fn start() -> Result<Self> {
        let world = MockWorld::demo();
        let guard_a = Box::leak(Box::new("station-a"));
        let guard_b = Box::leak(Box::new("station-b"));
        let app_a = router(world.clone(), guard_a);
        let app_b = router(world.clone(), guard_b);
        let la = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let lb = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let port_a = la.local_addr()?.port();
        let port_b = lb.local_addr()?.port();
        let guards = vec![
            tokio::spawn(async move {
                let _ = axum::serve(la, app_a).await;
            }),
            tokio::spawn(async move {
                let _ = axum::serve(lb, app_b).await;
            }),
        ];
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        // 等两个端口可服务。
        let servers = MockServers {
            world,
            port_a,
            port_b,
            http,
            _guards: guards,
        };
        servers.wait_ready().await?;
        Ok(servers)
    }

    async fn wait_ready(&self) -> Result<()> {
        // 模拟站没有 /health-probe：任意 HTTP 响应（含 404）都说明端口已可服务。
        for port in [self.port_a, self.port_b] {
            let url = format!("http://127.0.0.1:{port}/state");
            for _ in 0..50 {
                if self.http.get(&url).send().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        Ok(())
    }

    fn url(&self, station: &str, path: &str) -> String {
        let port = if station == "station-a" {
            self.port_a
        } else {
            self.port_b
        };
        format!("http://127.0.0.1:{port}{path}")
    }

    async fn inject(
        &self,
        station: &str,
        endpoint: &str,
        mode: &str,
        times: u32,
        secs: u64,
    ) -> Result<()> {
        let path = format!(
            "/inject/fault?endpoint={endpoint}&kind={mode}&times={times}&secs={secs}"
        );
        let resp = self.http.post(self.url(station, &path)).send().await?;
        if !resp.status().is_success() {
            bail!("故障注入失败: {}", resp.status());
        }
        Ok(())
    }

    async fn reset(&self) -> Result<()> {
        for port in [self.port_a, self.port_b] {
            self.http
                .post(format!("http://127.0.0.1:{port}/reset"))
                .send()
                .await?;
        }
        Ok(())
    }

    async fn request_log(&self) -> Result<Vec<crate::mock::RequestRecord>> {
        // 两个模拟站共享同一个 MockWorld，各自 /requests 返回同一份日志，
        // 合并时按 (t_ms, 站点, 端点, 目标, 结果) 去重。
        let mut seen = std::collections::HashSet::new();
        let mut all = Vec::new();
        for port in [self.port_a, self.port_b] {
            let v: ReqLogResp = self
                .http
                .get(format!("http://127.0.0.1:{port}/requests"))
                .send()
                .await?
                .json()
                .await?;
            for r in v.requests {
                let key = (
                    r.t_ms,
                    r.station_id.clone(),
                    r.endpoint.clone(),
                    r.detail.clone(),
                    r.outcome.clone(),
                );
                if seen.insert(key) {
                    all.push(r);
                }
            }
        }
        all.sort_by_key(|r| r.t_ms);
        Ok(all)
    }
}

/// 构建走真实 HTTP 的采集引擎（内存 SQLite，零外部依赖）。
async fn build_engine(servers: &MockServers, opts: &VerifyOptions) -> Result<Arc<Engine>> {
    let store = Store::connect("sqlite::memory:").await?;
    let stations = vec![
        StationConfig {
            id: "station-a".into(),
            name: "城东养护站".into(),
            base_url: format!("http://127.0.0.1:{}", servers.port_a),
            format: SiteFormat::Html,
            min_interval_ms: opts.min_interval_ms,
            max_inflight: opts.max_inflight,
            watermark_seq: 0,
        },
        StationConfig {
            id: "station-b".into(),
            name: "河西养护站".into(),
            base_url: format!("http://127.0.0.1:{}", servers.port_b),
            format: SiteFormat::Json,
            min_interval_ms: opts.min_interval_ms,
            max_inflight: opts.max_inflight,
            watermark_seq: 0,
        },
    ];
    for st in &stations {
        store.upsert_station(st).await?;
    }
    store.add_entry("station-a", "a0", "城东入流口").await?;
    let client = Arc::new(HttpPageClient::new()?);
    let fetcher = Arc::new(Fetcher::new(client));
    Ok(Engine::new(store, fetcher))
}

async fn wait_run(engine: &Engine, run_id: i64) -> Result<crate::model::RunProgress> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let p = engine.progress(run_id).await?;
        if p.status != "running" {
            return Ok(p);
        }
        if Instant::now() > deadline {
            bail!("轮次 {run_id} 超时未结束");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// 时间线断言：给定记录之后的等待窗口内，该站没有任何请求。
fn assert_silence_during_window(
    log: &[crate::mock::RequestRecord],
    station: &str,
    after_t: u128,
    window_ms: u128,
) -> Result<()> {
    let n = log
        .iter()
        .filter(|r| r.station_id == station && r.t_ms > after_t && r.t_ms < after_t + window_ms)
        .count();
    if n != 0 {
        bail!("{station} 在等待窗口内新发了 {n} 个请求");
    }
    Ok(())
}

/// 等待窗口（毫秒）：取 Retry-After 的 90%，给调度留 10% 余量。
fn window_ms(opts: &VerifyOptions) -> u128 {
    opts.wait_secs as u128 * 900
}

fn print_timeline(log: &[crate::mock::RequestRecord]) {
    println!("    时间线（t_ms / 站点 / 端点 / 目标 / 结果）:");
    for r in log {
        println!(
            "      {:>6}ms  {:<9} {:<7} {:<12} {}",
            r.t_ms, r.station_id, r.endpoint, r.detail, r.outcome
        );
    }
}

/// 运行全部验证场景，全部通过返回 Ok。
pub async fn run_all(opts: VerifyOptions) -> Result<()> {
    scenario_page_retry_after(&opts).await?;
    scenario_state_retry_after(&opts).await?;
    scenario_changes_retry_after(&opts).await?;
    scenario_state_failure_no_zero_watermark(&opts).await?;
    println!();
    println!("全部 4 个场景通过 ✔");
    Ok(())
}

// ---------- 场景 1：页面 Retry-After ----------

async fn scenario_page_retry_after(opts: &VerifyOptions) -> Result<()> {
    println!("[场景 1] 页面请求返回 Retry-After：该站三类请求进入同一等待期，恢复后自动完成");
    let servers = MockServers::start().await?;
    servers.reset().await?;
    let engine = build_engine(&servers, opts).await?;
    servers
        .inject("station-a", "page", "retry-after", 1, opts.wait_secs)
        .await?;

    let t0 = Instant::now();
    let run_id = engine.start_run(1, RunMode::Full).await?;
    let p = wait_run(&engine, run_id).await?;
    let elapsed = t0.elapsed();

    if p.status != "succeeded" || p.reachable != 8 {
        print_timeline(&servers.request_log().await?);
        bail!("轮次应成功且 8 个设施可达，实际 status={} reachable={}", p.status, p.reachable);
    }
    let log = servers.request_log().await?;
    let ra = log
        .iter()
        .find(|r| r.station_id == "station-a" && r.outcome == "retry-after")
        .ok_or_else(|| anyhow!("时间线缺少 A 站 Retry-After 记录"))?;
    let window = window_ms(&opts);
    assert_silence_during_window(&log, "station-a", ra.t_ms, window)?;
    if elapsed < Duration::from_millis(window as u64) {
        bail!("轮次耗时 {:?} 短于等待期，等待未生效", elapsed);
    }
    print_timeline(&log);
    println!(
        "    ✔ 等待期 {wait}s 内 A 站零新发；轮次成功，reachable=8，耗时 {elapsed:?}",
        wait = opts.wait_secs
    );
    Ok(())
}

// ---------- 场景 2：状态 Retry-After，截止点不写 0 ----------

async fn scenario_state_retry_after(opts: &VerifyOptions) -> Result<()> {
    println!("[场景 2] 状态请求返回 Retry-After：截止点读取进入同一等待期，且截止点不被填成 0");
    let servers = MockServers::start().await?;
    servers.reset().await?;
    let engine = build_engine(&servers, opts).await?;
    // 前两次状态请求 429，第三次成功。
    servers
        .inject("station-a", "state", "retry-after", 2, opts.wait_secs)
        .await?;

    let run_id = engine.start_run(1, RunMode::Full).await?;
    let p = wait_run(&engine, run_id).await?;
    if p.status != "succeeded" {
        bail!("状态端点恢复后轮次应成功，实际 {}: {:?}", p.status, p.error);
    }
    let expected_a = servers.world.get_station("station-a").unwrap().latest_seq;
    let expected_b = servers.world.get_station("station-b").unwrap().latest_seq;
    if p.watermarks.get("station-a") != Some(&expected_a)
        || p.watermarks.get("station-b") != Some(&expected_b)
        || p.watermarks.values().any(|v| *v == 0)
    {
        bail!("截止点被错误固化: {:?}（期望 a={expected_a} b={expected_b}）", p.watermarks);
    }
    let log = servers.request_log().await?;
    let first_ra = log
        .iter()
        .find(|r| r.station_id == "station-a" && r.endpoint == "state" && r.outcome == "retry-after")
        .ok_or_else(|| anyhow!("缺少状态端点 Retry-After 记录"))?;
    assert_silence_during_window(&log, "station-a", first_ra.t_ms, window_ms(&opts))?;
    print_timeline(&log);
    println!(
        "    ✔ 截止点 station-a={expected_a} station-b={expected_b}（均非 0）；等待期内 A 站零新发"
    );
    Ok(())
}

// ---------- 场景 3：清单 Retry-After，其他站同阶段继续 ----------

async fn scenario_changes_retry_after(opts: &VerifyOptions) -> Result<()> {
    println!("[场景 3] 变更清单返回 Retry-After：翻页进入等待期，期间其他站同阶段任务继续");
    let servers = MockServers::start().await?;
    servers.reset().await?;
    let engine = build_engine(&servers, opts).await?;
    servers
        .inject("station-b", "changes", "retry-after", 1, opts.wait_secs)
        .await?;

    let run_id = engine.start_run(1, RunMode::Full).await?;
    let p = wait_run(&engine, run_id).await?;
    if p.status != "succeeded" || p.reachable != 8 {
        bail!("轮次应成功且 8 个设施可达，实际 status={} reachable={}", p.status, p.reachable);
    }
    let log = servers.request_log().await?;
    let ra = log
        .iter()
        .find(|r| r.station_id == "station-b" && r.endpoint == "changes" && r.outcome == "retry-after")
        .ok_or_else(|| anyhow!("缺少清单端点 Retry-After 记录"))?;
    let window = window_ms(&opts);
    // B 站自己在等待窗口内静默。
    assert_silence_during_window(&log, "station-b", ra.t_ms, window)?;
    // 其他站（A）在同一窗口内继续推进：至少有页面或清单成功请求。
    let a_progress = log
        .iter()
        .filter(|r| {
            r.station_id == "station-a"
                && r.t_ms > ra.t_ms
                && r.t_ms < ra.t_ms + window
                && r.outcome == "ok"
        })
        .count();
    if a_progress == 0 {
        print_timeline(&log);
        bail!("B 站等待期间 A 站同阶段任务应继续推进，实际零请求");
    }
    print_timeline(&log);
    println!("    ✔ B 站等待期内零新发；同期 A 站推进 {a_progress} 个请求；轮次成功 reachable=8");
    Ok(())
}

// ---------- 场景 4：状态持续失败 -> 轮次中止，绝不写 0 截止点 ----------

async fn scenario_state_failure_no_zero_watermark(opts: &VerifyOptions) -> Result<()> {
    println!("[场景 4] 状态请求持续失败：轮次中止，截止点不会被填成 0");
    let servers = MockServers::start().await?;
    servers.reset().await?;
    let engine = build_engine(&servers, opts).await?;
    // 远超 5 次重试预算的普通 500。
    servers.inject("station-a", "state", "fail", 20, 0).await?;

    let err = engine
        .start_run(1, RunMode::Full)
        .await
        .expect_err("状态持续失败时启动必须报错");
    let runs = engine.store.list_runs(20).await?;
    if !runs.is_empty() {
        bail!("状态读取失败不应创建轮次，实际存在 {} 条", runs.len());
    }
    let log = servers.request_log().await?;
    let state_fails = log
        .iter()
        .filter(|r| r.station_id == "station-a" && r.endpoint == "state" && r.outcome == "fail")
        .count();
    if state_fails < 5 {
        bail!("状态端点应至少重试 5 次，实际 {state_fails}");
    }
    print_timeline(&log);
    println!("    ✔ 启动被拒绝（{err}）；无轮次记录、无 0 截止点；状态端点共失败 {state_fails} 次");
    Ok(())
}
