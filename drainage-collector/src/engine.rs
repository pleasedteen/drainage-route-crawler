//! 采集引擎：轮次编排、截止点固化、全量/增量遍历、断点续跑、跨进程在途单飞。
//!
//! ## 同截止点一致性
//! 全量与增量走**同一套遍历代码**：从入流口按下游链接做去重遍历，
//! 页面目标修订统一由“轮次截止点 + 变更清单”解析。差别只在缓存：
//!
//! - 全量：从入口重走，已缓存的同修订页面同样直接命中（跨入口共享抓取结果）；
//! - 增量：变更清单里未出现的页面保持旧修订，缓存命中更多，未修订页面不重复抓。
//!
//! 因此到达相同截止点时，两种模式算出的可达关系逐页相同。
//!
//! ## 跨进程在途单飞与接手
//! 常驻服务与独立 CLI 共用同一个采集库，两者可能从不同入口同时碰到冷缓存。
//! 冷缓存的抓取目标 `(站点, 页面, 目标)` 对应 `fetch_leases` 表中的唯一租约：
//!
//! - 抢租成功者才经过 [`Fetcher`] 发实际请求，抓取期间周期性续租；
//! - 其他进程/轮次抢不到租约只轮询等待，**即使响应很慢也不会发起第二次请求**；
//! - 抓取结果确认落库后置 `done`，等待者直接读 `page_revisions` 共享；
//! - 持租进程退出且结果尚未落库时，心跳停摆、租约过期，等待者用一条条件
//!   UPDATE 原子接手；已 `done/gone` 的终态则继续共享，不重复抓。
//!
//! 实际请求仍统一走 Fetcher 的站点限速、在途闸门与退避重试；
//! 汇合与成环的去重进度仍按轮次维护在 `run_pages`，互不干扰。
//!
//! ## 断点续跑
//! 所有进度都在确认落库后才推进：抓取结果、链接、可达关系、页面状态。
//! 进程退出后重启，`running` 轮次的 `fetching` 页面收回为 `pending`，
//! 工作线程从头消费队列即可接着完成，已完成页面不会重抓。
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::Notify;

use crate::fetch::{FetchTarget, Fetcher, PageFetchError};
use crate::model::*;
use crate::parse::parse_page;
use crate::store::{FetchLease, LeaseAcquire, Store};

/// 在途租约有效期；持租者按更短周期续租，退出后最迟一个 TTL 被接手。
const LEASE_TTL_MS: u64 = 15_000;
/// 续租周期，约为 TTL 的三分之一，容忍少量抖动与调度延迟。
const LEASE_HEARTBEAT_MS: u64 = 5_000;
/// 等待者轮询租约终态的间隔。
const LEASE_POLL_MS: u64 = 100;
/// 等待安全上限：正常情况下持租者最迟一个 TTL 内会续上或被接手。
const LEASE_WAIT_LIMIT_MS: u64 = 120_000;
/// 终态租约在启动清理时的保留时长（1 天）。
const LEASE_RETENTION_MS: u64 = 24 * 60 * 60 * 1_000;
/// 抓取期间租约易主的可重试错误标记。
const LEASE_TAKEN_OVER_MARKER: &str = "__lease_taken_over__";

/// 判断是否为“租约在抓取期间易主”的可重试错误。
fn is_takeover_retry(e: &anyhow::Error) -> bool {
    e.to_string().contains(LEASE_TAKEN_OVER_MARKER)
}

fn lease_ttl_from_env() -> u64 {
    std::env::var("DRAINAGE_LEASE_TTL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v >= 500)
        .unwrap_or(LEASE_TTL_MS)
}

fn lease_heartbeat_from_env() -> u64 {
    std::env::var("DRAINAGE_LEASE_HEARTBEAT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v >= 100)
        .unwrap_or(LEASE_HEARTBEAT_MS)
}

/// 租约的目标键：按明确修订，或按登记截止点取版本。
#[derive(Debug, Clone, Copy)]
enum LeaseTarget {
    Revision(i64),
    Seq(i64),
}

impl LeaseTarget {
    fn kind(self) -> &'static str {
        match self {
            LeaseTarget::Revision(_) => "revision",
            LeaseTarget::Seq(_) => "seq",
        }
    }

    fn value(self) -> i64 {
        match self {
            LeaseTarget::Revision(v) | LeaseTarget::Seq(v) => v,
        }
    }
}

pub struct Engine {
    pub store: Store,
    pub fetcher: Arc<Fetcher>,
    wake: Notify,
    /// 本进程持有的抓取租约标识（pid + 本地自增序号）。
    owner_id: String,
    /// 模拟本进程崩溃：关闭后所有续租任务立即停止（测试用，正常流程不触发）。
    crash: Arc<Notify>,
    lease_ttl_ms: u64,
    lease_heartbeat_ms: u64,
}

/// 单页处理的结果。
enum ProcessOutcome {
    Processed { links: Vec<LinkRef>, revision: i64 },
    Gone,
}

/// 单飞等待租约终态的结果。
enum LeaseWait {
    Done,
    Gone,
    /// 持租者退出、租约过期，调用者应重新抢租并自己抓取。
    TakeOver,
}

impl Engine {
    pub fn new(store: Store, fetcher: Arc<Fetcher>) -> Arc<Self> {
        Self::with_lease_timing(
            store,
            fetcher,
            lease_ttl_from_env(),
            lease_heartbeat_from_env(),
        )
    }

    /// 显式指定租约 TTL 与心跳周期（测试与高可用部署调优用，避免依赖进程级环境变量）。
    pub fn with_lease_timing(
        store: Store,
        fetcher: Arc<Fetcher>,
        lease_ttl_ms: u64,
        lease_heartbeat_ms: u64,
    ) -> Arc<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        // 全局唯一持租者标识：同一进程内的多个 Engine（如测试中的“服务/CLI”双引擎）
        // 也能凭序号区分；真实多进程再叠加 pid 进一步隔离。
        let owner_id = format!(
            "pid-{}-eng-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        );
        Arc::new(Self {
            store,
            fetcher,
            wake: Notify::new(),
            owner_id,
            crash: Arc::new(Notify::new()),
            lease_ttl_ms: lease_ttl_ms.max(500),
            lease_heartbeat_ms: lease_heartbeat_ms.max(100),
        })
    }

    /// 测试支撑：模拟本进程死亡，所有在途租约的续租立即停止，
    /// 租约将在一个 TTL 后被其他等待者接手。
    pub fn simulate_crash(&self) {
        self.crash.notify_waiters();
    }

    /// 本引擎的持租者标识（测试与诊断用）。
    pub fn owner_id_str(&self) -> String {
        self.owner_id.clone()
    }

    /// 启动时调用：收回崩溃残留状态，续跑所有未完成轮次。
    pub async fn resume_on_boot(self: &Arc<Self>) -> Result<Vec<i64>> {
        self.store.reset_open_run_pages().await?;
        // 上个进程残留的过期在途租约立即变为可接手，不必干等 TTL。
        self.store.reset_expired_fetch_leases().await?;
        // 清理超过保留期的终态租约，控制租约表规模（不影响 page_revisions 权威缓存）。
        let pruned = self
            .store
            .prune_terminal_fetch_leases(LEASE_RETENTION_MS)
            .await?;
        if pruned > 0 {
            tracing::info!(pruned, "已清理过期终态抓取租约");
        }
        let open: Vec<i64> = self
            .store
            .list_runs(100)
            .await?
            .into_iter()
            .filter(|r| r.status == RunStatus::Running)
            .map(|r| r.id)
            .collect();
        for id in &open {
            let eng = self.clone();
            let run_id = *id;
            tokio::spawn(async move {
                if let Err(e) = eng.execute_run(run_id).await {
                    tracing::error!(run_id, error = %e, "轮次异常终止");
                    let _ = eng
                        .store
                        .update_run_status(run_id, RunStatus::Failed, Some(&e.to_string()))
                        .await;
                }
            });
        }
        Ok(open)
    }

    /// 启动新轮次。每轮开始时固化各站登记截止点。
    pub async fn start_run(self: &Arc<Self>, entry_id: i64, mode: RunMode) -> Result<i64> {
        let entry = self
            .store
            .get_entry(entry_id)
            .await?
            .ok_or_else(|| anyhow!("入流口不存在: {entry_id}"))?;
        // 同入口已有在跑轮次则拒绝，避免重复采集。
        for r in self.store.list_runs(20).await? {
            if r.entry_id == entry_id && r.status == RunStatus::Running {
                return Err(anyhow!("入流口 {entry_id} 已有进行中的轮次 {}", r.id));
            }
        }

        // 1) 轮次开始：逐站读取并固化登记截止点。
        let stations = self.store.list_stations().await?;
        let mut watermarks = BTreeMap::new();
        for st in &stations {
            let cutoff = self.fetcher.fetch_state(st).await?;
            watermarks.insert(st.id.clone(), cutoff);
        }
        let run_id = self.store.create_run(entry_id, mode, &watermarks).await?;
        tracing::info!(run_id, ?mode, entry_id, ?watermarks, "轮次已创建");

        // 2) 入口页面入队，随后异步执行。
        self.store
            .enqueue_run_page(run_id, &entry.station_id, &entry.page_key, 0)
            .await?;
        let eng = self.clone();
        tokio::spawn(async move {
            if let Err(e) = eng.execute_run(run_id).await {
                tracing::error!(run_id, error = %e, "轮次异常终止");
                let _ = eng
                    .store
                    .update_run_status(run_id, RunStatus::Failed, Some(&e.to_string()))
                    .await;
            }
        });
        Ok(run_id)
    }

    /// 轮次主流程，可在进程重启后对同一 run_id 重复进入。
    async fn execute_run(self: &Arc<Self>, run_id: i64) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        let entry = self
            .store
            .get_entry(run.entry_id)
            .await?
            .ok_or_else(|| anyhow!("入流口不存在"))?;
        let stations = self.store.list_stations().await?;
        let station_by_id: HashMap<String, StationConfig> =
            stations.iter().map(|s| (s.id.clone(), s.clone())).collect();

        // 3) 按固化截止点续读各站变更清单并落库（崩溃后重复执行幂等）。
        //    清单同时承担“业务时间可早于登记时间”的正确排序：一切以登记 seq 为准。
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            let after = self.max_stored_change_seq(&st.id).await?;
            let changes = self.fetcher.fetch_changes_until(st, after, cutoff).await?;
            for c in changes {
                self.store.insert_change(&c).await?;
            }
        }

        // 4) 构建页面目标修订索引：(station, page) -> 截止点内最新 (seq, revision)。
        let mut target_index: HashMap<(String, String), (i64, i64)> = HashMap::new();
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            for c in self.store.list_changes(&st.id, 0).await? {
                if c.seq > cutoff {
                    continue;
                }
                target_index
                    .entry((c.station_id.clone(), c.page_key.clone()))
                    .and_modify(|cur| {
                        if c.seq > cur.0 {
                            *cur = (c.seq, c.revision);
                        }
                    })
                    .or_insert((c.seq, c.revision));
            }
        }

        // 5) 工作线程消费页面队列。SQLite 下连接池为 1，写天然串行；
        //    抓取在锁外进行，各站节奏由 Fetcher 的独立闸门控制。
        let worker_count = stations
            .iter()
            .map(|s| s.max_inflight.max(1) as usize)
            .sum::<usize>()
            .clamp(1, 8);
        let mut handles = Vec::new();
        for _ in 0..worker_count {
            let eng = self.clone();
            let idx = target_index.clone();
            let smap = station_by_id.clone();
            let ent = entry.clone();
            handles.push(tokio::spawn(async move {
                eng.worker(run_id, ent, idx, smap).await
            }));
        }
        for h in handles {
            h.await??;
        }

        // 6) 收尾：推进站点水位，标记轮次结果。
        let counts = self.store.count_run_pages(run_id).await?;
        if counts.failed > 0 {
            let msg = format!("{} 个页面抓取失败，可达关系为部分结果", counts.failed);
            self.store
                .update_run_status(run_id, RunStatus::Failed, Some(&msg))
                .await?;
            return Err(anyhow!(msg));
        }
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            if cutoff > st.watermark_seq {
                self.store.set_station_watermark(&st.id, cutoff).await?;
            }
        }
        self.store
            .update_run_status(run_id, RunStatus::Succeeded, None)
            .await?;
        tracing::info!(
            run_id,
            done = counts.done,
            gone = counts.total - counts.done,
            "轮次完成"
        );
        Ok(())
    }

    async fn max_stored_change_seq(&self, station_id: &str) -> Result<i64> {
        // list_changes 按 seq 升序，取末尾即最大；条目量在本地规模下可接受。
        let all = self.store.list_changes(station_id, 0).await?;
        Ok(all.last().map(|c| c.seq).unwrap_or(0))
    }

    /// 解析某页面在本轮截止点下的抓取请求。
    fn resolve_request(
        &self,
        run: &Run,
        target_index: &HashMap<(String, String), (i64, i64)>,
        station_id: &str,
        page_key: &str,
    ) -> FetchTarget {
        match target_index.get(&(station_id.to_string(), page_key.to_string())) {
            Some((_, revision)) => FetchTarget::AtRevision(*revision),
            None => FetchTarget::AtSeq(*run.watermarks.get(station_id).unwrap_or(&0)),
        }
    }

    /// 抓取目标对应的租约键。
    fn lease_target(request: FetchTarget) -> LeaseTarget {
        match request {
            FetchTarget::AtRevision(rev) => LeaseTarget::Revision(rev),
            FetchTarget::AtSeq(seq) => LeaseTarget::Seq(seq),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn worker(
        self: Arc<Self>,
        run_id: i64,
        entry: EntryPoint,
        target_index: HashMap<(String, String), (i64, i64)>,
        station_by_id: HashMap<String, StationConfig>,
    ) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        loop {
            // 认领一个待处理页面。
            let claimed = self.store.claim_run_page(run_id).await?;
            let (station_id, page_key, depth, _attempts) = match claimed {
                Some(v) => v,
                None => {
                    let counts = self.store.count_run_pages(run_id).await?;
                    if counts.pending == 0 && counts.fetching == 0 {
                        return Ok(()); // 本 worker 收工
                    }
                    // 等其他 worker 入队新页面或释放任务。
                    let notified = self.wake.notified();
                    tokio::pin!(notified);
                    let timeout = tokio::time::sleep(std::time::Duration::from_millis(50));
                    tokio::select! {
                        _ = notified => {}
                        _ = timeout => {}
                    }
                    continue;
                }
            };

            let station = match station_by_id.get(&station_id) {
                Some(s) => s.clone(),
                None => {
                    self.store
                        .fail_run_page(run_id, &station_id, &page_key, "未知站点")
                        .await?;
                    continue;
                }
            };

            let outcome = self
                .process_page(&run, &station, &page_key, depth, &target_index)
                .await;

            match outcome {
                Ok(ProcessOutcome::Processed { links, revision }) => {
                    self.store
                        .complete_run_page(run_id, &station_id, &page_key)
                        .await?;
                    // 可达关系随确认随写，崩溃后无需重算。
                    self.store
                        .upsert_reachable(run_id, entry.id, &station_id, &page_key, revision, depth)
                        .await?;
                    // 下游入队；唯一约束负责汇合/成环去重。
                    for l in links {
                        if !station_by_id.contains_key(&l.station_id) {
                            tracing::warn!(%station_id, %page_key, to_station=%l.station_id, to_page=%l.page_key, "链接指向未配置站点，跳过");
                            continue;
                        }
                        self.store
                            .enqueue_run_page(run_id, &l.station_id, &l.page_key, depth + 1)
                            .await?;
                    }
                    self.wake.notify_waiters();
                }
                Ok(ProcessOutcome::Gone) => {
                    // 页面在截止点前已移除：记录终态，不计入可达设施。
                    self.store
                        .complete_run_page(run_id, &station_id, &page_key)
                        .await?;
                    self.wake.notify_waiters();
                }
                Err(e) => {
                    tracing::warn!(run_id, %station_id, %page_key, error = %e, "页面抓取失败");
                    self.store
                        .fail_run_page(run_id, &station_id, &page_key, &e.to_string())
                        .await?;
                    self.wake.notify_waiters();
                }
            }
        }
    }

    /// 单页处理：共享缓存命中则零请求；冷缓存则经跨进程租约保证同一目标只抓一次。
    async fn process_page(
        &self,
        run: &Run,
        station: &StationConfig,
        page_key: &str,
        _depth: i64,
        target_index: &HashMap<(String, String), (i64, i64)>,
    ) -> Result<ProcessOutcome> {
        let request = self.resolve_request(run, target_index, &station.id, page_key);
        let target = Self::lease_target(request);

        // 1) 先查共享缓存：同页面同修订只抓一次（含其他进程刚落库的结果）。
        if let Some(pr) = self.cached_revision(station, page_key, request).await? {
            return self.assemble_from_cached(station, page_key, pr).await;
        }

        // 2) 冷缓存：通过 fetch_leases 在跨进程范围内单飞。
        let mut wait_deadline: Option<std::time::Instant> = None;
        // 持租期间租约被接管的重试上限（正常仅在严重卡顿/崩溃模拟下发生）。
        let mut takeover_retries = 0u32;
        loop {
            match self
                .store
                .acquire_fetch_lease(
                    &station.id,
                    page_key,
                    target.kind(),
                    target.value(),
                    &self.owner_id,
                    self.lease_ttl_ms,
                )
                .await?
            {
                LeaseAcquire::Completed(lease) => {
                    return self
                        .outcome_from_terminal_lease(station, page_key, request, lease)
                        .await;
                }
                LeaseAcquire::Held(lease) => {
                    match self
                        .fetch_under_lease(station, page_key, request, lease)
                        .await
                    {
                        Ok(outcome) => return Ok(outcome),
                        Err(e) if is_takeover_retry(&e) => {
                            takeover_retries += 1;
                            if takeover_retries > 5 {
                                return Err(e);
                            }
                            // 租约在抓取期间易主：回到抢租循环等待或接手，不发重复结果。
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
                LeaseAcquire::Waiting(_) => {
                    if wait_deadline.is_none() {
                        wait_deadline = Some(
                            std::time::Instant::now() + Duration::from_millis(LEASE_WAIT_LIMIT_MS),
                        );
                    }
                    match self
                        .wait_for_lease(station, page_key, target, wait_deadline.unwrap())
                        .await?
                    {
                        LeaseWait::TakeOver => continue, // 持租者已死，重新抢租接手
                        LeaseWait::Gone => return Ok(ProcessOutcome::Gone),
                        LeaseWait::Done => {
                            if let Some(pr) =
                                self.cached_revision(station, page_key, request).await?
                            {
                                return self.assemble_from_cached(station, page_key, pr).await;
                            }
                            // 终态已置 done 但缓存暂未读到：轮询重读，不自行抓取。
                            if std::time::Instant::now() > wait_deadline.unwrap() {
                                return Err(anyhow!("等待抓取结果落库超时"));
                            }
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(LEASE_POLL_MS)).await;
        }
    }

    /// 读共享缓存：明确修订取指定版本；按 seq 取最新已缓存版本。
    async fn cached_revision(
        &self,
        station: &StationConfig,
        page_key: &str,
        request: FetchTarget,
    ) -> Result<Option<PageRevision>> {
        match request {
            FetchTarget::AtRevision(rev) => {
                self.store
                    .get_page_revision(&station.id, page_key, rev)
                    .await
            }
            FetchTarget::AtSeq(_) => Ok(self
                .store
                .latest_revision(&station.id, page_key)
                .await?
                .map(|(_, p)| p)),
        }
    }

    /// 缓存/终态条目组装处理结果，并幂等补齐链接与设施信息。
    async fn assemble_from_cached(
        &self,
        station: &StationConfig,
        page_key: &str,
        page_rev: PageRevision,
    ) -> Result<ProcessOutcome> {
        let parsed = parse_page(station.format, &station.id, &page_rev.body)?;
        if parsed.revision != page_rev.revision {
            return Err(anyhow!(
                "页面自报修订 {} 与缓存修订 {} 不一致",
                parsed.revision,
                page_rev.revision
            ));
        }
        // 缓存条目由本进程或历史轮次写入，链接/设施信息幂等补齐。
        self.store
            .upsert_facility(&station.id, page_key, page_rev.revision, &parsed.facility)
            .await?;
        self.store
            .replace_links(&station.id, page_key, page_rev.revision, &parsed.links)
            .await?;
        Ok(ProcessOutcome::Processed {
            links: parsed.links,
            revision: page_rev.revision,
        })
    }

    /// 依据终态租约决定页面结果：done 读回缓存，gone 直接返回。
    async fn outcome_from_terminal_lease(
        &self,
        station: &StationConfig,
        page_key: &str,
        request: FetchTarget,
        lease: FetchLease,
    ) -> Result<ProcessOutcome> {
        if lease.state == "gone" {
            return Ok(ProcessOutcome::Gone);
        }
        // 优先用租约记录的实际修订精确读回；seq 类目标回退到最新缓存。
        let page_rev = if let Some(rev) = lease.actual_revision {
            self.store
                .get_page_revision(&station.id, page_key, rev)
                .await?
        } else {
            self.cached_revision(station, page_key, request).await?
        }
        .ok_or_else(|| anyhow!("租约已完成但修订缓存缺失"))?;
        self.assemble_from_cached(station, page_key, page_rev).await
    }

    /// 轮询等待其他持租者：终态即共享，租约过期则接手。
    async fn wait_for_lease(
        &self,
        station: &StationConfig,
        page_key: &str,
        target: LeaseTarget,
        deadline: std::time::Instant,
    ) -> Result<LeaseWait> {
        loop {
            let lease = self
                .store
                .get_fetch_lease(&station.id, page_key, target.kind(), target.value())
                .await?
                .ok_or_else(|| anyhow!("等待中的租约消失"))?;
            match lease.state.as_str() {
                "done" => return Ok(LeaseWait::Done),
                "gone" => return Ok(LeaseWait::Gone),
                _ if lease.expires_at <= crate::model::now_millis() => {
                    // 持租进程退出且未发布终态：回到抢租流程原子接手。
                    return Ok(LeaseWait::TakeOver);
                }
                _ if std::time::Instant::now() > deadline => {
                    return Err(anyhow!("等待在途抓取超时（租约未终态也未过期）"));
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_millis(LEASE_POLL_MS)).await;
        }
    }

    /// 持有租约完成一次抓取：续租 → 经 Fetcher 实际请求 → 落库并发布终态。
    ///
    /// 实际请求仍走 Fetcher 的站点限速、在途闸门与退避重试，本层不绕过。
    async fn fetch_under_lease(
        &self,
        station: &StationConfig,
        page_key: &str,
        request: FetchTarget,
        lease: FetchLease,
    ) -> Result<ProcessOutcome> {
        // 后台续租；心跳失败（租约被接手）或本进程崩溃模拟时立即停止。
        let heartbeat = self.spawn_heartbeat(lease.id);

        let fetched = self.fetcher.fetch_page(station, page_key, request).await;
        // 拿到响应后先确认租约仍属于自己，避免向已被接手的租约发布。
        let still_ours = self
            .store
            .heartbeat_fetch_lease(lease.id, &self.owner_id, self.lease_ttl_ms)
            .await?;
        heartbeat.abort();

        match fetched {
            Ok(fetched) if still_ours => {
                let parsed = parse_page(station.format, &station.id, &fetched.body)?;
                // 确认后立即落库；唯一约束保证并发/重入幂等。
                self.store
                    .insert_page_revision(
                        &station.id,
                        page_key,
                        parsed.revision,
                        &fetched.content_type,
                        &fetched.body,
                    )
                    .await?;
                self.store
                    .upsert_facility(&station.id, page_key, parsed.revision, &parsed.facility)
                    .await?;
                self.store
                    .replace_links(&station.id, page_key, parsed.revision, &parsed.links)
                    .await?;
                self.store
                    .complete_fetch_lease(lease.id, &self.owner_id, parsed.revision)
                    .await?;
                let pr = self
                    .store
                    .get_page_revision(&station.id, page_key, parsed.revision)
                    .await?
                    .ok_or_else(|| anyhow!("修订落库后读回失败"))?;
                self.assemble_from_cached(station, page_key, pr).await
            }
            Err(PageFetchError::Gone) if still_ours => {
                self.store
                    .mark_gone_fetch_lease(lease.id, &self.owner_id)
                    .await?;
                Ok(ProcessOutcome::Gone)
            }
            Err(e) => {
                // 可重试失败：主动放弃租约让等待者立即接手（也可由本进程下轮重试）；
                // 若租约已易主则什么都不做，由新持租者负责。
                if still_ours {
                    self.store
                        .release_fetch_lease(lease.id, &self.owner_id)
                        .await?;
                }
                Err(anyhow!(e))
            }
            Ok(_) => {
                // 租约已被接手：丢弃本次响应，回到抢租循环等待/接手，不重复发布。
                self.store
                    .release_fetch_lease(lease.id, &self.owner_id)
                    .await?;
                Err(anyhow!("{LEASE_TAKEN_OVER_MARKER}"))
            }
        }
    }

    /// 启动续租任务，返回其句柄用于抓取结束后中止。
    fn spawn_heartbeat(&self, lease_id: i64) -> tokio::task::AbortHandle {
        let store = self.store.clone();
        let owner = self.owner_id.clone();
        let ttl = self.lease_ttl_ms;
        let interval = self.lease_heartbeat_ms;
        let crash = self.crash.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(interval)) => {}
                    _ = crash.notified() => return,
                }
                match store.heartbeat_fetch_lease(lease_id, &owner, ttl).await {
                    Ok(true) => {}
                    // 租约被接手或已终态：续租无意义，停止。
                    _ => return,
                }
            }
        })
        .abort_handle()
    }

    // ---------- 查询 ----------

    pub async fn progress(&self, run_id: i64) -> Result<RunProgress> {
        let run = self.store.get_run(run_id).await?;
        let counts = self.store.count_run_pages(run_id).await?;
        let reachable = self.store.count_reachable(run_id).await?;
        Ok(RunProgress {
            run_id,
            status: run.status.as_str().to_string(),
            mode: run.mode.as_str().to_string(),
            entry_id: run.entry_id,
            watermarks: run.watermarks,
            total: counts.total,
            done: counts.done,
            pending: counts.pending,
            fetching: counts.fetching,
            failed: counts.failed,
            reachable,
            error: run.error,
            created_at: run.created_at,
            updated_at: run.updated_at,
        })
    }

    pub async fn reachable(&self, run_id: i64) -> Result<Vec<ReachableFacility>> {
        // 确认轮次存在。
        let _ = self.store.get_run(run_id).await?;
        self.store.list_reachable(run_id).await
    }

    /// 某入口当前已核实可达设施（取最近成功轮次）。
    pub async fn entry_reachable(
        &self,
        entry_id: i64,
    ) -> Result<Option<(i64, Vec<ReachableFacility>)>> {
        self.store.latest_reachable_for_entry(entry_id).await
    }

    pub async fn reachable_keys_for_equivalence(
        &self,
        run_id: i64,
    ) -> Result<HashSet<(String, String, i64)>> {
        Ok(self
            .store
            .list_reachable(run_id)
            .await?
            .into_iter()
            .map(|r| (r.station_id, r.page_key, r.revision.unwrap_or(-1)))
            .collect())
    }
}
