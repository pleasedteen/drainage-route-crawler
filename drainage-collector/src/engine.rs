//! 采集引擎：轮次编排、截止点固化、全量/增量遍历、断点续跑。
//!
//! ## 同截止点一致性
//! 全量与增量走**同一套遍历代码**：从入流口按下游链接做去重遍历，
//! 页面目标修订统一由“轮次截止点 + 变更清单”解析。差别只在缓存：
//! - 全量：从入口重走，已缓存的同修订页面同样直接命中（跨入口共享抓取结果）；
//! - 增量：变更清单里未出现的页面保持旧修订，缓存命中更多，未修订页面不重复抓。
//! 因此到达相同截止点时，两种模式算出的可达关系逐页相同。
//!
//! ## 断点续跑
//! 所有进度都在确认落库后才推进：抓取结果、链接、可达关系、页面状态。
//! 进程退出后重启，`running` 轮次的 `fetching` 页面收回为 `pending`，
//! 工作线程从头消费队列即可接着完成，已完成页面不会重抓。
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::{Mutex, Notify};

use crate::fetch::{FetchTarget, Fetcher, PageFetchError, PauseToken};
use crate::model::*;
use crate::parse::parse_page;
use crate::store::Store;

pub struct Engine {
    pub store: Store,
    pub fetcher: Arc<Fetcher>,
    wake: Notify,
    /// 本进程内正在执行的轮次：run_id -> 暂停信号（含执行任务句柄）。
    /// 进程重启后这张表为空，但暂停状态在库中，重启不会自动续跑暂停轮次。
    active: Mutex<HashMap<i64, ActiveRun>>,
}

struct ActiveRun {
    pause: PauseToken,
    handle: tokio::task::JoinHandle<()>,
    /// 状态看门狗（发现“另一进程确认的暂停”）。
    watchdog: tokio::task::AbortHandle,
    /// 执行任务退出时置位并唤醒等待者（暂停/继续用）。
    finished: Arc<AtomicBool>,
    done: Arc<Notify>,
}

/// 内部信号：轮次被值班员确认暂停，不是失败。
#[derive(Debug)]
struct PausedRun;
impl std::fmt::Display for PausedRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("轮次已暂停")
    }
}
impl std::error::Error for PausedRun {}

/// 抓取层把暂停表达为 `anyhow` 错误（`PageFetchError::Paused`），
/// 这里还原为内部信号，使其不会被当作页面失败。
fn paused_or_err(e: anyhow::Error) -> anyhow::Error {
    match e.downcast_ref::<PageFetchError>() {
        Some(PageFetchError::Paused) => anyhow::Error::new(PausedRun),
        _ => e,
    }
}

/// 单页处理的结果。
enum ProcessOutcome {
    Processed { links: Vec<LinkRef>, revision: i64 },
    Gone,
}

impl Engine {
    pub fn new(store: Store, fetcher: Arc<Fetcher>) -> Arc<Self> {
        Arc::new(Self {
            store,
            fetcher,
            wake: Notify::new(),
            active: Mutex::new(HashMap::new()),
        })
    }

    /// 启动时调用：收回崩溃残留状态，续跑所有未完成且未暂停的轮次。
    /// 值班员确认暂停的轮次保持 `paused`，不自动续跑，等 `resume_run` 唤醒。
    pub async fn resume_on_boot(self: &Arc<Self>) -> Result<Vec<i64>> {
        self.store.reset_open_run_pages().await?;
        let mut open = Vec::new();
        for r in self.store.list_runs(100).await? {
            if r.status == RunStatus::Running {
                open.push(r.id);
            }
        }
        for id in &open {
            self.spawn_run(*id).await;
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
        // 同入口已有未走完的轮次（running 或 paused）则拒绝，避免重复采集。
        // 暂停轮次必须走“继续”复用原轮次，不能借新轮次绕过暂停。
        for r in self.store.list_runs(20).await? {
            if r.entry_id == entry_id && r.status.is_open() {
                return Err(anyhow!(
                    "入流口 {entry_id} 已有{}轮次 {}",
                    if r.status == RunStatus::Paused {
                        "已暂停的"
                    } else {
                        "进行中的"
                    },
                    r.id
                ));
            }
        }

        // 1) 轮次开始：逐站读取并固化登记截止点。
        let stations = self.store.list_stations().await?;
        let mut watermarks = BTreeMap::new();
        for st in &stations {
            let cutoff = self
                .fetcher
                .fetch_state(st, &PauseToken::new())
                .await
                .map_err(paused_or_err)?;
            watermarks.insert(st.id.clone(), cutoff);
        }
        let run_id = self.store.create_run(entry_id, mode, &watermarks).await?;
        tracing::info!(run_id, ?mode, entry_id, ?watermarks, "轮次已创建");

        // 2) 入口页面入队，随后异步执行。
        self.store
            .enqueue_run_page(run_id, &entry.station_id, &entry.page_key, 0)
            .await?;
        self.spawn_run(run_id).await;
        Ok(run_id)
    }

    /// 丢弃本进程中迟迟未退出的旧执行任务（超时兜底），以便新任务接管。
    async fn abort_stale_task(&self, run_id: i64) {
        let mut g = self.active.lock().await;
        if let Some(a) = g.remove(&run_id) {
            a.watchdog.abort();
            a.handle.abort();
        }
    }

    /// 为某个轮次创建暂停信号并启动执行任务；同一进程内同一轮次只会有一个任务。
    async fn spawn_run(self: &Arc<Self>, run_id: i64) {
        let mut active = self.active.lock().await;
        if active.contains_key(&run_id) {
            return;
        }
        let token = PauseToken::new();
        let finished = Arc::new(AtomicBool::new(false));
        let done = Arc::new(Notify::new());
        let eng = self.clone();
        let token_for_task = token.clone();
        let finished_task = finished.clone();
        let done_task = done.clone();
        // 状态看门狗：同进程暂停由 PauseToken 立即打断；若暂停由另一进程
        // （如 CLI）确认，则这里轮询库中状态，同样尽快让本进程停下来。
        let watchdog_token = token.clone();
        let watchdog_store = self.store.clone();
        let watchdog = tokio::spawn(async move {
            loop {
                if watchdog_token.is_paused() {
                    return;
                }
                if let Ok(Some(st)) = watchdog_store.run_status(run_id).await {
                    if st != RunStatus::Running {
                        if st == RunStatus::Paused {
                            watchdog_token.pause();
                        }
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
        let watchdog_abort = watchdog.abort_handle();
        let handle = tokio::spawn(async move {
            match eng.execute_run(run_id, token_for_task).await {
                Ok(()) => {}
                Err(e) if e.downcast_ref::<PausedRun>().is_some() => {
                    tracing::info!(run_id, "轮次已暂停，等待继续");
                }
                Err(e) => {
                    tracing::error!(run_id, error = %e, "轮次异常终止");
                    // 只把仍在运行的轮次记为失败，绝不覆盖已暂停状态。
                    if let Ok(cur) = eng.store.get_run(run_id).await {
                        if cur.status == RunStatus::Running {
                            let _ = eng
                                .store
                                .update_run_status(
                                    run_id,
                                    RunStatus::Failed,
                                    Some(&e.to_string()),
                                )
                                .await;
                        }
                    }
                }
            }
            watchdog.abort();
            finished_task.store(true, Ordering::SeqCst);
            done_task.notify_waiters();
            eng.active.lock().await.remove(&run_id);
        });
        active.insert(
            run_id,
            ActiveRun {
                pause: token,
                handle,
                watchdog: watchdog_abort,
                finished,
                done,
            },
        );
    }

    /// 等待某轮次在本进程内的执行任务退出（不跨越进程）。
    async fn wait_task_exit(&self, run_id: i64, timeout: std::time::Duration) {
        let waiter = {
            let g = self.active.lock().await;
            g.get(&run_id).map(|a| (a.finished.clone(), a.done.clone()))
        };
        let Some((finished, done)) = waiter else {
            return;
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // 先登记通知再查标志，避免错过“刚退出”的窗口。
            let notified = done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if finished.load(Ordering::SeqCst) {
                return;
            }
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(deadline) => return,
            }
        }
    }

    // ---------- 值班员暂停 / 继续 ----------

    /// 确认暂停某个轮次：轮次停在当前进度，不再发新请求，已核实结果不动。
    /// 站点正在退避或等待时同样立即生效。
    /// 只拦截这个轮次，其他入口/其他轮次的抓取照常按站点原节奏进行。
    pub async fn pause_run(self: &Arc<Self>, run_id: i64) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        match run.status {
            RunStatus::Paused => return Ok(()),
            RunStatus::Succeeded | RunStatus::Failed => {
                return Err(anyhow!(
                    "轮次 {run_id} 已结束（{}），无法暂停",
                    run.status
                ))
            }
            RunStatus::Running => {}
        }

        // 1) 先落库暂停状态：重启后仍是 paused，不会自动续跑。
        if !self.store.pause_run(run_id, now_millis()).await? {
            // 并发/竞争下可能刚好已结束，按当前状态回报。
            let now = self.store.get_run(run_id).await?;
            return match now.status {
                RunStatus::Paused => Ok(()),
                other => Err(anyhow!("轮次 {run_id} 已结束（{}），无法暂停", other)),
            };
        }

        // 2) 通知抓取层立即停止发起新请求；已确认的结果保持原样。
        // 3) 等执行任务把在途页面放回队列后退出，使“已确认暂停”的进度与
        //    请求计数都稳定可查。暂停由另一进程发起时本进程没有任务，立即返回。
        let token = self.active.lock().await.get(&run_id).map(|a| a.pause.clone());
        if let Some(t) = token {
            t.pause();
        }
        self.wait_task_exit(run_id, std::time::Duration::from_secs(5))
            .await;
        self.abort_stale_task(run_id).await;
        tracing::info!(run_id, "轮次已确认暂停");
        Ok(())
    }

    /// 继续已暂停的轮次：沿用原轮次 ID 与各站截止点，已核实结果保留。
    pub async fn resume_run(self: &Arc<Self>, run_id: i64) -> Result<Option<i64>> {
        let run = self.store.get_run(run_id).await?;
        if run.status != RunStatus::Paused {
            return Err(anyhow!(
                "轮次 {run_id} 当前状态为 {}，不是已暂停",
                run.status
            ));
        }
        // 同入口不允许在此刻另起轮次（另一个未走完的轮次）。
        for r in self.store.list_runs(20).await? {
            if r.id != run_id && r.entry_id == run.entry_id && r.status.is_open() {
                return Err(anyhow!(
                    "入流口 {} 已有另一个未走完的轮次 {}",
                    run.entry_id,
                    r.id
                ));
            }
        }
        // 本进程内若还有旧执行任务在退出，先等它收尾，避免两个任务消费同一队列。
        self.wait_task_exit(run_id, std::time::Duration::from_secs(5))
            .await;
        // 超时兜底：旧任务仍未退出就中止并移除，由新任务接管。
        self.abort_stale_task(run_id).await;
        if !self.store.resume_run(run_id).await? {
            return Err(anyhow!("轮次 {run_id} 状态已变化，未能继续"));
        }
        // 暂停时把在途页面放回了队列；进程被直接杀掉时可能残留 fetching，一并收回。
        self.store.reset_run_pages(run_id).await?;

        self.spawn_run(run_id).await;
        tracing::info!(run_id, "轮次已继续");
        Ok(Some(run_id))
    }

    /// 当前是否还有本进程内的执行任务（测试/演示用）。
    pub async fn is_executing(&self, run_id: i64) -> bool {
        self.active.lock().await.contains_key(&run_id)
    }

    /// 轮次主流程，可在进程重启后对同一 run_id 重复进入。
    /// 返回 `Err(RunPaused)` 表示值班员确认暂停，轮次停在当前进度。
    async fn execute_run(self: &Arc<Self>, run_id: i64, pause: PauseToken) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        // 若进入时已被确认暂停（重启/继续竞态），不读清单、不发请求。
        if run.status == RunStatus::Paused || pause.is_paused() {
            return Err(anyhow::Error::new(PausedRun));
        }
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
            let changes = self
                .fetcher
                .fetch_changes_until(st, after, cutoff, &pause)
                .await
                .map_err(paused_or_err)?;
            for c in changes {
                self.store.insert_change(&c).await?;
            }
            if pause.is_paused() {
                return Err(anyhow::Error::new(PausedRun));
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
            let token = pause.clone();
            handles.push(tokio::spawn(async move {
                eng.worker(run_id, ent, idx, smap, token).await
            }));
        }
        for h in handles {
            h.await??;
        }

        // 工作线程全部退出后若仍处于暂停，轮次就停在这里：
        // 不推进站点水位、不改轮次状态，留待“继续”复用原队列与原截止点。
        if pause.is_paused() || self.store.get_run(run_id).await?.status == RunStatus::Paused {
            tracing::info!(run_id, "轮次已停在当前进度（暂停）");
            return Err(anyhow::Error::new(PausedRun));
        }

        // 6) 收尾：推进站点水位，标记轮次结果。
        let counts = self.store.count_run_pages(run_id).await?;
        if counts.failed > 0 {
            let msg = format!("{} 个页面抓取失败，可达关系为部分结果", counts.failed);
            if !self
                .store
                .settle_run(run_id, RunStatus::Failed, Some(&msg))
                .await?
            {
                return Err(anyhow::Error::new(PausedRun));
            }
            return Err(anyhow!(msg));
        }
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            if cutoff > st.watermark_seq {
                self.store.set_station_watermark(&st.id, cutoff).await?;
            }
        }
        if !self
            .store
            .settle_run(run_id, RunStatus::Succeeded, None)
            .await?
        {
            // 与确认暂停同时发生：以暂停为准，轮次不标记成功。
            return Err(anyhow::Error::new(PausedRun));
        }
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

    #[allow(clippy::too_many_arguments)]
    async fn worker(
        self: Arc<Self>,
        run_id: i64,
        entry: EntryPoint,
        target_index: HashMap<(String, String), (i64, i64)>,
        station_by_id: HashMap<String, StationConfig>,
        pause: PauseToken,
    ) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        loop {
            // 已确认暂停：不再领取新页面，本 worker 收工。
            // 同时查库：暂停可能由另一个进程（如 CLI）确认。
            if pause.is_paused() || self.store.run_status(run_id).await? == Some(RunStatus::Paused) {
                return Err(anyhow::Error::new(PausedRun));
            }
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

            // 认领后、发请求前再次确认暂停，把页面原样放回队列（不计失败）。
            if pause.is_paused() {
                self.store
                    .requeue_run_page(run_id, &station_id, &page_key)
                    .await?;
                return Err(anyhow::Error::new(PausedRun));
            }

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
                .process_page(&run, &station, &page_key, depth, &target_index, &pause)
                .await;

            // 暂停向量可能在请求返回之后才置位：此时把本页放回 pending，
            // 已落库的修订/链接/设施是“已确认的抓取结果”，继续时可原样复用。
            if pause.is_paused() || matches!(&outcome, Err(e) if e.downcast_ref::<PausedRun>().is_some())
            {
                self.store
                    .requeue_run_page(run_id, &station_id, &page_key)
                    .await?;
                return Err(anyhow::Error::new(PausedRun));
            }

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

    /// 单页处理：缓存命中则零请求，否则抓取并持久化修订。
    async fn process_page(
        &self,
        run: &Run,
        station: &StationConfig,
        page_key: &str,
        _depth: i64,
        target_index: &HashMap<(String, String), (i64, i64)>,
        pause: &PauseToken,
    ) -> Result<ProcessOutcome> {
        // 已确认暂停：连缓存命中页也不再推进，整轮停在当前进度。
        if pause.is_paused() || self.store.run_status(run.id).await? == Some(RunStatus::Paused) {
            return Err(anyhow::Error::new(PausedRun));
        }
        let request = self.resolve_request(run, target_index, &station.id, page_key);

        // 1) 先查共享缓存：同页面同修订只抓一次。
        let cached = match request {
            FetchTarget::AtRevision(rev) => {
                self.store
                    .get_page_revision(&station.id, page_key, rev)
                    .await?
            }
            FetchTarget::AtSeq(_) => self
                .store
                .latest_revision(&station.id, page_key)
                .await?
                .map(|(_, p)| p),
        };

        let page_rev = if let Some(pr) = cached {
            pr
        } else {
            // 2) 缓存未命中，按站节奏抓取（退避/暂停在 Fetcher 内处理）。
            let fetched = match self
                .fetcher
                .fetch_page(station, page_key, request, pause)
                .await
            {
                Ok(f) => f,
                Err(PageFetchError::Gone) => return Ok(ProcessOutcome::Gone),
                Err(PageFetchError::Paused) => return Err(anyhow::Error::new(PausedRun)),
                Err(e) => return Err(anyhow!(e)),
            };
            let parsed = parse_page(station.format, &station.id, &fetched.body)?;
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
                .get_page_revision(&station.id, page_key, parsed.revision)
                .await?
                .ok_or_else(|| anyhow!("修订落库后读回失败"))?
        };

        // 3) 解析正文（缓存命中也要解析以取得下游链接）。
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
            paused_at: run.paused_at,
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
