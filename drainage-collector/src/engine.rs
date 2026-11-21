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
//!
//! ## 逐站就绪
//! 轮次开始固化的截止点彼此独立：某站的变更清单仍在等待（退避 / 等待期）
//! 或该站页面积压时，其他站已就绪的页面照常认领、抓取、落库。工作线程
//! 只会从“清单已读完且本站不在等待期”的站点里认领页面。
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::sync::Notify;

use crate::fetch::{FetchTarget, Fetcher, PageFetchError};
use crate::model::*;
use crate::parse::parse_page;
use crate::store::Store;

pub struct Engine {
    pub store: Store,
    pub fetcher: Arc<Fetcher>,
    wake: Notify,
}

/// 单页处理的结果。
enum ProcessOutcome {
    Processed { links: Vec<LinkRef>, revision: i64 },
    Gone,
}

/// 轮次内的逐站就绪状态，供变更清单读取任务与工作线程共享。
///
/// 页面只有在“本站变更清单已续读到截止点”后才可认领：目标修订索引在该刻
/// 已完整，不会拿半截索引去解析页面。各站彼此独立，先就绪的站先开工。
#[derive(Default)]
struct RunState {
    /// 清单已读完的站点。
    ready: HashSet<String>,
    /// 清单最终读取失败的站点及原因。
    failed: HashMap<String, String>,
    /// (station, page) -> (seq, revision)，由各站就绪时写入；只写本站条目。
    targets: HashMap<(String, String), (i64, i64)>,
    /// 已结束（成功或失败）的清单读取任务数。
    preparers_done: usize,
    /// 清单读取任务总数。
    preparers_total: usize,
}

impl Engine {
    pub fn new(store: Store, fetcher: Arc<Fetcher>) -> Arc<Self> {
        Arc::new(Self {
            store,
            fetcher,
            wake: Notify::new(),
        })
    }

    /// 启动时调用：收回崩溃残留状态，续跑所有未完成轮次。
    pub async fn resume_on_boot(self: &Arc<Self>) -> Result<Vec<i64>> {
        self.store.reset_open_run_pages().await?;
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

        // 1) 轮次开始：逐站读取并固化登记截止点。各站并发读取、互不等待，
        //    因此某站仍在等待期不会推迟其他站的截止点；任一站读取失败（含
        //    非成功响应、Retry-After 用尽）则不创建轮次，交由值守端重试。
        let stations = self.store.list_stations().await?;
        let mut reads = Vec::new();
        for st in &stations {
            let fetcher = self.fetcher.clone();
            let stc = st.clone();
            reads.push(tokio::spawn(async move { fetcher.fetch_state(&stc).await }));
        }
        // 先收齐所有读取结果，避免任一失败时遗留仍在后台发请求的任务。
        let mut results = Vec::with_capacity(reads.len());
        for read in reads {
            results.push(read.await?);
        }
        let mut watermarks = BTreeMap::new();
        for (st, result) in stations.iter().zip(results) {
            let cutoff = result
                .map_err(|e| anyhow!("站点 {} 状态读取失败（未创建轮次）：{e}", st.id))?;
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

        // 3) 逐站续读变更清单到**该站自己的**截止点。各站并发读取、互不等待：
        //    某站清单在退避或等待期时，其他站先读完先就绪。读取失败（含非成功
        //    响应、Retry-After 用尽）的站点不会就绪，其页面也不会被认领，
        //    本轮最终按失败处理。
        let state = Arc::new(Mutex::new(RunState {
            preparers_total: stations.len(),
            ..Default::default()
        }));
        for st in &stations {
            let eng = self.clone();
            let run = run.clone();
            let st = st.clone();
            let state = state.clone();
            tokio::spawn(async move {
                eng.prepare_station(&run, &st, &state).await;
                eng.wake.notify_waiters();
            });
        }

        // 4) 工作线程消费页面队列。SQLite 下连接池为 1，写天然串行；
        //    抓取在锁外进行，各站节奏由 Fetcher 的独立闸门控制。
        let worker_count = stations
            .iter()
            .map(|s| s.max_inflight.max(1) as usize)
            .sum::<usize>()
            .clamp(1, 8);
        let mut handles = Vec::new();
        for _ in 0..worker_count {
            let eng = self.clone();
            let state = state.clone();
            let smap = station_by_id.clone();
            let ent = entry.clone();
            handles.push(tokio::spawn(async move {
                eng.worker(run_id, ent, state, smap).await
            }));
        }
        for h in handles {
            h.await??;
        }

        // 5) 收尾：先确认所有站的清单都已读完（失败站按失败处理），再落轮次结果。
        let (failed, unfinished) = {
            let g = state.lock().unwrap();
            (g.failed.clone(), g.preparers_done < g.preparers_total)
        };
        if unfinished || !failed.is_empty() {
            // 清单未就绪站点的页面不会被认领，这里统一按失败收尾，避免队列悬空。
            let detail = if failed.is_empty() {
                "未知站点清单未读完".to_string()
            } else {
                failed
                    .iter()
                    .map(|(s, e)| format!("{s}: {e}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            };
            self.store
                .fail_pending_run_pages(run_id, "站点状态或变更清单读取失败")
                .await?;
            let msg = format!("站点状态或变更清单读取失败：{detail}");
            self.store
                .update_run_status(run_id, RunStatus::Failed, Some(&msg))
                .await?;
            return Err(anyhow!(msg));
        }

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

    /// 读取某站变更清单到截止点，写入该站的目标修订索引并标记就绪。
    /// 失败（含 `Retry-After` 用尽、非成功响应）时记录原因，该站不就绪。
    async fn prepare_station(&self, run: &Run, station: &StationConfig, state: &Mutex<RunState>) {
        let cutoff = *run.watermarks.get(&station.id).unwrap_or(&0);
        let result = async {
            // 已读过的前缀从库里取（崩溃续跑幂等）。
            let after = self.max_stored_change_seq(&station.id).await?;
            let changes = self
                .fetcher
                .fetch_changes_until(station, after, cutoff)
                .await?;
            for c in changes {
                self.store.insert_change(&c).await?;
            }
            // 只按截止点内的条目为本站构建索引（业务时间可早于登记，一切以 seq 为准）。
            let mut targets: HashMap<(String, String), (i64, i64)> = HashMap::new();
            for c in self.store.list_changes(&station.id, 0).await? {
                if c.seq > cutoff {
                    continue;
                }
                targets
                    .entry((c.station_id.clone(), c.page_key.clone()))
                    .and_modify(|cur| {
                        if c.seq > cur.0 {
                            *cur = (c.seq, c.revision);
                        }
                    })
                    .or_insert((c.seq, c.revision));
            }
            Ok::<_, anyhow::Error>(targets)
        }
        .await;

        let mut g = state.lock().unwrap();
        match result {
            Ok(targets) => {
                g.targets.extend(targets);
                g.ready.insert(station.id.clone());
            }
            Err(e) => {
                tracing::warn!(station = %station.id, error = %e, "站点变更清单读取失败");
                g.failed.insert(station.id.clone(), e.to_string());
            }
        }
        g.preparers_done += 1;
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

    /// 当前可认领的站点集合及目标索引快照。
    /// 处于等待期的站点不参与认领，等待期结束后会自动重新纳入。
    async fn claim_scope(
        &self,
        state: &Mutex<RunState>,
    ) -> (Vec<String>, HashMap<(String, String), (i64, i64)>) {
        let (ids, targets) = {
            let g = state.lock().unwrap();
            (
                g.ready.iter().cloned().collect::<Vec<_>>(),
                g.targets.clone(),
            )
        };
        let mut available = Vec::with_capacity(ids.len());
        for id in ids {
            if self.fetcher.is_available(&id).await {
                available.push(id);
            }
        }
        (available, targets)
    }

    async fn worker(
        self: Arc<Self>,
        run_id: i64,
        entry: EntryPoint,
        state: Arc<Mutex<RunState>>,
        station_by_id: HashMap<String, StationConfig>,
    ) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        loop {
            // 只从“清单已读完且不在等待期”的站点认领页面；其他站的积压或等待
            // 不阻塞本站入库。所有站的清单读取都结束、且队列确实排空后才收工。
            let (ids, target_index) = self.claim_scope(&state).await;
            let claimed = self.store.claim_run_page_in(run_id, &ids).await?;
            let (station_id, page_key, depth, _attempts) = match claimed {
                Some(v) => v,
                None => {
                    let (finished, pending_stations) = {
                        let g = state.lock().unwrap();
                        (
                            g.preparers_done >= g.preparers_total,
                            g.failed.keys().cloned().collect::<Vec<_>>(),
                        )
                    };
                    if finished {
                        // 清单失败的站点永远无法就绪：其 pending 页面直接按未处理收尾，
                        // 否则工作线程会一直等一个不会到来的“就绪”。
                        if !pending_stations.is_empty() {
                            let mut left = self.store.pending_station_ids(run_id).await?;
                            left.retain(|s| pending_stations.contains(s));
                            if !left.is_empty() {
                                self.store
                                    .fail_pending_run_pages_for(run_id, &left, "站点变更清单未就绪")
                                    .await?;
                            }
                        }
                        let counts = self.store.count_run_pages(run_id).await?;
                        if counts.pending == 0 && counts.fetching == 0 {
                            return Ok(());
                        }
                    }
                    // 就绪站暂无待处理页面：等新页面入队或更多站点就绪。
                    let notified = self.wake.notified();
                    tokio::pin!(notified);
                    let timeout = tokio::time::sleep(Duration::from_millis(20));
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

    /// 单页处理：缓存命中则零请求，否则抓取并持久化修订。
    async fn process_page(
        &self,
        run: &Run,
        station: &StationConfig,
        page_key: &str,
        _depth: i64,
        target_index: &HashMap<(String, String), (i64, i64)>,
    ) -> Result<ProcessOutcome> {
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
            let fetched = match self.fetcher.fetch_page(station, page_key, request).await {
                Ok(f) => f,
                Err(PageFetchError::Gone) => return Ok(ProcessOutcome::Gone),
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
