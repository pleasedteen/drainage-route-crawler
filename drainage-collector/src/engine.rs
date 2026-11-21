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
use std::sync::Arc;

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

    /// 在当前进程内续跑一个已存在的 `running` 轮次（“另一入口续采”）。
    ///
    /// 与启动时自动续跑走同一条执行路径；已确认的进度与清单游标从库里读取，
    /// 因此不会重读已收到的清单页，也不依赖其它轮次的进度。
    pub async fn resume_run(self: &Arc<Self>, run_id: i64) -> Result<()> {
        let run = self.store.get_run(run_id).await?;
        if run.status != RunStatus::Running {
            return Err(anyhow!("轮次 {run_id} 不在运行中"));
        }
        self.execute_run(run_id).await
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

        // 3) 按固化截止点续读各站变更清单并逐页落库（崩溃后重复执行幂等）。
        //    每站只保留一页未落库清单：取一页、事务内落库并推进游标，再取下一页。
        //    游标随轮次数据一起持久化，重启或另一入口续采时跳过已确认范围；
        //    清单请求携带本轮截止点，站点侧裁剪，不会把整段清单读进内存。
        //    早于本轮截止点的已确认范围（反弹自更高截止点或更晚轮次）直接跳过，
        //    因此某轮次卡在后续清单页时不会拖住已齐清单的较早轮次。
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            self.sync_change_list(st, cutoff).await?;
        }

        // 4) 构建页面目标修订索引：(station, page) -> 截止点内最新 (seq, revision)。
        //    由数据库按页聚合，内存中只保留每个页面一条目标，不保留整段清单。
        let mut target_index: HashMap<(String, String), (i64, i64)> = HashMap::new();
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            for (page_key, seq, revision) in self.store.page_targets_up_to(&st.id, cutoff).await? {
                target_index.insert((st.id.clone(), page_key), (seq, revision));
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

    /// 把某站变更清单同步到截止点 `cutoff`，逐页落库并推进确认游标。
    ///
    /// 不变量：
    /// - 一页条目与其游标推进在同一事务提交，崩溃后整页重读或整页已确认；
    /// - 游标可跨过 seq 空洞：只要求“页内条目之间连续”，页与页之间由站点分页
    ///   保证没有遗漏，因此空档（例如登记序号跳号）也会被取齐；
    /// - 若某条目的 seq 超过截止点，则本轮所需清单已齐，停止续读。
    async fn sync_change_list(&self, station: &StationConfig, cutoff: i64) -> Result<()> {
        let mut cursor = self.store.change_cursor(&station.id).await?;
        // 游标已越过截止点：本轮所需范围已全部在库。
        if cursor >= cutoff {
            return Ok(());
        }
        // 防御：若截止点内已有条目的 seq 高于游标（例如升级前的旧库或手工写入），
        // 从库里补一个不超过截止点的起点，避免漏取 (max, cutoff] 之间的登记。
        if let Some(max_seq) = self
            .store
            .latest_change_seq_up_to(&station.id, cutoff)
            .await?
        {
            if max_seq > cursor {
                cursor = max_seq;
            }
        }
        loop {
            if cursor >= cutoff {
                break;
            }
            let (mut entries, next) = self
                .fetcher
                .fetch_changes_page(station, cursor, cutoff)
                .await?;
            // 防御站点忽略 until：超出截止点的条目留给后续轮次。
            entries.retain(|c| c.seq > cursor && c.seq <= cutoff);
            entries.sort_by_key(|c| c.seq);
            let page_max = entries.last().map(|c| c.seq).unwrap_or(cursor);
            let advanced = next
                .map(|n| n.min(cutoff))
                .filter(|n| *n > cursor)
                .unwrap_or(page_max)
                .max(cursor);
            // 一页条目 + 游标同一事务提交。
            self.store
                .ingest_change_page(&station.id, &entries, advanced)
                .await?;
            if advanced <= cursor {
                // 站点没有前进：没有更多不超过截止点的条目。
                break;
            }
            cursor = advanced;
        }
        Ok(())
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
