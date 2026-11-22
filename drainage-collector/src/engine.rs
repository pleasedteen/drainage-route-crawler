//! 采集引擎：轮次编排、截止点固化、全量/增量遍历、断点续跑、历史轮次修复。
//!
//! ## 同截止点一致性
//! 全量与增量走**同一套遍历代码**：从入流口按下游链接做去重遍历，
//! 页面目标修订统一由“轮次截止点 + 变更清单”解析。差别只在缓存：
//! - 全量：从入口重走，已缓存的同修订页面同样直接命中（跨入口共享抓取结果）；
//! - 增量：变更清单里未出现的页面保持旧修订，缓存命中更多，未修订页面不重复抓。
//! 因此到达相同截止点时，两种模式算出的可达关系逐页相同。
//!
//! ## 截断点正确性
//! 页面目标修订只有两种来源：截止点内的变更清单（明确修订号），或“按截止点取版本”
//! （`as_of`）。后者会先查已核实的 `page_as_of` 记忆，再决定是否请求站点；
//! **绝不**用“最新已缓存修订”顶替——那会把截止点之后登记的修订误收进来。
//!
//! ## 断点续跑
//! 所有进度都在确认落库后才推进：抓取结果、链接、可达关系、页面状态。
//! 进程退出后重启，`running` 轮次的 `fetching` 页面收回为 `pending`，
//! 工作线程从头消费队列即可接着完成，已完成页面不会重抓。
//!
//! ## 历史轮次修复
//! `repair_run` 以轮次**原已固化**的截止点重新遍历，把结果写入新的“结果代次”，
//! 全程不触碰当前已发布代次；全部页面核实成功后才用一条 `UPDATE` 翻转
//! `runs.published_generation`（写入量与设施数无关）。失败或中断不翻转，
//! 因此修复期间按轮次、按入口的查询始终返回原已发布结果。修复队列与
//! `page_as_of` 记忆都持久化，重启后接着修，已核实的目标修订不重抓。
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
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

/// 工作队列种类：普通轮次或历史轮次修复（两者遍历逻辑完全相同，
/// 差别仅在队列表与结果代次）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueKind {
    Run,
    Repair,
}

/// 一次遍历所需的共享上下文。
struct WorkCtx {
    run_id: i64,
    /// 结果写入的 `reachable.generation`。
    generation: i64,
    entry: EntryPoint,
    queue: QueueKind,
    target_index: HashMap<(String, String), (i64, i64)>,
    station_by_id: HashMap<String, StationConfig>,
}

impl Engine {
    pub fn new(store: Store, fetcher: Arc<Fetcher>) -> Arc<Self> {
        Arc::new(Self {
            store,
            fetcher,
            wake: Notify::new(),
        })
    }

    /// 启动时调用：收回崩溃残留状态，续跑所有未完成轮次与修复任务。
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
        // 未完成的修复任务：重启后接着修。
        for rec in self.store.list_open_repairs().await? {
            let eng = self.clone();
            tokio::spawn(async move {
                if let Err(e) = eng.repair_run(rec.run_id).await {
                    tracing::error!(run_id = rec.run_id, error = %e, "修复任务异常终止");
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
        let stations = self.store.list_stations().await?;

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

        // 4) 遍历。普通轮次写入其当前已发布代次（新轮次即第 1 代）。
        let generation = run.published_generation;
        let counts = self.traverse(&run, QueueKind::Run, generation).await?;

        // 5) 收尾：推进站点水位，标记轮次结果。
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

    // ---------- 遍历 ----------

    /// 依据已固化的截止点从变更清单构建目标修订索引。
    async fn build_target_index(
        &self,
        stations: &[StationConfig],
        watermarks: &BTreeMap<String, i64>,
    ) -> Result<HashMap<(String, String), (i64, i64)>> {
        let mut target_index: HashMap<(String, String), (i64, i64)> = HashMap::new();
        for st in stations {
            let cutoff = *watermarks.get(&st.id).unwrap_or(&0);
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
        Ok(target_index)
    }

    /// 共同遍历入口：普通轮次与历史修复都走这里。
    async fn traverse(
        self: &Arc<Self>,
        run: &Run,
        queue: QueueKind,
        generation: i64,
    ) -> Result<crate::store::RunPageCounts> {
        let entry = self
            .store
            .get_entry(run.entry_id)
            .await?
            .ok_or_else(|| anyhow!("入流口不存在"))?;
        let stations = self.store.list_stations().await?;
        let station_by_id: HashMap<String, StationConfig> =
            stations.iter().map(|s| (s.id.clone(), s.clone())).collect();
        let target_index = self.build_target_index(&stations, &run.watermarks).await?;

        // 入口页入队（幂等）：普通轮次通常已在 start_run 入队；修复队列首次在此登记。
        match queue {
            QueueKind::Run => {
                self.store
                    .enqueue_run_page(run.id, &entry.station_id, &entry.page_key, 0)
                    .await?;
            }
            QueueKind::Repair => {
                self.store
                    .enqueue_repair_page(run.id, &entry.station_id, &entry.page_key, 0)
                    .await?;
            }
        }

        // 5) 工作线程消费页面队列。SQLite 下连接池为 1，写天然串行；
        //    抓取在锁外进行，各站节奏由 Fetcher 的独立闸门控制。
        let worker_count = stations
            .iter()
            .map(|s| s.max_inflight.max(1) as usize)
            .sum::<usize>()
            .clamp(1, 8);
        let ctx = Arc::new(WorkCtx {
            run_id: run.id,
            generation,
            entry,
            queue,
            target_index,
            station_by_id,
        });
        let mut handles = Vec::new();
        for _ in 0..worker_count {
            let eng = self.clone();
            let ctx = ctx.clone();
            handles.push(tokio::spawn(async move { eng.worker(ctx).await }));
        }
        for h in handles {
            h.await??;
        }
        self.counts(queue, run.id).await
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

    // ---------- 队列操作按种类分派 ----------

    async fn claim(
        &self,
        queue: QueueKind,
        run_id: i64,
    ) -> Result<Option<(String, String, i64, i64)>> {
        match queue {
            QueueKind::Run => self.store.claim_run_page(run_id).await,
            QueueKind::Repair => self.store.claim_repair_page(run_id).await,
        }
    }

    async fn complete(&self, queue: QueueKind, run_id: i64, s: &str, p: &str) -> Result<()> {
        match queue {
            QueueKind::Run => self.store.complete_run_page(run_id, s, p).await,
            QueueKind::Repair => self.store.complete_repair_page(run_id, s, p).await,
        }
    }

    async fn fail(&self, queue: QueueKind, run_id: i64, s: &str, p: &str, e: &str) -> Result<()> {
        match queue {
            QueueKind::Run => self.store.fail_run_page(run_id, s, p, e).await,
            QueueKind::Repair => self.store.fail_repair_page(run_id, s, p, e).await,
        }
    }

    async fn counts(
        &self,
        queue: QueueKind,
        run_id: i64,
    ) -> Result<crate::store::RunPageCounts> {
        match queue {
            QueueKind::Run => self.store.count_run_pages(run_id).await,
            QueueKind::Repair => self.store.count_repair_pages(run_id).await,
        }
    }

    async fn enqueue_link(
        &self,
        queue: QueueKind,
        run_id: i64,
        l: &LinkRef,
        depth: i64,
    ) -> Result<()> {
        match queue {
            QueueKind::Run => {
                self.store
                    .enqueue_run_page(run_id, &l.station_id, &l.page_key, depth)
                    .await
            }
            QueueKind::Repair => {
                self.store
                    .enqueue_repair_page(run_id, &l.station_id, &l.page_key, depth)
                    .await
            }
        }
    }

    async fn worker(self: Arc<Self>, ctx: Arc<WorkCtx>) -> Result<()> {
        let run = self.store.get_run(ctx.run_id).await?;
        loop {
            // 认领一个待处理页面。
            let claimed = self.claim(ctx.queue, ctx.run_id).await?;
            let (station_id, page_key, depth, _attempts) = match claimed {
                Some(v) => v,
                None => {
                    let counts = self.counts(ctx.queue, ctx.run_id).await?;
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

            let station = match ctx.station_by_id.get(&station_id) {
                Some(s) => s.clone(),
                None => {
                    self.fail(ctx.queue, ctx.run_id, &station_id, &page_key, "未知站点")
                        .await?;
                    continue;
                }
            };

            let outcome = self
                .process_page(&run, &station, &page_key, depth, &ctx.target_index)
                .await;

            match outcome {
                Ok(ProcessOutcome::Processed { links, revision }) => {
                    // 先落可达关系与下游队列，最后才把页面标记 done：
                    // 若在 done 之前崩溃，页面会被收回 pending 重做，而这些写入
                    // 都是幂等的（upsert / ON CONFLICT DO NOTHING），不会重复。
                    // 反过来先标 done 再写，一旦中途崩溃就会永久漏掉可达行或下游页面。
                    self.store
                        .upsert_reachable(
                            ctx.run_id,
                            ctx.generation,
                            ctx.entry.id,
                            &station_id,
                            &page_key,
                            revision,
                            depth,
                        )
                        .await?;
                    // 下游入队；唯一约束负责汇合/成环去重。
                    for l in links {
                        if !ctx.station_by_id.contains_key(&l.station_id) {
                            tracing::warn!(%station_id, %page_key, to_station=%l.station_id, to_page=%l.page_key, "链接指向未配置站点，跳过");
                            continue;
                        }
                        self.enqueue_link(ctx.queue, ctx.run_id, &l, depth + 1)
                            .await?;
                    }
                    self.complete(ctx.queue, ctx.run_id, &station_id, &page_key)
                        .await?;
                    self.wake.notify_waiters();
                }
                Ok(ProcessOutcome::Gone) => {
                    // 页面在截止点前已移除：记录终态，不计入可达设施。
                    self.complete(ctx.queue, ctx.run_id, &station_id, &page_key)
                        .await?;
                    self.wake.notify_waiters();
                }
                Err(e) => {
                    tracing::warn!(run_id = ctx.run_id, %station_id, %page_key, error = %e, "页面抓取失败");
                    self.fail(
                        ctx.queue,
                        ctx.run_id,
                        &station_id,
                        &page_key,
                        &e.to_string(),
                    )
                    .await?;
                    self.wake.notify_waiters();
                }
            }
        }
    }

    /// 单页处理：缓存或“截止点→修订”记忆命中则零请求，否则抓取并持久化修订。
    async fn process_page(
        &self,
        run: &Run,
        station: &StationConfig,
        page_key: &str,
        _depth: i64,
        target_index: &HashMap<(String, String), (i64, i64)>,
    ) -> Result<ProcessOutcome> {
        let request = self.resolve_request(run, target_index, &station.id, page_key);

        // 1) 若只能按截止点取版本：先查已核实的“截止点→修订”记忆。
        //    -1 表示该截止点下已核实页面不存在（Gone），同样无需再请求。
        let mut resolved_as_of: Option<i64> = None;
        let request = match request {
            FetchTarget::AtSeq(seq) => {
                match self
                    .store
                    .get_page_as_of(&station.id, page_key, seq)
                    .await?
                {
                    Some(-1) => return Ok(ProcessOutcome::Gone),
                    Some(rev) => FetchTarget::AtRevision(rev),
                    None => {
                        resolved_as_of = Some(seq);
                        FetchTarget::AtSeq(seq)
                    }
                }
            }
            FetchTarget::AtRevision(rev) => FetchTarget::AtRevision(rev),
        };

        // 2) 查共享缓存：同页面同修订只抓一次。
        let cached = match request {
            FetchTarget::AtRevision(rev) => {
                self.store
                    .get_page_revision(&station.id, page_key, rev)
                    .await?
            }
            FetchTarget::AtSeq(_) => None,
        };

        let page_rev = if let Some(pr) = cached {
            pr
        } else {
            // 3) 缓存未命中，按站节奏抓取（退避/暂停在 Fetcher 内处理）。
            let fetched = match self.fetcher.fetch_page(station, page_key, request).await {
                Ok(f) => f,
                Err(PageFetchError::Gone) => {
                    if let Some(seq) = resolved_as_of {
                        self.store
                            .put_page_as_of(&station.id, page_key, seq, -1)
                            .await?;
                    }
                    return Ok(ProcessOutcome::Gone);
                }
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
            // 记住“该截止点下核实到的修订”，重启/再次修复时不重抓。
            if let Some(seq) = resolved_as_of {
                self.store
                    .put_page_as_of(&station.id, page_key, seq, parsed.revision)
                    .await?;
            }
            self.store
                .get_page_revision(&station.id, page_key, parsed.revision)
                .await?
                .ok_or_else(|| anyhow!("修订落库后读回失败"))?
        };

        // 4) 解析正文（缓存命中也要解析以取得下游链接）。
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

    // ---------- 历史轮次修复 ----------

    /// 按轮次号修复历史轮次：用该轮次**原已固化**的截止点重新核实路线，
    /// 结果写入新代次，成功后才原子切换。轮次编号、创建时间、历史顺序均不变。
    ///
    /// 可重复进入：修复成功后再调用为幂等空操作；失败/中断后再次调用会
    /// 接着未完成的页面继续（已核实的修订命中缓存/截止点记忆，不重抓）。
    pub async fn repair_run(self: &Arc<Self>, run_id: i64) -> Result<RepairOutcome> {
        let run = self.store.get_run(run_id).await?;
        if run.status == RunStatus::Running {
            bail!("轮次 {run_id} 仍在进行中，不能修复");
        }

        let existing = self.store.get_repair(run_id).await?;
        let generation = match &existing {
            Some(rec) if rec.status == "succeeded" && run.published_generation == rec.generation => {
                // 已修复并已切换：幂等返回，不重复遍历、不新增写入。
                return Ok(RepairOutcome {
                    run_id,
                    generation: rec.generation,
                    reachable: self.store.count_reachable(run_id).await?,
                    already_repaired: true,
                });
            }
            // 未完成的修复：沿用原代次续修。
            Some(rec) => rec.generation,
            None => run.published_generation + 1,
        };

        // 续修前收回崩溃残留的 fetching/failed，允许重试。
        self.store.reset_open_repair_pages(run_id).await?;
        self.store.begin_repair(run_id, generation).await?;

        // 用原截止点确保变更清单可用（本地站点；已存满时为空操作）。
        let stations = self.store.list_stations().await?;
        for st in &stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            let after = self.max_stored_change_seq(&st.id).await?;
            if after < cutoff {
                let changes = self.fetcher.fetch_changes_until(st, after, cutoff).await?;
                for c in changes {
                    self.store.insert_change(&c).await?;
                }
            }
        }

        let counts = self.traverse(&run, QueueKind::Repair, generation).await?;
        if counts.failed > 0 {
            let msg = format!("{} 个页面核实失败，未切换结果", counts.failed);
            self.store
                .update_repair_status(run_id, "failed", Some(&msg))
                .await?;
            bail!(msg);
        }
        if counts.pending > 0 || counts.fetching > 0 || counts.total == 0 {
            let msg = format!("修复未完成：total={} done={}", counts.total, counts.done);
            self.store
                .update_repair_status(run_id, "failed", Some(&msg))
                .await?;
            bail!(msg);
        }

        // 新结果一次切换：只翻转一个指针，写入量与设施数无关。
        self.store.publish_generation(run_id, generation).await?;
        self.store.update_repair_status(run_id, "succeeded", None).await?;
        let reachable = self.store.count_reachable(run_id).await?;
        tracing::info!(run_id, generation, reachable, "历史轮次修复完成并已切换");
        Ok(RepairOutcome {
            run_id,
            generation,
            reachable,
            already_repaired: false,
        })
    }

    /// 查询某轮次的修复状态（未修复返回 None）。
    pub async fn repair_status(&self, run_id: i64) -> Result<Option<RepairStatus>> {
        let run = self.store.get_run(run_id).await?;
        let rec = match self.store.get_repair(run_id).await? {
            Some(r) => r,
            None => return Ok(None),
        };
        let counts = self.store.count_repair_pages(run_id).await?;
        Ok(Some(RepairStatus {
            run_id,
            status: rec.status,
            generation: rec.generation,
            published_generation: run.published_generation,
            entry_id: run.entry_id,
            total: counts.total,
            done: counts.done,
            pending: counts.pending,
            fetching: counts.fetching,
            failed: counts.failed,
            error: rec.error,
            started_at: rec.started_at,
            updated_at: rec.updated_at,
        }))
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
