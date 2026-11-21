//! 采集引擎：轮次编排、截止点固化、全量/增量遍历、断点续跑。
//!
//! ## 同截止点一致性
//! 页面目标修订统一由“轮次截止点 + 变更清单”解析。全量从入口按下游链接做去重
//! 遍历；增量在已有成功轮次时只核查受新登记影响的路线（见下），否则退回全图遍历。
//! 两种模式到达相同截止点时算出的可达关系逐页相同。
//!
//! ## 断点续跑
//! 所有进度都在确认落库后才推进：抓取结果、链接、可达关系、页面状态。
//! 进程退出后重启，`running` 轮次的 `fetching` 页面收回为 `pending`，
//! 工作线程从头消费队列即可接着完成，已完成页面不会重抓。
//!
//! ## 增量只核查受影响的路线
//! 有成功轮次后，增量轮次不再从入口重走全部页面，而是以最近成功轮次为基线：
//!
//! 1. 用“基线截止点 → 本轮截止点”之间的新登记确定脏页（修订/新增/移除）；
//! 2. 对每个基线可达的脏页比对**旧边与新边**，只把“变更边”（新增/删除）的另一端
//!    作为种子。新增边沿新图前向闭包，只进入基线不可达的新页面（并入主干即停）；
//!    删除边沿旧图前向闭包，只沿基线可达者传播（跨站回流环失去最后入口时整环进入）。
//!    脏页上未变化的边不参与扩散，避免因一条支线改动而重走其整片下游；
//! 3. 区域外的可达关系原样继承基线；区域内的页面以“区域外的基线入边”为种子，
//!    按**新图**重算最短路并覆盖。
//!
//! 任何可达性变化都必然经过某条变更边，故区域内重算 + 区域外继承与同截止点全量
//! 逐页一致；受影响区域之外的页面一次也不读、不写。
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
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

/// 增量计划中的脏页：基线中的可达修订（若曾可达）。
#[derive(Debug, Clone)]
struct DirtyPage {
    base_revision: Option<i64>,
}

/// 一次页面装载：`revision` 为 `None` 表示页面在截止点前已移除。
#[derive(Debug, Clone)]
struct LoadedPage {
    revision: Option<i64>,
    links: Vec<LinkRef>,
}

/// 本轮采用的遍历方式（仅用于日志）。
#[derive(Debug, Clone, Copy)]
enum RunTraversal {
    Full,
    Incremental,
}

/// 旧边集合与新边集合的对称差（即“变更边”），任一方向的端点各返回一次。
fn changed_endpoints(old: &[LinkRef], new: &[LinkRef]) -> Vec<LinkRef> {
    let key = |l: &LinkRef| (l.station_id.clone(), l.page_key.clone());
    let old_set: HashSet<_> = old.iter().map(key).collect();
    let new_set: HashSet<_> = new.iter().map(key).collect();
    let mut out = Vec::new();
    for k in new_set.difference(&old_set) {
        out.push(LinkRef {
            station_id: k.0.clone(),
            page_key: k.1.clone(),
        });
    }
    for k in old_set.difference(&new_set) {
        out.push(LinkRef {
            station_id: k.0.clone(),
            page_key: k.1.clone(),
        });
    }
    out
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
        let target_index = self.build_target_index(&stations, &run).await?;

        // 有成功轮次后，增量只核查受新登记影响的路线；否则退回全图遍历。
        if run.mode == RunMode::Incremental {
            if let Some(base) = self.store.latest_succeeded_run(run.entry_id, None).await? {
                if base.id < run.id {
                    return self
                        .execute_incremental(
                            &run,
                            &entry,
                            &stations,
                            &station_by_id,
                            &target_index,
                            &base,
                        )
                        .await;
                }
            }
        }
        self.execute_full(&run, &entry, &stations, &station_by_id, target_index)
            .await
    }

    /// 构建截止点内的目标修订索引。
    async fn build_target_index(
        &self,
        stations: &[StationConfig],
        run: &Run,
    ) -> Result<HashMap<(String, String), (i64, i64)>> {
        let mut target_index: HashMap<(String, String), (i64, i64)> = HashMap::new();
        for st in stations {
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
        Ok(target_index)
    }

    /// 从入口重走全图（全量模式，或尚无成功轮次时的增量兜底）。
    async fn execute_full(
        self: &Arc<Self>,
        run: &Run,
        entry: &EntryPoint,
        stations: &[StationConfig],
        station_by_id: &HashMap<String, StationConfig>,
        target_index: HashMap<(String, String), (i64, i64)>,
    ) -> Result<()> {
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
            let run_id = run.id;
            handles.push(tokio::spawn(async move {
                eng.worker(run_id, ent, idx, smap).await
            }));
        }
        for h in handles {
            h.await??;
        }
        self.finalize_run(run, stations, RunTraversal::Full).await
    }

    /// 增量：以最近成功轮次为基线，仅重算受新登记影响的前向闭包。
    async fn execute_incremental(
        self: &Arc<Self>,
        run: &Run,
        entry: &EntryPoint,
        stations: &[StationConfig],
        station_by_id: &HashMap<String, StationConfig>,
        target_index: &HashMap<(String, String), (i64, i64)>,
        base: &Run,
    ) -> Result<()> {
        if !self.store.is_delta_prepared(run.id).await? {
            self.plan_incremental(run, entry, stations, station_by_id, target_index, base)
                .await?;
        }
        self.finalize_run(run, stations, RunTraversal::Incremental)
            .await
    }

    /// 计算并落库增量计划：脏页 -> 受影响区域 -> 继承区域外基线结果 -> 重算区域内可达性。
    ///
    /// 整个过程在 `run_delta.prepared` 之前可重入：未标记完成时再次进入会清空重算，
    /// 已确认的页面抓取结果（`page_revisions`）仍然复用，不会重复请求。
    async fn plan_incremental(
        self: &Arc<Self>,
        run: &Run,
        entry: &EntryPoint,
        stations: &[StationConfig],
        station_by_id: &HashMap<String, StationConfig>,
        target_index: &HashMap<(String, String), (i64, i64)>,
        base: &Run,
    ) -> Result<()> {
        type Key = (String, String);
        let base_reach: HashMap<Key, (i64, i64)> = self
            .store
            .reachable_set(base.id)
            .await?
            .into_iter()
            .map(|(s, p, rev, depth)| ((s, p), (rev, depth)))
            .collect();

        // 本页在截止点下的抓取目标。
        let request_for = |key: &Key| -> FetchTarget {
            match target_index.get(key) {
                Some((_, rev)) => FetchTarget::AtRevision(*rev),
                None => FetchTarget::AtSeq(*run.watermarks.get(&key.0).unwrap_or(&0)),
            }
        };

        // 1) 脏页：基线截止点之后、本轮截止点之前的登记条目。按登记顺序处理，
        //    同一页面多次登记以最后修订为目标；基线修订取其在基线中可达时的版本。
        let mut dirty: BTreeMap<Key, DirtyPage> = BTreeMap::new();
        for st in stations {
            let from = *base.watermarks.get(&st.id).unwrap_or(&0);
            let cut = *run.watermarks.get(&st.id).unwrap_or(&0);
            for c in self.store.list_changes(&st.id, from).await? {
                if c.seq > cut {
                    break;
                }
                let key: Key = (st.id.clone(), c.page_key.clone());
                dirty.insert(
                    key.clone(),
                    DirtyPage {
                        base_revision: base_reach.get(&key).map(|(r, _)| *r),
                    },
                );
            }
        }

        // 2) 受影响区域（最小前向闭包）。
        //
        //    可达性只可能在“变更边”的两端发生变化：
        //    - 新增边（新图有、旧图无）：其下游在新图中可能变为可达；
        //    - 删除边（旧图有、新图无）：其下游可能失去可达性（含跨站回流环
        //      失去最后一条入口路线时整环失达）。
        //    因此只把脏页的**变更边**作为种子沿对应图前向闭包，脏页自身也纳入重算。
        //    不沿脏页上未变化的边扩散，避免在大基线上因一条支线改动而重走整片下游。
        let mut affected: HashSet<Key> = dirty.keys().cloned().collect();
        let mut pages: HashMap<Key, LoadedPage> = HashMap::new();
        let mut removed_seeds: Vec<Key> = Vec::new();
        let mut added_seeds: Vec<Key> = Vec::new();

        for key in dirty.keys() {
            // 基线可达的脏页：比对新旧边，找出真正变化的边作为闭包种子。
            if let Some(base_rev) = dirty[key].base_revision {
                let page = self.load_page_key(station_by_id, key, request_for(key)).await?;
                let old = self.store.links_at_revision(&key.0, &key.1, base_rev).await?;
                for l in changed_endpoints(&old, &page.links) {
                    if !station_by_id.contains_key(&l.station_id) {
                        continue;
                    }
                    let lk = (l.station_id, l.page_key);
                    if old.iter().any(|o| o.station_id == lk.0 && o.page_key == lk.1) {
                        // 旧图有、新图无：沿旧图传播失达。
                        removed_seeds.push(lk);
                    } else {
                        // 新图新增的边：沿新图传播新增可达。
                        added_seeds.push(lk);
                    }
                }
                pages.insert(key.clone(), page);
                continue;
            }
            // 基线不可达的新登记页：若某个基线可达页（其边未变）本就指向它
            // ——即“悬空前向引用”此刻才登记上——它就是一条新增路线的起点。
            let incoming = self
                .store
                .incoming_reachable_edges(base.id, &key.0, &key.1)
                .await?;
            if !incoming.is_empty() {
                added_seeds.push(key.clone());
            }
            // 否则暂不加载：仅当其后被闭包/最短路抵达时才装载。
        }

        // 2a) 删除边：沿旧图前向闭包。仅基线可达者可能失达，遇到基线不可达者即止。
        //     非脏页的新旧边一致，用旧边继续传播即可。
        let mut removed_seen: HashSet<Key> = HashSet::new();
        let mut removed_queue: VecDeque<Key> = removed_seeds.into_iter().collect();
        while let Some(key) = removed_queue.pop_front() {
            if !removed_seen.insert(key.clone()) {
                continue;
            }
            let Some((base_rev, _)) = base_reach.get(&key).copied() else {
                continue;
            };
            affected.insert(key.clone());
            for l in self.store.links_at_revision(&key.0, &key.1, base_rev).await? {
                if station_by_id.contains_key(&l.station_id) {
                    removed_queue.push_back((l.station_id, l.page_key));
                }
            }
        }

        // 2b) 新增边：沿新图前向闭包，但只进入基线不可达的新页面；一旦抵达基线
        //     已可达的页面即并入基线结果，不再向其下游扩散。这样一条支线并入主干时
        //     只多核查支线本身与接入点，不会重走主干下游。
        let mut added_seen: HashSet<Key> = HashSet::new();
        let mut added_queue: VecDeque<Key> = added_seeds.into_iter().collect();
        while let Some(key) = added_queue.pop_front() {
            if !added_seen.insert(key.clone()) {
                continue;
            }
            if base_reach.contains_key(&key) {
                continue;
            }
            affected.insert(key.clone());
            let page = match pages.get(&key) {
                Some(p) => p.clone(),
                None => {
                    let p = self
                        .load_page_key(station_by_id, &key, request_for(&key))
                        .await?;
                    pages.insert(key.clone(), p.clone());
                    p
                }
            };
            for l in &page.links {
                if station_by_id.contains_key(&l.station_id) {
                    added_queue.push_back((l.station_id.clone(), l.page_key.clone()));
                }
            }
        }

        // 3) 重建本轮结果：继承基线中区域之外的页面，区域内的页面随后重算。
        self.store.clear_run_results(run.id).await?;
        for key in &affected {
            let role = if dirty.contains_key(key) {
                "changed"
            } else {
                "affected"
            };
            self.store.add_run_delta_page(run.id, &key.0, &key.1, role).await?;
        }
        let inherited = self.store.inherit_reachable_from(base.id, run.id).await?;
        self.store.inherit_run_pages_from(base.id, run.id).await?;
        for key in &affected {
            self.store.delete_reachable_page(run.id, &key.0, &key.1).await?;
            self.store.delete_run_page(run.id, &key.0, &key.1).await?;
        }
        self.store
            .set_run_delta(run.id, base.id, inherited as i64)
            .await?;

        // 4) 区域内可达性重算：入口与“区域外基线可达页面的入边”作为种子，
        //    在区域内按新图求最短路（深度取最小）。
        let mut pq: BinaryHeap<Reverse<(i64, String, String)>> = BinaryHeap::new();
        let entry_key: Key = (entry.station_id.clone(), entry.page_key.clone());
        if affected.contains(&entry_key) {
            pq.push(Reverse((0, entry_key.0.clone(), entry_key.1.clone())));
        }
        for key in &affected {
            if *key == entry_key {
                continue;
            }
            for (from_sid, from_page) in self
                .store
                .incoming_reachable_edges(base.id, &key.0, &key.1)
                .await?
            {
                let from_key = (from_sid, from_page);
                if affected.contains(&from_key) {
                    continue; // 区域内的入边稍后随最短路处理。
                }
                if let Some((_, d)) = base_reach.get(&from_key) {
                    pq.push(Reverse((d + 1, key.0.clone(), key.1.clone())));
                }
            }
        }

        let mut dist: HashMap<Key, i64> = HashMap::new();
        while let Some(Reverse((d, sid, page))) = pq.pop() {
            let key = (sid, page);
            if dist.get(&key).is_some_and(|cur| *cur <= d) {
                continue;
            }
            dist.insert(key.clone(), d);
            // 区域内的页面都已在计划和装载阶段覆盖；额外装载仅兜底。
            let page_data = match pages.get(&key) {
                Some(p) => p.clone(),
                None if affected.contains(&key) => {
                    let p = self
                        .load_page_key(station_by_id, &key, request_for(&key))
                        .await?;
                    pages.insert(key.clone(), p.clone());
                    p
                }
                None => continue,
            };
            if page_data.revision.is_none() {
                continue; // 截止点前已移除：无下游。
            }
            for l in &page_data.links {
                let lk = (l.station_id.clone(), l.page_key.clone());
                if !affected.contains(&lk) {
                    continue;
                }
                if dist.get(&lk).is_none_or(|cur| d + 1 < *cur) {
                    pq.push(Reverse((d + 1, l.station_id.clone(), l.page_key.clone())));
                }
            }
        }

        for key in &affected {
            // 与全量遍历对齐：只有真正被下游路线抵达的页面才进入本轮工作集。
            // 区域内重算后不可达者既不计入可达设施，也不留在工作集中。
            if let Some(d) = dist.get(key).copied() {
                self.store.mark_run_page_done(run.id, &key.0, &key.1, d).await?;
                if let Some(rev) = pages.get(key).and_then(|p| p.revision) {
                    self.store
                        .upsert_reachable(run.id, entry.id, &key.0, &key.1, rev, d)
                        .await?;
                    // 已移除页面仅记完成态，不计入可达设施（与全量一致）。
                }
            }
        }

        self.store.mark_delta_prepared(run.id).await?;
        tracing::info!(
            run_id = run.id,
            affected = affected.len(),
            reachable = dist.len(),
            "增量计划完成"
        );
        Ok(())
    }

    /// 按 (站点, 页面) 装载页面：缓存命中零请求，未命中才抓取并落库。
    async fn load_page_key(
        &self,
        station_by_id: &HashMap<String, StationConfig>,
        key: &(String, String),
        request: FetchTarget,
    ) -> Result<LoadedPage> {
        let station = station_by_id
            .get(&key.0)
            .ok_or_else(|| anyhow!("未知站点: {}", key.0))?;
        self.load_page(station, &key.1, request).await
    }

    /// 按目标装载页面：缓存命中零请求，未命中才抓取并落库。
    /// 返回 `revision = None` 表示页面在截止点前已移除。
    async fn load_page(
        &self,
        station: &StationConfig,
        page_key: &str,
        request: FetchTarget,
    ) -> Result<LoadedPage> {
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
        let page_rev = match cached {
            Some(pr) => pr,
            None => {
                let fetched = match self.fetcher.fetch_page(station, page_key, request).await {
                    Ok(f) => f,
                    Err(PageFetchError::Gone) => {
                        return Ok(LoadedPage {
                            revision: None,
                            links: Vec::new(),
                        })
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
                self.store
                    .get_page_revision(&station.id, page_key, parsed.revision)
                    .await?
                    .ok_or_else(|| anyhow!("修订落库后读回失败"))?
            }
        };
        let parsed = parse_page(station.format, &station.id, &page_rev.body)?;
        if parsed.revision != page_rev.revision {
            return Err(anyhow!(
                "页面自报修订 {} 与缓存修订 {} 不一致",
                parsed.revision,
                page_rev.revision
            ));
        }
        Ok(LoadedPage {
            revision: Some(page_rev.revision),
            links: parsed.links,
        })
    }

    /// 收尾：检查失败页、推进站点水位、标记轮次结果。
    async fn finalize_run(
        &self,
        run: &Run,
        stations: &[StationConfig],
        traversal: RunTraversal,
    ) -> Result<()> {
        let counts = self.store.count_run_pages(run.id).await?;
        if counts.failed > 0 {
            let msg = format!("{} 个页面抓取失败，可达关系为部分结果", counts.failed);
            self.store
                .update_run_status(run.id, RunStatus::Failed, Some(&msg))
                .await?;
            return Err(anyhow!(msg));
        }
        for st in stations {
            let cutoff = *run.watermarks.get(&st.id).unwrap_or(&0);
            if cutoff > st.watermark_seq {
                self.store.set_station_watermark(&st.id, cutoff).await?;
            }
        }
        self.store
            .update_run_status(run.id, RunStatus::Succeeded, None)
            .await?;
        tracing::info!(
            run_id = run.id,
            done = counts.done,
            gone = counts.total - counts.done,
            ?traversal,
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
