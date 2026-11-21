//! 增量“只核查受影响路线”的验收测试：
//! - 有成功轮次后，增量不再从入口重走全图，只核查受新登记影响的路线；
//! - 跨站回流环失去最后一条入口路线时，增量结果仍与同截止点全量一致；
//! - ≥2000 页基线上改动 ≤10 页独立支线，增量读写的设施与边记录各 ≤100 条；
//! - 新登记页补全悬空前向引用、页面移除等边界与全量一致。
mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{MockPageClient, MockStation, MockWorld};
use drainage_collector::model::{FacilityInfo, LinkRef, RunMode, SiteFormat, StationConfig};
use drainage_collector::store::{IoSnapshot, Store};

// ---------- 构造与工具 ----------

fn fac(name: &str) -> FacilityInfo {
    FacilityInfo {
        name: Some(name.into()),
        kind: Some("manhole".into()),
    }
}

fn link(station: &str, page: &str) -> LinkRef {
    LinkRef {
        station_id: station.into(),
        page_key: page.into(),
    }
}

fn station_cfg(id: &str, format: SiteFormat) -> StationConfig {
    StationConfig {
        id: id.into(),
        name: id.into(),
        base_url: format!("mock://{id}"),
        format,
        min_interval_ms: 0,
        max_inflight: 4,
        watermark_seq: 0,
    }
}

async fn engine_for(
    dir: &tempfile::TempDir,
    name: &str,
    world: MockWorld,
    stations: &[StationConfig],
) -> (Arc<Engine>, Store) {
    let url = format!("sqlite://{}", dir.path().join(name).display());
    let store = Store::connect(&url).await.unwrap();
    setup_store(&store, world, stations).await
}

async fn setup_store(
    store: &Store,
    world: MockWorld,
    stations: &[StationConfig],
) -> (Arc<Engine>, Store) {
    for st in stations {
        store.upsert_station(st).await.unwrap();
    }
    store.add_entry("station-a", "a0", "入口").await.unwrap();
    let client = Arc::new(MockPageClient::new(world));
    let fetcher = Arc::new(Fetcher::new(client));
    let engine = Engine::new(store.clone(), fetcher);
    (engine, store.clone())
}

async fn wait_ok(engine: &Engine, run_id: i64) -> drainage_collector::model::RunProgress {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(70);
    loop {
        let p = engine.progress(run_id).await.unwrap();
        if p.status != "running" {
            assert_eq!(p.status, "succeeded", "轮次失败: {:?}", p.error);
            return p;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "轮次 {run_id} 超时未结束"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

fn triples(
    items: &[drainage_collector::model::ReachableFacility],
) -> BTreeSet<(String, String, i64)> {
    items
        .iter()
        .map(|r| {
            (
                r.station_id.clone(),
                r.page_key.clone(),
                r.revision.unwrap_or(-1),
            )
        })
        .collect()
}

// ---------- 场景 1：跨站回流环失去最后入口 ----------

#[tokio::test]
async fn cycle_losing_last_entry_matches_full_at_same_cutoff() {
    // 站 A：a0(入口) -> a1 -> a2；a1 -> b1（跨站，也是进入环的唯一入口）。
    // 站 B：b1 -> b2 -> b3 -> b1（回流环）。
    let mut a = MockStation::new("station-a");
    a.put_version(1, "a0", 1, fac("A0"), vec![link("station-a", "a1")], "registered");
    a.put_version(
        2,
        "a1",
        1,
        fac("A1"),
        vec![link("station-a", "a2"), link("station-b", "b1")],
        "registered",
    );
    a.put_version(3, "a2", 1, fac("A2"), vec![], "registered");

    let mut b = MockStation::new("station-b");
    b.put_version(1, "b1", 1, fac("B1"), vec![link("station-b", "b2")], "registered");
    b.put_version(2, "b2", 1, fac("B2"), vec![link("station-b", "b3")], "registered");
    b.put_version(3, "b3", 1, fac("B3"), vec![link("station-b", "b1")], "registered");

    let world = MockWorld::new();
    world.add_station(a);
    world.add_station(b);

    let dir = tempfile::tempdir().unwrap();
    let stations = [
        station_cfg("station-a", SiteFormat::Html),
        station_cfg("station-b", SiteFormat::Json),
    ];
    let (engine, _store) = engine_for(&dir, "cycle.db", world.clone(), &stations).await;

    // 基线全量：环可达（b1,b2,b3）。
    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, base).await;
    let base_triples = triples(&engine.reachable(base).await.unwrap());
    for page in ["b1", "b2", "b3"] {
        assert!(
            base_triples.contains(&("station-b".to_string(), page.to_string(), 1)),
            "基线应包含 {page}"
        );
    }

    // 仅把 a1 修订为不再指向 b1：环失去最后一条入口路线。
    let mut a2 = world.get_station("station-a").unwrap();
    let s = a2.latest_seq + 1;
    a2.put_version(s, "a1", 2, fac("A1改"), vec![link("station-a", "a2")], "revised");
    world.add_station(a2);

    // 增量。
    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    wait_ok(&engine, inc).await;
    let inc_triples = triples(&engine.reachable(inc).await.unwrap());

    // 环整体不可达。
    for page in ["b1", "b2", "b3"] {
        assert!(
            !inc_triples.contains(&("station-b".to_string(), page.to_string(), 1)),
            "增量中环页面 {page} 应已失达"
        );
    }
    assert!(inc_triples.contains(&("station-a".to_string(), "a1".to_string(), 2)));
    assert!(inc_triples.contains(&("station-a".to_string(), "a2".to_string(), 1)));

    // 同截止点再跑一次全量，逐页一致。
    let full = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, full).await;
    let full_triples = triples(&engine.reachable(full).await.unwrap());
    assert_eq!(inc_triples, full_triples, "同截止点增量/全量可达关系必须一致");

    // 旧轮次（基线）结果不受后续登记影响。
    let base_after = triples(&engine.reachable(base).await.unwrap());
    assert_eq!(base_after, base_triples, "旧轮次结果应保持不变");
}

// ---------- 场景 3：增量中途退出后接着原轮次继续 ----------

/// 用独立运行时模拟“进程退出”：丢弃运行时即中止后台轮次任务，数据库文件保留。
#[test]
fn interrupted_incremental_resumes_same_run_and_keeps_old_queryable() {
    let a_pages = 200;
    let b_pages = 60;
    let world = build_big_world(a_pages, b_pages);
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("resume_inc.db");
    let db_url = format!("sqlite://{}", db_path.display());
    let stations = [
        station_cfg("station-a", SiteFormat::Html),
        station_cfg("station-b", SiteFormat::Json),
    ];

    // 第一次“进程”：跑基线全量并启动增量，随后在中途丢弃整个运行时。
    let (base_id, inc_id, base_triples) = {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let store = Store::connect(&db_url).await.unwrap();
            for st in &stations {
                let mut st = st.clone();
                st.min_interval_ms = 10; // 放慢，保证增量计划在中断窗口内尚未结束
                store.upsert_station(&st).await.unwrap();
            }
            store.add_entry("station-a", "a0", "入口").await.unwrap();
            let engine = Engine::new(
                store.clone(),
                Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone())))),
            );
            let base = engine.start_run(1, RunMode::Full).await.unwrap();
            wait_ok(&engine, base).await;
            let base_triples = triples(&engine.reachable(base).await.unwrap());

            // 新增一条 120 页支线（独立于主干），让增量计划耗时足够长。
            let mut a = world.get_station("station-a").unwrap();
            let mut seq = a.latest_seq;
            a.put_version(
                seq + 1,
                "a100",
                2,
                fac("A100改"),
                vec![link("station-a", "a101"), link("station-a", "rp0")],
                "revised",
            );
            for i in 0..120 {
                seq += 1;
                let links = if i + 1 < 120 {
                    vec![link("station-a", &format!("rp{}", i + 1))]
                } else {
                    vec![]
                };
                a.put_version(seq, &format!("rp{i}"), 1, fac(&format!("RP{i}")), links, "registered");
            }
            world.add_station(a);

            let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
            // 让它跑一小会儿，此时计划必然尚未完成（120 页 × 10ms 约 1.2s）。
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            (base, inc, base_triples)
        })
        // rt 在此被丢弃：后台轮次任务随运行时中止，等价于进程中途退出。
    };

    // 第二次“进程”：重开数据库，续跑原轮次。
    let rt2 = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (inc_triples, base_after) = rt2.block_on(async {
        let store = Store::connect(&db_url).await.unwrap();
        let engine = Engine::new(
            store.clone(),
            Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone())))),
        );

        // 原轮次仍是 running，续跑它（不新建轮次）。
        let run = store.get_run(inc_id).await.unwrap();
        assert_eq!(run.status.as_str(), "running", "中断的原轮次应仍为 running");
        let resumed = engine.resume_on_boot().await.unwrap();
        assert!(resumed.contains(&inc_id), "应恢复原增量轮次 {inc_id}，实际 {resumed:?}");

        let p = wait_ok(&engine, inc_id).await;
        assert_eq!(p.status, "succeeded");

        // 旧轮次照常查询且结果不变。
        let base_after = triples(&engine.reachable(base_id).await.unwrap());
        assert_eq!(base_after, base_triples, "旧轮次结果应保持不变");

        let inc_items = engine.reachable(inc_id).await.unwrap();

        // 同截止点全量对照。
        let full = engine.start_run(1, RunMode::Full).await.unwrap();
        wait_ok(&engine, full).await;
        let full_triples = triples(&engine.reachable(full).await.unwrap());
        (triples(&inc_items), (base_after, full_triples))
    });

    let (base_after, full_triples) = base_after;
    assert_eq!(base_after, base_triples);
    assert_eq!(
        inc_triples, full_triples,
        "续跑完成的增量结果应与同截止点全量一致"
    );
    assert!(inc_triples.contains(&("station-a".to_string(), "rp119".to_string(), 1)));
}


fn build_big_world(a_pages: usize, b_pages: usize) -> MockWorld {
    let mut a = MockStation::new("station-a");
    for i in 0..a_pages {
        let mut links = Vec::new();
        if i + 1 < a_pages {
            links.push(link("station-a", &format!("a{}", i + 1)));
        }
        if i == 0 {
            links.push(link("station-b", "b0")); // 保证 B 链可达
        } else if i % 50 == 0 {
            links.push(link("station-b", &format!("b{}", i % b_pages)));
        }
        a.put_version(
            (i + 1) as i64,
            &format!("a{i}"),
            1,
            fac(&format!("A{i}")),
            links,
            "registered",
        );
    }
    let mut b = MockStation::new("station-b");
    for i in 0..b_pages {
        let links = if i + 1 < b_pages {
            vec![link("station-b", &format!("b{}", i + 1))]
        } else {
            vec![]
        };
        b.put_version(
            (i + 1) as i64,
            &format!("b{i}"),
            1,
            fac(&format!("B{i}")),
            links,
            "registered",
        );
    }
    let world = MockWorld::new();
    world.add_station(a);
    world.add_station(b);
    world
}

fn io_of(s: IoSnapshot) -> (u64, u64) {
    (
        s.facility_reads + s.edge_reads,
        s.facility_writes + s.edge_writes,
    )
}

#[tokio::test]
async fn large_baseline_spur_stays_within_budget_and_matches_full() {
    let a_pages = 1100;
    let b_pages = 900;
    assert!(a_pages + b_pages >= 2000);
    let world = build_big_world(a_pages, b_pages);

    let stations = [
        // 大基线用 JSON 两站：验证的是“读取/写入条数”口径，与页面方言无关，
        // 且 debug 下 JSON 解析远快于 HTML，保证一条命令 80 秒内跑完。
        station_cfg("station-a", SiteFormat::Json),
        station_cfg("station-b", SiteFormat::Json),
    ];
    // 内存库：本场景不涉及重启，避免文件库 fsync 拖慢 2000 页的多趟遍历。
    let store = Store::connect("sqlite::memory:").await.unwrap();
    let (engine, store) = setup_store(&store, world.clone(), &stations).await;
    // ---- 基线准备（另计，不进预算）----
    let t0 = std::time::Instant::now();
    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    let bp = wait_ok(&engine, base).await;
    eprintln!("baseline full: {:?}, reachable={}", t0.elapsed(), bp.reachable);
    assert!(
        bp.reachable >= 2000,
        "基线应覆盖至少 2000 页，实际 {}",
        bp.reachable
    );

    // ---- 在基线上改动一条 9 页的独立支线 ----
    // 修订 a600（链上可达页）：仅**新增**一条通往 sp0 的边，原有边保持不变；
    // 再登记 sp0..sp7 共 8 页。合计 9 页受影响。
    let mut a = world.get_station("station-a").unwrap();
    let mut seq = a.latest_seq;
    a.put_version(
        seq + 1,
        "a600",
        2,
        fac("A600改"),
        vec![
            link("station-a", "a601"), // 原边：保持
            link("station-b", "b600"), // 原边（600%50==0）：保持
            link("station-a", "sp0"),  // 新增支线
        ],
        "revised",
    );
    for i in 0..8 {
        seq += 1;
        let links = if i + 1 < 8 {
            vec![link("station-a", &format!("sp{}", i + 1))]
        } else {
            vec![]
        };
        a.put_version(
            seq,
            &format!("sp{i}"),
            1,
            fac(&format!("SP{i}")),
            links,
            "registered",
        );
    }
    world.add_station(a);

    // ---- 增量（预算仅统计这一段）----
    store.io.reset();
    let ti = std::time::Instant::now();
    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    let ip = wait_ok(&engine, inc).await;
    let snap = store.io.snapshot();
    let (reads, writes) = io_of(snap);
    eprintln!("incremental: {:?}, io={snap:?}", ti.elapsed());

    assert!(
        reads <= 100,
        "增量设施+边读取应 ≤100 条，实际 {reads} ({snap:?})"
    );
    assert!(
        writes <= 100,
        "增量设施+边写入应 ≤100 条，实际 {writes} ({snap:?})"
    );
    assert!(ip.reachable >= bp.reachable, "支线为新增，可达数不应减少");

    // 支线末端可达。
    let inc_items = engine.reachable(inc).await.unwrap();
    assert!(
        inc_items.iter().any(|r| r.page_key == "sp7" && r.revision == Some(1)),
        "支线末端 sp7 应可达"
    );

    // ---- 同截止点全量对照（另计）----
    let full = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, full).await;
    let full_items = engine.reachable(full).await.unwrap();
    assert_eq!(
        triples(&inc_items),
        triples(&full_items),
        "同截止点增量/全量可达关系必须一致"
    );

    // 旧轮次（基线）仍可查询且不受影响。
    let base_items = engine.reachable(base).await.unwrap();
    assert!(base_items
        .iter()
        .any(|r| r.page_key == "a600" && r.revision == Some(1)));
    assert!(!base_items.iter().any(|r| r.page_key == "sp7"));
}

// ---------- 场景 4：悬空前向引用补全 ----------

#[tokio::test]
async fn dangling_forward_reference_resolved_by_new_registration() {
    // 基线：a0 -> a1，且 a1 有一条指向尚未登记的 x0 的边（悬空前向引用）。
    let mut a = MockStation::new("station-a");
    a.put_version(1, "a0", 1, fac("A0"), vec![link("station-a", "a1")], "registered");
    a.put_version(
        2,
        "a1",
        1,
        fac("A1"),
        vec![link("station-a", "x0")],
        "registered",
    );
    let world = MockWorld::new();
    world.add_station(a);

    let dir = tempfile::tempdir().unwrap();
    let stations = [station_cfg("station-a", SiteFormat::Json)];
    let (engine, _store) = engine_for(&dir, "dangling.db", world.clone(), &stations).await;

    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, base).await;
    // 基线中 x0 抓取失败（404/Gone），不计入可达。
    let base_t = triples(&engine.reachable(base).await.unwrap());
    assert!(!base_t.iter().any(|(_, p, _)| p == "x0"), "基线不应含 x0");

    // 现在登记 x0（a1 的边未变），它应凭既有悬空边变为可达。
    let mut a = world.get_station("station-a").unwrap();
    let s = a.latest_seq + 1;
    a.put_version(s, "x0", 1, fac("X0"), vec![link("station-a", "x1")], "registered");
    a.put_version(s + 1, "x1", 1, fac("X1"), vec![], "registered");
    world.add_station(a);

    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    wait_ok(&engine, inc).await;
    let inc_t = triples(&engine.reachable(inc).await.unwrap());

    let full = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, full).await;
    let full_t = triples(&engine.reachable(full).await.unwrap());
    assert_eq!(inc_t, full_t, "悬空引用补全后增量应与全量一致");
    assert!(inc_t.iter().any(|(_, p, _)| p == "x0"), "增量中 x0 应可达");
    assert!(inc_t.iter().any(|(_, p, _)| p == "x1"), "增量中 x1 应可达");
}

// ---------- 场景 5：页面在截止点前被移除 ----------

#[tokio::test]
async fn removed_page_drops_subtree_consistently_in_incremental() {
    // 基线：a0 -> a1 -> a2 -> a3。a1 还有一条跨站边到 b0 -> b1。
    let mut a = MockStation::new("station-a");
    a.put_version(1, "a0", 1, fac("A0"), vec![link("station-a", "a1")], "registered");
    a.put_version(
        2,
        "a1",
        1,
        fac("A1"),
        vec![link("station-a", "a2"), link("station-b", "b0")],
        "registered",
    );
    a.put_version(3, "a2", 1, fac("A2"), vec![link("station-a", "a3")], "registered");
    a.put_version(4, "a3", 1, fac("A3"), vec![], "registered");
    let mut b = MockStation::new("station-b");
    b.put_version(1, "b0", 1, fac("B0"), vec![link("station-b", "b1")], "registered");
    b.put_version(2, "b1", 1, fac("B1"), vec![], "registered");
    let world = MockWorld::new();
    world.add_station(a);
    world.add_station(b);

    let dir = tempfile::tempdir().unwrap();
    let stations = [
        station_cfg("station-a", SiteFormat::Json),
        station_cfg("station-b", SiteFormat::Json),
    ];
    let (engine, _store) = engine_for(&dir, "removed.db", world.clone(), &stations).await;

    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, base).await;
    let base_t = triples(&engine.reachable(base).await.unwrap());
    assert!(base_t.iter().any(|(_, p, _)| p == "a3"));
    assert!(base_t.iter().any(|(_, p, _)| p == "b1"));

    // 移除 a2：其子树 a3 失达；b 链经 a1->b0 仍可达。
    let mut a = world.get_station("station-a").unwrap();
    let s = a.latest_seq + 1;
    a.remove_version(s, "a2", 2);
    world.add_station(a);

    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    wait_ok(&engine, inc).await;
    let inc_t = triples(&engine.reachable(inc).await.unwrap());
    assert!(!inc_t.iter().any(|(_, p, _)| p == "a2"), "被移除的 a2 不应可达");
    assert!(!inc_t.iter().any(|(_, p, _)| p == "a3"), "a2 的子页 a3 应失达");
    assert!(inc_t.iter().any(|(_, p, _)| p == "b1"), "b 链仍应可达");

    let full = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, full).await;
    let full_t = triples(&engine.reachable(full).await.unwrap());
    assert_eq!(inc_t, full_t, "页面移除后增量应与全量一致");
}
