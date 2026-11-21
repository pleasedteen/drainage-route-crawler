//! 确定性随机图上的增量/全量一致性回归。
//!
//! 每次迭代构造一个含跨站链接、汇合与回流环的小图，跑全量得到基线；随后随机
//! 修订页面、增删链接、登记新页，再跑增量与同截止点全量，逐页比对可达关系。
//! 用于覆盖面广地保护“受影响区域重算 + 区域外继承”的正确性。
mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::{MockPageClient, MockWorld};
use drainage_collector::model::{FacilityInfo, LinkRef, RunMode, SiteFormat, StationConfig};
use drainage_collector::store::Store;

/// 简单确定性 LCG，避免引入额外依赖。
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

fn fac(name: &str) -> FacilityInfo {
    FacilityInfo {
        name: Some(name.into()),
        kind: Some("manhole".into()),
    }
}

fn cfg(id: &str, f: SiteFormat) -> StationConfig {
    StationConfig {
        id: id.into(),
        name: id.into(),
        base_url: format!("mock://{id}"),
        format: f,
        min_interval_ms: 0,
        max_inflight: 4,
        watermark_seq: 0,
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

async fn wait_ok(engine: &Engine, run_id: i64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let p = engine.progress(run_id).await.unwrap();
        if p.status != "running" {
            assert_eq!(p.status, "succeeded", "轮次失败: {:?}", p.error);
            return;
        }
        assert!(std::time::Instant::now() < deadline, "轮次超时");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

fn all_pages(a: u64, b: u64) -> Vec<(String, String)> {
    let mut v = Vec::new();
    for i in 0..a {
        v.push(("station-a".to_string(), format!("a{i}")));
    }
    for i in 0..b {
        v.push(("station-b".to_string(), format!("b{i}")));
    }
    v
}

fn random_links(rng: &mut Rng, pages: &[(String, String)], max: u64) -> Vec<LinkRef> {
    let k = rng.below(max + 1);
    let mut out = Vec::new();
    for _ in 0..k {
        let (s, p) = &pages[rng.below(pages.len() as u64) as usize];
        let l = LinkRef {
            station_id: s.clone(),
            page_key: p.clone(),
        };
        if !out.iter().any(|x: &LinkRef| x.station_id == l.station_id && x.page_key == l.page_key)
        {
            out.push(l);
        }
    }
    out
}

fn set_version(world: &MockWorld, station: &str, page: &str, rev: i64, links: Vec<LinkRef>, kind: &str) {
    let mut st = world.get_station(station).unwrap();
    let seq = st.latest_seq + 1;
    st.put_version(seq, page, rev, fac(&format!("{page}r{rev}")), links, kind);
    world.add_station(st);
}

#[tokio::test]
async fn randomized_incremental_matches_full() {
    for seed in 0..80u64 {
        run_case(seed).await;
    }
}

async fn run_case(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1));
    let n_a = 1 + rng.below(6); // 1..6
    let n_b = rng.below(5); // 0..4

    // 建初始世界。
    let world = MockWorld::new();
    let mut a = drainage_collector::mock::MockStation::new("station-a");
    let mut b = drainage_collector::mock::MockStation::new("station-b");
    let mut pages = all_pages(n_a, n_b);
    // a0 是入口，至少连一条边。
    for (idx, (station, page)) in pages.clone().iter().enumerate() {
        let mut links = random_links(&mut rng, &pages, 3);
        if idx == 0 && links.is_empty() {
            links.push(LinkRef {
                station_id: "station-a".into(),
                page_key: "a0".into(),
            }); // 自环，保证有图
        }
        let seq = {
            let st = if station == "station-a" { &a } else { &b };
            st.latest_seq + 1
        };
        if station == "station-a" {
            a.put_version(seq, page, 1, fac(page), links, "registered");
        } else {
            b.put_version(seq, page, 1, fac(page), links, "registered");
        }
    }
    world.add_station(a);
    world.add_station(b);

    let store = Store::connect("sqlite::memory:").await.unwrap();
    store
        .upsert_station(&cfg("station-a", SiteFormat::Json))
        .await
        .unwrap();
    store
        .upsert_station(&cfg("station-b", SiteFormat::Html))
        .await
        .unwrap();
    store.add_entry("station-a", "a0", "入口").await.unwrap();
    let engine = Engine::new(
        store,
        Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world.clone())))),
    );

    // 基线全量。
    let base = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, base).await;

    // 随机变更：修订若干页的链接、登记若干新页。
    let mut next_a = world.get_station("station-a").unwrap().latest_seq;
    let mut next_b = world.get_station("station-b").unwrap().latest_seq;
    let _ = (&mut next_a, &mut next_b);

    let revisions = 1 + rng.below(3);
    for _ in 0..revisions {
        let (station, page) = pages[rng.below(pages.len() as u64) as usize].clone();
        let links = random_links(&mut rng, &pages, 3);
        let st = world.get_station(&station).unwrap();
        let existing = st
            .pages
            .get(&page)
            .map(|v| v.last().unwrap().revision + 1)
            .unwrap_or(2);
        set_version(&world, &station, &page, existing, links, "revised");
    }
    // 新增页面。
    let new_pages = rng.below(3);
    for i in 0..new_pages {
        let station = if rng.chance(50) { "station-a" } else { "station-b" };
        let page = format!("new{i}");
        pages.push((station.to_string(), page.clone()));
        let links = random_links(&mut rng, &pages, 2);
        set_version(&world, station, &page, 1, links, "registered");
    }

    // 增量 + 同截止点全量。
    let inc = engine.start_run(1, RunMode::Incremental).await.unwrap();
    wait_ok(&engine, inc).await;
    let full = engine.start_run(1, RunMode::Full).await.unwrap();
    wait_ok(&engine, full).await;

    let inc_t = triples(&engine.reachable(inc).await.unwrap());
    let full_t = triples(&engine.reachable(full).await.unwrap());
    assert_eq!(inc_t, full_t, "seed={seed} 增量与全量不一致");
    // 基线仍可查、不受影响。
    let _ = triples(&engine.reachable(base).await.unwrap());
}
