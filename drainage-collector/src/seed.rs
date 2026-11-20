//! 演示数据播种：写入站点与入流口，可选地起本地 HTML / JSON 模拟站。
use std::sync::Arc;

use anyhow::Result;

use crate::config::AppConfig;
use crate::fetch::{Fetcher, HttpPageClient};
use crate::mock::{MockPageClient, MockWorld};
use crate::model::SiteFormat;
use crate::store::Store;

/// 准备运行所需的抓取器。本地演示（配置含 mock 段）使用进程内模拟站，
/// 生产配置则使用 HTTP 客户端。
pub async fn build_fetcher(cfg: &AppConfig) -> Result<Arc<Fetcher>> {
    if cfg.mock.is_some() {
        let world = MockWorld::demo();
        let client = Arc::new(MockPageClient::new(world));
        return Ok(Arc::new(Fetcher::new(client)));
    }
    let client = Arc::new(HttpPageClient::new()?);
    Ok(Arc::new(Fetcher::new(client)))
}

/// 站点与入流口首次入库（重复执行幂等）。
pub async fn seed_reference_data(store: &Store, cfg: &AppConfig) -> Result<()> {
    for st in &cfg.stations {
        store.upsert_station(st).await?;
    }
    let existing = store.list_entries().await?;
    for e in &cfg.entry_points {
        let exists = existing
            .iter()
            .any(|x| x.station_id == e.station_id && x.page_key == e.page_key);
        if !exists {
            store
                .add_entry(&e.station_id, &e.page_key, &e.label)
                .await?;
        }
    }
    Ok(())
}

/// 仅测试/示例使用：构造内存模拟站与内存数据库。
pub async fn demo_store_and_fetcher() -> Result<(Store, Arc<Fetcher>, MockWorld)> {
    let store = Store::connect("sqlite::memory:").await?;
    let world = MockWorld::demo();
    let client = Arc::new(MockPageClient::new(world.clone()));
    let fetcher = Arc::new(Fetcher::new(client));
    seed_demo_stations(&store).await?;
    Ok((store, fetcher, world))
}

pub async fn seed_demo_stations(store: &Store) -> Result<()> {
    use crate::model::StationConfig;
    store
        .upsert_station(&StationConfig {
            id: "station-a".into(),
            name: "城东养护站".into(),
            base_url: "mock://station-a".into(),
            format: SiteFormat::Html,
            min_interval_ms: 5,
            max_inflight: 2,
            watermark_seq: 0,
        })
        .await?;
    store
        .upsert_station(&StationConfig {
            id: "station-b".into(),
            name: "河西养护站".into(),
            base_url: "mock://station-b".into(),
            format: SiteFormat::Json,
            min_interval_ms: 5,
            max_inflight: 2,
            watermark_seq: 0,
        })
        .await?;
    store.add_entry("station-a", "a0", "城东入流口").await?;
    Ok(())
}
