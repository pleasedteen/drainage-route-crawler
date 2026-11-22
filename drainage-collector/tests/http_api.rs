//! HTTP API 端到端测试：起真实监听，用 reqwest 走完整链路。
mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use drainage_collector::api::{router, AppState};
use drainage_collector::engine::Engine;
use drainage_collector::fetch::Fetcher;
use drainage_collector::mock::MockPageClient;
use drainage_collector::seed::seed_demo_stations;
use drainage_collector::store::Store;

async fn spawn_app() -> (String, Arc<Engine>) {
    let store = Store::connect("sqlite::memory:").await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    let world = drainage_collector::mock::MockWorld::demo();
    let fetcher = Arc::new(Fetcher::new(Arc::new(MockPageClient::new(world))));
    let engine = Engine::new(store.clone(), fetcher);
    let app = router(AppState {
        engine: engine.clone(),
        store,
    });
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), engine)
}

#[tokio::test]
async fn http_api_full_then_incremental_and_queries() {
    let (base, engine) = spawn_app().await;
    let http = reqwest::Client::new();

    // 健康检查与基础数据。
    let health: serde_json::Value = http
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    let entries: Vec<serde_json::Value> = http
        .get(format!("{base}/entries"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);

    // 全量采集。
    let started: serde_json::Value = http
        .post(format!("{base}/runs/start"))
        .json(&serde_json::json!({"entry_id": 1, "mode": "full"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let full_id = started["run_id"].as_i64().unwrap();
    common::wait_run(&engine, full_id).await;

    // 进度端点。
    let progress: serde_json::Value = http
        .get(format!("{base}/runs/{full_id}/progress"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(progress["status"], "succeeded");
    assert_eq!(progress["reachable"], 8);

    // 可达设施端点。
    let reach: Vec<serde_json::Value> = http
        .get(format!("{base}/runs/{full_id}/reachable"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reach.len(), 8);

    // 增量采集，结果与全量一致。
    let started2: serde_json::Value = http
        .post(format!("{base}/runs/start"))
        .json(&serde_json::json!({"entry_id": 1, "mode": "incremental"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let inc_id = started2["run_id"].as_i64().unwrap();
    common::wait_run(&engine, inc_id).await;
    assert_eq!(
        engine
            .reachable_keys_for_equivalence(full_id)
            .await
            .unwrap(),
        engine.reachable_keys_for_equivalence(inc_id).await.unwrap()
    );

    // 入口视角查询。
    let entry_view: serde_json::Value = http
        .get(format!("{base}/entries/1/reachable"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(entry_view["facilities"].as_array().unwrap().len(), 8);

    // 非法模式返回 400。
    let bad = http
        .post(format!("{base}/runs/start"))
        .json(&serde_json::json!({"entry_id": 1, "mode": "weird"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);

    // 修复接口：把轮次截止点回拨到早期后，POST /runs/:id/repair 后台修复并一次切换。
    {
        use sqlx::Row;
        sqlx::query("UPDATE runs SET watermarks = ? WHERE id = ?")
            .bind(r#"{"station-a":4,"station-b":7}"#)
            .bind(full_id)
            .execute(&engine.store.pool)
            .await
            .unwrap();
        let _: i64 = sqlx::query("SELECT COUNT(*) AS c FROM runs")
            .fetch_one(&engine.store.pool)
            .await
            .unwrap()
            .try_get("c")
            .unwrap();
    }
    let pre = http
        .get(format!("{base}/runs/{full_id}/reachable"))
        .send()
        .await
        .unwrap()
        .json::<Vec<serde_json::Value>>()
        .await
        .unwrap();
    assert_eq!(pre.len(), 8, "回拨后仍是污染的 8 项");

    let resp = http
        .post(format!("{base}/runs/{full_id}/repair"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // 轮询修复状态直到成功。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let st = http
            .get(format!("{base}/runs/{full_id}/repair-status"))
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        if st["status"] == "succeeded" {
            assert_eq!(st["published_generation"], 2);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "修复超时: {st}");
        assert!(
            st["status"] == "running" || st["status"] == "none",
            "修复状态意外: {st}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }

    let post = http
        .get(format!("{base}/runs/{full_id}/reachable"))
        .send()
        .await
        .unwrap()
        .json::<Vec<serde_json::Value>>()
        .await
        .unwrap();
    assert_eq!(post.len(), 7, "修复后应为截止点内 7 项");
    assert!(!post.iter().any(|f| f["page_key"] == "a4"));
}
