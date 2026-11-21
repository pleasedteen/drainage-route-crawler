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

async fn spawn_app() -> (String, Arc<Engine>, tempfile::TempDir) {
    // 用文件型 SQLite 并放慢站点节奏，保证暂停命令发出时轮次仍在进行。
    let dir = tempfile::tempdir().unwrap();
    let db = format!("sqlite://{}", dir.path().join("http.db").display());
    let store = Store::connect(&db).await.unwrap();
    seed_demo_stations(&store).await.unwrap();
    for mut st in store.list_stations().await.unwrap() {
        st.min_interval_ms = 30;
        st.max_inflight = 1;
        store.upsert_station(&st).await.unwrap();
    }
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
    (format!("http://{addr}"), engine, dir)
}

#[tokio::test]
async fn http_api_full_then_incremental_and_queries() {
    let (base, engine, _dir) = spawn_app().await;
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
}


/// 暂停 -> 查询可见 -> 同入口拒绝新轮次 -> 继续（沿用原轮次）-> 完成。
#[tokio::test]
async fn http_pause_resume_round_trip() {
    let (base, engine, _dir) = spawn_app().await;
    let http = reqwest::Client::new();

    let started: serde_json::Value = http
        .post(format!("{base}/runs/start"))
        .json(&serde_json::json!({"entry_id": 1, "mode": "full"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = started["run_id"].as_i64().unwrap();
    // 等轮次真正开始产出进度（站点放慢，此时必然仍在进行）。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let p: serde_json::Value = http
            .get(format!("{base}/runs/{run_id}/progress"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if p["done"].as_i64().unwrap_or(0) >= 2 && p["status"] == "running" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "轮次迟迟没有进度");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // 暂停。
    let paused: (reqwest::StatusCode, serde_json::Value) = {
        let resp = http
            .post(format!("{base}/runs/{run_id}/pause"))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap())
    };
    assert_eq!(paused.0, 200);
    {
        assert_eq!(paused.1["status"], "paused");
        assert!(paused.1["paused_at"].as_i64().is_some());

        // 进度查询能看出已确认暂停。
        let p: serde_json::Value = http
            .get(format!("{base}/runs/{run_id}/progress"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(p["status"], "paused");
        assert!(p["paused_at"].as_i64().is_some());
        assert_eq!(p["fetching"], 0);
        // 同入口不能另起轮次。
        let dup = http
            .post(format!("{base}/runs/start"))
            .json(&serde_json::json!({"entry_id": 1, "mode": "full"}))
            .send()
            .await
            .unwrap();
        assert_eq!(dup.status(), 409);
        // 继续：沿用原轮次 ID。
        let resumed: serde_json::Value = http
            .post(format!("{base}/runs/{run_id}/resume"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resumed["run_id"], run_id);
        assert_eq!(resumed["status"], "running");

        // 重复继续被拒绝。
        let again = http
            .post(format!("{base}/runs/{run_id}/resume"))
            .send()
            .await
            .unwrap();
        assert_eq!(again.status(), 409);
    }
    let p = common::wait_run(&engine, run_id).await;
    assert_eq!(p.status, "succeeded", "{:?}", p.error);
    assert_eq!(p.reachable, 8);
}
