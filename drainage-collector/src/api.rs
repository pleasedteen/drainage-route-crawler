//! HTTP API：值班员操作采集与查询进度/可达设施。
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::engine::Engine;
use crate::model::RunMode;
use crate::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    pub store: Store,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/stations", get(list_stations))
        .route("/entries", get(list_entries))
        .route("/runs", get(list_runs))
        .route("/runs/start", post(start_run))
        .route("/runs/:run_id", get(get_run))
        .route("/runs/:run_id/progress", get(run_progress))
        .route("/runs/:run_id/reachable", get(run_reachable))
        .route("/entries/:entry_id/reachable", get(entry_reachable))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn list_stations(State(s): State<AppState>) -> impl IntoResponse {
    match s.store.list_stations().await {
        Ok(v) => Json(v).into_response(),
        Err(e) => internal(e),
    }
}

async fn list_entries(State(s): State<AppState>) -> impl IntoResponse {
    match s.store.list_entries().await {
        Ok(v) => Json(v).into_response(),
        Err(e) => internal(e),
    }
}

#[derive(Deserialize)]
struct StartBody {
    entry_id: i64,
    /// full | incremental
    mode: String,
}

#[derive(Serialize)]
struct StartResp {
    run_id: i64,
    status: String,
}

async fn start_run(
    State(s): State<AppState>,
    Json(body): Json<StartBody>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let mode = RunMode::parse(&body.mode).map_err(bad_request)?;
    match s.engine.start_run(body.entry_id, mode).await {
        Ok(run_id) => Ok(Json(StartResp {
            run_id,
            status: "running".into(),
        })),
        Err(e) => Err((StatusCode::CONFLICT, e.to_string())),
    }
}

#[derive(Deserialize)]
struct RunListQuery {
    limit: Option<i64>,
}

async fn list_runs(State(s): State<AppState>, Query(q): Query<RunListQuery>) -> impl IntoResponse {
    match s.store.list_runs(q.limit.unwrap_or(20)).await {
        Ok(v) => {
            let view: Vec<_> = v
                .into_iter()
                .map(|r| {
                    serde_json::json!({
                        "id": r.id,
                        "entry_id": r.entry_id,
                        "mode": r.mode.as_str(),
                        "status": r.status.as_str(),
                        "watermarks": r.watermarks,
                        "error": r.error,
                        "created_at": r.created_at,
                        "updated_at": r.updated_at,
                    })
                })
                .collect();
            Json(view).into_response()
        }
        Err(e) => internal(e),
    }
}

async fn get_run(
    State(s): State<AppState>,
    Path(run_id): Path<i64>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let run = s
        .store
        .get_run(run_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "id": run.id,
        "entry_id": run.entry_id,
        "mode": run.mode.as_str(),
        "status": run.status.as_str(),
        "watermarks": run.watermarks,
        "error": run.error,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    })))
}

async fn run_progress(
    State(s): State<AppState>,
    Path(run_id): Path<i64>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    s.engine
        .progress(run_id)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn run_reachable(
    State(s): State<AppState>,
    Path(run_id): Path<i64>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    s.engine
        .reachable(run_id)
        .await
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

async fn entry_reachable(
    State(s): State<AppState>,
    Path(entry_id): Path<i64>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    match s
        .engine
        .entry_reachable(entry_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        Some((run_id, items)) => Ok(Json(serde_json::json!({
            "run_id": run_id,
            "entry_id": entry_id,
            "facilities": items,
        }))),
        None => Ok(Json(serde_json::json!({
            "run_id": null,
            "entry_id": entry_id,
            "facilities": [],
            "message": "该入口尚无成功轮次",
        }))),
    }
}

fn bad_request(e: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}

fn internal(e: anyhow::Error) -> axum::response::Response {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}
