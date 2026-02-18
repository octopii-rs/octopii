use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use octopii::OctopiiNode;
use std::sync::Arc;

use crate::sharded::ShardedStore;
use crate::types::{
    GetResponse, HealthResponse, InternalGetResponse, InternalPutRequest, SetRequest,
    ShardedDeleteResponse, ShardedGetResponse, ShardedPutResponse, StatusResponse,
};

pub struct AppState {
    pub node: Arc<OctopiiNode>,
    pub sharded: Arc<ShardedStore>,
}

pub async fn put_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Json(body): Json<SetRequest>,
) -> (StatusCode, Json<StatusResponse>) {
    let command = format!("SET {} {}", key, body.value);

    match state.node.propose(command.into_bytes()).await {
        Ok(_) => (
            StatusCode::OK,
            Json(StatusResponse {
                status: "ok".into(),
            }),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(StatusResponse {
                status: format!("error: {}", e),
            }),
        ),
    }
}

pub async fn get_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    let command = format!("GET {}", key);

    match state.node.query(command.as_bytes()).await {
        Ok(result) => {
            let value = String::from_utf8_lossy(&result);
            if value == "NOT_FOUND" {
                (
                    StatusCode::NOT_FOUND,
                    Json(GetResponse {
                        value: None,
                        error: Some("NOT_FOUND".into()),
                    }),
                )
            } else {
                (
                    StatusCode::OK,
                    Json(GetResponse {
                        value: Some(value.into_owned()),
                        error: None,
                    }),
                )
            }
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GetResponse {
                value: None,
                error: Some(e.to_string()),
            }),
        ),
    }
}

pub async fn delete_key(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> (StatusCode, Json<StatusResponse>) {
    let command = format!("DELETE {}", key);

    match state.node.propose(command.into_bytes()).await {
        Ok(_) => (
            StatusCode::OK,
            Json(StatusResponse {
                status: "ok".into(),
            }),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(StatusResponse {
                status: format!("error: {}", e),
            }),
        ),
    }
}

pub async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        node_id: state.node.id(),
        is_leader: state.node.is_leader().await,
        has_leader: state.node.has_leader().await,
    })
}

// Sharded KV endpoints

pub async fn sharded_get(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<ShardedGetResponse> {
    let (value, routing) = state.sharded.get(&key).await;
    Json(ShardedGetResponse { value, routing })
}

pub async fn sharded_put(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Json(body): Json<SetRequest>,
) -> Json<ShardedPutResponse> {
    let (ok, routing) = state.sharded.put(&key, &body.value).await;
    Json(ShardedPutResponse { ok, routing })
}

pub async fn sharded_delete(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<ShardedDeleteResponse> {
    let (deleted, routing) = state.sharded.delete(&key).await;
    Json(ShardedDeleteResponse { deleted, routing })
}

// Internal endpoints for node-to-node forwarding

pub async fn sharded_internal_get(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Json<InternalGetResponse> {
    let value = state.sharded.local_get(&key);
    Json(InternalGetResponse { value })
}

pub async fn sharded_internal_put(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    Json(body): Json<InternalPutRequest>,
) -> StatusCode {
    state.sharded.local_put(&key, &body.value);
    StatusCode::OK
}

pub async fn sharded_internal_delete(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> StatusCode {
    state.sharded.local_delete(&key);
    StatusCode::OK
}
