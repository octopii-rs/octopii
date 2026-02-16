use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use octopii::OctopiiNode;
use std::sync::Arc;

use crate::types::{GetResponse, HealthResponse, SetRequest, StatusResponse};

pub async fn put_key(
    State(node): State<Arc<OctopiiNode>>,
    Path(key): Path<String>,
    Json(body): Json<SetRequest>,
) -> (StatusCode, Json<StatusResponse>) {
    let command = format!("SET {} {}", key, body.value);

    match node.propose(command.into_bytes()).await {
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
    State(node): State<Arc<OctopiiNode>>,
    Path(key): Path<String>,
) -> (StatusCode, Json<GetResponse>) {
    let command = format!("GET {}", key);

    match node.query(command.as_bytes()).await {
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
    State(node): State<Arc<OctopiiNode>>,
    Path(key): Path<String>,
) -> (StatusCode, Json<StatusResponse>) {
    let command = format!("DELETE {}", key);

    match node.propose(command.into_bytes()).await {
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

pub async fn health(State(node): State<Arc<OctopiiNode>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        node_id: node.id(),
        is_leader: node.is_leader().await,
        has_leader: node.has_leader().await,
    })
}
