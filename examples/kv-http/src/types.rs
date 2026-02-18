use crate::sharded::RoutingInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct SetRequest {
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct ShardedGetResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub routing: RoutingInfo,
}

#[derive(Debug, Serialize)]
pub struct ShardedPutResponse {
    pub ok: bool,
    pub routing: RoutingInfo,
}

#[derive(Debug, Serialize)]
pub struct ShardedDeleteResponse {
    pub deleted: bool,
    pub routing: RoutingInfo,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InternalGetResponse {
    pub value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InternalPutRequest {
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct GetResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub node_id: u64,
    pub is_leader: bool,
    pub has_leader: bool,
}
