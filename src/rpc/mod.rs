mod handler;

pub use handler::RpcHandler;

use crate::error::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub type MessageId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    Request(RpcRequest),
    Response(RpcResponse),
    OneWay(OneWayMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub id: MessageId,
    pub payload: RequestPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestPayload {
    #[cfg(feature = "openraft")]
    OpenRaft {
        kind: String,
        data: Bytes,
    },
    Custom {
        operation: String,
        data: Bytes,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: MessageId,
    pub payload: ResponsePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponsePayload {
    #[cfg(feature = "openraft")]
    OpenRaft {
        kind: String,
        data: Bytes,
    },
    CustomResponse {
        success: bool,
        data: Bytes,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OneWayMessage {
    Heartbeat { node_id: u64, timestamp: u64 },
    Custom { operation: String, data: Bytes },
}

impl RpcMessage {
    pub fn new_request(id: MessageId, payload: RequestPayload) -> Self {
        RpcMessage::Request(RpcRequest { id, payload })
    }

    pub fn new_response(id: MessageId, payload: ResponsePayload) -> Self {
        RpcMessage::Response(RpcResponse { id, payload })
    }

    pub fn new_one_way(message: OneWayMessage) -> Self {
        RpcMessage::OneWay(message)
    }

    pub fn message_id(&self) -> Option<MessageId> {
        match self {
            RpcMessage::Request(req) => Some(req.id),
            RpcMessage::Response(resp) => Some(resp.id),
            RpcMessage::OneWay(_) => None,
        }
    }
}

pub fn serialize<T: Serialize>(msg: &T) -> Result<Bytes> {
    Ok(Bytes::from(bincode::serialize(msg)?))
}

pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    Ok(bincode::deserialize(data)?)
}
