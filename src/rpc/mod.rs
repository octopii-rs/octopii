mod message;
mod handler;

pub use message::{
    RpcMessage, RpcRequest, RpcResponse, MessageId,
    RequestPayload, ResponsePayload, OneWayMessage
};
pub use handler::RpcHandler;

use crate::error::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Serialize an RPC message to bytes
pub fn serialize<T: Serialize>(msg: &T) -> Result<Bytes> {
    let data = bincode::serialize(msg)?;
    Ok(Bytes::from(data))
}

/// Deserialize an RPC message from bytes
pub fn deserialize<T: for<'de> Deserialize<'de>>(data: &[u8]) -> Result<T> {
    let msg = bincode::deserialize(data)?;
    Ok(msg)
}
