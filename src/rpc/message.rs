use serde::{Deserialize, Serialize};
use bytes::Bytes;

/// Unique identifier for RPC messages
pub type MessageId = u64;

/// RPC message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    /// Request with expectation of response
    Request(RpcRequest),
    /// Response to a request
    Response(RpcResponse),
    /// One-way message (no response expected)
    OneWay(OneWayMessage),
}

/// RPC request types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub id: MessageId,
    pub payload: RequestPayload,
}

/// Request payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestPayload {
    /// Raft AppendEntries RPC
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Bytes>,
        leader_commit: u64,
    },
    /// Raft RequestVote RPC
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    /// Raft Snapshot transfer
    RaftSnapshot {
        term: u64,
        leader_id: u64,
        snapshot_index: u64,
        snapshot_term: u64,
        snapshot_data: Bytes,
        conf_state_data: Bytes,
    },
    /// Custom application-level request
    Custom {
        operation: String,
        data: Bytes,
    },
}

/// RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub id: MessageId,
    pub payload: ResponsePayload,
}

/// Response payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponsePayload {
    /// AppendEntries response
    AppendEntriesResponse {
        term: u64,
        success: bool,
    },
    /// RequestVote response
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    /// Snapshot response
    SnapshotResponse {
        term: u64,
        success: bool,
    },
    /// Custom application response
    CustomResponse {
        success: bool,
        data: Bytes,
    },
    /// Error response
    Error {
        message: String,
    },
}

/// One-way message (no response expected)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OneWayMessage {
    /// Heartbeat
    Heartbeat { node_id: u64, timestamp: u64 },
    /// Custom one-way message
    Custom { operation: String, data: Bytes },
}

impl RpcMessage {
    /// Create a new request
    pub fn new_request(id: MessageId, payload: RequestPayload) -> Self {
        RpcMessage::Request(RpcRequest { id, payload })
    }

    /// Create a new response
    pub fn new_response(id: MessageId, payload: ResponsePayload) -> Self {
        RpcMessage::Response(RpcResponse { id, payload })
    }

    /// Create a new one-way message
    pub fn new_one_way(message: OneWayMessage) -> Self {
        RpcMessage::OneWay(message)
    }

    /// Get the message ID if this is a request or response
    pub fn message_id(&self) -> Option<MessageId> {
        match self {
            RpcMessage::Request(req) => Some(req.id),
            RpcMessage::Response(resp) => Some(resp.id),
            RpcMessage::OneWay(_) => None,
        }
    }
}
