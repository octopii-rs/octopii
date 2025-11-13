#![cfg(feature = "openraft")]

use crate::rpc::{RequestPayload, RpcHandler, RpcMessage, ResponsePayload};
use crate::transport::QuicTransport;
use crate::openraft::types::{AppNodeId, AppTypeConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// In-process network shim for OpenRaft.
///
/// NOTE: Initial implementation is in-process only to get tests green quickly.
/// TODO(quic): Replace send_* impls to serialize OpenRaft RPC messages and send
/// over QUIC using `RpcHandler`/`QuicTransport`.
pub struct InProcNetwork {
	_peers: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, tokio::sync::mpsc::UnboundedSender<RpcMessage>>>>,
}

impl InProcNetwork {
	pub fn new() -> Self {
		Self {
			_peers: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
		}
	}

	/// Register a peer message sender for in-proc delivery.
	pub fn register(&self, id: AppNodeId, tx: tokio::sync::mpsc::UnboundedSender<RpcMessage>) {
		let mut g = self._peers.blocking_write();
		g.insert(id, tx);
	}
}

/// Placeholder trait adapter signatures to mirror OpenRaft without binding here.
#[allow(dead_code)]
impl InProcNetwork {
	pub async fn send_append_entries(&self, _target: AppNodeId, _data: Vec<u8>) -> anyhow::Result<()> {
		// TODO(openraft): implement via in-proc channel; QUIC later.
		Ok(())
	}
	pub async fn send_install_snapshot(&self, _target: AppNodeId, _data: Vec<u8>) -> anyhow::Result<()> {
		Ok(())
	}
	pub async fn send_vote(&self, _target: AppNodeId, _data: Vec<u8>) -> anyhow::Result<()> {
		Ok(())
	}
}

/// QUIC-backed network for OpenRaft messages.
pub struct QuinnNetwork {
	rpc: Arc<RpcHandler>,
	peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
}

impl QuinnNetwork {
	pub fn new(rpc: Arc<RpcHandler>, peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>) -> Self {
		Self { rpc, peer_addrs }
	}

	async fn peer_addr(&self, target: AppNodeId) -> Option<SocketAddr> {
		let g = self.peer_addrs.read().await;
		g.get(&target).copied()
	}

	pub async fn send_openraft(&self, target: AppNodeId, kind: &str, data: Vec<u8>) -> anyhow::Result<()> {
		let Some(addr) = self.peer_addr(target).await else {
			anyhow::bail!("no address for peer {}", target);
		};
		let payload = RequestPayload::OpenRaft { kind: kind.to_string(), data: bytes::Bytes::from(data) };
		let resp = self.rpc.request(addr, payload, tokio::time::Duration::from_secs(5)).await?;
		match resp.payload {
			ResponsePayload::CustomResponse { success, .. } if success => Ok(()),
			ResponsePayload::AppendEntriesResponse { .. } => Ok(()),
			ResponsePayload::RequestVoteResponse { .. } => Ok(()),
			ResponsePayload::SnapshotResponse { .. } => Ok(()),
			other => anyhow::bail!("unexpected response: {:?}", other),
		}
	}
}


