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
	self_id: AppNodeId,
	#[cfg(feature = "openraft-filters")]
	pub(crate) filters: Arc<OpenRaftFilters>,
}

impl QuinnNetwork {
	pub fn new(
		rpc: Arc<RpcHandler>,
		peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
		self_id: AppNodeId,
		#[cfg(feature = "openraft-filters")] filters: Arc<OpenRaftFilters>,
	) -> Self {
		Self {
			rpc,
			peer_addrs,
			self_id,
			#[cfg(feature = "openraft-filters")]
			filters,
		}
	}

	async fn peer_addr(&self, target: AppNodeId) -> Option<SocketAddr> {
		let g = self.peer_addrs.read().await;
		g.get(&target).copied()
	}

	pub async fn send_openraft(&self, target: AppNodeId, kind: &str, data: Vec<u8>) -> anyhow::Result<ResponsePayload> {
		#[cfg(feature = "openraft-filters")]
		{
			// Partition check
			for (g1, g2) in self.filters.partitions.read().await.iter() {
				if (g1.contains(&self.self_id) && g2.contains(&target))
					|| (g2.contains(&self.self_id) && g1.contains(&target))
				{
					anyhow::bail!("openraft-filters: partition drop {}->{target}", self.self_id);
				}
			}
			// Pair-specific drop
			if self.filters.drop_pairs.read().await.contains(&(self.self_id, target)) {
				anyhow::bail!("openraft-filters: drop pair {}->{target}", self.self_id);
			}
			// Delay if configured
			if let Some(d) = self.filters.delay_pairs.read().await.get(&(self.self_id, target)).copied() {
				tokio::time::sleep(d).await;
			}
		}

		let Some(addr) = self.peer_addr(target).await else {
			anyhow::bail!("no address for peer {}", target);
		};
		let payload = RequestPayload::OpenRaft { kind: kind.to_string(), data: bytes::Bytes::from(data) };
		let resp = self.rpc.request(addr, payload, tokio::time::Duration::from_secs(5)).await?;
		Ok(resp.payload)
	}
}

#[cfg(feature = "openraft-filters")]
pub struct OpenRaftFilters {
	pub(crate) drop_pairs: tokio::sync::RwLock<std::collections::HashSet<(AppNodeId, AppNodeId)>>,
	pub(crate) delay_pairs: tokio::sync::RwLock<std::collections::HashMap<(AppNodeId, AppNodeId), Duration>>,
	pub(crate) partitions: tokio::sync::RwLock<Vec<(std::collections::HashSet<AppNodeId>, std::collections::HashSet<AppNodeId>)>>,
}

#[cfg(feature = "openraft-filters")]
impl OpenRaftFilters {
	pub fn new() -> Self {
		Self {
			drop_pairs: tokio::sync::RwLock::new(Default::default()),
			delay_pairs: tokio::sync::RwLock::new(Default::default()),
			partitions: tokio::sync::RwLock::new(vec![]),
		}
	}
	pub async fn clear(&self) {
		self.drop_pairs.write().await.clear();
		self.delay_pairs.write().await.clear();
		self.partitions.write().await.clear();
	}
}


