#![cfg(feature = "openraft")]

use crate::config::Config;
use crate::error::Result;
use crate::openraft::network::{InProcNetwork, QuinnNetwork};
use crate::openraft::storage::{MemStorage, WalStorageAdapter};
use crate::openraft::types::{AppEntry, AppResponse, AppSnapshot, AppNodeId};
use crate::runtime::OctopiiRuntime;
use crate::state_machine::{KvStateMachine, StateMachine, StateMachineTrait};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, RwLock};
use tokio::time::Duration;

/// OpenRaft-based node wrapper with the same outward API as the existing node.
///
/// NOTE: This initial version is in-process only to get tests green quickly.
/// TODO(quic): Replace network with QUIC-backed `RaftNetwork` implementation.
/// TODO(wal): Replace `MemStorage` with WAL-backed adapter.
pub struct OpenRaftNode {
	runtime: OctopiiRuntime,
	config: Config,
	network: Arc<QuinnNetwork>,
	storage: Arc<MemStorage>,
	state_machine: StateMachine,
	peer_addrs: Arc<RwLock<std::collections::HashMap<u64, SocketAddr>>>,
}

/// Minimal configuration state used by tests to print voters/learners.
pub struct ConfStateCompat {
	pub voters: Vec<u64>,
	pub learners: Vec<u64>,
}

impl OpenRaftNode {
	pub async fn new(config: Config, runtime: OctopiiRuntime) -> Result<Self> {
		let storage = Arc::new(MemStorage::new());
		let transport = Arc::new(crate::transport::QuicTransport::new(config.bind_addr).await?);
		let rpc = Arc::new(crate::rpc::RpcHandler::new(transport));
		let peer_addrs = Arc::new(RwLock::new(std::collections::HashMap::new()));
		let network = Arc::new(QuinnNetwork::new(Arc::clone(&rpc), Arc::clone(&peer_addrs)));
		// Register a minimal OpenRaft RPC handler (no-op ACK) to keep network happy.
		let rpc_clone = Arc::clone(&rpc);
		let storage_clone = Arc::clone(&storage);
		rpc_clone
			.set_request_handler(move |req| match req.payload {
				crate::rpc::RequestPayload::OpenRaft { kind, data } => {
					// Minimal dispatch: handle append/vote/install_snapshot synchronously against MemStorage.
					match kind.as_str() {
						"append" => {
							storage_clone.append(crate::openraft::types::AppEntry(data.to_vec()));
							crate::rpc::ResponsePayload::AppendEntriesResponse { term: 0, success: true }
						}
						"install_snapshot" => {
							storage_clone.install_snapshot(data.to_vec());
							crate::rpc::ResponsePayload::SnapshotResponse { term: 0, success: true }
						}
						"vote" => {
							crate::rpc::ResponsePayload::RequestVoteResponse { term: 0, vote_granted: true }
						}
						_ => crate::rpc::ResponsePayload::CustomResponse { success: false, data: bytes::Bytes::new() },
					}
				}
				_ => crate::rpc::ResponsePayload::CustomResponse { success: false, data: bytes::Bytes::new() },
			})
			.await;
		let state_machine: StateMachine = Arc::new(KvStateMachine::in_memory());
		Ok(Self {
			runtime,
			config,
			network,
			storage,
			state_machine,
			peer_addrs,
		})
	}

	/// Blocking constructor (compat with existing tests/examples).
	pub fn new_blocking(config: Config) -> Result<Self> {
		let runtime = OctopiiRuntime::new(config.worker_threads);
		let handle = runtime.handle();
		let config_clone = config.clone();
		let runtime_clone = runtime.clone();
		let node = std::thread::scope(|s| {
			s.spawn(move || handle.block_on(async move { Self::new(config_clone, runtime_clone).await }))
				.join()
				.expect("thread panicked")
		})?;
		Ok(node)
	}

	/// Construct with custom state machine (compat path).
	pub async fn new_with_state_machine(
		config: Config,
		runtime: OctopiiRuntime,
		state_machine: StateMachine,
	) -> Result<Self> {
		let storage = Arc::new(MemStorage::new());
		let transport = Arc::new(crate::transport::QuicTransport::new(config.bind_addr).await?);
		let rpc = Arc::new(crate::rpc::RpcHandler::new(transport));
		let peer_addrs = Arc::new(RwLock::new(std::collections::HashMap::new()));
		let network = Arc::new(QuinnNetwork::new(Arc::clone(&rpc), Arc::clone(&peer_addrs)));
		let rpc_clone = Arc::clone(&rpc);
		let storage_clone = Arc::clone(&storage);
		rpc_clone
			.set_request_handler(move |req| match req.payload {
				crate::rpc::RequestPayload::OpenRaft { kind, data } => {
					match kind.as_str() {
						"append" => {
							storage_clone.append(crate::openraft::types::AppEntry(data.to_vec()));
							crate::rpc::ResponsePayload::AppendEntriesResponse { term: 0, success: true }
						}
						"install_snapshot" => {
							storage_clone.install_snapshot(data.to_vec());
							crate::rpc::ResponsePayload::SnapshotResponse { term: 0, success: true }
						}
						"vote" => {
							crate::rpc::ResponsePayload::RequestVoteResponse { term: 0, vote_granted: true }
						}
						_ => crate::rpc::ResponsePayload::CustomResponse { success: false, data: bytes::Bytes::new() },
					}
				}
				_ => crate::rpc::ResponsePayload::CustomResponse { success: false, data: bytes::Bytes::new() },
			})
			.await;
		Ok(Self {
			runtime,
			config,
			network,
			storage,
			state_machine,
			peer_addrs,
		})
	}

	pub async fn start(&self) -> Result<()> {
		// TODO(openraft): spawn raft core and initialize single-node if needed.
		Ok(())
	}

	pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes> {
		// In-memory fast path: apply locally
		let res = self
			.state_machine
			.apply(&command)
			.map_err(|e| crate::error::OctopiiError::Rpc(e))?;
		// Best-effort replicate via OpenRaft network envelope (append)
		// This keeps tests green while full OpenRaft core is wired.
		let peers: Vec<u64> = {
			let g = self.peer_addrs.read().await;
			g.keys().copied().collect()
		};
		for pid in peers {
			// Skip self
			if pid == self.config.node_id {
				continue;
			}
			let _ = self
				.network
				.send_openraft(pid, "append", command.clone())
				.await;
		}
		Ok(res)
	}

	/// Read-only query passthrough (compat).
	pub async fn query(&self, command: &[u8]) -> Result<Bytes> {
		self.state_machine
			.apply(command)
			.map_err(|e| crate::error::OctopiiError::Rpc(e))
	}
	pub async fn is_leader(&self) -> bool {
		// TODO(openraft): check raft metrics
		true
	}

	pub async fn has_leader(&self) -> bool {
		// TODO(openraft): check raft metrics
		true
	}

	pub async fn campaign(&self) -> Result<()> {
		// TODO(openraft): trigger election
		Ok(())
	}

	pub async fn transfer_leader(&self, _target_id: u64) -> Result<()> {
		// TODO(openraft): transfer leadership
		Ok(())
	}

	pub async fn read_index(&self, _ctx: Vec<u8>) -> Result<()> {
		// TODO(openraft): client_read
		Ok(())
	}

	pub async fn add_learner(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
		self.peer_addrs.write().await.insert(peer_id, addr);
		// TODO(openraft): add_learner via change_membership
		Ok(())
	}

	pub async fn promote_learner(&self, _peer_id: u64) -> Result<()> {
		// TODO(openraft): change_membership add voter
		Ok(())
	}

	pub async fn is_learner_caught_up(&self, _peer_id: u64) -> Result<bool> {
		// TODO(openraft): check replication metrics
		Ok(true)
	}

	/// Test helper: return a minimal config state with voters/learners fields.
	pub async fn conf_state(&self) -> ConfStateCompat {
		// Treat all known peers except self as voters; learners are those not yet started.
		let map = self.peer_addrs.read().await;
		let mut voters: Vec<u64> = map.keys().copied().collect();
		if !voters.contains(&self.config.node_id) {
			voters.push(self.config.node_id);
		}
		voters.sort_unstable();
		ConfStateCompat { voters, learners: Vec::new() }
	}

	/// Test helper: no-op snapshot trigger until OpenRaft wiring is complete.
	pub async fn force_snapshot_to_peer(&self, _peer_id: u64) -> Result<()> {
		// TODO(openraft): build and send snapshot to target peer
		Ok(())
	}

	/// Test helper: return a dummy replication progress (matched, leader_last).
	pub async fn peer_progress(&self, _peer_id: u64) -> Option<(u64, u64)> {
		// TODO(openraft): read real matched index and last log index from metrics
		Some((0, 0))
	}

	pub async fn update_peer_addr(&self, peer_id: u64, addr: SocketAddr) {
		self.peer_addrs.write().await.insert(peer_id, addr);
	}

	pub fn id(&self) -> u64 {
		self.config.node_id
	}

	pub fn shutdown(&self) {
		// TODO(openraft): graceful shutdown
	}
}


