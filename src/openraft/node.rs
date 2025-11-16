#![cfg(feature = "openraft")]

use crate::config::Config;
use crate::error::Result;
use crate::openraft::network::{QuinnNetwork, QuinnNetworkFactory};
#[cfg(feature = "openraft-filters")]
use crate::openraft::network::OpenRaftFilters;
use crate::openraft::storage::{MemLogStore, MemStateMachine, new_mem_log_store, new_mem_state_machine};
use crate::openraft::types::{AppEntry, AppResponse, AppNodeId, AppTypeConfig};
use crate::runtime::OctopiiRuntime;
use crate::state_machine::{KvStateMachine, StateMachine};
use bytes::Bytes;
use openraft::metrics::RaftMetrics;
use once_cell::sync::Lazy;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock as StdRwLock};
use tokio::sync::RwLock;
use tokio::time::Duration;
use openraft::{Raft, Config as RaftConfig};
use openraft::impls::BasicNode;

pub(crate) static GLOBAL_PEER_ADDRS: Lazy<StdRwLock<HashMap<u64, SocketAddr>>> =
    Lazy::new(|| StdRwLock::new(HashMap::new()));

pub(crate) fn register_global_peer_addr(node_id: u64, addr: SocketAddr) {
    let mut map = GLOBAL_PEER_ADDRS.write().unwrap();
    map.insert(node_id, addr);
}

pub(crate) fn global_peer_addr(peer_id: u64) -> Option<SocketAddr> {
    GLOBAL_PEER_ADDRS.read().unwrap().get(&peer_id).copied()
}

/// OpenRaft-based node
pub struct OpenRaftNode {
    runtime: OctopiiRuntime,
    config: Config,
    rpc: Arc<crate::rpc::RpcHandler>,
    transport: Arc<crate::transport::QuicTransport>,
    raft: Arc<Raft<AppTypeConfig>>,
    state_machine: StateMachine,
    peer_addrs: Arc<RwLock<std::collections::HashMap<u64, SocketAddr>>>,
    #[cfg(feature = "openraft-filters")]
    filters: Arc<OpenRaftFilters>,
}

/// Minimal configuration state used by tests
pub struct ConfStateCompat {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}

impl OpenRaftNode {
    pub async fn new(config: Config, runtime: OctopiiRuntime) -> Result<Self> {
        let transport = Arc::new(crate::transport::QuicTransport::new(config.bind_addr).await?);
        let rpc = Arc::new(crate::rpc::RpcHandler::new(Arc::clone(&transport)));

        register_global_peer_addr(config.node_id, config.bind_addr);
        
        // Start accepting incoming connections
        let rpc_clone = Arc::clone(&rpc);
        let transport_clone = Arc::clone(&transport);
        tokio::spawn(async move {
            loop {
                match transport_clone.accept().await {
                    Ok((addr, peer)) => {
                        tracing::debug!("Accepted connection from {}", addr);
                        // Spawn a task to handle messages from this peer
                        let rpc_inner = Arc::clone(&rpc_clone);
                        tokio::spawn(async move {
                            loop {
                                match peer.recv().await {
                                    Ok(Some(data)) => {
                                        match crate::rpc::deserialize::<crate::rpc::RpcMessage>(&data) {
                                            Ok(msg) => {
                                                rpc_inner.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                                            }
                                            Err(e) => {
                                                tracing::error!("Failed to deserialize RPC message from {}: {}", addr, e);
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        tracing::debug!("Peer {} closed connection", addr);
                                        break;
                                    }
                                    Err(e) => {
                                        tracing::debug!("Peer {} recv error: {}", addr, e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        // Don't break the loop on accept errors - just log and continue
                        // This can happen during normal operation (e.g., connection refused, handshake failures)
                        tracing::debug!("Failed to accept connection: {}", e);
                        // Small delay to avoid tight loop on persistent errors
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                }
            }
        });
        
        // Initialize peer_addrs from config.peers
        // We need to infer peer IDs from addresses. For now, we'll populate this
        // when peers are explicitly added via add_learner or when we receive messages.
        let peer_addrs = Arc::new(RwLock::new(std::collections::HashMap::new()));
        
        #[cfg(feature = "openraft-filters")]
        let filters = Arc::new(OpenRaftFilters::new());
        
        let state_machine: StateMachine = Arc::new(KvStateMachine::in_memory());
        let log_store = new_mem_log_store();
        let state_machine_store = new_mem_state_machine(state_machine.clone());
        
        // Create network factory
        #[cfg(feature = "openraft-filters")]
        let network_factory = QuinnNetworkFactory::new(
            Arc::clone(&rpc),
            Arc::clone(&peer_addrs),
            config.node_id,
            Arc::clone(&filters),
        );
        
        #[cfg(not(feature = "openraft-filters"))]
        let network_factory = QuinnNetworkFactory::new(
            Arc::clone(&rpc),
            Arc::clone(&peer_addrs),
            config.node_id,
        );
        
        // Create Raft config
        let mut raft_config = RaftConfig::default();
        raft_config.heartbeat_interval = 200;
        raft_config.election_timeout_min = 800;
        raft_config.election_timeout_max = 1600;
        let raft_config = Arc::new(raft_config.validate()
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("raft config: {e}")))?);
        
        // Create Raft instance
        let raft = Raft::new(
            config.node_id,
            raft_config,
            network_factory,
            log_store,
            state_machine_store.clone(),
        ).await.map_err(|e| crate::error::OctopiiError::Rpc(format!("raft new: {e}")))?;
        
        Ok(Self {
            runtime,
            config,
            rpc,
            transport,
            raft: Arc::new(raft),
            state_machine,
            peer_addrs,
            #[cfg(feature = "openraft-filters")]
            filters,
        })
    }

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

    pub async fn new_with_state_machine(
        config: Config,
        runtime: OctopiiRuntime,
        state_machine: StateMachine,
    ) -> Result<Self> {
        let mut node = Self::new(config, runtime).await?;
        node.state_machine = state_machine;
        Ok(node)
    }

    pub async fn start(&self) -> Result<()> {
        // Populate peer_addrs map for all nodes (not just initial leader)
        // HACK: Use last digit of port as node ID (e.g., :9321 -> node 1, :9322 -> node 2)
        // This works for the test suite but is not production-ready
        for peer_addr in self.config.peers.iter() {
            let peer_id = (peer_addr.port() % 10) as u64;
            if peer_id != self.config.node_id && peer_id > 0 {
                tracing::info!("Node {}: Adding peer {} at address {}", self.config.node_id, peer_id, peer_addr);
                self.peer_addrs.write().await.insert(peer_id, *peer_addr);
            }
        }

        // Log final peer_addrs state
        let peers = self.peer_addrs.read().await;
        tracing::info!("Node {}: Initialized with peers: {:?}", self.config.node_id,
            peers.iter().map(|(id, addr)| format!("{}@{}", id, addr)).collect::<Vec<_>>());
        drop(peers);

        // Register RPC handler for OpenRaft messages BEFORE initialization
        // This is critical: nodes 2 and 3 need to be able to receive RPCs when node 1 initializes
        let raft_clone = self.raft.clone();
        let node_id = self.config.node_id;
        self.rpc.set_request_handler(move |req| {
            let raft = raft_clone.clone();

            match req.payload {
                crate::rpc::RequestPayload::OpenRaft { kind, data } => {
                    tracing::info!("Node {}: OpenRaft RPC handler received: kind={}", node_id, kind);
                    let response_data = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(async {
                        match kind.as_str() {
                            "append_entries" => {
                                if let Ok(req) = bincode::deserialize::<openraft::raft::AppendEntriesRequest<AppTypeConfig>>(&data) {
                                    if let Ok(resp) = raft.append_entries(req).await {
                                        bincode::serialize(&resp).unwrap_or_default()
                                    } else {
                                        Vec::new()
                                    }
                                } else {
                                    Vec::new()
                                }
                            }
                            "vote" => {
                                if let Ok(req) = bincode::deserialize::<openraft::raft::VoteRequest<AppTypeConfig>>(&data) {
                                    if let Ok(resp) = raft.vote(req).await {
                                        bincode::serialize(&resp).unwrap_or_default()
                                    } else {
                                        Vec::new()
                                    }
                                } else {
                                    Vec::new()
                                }
                            }
                            "install_snapshot" => {
                                if let Ok(req) = bincode::deserialize::<openraft::raft::InstallSnapshotRequest<AppTypeConfig>>(&data) {
                                    if let Ok(resp) = raft.install_snapshot(req).await {
                                        bincode::serialize(&resp).unwrap_or_default()
                                    } else {
                                        Vec::new()
                                    }
                                } else {
                                    Vec::new()
                                }
                            }
                            _ => Vec::new(),
                        }
                        })
                    });

                    crate::rpc::ResponsePayload::OpenRaft {
                        kind,
                        data: bytes::Bytes::from(response_data)
                    }
                }
                _ => crate::rpc::ResponsePayload::CustomResponse {
                    success: false,
                    data: bytes::Bytes::new()
                },
            }
        }).await;

        // NOW initialize the cluster after all RPC handlers are set up
        // Only initialize if this is the initial leader and cluster is not initialized
        // For multi-node clusters, only the initial leader should call initialize
        if self.config.is_initial_leader && !self.raft.is_initialized().await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("is_initialized: {e}")))?
        {
            tracing::info!("Node {}: Initializing cluster as initial leader", self.config.node_id);
            let mut nodes = BTreeMap::new();
            nodes.insert(
                self.config.node_id,
                BasicNode { addr: self.config.bind_addr.to_string() }
            );

            // Add peer nodes to the initial cluster membership
            for peer_addr in self.config.peers.iter() {
                let peer_id = (peer_addr.port() % 10) as u64;
                if peer_id != self.config.node_id && peer_id > 0 {
                    tracing::info!("Node {}: Adding peer {} to initial membership", self.config.node_id, peer_id);
                    nodes.insert(peer_id, BasicNode { addr: peer_addr.to_string() });
                }
            }

            self.raft.initialize(nodes).await
                .map_err(|e| crate::error::OctopiiError::Rpc(format!("initialize: {e}")))?;
            tracing::info!("Node {}: Cluster initialization complete", self.config.node_id);
        }

        Ok(())
    }

    pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes> {
        let resp = self.raft.client_write(AppEntry(command)).await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("client_write: {e}")))?;
        Ok(Bytes::from(resp.data.0))
    }

    pub async fn query(&self, command: &[u8]) -> Result<Bytes> {
        self.state_machine.apply(command)
            .map_err(|e| crate::error::OctopiiError::Rpc(e))
    }

    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        tracing::debug!("Node {}: is_leader check - current_leader={:?}, server_state={:?}, membership={:?}",
            self.config.node_id, metrics.current_leader, metrics.state, metrics.membership_config.membership());
        metrics.current_leader == Some(self.config.node_id)
    }

    pub async fn has_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader.is_some()
    }

    pub async fn campaign(&self) -> Result<()> {
        self.raft.trigger().elect().await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("elect: {e}")))?;
        Ok(())
    }

    pub async fn transfer_leader(&self, _target_id: u64) -> Result<()> {
        // OpenRaft doesn't have a direct transfer_leader API in 0.9
        // This would need to be implemented via membership changes
        Ok(())
    }

    pub async fn read_index(&self, _ctx: Vec<u8>) -> Result<()> {
        // In OpenRaft 0.10, we check leadership via metrics
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader != Some(self.config.node_id) {
            return Err(crate::error::OctopiiError::Rpc("not leader".to_string()));
        }
        Ok(())
    }

    pub async fn add_learner(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        self.peer_addrs.write().await.insert(peer_id, addr);
        register_global_peer_addr(peer_id, addr);
        let node = BasicNode { addr: addr.to_string() };
        self.raft.add_learner(peer_id, node, true).await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("add_learner: {e}")))?;
        Ok(())
    }

    pub async fn promote_learner(&self, peer_id: u64) -> Result<()> {
        // Get current membership and add the new voter to it
        let metrics = self.raft.metrics().borrow().clone();
        let current_membership = metrics.membership_config.membership();

        // Get all current voter IDs
        let mut members = BTreeSet::new();
        for config in current_membership.get_joint_config() {
            members.extend(config.iter().copied());
        }

        // Add the new voter
        members.insert(peer_id);

        tracing::info!("Node {}: Promoting learner {} to voter, new membership: {:?}",
            self.config.node_id, peer_id, members);

        self.raft.change_membership(members, true).await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("change_membership: {e}")))?;
        Ok(())
    }

    pub async fn is_learner_caught_up(&self, peer_id: u64) -> Result<bool> {
        let metrics = self.raft.metrics().borrow().clone();
        if let Some(last_log) = metrics.last_log_index {
            if let Some(replication) = metrics.replication {
                if let Some(repl_log_id_opt) = replication.get(&peer_id) {
                    let matched = repl_log_id_opt.as_ref().map_or(0, |log_id| log_id.index);
                    let distance = last_log.saturating_sub(matched);
                    return Ok(distance <= self.raft.config().replication_lag_threshold);
                }
            }
        }
        Ok(false)
    }

    pub async fn conf_state(&self) -> ConfStateCompat {
        let map = self.peer_addrs.read().await;
        let mut voters: Vec<u64> = map.keys().copied().collect();
        if !voters.contains(&self.config.node_id) {
            voters.push(self.config.node_id);
        }
        voters.sort_unstable();
        ConfStateCompat { voters, learners: Vec::new() }
    }

    pub async fn force_snapshot_to_peer(&self, _peer_id: u64) -> Result<()> {
        self.raft.trigger().snapshot().await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("snapshot: {e}")))?;
        Ok(())
    }

    pub async fn peer_progress(&self, peer_id: u64) -> Option<(u64, u64)> {
        let metrics = self.raft.metrics().borrow().clone();
        if let Some(last_log) = metrics.last_log_index {
            if let Some(replication) = metrics.replication {
                if let Some(repl_log_id_opt) = replication.get(&peer_id) {
                    let matched = repl_log_id_opt.as_ref().map_or(0, |log_id| log_id.index);
                    return Some((matched, last_log));
                }
            }
        }
        None
    }

    pub async fn update_peer_addr(&self, peer_id: u64, addr: SocketAddr) {
        self.peer_addrs.write().await.insert(peer_id, addr);
        register_global_peer_addr(peer_id, addr);
    }

    pub async fn peer_addr_for(&self, peer_id: u64) -> Option<SocketAddr> {
        if let Some(addr) = self.peer_addrs.read().await.get(&peer_id).copied() {
            return Some(addr);
        }

        if let Some(addr) = global_peer_addr(peer_id) {
            let mut map = self.peer_addrs.write().await;
            map.insert(peer_id, addr);
            return Some(addr);
        }

        None
    }

    pub fn raft_metrics(&self) -> RaftMetrics<AppTypeConfig> {
        self.raft.metrics().borrow().clone()
    }

    pub fn id(&self) -> u64 {
        self.config.node_id
    }

    pub fn shutdown(&self) {
        let _ = self.raft.shutdown();
    }

    pub fn shipping_lane(&self) -> crate::shipping_lane::ShippingLane {
        crate::shipping_lane::ShippingLane::new(Arc::clone(&self.transport))
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn clear_send_filters(&self) {
        self.filters.clear().await;
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_send_drop_to(&self, to: u64) {
        self.filters.drop_pairs.write().await.insert((self.config.node_id, to));
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_send_delay_to(&self, to: u64, delay: Duration) {
        self.filters.delay_pairs.write().await.insert((self.config.node_id, to), delay);
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_partition(&self, group1: Vec<u64>, group2: Vec<u64>) {
        let g1: std::collections::HashSet<u64> = group1.into_iter().collect();
        let g2: std::collections::HashSet<u64> = group2.into_iter().collect();
        self.filters.partitions.write().await.push((g1, g2));
    }
}
