#![cfg(feature = "openraft")]

use crate::config::Config;
use crate::error::Result;
mod bootstrap;
mod membership;
mod rpc;

use crate::invariants::sim_assert;
#[cfg(feature = "openraft-filters")]
use crate::openraft::network::OpenRaftFilters;
use crate::openraft::peer_registry::{global_peer_addr, persist_peer_addr};
use crate::openraft::types::{AppEntry, AppTypeConfig};
use crate::runtime::OctopiiRuntime;
use crate::state_machine::StateMachine;
use crate::transport::Transport;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use openraft::metrics::RaftMetrics;
use openraft::storage::{LogState, RaftLogReader, RaftLogStorage};
use openraft::{LogId, Raft, ServerState, Vote};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

pub use crate::openraft::peer_registry::{
    clear_global_peer_addrs, clear_global_peer_addrs_for, peer_namespace_from_base,
};

/// OpenRaft-based node
pub struct OpenRaftNode {
    runtime: OctopiiRuntime,
    config: Config,
    rpc: Arc<crate::rpc::RpcHandler>,
    transport: Arc<dyn Transport>,
    quic_transport: Option<Arc<crate::transport::QuicTransport>>,
    raft: Arc<Raft<AppTypeConfig>>,
    log_store: crate::openraft::storage::WalLogStore,
    state_machine: StateMachine,
    peer_addrs: Arc<RwLock<std::collections::HashMap<u64, SocketAddr>>>,
    peer_addr_wal: Arc<WriteAheadLog>,
    peer_namespace: Arc<String>,
    #[cfg(feature = "openraft-filters")]
    filters: Arc<OpenRaftFilters>,
}

/// Minimal configuration state used by tests
pub struct ConfStateCompat {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}

impl OpenRaftNode {
    fn peer_id_from_addr(addr: &SocketAddr) -> u64 {
        (addr.port() % 10) as u64
    }

    async fn seed_peer_addrs_from_config(&self) -> Result<()> {
        bootstrap::seed_peer_addrs_from_config(self).await
    }

    async fn initialize_cluster_if_needed(&self) -> Result<()> {
        bootstrap::initialize_cluster_if_needed(self).await
    }

    async fn new_with_transport(
        config: Config,
        runtime: OctopiiRuntime,
        transport: Arc<dyn Transport>,
        quic_transport: Option<Arc<crate::transport::QuicTransport>>,
        custom_state_machine: Option<StateMachine>,
    ) -> Result<Self> {
        bootstrap::new_with_transport(
            config,
            runtime,
            transport,
            quic_transport,
            custom_state_machine,
        )
        .await
    }

    pub async fn new(config: Config, runtime: OctopiiRuntime) -> Result<Self> {
        let quic_transport =
            Arc::new(crate::transport::QuicTransport::new(config.bind_addr).await?);
        let transport: Arc<dyn Transport> = quic_transport.clone();
        Self::new_with_transport(config, runtime, transport, Some(quic_transport), None).await
    }

    #[cfg(feature = "simulation")]
    pub async fn new_sim(
        config: Config,
        runtime: OctopiiRuntime,
        transport: Arc<dyn Transport>,
    ) -> Result<Self> {
        Self::new_with_transport(config, runtime, transport, None, None).await
    }

    pub fn new_blocking(config: Config) -> Result<Self> {
        let runtime = OctopiiRuntime::new(config.worker_threads);
        let handle = runtime.handle();
        let config_clone = config.clone();
        let runtime_clone = runtime.clone();
        let node = std::thread::scope(|s| {
            s.spawn(move || {
                handle.block_on(async move { Self::new(config_clone, runtime_clone).await })
            })
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
        let quic_transport =
            Arc::new(crate::transport::QuicTransport::new(config.bind_addr).await?);
        let transport: Arc<dyn Transport> = quic_transport.clone();
        Self::new_with_transport(
            config,
            runtime,
            transport,
            Some(quic_transport),
            Some(state_machine),
        )
        .await
    }

    pub async fn log_state(&self) -> std::result::Result<LogState<AppTypeConfig>, io::Error> {
        let mut store = self.log_store.clone();
        store.get_log_state().await
    }

    pub async fn log_entries(
        &self,
        range: std::ops::RangeInclusive<u64>,
    ) -> std::result::Result<Vec<openraft::Entry<AppTypeConfig>>, io::Error> {
        let mut store = self.log_store.clone();
        store.try_get_log_entries(range).await
    }

    pub async fn read_vote(&self) -> std::result::Result<Option<Vote<AppTypeConfig>>, io::Error> {
        let mut store = self.log_store.clone();
        store.read_vote().await
    }

    pub async fn read_committed(
        &self,
    ) -> std::result::Result<Option<LogId<AppTypeConfig>>, io::Error> {
        let mut store = self.log_store.clone();
        store.read_committed().await
    }

    async fn persist_peer_addr_if_needed(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        persist_peer_addr(
            &self.peer_addrs,
            &self.peer_addr_wal,
            self.peer_namespace.as_str(),
            peer_id,
            addr,
        )
        .await
    }

    pub async fn start(&self) -> Result<()> {
        self.seed_peer_addrs_from_config().await?;
        self.set_openraft_request_handler().await;
        self.initialize_cluster_if_needed().await?;
        Ok(())
    }

    pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes> {
        #[cfg(feature = "simulation")]
        {
            let metrics = self.raft.metrics().borrow().clone();
            sim_assert(
                metrics.state == ServerState::Leader,
                "propose called when not leader",
            );
            sim_assert(
                metrics.current_leader == Some(self.config.node_id),
                "propose leader id mismatch",
            );
        }
        let resp = self
            .raft
            .client_write(AppEntry(command))
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("client_write: {e}")))?;
        Ok(Bytes::from(resp.data.0))
    }

    pub async fn query(&self, command: &[u8]) -> Result<Bytes> {
        self.state_machine
            .apply(command)
            .map_err(crate::error::OctopiiError::Rpc)
    }

    pub async fn campaign(&self) -> Result<()> {
        self.raft
            .trigger()
            .elect()
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("elect: {e}")))?;
        Ok(())
    }

    pub async fn transfer_leader(&self, _target_id: u64) -> Result<()> {
        Ok(())
    }

    pub async fn read_index(&self, _ctx: Vec<u8>) -> Result<()> {
        let metrics = self.raft.metrics().borrow().clone();
        #[cfg(feature = "simulation")]
        {
            if metrics.current_leader == Some(self.config.node_id) {
                sim_assert(
                    metrics.state == ServerState::Leader,
                    "read_index leader id set but state not leader",
                );
            }
        }
        if metrics.current_leader != Some(self.config.node_id) {
            return Err(crate::error::OctopiiError::Rpc("not leader".to_string()));
        }
        Ok(())
    }

    pub async fn conf_state(&self) -> ConfStateCompat {
        let map = self.peer_addrs.read().await;
        let mut voters: Vec<u64> = map.keys().copied().collect();
        if !voters.contains(&self.config.node_id) {
            voters.push(self.config.node_id);
        }
        voters.sort_unstable();
        ConfStateCompat {
            voters,
            learners: Vec::new(),
        }
    }

    pub async fn force_snapshot_to_peer(&self, _peer_id: u64) -> Result<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("snapshot: {e}")))?;
        Ok(())
    }

    pub async fn update_peer_addr(&self, peer_id: u64, addr: SocketAddr) {
        let _ = self.persist_peer_addr_if_needed(peer_id, addr).await;
    }

    pub async fn peer_addr_for(&self, peer_id: u64) -> Option<SocketAddr> {
        if let Some(addr) = self.peer_addrs.read().await.get(&peer_id).copied() {
            return Some(addr);
        }

        if let Some(addr) = global_peer_addr(self.peer_namespace.as_str(), peer_id) {
            let _ = self.persist_peer_addr_if_needed(peer_id, addr).await;
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

    pub fn set_election_enabled(&self, enabled: bool) {
        self.raft.runtime_config().elect(enabled);
    }

    pub async fn shutdown(&self) {
        let _ = self.raft.shutdown().await;
        self.transport.close();
    }

    pub fn shipping_lane(&self) -> crate::shipping_lane::ShippingLane {
        let transport = self
            .quic_transport
            .as_ref()
            .expect("shipping lane requires QUIC transport");
        crate::shipping_lane::ShippingLane::new(Arc::clone(transport))
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn clear_send_filters(&self) {
        self.filters.clear().await;
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_send_drop_to(&self, to: u64) {
        self.filters
            .drop_pairs
            .write()
            .await
            .insert((self.config.node_id, to));
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_send_delay_to(&self, to: u64, delay: Duration) {
        self.filters
            .delay_pairs
            .write()
            .await
            .insert((self.config.node_id, to), delay);
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn add_partition(&self, group1: Vec<u64>, group2: Vec<u64>) {
        let g1: std::collections::HashSet<u64> = group1.into_iter().collect();
        let g2: std::collections::HashSet<u64> = group2.into_iter().collect();
        self.filters.partitions.write().await.push((g1, g2));
    }
}
