#![cfg(feature = "openraft")]

use super::{ConfStateCompat, OpenRaftNode};
use crate::config::Config;
use crate::error::Result;
use crate::openraft::peer_registry::{
    cluster_namespace_from_wal_dir, load_peer_addr_records, persist_peer_addr,
    register_global_peer_addr,
};
use crate::openraft::storage::{new_wal_log_store, MemStateMachine};
use crate::openraft::types::AppTypeConfig;
use crate::runtime::OctopiiRuntime;
use crate::state_machine::{KvStateMachine, StateMachine};
use crate::transport::Transport;
use crate::wal::WriteAheadLog;
use openraft::impls::BasicNode;
use openraft::storage::{LogState, RaftLogReader, RaftLogStorage};
use openraft::{Config as RaftConfig, LogId, Raft, Vote};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

#[cfg(feature = "openraft-filters")]
use crate::openraft::network::OpenRaftFilters;
use crate::openraft::network::QuinnNetworkFactory;
#[cfg(feature = "openraft-filters")]
pub(crate) fn build_network_factory(
    rpc: Arc<crate::rpc::RpcHandler>,
    peer_addrs: Arc<RwLock<HashMap<u64, SocketAddr>>>,
    node_id: u64,
    cluster_namespace: Arc<String>,
    filters: Arc<OpenRaftFilters>,
) -> QuinnNetworkFactory {
    QuinnNetworkFactory::new(rpc, peer_addrs, node_id, cluster_namespace, filters)
}

#[cfg(not(feature = "openraft-filters"))]
pub(crate) fn build_network_factory(
    rpc: Arc<crate::rpc::RpcHandler>,
    peer_addrs: Arc<RwLock<HashMap<u64, SocketAddr>>>,
    node_id: u64,
    cluster_namespace: Arc<String>,
) -> QuinnNetworkFactory {
    QuinnNetworkFactory::new(rpc, peer_addrs, node_id, cluster_namespace)
}

pub(crate) async fn init_wal_stores(
    config: &Config,
) -> Result<(
    crate::openraft::storage::WalLogStore,
    Arc<WriteAheadLog>,
    Arc<WriteAheadLog>,
)> {
    let flush_interval = Duration::from_millis(config.wal_flush_interval_ms);
    let log_store = new_wal_log_store(Arc::new(
        WriteAheadLog::new(
            config.wal_dir.join("openraft_log"),
            config.wal_batch_size,
            flush_interval,
        )
        .await?,
    ))
    .await?;

    let peer_addr_wal = Arc::new(
        WriteAheadLog::new(
            config.wal_dir.join("peer_addrs"),
            config.wal_batch_size,
            flush_interval,
        )
        .await?,
    );

    let meta_wal = Arc::new(
        WriteAheadLog::new(
            config.wal_dir.join("state_machine"),
            config.wal_batch_size,
            flush_interval,
        )
        .await?,
    );

    Ok((log_store, peer_addr_wal, meta_wal))
}

pub(crate) async fn init_peer_addrs(
    config: &Config,
    peer_addr_wal: &Arc<WriteAheadLog>,
    cluster_namespace: &str,
) -> Result<Arc<RwLock<HashMap<u64, SocketAddr>>>> {
    let peer_addrs_map = load_peer_addr_records(peer_addr_wal).await;
    let peers = Arc::new(RwLock::new(peer_addrs_map));

    register_global_peer_addr(cluster_namespace, config.node_id, config.bind_addr);

    if persist_peer_addr(peers.as_ref(), peer_addr_wal, cluster_namespace, config.node_id, config.bind_addr).await.is_err() {
    }

    Ok(peers)
}

pub(crate) fn build_raft_config() -> Result<Arc<RaftConfig>> {
    let mut raft_config = RaftConfig::default();
    raft_config.heartbeat_interval = 200;
    raft_config.election_timeout_min = 800;
    raft_config.election_timeout_max = 1600;
    raft_config.allow_log_reversion = Some(true);
    Ok(Arc::new(
        raft_config
            .validate()
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("raft config: {e}")))?,
    ))
}

pub(crate) async fn new_with_transport(
    config: Config,
    runtime: OctopiiRuntime,
    transport: Arc<dyn Transport>,
    quic_transport: Option<Arc<crate::transport::QuicTransport>>,
    custom_state_machine: Option<StateMachine>,
) -> Result<OpenRaftNode> {
    let rpc = Arc::new(crate::rpc::RpcHandler::new(Arc::clone(&transport)));

    std::fs::create_dir_all(&config.wal_dir).map_err(|e| crate::error::OctopiiError::Io(e))?;

    let cluster_namespace = Arc::new(cluster_namespace_from_wal_dir(&config.wal_dir));
    register_global_peer_addr(cluster_namespace.as_str(), config.node_id, config.bind_addr);

    let (log_store, peer_addr_wal, meta_wal) = init_wal_stores(&config).await?;
    let log_store_for_raft = log_store.clone();
    let peer_addrs = init_peer_addrs(&config, &peer_addr_wal, cluster_namespace.as_str()).await?;

    // Start accepting incoming connections
    rpc.spawn_accept_loop(Arc::clone(&transport));

    #[cfg(feature = "openraft-filters")]
    let filters = Arc::new(OpenRaftFilters::new());

    let state_machine: StateMachine = custom_state_machine.unwrap_or_else(|| Arc::new(KvStateMachine::in_memory()));
    let state_machine_store = MemStateMachine::new_with_wal(state_machine.clone(), meta_wal).await;

    let network_factory = build_network_factory(
        Arc::clone(&rpc),
        Arc::clone(&peer_addrs),
        config.node_id,
        Arc::clone(&cluster_namespace),
        #[cfg(feature = "openraft-filters")]
        Arc::clone(&filters),
    );

    let raft_config = build_raft_config()?;

    let raft = Raft::new(
        config.node_id,
        raft_config,
        network_factory,
        log_store_for_raft,
        state_machine_store.clone(),
    )
    .await
    .map_err(|e| crate::error::OctopiiError::Rpc(format!("raft new: {e}")))?;

    Ok(OpenRaftNode {
        runtime,
        config,
        rpc,
        transport,
        quic_transport,
        raft: Arc::new(raft),
        log_store,
        state_machine,
        peer_addrs,
        peer_addr_wal,
        peer_namespace: Arc::clone(&cluster_namespace),
        #[cfg(feature = "openraft-filters")]
        filters,
    })
}

pub(crate) async fn seed_peer_addrs_from_config(node: &OpenRaftNode) -> Result<()> {
    for peer_addr in node.config.peers.iter() {
        let peer_id = OpenRaftNode::peer_id_from_addr(peer_addr);
        if peer_id != node.config.node_id && peer_id > 0 {
            node.persist_peer_addr_if_needed(peer_id, *peer_addr).await?;
        }
    }
    Ok(())
}

pub(crate) async fn initialize_cluster_if_needed(node: &OpenRaftNode) -> Result<()> {
    if !node.config.is_initial_leader {
        return Ok(());
    }

    let mut store = node.log_store.clone();
    let state = store.get_log_state().await?;

    if state.last_log_id.is_none() {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node.config.node_id,
            BasicNode {
                addr: node.config.bind_addr.to_string(),
            },
        );

        for peer_addr in node.config.peers.iter() {
            let peer_id = OpenRaftNode::peer_id_from_addr(peer_addr);
            if peer_id != node.config.node_id && peer_id > 0 {
                nodes.insert(
                    peer_id,
                    BasicNode {
                        addr: peer_addr.to_string(),
                    },
                );
            }
        }

        node
            .raft
            .initialize(nodes)
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("initialize: {e}")))?;
    }

    Ok(())
}

pub(crate) async fn log_state(node: &OpenRaftNode) -> std::result::Result<LogState<AppTypeConfig>, io::Error> {
    let mut store = node.log_store.clone();
    store.get_log_state().await
}

pub(crate) async fn log_entries(
    node: &OpenRaftNode,
    range: std::ops::RangeInclusive<u64>,
) -> std::result::Result<Vec<openraft::Entry<AppTypeConfig>>, io::Error> {
    let mut store = node.log_store.clone();
    store.try_get_log_entries(range).await
}

pub(crate) async fn read_vote(node: &OpenRaftNode) -> std::result::Result<Option<Vote<AppTypeConfig>>, io::Error> {
    let mut store = node.log_store.clone();
    store.read_vote().await
}

pub(crate) async fn read_committed(node: &OpenRaftNode) -> std::result::Result<Option<LogId<AppTypeConfig>>, io::Error> {
    let mut store = node.log_store.clone();
    store.read_committed().await
}

pub(crate) async fn conf_state(node: &OpenRaftNode) -> ConfStateCompat {
    let map = node.peer_addrs.read().await;
    let mut voters: Vec<u64> = map.keys().copied().collect();
    if !voters.contains(&node.config.node_id) {
        voters.push(node.config.node_id);
    }
    voters.sort_unstable();
    ConfStateCompat {
        voters,
        learners: Vec::new(),
    }
}

pub(crate) async fn persist_peer_addr_if_needed(
    peer_addrs: &Arc<RwLock<HashMap<u64, SocketAddr>>>,
    peer_addr_wal: &Arc<WriteAheadLog>,
    peer_namespace: &str,
    peer_id: u64,
    addr: SocketAddr,
) -> Result<()> {
    persist_peer_addr(peer_addrs, peer_addr_wal, peer_namespace, peer_id, addr).await
}
