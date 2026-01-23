#![cfg(feature = "openraft")]

use super::OpenRaftNode;
use crate::config::Config;
use crate::error::Result;
use crate::openraft::peer_registry::{
    cluster_namespace_from_wal_dir, load_peer_addr_records, persist_peer_addr,
    register_global_peer_addr,
};
use crate::openraft::storage::{new_wal_log_store, MemStateMachine};
use crate::runtime::OctopiiRuntime;
use crate::state_machine::{KvStateMachine, StateMachine};
use crate::transport::Transport;
use crate::wal::WriteAheadLog;
use openraft::impls::BasicNode;
use openraft::storage::RaftLogStorage;
use openraft::{Config as RaftConfig, Raft};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

#[cfg(feature = "openraft-filters")]
use crate::openraft::network::OpenRaftFilters;
use crate::openraft::network::QuinnNetworkFactory;

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

    persist_peer_addr(
        peers.as_ref(),
        peer_addr_wal,
        cluster_namespace,
        config.node_id,
        config.bind_addr,
    )
    .await
    .is_err();

    Ok(peers)
}

pub(crate) fn build_raft_config() -> Result<Arc<RaftConfig>> {
    let raft_config = RaftConfig {
        heartbeat_interval: 200,
        election_timeout_min: 800,
        election_timeout_max: 1600,
        allow_log_reversion: Some(true),
        ..Default::default()
    };
    Ok(Arc::new(raft_config.validate().map_err(|e| {
        crate::error::OctopiiError::Rpc(format!("raft config: {e}"))
    })?))
}

pub(crate) async fn new_with_transport(
    config: Config,
    runtime: OctopiiRuntime,
    transport: Arc<dyn Transport>,
    quic_transport: Option<Arc<crate::transport::QuicTransport>>,
    custom_state_machine: Option<StateMachine>,
) -> Result<OpenRaftNode> {
    let rpc = Arc::new(crate::rpc::RpcHandler::new(Arc::clone(&transport)));

    std::fs::create_dir_all(&config.wal_dir).map_err(crate::error::OctopiiError::Io)?;

    let cluster_namespace = Arc::new(cluster_namespace_from_wal_dir(&config.wal_dir));
    register_global_peer_addr(cluster_namespace.as_str(), config.node_id, config.bind_addr);

    let (log_store, peer_addr_wal, meta_wal) = init_wal_stores(&config).await?;
    let log_store_for_raft = log_store.clone();
    let peer_addrs = init_peer_addrs(&config, &peer_addr_wal, cluster_namespace.as_str()).await?;

    // Start accepting incoming connections
    rpc.spawn_accept_loop(Arc::clone(&transport));

    #[cfg(feature = "openraft-filters")]
    let filters = Arc::new(OpenRaftFilters::new());

    let state_machine: StateMachine =
        custom_state_machine.unwrap_or_else(|| Arc::new(KvStateMachine::in_memory()));
    let state_machine_store = MemStateMachine::new_with_wal(state_machine.clone(), meta_wal).await;

    let network_factory = QuinnNetworkFactory::new(
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
            node.persist_peer_addr_if_needed(peer_id, *peer_addr)
                .await?;
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

        node.raft
            .initialize(nodes)
            .await
            .map_err(|e| crate::error::OctopiiError::Rpc(format!("initialize: {e}")))?;
    }

    Ok(())
}
