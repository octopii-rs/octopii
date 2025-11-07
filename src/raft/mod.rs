mod storage;
mod state_machine;
mod rpc;

pub use storage::WalStorage;
pub use state_machine::StateMachine;
pub use rpc::*;

use crate::error::Result;
use raft::{prelude::*, Config as RaftConfig, RawNode};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Raft node wrapper
pub struct RaftNode {
    raw_node: Arc<RwLock<RawNode<WalStorage>>>,
    node_id: u64,
}

impl RaftNode {
    /// Create a new Raft node
    pub async fn new(
        node_id: u64,
        peers: Vec<u64>,
        storage: WalStorage,
    ) -> Result<Self> {
        // Bootstrap the cluster with all peers (including self)
        let mut all_peers = vec![node_id];
        all_peers.extend(peers.iter());

        tracing::info!("Bootstrapping Raft node {} with peers: {:?}", node_id, all_peers);

        // Set initial ConfState in storage
        let mut conf_state = ConfState::default();
        conf_state.voters = all_peers.clone();
        storage.set_conf_state(conf_state);

        // Add initial empty entry to the log so Raft can campaign
        // Without this, Raft refuses to campaign on an empty log
        let mut initial_entry = Entry::default();
        initial_entry.entry_type = EntryType::EntryNormal.into();
        initial_entry.term = 1;
        initial_entry.index = 1;
        initial_entry.data = vec![].into();

        storage.append_entries(&[initial_entry]).await?;

        // Set initial HardState with term 1
        let mut hard_state = HardState::default();
        hard_state.term = 1;
        hard_state.commit = 0;
        storage.set_hard_state(hard_state);

        tracing::info!("Added initial log entry for node {}", node_id);

        let config = RaftConfig {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };

        config.validate()?;

        let raw_node = RawNode::new(&config, storage, &slog::Logger::root(slog::Discard, slog::o!()))?;

        Ok(Self {
            raw_node: Arc::new(RwLock::new(raw_node)),
            node_id,
        })
    }

    /// Get the node ID
    pub fn id(&self) -> u64 {
        self.node_id
    }

    /// Propose a change to the state machine
    pub async fn propose(&self, data: Vec<u8>) -> Result<()> {
        let mut node = self.raw_node.write().await;
        node.propose(vec![], data)?;
        Ok(())
    }

    /// Tick the Raft node (should be called periodically)
    pub async fn tick(&self) {
        let mut node = self.raw_node.write().await;
        node.tick();
    }

    /// Process a Raft message
    pub async fn step(&self, msg: Message) -> Result<()> {
        let mut node = self.raw_node.write().await;
        node.step(msg)?;
        Ok(())
    }

    /// Check if there are ready messages to process
    pub async fn ready(&self) -> Option<Ready> {
        let mut node = self.raw_node.write().await;
        if node.has_ready() {
            Some(node.ready())
        } else {
            None
        }
    }

    /// Advance the Raft state machine after processing ready
    pub async fn advance(&self, rd: Ready) {
        let mut node = self.raw_node.write().await;
        node.advance(rd);
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let node = self.raw_node.read().await;
        node.raft.state == raft::StateRole::Leader
    }

    /// Campaign to become leader (triggers election)
    pub async fn campaign(&self) -> Result<()> {
        let mut node = self.raw_node.write().await;
        node.campaign()?;
        tracing::info!("Node {} starting election campaign", self.node_id);
        Ok(())
    }
}
