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
    ///
    /// For a fresh cluster:
    /// - Leader node (is_leader=true): Initializes with a snapshot containing only itself as voter
    /// - Follower nodes (is_leader=false): Start with empty storage, will initialize when receiving first message
    pub async fn new(
        node_id: u64,
        peers: Vec<u64>,
        storage: WalStorage,
        is_leader: bool,
    ) -> Result<Self> {
        if is_leader {
            // Leader initializes with snapshot containing only itself
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = 1;
            snapshot.mut_metadata().term = 1;
            snapshot.mut_metadata().mut_conf_state().voters = vec![node_id];

            storage.apply_snapshot(snapshot)?;

            tracing::info!(
                "Bootstrapped Raft LEADER node {} (will add {} peers via ConfChange)",
                node_id,
                peers.len()
            );
        } else {
            // Followers start with minimal state
            // They will initialize properly when receiving first message from leader
            tracing::info!("Created Raft FOLLOWER node {} (will initialize from leader)", node_id);
        }

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
        let last_index_before = node.raft.raft_log.last_index();
        node.propose(vec![], data)?;
        tracing::debug!("Proposed entry, last_index was {} (role: {:?})",
            last_index_before, node.raft.state);
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
    /// Returns LightReady which contains committed entries
    pub async fn advance(&self, rd: Ready) -> raft::LightReady {
        let mut node = self.raw_node.write().await;
        let light_rd = node.advance(rd);
        light_rd
    }

    /// Finish applying committed entries
    pub async fn advance_apply(&self) {
        let mut node = self.raw_node.write().await;
        node.advance_apply();
    }

    /// Persist entries from Ready to storage
    pub async fn persist_entries(&self, entries: &[Entry]) {
        let node = self.raw_node.read().await;
        node.store().append_entries_sync(entries);
        tracing::debug!("Persisted {} entries to storage", entries.len());
    }

    /// Persist hard state from Ready to storage
    pub async fn persist_hard_state(&self, hs: HardState) {
        let node = self.raw_node.read().await;
        node.store().set_hard_state(hs);
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
