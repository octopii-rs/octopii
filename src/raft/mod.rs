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
    /// For a fresh cluster (following TiKV five_mem_node pattern):
    /// - Leader node (is_leader=true): Initializes with a snapshot containing ONLY itself as voter
    /// - Follower nodes (is_leader=false): Start with empty storage, will initialize from leader messages
    pub async fn new(
        node_id: u64,
        _peers: Vec<u64>,
        storage: WalStorage,
        is_leader: bool,
    ) -> Result<Self> {
        if is_leader {
            // Leader initializes with snapshot containing ONLY itself as voter
            // This matches TiKV's pattern: voters = vec![1] (only leader)
            let mut snapshot = Snapshot::default();
            snapshot.mut_metadata().index = 1;
            snapshot.mut_metadata().term = 1;
            snapshot.mut_metadata().mut_conf_state().voters = vec![node_id];

            storage.apply_snapshot(snapshot)?;

            tracing::info!(
                "Bootstrapped Raft LEADER node {} (will add {} peers via ConfChange)",
                node_id,
                _peers.len()
            );
        } else {
            // Followers start with minimal empty state
            // They will initialize when receiving first message from leader (lazy initialization)
            // This is critical - followers should NOT have voters pre-configured!
            tracing::info!(
                "Created Raft FOLLOWER node {} (will initialize from leader)",
                node_id
            );
        }

        let config = RaftConfig {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            pre_vote: true,  // Enable pre-vote to prevent election storms
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

    /// Check if the cluster has a leader
    pub async fn has_leader(&self) -> bool {
        let node = self.raw_node.read().await;
        node.raft.leader_id != raft::INVALID_ID
    }

    /// Campaign to become leader (triggers election)
    pub async fn campaign(&self) -> Result<()> {
        let mut node = self.raw_node.write().await;
        node.campaign()?;
        tracing::info!("Node {} starting election campaign", self.node_id);
        Ok(())
    }

    /// Propose a configuration change (add/remove node)
    pub async fn propose_conf_change(&self, context: Vec<u8>, cc: ConfChange) -> Result<()> {
        let change_type = cc.get_change_type();
        let node_id = cc.node_id;
        let mut node = self.raw_node.write().await;
        node.propose_conf_change(context, cc)?;
        tracing::info!("Proposed ConfChange: {:?} for node {}",
            change_type, node_id);
        Ok(())
    }

    /// Apply a committed configuration change
    pub async fn apply_conf_change(&self, cc: &ConfChange) -> Result<ConfState> {
        let mut node = self.raw_node.write().await;
        let conf_state = node.apply_conf_change(cc)?;
        tracing::info!("Applied ConfChange: {:?} for node {}, new voters: {:?}",
            cc.get_change_type(), cc.node_id, conf_state.voters);
        Ok(conf_state)
    }

    /// Get current hard state
    pub async fn hard_state(&self) -> HardState {
        let node = self.raw_node.read().await;
        node.raft.hard_state().clone()
    }

    /// Try to compact logs (threshold-based)
    pub async fn try_compact_logs(&self, applied_index: u64, state_machine_data: Vec<u8>) -> Result<()> {
        let node = self.raw_node.read().await;
        node.store().compact_logs(applied_index, state_machine_data)?;
        Ok(())
    }

    /// Get progress information for a peer (used for learner promotion)
    pub async fn peer_progress(&self, peer_id: u64) -> Option<(u64, u64)> {
        let node = self.raw_node.read().await;
        if let Some(progress) = node.raft.prs().get(peer_id) {
            let leader_last_index = node.raft.raft_log.last_index();
            Some((progress.matched, leader_last_index))
        } else {
            None
        }
    }

    /// Expose raw_node for advanced operations
    pub fn raw_node(&self) -> &Arc<RwLock<RawNode<WalStorage>>> {
        &self.raw_node
    }
}
