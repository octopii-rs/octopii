#![cfg(feature = "openraft")]

use super::OpenRaftNode;
use crate::error::Result;
use crate::invariants::sim_assert;
use crate::sim_time;
use openraft::impls::BasicNode;
use openraft::ServerState;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use tokio::time::Duration;

impl OpenRaftNode {
    const MEMBERSHIP_RETRY_MAX: u32 = 50;
    const MEMBERSHIP_RETRY_DELAY: Duration = Duration::from_millis(100);

    fn is_membership_in_progress_error(err_str: &str) -> bool {
        err_str.contains("already undergoing a configuration change")
            || err_str.contains("ChangeMembershipError::InProgress")
    }

    /// Retry a membership operation with backoff on "in progress" errors
    async fn retry_membership_op<F, Fut, E>(&self, op_name: &str, mut op: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Display,
    {
        for attempt in 0..Self::MEMBERSHIP_RETRY_MAX {
            match op().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let err_str = format!("{e}");
                    if Self::is_membership_in_progress_error(&err_str)
                        && attempt + 1 < Self::MEMBERSHIP_RETRY_MAX
                    {
                        tracing::debug!(
                            "{op_name}: membership change in progress, retrying (attempt {})",
                            attempt + 1
                        );
                        sim_time::sleep(Self::MEMBERSHIP_RETRY_DELAY).await;
                        continue;
                    }
                    return Err(crate::error::OctopiiError::Rpc(format!("{op_name}: {e}")));
                }
            }
        }
        Err(crate::error::OctopiiError::Rpc(format!(
            "{op_name}: max retries exceeded waiting for membership change"
        )))
    }

    /// Get peer replication progress as (matched_index, last_log_index)
    fn get_peer_replication_progress(&self, peer_id: u64) -> Option<(u64, u64)> {
        let metrics = self.raft.metrics().borrow().clone();
        let last_log = metrics.last_log_index?;
        let replication = metrics.replication.as_ref()?;
        let repl_log_id_opt = replication.get(&peer_id)?;
        let matched = repl_log_id_opt.as_ref().map_or(0, |log_id| log_id.index);
        #[cfg(feature = "simulation")]
        sim_assert(
            matched <= last_log,
            "replication matched index exceeds last_log_index",
        );
        Some((matched, last_log))
    }

    pub async fn add_learner(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        self.persist_peer_addr_if_needed(peer_id, addr).await?;
        let node = BasicNode {
            addr: addr.to_string(),
        };
        self.retry_membership_op("add_learner", || async {
            self.raft
                .add_learner(peer_id, node.clone(), true)
                .await
                .map(|_| ())
        })
        .await
    }

    pub async fn promote_learner(&self, peer_id: u64) -> Result<()> {
        self.retry_membership_op("promote_learner", || async {
            let metrics = self.raft.metrics().borrow().clone();
            let current_membership = metrics.membership_config.membership();

            let mut members = BTreeSet::new();
            for config in current_membership.get_joint_config() {
                members.extend(config.iter().copied());
            }
            members.insert(peer_id);

            self.raft.change_membership(members, true).await.map(|_| ())
        })
        .await
    }

    pub async fn is_learner_caught_up(&self, peer_id: u64) -> Result<bool> {
        if let Some((matched, last_log)) = self.get_peer_replication_progress(peer_id) {
            let distance = last_log.saturating_sub(matched);
            return Ok(distance <= self.raft.config().replication_lag_threshold);
        }
        Ok(false)
    }

    pub async fn peer_progress(&self, peer_id: u64) -> Option<(u64, u64)> {
        self.get_peer_replication_progress(peer_id)
    }

    pub async fn has_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        #[cfg(feature = "simulation")]
        {
            if let Some(leader_id) = metrics.current_leader {
                let membership = metrics.membership_config.membership();
                sim_assert(
                    membership.get_node(&leader_id).is_some(),
                    "current_leader missing from membership",
                );
            }
        }
        metrics.current_leader.is_some()
    }

    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        #[cfg(feature = "simulation")]
        {
            let leader_matches = metrics.current_leader == Some(self.config.node_id);
            let state_is_leader = metrics.state == ServerState::Leader;
            sim_assert(
                !state_is_leader || leader_matches,
                "state leader without matching current_leader",
            );
        }
        metrics.state == ServerState::Leader
    }
}
