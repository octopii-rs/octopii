#![cfg(feature = "raft-rs-impl")]
/// Core Raft test suite
///
/// This test suite covers fundamental Raft functionality including:
/// - Basic cluster operations (3-node, 5-node clusters)
/// - Leader election and failover
/// - Follower crash and recovery
/// - Durability edge cases
/// - Pre-vote protocol

// Test infrastructure from TiKV
#[path = "test_infrastructure/mod.rs"]
#[macro_use]
mod test_infrastructure;

#[path = "raft_comprehensive/common.rs"]
mod common;

#[path = "raft_comprehensive/cluster_scenarios.rs"]
mod cluster_scenarios;

#[path = "raft_comprehensive/durability_edge_cases.rs"]
mod durability_edge_cases;

#[path = "raft_comprehensive/pre_vote_tests.rs"]
mod pre_vote_tests;

#[path = "raft_comprehensive/test_basic_cluster.rs"]
mod test_basic_cluster;
