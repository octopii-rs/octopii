/// Comprehensive Raft test suite
///
/// This test suite provides extensive coverage of Raft implementation including:
/// - Cluster scenarios (3-node, 5-node, leader failures, follower failures)
/// - Chaos tests (rapid changes, crashes during operations, rolling restarts)
/// - Advanced durability (crashes during proposals, log replication, state machine apply)
/// - Consistency verification (linearizability, monotonicity, convergence)

#[path = "raft_comprehensive/common.rs"]
mod common;

#[path = "raft_comprehensive/cluster_scenarios.rs"]
mod cluster_scenarios;

#[path = "raft_comprehensive/chaos_tests.rs"]
mod chaos_tests;

#[path = "raft_comprehensive/durability_edge_cases.rs"]
mod durability_edge_cases;

#[path = "raft_comprehensive/consistency_tests.rs"]
mod consistency_tests;
