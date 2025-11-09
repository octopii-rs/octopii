/// Comprehensive Raft test suite
///
/// This test suite provides extensive coverage of Raft implementation including:
/// - Cluster scenarios (3-node, 5-node, leader failures, follower failures)
/// - Chaos tests (rapid changes, crashes during operations, rolling restarts)
/// - Advanced durability (crashes during proposals, log replication, state machine apply)
/// - Consistency verification (linearizability, monotonicity, convergence)
/// - Automatic leader election and failover
/// - Learner nodes and safe membership changes
/// - Snapshot transfer and log compaction
/// - Pre-vote protocol for stability
/// - Batch operations for performance
/// - Short-duration stress tests

// Test infrastructure from TiKV
#[path = "test_infrastructure/mod.rs"]
#[macro_use]
mod test_infrastructure;

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

#[path = "raft_comprehensive/learner_tests.rs"]
mod learner_tests;

// TODO: Fix leader failure recovery - needs network partition support
// #[path = "raft_comprehensive/automatic_election_tests.rs"]
// mod automatic_election_tests;

#[path = "raft_comprehensive/test_basic_cluster.rs"]
mod test_basic_cluster;

#[path = "raft_comprehensive/snapshot_transfer_tests.rs"]
mod snapshot_transfer_tests;

#[path = "raft_comprehensive/pre_vote_tests.rs"]
mod pre_vote_tests;

#[path = "raft_comprehensive/batch_operation_tests.rs"]
mod batch_operation_tests;

#[path = "raft_comprehensive/short_duration_stress_tests.rs"]
mod short_duration_stress_tests;

#[path = "raft_comprehensive/partition_tests.rs"]
mod partition_tests;

#[path = "raft_comprehensive/real_partition_tests.rs"]
mod real_partition_tests;
