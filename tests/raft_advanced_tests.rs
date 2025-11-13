#![cfg(feature = "raft-rs-impl")]
/// Raft advanced features test suite
///
/// This test suite covers advanced Raft features including:
/// - Learner nodes and promotion
/// - Custom state machines
/// - Snapshot transfer and log compaction
/// - Read index optimization
/// - Production features

// Test infrastructure from TiKV
#[path = "test_infrastructure/mod.rs"]
#[macro_use]
mod test_infrastructure;

#[path = "raft_comprehensive/common.rs"]
mod common;

#[path = "raft_comprehensive/learner_tests.rs"]
mod learner_tests;

#[path = "raft_comprehensive/custom_state_machine_tests.rs"]
mod custom_state_machine_tests;

#[path = "raft_comprehensive/snapshot_transfer_tests.rs"]
mod snapshot_transfer_tests;

#[path = "raft_comprehensive/read_index_tests.rs"]
mod read_index_tests;

#[path = "raft_comprehensive/production_features_tests.rs"]
mod production_features_tests;
