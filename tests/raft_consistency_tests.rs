#![cfg(feature = "raft-rs-impl")]
/// Raft consistency and streaming test suite
///
/// This test suite covers consistency guarantees and log streaming including:
/// - Linearizability verification
/// - Consistency tests (convergence, no data loss)
/// - Log streaming and catch-up
/// - Batch operations
/// - State machine consistency

// Test infrastructure from TiKV
#[path = "test_infrastructure/mod.rs"]
#[macro_use]
mod test_infrastructure;

#[path = "raft_comprehensive/common.rs"]
mod common;

#[path = "raft_comprehensive/linearizability_checker.rs"]
mod linearizability_checker;

#[path = "raft_comprehensive/linearizability_tests.rs"]
mod linearizability_tests;

#[path = "raft_comprehensive/consistency_tests.rs"]
mod consistency_tests;

#[path = "raft_comprehensive/log_streaming_tests.rs"]
mod log_streaming_tests;

#[path = "raft_comprehensive/batch_operation_tests.rs"]
mod batch_operation_tests;
