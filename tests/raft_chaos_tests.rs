/// Raft chaos and stress test suite
///
/// This test suite covers chaos engineering and stress testing including:
/// - Chaos tests (rapid crashes, rolling restarts, combined failures)
/// - Network partitions and healing
/// - Message chaos (reordering, duplication, delays)
/// - Short-duration stress tests
/// - Real partition scenarios

// Test infrastructure from TiKV
#[path = "test_infrastructure/mod.rs"]
#[macro_use]
mod test_infrastructure;

#[path = "raft_comprehensive/common.rs"]
mod common;

#[path = "raft_comprehensive/chaos_tests.rs"]
mod chaos_tests;

#[path = "raft_comprehensive/message_chaos_tests.rs"]
mod message_chaos_tests;

#[path = "raft_comprehensive/partition_tests.rs"]
mod partition_tests;

#[path = "raft_comprehensive/real_partition_tests.rs"]
mod real_partition_tests;

#[path = "raft_comprehensive/short_duration_stress_tests.rs"]
mod short_duration_stress_tests;
