// Test infrastructure ported from TiKV
//
// This module provides battle-tested utilities for Raft cluster testing:
// - Network simulation (partitions, packet loss, delays)
// - Test utilities (port allocation, temp dirs, retry logic)
// - Filter system for message interception
//
// Source: TiKV tikv/components/test_raftstore and tikv/components/test_util
// Licensed under Apache-2.0

#[macro_use]
pub mod macros;
pub mod util;
pub mod filter;

// Re-export commonly used items
pub use util::{alloc_port, temp_dir, eventually, assert_eq_debug};
pub use filter::{
    Filter, FilterFactory, FilterError,
    DropPacketFilter, DelayFilter, PartitionFilter,
    PartitionFilterFactory, IsolationFilterFactory,
    MessageTypeFilter, ConditionalFilter, CountFilter,
    MessageDuplicationFilter, MessageReorderFilter, ThrottleFilter,
    Direction,
};
