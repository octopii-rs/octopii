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
#[cfg(feature = "raft-rs-impl")]
pub mod filter;
pub mod util;

// Re-export commonly used items
#[cfg(feature = "raft-rs-impl")]
pub use filter::{
    ConditionalFilter, CountFilter, DelayFilter, Direction, DropPacketFilter, Filter, FilterError,
    FilterFactory, IsolationFilterFactory, MessageDuplicationFilter, MessageReorderFilter,
    MessageTypeFilter, PartitionFilter, PartitionFilterFactory, ThrottleFilter,
};
pub use util::{alloc_port, assert_eq_debug, eventually, temp_dir};
