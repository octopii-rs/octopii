use std::sync::mpsc;
use std::sync::{Arc, OnceLock};

mod allocator;
mod background;
mod index;
mod reader;
mod walrus;
mod walrus_read;
mod walrus_write;
mod writer;

#[allow(unused_imports)]
pub use index::{BlockPos, WalIndex};
pub use walrus::{ReadConsistency, Walrus};

pub(super) static DELETION_TX: OnceLock<Arc<mpsc::Sender<String>>> = OnceLock::new();

/// Clear all global tracker state (used for simulation crash testing)
#[cfg(feature = "simulation")]
pub(crate) fn clear_tracker_state_for_tests() {
    allocator::BlockStateTracker::clear_all();
    allocator::FileStateTracker::clear_all();
}
