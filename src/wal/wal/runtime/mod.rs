use std::sync::mpsc;
use std::sync::Arc;

#[cfg(not(feature = "simulation"))]
use std::sync::OnceLock;

#[cfg(feature = "simulation")]
use std::cell::RefCell;

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

#[cfg(not(feature = "simulation"))]
pub(super) static DELETION_TX: OnceLock<Arc<mpsc::Sender<String>>> = OnceLock::new();

#[cfg(feature = "simulation")]
thread_local! {
    static DELETION_TX: RefCell<Option<Arc<mpsc::Sender<String>>>> = const { RefCell::new(None) };
}

#[cfg(feature = "simulation")]
pub(super) fn set_deletion_tx(tx: Arc<mpsc::Sender<String>>) {
    DELETION_TX.with(|tls| {
        *tls.borrow_mut() = Some(tx);
    });
}

#[cfg(feature = "simulation")]
pub(super) fn get_deletion_tx() -> Option<Arc<mpsc::Sender<String>>> {
    DELETION_TX.with(|tls| tls.borrow().clone())
}

/// Clear all global tracker state (used for simulation crash testing)
#[cfg(feature = "simulation")]
pub(crate) fn clear_tracker_state_for_tests() {
    allocator::BlockStateTracker::clear_all();
    allocator::FileStateTracker::clear_all();
}
