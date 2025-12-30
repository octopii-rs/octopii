mod block;
mod config;
mod paths;
mod runtime;
mod storage;
pub mod vfs;

pub use block::Entry;
pub use config::{disable_fd_backend, enable_fd_backend, FsyncSchedule};
pub use runtime::{ReadConsistency, WalIndex, Walrus};

#[doc(hidden)]
pub fn __set_thread_namespace_for_tests(key: &str) {
    paths::set_thread_namespace(key);
}

#[doc(hidden)]
pub fn __clear_thread_namespace_for_tests() {
    paths::clear_thread_namespace();
}

#[doc(hidden)]
pub fn __current_thread_namespace_for_tests() -> Option<String> {
    paths::thread_namespace()
}

/// Set a per-thread WAL root directory for simulation tests.
#[doc(hidden)]
#[cfg(feature = "simulation")]
pub fn __set_thread_wal_data_dir_for_tests(path: std::path::PathBuf) {
    config::set_thread_wal_data_dir_for_tests(path);
}

/// Clear the per-thread WAL root directory override for simulation tests.
#[doc(hidden)]
#[cfg(feature = "simulation")]
pub fn __clear_thread_wal_data_dir_for_tests() {
    config::clear_thread_wal_data_dir_for_tests();
}

/// Clear all cached storage and tracker instances (used for simulation crash testing)
#[doc(hidden)]
#[cfg(feature = "simulation")]
pub fn __clear_storage_cache_for_tests() {
    storage::SharedMmapKeeper::clear_all();
    runtime::clear_tracker_state_for_tests();
}
