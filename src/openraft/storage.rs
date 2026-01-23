#![cfg(feature = "openraft")]

mod log_store;
pub mod state_machine;
mod wal;
mod wal_log_store;

pub use log_store::MemLogStore;
pub(crate) use log_store::MemLogStoreInner;
pub use state_machine::{MemStateMachine, StateMachineData, StoredSnapshot};
pub use wal_log_store::{new_wal_log_store, WalLogStore};
pub(crate) use wal_log_store::WalLogRecord;

#[cfg(all(test, feature = "simulation", feature = "openraft"))]
mod tests;
