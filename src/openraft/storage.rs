#![cfg(feature = "openraft")]

pub mod state_machine;
mod wal_log_store;

pub use state_machine::{MemStateMachine, StateMachineData, StoredSnapshot};
pub(crate) use wal_log_store::MemLogStoreInner;
pub(crate) use wal_log_store::WalLogRecord;
pub use wal_log_store::{new_wal_log_store, WalLogStore};

#[cfg(all(test, feature = "simulation", feature = "openraft"))]
mod tests;
