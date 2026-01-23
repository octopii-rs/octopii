#![cfg(all(test, feature = "simulation", feature = "openraft"))]

use super::WalLogStore;
use crate::openraft::types::AppTypeConfig;
use crate::wal::wal::vfs::sim;
use crate::wal::WriteAheadLog;
use openraft::{Entry, LogId};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub fn sync_oracle_from_store(
    rt: &tokio::runtime::Runtime,
    store: &mut WalLogStore,
    oracle_log: &mut BTreeMap<u64, Entry<AppTypeConfig>>,
    oracle_vote: &mut Option<openraft::Vote<AppTypeConfig>>,
    oracle_committed: &mut Option<LogId<AppTypeConfig>>,
    oracle_last_purged: &mut Option<LogId<AppTypeConfig>>,
    next_index: &mut u64,
) {
    let state = rt.block_on(store.get_log_state()).expect("read log state failed");
    *oracle_last_purged = state.last_purged_log_id.clone();

    let first_index = state.last_purged_log_id.map(|p| p.index + 1).unwrap_or(1);
    let last_index = state.last_log_id.map(|l| l.index).unwrap_or(0);

    oracle_log.clear();
    if last_index >= first_index {
        let logs = rt
            .block_on(store.try_get_log_entries(first_index..=last_index))
            .expect("read logs failed");
        for entry in logs {
            oracle_log.insert(entry.log_id.index, entry);
        }
    }

    *oracle_vote = rt.block_on(store.read_vote()).expect("read vote failed");
    *oracle_committed = rt.block_on(store.read_committed()).expect("read committed failed");
    *next_index = last_index + 1;
}

pub fn create_wal_with_retry(
    rt: &tokio::runtime::Runtime,
    wal_path: std::path::PathBuf,
    scenario_seed: u64,
    retries: usize,
) -> Arc<WriteAheadLog> {
    for attempt in 0..retries {
        match rt.block_on(WriteAheadLog::new(wal_path.clone(), 0, Duration::from_millis(0))) {
            Ok(wal) => return Arc::new(wal),
            Err(_) => {
                sim::advance_time(Duration::from_millis(1));
                if attempt + 1 == retries {
                    break;
                }
            }
        }
    }
    panic!(
        "Failed to create WriteAheadLog after {} retries (seed={})",
        retries, scenario_seed
    );
}
