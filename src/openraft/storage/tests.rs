#![cfg(all(test, feature = "simulation", feature = "openraft"))]

use super::*;
use crate::wal::wal::vfs::sim::{self, SimConfig};
use crate::wal::wal::vfs;
use openraft::type_config::alias::CommittedLeaderIdOf;
use openraft::vote::RaftLeaderId;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::runtime::Builder;

mod test_utils;

#[test]
fn strictly_at_once_wal_log_store_recovery() {
    const SCENARIOS: usize = 10;
    const CRASH_CYCLES: usize = 4;
    const OPS_PER_CYCLE: usize = 200;
    const BASE_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
    const WAL_CREATE_RETRIES: usize = 32;

    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");

    for scenario in 0..SCENARIOS {
        let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
        sim::setup(SimConfig {
            seed: scenario_seed,
            io_error_rate: 0.18,
            initial_time_ns: 1_700_000_000_000_000_000,
            enable_partial_writes: true,
        });

        let root_dir = std::env::temp_dir().join(format!("walrus_strict_wal_log_store_{scenario}"));
        let prev_rate = sim::get_io_error_rate();
        let prev_partial = sim::get_partial_writes_enabled();
        sim::set_io_error_rate(0.0);
        sim::set_partial_writes_enabled(false);
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");
        sim::set_io_error_rate(prev_rate);
        sim::set_partial_writes_enabled(prev_partial);

        let wal_path = root_dir.join("wal_log_store.log");
        let mut rng = sim::XorShift128::new(scenario_seed ^ 0xd1b5_4a32_1a2b_3c4d);
        let mut oracle_log: BTreeMap<u64, Entry<AppTypeConfig>> = BTreeMap::new();
        let mut oracle_vote: Option<openraft::Vote<AppTypeConfig>> = None;
        let mut oracle_committed: Option<LogId<AppTypeConfig>> = None;
        let mut oracle_last_purged: Option<LogId<AppTypeConfig>> = None;
        let mut next_index: u64 = 1;

        for _cycle in 0..CRASH_CYCLES {
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let wal = test_utils::create_wal_with_retry(&rt, wal_path.clone(), scenario_seed, WAL_CREATE_RETRIES);
            let mut store = rt
                .block_on(WalLogStore::new(Arc::clone(&wal)))
                .expect("Failed to create WalLogStore");

            test_utils::sync_oracle_from_store(
                &rt,
                &mut store,
                &mut oracle_log,
                &mut oracle_vote,
                &mut oracle_committed,
                &mut oracle_last_purged,
                &mut next_index,
            );
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            for _ in 0..OPS_PER_CYCLE {
                let action = rng.next_usize(7);
                match action {
                    0 | 1 | 2 => {
                        let term = (rng.next_u64() % 10) + 1;
                        let node_id = (rng.next_u64() % 5) + 1;
                        let leader_id = CommittedLeaderIdOf::<AppTypeConfig>::new(term, node_id);
                        let payload_len = (rng.next_u64() % 24 + 8) as usize;
                        let mut payload = vec![0u8; payload_len];
                        for byte in &mut payload {
                            *byte = b'a' + (rng.next_u64() % 26) as u8;
                        }
                        let entry = Entry::<AppTypeConfig> {
                            log_id: LogId::new(leader_id, next_index),
                            payload: EntryPayload::Normal(AppEntry(payload)),
                        };
                        if rt
                            .block_on(store.persist_record(&WalLogRecord::LogEntry(entry.clone())))
                            .is_ok()
                        {
                            oracle_log.insert(next_index, entry);
                            next_index += 1;
                        }
                    }
                    3 => {
                        let term = (rng.next_u64() % 10) + 1;
                        let node_id = (rng.next_u64() % 5) + 1;
                        let vote = openraft::Vote::<AppTypeConfig>::new(term, node_id);
                        if rt
                            .block_on(store.persist_record(&WalLogRecord::Vote(vote.clone())))
                            .is_ok()
                        {
                            oracle_vote = Some(vote);
                        }
                    }
                    4 => {
                        let committed = oracle_log.iter().next_back().map(|(_, entry)| entry.log_id);
                        if rt
                            .block_on(store.persist_record(&WalLogRecord::Committed(committed)))
                            .is_ok()
                        {
                            oracle_committed = committed;
                        }
                    }
                    5 => {
                        let last_index = oracle_log.keys().next_back().copied().unwrap_or(0);
                        let committed_index = oracle_committed.map(|id| id.index).unwrap_or(0);
                        if last_index > committed_index {
                            let span = last_index - committed_index;
                            let cutoff = committed_index + 1 + (rng.next_u64() % span.max(1));
                            if let Some(entry) = oracle_log.get(&cutoff).cloned() {
                                if rt
                                    .block_on(store.persist_record(&WalLogRecord::Truncated(entry.log_id)))
                                    .is_ok()
                                {
                                    let keys: Vec<u64> =
                                        oracle_log.range(cutoff..).map(|(idx, _)| *idx).collect();
                                    for key in keys {
                                        oracle_log.remove(&key);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        let committed_index = oracle_committed.map(|id| id.index).unwrap_or(0);
                        if committed_index > 0 {
                            let purge_index = 1 + (rng.next_u64() % committed_index);
                            if let Some(entry) = oracle_log.get(&purge_index).cloned() {
                                if rt
                                    .block_on(store.persist_record(&WalLogRecord::Purged(entry.log_id)))
                                    .is_ok()
                                {
                                    let keys: Vec<u64> =
                                        oracle_log.range(..=purge_index).map(|(idx, _)| *idx).collect();
                                    for key in keys {
                                        oracle_log.remove(&key);
                                    }
                                    oracle_last_purged = Some(entry.log_id);
                                }
                            }
                        }
                    }
                }
            }

            drop(store);
            drop(wal);
            crate::wal::wal::__clear_storage_cache_for_tests();
            sim::advance_time(std::time::Duration::from_secs(1));
        }

        let prev_rate = sim::get_io_error_rate();
        let prev_partial = sim::get_partial_writes_enabled();
        sim::set_io_error_rate(0.0);
        sim::set_partial_writes_enabled(false);
        let wal = test_utils::create_wal_with_retry(
            &rt,
            root_dir.join("wal_log_store.log"),
            scenario_seed,
            WAL_CREATE_RETRIES,
        );
        let _recovered = rt
            .block_on(WalLogStore::new(Arc::clone(&wal)))
            .expect("Failed to create WalLogStore - final recovery failed");
        sim::set_io_error_rate(prev_rate);
        sim::set_partial_writes_enabled(prev_partial);

        sim::set_io_error_rate(0.0);
        sim::set_partial_writes_enabled(false);
        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }
}
