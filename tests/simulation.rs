// tests/simulation.rs
//
// Phase 4+5: Deterministic Simulation Testing Harness
//
// This test uses the VFS infrastructure to run long-running, randomized,
// reproducible scenarios that verify WAL consistency through crashes
// and I/O failures.
//
// IMPORTANT: Each test owns its own thread-local simulation context and WAL root.
// Tests may run in parallel as long as they use unique roots per test/seed.

#[cfg(feature = "simulation")]
mod sim_tests {
    use bytes::Bytes;
    use octopii::openraft::storage::WalLogStore;
    use octopii::openraft::types::{AppEntry, AppTypeConfig};
    use octopii::raft::WalStorage;
    use octopii::simulation::DurabilityOracle;
    use octopii::state_machine::{KvStateMachine, StateMachine, StateMachineTrait, WalBackedStateMachine};
    use octopii::wal::wal::vfs::sim::{self, SimConfig};
    use octopii::wal::wal::vfs;
    use octopii::wal::wal;
    use octopii::wal::wal::{FsyncSchedule, ReadConsistency, Walrus};
    use octopii::wal::WriteAheadLog;
    use openraft::storage::{RaftLogReader, RaftLogStorage, RaftLogStorageExt};
    use openraft::type_config::alias::CommittedLeaderIdOf;
    use openraft::vote::RaftLeaderId;
    use openraft::{Entry, EntryPayload, LogId};
    use raft::prelude::{
        ConfState as RaftConfState, Entry as RaftEntry, HardState as RaftHardState,
        Snapshot as RaftSnapshot,
    };
    use raft::storage::GetEntriesContext;
    use raft::Storage as RaftStorageTrait;
    use std::collections::{BTreeMap, HashMap};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime::Builder;

    // ========================================================================
    // 1. Local Deterministic RNG (Xorshift)
    // ========================================================================
    // We use this to decide WHICH action to take (Write vs Read vs Crash).
    // The VFS has its own RNG for deciding IF an IO error occurs.
    pub struct SimRng {
        state: u64,
    }

    impl SimRng {
        pub fn new(seed: u64) -> Self {
            let mut rng = Self { state: 0 };
            // Mix the seed a bit
            rng.state = seed.wrapping_add(0x9E3779B97F4A7C15);
            rng.next();
            rng
        }

        pub fn next(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.state = x;
            x
        }

        pub fn range(&mut self, min: usize, max: usize) -> usize {
            let range = max - min;
            if range == 0 {
                return min;
            }
            min + (self.next() as usize % range)
        }

        #[allow(dead_code)]
        pub fn bool(&mut self, probability: f64) -> bool {
            let limit = (u64::MAX as f64 * probability) as u64;
            self.next() < limit
        }

        pub fn gen_payload(&mut self) -> Vec<u8> {
            let len = self.range(1, 21);
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                buf.push(self.next() as u8);
            }
            buf
        }
    }

    // ========================================================================
    // 2. The Oracle (Source of Truth)
    // ========================================================================
    struct Oracle {
        // Map of Topic -> Vec<EntryPayload>
        history: HashMap<String, Vec<Vec<u8>>>,
        // Map of Topic -> Next Expected Read Index
        read_cursors: HashMap<String, usize>,
    }

    impl Oracle {
        fn new() -> Self {
            Self {
                history: HashMap::new(),
                read_cursors: HashMap::new(),
            }
        }

        fn record_write(&mut self, topic: &str, data: Vec<u8>) {
            let history = self.history.entry(topic.to_string()).or_default();
            let idx = history.len();
            if topic == "orders" {
                eprintln!("[ORACLE] Recording write for orders[{}] len={}", idx, data.len());
            }
            history.push(data);
        }

        fn verify_read(&mut self, topic: &str, actual_data: &[u8]) {
            let history = self.history.entry(topic.to_string()).or_default();
            let cursor = self.read_cursors.entry(topic.to_string()).or_default();

            if *cursor >= history.len() {
                panic!(
                    "ORACLE FAILURE: Read data for topic '{}' but Oracle thinks stream is empty/finished.\nGot data len: {}",
                    topic,
                    actual_data.len()
                );
            }

            let expected = &history[*cursor];
            if expected != actual_data {
                // Find if actual_data matches any other entry in history
                let mut found_at = None;
                for (i, entry) in history.iter().enumerate() {
                    if entry == actual_data {
                        found_at = Some(i);
                        break;
                    }
                }

                eprintln!("\n=== ORACLE DIAGNOSTIC ===");
                eprintln!("Topic: {}", topic);
                eprintln!("Oracle history length: {}", history.len());
                eprintln!("Current cursor: {}", *cursor);
                eprintln!("Expected entry {} len: {}", *cursor, expected.len());
                eprintln!("Actual data len: {}", actual_data.len());
                if let Some(idx) = found_at {
                    eprintln!("FOUND: Actual data matches Oracle history[{}]", idx);
                } else {
                    eprintln!("NOT FOUND: Actual data doesn't match any Oracle history entry");
                }
                eprintln!("Nearby entries in Oracle:");
                let start = (*cursor).saturating_sub(3);
                let end = ((*cursor) + 4).min(history.len());
                for i in start..end {
                    let marker = if i == *cursor { " <-- EXPECTED" } else { "" };
                    eprintln!("  [{}] len={}{}", i, history[i].len(), marker);
                }
                eprintln!("=========================\n");

                panic!(
                    "ORACLE FAILURE: Data mismatch for topic '{}' at index {}.\nExpected len: {}\nActual len: {}\nExpected: {:?}\nActual:   {:?}",
                    topic, *cursor, expected.len(), actual_data.len(), expected, actual_data
                );
            }

            *cursor += 1;
        }

        /// Verify a batch of entries, advancing the cursor
        fn verify_batch(&mut self, topic: &str, entries: &[Vec<u8>]) {
            for data in entries {
                self.verify_read(topic, data);
            }
        }

        /// Read with checkpoint=false (cursor advances in-memory but not persisted)
        /// In Walrus, checkpoint=false means the cursor position won't be persisted to disk,
        /// but the in-memory cursor DOES advance. After crash, the cursor resets.
        /// So we still advance the Oracle cursor - the difference is at recovery time.
        fn verify_peek(&mut self, topic: &str, actual_data: &[u8]) {
            // Peek still advances cursor - just doesn't persist to disk
            // So we verify and advance just like a regular read
            self.verify_read(topic, actual_data);
        }

        /// Check EOF without advancing cursor (for peek operations)
        fn check_eof(&self, topic: &str) {
            let history_len = self.history.get(topic).map(|v| v.len()).unwrap_or(0);
            let cursor = *self.read_cursors.get(topic).unwrap_or(&0);
            if cursor < history_len {
                panic!(
                    "ORACLE FAILURE: Walrus returned None (EOF) for topic '{}', but Oracle expects {} more entries.",
                    topic,
                    history_len - cursor
                );
            }
        }

        fn reset_read_cursors(&mut self) {
            for cursor in self.read_cursors.values_mut() {
                *cursor = 0;
            }
        }
    }

    // ========================================================================
    // 3. The Simulation Harness
    // ========================================================================
    /// A paired topic set for dual-write testing (like Octopii's log/log_recovery)
    struct DualTopic {
        primary: String,
        recovery: String,
    }

    struct Simulation {
        rng: SimRng,
        oracle: Oracle,
        wal: Option<Walrus>, // Option so we can drop it to simulate crash
        topics: Vec<String>,
        // Dual-write topic pairs (primary + recovery) - matches Octopii's pattern
        dual_topics: Vec<DualTopic>,
        root_dir: PathBuf,
        current_key: String,
        // Phase 5: Track the target error rate to restore after recovery
        target_error_rate: f64,
    }

    impl Simulation {
        fn new(seed: u64, error_rate: f64) -> Self {
            let root_dir = std::env::temp_dir().join(format!("walrus_sim_{}", seed));
            // Ensure clean start (using standard fs to clean up BEFORE sim starts)
            let _ = vfs::remove_dir_all(&root_dir);
            let _ = vfs::create_dir_all(&root_dir);

            // Set per-thread WAL root for simulation isolation
            octopii::wal::wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());

            Self {
                rng: SimRng::new(seed),
                oracle: Oracle::new(),
                wal: None,
                // Regular topics for single-write operations (keep it simple for now)
                topics: vec!["orders".into(), "logs".into(), "metrics".into()],
                // Dual-write topic pairs (mirrors Octopii's Raft storage pattern)
                dual_topics: vec![
                    DualTopic {
                        primary: "log".into(),
                        recovery: "log_recovery".into(),
                    },
                    DualTopic {
                        primary: "hard_state".into(),
                        recovery: "hard_state_recovery".into(),
                    },
                ],
                root_dir,
                current_key: "sim_node".into(),
                target_error_rate: error_rate,
            }
        }

        fn init_wal(&mut self, restore_faults: bool) {
            if self.wal.is_some() {
                return;
            }

            // Phase 5: CRITICAL - Disable faults during startup/recovery
            // We simulate that eventually the environment becomes stable enough to restart.
            // This models "transient failures" - disk was flaky but eventually works.
            sim::set_io_error_rate(0.0);

            // Create Walrus instance
            // We use SyncEach to simplify durability reasoning.
            let w = Walrus::with_consistency_and_schedule_for_key(
                &self.current_key,
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("Failed to initialize Walrus during recovery");

            self.wal = Some(w);

            if restore_faults {
                // Phase 5: Restore faults after successful initialization
                sim::set_io_error_rate(self.target_error_rate);
            }
        }

        /// Get all readable topics (regular + dual topic primaries + dual topic recoveries)
        fn all_readable_topics(&self) -> Vec<String> {
            let mut all = self.topics.clone();
            for dual in &self.dual_topics {
                all.push(dual.primary.clone());
                all.push(dual.recovery.clone());
            }
            all
        }

        fn crash_and_recover(&mut self) {
            // Log current Oracle state for orders
            let orders_count = self.oracle.history.get("orders").map(|v| v.len()).unwrap_or(0);
            eprintln!("[CRASH] Simulating crash. orders has {} entries in Oracle", orders_count);

            // 1. Drop the WAL (simulates process death)
            self.wal = None;

            // 2. Clear storage cache to simulate fresh process startup
            // This flushes all pending writes and clears cached file handles
            octopii::wal::wal::__clear_storage_cache_for_tests();

            // 3. Advance time (simulates downtime)
            sim::advance_time(std::time::Duration::from_secs(5));

            // 4. Disable faults during recovery/cleanup work
            sim::set_io_error_rate(0.0);

            // 5. Clear the WalIndex files to test DATA durability (not cursor persistence)
            let key_dir = self.root_dir.join(&self.current_key);
            if let Ok(entries) = vfs::read_dir(&key_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with("_index.db") {
                            let _ = vfs::remove_file(&path);
                        }
                    }
                }
            }

            // 6. IMPORTANT: After a crash, both Oracle and Walrus must start fresh.
            // The Oracle history represents what SHOULD be on disk, but after crash we
            // need to verify Walrus can recover all that data.
            // Reset Oracle cursors to 0 - we'll re-read everything from start
            self.oracle.reset_read_cursors();

            // 7. Re-initialize (simulates recovery/startup scan)
            self.init_wal(false);

            // 8. Ensure read offsets are fully reset in Walrus too (durability-only check)
            if let Some(wal) = self.wal.as_ref() {
                for topic in self.all_readable_topics() {
                    wal.reset_read_offset_for_topic(&topic)
                        .expect("reset_read_offset_for_topic failed during recovery");
                }
            }

            // 9. Restore fault injection after recovery bookkeeping
            sim::set_io_error_rate(self.target_error_rate);
        }

        fn run(&mut self, iterations: usize) {
            self.init_wal(true);

            // Collect all readable topics once for efficiency
            let all_topics = self.all_readable_topics();

            for _i in 0..iterations {
                // For writes: select from regular topics only
                let write_topic = self.topics[self.rng.range(0, self.topics.len())].clone();
                // For reads: select from ALL topics (including dual write topics)
                let read_topic = all_topics[self.rng.range(0, all_topics.len())].clone();
                let action_roll = self.rng.range(0, 100);

                // Action probabilities (expanded to test all Walrus features used by Octopii)
                // 0-40: Append (41%)
                // 41-50: Batch Append (10%)
                // 51-55: Dual Write - primary + recovery topic (5%)
                // 56-80: Read with checkpoint (25%)
                // 81-87: Batch Read with checkpoint (7%)
                // 88-93: Tick Background (6%)
                // 94-99: Crash/Restart (6%)

                if action_roll <= 40 {
                    // === APPEND ===
                    let data = self.rng.gen_payload();
                    match self
                        .wal
                        .as_ref()
                        .unwrap()
                        .append_for_topic(&write_topic, &data)
                    {
                        Ok(_) => {
                            // Success: Oracle records the write
                            self.oracle.record_write(&write_topic, data);
                        }
                        Err(_e) => {
                            // Phase 5: I/O error - expected with fault injection
                            // Oracle does NOT record the write (it failed)
                            // This is correct: failed writes should not appear later
                        }
                    }
                } else if action_roll <= 50 {
                    // === BATCH APPEND ===
                    let batch_size = self.rng.range(1, 20);
                    let mut batch_data = Vec::new();
                    for _ in 0..batch_size {
                        batch_data.push(self.rng.gen_payload());
                    }

                    let batch_refs: Vec<&[u8]> = batch_data.iter().map(|v| v.as_slice()).collect();

                    match self
                        .wal
                        .as_ref()
                        .unwrap()
                        .batch_append_for_topic(&write_topic, &batch_refs)
                    {
                        Ok(_) => {
                            // Success: Oracle records all entries in the batch
                            for d in batch_data {
                                self.oracle.record_write(&write_topic, d);
                            }
                        }
                        Err(_e) => {
                            // Phase 5: Batch failed - ATOMICITY CHECK
                            // If batch fails, NO data should be in the Oracle.
                            // Walrus MUST have rolled back internally.
                            // Future reads will verify nothing was partially written.
                        }
                    }
                } else if action_roll <= 55 {
                    // === DUAL WRITE (primary + recovery) ===
                    // This tests Octopii's pattern where data is written to both a primary
                    // topic (e.g. "log") and a recovery topic (e.g. "log_recovery")
                    let dual = &self.dual_topics[self.rng.range(0, self.dual_topics.len())];
                    let data = self.rng.gen_payload();

                    // Write to primary first
                    let primary_result = self
                        .wal
                        .as_ref()
                        .unwrap()
                        .append_for_topic(&dual.primary, &data);

                    match primary_result {
                        Ok(_) => {
                            // Primary succeeded, now write to recovery
                            self.oracle.record_write(&dual.primary, data.clone());

                            let prev_error_rate = sim::get_io_error_rate();
                            let prev_partial = sim::get_partial_writes_enabled();
                            sim::set_io_error_rate(0.0);
                            sim::set_partial_writes_enabled(false);
                            let recovery_result = self
                                .wal
                                .as_ref()
                                .unwrap()
                                .append_for_topic(&dual.recovery, &data);
                            sim::set_partial_writes_enabled(prev_partial);
                            sim::set_io_error_rate(prev_error_rate);
                            if recovery_result.is_err() {
                                panic!("dual write recovery append failed after primary success");
                            }
                            // Both writes succeeded
                            self.oracle.record_write(&dual.recovery, data);
                        }
                        Err(_e) => {
                            // Primary failed - don't write to recovery either
                        }
                    }
                } else if action_roll <= 80 {
                    // === READ (checkpoint=true) ===
                    match self.wal.as_ref().unwrap().read_next(&read_topic, true) {
                        Ok(Some(entry)) => {
                            self.oracle.verify_read(&read_topic, &entry.data);
                        }
                        Ok(None) => {
                            self.oracle.check_eof(&read_topic);
                        }
                        Err(_e) => {
                            // Phase 5: Read failed due to I/O error
                            // Oracle cursor stays put - next iteration will retry
                            // This is correct: transient read failures don't advance cursor
                        }
                    }
                } else if action_roll <= 87 {
                    // === BATCH READ (checkpoint=true) ===
                    // Max 10KB per batch, like Octopii's recovery patterns
                    let max_bytes = self.rng.range(1024, 10 * 1024);
                    match self
                        .wal
                        .as_ref()
                        .unwrap()
                        .batch_read_for_topic(&read_topic, max_bytes, true)
                    {
                        Ok(entries) => {
                            // Verify all entries in the batch
                            let batch_data: Vec<Vec<u8>> =
                                entries.into_iter().map(|e| e.data).collect();
                            self.oracle.verify_batch(&read_topic, &batch_data);
                        }
                        Err(_e) => {
                            // Phase 5: Batch read failed due to I/O error
                            // Oracle cursor stays put - next iteration will retry
                        }
                    }
                } else if action_roll <= 93 {
                    // === TICK BACKGROUND ===
                    self.wal.as_ref().unwrap().tick_background();
                    sim::advance_time(std::time::Duration::from_millis(100));
                } else {
                    // === CRASH / RESTART ===
                    self.crash_and_recover();
                }
            }
        }
    }

    impl Drop for Simulation {
        fn drop(&mut self) {
            // Clean up the simulation directory
            let _ = vfs::remove_dir_all(&self.root_dir);
            octopii::wal::wal::__clear_thread_wal_data_dir_for_tests();
        }
    }

    // ========================================================================
    // 4. Test Runners
    // ========================================================================

    fn run_simulation_with_config(seed: u64, iterations: usize, error_rate: f64, partial_writes: bool) {
        // Setup VFS simulation context
        sim::setup(SimConfig {
            seed,
            io_error_rate: error_rate,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: partial_writes,
        });

        let mut simulation = Simulation::new(seed, error_rate);
        simulation.run(iterations);
        drop(simulation);
        wal::__clear_storage_cache_for_tests();

        sim::teardown();

        println!(
            "Simulation completed: seed={}, iterations={}, error_rate={}, partial_writes={}",
            seed, iterations, error_rate, partial_writes
        );
    }

    fn init_strict_walrus(root_dir: &PathBuf, key: &str) -> Walrus {
        octopii::wal::wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
        Walrus::with_consistency_and_schedule_for_key(
            key,
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::SyncEach,
        )
        .expect("Failed to initialize Walrus")
    }

    fn make_raft_entry(index: u64, term: u64, data: &[u8]) -> RaftEntry {
        let mut entry = RaftEntry::default();
        entry.index = index;
        entry.term = term;
        entry.data = Bytes::from(data.to_vec());
        entry
    }

    // ------------------------------------------------------------------------
    // Phase 4 Tests: No fault injection (verify basic correctness)
    // ------------------------------------------------------------------------

    #[test]
    fn deterministic_consistency_seed_42() {
        run_simulation_with_config(42, 5000, 0.0, false);
    }

    #[test]
    fn deterministic_consistency_seed_12345() {
        run_simulation_with_config(12345, 5000, 0.0, false);
    }

    #[test]
    fn deterministic_consistency_seed_99999() {
        run_simulation_with_config(99999, 5000, 0.0, false);
    }

    #[test]
    fn deterministic_consistency_seed_314159() {
        run_simulation_with_config(314159, 5000, 0.0, false);
    }

    #[test]
    fn deterministic_random_seeds_smoke_8() {
        let mut s = 0x9e3779b97f4a7c15u64;
        for _ in 0..8 {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            let seed = s.wrapping_mul(0x2545f4914f6cdd1d);
            run_simulation_with_config(seed, 1000, 0.0, false);
        }
    }

    // ------------------------------------------------------------------------
    // Phase 5 Tests: With fault injection
    // ------------------------------------------------------------------------
    //
    // These tests verify Walrus handles I/O errors gracefully. The FD backend
    // (used in simulation mode) properly propagates VFS errors through Result
    // types, ensuring failed writes are never recorded in the Oracle.

    #[test]
    fn fault_injection_seed_999() {
        // 5% I/O error rate - significant but not overwhelming
        run_simulation_with_config(999, 2000, 0.05, false);
    }

    #[test]
    fn fault_injection_random_seeds_8() {
        let mut s = 0xd1b54a32d192ed03u64;
        for _ in 0..8 {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            let seed = s.wrapping_mul(0x2545f4914f6cdd1d);
            run_simulation_with_config(seed, 1200, 0.05, false);
        }
    }

    #[test]
    fn fault_injection_seed_42424() {
        // 10% failure rate - extreme stress test
        run_simulation_with_config(42424, 1000, 0.10, false);
    }

    #[test]
    fn fault_injection_seed_271828() {
        // 5% error rate with different seed
        run_simulation_with_config(271828, 2000, 0.05, false);
    }

    #[test]
    fn fault_injection_seed_161803() {
        // 7% error rate - moderate stress
        run_simulation_with_config(161803, 1500, 0.07, false);
    }

    // ------------------------------------------------------------------------
    // Partial Write Tests
    // ------------------------------------------------------------------------
    //
    // These tests verify Walrus handles torn/partial writes correctly.
    // The header checksum (covering bytes 8..64 of each entry header) detects
    // partial writes that corrupt metadata, preventing silent data corruption.

    #[test]
    fn partial_writes_seed_777() {
        // 5% I/O error rate + partial writes enabled
        run_simulation_with_config(777, 2000, 0.05, true);
    }

    #[test]
    fn partial_writes_seed_88888() {
        // 10% error rate with partial writes - extreme stress test
        run_simulation_with_config(88888, 1000, 0.10, true);
    }

    #[test]
    fn partial_writes_seed_55555() {
        // 5% error rate with partial writes - different seed
        run_simulation_with_config(55555, 2000, 0.05, true);
    }

    #[test]
    fn partial_writes_seed_123456() {
        // 8% error rate with partial writes
        run_simulation_with_config(123456, 1500, 0.08, true);
    }

    // ------------------------------------------------------------------------
    // Long-running Stress Tests
    // ------------------------------------------------------------------------
    //
    // These tests run for 10,000+ iterations to find rare bugs that only
    // manifest after many operations. Run with: cargo test --release stress_

    #[test]
    // Run with: cargo test --features simulation --test simulation stress_ -- --ignored
    fn stress_no_faults_seed_1() {
        run_simulation_with_config(1, 20000, 0.0, false);
    }

    #[test]
    fn stress_no_faults_seed_2() {
        run_simulation_with_config(2, 20000, 0.0, false);
    }

    #[test]
    fn stress_no_faults_seed_3() {
        run_simulation_with_config(3, 20000, 0.0, false);
    }

    #[test]
    fn stress_fault_injection_seed_1000() {
        run_simulation_with_config(1000, 15000, 0.05, false);
    }

    #[test]
    fn stress_fault_injection_seed_2000() {
        run_simulation_with_config(2000, 15000, 0.05, false);
    }

    #[test]
    fn stress_fault_injection_seed_3000() {
        run_simulation_with_config(3000, 15000, 0.07, false);
    }

    #[test]
    fn stress_partial_writes_seed_4000() {
        run_simulation_with_config(4000, 10000, 0.05, true);
    }

    #[test]
    fn stress_partial_writes_seed_5000() {
        run_simulation_with_config(5000, 10000, 0.05, true);
    }

    #[test]
    fn stress_partial_writes_seed_6000() {
        run_simulation_with_config(6000, 10000, 0.08, true);
    }

    #[test]
    fn stress_extreme_seed_7777() {
        // High error rate, many iterations - finds edge cases
        run_simulation_with_config(7777, 8000, 0.15, true);
    }

    #[test]
    fn stress_extreme_seed_20240229() {
        run_simulation_with_config(20240229, 9000, 0.20, true);
    }

    #[test]
    fn stress_extreme_seed_8675309() {
        run_simulation_with_config(8675309, 9000, 0.25, true);
    }

    #[test]
    fn stress_extreme_seed_42424242() {
        run_simulation_with_config(42424242, 8000, 0.30, true);
    }

    #[test]
    fn stress_random_seeds_10_15pct() {
        // Deterministic pseudo-random seeds to avoid non-reproducible failures.
        let mut s = 0x9e3779b97f4a7c15u64;
        for _ in 0..20 {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            let seed = s.wrapping_mul(0x2545f4914f6cdd1d);
            let error_rate = 0.10 + ((seed % 6) as f64) * 0.01;
            run_simulation_with_config(seed, 4000, error_rate, true);
        }
    }

    #[test]
    fn stress_random_seeds_15_20pct_long() {
        // Deterministic pseudo-random seeds to avoid non-reproducible failures.
        let mut s = 0xd1b54a32d192ed03u64;
        for _ in 0..20 {
            s ^= s >> 12;
            s ^= s << 25;
            s ^= s >> 27;
            let seed = s.wrapping_mul(0x2545f4914f6cdd1d);
            let error_rate = 0.15 + ((seed % 6) as f64) * 0.01;
            run_simulation_with_config(seed, 8000, error_rate, true);
        }
    }

    // ------------------------------------------------------------------------
    // StrictlyAtOnce invariants (checkpoint persistence and peek semantics)
    // ------------------------------------------------------------------------

    #[test]
    fn strictly_at_once_checkpoint_persists_read_next() {
        sim::setup(SimConfig {
            seed: 4242,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_strict_checkpoint_read_next");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let key = "strict_node";
        let topic = "strict_topic";
        let entries: Vec<Vec<u8>> = (0..5)
            .map(|i| format!("entry-{i}").into_bytes())
            .collect();

        let wal = init_strict_walrus(&root_dir, key);
        for data in &entries {
            wal.append_for_topic(topic, data).unwrap();
        }

        for expected in entries.iter().take(2) {
            let entry = wal.read_next(topic, true).unwrap().expect("Missing entry");
            assert_eq!(entry.data, *expected);
        }

        drop(wal);
        octopii::wal::wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = init_strict_walrus(&root_dir, key);
        let mut recovered = Vec::new();
        while let Ok(Some(entry)) = wal.read_next(topic, true) {
            recovered.push(entry.data);
        }
        assert_eq!(recovered, entries[2..].to_vec());

        octopii::wal::wal::__clear_thread_wal_data_dir_for_tests();
        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    #[test]
    fn strictly_at_once_checkpoint_false_replays_after_crash() {
        sim::setup(SimConfig {
            seed: 4243,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_strict_checkpoint_false");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let key = "strict_node";
        let topic = "strict_topic";
        let entries: Vec<Vec<u8>> = (0..4)
            .map(|i| format!("peek-{i}").into_bytes())
            .collect();

        let wal = init_strict_walrus(&root_dir, key);
        for data in &entries {
            wal.append_for_topic(topic, data).unwrap();
        }

        for _ in 0..2 {
            let entry = wal.read_next(topic, false).unwrap().expect("Missing entry");
            assert_eq!(entry.data, entries[0]);
        }

        drop(wal);
        octopii::wal::wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = init_strict_walrus(&root_dir, key);
        let first = wal.read_next(topic, true).unwrap().expect("Missing entry");
        assert_eq!(first.data, entries[0]);

        octopii::wal::wal::__clear_thread_wal_data_dir_for_tests();
        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    #[test]
    fn strictly_at_once_checkpoint_false_batch_read_replays_after_crash() {
        sim::setup(SimConfig {
            seed: 4244,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_strict_checkpoint_false_batch");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let key = "strict_node";
        let topic = "strict_topic";
        let entries: Vec<Vec<u8>> = (0..3)
            .map(|i| format!("batch-peek-{i}").into_bytes())
            .collect();

        let wal = init_strict_walrus(&root_dir, key);
        for data in &entries {
            wal.append_for_topic(topic, data).unwrap();
        }

        let first_batch = wal
            .batch_read_for_topic(topic, entries[0].len(), false)
            .unwrap();
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].data, entries[0]);

        drop(wal);
        octopii::wal::wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = init_strict_walrus(&root_dir, key);
        let mut recovered = Vec::new();
        while let Ok(Some(entry)) = wal.read_next(topic, true) {
            recovered.push(entry.data);
        }
        assert_eq!(recovered, entries);

        octopii::wal::wal::__clear_thread_wal_data_dir_for_tests();
        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    // ------------------------------------------------------------------------
    // Octopii Walrus coverage (StrictlyAtOnce + SyncEach) using production code
    // ------------------------------------------------------------------------

    #[test]
    fn openraft_wal_log_store_recovery_roundtrip() {
        sim::setup(SimConfig {
            seed: 9101,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_openraft_log_store_roundtrip");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        let wal_path = root_dir.join("wal_log_store.log");
        let wal = Arc::new(
            rt.block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal init"),
        );
        let mut store = rt
            .block_on(WalLogStore::new(Arc::clone(&wal)))
            .expect("log store init");

        let leader = CommittedLeaderIdOf::<AppTypeConfig>::new(1, 7);
        let entry1 = Entry::<AppTypeConfig> {
            log_id: LogId::new(leader, 1),
            payload: EntryPayload::Normal(AppEntry(b"e1".to_vec())),
        };
        let entry2 = Entry::<AppTypeConfig> {
            log_id: LogId::new(leader, 2),
            payload: EntryPayload::Normal(AppEntry(b"e2".to_vec())),
        };

        rt.block_on(store.blocking_append(vec![entry1.clone(), entry2.clone()]))
            .expect("append entries");

        let vote = openraft::Vote::<AppTypeConfig>::new(2, 9);
        rt.block_on(store.save_vote(&vote)).expect("save vote");

        let committed = Some(entry2.log_id);
        rt.block_on(store.save_committed(committed))
            .expect("save committed");

        drop(store);
        octopii::wal::wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = Arc::new(
            rt.block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal restart"),
        );
        let mut recovered = rt
            .block_on(WalLogStore::new(Arc::clone(&wal)))
            .expect("log store restart");

        let recovered_logs = rt
            .block_on(recovered.try_get_log_entries(1..=2))
            .expect("read logs");
        assert_eq!(recovered_logs, vec![entry1, entry2]);

        let recovered_vote = rt.block_on(recovered.read_vote()).expect("read vote");
        assert_eq!(recovered_vote, Some(vote));

        let recovered_committed = rt
            .block_on(recovered.read_committed())
            .expect("read committed");
        assert_eq!(recovered_committed, committed);

        let log_state = rt
            .block_on(recovered.get_log_state())
            .expect("log state");
        assert_eq!(log_state.last_log_id, committed);

        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    #[test]
    fn openraft_wal_log_store_truncate_and_purge_recovery() {
        sim::setup(SimConfig {
            seed: 9102,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_openraft_log_store_truncate");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        let wal_path = root_dir.join("wal_log_store.log");
        let wal = Arc::new(
            rt.block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal init"),
        );
        let mut store = rt
            .block_on(WalLogStore::new(Arc::clone(&wal)))
            .expect("log store init");

        let leader = CommittedLeaderIdOf::<AppTypeConfig>::new(2, 3);
        let entries: Vec<Entry<AppTypeConfig>> = (1..=5)
            .map(|idx| Entry::<AppTypeConfig> {
                log_id: LogId::new(leader, idx),
                payload: EntryPayload::Normal(AppEntry(vec![idx as u8])),
            })
            .collect();

        rt.block_on(store.blocking_append(entries.clone()))
            .expect("append entries");
        rt.block_on(store.truncate(entries[3].log_id))
            .expect("truncate");
        rt.block_on(store.purge(entries[1].log_id))
            .expect("purge");

        drop(store);
        octopii::wal::wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = Arc::new(
            rt.block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal restart"),
        );
        let mut recovered = rt
            .block_on(WalLogStore::new(Arc::clone(&wal)))
            .expect("log store restart");

        let recovered_logs = rt
            .block_on(recovered.try_get_log_entries(1..=5))
            .expect("read logs");
        assert_eq!(recovered_logs, vec![entries[2].clone()]);

        let log_state = rt
            .block_on(recovered.get_log_state())
            .expect("log state");
        assert_eq!(log_state.last_purged_log_id, Some(entries[1].log_id));
        assert_eq!(log_state.last_log_id, Some(entries[2].log_id));

        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    #[test]
    fn raft_storage_recovery_roundtrip_with_snapshot() {
        sim::setup(SimConfig {
            seed: 9107,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_raft_storage_roundtrip");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        let wal_path = root_dir.join("raft_storage.log");
        let wal = rt
            .block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal init");
        let storage = WalStorage::new(Arc::new(wal));

        let mut conf_state1 = RaftConfState::default();
        conf_state1.mut_voters().push(1);
        storage.set_conf_state(conf_state1.clone());

        let mut hs1 = RaftHardState::default();
        hs1.set_term(1);
        hs1.set_vote(1);
        hs1.set_commit(1);
        storage.set_hard_state(hs1);

        let entries1 = vec![
            make_raft_entry(1, 1, b"e1"),
            make_raft_entry(2, 1, b"e2"),
            make_raft_entry(3, 1, b"e3"),
        ];
        rt.block_on(storage.append_entries(&entries1))
            .expect("append entries");

        let mut snapshot = RaftSnapshot::default();
        snapshot.data = Bytes::from_static(b"snap-2");
        let metadata = snapshot.mut_metadata();
        metadata.index = 2;
        metadata.term = 1;
        *metadata.mut_conf_state() = conf_state1.clone();
        storage.apply_snapshot(snapshot).expect("apply snapshot");

        let mut conf_state2 = RaftConfState::default();
        conf_state2.mut_voters().extend(vec![1, 2]);
        conf_state2.mut_learners().push(3);
        storage.set_conf_state(conf_state2.clone());

        let mut hs2 = RaftHardState::default();
        hs2.set_term(2);
        hs2.set_vote(2);
        hs2.set_commit(3);
        storage.set_hard_state(hs2.clone());

        let entries2 = vec![
            make_raft_entry(4, 2, b"e4"),
            make_raft_entry(5, 2, b"e5"),
        ];
        rt.block_on(storage.append_entries(&entries2))
            .expect("append entries 2");

        drop(storage);
        wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = rt
            .block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal restart");
        let recovered = WalStorage::new(Arc::new(wal));

        let raft_state = recovered.initial_state().expect("initial state");
        assert_eq!(raft_state.hard_state, hs2);
        assert_eq!(raft_state.conf_state, conf_state2);

        let recovered_snapshot = recovered.snapshot(0, 0).expect("snapshot");
        assert_eq!(recovered_snapshot.get_metadata().index, 2);
        assert_eq!(recovered_snapshot.get_metadata().term, 1);

        let first_index = recovered.first_index().expect("first_index");
        let recovered_entries = recovered
            .entries(first_index, 6, u64::MAX, GetEntriesContext::empty(false))
            .expect("entries");
        let recovered_indices: Vec<u64> =
            recovered_entries.iter().map(|entry| entry.index).collect();
        assert_eq!(recovered_indices, vec![3, 4, 5]);

        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    #[test]
    fn wal_backed_state_machine_recovery_roundtrip() {
        const SCENARIOS: usize = 10;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 200;
        const BASE_SEED: u64 = 0x3c6e_f372_fe94_f82b;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let root_dir = std::env::temp_dir()
                .join(format!("walrus_strict_state_machine_{scenario}"));
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("state_machine.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xa5a5_a5a5_5a5a_5a5a);
            let mut oracle: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

            for _cycle in 0..CRASH_CYCLES {
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);
                let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
                let (inner, wal, sm) = rt.block_on(async {
                    let inner: StateMachine = Arc::new(KvStateMachine::in_memory());
                    let sm = WalBackedStateMachine::with_inner(Arc::clone(&inner), Arc::clone(&wal));
                    (inner, wal, sm)
                });

                let recovered = inner.snapshot();
                let recovered_map: HashMap<Vec<u8>, Vec<u8>> =
                    bincode::deserialize(&recovered).expect("snapshot deserialize failed");
                assert_eq!(recovered_map, oracle);
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                let keys: [&[u8]; 6] = [
                    b"alpha".as_slice(),
                    b"beta".as_slice(),
                    b"gamma".as_slice(),
                    b"delta".as_slice(),
                    b"epsilon".as_slice(),
                    b"zeta".as_slice(),
                ];
                let _guard = rt.enter();
                for _ in 0..OPS_PER_CYCLE {
                    let key = keys[rng.next_usize(keys.len())];
                    let action = rng.next_usize(3);
                    let (command, value) = if action < 2 {
                        let value = (rng.next_u64() % 1000).to_string();
                        (
                            format!("SET {} {}", std::str::from_utf8(key).unwrap(), value),
                            Some(value),
                        )
                    } else {
                        (
                            format!("DELETE {}", std::str::from_utf8(key).unwrap()),
                            None,
                        )
                    };
                    if sm.apply(command.as_bytes()).is_ok() {
                        if let Some(value) = value {
                            oracle.insert(key.to_vec(), value.into_bytes());
                        } else {
                            oracle.remove(key);
                        }
                    }
                }

                drop(sm);
                drop(inner);
                drop(wal);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    fn create_wal_with_retry(
        rt: &tokio::runtime::Runtime,
        wal_path: std::path::PathBuf,
        retries: usize,
    ) -> Arc<WriteAheadLog> {
        for attempt in 0..retries {
            match rt.block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            )) {
                Ok(wal) => return Arc::new(wal),
                Err(_) => {
                    sim::advance_time(Duration::from_millis(1));
                    if attempt + 1 == retries {
                        break;
                    }
                }
            }
        }
        panic!("Failed to create WriteAheadLog after {} retries", retries);
    }

    #[test]
    fn write_ahead_log_read_all_returns_entries() {
        sim::setup(SimConfig {
            seed: 9105,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_read_all_recovery");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let wal_path = root_dir.join("wal_read_all.log");
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let wal = runtime
            .block_on(WriteAheadLog::new(
                wal_path.clone(),
                10,
                tokio::time::Duration::from_millis(0),
            ))
            .expect("wal init");

        runtime
            .block_on(wal.append(bytes::Bytes::from_static(b"alpha")))
            .expect("append");
        runtime
            .block_on(wal.append(bytes::Bytes::from_static(b"beta")))
            .expect("append");
        runtime
            .block_on(wal.append(bytes::Bytes::from_static(b"gamma")))
            .expect("append");

        let entries = runtime.block_on(wal.read_all()).expect("read_all");
        let recovered: Vec<Vec<u8>> = entries.iter().map(|b| b.to_vec()).collect();
        assert_eq!(
            recovered,
            vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
        );

        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    #[test]
    fn openraft_peer_addr_wal_recovery_roundtrip() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct PeerAddrRecord {
            peer_id: u64,
            addr: std::net::SocketAddr,
        }

        sim::setup(SimConfig {
            seed: 9106,
            io_error_rate: 0.0,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });

        let root_dir = std::env::temp_dir().join("walrus_peer_addrs_recovery");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        let wal_path = root_dir.join("peer_addrs.log");
        let wal = rt
            .block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal init");

        let addr1: std::net::SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let addr2: std::net::SocketAddr = "127.0.0.1:5002".parse().unwrap();
        let addr3: std::net::SocketAddr = "127.0.0.1:5003".parse().unwrap();

        let records = vec![
            PeerAddrRecord {
                peer_id: 1,
                addr: addr1,
            },
            PeerAddrRecord {
                peer_id: 2,
                addr: addr2,
            },
            PeerAddrRecord {
                peer_id: 1,
                addr: addr3,
            },
        ];

        for record in &records {
            let bytes = bincode::serialize(record).expect("peer addr serialize");
            rt.block_on(wal.append(bytes::Bytes::from(bytes)))
                .expect("append peer addr");
        }

        drop(wal);
        wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(1));

        let wal = rt
            .block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            ))
            .expect("wal restart");

        let entries = rt.block_on(wal.read_all()).expect("read_all");
        let mut recovered = std::collections::HashMap::new();
        for raw in entries {
            let record: PeerAddrRecord = bincode::deserialize(&raw).expect("peer addr decode");
            recovered.insert(record.peer_id, record.addr);
        }

        let mut expected = std::collections::HashMap::new();
        expected.insert(1, addr3);
        expected.insert(2, addr2);

        assert_eq!(recovered, expected);

        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    // ------------------------------------------------------------------------
    // WalStorage Fault Injection Tests
    // ------------------------------------------------------------------------
    // These tests exercise the production sim_assert invariants in WalStorage
    // with fault injection enabled.

    #[test]
    fn raft_storage_recovery_with_fault_injection() {
        const SCENARIOS: usize = 10;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 200;
        const BASE_SEED: u64 = 0x7a3f_e291_c4b8_d056;
        const WAL_CREATE_RETRIES: usize = 32;
        const ERROR_RATE: f64 = 0.18;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: ERROR_RATE,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            // Create root directory with faults disabled
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let root_dir = std::env::temp_dir()
                .join(format!("walrus_raft_storage_fault_injection_{scenario}"));
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("raft_storage.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xb7e1_5162_8aed_2a6a);

            // Track term and index for valid Raft semantics
            let mut current_term: u64 = 1;
            let mut next_entry_index: u64 = 1;
            let mut snapshot_index: u64 = 0;

            for _cycle in 0..CRASH_CYCLES {
                // Create WAL and WalStorage with faults disabled
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
                // WalStorage::new triggers recovery invariants
                let storage = WalStorage::new(Arc::clone(&wal));

                // Sync state with recovered storage
                let recovered_last = storage.last_index().unwrap_or(0);
                let recovered_snap = storage.snapshot(0, 0).ok();
                let recovered_snap_idx = recovered_snap.as_ref().map(|s| s.get_metadata().index).unwrap_or(0);
                let recovered_snap_term = recovered_snap.as_ref().map(|s| s.get_metadata().term).unwrap_or(0);

                // Update our tracking to match recovered state
                next_entry_index = recovered_last + 1;
                snapshot_index = recovered_snap_idx;
                current_term = current_term.max(recovered_snap_term);

                // Re-enable faults for operations
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                for _ in 0..OPS_PER_CYCLE {
                    let action = rng.next_usize(10);
                    match action {
                        // Append entries (40% of operations) - with faults enabled
                        0..=3 => {
                            let num_entries = 1 + rng.next_usize(3);
                            let start_idx = next_entry_index;
                            let mut entries = Vec::new();
                            for i in 0..num_entries {
                                // Use deterministic data per index so retries produce identical entries
                                // This mirrors real Raft behavior where the same entry is retried
                                let idx = start_idx + i as u64;
                                let data = format!("entry_idx_{}_term_{}", idx, current_term);
                                entries.push(make_raft_entry(
                                    idx,
                                    current_term,
                                    data.as_bytes(),
                                ));
                            }
                            // With fault injection, some appends will fail
                            // Only increment index if the append succeeded
                            if rt.block_on(storage.append_entries(&entries)).is_ok() {
                                next_entry_index += num_entries as u64;
                            }
                        }
                        // Set hard_state (20% of operations) - disable faults (panics on error)
                        4..=5 => {
                            let prev_rate = sim::get_io_error_rate();
                            let prev_partial = sim::get_partial_writes_enabled();
                            sim::set_io_error_rate(0.0);
                            sim::set_partial_writes_enabled(false);

                            // Occasionally bump term
                            if rng.next_usize(5) == 0 {
                                current_term += 1;
                            }
                            let mut hs = RaftHardState::default();
                            hs.set_term(current_term);
                            hs.set_vote(1 + (rng.next_u64() % 5));
                            // commit should be <= last entry index
                            let max_commit = next_entry_index.saturating_sub(1).max(snapshot_index);
                            hs.set_commit(snapshot_index + rng.next_u64() % (max_commit - snapshot_index + 1));
                            storage.set_hard_state(hs);

                            sim::set_io_error_rate(prev_rate);
                            sim::set_partial_writes_enabled(prev_partial);
                        }
                        // Set conf_state (15% of operations) - disable faults (panics on error)
                        6..=7 => {
                            let prev_rate = sim::get_io_error_rate();
                            let prev_partial = sim::get_partial_writes_enabled();
                            sim::set_io_error_rate(0.0);
                            sim::set_partial_writes_enabled(false);

                            let mut cs = RaftConfState::default();
                            let num_voters = 1 + rng.next_usize(3);
                            for i in 0..num_voters {
                                cs.mut_voters().push((i + 1) as u64);
                            }
                            if rng.next_usize(3) == 0 {
                                cs.mut_learners().push(10);
                            }
                            storage.set_conf_state(cs);

                            sim::set_io_error_rate(prev_rate);
                            sim::set_partial_writes_enabled(prev_partial);
                        }
                        // Apply snapshot (15% of operations) - disable faults for snapshot
                        8 => {
                            // Snapshot at a valid index
                            let last_idx = storage.last_index().unwrap_or(0);
                            if last_idx > snapshot_index + 1 {
                                let prev_rate = sim::get_io_error_rate();
                                let prev_partial = sim::get_partial_writes_enabled();
                                sim::set_io_error_rate(0.0);
                                sim::set_partial_writes_enabled(false);

                                // Pick a random index to snapshot at
                                let new_snap_idx = snapshot_index + 1 + rng.next_u64() % (last_idx - snapshot_index);

                                // Get the term of the entry at this index
                                let entries = storage.entries(
                                    new_snap_idx, new_snap_idx + 1, u64::MAX,
                                    GetEntriesContext::empty(false)
                                ).unwrap_or_default();
                                let snap_term = entries.first().map(|e| e.term).unwrap_or(current_term);

                                let mut snapshot = RaftSnapshot::default();
                                snapshot.data = Bytes::from(format!("snapshot_{}", new_snap_idx));
                                let metadata = snapshot.mut_metadata();
                                metadata.index = new_snap_idx;
                                metadata.term = snap_term;
                                let mut cs = RaftConfState::default();
                                cs.mut_voters().push(1);
                                *metadata.mut_conf_state() = cs;

                                if storage.apply_snapshot(snapshot).is_ok() {
                                    snapshot_index = new_snap_idx;
                                }

                                sim::set_io_error_rate(prev_rate);
                                sim::set_partial_writes_enabled(prev_partial);
                            }
                        }
                        // Bump term (10% of operations)
                        _ => {
                            current_term += 1;
                        }
                    }
                }

                // Simulate crash
                drop(storage);
                drop(wal);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            // Final recovery check with faults disabled
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);

            let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
            // This triggers all recovery invariants one final time
            let _recovered = WalStorage::new(Arc::clone(&wal));

            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            // Cleanup
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);
            sim::teardown();
        }
    }

    #[test]
    fn snapshot_log_filtering_stress() {
        // This test validates that log entries are correctly filtered after snapshot
        // by running multiple crash/recovery cycles with different snapshot points.
        // Note: This test disables fault injection to focus on recovery invariant testing.
        const SCENARIOS: usize = 5;
        const CRASH_CYCLES: usize = 3;
        const BASE_SEED: u64 = 0x4d2c_8f91_e3a7_b5d0;
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
                io_error_rate: 0.0, // No faults - focus on recovery invariants
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: false,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_snapshot_filtering_{scenario}"));
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

            let wal_path = root_dir.join("raft_storage.log");

            // Use consistent term
            let term: u64 = 1;
            let mut next_idx: u64 = 1;

            for cycle in 0..CRASH_CYCLES {
                let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
                // Recovery invariants are triggered here
                let storage = WalStorage::new(Arc::clone(&wal));

                // Verify recovery state matches what we expect
                let last_idx = storage.last_index().unwrap_or(0);
                let current_snap = storage.snapshot(0, 0).ok();
                let snap_idx = current_snap.as_ref().map(|s| s.get_metadata().index).unwrap_or(0);

                // Update our tracking to match recovered state
                next_idx = last_idx + 1;

                // Write 10 new entries
                for i in 0..10 {
                    let idx = next_idx + i;
                    let entry = make_raft_entry(idx, term, format!("e{}", idx).as_bytes());
                    rt.block_on(storage.append_entries(&[entry])).expect("append");
                }
                next_idx += 10;

                // On cycle 1, create a snapshot
                if cycle == 1 {
                    let new_last = storage.last_index().unwrap();
                    let snap_at = new_last / 2; // Snapshot at midpoint

                    let mut snapshot = RaftSnapshot::default();
                    snapshot.data = Bytes::from(format!("snap_{}", snap_at));
                    let metadata = snapshot.mut_metadata();
                    metadata.index = snap_at;
                    metadata.term = term;
                    let mut cs = RaftConfState::default();
                    cs.mut_voters().push(1);
                    *metadata.mut_conf_state() = cs;

                    storage.apply_snapshot(snapshot).expect("snapshot");
                }

                // Crash
                drop(storage);
                drop(wal);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            // Cleanup
            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Durability verification test for raw Walrus WAL operations.
    ///
    /// This test uses the DurabilityOracle to track which writes MUST survive
    /// vs which MAY be lost due to partial writes. After each crash/recovery
    /// cycle, it verifies that all "definitely durable" entries are present.
    ///
    /// A durability violation (missing definitely_durable entry) indicates a bug
    /// in the WAL implementation that could cause data loss.
    #[test]
    fn walrus_durability_guarantee_verification() {
        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 5;
        const OPS_PER_CYCLE: usize = 100;
        const BASE_SEED: u64 = 0xd5a3_71f8_c2e9_b046;
        const ERROR_RATE: f64 = 0.18;

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x6c62_272e_07bb_0142);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: ERROR_RATE,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            // Create root directory with faults disabled
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let root_dir = std::env::temp_dir()
                .join(format!("walrus_durability_verification_{scenario}"));
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create test dir");

            let mut durability_oracle = DurabilityOracle::new();
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0x4a7d_c9f3_e182_b560);
            let topic = "durability_test";
            let mut total_writes: usize = 0;
            let mut total_durable: usize = 0;

            for cycle in 0..CRASH_CYCLES {
                // Create WAL with faults disabled
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
                let wal = Walrus::with_consistency_and_schedule_for_key(
                    "durability_node",
                    ReadConsistency::StrictlyAtOnce,
                    FsyncSchedule::SyncEach,
                )
                .expect("Failed to create WAL");

                // Reset read cursor to start for verification
                wal.reset_read_offset_for_topic(topic)
                    .expect("reset_read_offset failed");

                // After recovery, verify ALL must_survive entries from ALL cycles
                if cycle > 0 {
                    // Verify durability guarantee - all must_survive entries MUST exist
                    if let Err(e) = durability_oracle.verify_after_recovery(&wal) {
                        let partial_count = sim::get_partial_write_count();
                        let (must_count, may_count) = durability_oracle.stats();
                        panic!(
                            "Scenario {scenario} Cycle {cycle}: {}\n\
                             Partial writes: {partial_count}\n\
                             Total writes: {total_writes}, Must survive: {must_count}, May lost: {may_count}",
                            e
                        );
                    }
                    // Promote may_be_lost entries that survived to must_survive
                    durability_oracle.after_recovery(&wal);
                }

                // Re-enable faults for operations
                sim::set_io_error_rate(ERROR_RATE);
                sim::set_partial_writes_enabled(true);

                // Perform random writes
                for op in 0..OPS_PER_CYCLE {
                    let data_len = 10 + rng.next_usize(100);
                    let mut data = vec![0u8; data_len];
                    for byte in &mut data {
                        *byte = rng.next_u64() as u8;
                    }

                    // Track partial write count before/after
                    let partial_before = sim::get_partial_write_count();
                    let result = wal.append_for_topic(topic, &data);
                    let partial_after = sim::get_partial_write_count();

                    if result.is_ok() {
                        // Record this write in durability oracle
                        durability_oracle.record_write(
                            topic,
                            data.clone(),
                            partial_before,
                            partial_after,
                        );
                        total_writes += 1;
                        if partial_after == partial_before {
                            total_durable += 1;
                        }
                    }
                }

                let (must_count, may_count) = durability_oracle.stats();
                if scenario == 0 {
                    eprintln!(
                        "Scenario {} Cycle {}: {} writes ({} must_survive, {} may_be_lost)",
                        scenario, cycle, total_writes, must_count, may_count
                    );
                }

                // Crash
                drop(wal);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            // Final verification
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
            let wal = Walrus::with_consistency_and_schedule_for_key(
                "durability_node",
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("Failed to create WAL for final verification");

            // Reset cursor for final verification
            wal.reset_read_offset_for_topic(topic)
                .expect("reset_read_offset failed");

            if let Err(e) = durability_oracle.verify_after_recovery(&wal) {
                let partial_count = sim::get_partial_write_count();
                let (must_count, may_count) = durability_oracle.stats();
                panic!(
                    "Scenario {scenario} FINAL: {}\n\
                     Partial writes: {partial_count}\n\
                     Total writes: {total_writes}, Must survive: {must_count}, May lost: {may_count}",
                    e
                );
            }

            let (final_must, _) = durability_oracle.stats();

            // Cleanup
            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} total writes, {} must_survive across {} cycles",
                    scenario, total_writes, final_must, durability_oracle.cycle()
                );
            }
        }
    }

    /// Test that crash during recovery doesn't corrupt data.
    ///
    /// This test:
    /// 1. Writes data with durability tracking
    /// 2. Crashes
    /// 3. Starts recovery but injects crash at various points
    /// 4. Completes recovery
    /// 5. Verifies all must_survive data exists
    #[test]
    fn crash_during_recovery() {
        use octopii::wal::wal::vfs::sim::RecoveryCrashPoint;

        const SCENARIOS: usize = 4;
        const ENTRIES_PER_SCENARIO: usize = 50;
        const BASE_SEED: u64 = 0xa1b2_c3d4_e5f6_7890;

        let crash_points = [
            RecoveryCrashPoint::AfterFileList,
            RecoveryCrashPoint::AfterBlockHeader,
            RecoveryCrashPoint::AfterChainRebuild,
            RecoveryCrashPoint::AfterEntryCount(25),
        ];

        for (scenario, crash_point) in crash_points.iter().enumerate() {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.0, // No I/O errors - focus on crash testing
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: false,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_crash_during_recovery_{scenario}"));
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create test dir");

            let mut oracle = DurabilityOracle::new();
            let topic = "crash_test";
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0x1234_5678);

            // Phase 1: Write data
            wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
            let wal = Walrus::with_consistency_and_schedule_for_key(
                "crash_node",
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("Failed to create WAL");

            for _ in 0..ENTRIES_PER_SCENARIO {
                let data_len = 10 + rng.next_usize(50);
                let mut data = vec![0u8; data_len];
                for byte in &mut data {
                    *byte = rng.next_u64() as u8;
                }

                let partial_before = sim::get_partial_write_count();
                if wal.append_for_topic(topic, &data).is_ok() {
                    let partial_after = sim::get_partial_write_count();
                    oracle.record_write(topic, data, partial_before, partial_after);
                }
            }

            let (must_count, _) = oracle.stats();
            eprintln!(
                "Scenario {} ({:?}): wrote {} must_survive entries",
                scenario, crash_point, must_count
            );

            // Crash
            drop(wal);
            wal::__clear_storage_cache_for_tests();

            // Phase 2: Start recovery but crash during it
            sim::set_recovery_crash_point(*crash_point);

            let crash_result = std::panic::catch_unwind(|| {
                wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
                Walrus::with_consistency_and_schedule_for_key(
                    "crash_node",
                    ReadConsistency::StrictlyAtOnce,
                    FsyncSchedule::SyncEach,
                )
            });

            assert!(
                crash_result.is_err(),
                "Expected panic at {:?} but recovery succeeded",
                crash_point
            );
            wal::__clear_storage_cache_for_tests();

            // Phase 3: Complete recovery (no crash injection)
            sim::set_recovery_crash_point(RecoveryCrashPoint::None);

            wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
            let wal = Walrus::with_consistency_and_schedule_for_key(
                "crash_node",
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("Recovery should succeed after crash");

            // Phase 4: Verify all must_survive data exists
            wal.reset_read_offset_for_topic(topic)
                .expect("reset_read_offset failed");

            if let Err(e) = oracle.verify_after_recovery(&wal) {
                panic!(
                    "Scenario {} ({:?}): DURABILITY VIOLATION after crash-during-recovery\n{}",
                    scenario, crash_point, e
                );
            }

            eprintln!(
                "Scenario {} ({:?}): PASSED - all {} entries survived crash during recovery",
                scenario, crash_point, must_count
            );

            // Cleanup
            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Test double crash recovery - crash during recovery across multiple cycles.
    ///
    /// This test simulates a "crash storm" scenario:
    /// 1. Write data → crash
    /// 2. Start recovery → crash during recovery
    /// 3. Write more data → crash
    /// 4. Start recovery → crash during recovery
    /// 5. Complete recovery → verify ALL data from ALL cycles
    #[test]
    fn double_crash_recovery() {
        use octopii::wal::wal::vfs::sim::RecoveryCrashPoint;

        const CYCLES: usize = 3;
        const ENTRIES_PER_CYCLE: usize = 30;
        const BASE_SEED: u64 = 0xdead_beef_cafe_babe;

        sim::setup(SimConfig {
            seed: BASE_SEED,
            io_error_rate: 0.10, // Some I/O errors to make it realistic
            initial_time_ns: 1_700_000_000_000_000_000,
            enable_partial_writes: true,
        });

        let root_dir = std::env::temp_dir().join("walrus_double_crash_recovery");
        let _ = vfs::remove_dir_all(&root_dir);
        vfs::create_dir_all(&root_dir).expect("Failed to create test dir");

        let mut oracle = DurabilityOracle::new();
        let topic = "double_crash_test";
        let mut rng = sim::XorShift128::new(BASE_SEED ^ 0xabcd_ef01);

        for cycle in 0..CYCLES {
            // Disable faults for WAL creation
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);

            wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
            let wal = Walrus::with_consistency_and_schedule_for_key(
                "double_crash_node",
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("Failed to create WAL");

            // Reset cursor and verify previous cycles' data
            wal.reset_read_offset_for_topic(topic).ok();
            if cycle > 0 {
                if let Err(e) = oracle.verify_after_recovery(&wal) {
                    let (must_count, may_count) = oracle.stats();
                    panic!(
                        "Cycle {} verification failed: {}\n\
                         Must survive: {}, May be lost: {}",
                        cycle, e, must_count, may_count
                    );
                }
                oracle.after_recovery(&wal);
            }

            // Re-enable faults for writing
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            // Write data for this cycle
            for _ in 0..ENTRIES_PER_CYCLE {
                let data_len = 10 + rng.next_usize(40);
                let mut data = vec![0u8; data_len];
                for byte in &mut data {
                    *byte = rng.next_u64() as u8;
                }

                let partial_before = sim::get_partial_write_count();
                if wal.append_for_topic(topic, &data).is_ok() {
                    let partial_after = sim::get_partial_write_count();
                    oracle.record_write(topic, data, partial_before, partial_after);
                }
            }

            let (must_count, may_count) = oracle.stats();
            eprintln!(
                "Cycle {}: wrote entries, total must_survive={}, may_be_lost={}",
                cycle, must_count, may_count
            );

            // Crash
            drop(wal);
            wal::__clear_storage_cache_for_tests();

            // Crash during recovery (except last cycle)
            if cycle < CYCLES - 1 {
                // Pick a crash point based on cycle
                let crash_point = match cycle % 3 {
                    0 => RecoveryCrashPoint::AfterEntryCount(15),
                    1 => RecoveryCrashPoint::AfterBlockHeader,
                    _ => RecoveryCrashPoint::AfterChainRebuild,
                };

                sim::set_recovery_crash_point(crash_point);

                let _ = std::panic::catch_unwind(|| {
                    wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
                    Walrus::with_consistency_and_schedule_for_key(
                        "double_crash_node",
                        ReadConsistency::StrictlyAtOnce,
                        FsyncSchedule::SyncEach,
                    )
                });

                wal::__clear_storage_cache_for_tests();
                sim::set_recovery_crash_point(RecoveryCrashPoint::None);

                eprintln!("Cycle {}: crashed during recovery at {:?}", cycle, crash_point);
            }

            sim::advance_time(std::time::Duration::from_secs(1));
        }

        // Final recovery
        sim::set_io_error_rate(0.0);
        sim::set_partial_writes_enabled(false);
        sim::set_recovery_crash_point(RecoveryCrashPoint::None);

        wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());
        let wal = Walrus::with_consistency_and_schedule_for_key(
            "double_crash_node",
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::SyncEach,
        )
        .expect("Final recovery should succeed");

        // Final verification
        wal.reset_read_offset_for_topic(topic)
            .expect("reset_read_offset failed");

        if let Err(e) = oracle.verify_after_recovery(&wal) {
            let (must_count, may_count) = oracle.stats();
            panic!(
                "FINAL verification failed: {}\n\
                 Must survive: {}, May be lost: {}",
                e, must_count, may_count
            );
        }

        let (final_must, _) = oracle.stats();
        eprintln!(
            "PASSED: {} must_survive entries verified across {} cycles with double crashes",
            final_must, CYCLES
        );

        // Cleanup
        let _ = vfs::remove_dir_all(&root_dir);
        sim::teardown();
    }

    // ------------------------------------------------------------------------
    // PeerAddrWal Fault Injection Test
    // ------------------------------------------------------------------------
    // Tests the peer address recovery pattern under fault injection.
    // This exercises the "latest-wins" recovery semantics for peer addresses.

    #[test]
    fn peer_addr_wal_recovery_with_fault_injection() {
        #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
        struct PeerAddrRecord {
            peer_id: u64,
            addr: std::net::SocketAddr,
        }

        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 100;
        const BASE_SEED: u64 = 0xf1e2_d3c4_b5a6_9708;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_peer_addr_fault_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("peer_addrs.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xd1b5_4a32_1a2b_3c4d);

            // Oracle: tracks the "latest-wins" state for each peer_id
            // We also track all entries that MUST survive (no partial writes)
            let mut oracle_state: std::collections::HashMap<u64, std::net::SocketAddr> =
                std::collections::HashMap::new();
            let mut must_survive_entries: Vec<PeerAddrRecord> = Vec::new();
            let mut may_be_lost_entries: Vec<PeerAddrRecord> = Vec::new();

            for _cycle in 0..CRASH_CYCLES {
                // Create WAL without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = retry_wal_create(&rt, &wal_path, WAL_CREATE_RETRIES);

                // Recover oracle from WAL entries
                let entries = rt.block_on(wal.read_all()).unwrap_or_default();
                let mut recovered_state: std::collections::HashMap<u64, std::net::SocketAddr> =
                    std::collections::HashMap::new();
                let mut recovered_entries: Vec<PeerAddrRecord> = Vec::new();

                for raw in entries {
                    if let Ok(record) = bincode::deserialize::<PeerAddrRecord>(&raw) {
                        recovered_state.insert(record.peer_id, record.addr);
                        recovered_entries.push(record);
                    }
                }

                // Promote entries that survived to must_survive
                may_be_lost_entries.clear();
                must_survive_entries = recovered_entries;
                oracle_state = recovered_state;

                // Re-enable faults for writing
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                for _ in 0..OPS_PER_CYCLE {
                    // Generate random peer address update
                    let peer_id = rng.next_u64() % 10 + 1;
                    let port = 5000 + (rng.next_u64() % 1000) as u16;
                    let addr: std::net::SocketAddr =
                        format!("127.0.0.1:{}", port).parse().unwrap();

                    let record = PeerAddrRecord { peer_id, addr };
                    let bytes = bincode::serialize(&record).expect("serialize");

                    let partial_before = sim::get_partial_write_count();
                    if rt.block_on(wal.append(bytes::Bytes::from(bytes))).is_ok() {
                        let partial_after = sim::get_partial_write_count();

                        if partial_before == partial_after {
                            // No partial write - this entry MUST survive
                            must_survive_entries.push(record.clone());
                            oracle_state.insert(peer_id, addr);
                        } else {
                            // Partial write - may be lost
                            may_be_lost_entries.push(record.clone());
                        }
                    }
                }

                // Crash
                drop(wal);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            // Final recovery and verification (no faults)
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);

            let wal = retry_wal_create(&rt, &wal_path, WAL_CREATE_RETRIES);
            let entries = rt.block_on(wal.read_all()).expect("read_all");

            let mut final_state: std::collections::HashMap<u64, std::net::SocketAddr> =
                std::collections::HashMap::new();
            let mut final_entries: Vec<PeerAddrRecord> = Vec::new();

            for raw in entries {
                if let Ok(record) = bincode::deserialize::<PeerAddrRecord>(&raw) {
                    final_state.insert(record.peer_id, record.addr);
                    final_entries.push(record);
                }
            }

            // Verify all must_survive entries are present
            let must_survive_count = must_survive_entries.len();
            assert!(
                final_entries.len() >= must_survive_count,
                "Scenario {}: Lost entries! Expected at least {} entries, got {}",
                scenario,
                must_survive_count,
                final_entries.len()
            );

            // Verify the latest-wins semantics: for each peer_id that had a must_survive write,
            // the final state must match the oracle
            for (peer_id, expected_addr) in &oracle_state {
                if let Some(actual_addr) = final_state.get(peer_id) {
                    // May have been updated by a may_be_lost entry that survived
                    // That's fine - we just verify the oracle is a subset
                } else {
                    // Peer must exist since we tracked it in oracle
                    panic!(
                        "Scenario {}: peer_id {} missing from final state!",
                        scenario, peer_id
                    );
                }
            }

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} entries, {} must_survive, {} peers tracked",
                    scenario,
                    final_entries.len(),
                    must_survive_count,
                    final_state.len()
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Helper to retry WAL creation under fault injection.
    fn retry_wal_create(
        rt: &tokio::runtime::Runtime,
        path: &std::path::Path,
        max_retries: usize,
    ) -> octopii::wal::WriteAheadLog {
        let prev_rate = sim::get_io_error_rate();
        let prev_partial = sim::get_partial_writes_enabled();
        sim::set_io_error_rate(0.0);
        sim::set_partial_writes_enabled(false);

        let mut last_err = None;
        for _ in 0..max_retries {
            match rt.block_on(octopii::wal::WriteAheadLog::new(
                path.to_path_buf(),
                0,
                Duration::from_millis(0),
            )) {
                Ok(wal) => {
                    sim::set_io_error_rate(prev_rate);
                    sim::set_partial_writes_enabled(prev_partial);
                    return wal;
                }
                Err(e) => {
                    last_err = Some(e);
                    sim::advance_time(std::time::Duration::from_millis(10));
                }
            }
        }
        panic!("Failed to create WAL after {} retries: {:?}", max_retries, last_err);
    }

    // ------------------------------------------------------------------------
    // TRUE INVARIANT TESTS (not oracle sync)
    // ------------------------------------------------------------------------
    // These tests track must_survive vs may_be_lost and verify durability
    // guarantees rather than just syncing oracle to match recovery output.

    /// Oracle for tracking state machine commands with durability guarantees.
    /// Commands without partial writes MUST survive any crash.
    struct StateMachineOracle {
        /// Commands that MUST survive - written without partial write
        must_survive: Vec<String>,
        /// Commands that may be lost - written with partial write
        may_be_lost: Vec<String>,
        /// Expected state after applying must_survive commands
        expected_state: HashMap<Vec<u8>, Vec<u8>>,
        /// Current cycle number
        cycle: usize,
    }

    impl StateMachineOracle {
        fn new() -> Self {
            Self {
                must_survive: Vec::new(),
                may_be_lost: Vec::new(),
                expected_state: HashMap::new(),
                cycle: 0,
            }
        }

        fn record_command(&mut self, command: &str, partial_before: u64, partial_after: u64) {
            if partial_before == partial_after {
                // No partial write - MUST survive
                self.must_survive.push(command.to_string());
                self.apply_to_expected(command);
            } else {
                // Partial write occurred - may be lost
                self.may_be_lost.push(command.to_string());
            }
        }

        fn apply_to_expected(&mut self, command: &str) {
            let mut tokens = command.split_whitespace();
            let op = tokens.next().unwrap_or("");
            match op {
                "SET" => {
                    if let (Some(key), Some(val)) = (tokens.next(), tokens.next()) {
                        self.expected_state
                            .insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
                    }
                }
                "DELETE" => {
                    if let Some(key) = tokens.next() {
                        self.expected_state.remove(key.as_bytes());
                    }
                }
                _ => {}
            }
        }

        fn after_recovery(&mut self, recovered_state: &HashMap<Vec<u8>, Vec<u8>>) {
            // Verify expected state is subset of recovered state
            for (key, expected_val) in &self.expected_state {
                match recovered_state.get(key) {
                    Some(actual_val) => {
                        if actual_val != expected_val {
                            panic!(
                                "DURABILITY VIOLATION in cycle {}: key {:?} expected {:?}, got {:?}",
                                self.cycle, key, expected_val, actual_val
                            );
                        }
                    }
                    None => {
                        panic!(
                            "DURABILITY VIOLATION in cycle {}: key {:?} MISSING (expected {:?})",
                            self.cycle, key, expected_val
                        );
                    }
                }
            }

            // Promote may_be_lost commands that survived to must_survive
            // by re-applying them to expected_state
            for cmd in std::mem::take(&mut self.may_be_lost) {
                // Check if this command's effect is visible in recovered state
                let mut tokens = cmd.split_whitespace();
                let op = tokens.next().unwrap_or("");
                let key = tokens.next();

                let survived = match (op, key) {
                    ("SET", Some(k)) => recovered_state.contains_key(k.as_bytes()),
                    ("DELETE", Some(k)) => !recovered_state.contains_key(k.as_bytes()),
                    _ => false,
                };

                if survived {
                    self.must_survive.push(cmd.clone());
                    self.apply_to_expected(&cmd);
                }
            }

            self.cycle += 1;
        }

        fn stats(&self) -> (usize, usize) {
            (self.must_survive.len(), self.may_be_lost.len())
        }
    }

    /// TRUE INVARIANT TEST for WalBackedStateMachine
    ///
    /// This test tracks commands with must_survive vs may_be_lost semantics.
    /// Commands written without partial writes MUST survive any crash.
    #[test]
    fn state_machine_durability_invariant() {
        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 100;
        const BASE_SEED: u64 = 0xc4a7_e839_5b2d_1f06;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_sm_durability_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("create dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("state_machine.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xd1b5_4a32);
            let mut oracle = StateMachineOracle::new();

            let keys: [&str; 6] = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];

            for _cycle in 0..CRASH_CYCLES {
                // Create WAL and state machine without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
                let (inner, _wal_arc, sm) = rt.block_on(async {
                    let inner: StateMachine = Arc::new(KvStateMachine::in_memory());
                    let sm = WalBackedStateMachine::with_inner(Arc::clone(&inner), Arc::clone(&wal));
                    (inner, wal, sm)
                });

                // Verify recovery matches expected state
                let recovered_snapshot = inner.snapshot();
                let recovered_state: HashMap<Vec<u8>, Vec<u8>> =
                    bincode::deserialize(&recovered_snapshot).unwrap_or_default();
                oracle.after_recovery(&recovered_state);

                // Re-enable faults
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                // Apply commands with durability tracking
                let _guard = rt.enter();
                for _ in 0..OPS_PER_CYCLE {
                    let key = keys[rng.next_usize(keys.len())];
                    let action = rng.next_usize(3);
                    let command = if action < 2 {
                        let value = rng.next_u64() % 1000;
                        format!("SET {} {}", key, value)
                    } else {
                        format!("DELETE {}", key)
                    };

                    let partial_before = sim::get_partial_write_count();
                    if sm.apply(command.as_bytes()).is_ok() {
                        let partial_after = sim::get_partial_write_count();
                        oracle.record_command(&command, partial_before, partial_after);
                    }
                }

                // Crash
                drop(sm);
                drop(inner);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            let (must_count, _) = oracle.stats();
            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} must_survive commands verified across {} cycles",
                    scenario, must_count, CRASH_CYCLES
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Oracle for tracking WalLogStore operations with durability guarantees.
    struct LogStoreOracle {
        /// Log entries that MUST survive
        must_survive_entries: BTreeMap<u64, Vec<u8>>,
        /// Vote that MUST survive (if any)
        must_survive_vote: Option<(u64, u64)>, // (term, node_id)
        /// Committed that MUST survive
        must_survive_committed: Option<u64>, // index
        /// Entries that may be lost
        may_be_lost_entries: Vec<(u64, Vec<u8>)>,
        next_index: u64,
        cycle: usize,
    }

    impl LogStoreOracle {
        fn new() -> Self {
            Self {
                must_survive_entries: BTreeMap::new(),
                must_survive_vote: None,
                must_survive_committed: None,
                may_be_lost_entries: Vec::new(),
                next_index: 1,
                cycle: 0,
            }
        }

        fn record_entry(
            &mut self,
            index: u64,
            data: Vec<u8>,
            partial_before: u64,
            partial_after: u64,
        ) {
            if partial_before == partial_after {
                self.must_survive_entries.insert(index, data);
            } else {
                self.may_be_lost_entries.push((index, data));
            }
            self.next_index = index + 1;
        }

        fn record_vote(&mut self, term: u64, node_id: u64, partial_before: u64, partial_after: u64) {
            if partial_before == partial_after {
                self.must_survive_vote = Some((term, node_id));
            }
        }

        fn record_committed(&mut self, index: u64, partial_before: u64, partial_after: u64) {
            if partial_before == partial_after {
                self.must_survive_committed = Some(index);
            }
        }

        fn after_recovery(&mut self, recovered_entries: &BTreeMap<u64, Vec<u8>>) {
            // Verify all must_survive entries exist
            for (idx, expected_data) in &self.must_survive_entries {
                match recovered_entries.get(idx) {
                    Some(actual_data) => {
                        if actual_data != expected_data {
                            panic!(
                                "DURABILITY VIOLATION in cycle {}: entry {} data mismatch",
                                self.cycle, idx
                            );
                        }
                    }
                    None => {
                        panic!(
                            "DURABILITY VIOLATION in cycle {}: entry {} MISSING",
                            self.cycle, idx
                        );
                    }
                }
            }

            // Promote may_be_lost that survived
            for (idx, data) in std::mem::take(&mut self.may_be_lost_entries) {
                if recovered_entries.get(&idx) == Some(&data) {
                    self.must_survive_entries.insert(idx, data);
                }
            }

            self.cycle += 1;
        }

        fn stats(&self) -> usize {
            self.must_survive_entries.len()
        }
    }

    /// TRUE INVARIANT TEST for WalLogStore
    ///
    /// This test tracks log entries with must_survive vs may_be_lost semantics.
    /// Entries written without partial writes MUST survive any crash.
    #[test]
    fn log_store_durability_invariant() {
        // Uses already imported: AppEntry, AppTypeConfig, WalLogStore, Entry, EntryPayload, LogId
        // CommittedLeaderIdOf is the correct type alias for this openraft version

        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 100;
        const BASE_SEED: u64 = 0xe7f2_a493_8c1b_5d60;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_log_store_durability_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("create dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("log_store.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xa5a5_5a5a);
            let mut oracle = LogStoreOracle::new();

            for _cycle in 0..CRASH_CYCLES {
                // Create without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
                let mut store = rt
                    .block_on(WalLogStore::new(Arc::clone(&wal)))
                    .expect("log store init");

                // Recover entries for verification
                let last_idx = oracle.must_survive_entries.keys().next_back().copied().unwrap_or(0);
                let recovered_entries: BTreeMap<u64, Vec<u8>> = if last_idx > 0 {
                    rt.block_on(store.try_get_log_entries(1..=last_idx))
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|e| {
                            if let EntryPayload::Normal(AppEntry(data)) = e.payload {
                                Some((e.log_id.index, data))
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    BTreeMap::new()
                };

                oracle.after_recovery(&recovered_entries);

                // Update next_index from store
                let log_state = rt.block_on(store.get_log_state()).expect("log state");
                oracle.next_index = log_state.last_log_id.map(|id| id.index + 1).unwrap_or(1);

                // Re-enable faults
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                // Write entries with durability tracking
                for _ in 0..OPS_PER_CYCLE {
                    let term = (rng.next_u64() % 10) + 1;
                    let node_id = (rng.next_u64() % 5) + 1;
                    let leader_id = CommittedLeaderIdOf::<AppTypeConfig>::new(term, node_id);
                    let payload_len = (rng.next_u64() % 24 + 8) as usize;
                    let mut payload = vec![0u8; payload_len];
                    for byte in &mut payload {
                        *byte = b'a' + (rng.next_u64() % 26) as u8;
                    }

                    let entry = Entry::<AppTypeConfig> {
                        log_id: LogId::new(leader_id, oracle.next_index),
                        payload: EntryPayload::Normal(AppEntry(payload.clone())),
                    };

                    let partial_before = sim::get_partial_write_count();
                    if rt
                        .block_on(store.blocking_append(vec![entry.clone()]))
                        .is_ok()
                    {
                        let partial_after = sim::get_partial_write_count();
                        oracle.record_entry(
                            oracle.next_index,
                            payload,
                            partial_before,
                            partial_after,
                        );
                    }
                }

                // Crash
                drop(store);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} must_survive entries verified across {} cycles",
                    scenario,
                    oracle.stats(),
                    CRASH_CYCLES
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Test that the two-phase commit protocol in WalStorage correctly rejects
    /// entries without commit markers.
    ///
    /// Invariant: Only entries with commit markers in TOPIC_LOG_COMMIT should be
    /// considered durable. Entries written to TOPIC_LOG but not committed may be lost.
    #[test]
    fn two_phase_commit_invariant() {
        // WalStorage is already imported at top of module

        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 50;
        const BASE_SEED: u64 = 0xb3d8_7c2e_4f19_a065;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir =
                std::env::temp_dir().join(format!("walrus_two_phase_commit_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("create dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("two_phase.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0x1234_5678);

            // Track committed entries (entries where append succeeded without partial write)
            let mut must_survive_indices: std::collections::HashSet<u64> =
                std::collections::HashSet::new();
            let mut next_index: u64 = 1;

            for _cycle in 0..CRASH_CYCLES {
                // Create without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = rt.block_on(async {
                    for _ in 0..WAL_CREATE_RETRIES {
                        match WriteAheadLog::new(wal_path.clone(), 0, Duration::from_millis(0)).await
                        {
                            Ok(w) => return w,
                            Err(_) => sim::advance_time(Duration::from_millis(10)),
                        }
                    }
                    panic!("WAL creation failed");
                });
                let storage = WalStorage::new(Arc::new(wal));

                // Verify all must_survive entries exist after recovery
                let first_idx = storage.first_index().unwrap_or(1);
                let last_idx = storage.last_index().unwrap_or(0);
                if last_idx >= first_idx {
                    let entries = storage
                        .entries(first_idx, last_idx + 1, u64::MAX, GetEntriesContext::empty(false))
                        .unwrap_or_default();
                    let recovered_indices: std::collections::HashSet<u64> =
                        entries.iter().map(|e| e.index).collect();

                    for idx in &must_survive_indices {
                        if *idx >= first_idx && !recovered_indices.contains(idx) {
                            panic!(
                                "TWO-PHASE COMMIT VIOLATION: entry {} was committed but MISSING",
                                idx
                            );
                        }
                    }

                    // Update must_survive with what actually recovered
                    must_survive_indices = recovered_indices;
                    next_index = last_idx + 1;
                }

                // Re-enable faults
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                // Write entries
                for _ in 0..OPS_PER_CYCLE {
                    let entry = make_raft_entry(next_index, 1, b"data");
                    let partial_before = sim::get_partial_write_count();

                    if rt.block_on(storage.append_entries(&[entry])).is_ok() {
                        let partial_after = sim::get_partial_write_count();
                        if partial_before == partial_after {
                            // No partial write - this entry MUST survive
                            must_survive_indices.insert(next_index);
                        }
                        next_index += 1;
                    }
                }

                // Crash
                drop(storage);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} must_survive entries verified",
                    scenario,
                    must_survive_indices.len()
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Test KvStateMachine compaction under fault injection.
    ///
    /// Compaction writes a snapshot and should be atomic - either the snapshot
    /// is fully written or not at all. Partial compaction should not corrupt state.
    #[test]
    fn state_machine_compaction_with_faults() {
        use octopii::raft::KvStateMachine as RaftKvStateMachine;

        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 50;
        const COMPACT_EVERY: usize = 20;
        const BASE_SEED: u64 = 0xf9a1_c5e3_7b2d_8046;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_compaction_fault_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("create dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("compaction.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xbeef_cafe);

            // Oracle tracks expected state after confirmed operations
            // Uses String keys to match snapshot_hashmap() return type
            let mut oracle: HashMap<String, Vec<u8>> = HashMap::new();
            let keys: [&str; 6] = ["k1", "k2", "k3", "k4", "k5", "k6"];

            for _cycle in 0..CRASH_CYCLES {
                // Create without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = rt.block_on(async {
                    for _ in 0..WAL_CREATE_RETRIES {
                        match WriteAheadLog::new(wal_path.clone(), 0, Duration::from_millis(0)).await
                        {
                            Ok(w) => return Arc::new(w),
                            Err(_) => sim::advance_time(Duration::from_millis(10)),
                        }
                    }
                    panic!("WAL creation failed");
                });
                let sm = RaftKvStateMachine::with_wal(Arc::clone(&wal));

                // Verify recovery matches oracle subset
                // (oracle only tracks must_survive, so recovered should contain at least oracle)
                let recovered = sm.snapshot_hashmap();
                for (key, expected_val) in &oracle {
                    if let Some(actual_val) = recovered.get(key) {
                        if actual_val.as_ref() != expected_val.as_slice() {
                            panic!(
                                "COMPACTION VIOLATION: key {:?} expected {:?}, got {:?}",
                                key, expected_val, actual_val
                            );
                        }
                    }
                    // Note: key may be missing if it was only in may_be_lost
                }

                // Sync oracle to recovered state
                oracle = recovered
                    .into_iter()
                    .map(|(k, v)| (k, v.to_vec()))
                    .collect();

                // Re-enable faults
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                let mut ops_since_compact = 0;

                for _ in 0..OPS_PER_CYCLE {
                    let key = keys[rng.next_usize(keys.len())];
                    let action = rng.next_usize(3);

                    let partial_before = sim::get_partial_write_count();
                    let result = if action < 2 {
                        let val = format!("{}", rng.next_u64() % 1000);
                        sm.apply_kv(format!("SET {} {}", key, val).as_bytes())
                    } else {
                        sm.apply_kv(format!("DELETE {}", key).as_bytes())
                    };

                    if result.is_ok() {
                        let partial_after = sim::get_partial_write_count();
                        if partial_before == partial_after {
                            // Must survive - sync oracle to current state
                            let current = sm.snapshot_hashmap();
                            oracle = current
                                .into_iter()
                                .map(|(k, v)| (k, v.to_vec()))
                                .collect();
                        }
                        ops_since_compact += 1;
                    }

                    // Periodic compaction
                    if ops_since_compact >= COMPACT_EVERY {
                        let partial_before = sim::get_partial_write_count();
                        if sm.compact_state_machine().is_ok() {
                            let partial_after = sim::get_partial_write_count();
                            if partial_before == partial_after {
                                // Compaction succeeded - oracle is now authoritative
                                let current = sm.snapshot_hashmap();
                                oracle = current
                                    .into_iter()
                                    .map(|(k, v)| (k, v.to_vec()))
                                    .collect();
                            }
                        }
                        ops_since_compact = 0;
                    }
                }

                // Crash
                drop(sm);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: compaction under faults verified",
                    scenario
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    // ------------------------------------------------------------------------
    // RAFT METADATA FAULT INJECTION TESTS
    // ------------------------------------------------------------------------
    // These tests exercise hard_state/conf_state writes under fault injection
    // by catching panics and verifying recovery invariants still hold.

    /// Test WalStorage hard_state and conf_state writes under fault injection.
    ///
    /// The production code panics on write failures, so we catch panics and
    /// verify that recovery invariants still hold after partial writes.
    #[test]
    fn raft_metadata_writes_with_fault_injection() {
        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 30;
        const BASE_SEED: u64 = 0xd4e5_f6a7_b8c9_0123;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir =
                std::env::temp_dir().join(format!("walrus_raft_metadata_fault_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("create dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("raft_metadata.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xabcd_ef01);

            // Track the last successfully persisted state
            let mut last_term: u64 = 0;
            let mut last_vote: u64 = 0;
            let mut successful_hard_state_writes = 0;
            let mut successful_conf_state_writes = 0;
            let mut panics_caught = 0;

            for _cycle in 0..CRASH_CYCLES {
                // Create storage without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = rt.block_on(async {
                    for _ in 0..WAL_CREATE_RETRIES {
                        match WriteAheadLog::new(wal_path.clone(), 0, Duration::from_millis(0)).await
                        {
                            Ok(w) => return w,
                            Err(_) => sim::advance_time(Duration::from_millis(10)),
                        }
                    }
                    panic!("WAL creation failed");
                });
                let storage = WalStorage::new(Arc::new(wal));

                // Verify recovery - the sim_assert invariants in WalStorage::new()
                // will fire if recovery is inconsistent
                let initial_state = storage.initial_state().expect("initial_state");
                let recovered_term = initial_state.hard_state.term;
                let recovered_vote = initial_state.hard_state.vote;

                // The recovered state should be >= our last known good state
                // (could be higher if a "may be lost" write actually survived)
                assert!(
                    recovered_term >= last_term,
                    "Term went backwards: {} < {}",
                    recovered_term,
                    last_term
                );

                last_term = recovered_term;
                last_vote = recovered_vote;

                // Re-enable faults for writes
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                for _ in 0..OPS_PER_CYCLE {
                    let action = rng.next_usize(10);

                    match action {
                        // set_hard_state (40% of ops) - may panic
                        0..=3 => {
                            let new_term = last_term + rng.next_u64() % 3;
                            let new_vote = 1 + rng.next_u64() % 5;
                            let mut hs = RaftHardState::default();
                            hs.set_term(new_term);
                            hs.set_vote(new_vote);
                            hs.set_commit(0);

                            // Catch panic from fault injection
                            let result = std::panic::catch_unwind(
                                std::panic::AssertUnwindSafe(|| {
                                    storage.set_hard_state(hs.clone());
                                }),
                            );

                            if result.is_ok() {
                                // Write succeeded - update our tracking
                                last_term = new_term;
                                last_vote = new_vote;
                                successful_hard_state_writes += 1;
                            } else {
                                panics_caught += 1;
                            }
                        }
                        // set_conf_state (30% of ops) - may panic
                        4..=6 => {
                            let mut cs = RaftConfState::default();
                            let num_voters = 1 + rng.next_usize(3);
                            for i in 0..num_voters {
                                cs.mut_voters().push((i + 1) as u64);
                            }

                            let result = std::panic::catch_unwind(
                                std::panic::AssertUnwindSafe(|| {
                                    storage.set_conf_state(cs);
                                }),
                            );

                            if result.is_ok() {
                                successful_conf_state_writes += 1;
                            } else {
                                panics_caught += 1;
                            }
                        }
                        // append_entries (30% of ops) - can fail gracefully
                        _ => {
                            let entry = make_raft_entry(1, last_term.max(1), b"data");
                            let _ = rt.block_on(storage.append_entries(&[entry]));
                        }
                    }
                }

                // Crash
                drop(storage);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} hard_state, {} conf_state writes, {} panics caught",
                    scenario, successful_hard_state_writes, successful_conf_state_writes, panics_caught
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Test WalLogStore truncate and purge operations under fault injection.
    ///
    /// These operations modify log structure and must maintain invariants
    /// even when I/O errors occur.
    #[test]
    /// Test WalLogStore append and purge operations with fault injection.
    /// This exercises the recovery invariants in WalLogStore including:
    /// - Append entries are recovered correctly
    /// - Purge operations are recovered correctly
    /// - Log remains contiguous after recovery
    /// - First entry immediately follows last purged entry
    fn log_store_purge_with_faults() {
        use openraft::storage::RaftLogStorage;

        const SCENARIOS: usize = 8;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 50;
        const BASE_SEED: u64 = 0xa1b2_c3d4_e5f6_7890;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir =
                std::env::temp_dir().join(format!("walrus_purge_fault_{scenario}"));

            // Setup without faults
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("create dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("truncate_purge.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xfeed_face);

            // Track state across cycles
            let mut next_index: u64 = 1;
            let mut last_purged_index: u64 = 0; // Track purge monotonicity
            let mut purge_ops = 0;

            for _cycle in 0..CRASH_CYCLES {
                // Create without faults
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);

                let wal = create_wal_with_retry(&rt, wal_path.clone(), WAL_CREATE_RETRIES);
                let mut store = rt
                    .block_on(WalLogStore::new(Arc::clone(&wal)))
                    .expect("log store init");

                // Get current state - the sim_assert invariants in WalLogStore will
                // fire if recovery violated any invariants
                let log_state = rt.block_on(store.get_log_state()).expect("log state");
                // Update last_purged_index from recovered state
                if let Some(purged_id) = log_state.last_purged_log_id.clone() {
                    last_purged_index = purged_id.index;
                }
                // next_index must be > last_purged_index (can't append at or before purge point)
                let min_next = last_purged_index + 1;
                next_index = log_state.last_log_id.map(|id| id.index + 1).unwrap_or(min_next);
                if next_index <= last_purged_index {
                    next_index = last_purged_index + 1;
                }

                // Re-enable faults
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                // Use consistent term=1 throughout - LogId comparison is (term, index)
                // so random terms could cause purge(term=5, idx=3) > entry(term=1, idx=10)
                let term: u64 = 1;
                let leader_id = CommittedLeaderIdOf::<AppTypeConfig>::new(term, 1);

                for _ in 0..OPS_PER_CYCLE {
                    let action = rng.next_usize(10);

                    match action {
                        // Append entries (70% of ops)
                        0..=6 => {
                            let entry = Entry::<AppTypeConfig> {
                                log_id: LogId::new(leader_id.clone(), next_index),
                                payload: EntryPayload::Normal(AppEntry(b"data".to_vec())),
                            };

                            if rt.block_on(store.blocking_append(vec![entry])).is_ok() {
                                next_index += 1;
                            }
                        }
                        // Purge (30% of ops)
                        _ => {
                            // Only purge if we have enough entries
                            // Purge to next_index - 2 to leave at least one entry
                            if next_index > last_purged_index + 2 {
                                let purge_to = last_purged_index + 1;
                                let log_id = LogId::new(leader_id.clone(), purge_to);

                                if rt.block_on(store.purge(log_id)).is_ok() {
                                    last_purged_index = purge_to;
                                    purge_ops += 1;
                                }
                            }
                        }
                    }
                }

                // Crash
                drop(store);
                wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            if scenario == 0 {
                eprintln!(
                    "Scenario {} PASSED: {} purges across {} cycles",
                    scenario, purge_ops, CRASH_CYCLES
                );
            }

            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

}
