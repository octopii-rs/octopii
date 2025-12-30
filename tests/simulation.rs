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
    use octopii::wal::wal::vfs::sim::{self, SimConfig};
    use octopii::wal::wal::vfs;
    use octopii::wal::wal::{FsyncSchedule, ReadConsistency, Walrus};
    use std::collections::HashMap;
    use std::path::PathBuf;

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

                            match self
                                .wal
                                .as_ref()
                                .unwrap()
                                .append_for_topic(&dual.recovery, &data)
                            {
                                Ok(_) => {
                                    // Both writes succeeded
                                    self.oracle.record_write(&dual.recovery, data);
                                }
                                Err(_e) => {
                                    // Recovery write failed - primary still valid
                                    // This is okay: recovery is best-effort in some patterns
                                }
                            }
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

}
