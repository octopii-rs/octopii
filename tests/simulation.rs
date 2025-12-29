// tests/simulation.rs
//
// Phase 4+5: Deterministic Simulation Testing Harness
//
// This test uses the VFS infrastructure to run long-running, randomized,
// reproducible scenarios that verify WAL consistency through crashes
// and I/O failures.
//
// IMPORTANT: These tests MUST run serially (not in parallel) because:
// 1. They use process-wide environment variables (WALRUS_DATA_DIR)
// 2. The VFS simulation context is thread-local but tests may interfere

#[cfg(feature = "simulation")]
mod sim_tests {
    use octopii::wal::wal::vfs::sim::{self, SimConfig};
    use octopii::wal::wal::{FsyncSchedule, ReadConsistency, Walrus};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    // Global mutex to ensure tests run serially
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

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
            let len = self.range(10, 100);
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
            self.history
                .entry(topic.to_string())
                .or_default()
                .push(data);
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
                panic!(
                    "ORACLE FAILURE: Data mismatch for topic '{}' at index {}.\nExpected len: {}\nActual len: {}\nExpected: {:?}\nActual:   {:?}",
                    topic, *cursor, expected.len(), actual_data.len(), expected, actual_data
                );
            }

            *cursor += 1;
        }

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
    struct Simulation {
        rng: SimRng,
        oracle: Oracle,
        wal: Option<Walrus>, // Option so we can drop it to simulate crash
        topics: Vec<String>,
        root_dir: PathBuf,
        current_key: String,
        // Phase 5: Track the target error rate to restore after recovery
        target_error_rate: f64,
    }

    impl Simulation {
        fn new(seed: u64, error_rate: f64) -> Self {
            let root_dir = std::env::temp_dir().join(format!("walrus_sim_{}", seed));
            // Ensure clean start (using standard fs to clean up BEFORE sim starts)
            let _ = std::fs::remove_dir_all(&root_dir);
            let _ = std::fs::create_dir_all(&root_dir);

            // Set env var for Walrus to find the dir
            std::env::set_var("WALRUS_DATA_DIR", root_dir.to_str().unwrap());

            Self {
                rng: SimRng::new(seed),
                oracle: Oracle::new(),
                wal: None,
                topics: vec!["orders".into(), "logs".into(), "metrics".into()],
                root_dir,
                current_key: "sim_node".into(),
                target_error_rate: error_rate,
            }
        }

        fn init_wal(&mut self) {
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

            // Phase 5: Restore faults after successful initialization
            sim::set_io_error_rate(self.target_error_rate);
        }

        fn crash_and_recover(&mut self) {
            // 1. Drop the WAL (simulates process death)
            self.wal = None;

            // 2. Advance time (simulates downtime)
            sim::advance_time(std::time::Duration::from_secs(5));

            // 3. Clear the WalIndex files to test DATA durability (not cursor persistence)
            let key_dir = self.root_dir.join(&self.current_key);
            if let Ok(entries) = std::fs::read_dir(&key_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with("_index.db") {
                            let _ = std::fs::remove_file(&path);
                        }
                    }
                }
            }

            // 4. Reset Oracle cursors to 0 - we'll re-read everything from start
            self.oracle.reset_read_cursors();

            // 5. Re-initialize (simulates recovery/startup scan)
            self.init_wal();
        }

        fn run(&mut self, iterations: usize) {
            self.init_wal();

            for _i in 0..iterations {
                let topic = self.topics[self.rng.range(0, self.topics.len())].clone();
                let action_roll = self.rng.range(0, 100);

                // Action probabilities
                // 0-40: Append
                // 41-50: Batch Append
                // 51-90: Read
                // 91-95: Tick Background
                // 96-99: Crash/Restart

                if action_roll <= 40 {
                    // === APPEND ===
                    let data = self.rng.gen_payload();
                    match self.wal.as_ref().unwrap().append_for_topic(&topic, &data) {
                        Ok(_) => {
                            // Success: Oracle records the write
                            self.oracle.record_write(&topic, data);
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
                        .batch_append_for_topic(&topic, &batch_refs)
                    {
                        Ok(_) => {
                            // Success: Oracle records all entries in the batch
                            for d in batch_data {
                                self.oracle.record_write(&topic, d);
                            }
                        }
                        Err(_e) => {
                            // Phase 5: Batch failed - ATOMICITY CHECK
                            // If batch fails, NO data should be in the Oracle.
                            // Walrus MUST have rolled back internally.
                            // Future reads will verify nothing was partially written.
                        }
                    }
                } else if action_roll <= 90 {
                    // === READ ===
                    match self.wal.as_ref().unwrap().read_next(&topic, true) {
                        Ok(Some(entry)) => {
                            self.oracle.verify_read(&topic, &entry.data);
                        }
                        Ok(None) => {
                            self.oracle.check_eof(&topic);
                        }
                        Err(_e) => {
                            // Phase 5: Read failed due to I/O error
                            // Oracle cursor stays put - next iteration will retry
                            // This is correct: transient read failures don't advance cursor
                        }
                    }
                } else if action_roll <= 95 {
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
            let _ = std::fs::remove_dir_all(&self.root_dir);
        }
    }

    // ========================================================================
    // 4. Test Runners
    // ========================================================================

    fn run_simulation_with_config(seed: u64, iterations: usize, error_rate: f64, partial_writes: bool) {
        // Acquire global lock to ensure serial execution
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

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
    #[ignore] // Run with: cargo test --features simulation --test simulation stress_ -- --ignored
    fn stress_no_faults_seed_1() {
        run_simulation_with_config(1, 20000, 0.0, false);
    }

    #[test]
    #[ignore]
    fn stress_no_faults_seed_2() {
        run_simulation_with_config(2, 20000, 0.0, false);
    }

    #[test]
    #[ignore]
    fn stress_no_faults_seed_3() {
        run_simulation_with_config(3, 20000, 0.0, false);
    }

    #[test]
    #[ignore]
    fn stress_fault_injection_seed_1000() {
        run_simulation_with_config(1000, 15000, 0.05, false);
    }

    #[test]
    #[ignore]
    fn stress_fault_injection_seed_2000() {
        run_simulation_with_config(2000, 15000, 0.05, false);
    }

    #[test]
    #[ignore]
    fn stress_fault_injection_seed_3000() {
        run_simulation_with_config(3000, 15000, 0.07, false);
    }

    #[test]
    #[ignore]
    fn stress_partial_writes_seed_4000() {
        run_simulation_with_config(4000, 10000, 0.05, true);
    }

    #[test]
    #[ignore]
    fn stress_partial_writes_seed_5000() {
        run_simulation_with_config(5000, 10000, 0.05, true);
    }

    #[test]
    #[ignore]
    fn stress_partial_writes_seed_6000() {
        run_simulation_with_config(6000, 10000, 0.08, true);
    }

    #[test]
    #[ignore]
    fn stress_extreme_seed_7777() {
        // High error rate, many iterations - finds edge cases
        run_simulation_with_config(7777, 8000, 0.15, true);
    }
}
