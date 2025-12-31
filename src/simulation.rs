use crate::wal::wal::vfs::sim::{self, SimConfig};
use crate::wal::wal::vfs;
use crate::wal::wal::{FsyncSchedule, ReadConsistency, Walrus};
use std::collections::HashMap;
use std::path::PathBuf;

// Deterministic Simulation Harness (for long-running fuzz-style runs)

pub struct SimRng {
    state: u64,
}

impl SimRng {
    pub fn new(seed: u64) -> Self {
        let mut rng = Self { state: 0 };
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

struct Oracle {
    history: HashMap<String, Vec<Vec<u8>>>,
    read_cursors: HashMap<String, usize>,
}

impl Oracle {
    fn should_log() -> bool {
        matches!(std::env::var("SIM_VERBOSE").as_deref(), Ok("1"))
    }

    fn new() -> Self {
        Self {
            history: HashMap::new(),
            read_cursors: HashMap::new(),
        }
    }

    fn record_write(&mut self, topic: &str, data: Vec<u8>) {
        let history = self.history.entry(topic.to_string()).or_default();
        let idx = history.len();
        if topic == "orders" && Self::should_log() {
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
            let mut found_at = None;
            for (i, entry) in history.iter().enumerate() {
                if entry == actual_data {
                    found_at = Some(i);
                    break;
                }
            }

            eprintln!("ORACLE MISMATCH for topic '{}'", topic);
            eprintln!("Expected (cursor={}): {:?}", cursor, expected);
            eprintln!("Actual: {:?}", actual_data);
            eprintln!("Oracle history length: {}", history.len());
            if let Some(idx) = found_at {
                eprintln!("FOUND: Actual data matches Oracle history[{}]", idx);
            } else {
                eprintln!("NOT FOUND: Actual data doesn't match any Oracle history entry");
            }
            eprintln!("Nearby entries in Oracle:");
            let start = cursor.saturating_sub(2);
            let end = (*cursor + 3).min(history.len());
            for (i, entry) in history.iter().enumerate().take(end).skip(start) {
                eprintln!("  [{}] {:?}", i, entry);
            }
            panic!("Oracle validation failed for topic '{}'", topic);
        }

        *cursor += 1;
    }

    fn verify_batch_read(&mut self, topic: &str, actual_entries: &[Vec<u8>]) {
        if actual_entries.is_empty() {
            return;
        }

        let history = self.history.entry(topic.to_string()).or_default();
        let cursor = self.read_cursors.entry(topic.to_string()).or_default();

        if *cursor >= history.len() {
            panic!(
                "ORACLE FAILURE: Walrus returned entries for topic '{}' but Oracle thinks stream is empty.",
                topic
            );
        }

        for entry in actual_entries {
            let expected = &history[*cursor];
            if expected != entry {
                panic!(
                    "ORACLE FAILURE: batch read mismatch for topic '{}' at cursor {}.\nExpected: {:?}\nGot: {:?}",
                    topic,
                    cursor,
                    expected,
                    entry
                );
            }
            *cursor += 1;
        }
    }

    fn reset_read_cursors(&mut self) {
        for cursor in self.read_cursors.values_mut() {
            *cursor = 0;
        }
    }
}

/// Tracked entry with cycle information for debugging
#[derive(Debug, Clone)]
struct TrackedEntry {
    cycle: usize,
    data: Vec<u8>,
}

/// Durability oracle that tracks entries across ALL crash cycles.
///
/// Key insight: if an append returns Ok AND no partial write occurred,
/// that entry is "must_survive" and MUST exist after ANY number of crashes.
/// If a partial write occurred, the entry "may_be_lost" until we confirm
/// it survived a crash (then it becomes must_survive).
///
/// Unlike the simple sync-from-recovery pattern, this oracle:
/// 1. Tracks must_survive entries FOREVER (not reset each cycle)
/// 2. Promotes may_be_lost entries that survive to must_survive
/// 3. Verifies ALL must_survive entries exist after EVERY recovery
#[derive(Debug, Clone)]
pub struct DurabilityOracle {
    /// Entries that MUST survive - accumulated across ALL cycles.
    /// Once an entry is here, it must exist after every future crash.
    must_survive: HashMap<String, Vec<TrackedEntry>>,

    /// Entries that MAY be lost - from current cycle only.
    /// After crash, entries that survived move to must_survive.
    may_be_lost: HashMap<String, Vec<TrackedEntry>>,

    /// Current crash cycle number
    current_cycle: usize,

    /// How many entries we've verified per topic (for resuming verification)
    verified_count: HashMap<String, usize>,
}

impl DurabilityOracle {
    pub fn new() -> Self {
        Self {
            must_survive: HashMap::new(),
            may_be_lost: HashMap::new(),
            current_cycle: 0,
            verified_count: HashMap::new(),
        }
    }

    /// Record a write operation with its durability status.
    /// Call this after every successful append.
    pub fn record_write(
        &mut self,
        topic: &str,
        data: Vec<u8>,
        partial_before: u64,
        partial_after: u64,
    ) {
        let entry = TrackedEntry {
            cycle: self.current_cycle,
            data,
        };

        if partial_after == partial_before {
            // No partial write - this entry MUST survive all future crashes
            self.must_survive
                .entry(topic.into())
                .or_default()
                .push(entry);
        } else {
            // Partial write happened - might be lost on this crash
            self.may_be_lost
                .entry(topic.into())
                .or_default()
                .push(entry);
        }
    }

    /// Verify durability guarantees after recovery.
    /// ALL must_survive entries from ALL previous cycles MUST exist in order.
    /// Returns Err with details on durability violation.
    ///
    /// Note: This advances the WAL read cursor. Call after_recovery() next to
    /// check may_be_lost entries (which continues from where verification left off).
    pub fn verify_after_recovery(&self, wal: &Walrus) -> Result<(), String> {
        for (topic, entries) in &self.must_survive {
            // Read entries with checkpoint=true to advance cursor
            for (idx, tracked) in entries.iter().enumerate() {
                match wal.read_next(topic, true) {
                    Ok(Some(entry)) => {
                        if entry.data != tracked.data {
                            return Err(format!(
                                "DURABILITY VIOLATION: topic '{}' entry {} (from cycle {}) data mismatch.\n\
                                 Expected {} bytes: {:?}\n\
                                 Got {} bytes: {:?}",
                                topic,
                                idx,
                                tracked.cycle,
                                tracked.data.len(),
                                &tracked.data[..tracked.data.len().min(50)],
                                entry.data.len(),
                                &entry.data[..entry.data.len().min(50)]
                            ));
                        }
                    }
                    Ok(None) => {
                        return Err(format!(
                            "DURABILITY VIOLATION: topic '{}' entry {} (from cycle {}) MISSING.\n\
                             Expected {} bytes, got EOF.\n\
                             Total must_survive entries: {}, verified so far: {}",
                            topic,
                            idx,
                            tracked.cycle,
                            tracked.data.len(),
                            entries.len(),
                            idx
                        ));
                    }
                    Err(e) => {
                        return Err(format!(
                            "DURABILITY VIOLATION: topic '{}' entry {} (from cycle {}) read error.\n\
                             Expected {} bytes, got error: {:?}",
                            topic, idx, tracked.cycle, tracked.data.len(), e
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Call after successful recovery verification.
    /// Promotes may_be_lost entries that survived to must_survive.
    /// Increments cycle counter.
    ///
    /// Note: This continues reading from where verify_after_recovery left off.
    pub fn after_recovery(&mut self, wal: &Walrus) {
        // For each topic with may_be_lost entries, check which survived
        // verify_after_recovery already read past must_survive entries,
        // so we continue from there
        let may_be_lost = std::mem::take(&mut self.may_be_lost);

        for (topic, entries) in may_be_lost {
            let must_survive = self.must_survive.entry(topic.clone()).or_default();

            // Check which may_be_lost entries survived (continuing from where verify left off)
            for tracked in entries {
                match wal.read_next(&topic, true) {
                    Ok(Some(entry)) if entry.data == tracked.data => {
                        // This entry survived! Promote to must_survive
                        must_survive.push(tracked);
                    }
                    _ => {
                        // Entry was lost or mismatched - that's allowed for may_be_lost
                        // Stop checking further entries for this topic
                        break;
                    }
                }
            }
        }

        self.current_cycle += 1;
        self.verified_count.clear();
    }

    /// Get counts for logging/debugging
    pub fn stats(&self) -> (usize, usize) {
        let must_count: usize = self.must_survive.values().map(|v| v.len()).sum();
        let may_count: usize = self.may_be_lost.values().map(|v| v.len()).sum();
        (must_count, may_count)
    }

    /// Get current cycle number
    pub fn cycle(&self) -> usize {
        self.current_cycle
    }

    /// Clear all state (for new test scenario)
    pub fn clear(&mut self) {
        self.must_survive.clear();
        self.may_be_lost.clear();
        self.current_cycle = 0;
        self.verified_count.clear();
    }
}

struct DualTopic {
    primary: String,
    recovery: String,
}

struct Simulation {
    rng: SimRng,
    oracle: Oracle,
    wal: Option<Walrus>,
    topics: Vec<String>,
    dual_topics: Vec<DualTopic>,
    root_dir: PathBuf,
    current_key: String,
    target_error_rate: f64,
    seed: u64,
    progress_every: Option<usize>,
}

impl Simulation {
    fn new(seed: u64, error_rate: f64, progress_every: Option<usize>) -> Self {
        let root_dir = std::env::temp_dir().join(format!("walrus_sim_{}", seed));
        let _ = vfs::remove_dir_all(&root_dir);
        let _ = vfs::create_dir_all(&root_dir);

        crate::wal::wal::__set_thread_wal_data_dir_for_tests(root_dir.clone());

        Self {
            rng: SimRng::new(seed),
            oracle: Oracle::new(),
            wal: None,
            topics: vec!["orders".into(), "logs".into(), "metrics".into()],
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
            seed,
            progress_every,
        }
    }

    fn init_wal(&mut self) {
        if self.wal.is_some() {
            return;
        }

        sim::set_io_error_rate(0.0);

        let w = Walrus::with_consistency_and_schedule_for_key(
            &self.current_key,
            ReadConsistency::StrictlyAtOnce,
            FsyncSchedule::SyncEach,
        )
        .expect("Failed to initialize Walrus during recovery");

        self.wal = Some(w);

        sim::set_io_error_rate(self.target_error_rate);
    }

    fn all_readable_topics(&self) -> Vec<String> {
        let mut all = self.topics.clone();
        for dual in &self.dual_topics {
            all.push(dual.primary.clone());
            all.push(dual.recovery.clone());
        }
        all
    }

    fn crash_and_recover(&mut self) {
        if Oracle::should_log() {
            let orders_count = self.oracle.history.get("orders").map(|v| v.len()).unwrap_or(0);
            eprintln!(
                "[CRASH] Simulating crash. orders has {} entries in Oracle",
                orders_count
            );
        }

        self.wal = None;
        crate::wal::wal::__clear_storage_cache_for_tests();
        sim::advance_time(std::time::Duration::from_secs(5));

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

        self.oracle.reset_read_cursors();
        self.init_wal();
    }

    fn run(&mut self, iterations: usize) {
        self.init_wal();

        for step in 0..iterations {
            if let Some(every) = self.progress_every {
                if step > 0 && step % every == 0 {
                    eprintln!(
                        "[seed {}] progress {}/{}",
                        self.seed, step, iterations
                    );
                }
            }
            let action_roll = self.rng.range(0, 100);

            if action_roll <= 40 {
                let topic_idx = self.rng.range(0, self.topics.len());
                let topic = &self.topics[topic_idx];
                let payload = self.rng.gen_payload();

                let wal = self.wal.as_ref().unwrap();
                let result = wal.append_for_topic(topic, &payload);

                match result {
                    Ok(_) => {
                        self.oracle.record_write(topic, payload);
                    }
                    Err(_) => {}
                }
            } else if action_roll <= 55 {
                let topic_idx = self.rng.range(0, self.topics.len());
                let topic = &self.topics[topic_idx];
                let batch_size = self.rng.range(1, 10);

                let mut payloads = Vec::with_capacity(batch_size);
                for _ in 0..batch_size {
                    payloads.push(self.rng.gen_payload());
                }

                let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
                let wal = self.wal.as_ref().unwrap();
                let result = wal.batch_append_for_topic(topic, &payload_refs);

                match result {
                    Ok(_) => {
                        for payload in payloads {
                            self.oracle.record_write(topic, payload);
                        }
                    }
                    Err(_) => {}
                }
            } else if action_roll <= 70 {
                let all_topics = self.all_readable_topics();
                let topic_idx = self.rng.range(0, all_topics.len());
                let topic = &all_topics[topic_idx];

                let wal = self.wal.as_ref().unwrap();
                match wal.read_next(topic, true) {
                    Ok(Some(entry)) => {
                        self.oracle.verify_read(topic, &entry.data);
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            } else if action_roll <= 85 {
                let all_topics = self.all_readable_topics();
                let topic_idx = self.rng.range(0, all_topics.len());
                let topic = &all_topics[topic_idx];
                let max_bytes = self.rng.range(50, 2000);

                let wal = self.wal.as_ref().unwrap();
                match wal.batch_read_for_topic(topic, max_bytes, true) {
                    Ok(entries) => {
                        if !entries.is_empty() {
                            let payloads: Vec<Vec<u8>> =
                                entries.into_iter().map(|e| e.data).collect();
                            self.oracle.verify_batch_read(topic, &payloads);
                        }
                    }
                    Err(_) => {}
                }
            } else if action_roll <= 93 {
                self.wal.as_ref().unwrap().tick_background();
                sim::advance_time(std::time::Duration::from_millis(100));
            } else {
                self.crash_and_recover();
            }
        }
    }
}

impl Drop for Simulation {
    fn drop(&mut self) {
        let _ = vfs::remove_dir_all(&self.root_dir);
        crate::wal::wal::__clear_thread_wal_data_dir_for_tests();
    }
}

pub fn run_simulation_with_config(
    seed: u64,
    iterations: usize,
    error_rate: f64,
    partial_writes: bool,
) {
    let progress_every = std::env::var("SIM_PROGRESS_EVERY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0);

    sim::setup(SimConfig {
        seed,
        io_error_rate: error_rate,
        initial_time_ns: 1700000000_000_000_000,
        enable_partial_writes: partial_writes,
    });

    let mut simulation = Simulation::new(seed, error_rate, progress_every);
    simulation.run(iterations);

    sim::teardown();

    println!(
        "Simulation completed: seed={}, iterations={}, error_rate={}, partial_writes={}",
        seed, iterations, error_rate, partial_writes
    );
}
