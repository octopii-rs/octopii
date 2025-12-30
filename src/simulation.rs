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
        let len = self.range(10, 100);
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
