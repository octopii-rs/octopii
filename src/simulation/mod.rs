use crate::wal::wal::vfs;
use crate::wal::wal::vfs::sim::{self, SimConfig};
use crate::wal::wal::{FsyncSchedule, ReadConsistency, Walrus};
use std::path::PathBuf;

mod oracle;

pub use oracle::{DurabilityOracle, Oracle};

#[derive(Clone)]
pub struct SimRng {
    state: u64,
}

const GOLDEN_RATIO: u64 = 0x9E3779B97F4A7C15;

impl SimRng {
    pub fn new(seed: u64) -> Self {
        let mut rng = Self { state: 0 };
        rng.state = seed.wrapping_add(GOLDEN_RATIO);
        rng.next_u64();
        rng
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn next_range(&mut self, upper_exclusive: u64) -> u64 {
        if upper_exclusive <= 1 {
            return 0;
        }
        self.next_u64() % upper_exclusive
    }

    pub fn range(&mut self, min: usize, max: usize) -> usize {
        let range = max - min;
        if range == 0 {
            return min;
        }
        min + (self.next_u64() as usize % range)
    }

    pub fn gen_payload(&mut self) -> Vec<u8> {
        let len = self.range(1, 21);
        let mut buf = Vec::with_capacity(len);
        for _ in 0..len {
            buf.push(self.next_u64() as u8);
        }
        buf
    }
}

struct Simulation {
    rng: SimRng,
    oracle: Oracle,
    wal: Option<Walrus>,
    topics: Vec<String>,
    root_dir: PathBuf,
    current_key: String,
    target_error_rate: f64,
    seed: u64,
    progress_every: Option<usize>,
}

impl Simulation {
    const WRITE_THRESHOLD: usize = 40; // 0-40: single write (41%)
    const BATCH_WRITE_THRESHOLD: usize = 55; // 41-55: batch write (15%)
    const READ_THRESHOLD: usize = 70; // 56-70: single read (15%)
    const BATCH_READ_THRESHOLD: usize = 85; // 71-85: batch read (15%)
    const TICK_THRESHOLD: usize = 93; // 86-93: tick background (8%)

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

    fn crash_and_recover(&mut self) {
        if Oracle::should_log() {
            let orders_count = self.oracle.history_len("orders");
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
                    eprintln!("[seed {}] progress {}/{}", self.seed, step, iterations);
                }
            }
            let action_roll = self.rng.range(0, 100);

            if action_roll <= Self::WRITE_THRESHOLD {
                let topic_idx = self.rng.range(0, self.topics.len());
                let topic = &self.topics[topic_idx];
                let payload = self.rng.gen_payload();

                let wal = self.wal.as_ref().unwrap();
                if wal.append_for_topic(topic, &payload).is_ok() {
                    self.oracle.record_write(topic, payload);
                }
            } else if action_roll <= Self::BATCH_WRITE_THRESHOLD {
                let topic_idx = self.rng.range(0, self.topics.len());
                let topic = &self.topics[topic_idx];
                let batch_size = self.rng.range(1, 10);

                let mut payloads = Vec::with_capacity(batch_size);
                for _ in 0..batch_size {
                    payloads.push(self.rng.gen_payload());
                }

                let payload_refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
                let wal = self.wal.as_ref().unwrap();
                if wal.batch_append_for_topic(topic, &payload_refs).is_ok() {
                    for payload in payloads {
                        self.oracle.record_write(topic, payload);
                    }
                }
            } else if action_roll <= Self::READ_THRESHOLD {
                let topic_idx = self.rng.range(0, self.topics.len());
                let topic = &self.topics[topic_idx];

                let wal = self.wal.as_ref().unwrap();
                if let Ok(Some(entry)) = wal.read_next(topic, true) {
                    self.oracle.verify_read(topic, &entry.data);
                }
            } else if action_roll <= Self::BATCH_READ_THRESHOLD {
                let topic_idx = self.rng.range(0, self.topics.len());
                let topic = &self.topics[topic_idx];
                let max_bytes = self.rng.range(50, 2000);

                let wal = self.wal.as_ref().unwrap();
                if let Ok(entries) = wal.batch_read_for_topic(topic, max_bytes, true) {
                    if !entries.is_empty() {
                        let payloads: Vec<Vec<u8>> = entries.into_iter().map(|e| e.data).collect();
                        self.oracle.verify_batch_read(topic, &payloads);
                    }
                }
            } else if action_roll <= Self::TICK_THRESHOLD {
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
        initial_time_ns: 1_700_000_000_000_000_000,
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
