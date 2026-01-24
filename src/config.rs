use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub node_id: u64,
    pub bind_addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub wal_dir: PathBuf,
    pub worker_threads: usize,
    pub wal_batch_size: usize,
    pub wal_flush_interval_ms: u64,
    pub is_initial_leader: bool,
    pub snapshot_lag_threshold: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,
            bind_addr: "127.0.0.1:5000".parse().unwrap(),
            peers: Vec::new(),
            wal_dir: PathBuf::from("./data"),
            worker_threads: 4,
            wal_batch_size: 100,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 500,
        }
    }
}
