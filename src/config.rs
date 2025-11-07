use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::net::SocketAddr;

/// Configuration for an Octopii node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Node ID
    pub node_id: u64,

    /// Address to bind to for QUIC connections
    pub bind_addr: SocketAddr,

    /// List of peer addresses
    pub peers: Vec<SocketAddr>,

    /// Path to WAL directory
    pub wal_dir: PathBuf,

    /// Number of worker threads for the isolated runtime
    pub worker_threads: usize,

    /// WAL batch size (number of entries before forcing a write)
    pub wal_batch_size: usize,

    /// WAL flush interval in milliseconds
    pub wal_flush_interval_ms: u64,
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
        }
    }
}
