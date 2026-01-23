use bytes::Bytes;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum ChunkSource {
    File(PathBuf),
    Memory(Bytes),
}

#[derive(Debug, Clone)]
pub struct TransferResult {
    pub peer: SocketAddr,
    pub success: bool,
    pub bytes_transferred: u64,
    pub checksum_verified: bool,
    pub duration: Duration,
    pub error: Option<String>,
}

impl TransferResult {
    pub fn success(peer: SocketAddr, bytes: u64, duration: Duration) -> Self {
        Self {
            peer,
            success: true,
            bytes_transferred: bytes,
            checksum_verified: true,
            duration,
            error: None,
        }
    }

    pub fn failure(peer: SocketAddr, error: String) -> Self {
        Self {
            peer,
            success: false,
            bytes_transferred: 0,
            checksum_verified: false,
            duration: Duration::ZERO,
            error: Some(error),
        }
    }
}
