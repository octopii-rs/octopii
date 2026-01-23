use crate::chunk::ChunkSource;
use crate::error::{OctopiiError, Result};
use quinn::Connection;
use std::path::Path;

use crate::transport::Peer;

mod receive;
mod send;

pub struct PeerConnection {
    pub(crate) connection: Connection,
}

impl PeerConnection {
    pub(crate) fn new(connection: Connection) -> Self {
        Self { connection }
    }

    /// Send data to the peer (length-prefixed with ack)
    pub async fn send(&self, data: bytes::Bytes) -> Result<()> {
        send::send_message(&self.connection, data).await
    }

    /// Receive data from the peer (length-prefixed with ack)
    pub async fn recv(&self) -> Result<Option<bytes::Bytes>> {
        receive::recv_message(&self.connection).await
    }

    /// Check if the connection is closed
    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    /// Get connection statistics
    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    /// Send a chunk with checksum verification
    ///
    /// Returns the number of bytes transferred
    pub async fn send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64> {
        send::send_chunk_verified(&self.connection, chunk).await
    }

    /// Receive a chunk with checksum verification
    pub async fn recv_chunk_verified(&self) -> Result<Option<bytes::Bytes>> {
        receive::recv_chunk_verified(&self.connection).await
    }

    /// Receive a chunk into a file (checksum verified)
    pub async fn recv_chunk_verified_to_file(&self, path: &Path) -> Result<Option<u64>> {
        receive::recv_chunk_verified_to_file(&self.connection, path).await
    }

    /// Backwards-compatible name used by shipping_lane and docs.
    pub async fn recv_chunk_to_path<P: AsRef<Path>>(&self, path: P) -> Result<Option<u64>> {
        self.recv_chunk_verified_to_file(path.as_ref()).await
    }
}

impl Peer for PeerConnection {
    fn send(&self, data: bytes::Bytes) -> super::TransportFut<'_, ()> {
        Box::pin(async move { self.send(data).await })
    }

    fn recv(&self) -> super::TransportFut<'_, Option<bytes::Bytes>> {
        Box::pin(async move { self.recv().await })
    }

    fn is_closed(&self) -> bool {
        self.is_closed()
    }
}
