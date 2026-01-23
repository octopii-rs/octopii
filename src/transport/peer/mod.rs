use crate::chunk::ChunkSource;
use crate::error::Result;
use bytes::Bytes;
use quinn::Connection;
use std::path::Path;

use crate::transport::Peer;

mod receive;
mod send;

/// Shared buffer size for chunk transfer operations (64KB)
const BUFFER_SIZE: usize = 64 * 1024;

/// Maximum in-memory allocation for receiving chunks (10MB)
const MEMORY_RECEIVE_CAP: usize = 10 * 1024 * 1024;

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
    fn send(&self, data: Bytes) -> super::TransportFut<'_, ()> {
        Box::pin(async move { PeerConnection::send(self, data).await })
    }

    fn recv(&self) -> super::TransportFut<'_, Option<Bytes>> {
        Box::pin(async move { PeerConnection::recv(self).await })
    }

    fn is_closed(&self) -> bool {
        PeerConnection::is_closed(self)
    }

    fn send_chunk_verified(&self, chunk: &ChunkSource) -> super::TransportFut<'_, u64> {
        let chunk = chunk.clone();
        Box::pin(async move { PeerConnection::send_chunk_verified(self, &chunk).await })
    }

    fn recv_chunk_verified(&self) -> super::TransportFut<'_, Option<Bytes>> {
        Box::pin(async move { PeerConnection::recv_chunk_verified(self).await })
    }

    fn recv_chunk_verified_to_file(&self, path: &Path) -> super::TransportFut<'_, Option<u64>> {
        let path = path.to_path_buf();
        Box::pin(async move { PeerConnection::recv_chunk_verified_to_file(self, &path).await })
    }
}
