use crate::chunk::ChunkSource;
use crate::error::Result;
use bytes::Bytes;
use quinn::Connection;
use std::path::Path;

use crate::transport::Peer;

mod receive;
mod send;

const BUFFER_SIZE: usize = 64 * 1024;

const MEMORY_RECEIVE_CAP: usize = 10 * 1024 * 1024;

pub struct PeerConnection {
    pub(crate) connection: Connection,
}

impl PeerConnection {
    pub(crate) fn new(connection: Connection) -> Self {
        Self { connection }
    }

    pub async fn send(&self, data: bytes::Bytes) -> Result<()> {
        send::send_message(&self.connection, data).await
    }

    pub async fn recv(&self) -> Result<Option<bytes::Bytes>> {
        receive::recv_message(&self.connection).await
    }

    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    pub async fn send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64> {
        send::send_chunk_verified(&self.connection, chunk).await
    }

    pub async fn recv_chunk_verified(&self) -> Result<Option<bytes::Bytes>> {
        receive::recv_chunk_verified(&self.connection).await
    }

    pub async fn recv_chunk_verified_to_file(&self, path: &Path) -> Result<Option<u64>> {
        receive::recv_chunk_verified_to_file(&self.connection, path).await
    }

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
