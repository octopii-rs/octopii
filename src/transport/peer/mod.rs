use crate::chunk::ChunkSource;
use crate::error::{OctopiiError, Result};
use bytes::{Bytes, BytesMut};
use quinn::{Connection, SendStream};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::transport::Peer;

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
        send_message(&self.connection, data).await
    }

    pub async fn recv(&self) -> Result<Option<bytes::Bytes>> {
        recv_message(&self.connection).await
    }

    pub fn is_closed(&self) -> bool {
        self.connection.close_reason().is_some()
    }

    pub fn stats(&self) -> quinn::ConnectionStats {
        self.connection.stats()
    }

    pub async fn send_chunk_verified(&self, chunk: ChunkSource) -> Result<u64> {
        send_chunk_verified(&self.connection, chunk).await
    }

    pub async fn recv_chunk_verified(&self) -> Result<Option<bytes::Bytes>> {
        recv_chunk_verified(&self.connection).await
    }

    pub async fn recv_chunk_verified_to_file(&self, path: &Path) -> Result<Option<u64>> {
        recv_chunk_verified_to_file(&self.connection, path).await
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

    fn send_chunk_verified(&self, chunk: ChunkSource) -> super::TransportFut<'_, u64> {
        Box::pin(async move { PeerConnection::send_chunk_verified(self, chunk).await })
    }

    fn recv_chunk_verified(&self) -> super::TransportFut<'_, Option<Bytes>> {
        Box::pin(async move { PeerConnection::recv_chunk_verified(self).await })
    }

    fn recv_chunk_verified_to_file(&self, path: &Path) -> super::TransportFut<'_, Option<u64>> {
        let path = path.to_path_buf();
        Box::pin(async move { PeerConnection::recv_chunk_verified_to_file(self, &path).await })
    }
}

pub async fn send_message(connection: &Connection, data: Bytes) -> Result<()> {
    let (mut send, mut recv) = connection.open_bi().await?;

    let len = data.len() as u32;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(&data).await?;
    send.finish()
        .map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

    let _ = recv
        .read_to_end(0)
        .await
        .map_err(|e| OctopiiError::Transport(format!("Read error: {}", e)))?;

    Ok(())
}

pub async fn send_chunk_verified(connection: &Connection, chunk: ChunkSource) -> Result<u64> {
    use tokio::io::AsyncReadExt;

    let (mut send_stream, mut recv_stream) = connection.open_bi().await?;

    let (size, final_checksum) = match chunk {
        ChunkSource::Memory(bytes) => {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let checksum = hasher.finalize().to_vec();
            let size = bytes.len() as u64;

            send_stream.write_all(&size.to_le_bytes()).await?;
            send_stream.write_all(&bytes).await?;

            (size, checksum)
        }
        ChunkSource::File(path) => {
            let metadata = tokio::fs::metadata(&path).await?;
            let size = metadata.len();

            send_stream.write_all(&size.to_le_bytes()).await?;
            let checksum = stream_file(&mut send_stream, &path).await?;

            (size, checksum)
        }
        ChunkSource::Stream { size, mut reader } => {
            send_stream.write_all(&size.to_le_bytes()).await?;

            let mut hasher = Sha256::new();
            let mut buffer = vec![0u8; BUFFER_SIZE];
            let mut remaining = size;

            while remaining > 0 {
                let to_read = std::cmp::min(BUFFER_SIZE as u64, remaining) as usize;
                let n = reader
                    .read(&mut buffer[..to_read])
                    .await
                    .map_err(|e| OctopiiError::Transport(format!("Stream read error: {}", e)))?;
                if n == 0 {
                    return Err(OctopiiError::Transport(format!(
                        "Stream ended early: expected {} more bytes",
                        remaining
                    )));
                }
                hasher.update(&buffer[..n]);
                send_stream.write_all(&buffer[..n]).await?;
                remaining -= n as u64;
            }

            let checksum = hasher.finalize().to_vec();
            (size, checksum)
        }
    };

    send_stream.write_all(&final_checksum).await?;
    send_stream
        .finish()
        .map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

    let mut ack_buf = [0u8; 1];
    recv_stream
        .read_exact(&mut ack_buf)
        .await
        .map_err(|e| OctopiiError::Transport(format!("Failed to read ACK: {}", e)))?;

    match ack_buf[0] {
        0 => Ok(size),
        1 => Err(OctopiiError::Transport(
            "Checksum verification failed on peer".to_string(),
        )),
        _ => Err(OctopiiError::Transport("Unknown error on peer".to_string())),
    }
}

async fn stream_file(send_stream: &mut SendStream, path: &std::path::Path) -> Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; BUFFER_SIZE];

    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }

        hasher.update(&buffer[..n]);
        send_stream.write_all(&buffer[..n]).await?;
    }

    Ok(hasher.finalize().to_vec())
}

pub enum RecvChunkResult {
    Memory(Bytes),
    File(u64),
}

enum RecvChunkMode {
    Memory,
    File(std::path::PathBuf),
}

enum ChunkSink {
    Memory(BytesMut),
    File(File),
}

impl ChunkSink {
    fn memory(total_size: u64) -> Self {
        let cap = std::cmp::min(total_size as usize, MEMORY_RECEIVE_CAP);
        ChunkSink::Memory(BytesMut::with_capacity(cap))
    }

    async fn file(path: std::path::PathBuf) -> Result<Self> {
        let file = File::create(path)
            .await
            .map_err(|e| OctopiiError::Transport(format!("Failed to create file: {}", e)))?;
        Ok(ChunkSink::File(file))
    }

    async fn write_chunk(&mut self, buf: &[u8]) -> Result<()> {
        match self {
            ChunkSink::Memory(data) => {
                data.extend_from_slice(buf);
                Ok(())
            }
            ChunkSink::File(file) => file
                .write_all(buf)
                .await
                .map_err(|e| OctopiiError::Transport(format!("File write failed: {}", e))),
        }
    }

    async fn finish(self, _received: u64) -> Result<RecvChunkResult> {
        match self {
            ChunkSink::Memory(data) => Ok(RecvChunkResult::Memory(data.freeze())),
            ChunkSink::File(mut file) => {
                file.flush()
                    .await
                    .map_err(|e| OctopiiError::Transport(format!("Flush failed: {}", e)))?;
                let metadata = file
                    .metadata()
                    .await
                    .map_err(|e| OctopiiError::Transport(format!("Metadata failed: {}", e)))?;
                Ok(RecvChunkResult::File(metadata.len()))
            }
        }
    }
}

pub async fn recv_message(connection: &Connection) -> Result<Option<Bytes>> {
    let (mut send, mut recv) = match connection.accept_bi().await {
        Ok(stream) => stream,
        Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(|e| OctopiiError::Transport(format!("Read error: {}", e)))?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut data = BytesMut::with_capacity(len);
    data.resize(len, 0);
    recv.read_exact(&mut data)
        .await
        .map_err(|e| OctopiiError::Transport(format!("Read error: {}", e)))?;

    send.finish()
        .map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

    Ok(Some(data.freeze()))
}

pub async fn recv_chunk_verified(connection: &Connection) -> Result<Option<Bytes>> {
    let result = recv_chunk_impl(connection, RecvChunkMode::Memory).await?;
    match result {
        Some(RecvChunkResult::Memory(bytes)) => Ok(Some(bytes)),
        Some(RecvChunkResult::File(_)) => Err(OctopiiError::Transport(
            "Unexpected file result for memory receive".to_string(),
        )),
        None => Ok(None),
    }
}

pub async fn recv_chunk_verified_to_file(
    connection: &Connection,
    path: &std::path::Path,
) -> Result<Option<u64>> {
    let result = recv_chunk_impl(connection, RecvChunkMode::File(path.to_path_buf())).await?;
    match result {
        Some(RecvChunkResult::File(len)) => Ok(Some(len)),
        Some(RecvChunkResult::Memory(_)) => Err(OctopiiError::Transport(
            "Unexpected memory result for file receive".to_string(),
        )),
        None => Ok(None),
    }
}

async fn recv_chunk_impl(
    connection: &Connection,
    mode: RecvChunkMode,
) -> Result<Option<RecvChunkResult>> {
    let (mut send_stream, mut recv_stream) = match connection.accept_bi().await {
        Ok(stream) => stream,
        Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut size_buf = [0u8; 8];
    recv_stream
        .read_exact(&mut size_buf)
        .await
        .map_err(|e| OctopiiError::Transport(format!("Failed to read size: {}", e)))?;
    let size = u64::from_le_bytes(size_buf);

    let mut sink = match mode {
        RecvChunkMode::Memory => ChunkSink::memory(size),
        RecvChunkMode::File(ref path) => ChunkSink::file(path.clone()).await?,
    };

    let mut hasher = Sha256::new();
    let mut received: u64 = 0;
    let mut buffer = vec![0u8; BUFFER_SIZE];

    while received < size {
        let to_read = std::cmp::min(BUFFER_SIZE as u64, size - received) as usize;
        recv_stream
            .read_exact(&mut buffer[..to_read])
            .await
            .map_err(|e| OctopiiError::Transport(format!("Failed to read chunk: {}", e)))?;
        sink.write_chunk(&buffer[..to_read]).await?;
        hasher.update(&buffer[..to_read]);
        received += to_read as u64;
    }

    let mut checksum = [0u8; 32];
    recv_stream
        .read_exact(&mut checksum)
        .await
        .map_err(|e| OctopiiError::Transport(format!("Failed to read checksum: {}", e)))?;

    let computed = hasher.finalize().to_vec();
    let status = if checksum == computed.as_slice() {
        0u8
    } else {
        1u8
    };

    send_stream
        .write_all(&[status])
        .await
        .map_err(|e| OctopiiError::Transport(format!("Failed to send ACK: {}", e)))?;
    send_stream
        .finish()
        .map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

    if status == 1 {
        return Err(OctopiiError::Transport(
            "Checksum verification failed on receiver".to_string(),
        ));
    }

    sink.finish(received).await.map(Some)
}
