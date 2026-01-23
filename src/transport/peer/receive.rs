use crate::error::{OctopiiError, Result};
use bytes::{Bytes, BytesMut};
use quinn::Connection;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BUFFER_SIZE: usize = 64 * 1024;

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
        let cap = std::cmp::min(total_size as usize, 10 * 1024 * 1024);
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
            ChunkSink::File(file) => file.write_all(buf).await.map_err(|e| {
                OctopiiError::Transport(format!("File write failed: {}", e))
            }),
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
