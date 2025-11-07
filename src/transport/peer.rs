use crate::chunk::ChunkSource;
use crate::error::{OctopiiError, Result};
use bytes::{Bytes, BytesMut};
use quinn::Connection;
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A connection to a peer
pub struct PeerConnection {
    connection: Connection,
}

impl PeerConnection {
    pub(crate) fn new(connection: Connection) -> Self {
        Self { connection }
    }

    /// Send data to the peer
    ///
    /// Uses a bi-directional stream with framing:
    /// [4 bytes: message length][message data]
    pub async fn send(&self, data: Bytes) -> Result<()> {
        let (mut send, mut recv) = self.connection.open_bi().await?;

        // Write length-prefixed message
        let len = data.len() as u32;
        send.write_all(&len.to_le_bytes()).await?;
        send.write_all(&data).await?;
        send.finish().map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

        // Wait for acknowledgment (empty response)
        let _ = recv.read_to_end(0).await.map_err(|e| OctopiiError::Transport(format!("Read error: {}", e)))?;

        Ok(())
    }

    /// Receive data from a stream
    ///
    /// Reads a length-prefixed message and sends acknowledgment
    pub async fn recv(&self) -> Result<Option<Bytes>> {
        let (mut send, mut recv) = match self.connection.accept_bi().await {
            Ok(stream) => stream,
            Err(quinn::ConnectionError::ApplicationClosed(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        // Read length prefix
        let mut len_buf = [0u8; 4];
        recv.read_exact(&mut len_buf).await.map_err(|e| OctopiiError::Transport(format!("Read error: {}", e)))?;
        let len = u32::from_le_bytes(len_buf) as usize;

        // Read message data
        let mut data = BytesMut::with_capacity(len);
        data.resize(len, 0);
        recv.read_exact(&mut data).await.map_err(|e| OctopiiError::Transport(format!("Read error: {}", e)))?;

        // Send acknowledgment
        send.finish().map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

        Ok(Some(data.freeze()))
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
    /// Protocol:
    /// - [8 bytes: chunk size]
    /// - [N bytes: chunk data] (streamed in 64KB buffers)
    /// - [32 bytes: SHA256 checksum]
    /// - Wait for [1 byte: status] (0=OK, 1=checksum_fail, 2=error)
    ///
    /// Returns the number of bytes transferred
    pub async fn send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64> {
        const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

        let (mut send_stream, mut recv_stream) = self.connection.open_bi().await?;

        // Read chunk data and compute checksum
        let (data, size, checksum) = match chunk {
            ChunkSource::Memory(bytes) => {
                // For memory chunks, we already have the data
                let mut hasher = Sha256::new();
                hasher.update(&bytes);
                let hash = hasher.finalize();
                (Some(bytes.clone()), bytes.len() as u64, hash.to_vec())
            }
            ChunkSource::File(path) => {
                // For file chunks, we'll stream it
                let metadata = tokio::fs::metadata(path).await?;
                let size = metadata.len();
                (None, size, Vec::new()) // We'll compute hash while streaming
            }
        };

        // Send size
        send_stream.write_all(&size.to_le_bytes()).await?;

        // Send data and compute checksum (if needed)
        let final_checksum = if let Some(bytes) = data {
            // Memory: send all at once
            send_stream.write_all(&bytes).await?;
            checksum
        } else {
            // File: stream in chunks
            if let ChunkSource::File(path) = chunk {
                let mut file = File::open(path).await?;
                let mut hasher = Sha256::new();
                let mut buffer = vec![0u8; BUFFER_SIZE];
                let mut total_written = 0u64;

                loop {
                    let n = file.read(&mut buffer).await?;
                    if n == 0 {
                        break;
                    }

                    // Update checksum
                    hasher.update(&buffer[..n]);

                    // Write to stream
                    send_stream.write_all(&buffer[..n]).await?;
                    total_written += n as u64;
                }

                hasher.finalize().to_vec()
            } else {
                unreachable!()
            }
        };

        // Send checksum
        send_stream.write_all(&final_checksum).await?;
        send_stream
            .finish()
            .map_err(|e| OctopiiError::Transport(format!("Stream closed: {}", e)))?;

        // Wait for ACK
        let mut ack_buf = [0u8; 1];
        recv_stream
            .read_exact(&mut ack_buf)
            .await
            .map_err(|e| OctopiiError::Transport(format!("Failed to read ACK: {}", e)))?;

        match ack_buf[0] {
            0 => Ok(size), // Success
            1 => Err(OctopiiError::Transport(
                "Checksum verification failed on peer".to_string(),
            )),
            _ => Err(OctopiiError::Transport(
                "Unknown error on peer".to_string(),
            )),
        }
    }
}
