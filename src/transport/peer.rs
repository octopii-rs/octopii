use crate::error::{OctopiiError, Result};
use bytes::{Bytes, BytesMut};
use quinn::Connection;
use tokio::io::AsyncWriteExt;

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
}
