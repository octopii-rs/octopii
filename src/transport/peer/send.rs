use super::BUFFER_SIZE;
use crate::chunk::ChunkSource;
use crate::error::{OctopiiError, Result};
use bytes::Bytes;
use quinn::{Connection, SendStream};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

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

pub async fn send_chunk_verified(connection: &Connection, chunk: &ChunkSource) -> Result<u64> {
    let (mut send_stream, mut recv_stream) = connection.open_bi().await?;

    let (data, size, checksum) = match chunk {
        ChunkSource::Memory(bytes) => {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            let hash = hasher.finalize();
            (Some(bytes.clone()), bytes.len() as u64, hash.to_vec())
        }
        ChunkSource::File(path) => {
            let metadata = tokio::fs::metadata(path).await?;
            let size = metadata.len();
            (None, size, Vec::new())
        }
    };

    send_stream.write_all(&size.to_le_bytes()).await?;

    let final_checksum = if let Some(bytes) = data {
        send_stream.write_all(&bytes).await?;
        checksum
    } else if let ChunkSource::File(path) = chunk {
        stream_file(&mut send_stream, path).await?
    } else {
        unreachable!()
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
