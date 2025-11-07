use crate::error::{OctopiiError, Result};
use bytes::{BufMut, Bytes, BytesMut};
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};

/// Write-Ahead Log for durable storage
///
/// Format: [8 bytes: entry length][entry data]
///
/// Performance features:
/// - Batched writes (reduce fsync calls)
/// - Async I/O
/// - Sequential writes
pub struct WriteAheadLog {
    path: PathBuf,
    write_tx: mpsc::UnboundedSender<WalCommand>,
}

enum WalCommand {
    Append {
        data: Bytes,
        response: tokio::sync::oneshot::Sender<Result<u64>>,
    },
    Flush {
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

impl WriteAheadLog {
    /// Create a new WAL instance
    pub async fn new(path: PathBuf, batch_size: usize, flush_interval: Duration) -> Result<Self> {
        // Create directory if it doesn't exist
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let (write_tx, write_rx) = mpsc::unbounded_channel();

        // Spawn background writer task
        let wal_path = path.clone();
        tokio::spawn(async move {
            if let Err(e) = wal_writer_task(wal_path, write_rx, batch_size, flush_interval).await
            {
                tracing::error!("WAL writer task failed: {}", e);
            }
        });

        Ok(Self { path, write_tx })
    }

    /// Append data to the WAL
    /// Returns the offset where data was written
    pub async fn append(&self, data: Bytes) -> Result<u64> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.write_tx
            .send(WalCommand::Append { data, response: tx })
            .map_err(|_| OctopiiError::Wal("WAL writer task died".to_string()))?;

        rx.await
            .map_err(|_| OctopiiError::Wal("WAL response channel closed".to_string()))?
    }

    /// Force flush all pending writes
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.write_tx
            .send(WalCommand::Flush { response: tx })
            .map_err(|_| OctopiiError::Wal("WAL writer task died".to_string()))?;

        rx.await
            .map_err(|_| OctopiiError::Wal("WAL response channel closed".to_string()))?
    }

    /// Read all entries from the WAL (for recovery)
    pub async fn read_all(&self) -> Result<Vec<Bytes>> {
        let mut file = File::open(&self.path).await?;
        let mut entries = Vec::new();

        loop {
            // Read entry length
            let mut len_buf = [0u8; 8];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let len = u64::from_le_bytes(len_buf) as usize;

            // Read entry data
            let mut data = vec![0u8; len];
            file.read_exact(&mut data).await?;

            entries.push(Bytes::from(data));
        }

        Ok(entries)
    }
}

/// Background task that handles batched writes
async fn wal_writer_task(
    path: PathBuf,
    mut rx: mpsc::UnboundedReceiver<WalCommand>,
    batch_size: usize,
    flush_interval: Duration,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await?;

    // Track current file position
    let mut current_offset = file.seek(std::io::SeekFrom::End(0)).await?;

    let mut batch = Vec::with_capacity(batch_size);
    let mut flush_timer = interval(flush_interval);

    loop {
        tokio::select! {
            // Receive write commands
            cmd = rx.recv() => {
                match cmd {
                    Some(WalCommand::Append { data, response }) => {
                        batch.push((data, response, current_offset));

                        // Flush if batch is full
                        if batch.len() >= batch_size {
                            current_offset = flush_batch(&mut file, &mut batch).await?;
                        }
                    }
                    Some(WalCommand::Flush { response }) => {
                        current_offset = flush_batch(&mut file, &mut batch).await?;
                        let _ = response.send(Ok(()));
                    }
                    None => break, // Channel closed
                }
            }

            // Periodic flush
            _ = flush_timer.tick() => {
                if !batch.is_empty() {
                    current_offset = flush_batch(&mut file, &mut batch).await?;
                }
            }
        }
    }

    Ok(())
}

/// Flush accumulated batch to disk
async fn flush_batch(
    file: &mut File,
    batch: &mut Vec<(Bytes, tokio::sync::oneshot::Sender<Result<u64>>, u64)>,
) -> Result<u64> {
    if batch.is_empty() {
        return Ok(file.seek(std::io::SeekFrom::Current(0)).await?);
    }

    // Prepare write buffer
    let mut write_buf = BytesMut::new();
    for (data, _, _) in batch.iter() {
        write_buf.put_u64_le(data.len() as u64);
        write_buf.put(data.clone());
    }

    // Write to file
    file.write_all(&write_buf).await?;
    file.flush().await?;

    // Send responses
    for (_, response, offset) in batch.drain(..) {
        let _ = response.send(Ok(offset));
    }

    Ok(file.seek(std::io::SeekFrom::Current(0)).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wal_write_and_read() {
        let temp_dir = std::env::temp_dir();
        let wal_path = temp_dir.join("test_wal.log");

        // Clean up old file if exists
        let _ = tokio::fs::remove_file(&wal_path).await;

        let wal = WriteAheadLog::new(
            wal_path.clone(),
            10,
            Duration::from_millis(100),
        )
        .await
        .unwrap();

        // Write some entries
        let data1 = Bytes::from("hello");
        let data2 = Bytes::from("world");

        let offset1 = wal.append(data1.clone()).await.unwrap();
        let offset2 = wal.append(data2.clone()).await.unwrap();

        assert!(offset2 > offset1);

        // Flush to ensure data is written
        wal.flush().await.unwrap();

        // Read back
        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], data1);
        assert_eq!(entries[1], data2);

        // Clean up
        let _ = tokio::fs::remove_file(&wal_path).await;
    }
}
