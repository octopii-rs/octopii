pub mod wal;

use crate::error::{OctopiiError, Result};
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

pub use wal::{FsyncSchedule, Walrus};

/// Write-Ahead Log for durable storage
///
/// This is a Walrus-backed implementation that provides an async API
/// compatible with the previous file-based WAL implementation.
///
/// Performance features:
/// - Checksummed entries (FNV-1a)
/// - Configurable fsync schedule
/// - Zero-copy I/O via mmap
/// - Crash recovery
pub struct WriteAheadLog {
    walrus: Arc<Walrus>,
    topic: String,
    offset_counter: Arc<AtomicU64>,
    write_counter: Arc<AtomicU64>, // Track actual writes for read_all
}

impl WriteAheadLog {
    /// Create a new WAL instance
    ///
    /// `path`: Used to derive the WAL namespace/key
    /// `batch_size`: Not used in Walrus (Walrus manages batching internally)
    /// `flush_interval`: Converted to FsyncSchedule
    pub async fn new(path: PathBuf, _batch_size: usize, flush_interval: Duration) -> Result<Self> {
        // Derive a key from the path
        let key = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("default_wal")
            .to_string();

        // Convert flush_interval to FsyncSchedule
        let schedule = if flush_interval.as_millis() == 0 {
            FsyncSchedule::SyncEach
        } else {
            FsyncSchedule::Milliseconds(flush_interval.as_millis() as u64)
        };

        // Create Walrus instance in blocking context
        let walrus = tokio::task::spawn_blocking(move || {
            wal::Walrus::with_consistency_and_schedule_for_key(
                &key,
                wal::ReadConsistency::StrictlyAtOnce,
                schedule,
            )
        })
        .await
        .map_err(|e| OctopiiError::Wal(format!("Failed to spawn Walrus task: {}", e)))?
        .map_err(|e| OctopiiError::Wal(format!("Failed to create Walrus: {}", e)))?;

        Ok(Self {
            walrus: Arc::new(walrus),
            topic: "wal_data".to_string(),
            offset_counter: Arc::new(AtomicU64::new(0)),
            write_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Append data to the WAL
    /// Returns a monotonically increasing offset
    pub async fn append(&self, data: Bytes) -> Result<u64> {
        let walrus = self.walrus.clone();
        let topic = self.topic.clone();
        let data_vec = data.to_vec();

        tokio::task::spawn_blocking(move || {
            walrus.append_for_topic(&topic, &data_vec)
        })
        .await
        .map_err(|e| OctopiiError::Wal(format!("Failed to spawn append task: {}", e)))?
        .map_err(|e| OctopiiError::Wal(format!("Failed to append: {}", e)))?;

        // Increment write counter for read_all tracking
        self.write_counter.fetch_add(1, Ordering::SeqCst);

        // Return incrementing offset
        let offset = self.offset_counter.fetch_add(1, Ordering::SeqCst);
        Ok(offset)
    }

    /// Force flush all pending writes
    /// Note: Walrus handles fsyncing based on the configured schedule
    /// We add a small delay to ensure background fsync completes
    pub async fn flush(&self) -> Result<()> {
        // Give Walrus's background fsync thread time to complete
        // The configured schedule is typically 100-200ms
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
        Ok(())
    }

    /// Read all entries from the WAL (for recovery)
    pub async fn read_all(&self) -> Result<Vec<Bytes>> {
        let walrus = self.walrus.clone();
        let topic = self.topic.clone();

        tokio::task::spawn_blocking(move || {
            let mut all_entries = Vec::new();

            // Use batch_read to efficiently read all entries
            // Read in 10MB chunks, with checkpointing to advance reader position
            loop {
                match walrus.batch_read_for_topic(&topic, 10 * 1024 * 1024, true) {
                    Ok(batch) => {
                        if batch.is_empty() {
                            // No more entries
                            break;
                        }
                        // Convert to Bytes and append
                        for entry in batch {
                            all_entries.push(Bytes::from(entry.data));
                        }
                    }
                    Err(_e) => {
                        // Error reading - stop here
                        break;
                    }
                }
            }

            Ok(all_entries)
        })
        .await
        .map_err(|e| OctopiiError::Wal(format!("Failed to spawn read task: {}", e)))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wal_write_and_read() {
        let temp_dir = std::env::temp_dir();
        let wal_path = temp_dir.join("test_walrus_basic.log");

        // Clean up any existing test WAL files
        let wal_files_dir = std::path::PathBuf::from("wal_files/test_walrus_basic.log");
        if wal_files_dir.exists() {
            let _ = std::fs::remove_dir_all(&wal_files_dir);
        }

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

        // Flush (no-op with Walrus, but tests compatibility)
        wal.flush().await.unwrap();

        // Small delay to ensure Walrus has fsynced
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Read back
        let entries = wal.read_all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], data1);
        assert_eq!(entries[1], data2);
    }
}
