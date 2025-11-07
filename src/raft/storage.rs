use crate::error::Result;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use raft::{prelude::*, Storage};
use std::sync::{Arc, RwLock as StdRwLock};

/// Raft storage implementation backed by WAL
pub struct WalStorage {
    wal: Arc<WriteAheadLog>,
    // In-memory cache for recent entries
    entries: StdRwLock<Vec<Entry>>,
    hard_state: StdRwLock<HardState>,
    conf_state: StdRwLock<ConfState>,
    snapshot: StdRwLock<Snapshot>,
}

impl WalStorage {
    /// Create a new WAL-backed storage
    pub fn new(wal: Arc<WriteAheadLog>) -> Self {
        Self {
            wal,
            entries: StdRwLock::new(Vec::new()),
            hard_state: StdRwLock::new(HardState::default()),
            conf_state: StdRwLock::new(ConfState::default()),
            snapshot: StdRwLock::new(Snapshot::default()),
        }
    }

    /// Append entries to storage
    pub async fn append_entries(&self, entries: &[Entry]) -> Result<()> {
        for entry in entries {
            // Serialize entry using protobuf
            let data = protobuf::Message::write_to_bytes(entry)
                .map_err(|e| crate::error::OctopiiError::Wal(format!("Protobuf error: {}", e)))?;
            let bytes = Bytes::from(data);

            // Write to WAL
            self.wal.append(bytes).await?;

            // Add to in-memory cache
            let mut cache = self.entries.write().unwrap();
            cache.push(entry.clone());
        }

        Ok(())
    }

    /// Set hard state
    pub fn set_hard_state(&self, hs: HardState) {
        let mut state = self.hard_state.write().unwrap();
        *state = hs;
    }

    /// Set conf state
    pub fn set_conf_state(&self, cs: ConfState) {
        let mut state = self.conf_state.write().unwrap();
        *state = cs;
    }
}

impl Storage for WalStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.hard_state.read().unwrap().clone();
        let conf_state = self.conf_state.read().unwrap().clone();

        Ok(RaftState {
            hard_state,
            conf_state,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let entries = self.entries.read().unwrap();

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let first_index = entries[0].index;
        let last_index = entries[entries.len() - 1].index;

        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if high > last_index + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let start = (low - first_index) as usize;
        let end = (high - first_index) as usize;

        let mut result = entries[start..end].to_vec();

        if let Some(max) = max_size.into() {
            let mut size = 0u64;
            let mut count = 0;
            for entry in &result {
                size += entry.data.len() as u64;
                if size > max {
                    break;
                }
                count += 1;
            }
            result.truncate(count);
        }

        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();

        if entries.is_empty() {
            let snapshot = self.snapshot.read().unwrap();
            if idx == snapshot.get_metadata().index {
                return Ok(snapshot.get_metadata().term);
            }
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let first_index = entries[0].index;
        let last_index = entries[entries.len() - 1].index;

        if idx < first_index {
            let snapshot = self.snapshot.read().unwrap();
            if idx == snapshot.get_metadata().index {
                return Ok(snapshot.get_metadata().term);
            }
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }

        if idx > last_index {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let offset = (idx - first_index) as usize;
        Ok(entries[offset].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        if let Some(entry) = entries.first() {
            Ok(entry.index)
        } else {
            let snapshot = self.snapshot.read().unwrap();
            Ok(snapshot.get_metadata().index + 1)
        }
    }

    fn last_index(&self) -> raft::Result<u64> {
        let entries = self.entries.read().unwrap();
        if let Some(entry) = entries.last() {
            Ok(entry.index)
        } else {
            let snapshot = self.snapshot.read().unwrap();
            Ok(snapshot.get_metadata().index)
        }
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let snapshot = self.snapshot.read().unwrap();
        Ok(snapshot.clone())
    }
}
