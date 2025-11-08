use crate::error::Result;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use raft::{prelude::*, Storage};
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::{Arc, RwLock as StdRwLock};

// Walrus topic names for different Raft state components
const TOPIC_LOG: &str = "raft_log";
const TOPIC_HARD_STATE: &str = "raft_hard_state";
const TOPIC_CONF_STATE: &str = "raft_conf_state";
const TOPIC_SNAPSHOT: &str = "raft_snapshot";

// Log compaction threshold: create snapshot after this many entries
const LOG_COMPACTION_THRESHOLD: u64 = 500;  // Reduced from 1000 for more aggressive compaction

// Serializable wrapper types for rkyv

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct HardStateData {
    term: u64,
    vote: u64,
    commit: u64,
}

impl From<&HardState> for HardStateData {
    fn from(hs: &HardState) -> Self {
        Self {
            term: hs.term,
            vote: hs.vote,
            commit: hs.commit,
        }
    }
}

impl From<&HardStateData> for HardState {
    fn from(data: &HardStateData) -> Self {
        let mut hs = HardState::default();
        hs.term = data.term;
        hs.vote = data.vote;
        hs.commit = data.commit;
        hs
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct ConfStateData {
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
}

impl From<&ConfState> for ConfStateData {
    fn from(cs: &ConfState) -> Self {
        Self {
            voters: cs.voters.clone(),
            learners: cs.learners.clone(),
            voters_outgoing: cs.voters_outgoing.clone(),
            learners_next: cs.learners_next.clone(),
            auto_leave: cs.auto_leave,
        }
    }
}

impl From<&ConfStateData> for ConfState {
    fn from(data: &ConfStateData) -> Self {
        let mut cs = ConfState::default();
        cs.voters = data.voters.clone();
        cs.learners = data.learners.clone();
        cs.voters_outgoing = data.voters_outgoing.clone();
        cs.learners_next = data.learners_next.clone();
        cs.auto_leave = data.auto_leave;
        cs
    }
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct SnapshotMetadataData {
    conf_state: ConfStateData,
    index: u64,
    term: u64,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct SnapshotData {
    metadata: SnapshotMetadataData,
    data: Vec<u8>,
}

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
    /// Create a new WAL-backed storage with crash recovery
    pub fn new(wal: Arc<WriteAheadLog>) -> Self {
        let storage = Self {
            wal,
            entries: StdRwLock::new(Vec::new()),
            hard_state: StdRwLock::new(HardState::default()),
            conf_state: StdRwLock::new(ConfState::default()),
            snapshot: StdRwLock::new(Snapshot::default()),
        };

        // Recover state from Walrus topics
        storage.recover_from_walrus();
        storage
    }

    /// Recover all Raft state from Walrus topics
    fn recover_from_walrus(&self) {
        tracing::info!("Starting Raft state recovery from Walrus...");

        // 1. Recover hard state (latest entry wins)
        if let Ok(hs) = self.recover_hard_state() {
            if hs.term > 0 || hs.vote > 0 || hs.commit > 0 {
                *self.hard_state.write().unwrap() = hs.clone();
                tracing::info!("✓ Recovered hard state: term={}, vote={}, commit={}",
                    hs.term, hs.vote, hs.commit);
            }
        }

        // 2. Recover conf state (latest entry wins)
        if let Ok(cs) = self.recover_conf_state() {
            if !cs.voters.is_empty() {
                *self.conf_state.write().unwrap() = cs.clone();
                tracing::info!("✓ Recovered conf state: voters={:?}, learners={:?}",
                    cs.voters, cs.learners);
            }
        }

        // 3. Recover snapshot (latest entry wins)
        if let Ok(snap) = self.recover_snapshot() {
            if snap.get_metadata().index > 0 {
                *self.snapshot.write().unwrap() = snap.clone();
                tracing::info!("✓ Recovered snapshot at index {} (term {})",
                    snap.get_metadata().index, snap.get_metadata().term);
            }
        }

        // 4. Recover log entries (rebuild entire log)
        if let Ok(entries) = self.recover_log_entries() {
            if !entries.is_empty() {
                *self.entries.write().unwrap() = entries.clone();
                tracing::info!("✓ Recovered {} log entries", entries.len());
            }
        }

        tracing::info!("Raft state recovery complete");
    }

    fn recover_hard_state(&self) -> crate::error::Result<HardState> {
        let walrus = &self.wal.walrus;
        let mut latest: Option<HardState> = None;
        let mut entry_count = 0;

        // Read all entries WITH checkpointing (checkpoint=true advances cursor for next read)
        // Since we only care about LATEST hard state, older entries are automatically reclaimed
        loop {
            match walrus.read_next(TOPIC_HARD_STATE, true) {
                Ok(Some(entry)) => {
                    entry_count += 1;
                    // Zero-copy deserialize with rkyv
                    let archived = unsafe {
                        rkyv::archived_root::<HardStateData>(&entry.data)
                    };
                    let data: HardStateData = match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(d) => d,
                        Err(_) => { tracing::warn!("Failed to deserialize hard state entry"); break; }
                    };
                            latest = Some((&data).into());
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        if entry_count > 0 {
            tracing::debug!("Recovered hard_state from {} entries (older entries reclaimable)", entry_count);
        }

        Ok(latest.unwrap_or_default())
    }

    fn recover_conf_state(&self) -> crate::error::Result<ConfState> {
        let walrus = &self.wal.walrus;
        let mut latest: Option<ConfState> = None;
        let mut entry_count = 0;

        // Read all entries WITH checkpointing
        loop {
            match walrus.read_next(TOPIC_CONF_STATE, true) {
                Ok(Some(entry)) => {
                    entry_count += 1;
                    let archived = unsafe {
                        rkyv::archived_root::<ConfStateData>(&entry.data)
                    };
                    let data: ConfStateData = match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(d) => d,
                        Err(_) => {
                            tracing::warn!("Failed to deserialize conf state entry");
                            break;
                        }
                    };
                    latest = Some((&data).into());
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        if entry_count > 0 {
            tracing::debug!("Recovered conf_state from {} entries", entry_count);
        }

        Ok(latest.unwrap_or_default())
    }

    fn recover_snapshot(&self) -> crate::error::Result<Snapshot> {
        let walrus = &self.wal.walrus;
        let mut latest: Option<Snapshot> = None;
        let mut entry_count = 0;

        // Read all entries WITH checkpointing
        loop {
            match walrus.read_next(TOPIC_SNAPSHOT, true) {
                Ok(Some(entry)) => {
                    entry_count += 1;
                    let archived = unsafe {
                        rkyv::archived_root::<SnapshotData>(&entry.data)
                    };
                    let snap_data: SnapshotData = match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(d) => d,
                        Err(_) => {
                            tracing::warn!("Failed to deserialize snapshot entry");
                            break;
                        }
                    };

                    let mut snapshot = Snapshot::default();
                    snapshot.data = Bytes::from(snap_data.data);

                    let metadata = snapshot.mut_metadata();
                    metadata.index = snap_data.metadata.index;
                    metadata.term = snap_data.metadata.term;
                    *metadata.mut_conf_state() = (&snap_data.metadata.conf_state).into();

                    latest = Some(snapshot);
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        if entry_count > 0 {
            tracing::debug!("Recovered snapshot from {} entries", entry_count);
        }

        Ok(latest.unwrap_or_default())
    }

    fn recover_log_entries(&self) -> crate::error::Result<Vec<Entry>> {
        let walrus = &self.wal.walrus;
        let mut entries = Vec::new();

        // Get snapshot index to determine which entries to keep
        let snapshot = self.snapshot.read().unwrap();
        let snapshot_index = snapshot.get_metadata().index;

        tracing::info!("Recovering log entries (snapshot at index {}) using batch reads", snapshot_index);

        // Read log entries in batches WITH checkpointing
        // CRITICAL: We checkpoint ALL entries (even old ones) to enable Walrus space reclamation,
        // but only KEEP entries after the snapshot index in memory
        let mut total_entries = 0;
        let mut compacted_entries = 0;

        // Use batch reads for 10-50x faster recovery
        const MAX_BATCH_BYTES: usize = 10_000_000; // 10MB per batch

        loop {
            match walrus.batch_read_for_topic(TOPIC_LOG, MAX_BATCH_BYTES, true) {
                Ok(batch) if batch.is_empty() => break, // No more entries
                Ok(batch) => {
                    for entry_data in batch {
                        total_entries += 1;

                        // Deserialize protobuf Entry (from raft-rs)
                        let parse_result: protobuf::ProtobufResult<Entry> = protobuf::Message::parse_from_bytes(&entry_data.data);
                        match parse_result {
                            Ok(raft_entry) => {
                                if raft_entry.index > snapshot_index {
                                    // KEEP entries after snapshot - needed for recovery
                                    entries.push(raft_entry);
                                } else {
                                    // DISCARD entries before snapshot - already included in snapshot
                                    // But we STILL checkpointed them (checkpoint=true above)
                                    // This allows Walrus to reclaim their blocks!
                                    compacted_entries += 1;
                                    tracing::trace!("Skipping compacted entry at index {}", raft_entry.index);
                                }
                            }
                            Err(e) => {
                                tracing::warn!("Failed to deserialize log entry: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Batch read error during recovery: {}", e);
                    break;
                }
            }
        }

        if total_entries > 0 {
            tracing::info!(
                "✓ Recovered {} log entries: {} kept (after snapshot), {} compacted (reclaimable by Walrus)",
                total_entries,
                entries.len(),
                compacted_entries
            );
        }

        Ok(entries)
    }

    /// Apply a snapshot to storage (NOW DURABLE!)
    pub fn apply_snapshot(&self, snapshot: Snapshot) -> crate::error::Result<()> {
        // Serialize snapshot with rkyv
        let snap_data = SnapshotData {
            metadata: SnapshotMetadataData {
                conf_state: ConfStateData::from(snapshot.get_metadata().get_conf_state()),
                index: snapshot.get_metadata().index,
                term: snapshot.get_metadata().term,
            },
            data: snapshot.data.to_vec(),
        };

        let bytes = rkyv::to_bytes::<_, 4096>(&snap_data)
            .map_err(|e| crate::error::OctopiiError::Wal(
                format!("Failed to serialize snapshot: {:?}", e)
            ))?;

        // Persist to Walrus
        tokio::task::block_in_place(|| {
            self.wal.walrus.append_for_topic(TOPIC_SNAPSHOT, &bytes)
        })?;

        tracing::debug!("Persisted snapshot at index {} (term {})",
            snapshot.get_metadata().index, snapshot.get_metadata().term);

        // Update in-memory state
        let mut snap = self.snapshot.write().unwrap();
        *snap = snapshot.clone();

        let mut conf = self.conf_state.write().unwrap();
        *conf = snapshot.get_metadata().get_conf_state().clone();

        let mut hs = self.hard_state.write().unwrap();
        hs.term = snapshot.get_metadata().term;
        hs.commit = snapshot.get_metadata().index;

        Ok(())
    }

    /// Append entries to storage (async version for WAL persistence)
    /// Uses Walrus batch_append_for_topic for improved performance (up to 2000 entries atomically)
    pub async fn append_entries(&self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Serialize all entries to protobuf
        let serialized: Vec<Vec<u8>> = entries
            .iter()
            .map(|entry| {
                protobuf::Message::write_to_bytes(entry)
                    .map_err(|e| crate::error::OctopiiError::Wal(format!("Protobuf error: {}", e)))
            })
            .collect::<Result<Vec<_>>>()?;

        // Convert to Vec<&[u8]> for batch_append API
        let batch_refs: Vec<&[u8]> = serialized.iter().map(|v| v.as_slice()).collect();

        // ATOMIC batch write to Walrus (up to 2000 entries, which is plenty for Raft)
        // This is 2-5x faster than individual appends due to io_uring batching on Linux
        let walrus = &self.wal.walrus;
        tokio::task::block_in_place(|| {
            walrus.batch_append_for_topic(TOPIC_LOG, &batch_refs)
        })?;

        tracing::debug!("Batch appended {} entries to WAL", entries.len());

        // Update in-memory cache
        let mut cache = self.entries.write().unwrap();
        cache.extend_from_slice(entries);

        Ok(())
    }

    /// Append entries synchronously (updates in-memory cache only)
    /// WAL persistence happens separately
    pub fn append_entries_sync(&self, entries: &[Entry]) {
        let mut cache = self.entries.write().unwrap();
        for entry in entries {
            cache.push(entry.clone());
        }
        tracing::trace!("Appended {} entries to in-memory cache", entries.len());
    }

    /// Set hard state (NOW DURABLE!)
    pub fn set_hard_state(&self, hs: HardState) {
        // Serialize with rkyv
        let data = HardStateData::from(&hs);
        let bytes = rkyv::to_bytes::<_, 256>(&data)
            .expect("Failed to serialize hard state");

        // Persist to Walrus (sync for simplicity in ready handler)
        tokio::task::block_in_place(|| {
            self.wal.walrus.append_for_topic(TOPIC_HARD_STATE, &bytes)
        })
        .expect("Failed to persist hard state");

        tracing::debug!("✓ Persisted hard state: term={}, vote={}, commit={}",
            data.term, data.vote, data.commit);

        // Update in-memory cache
        let mut state = self.hard_state.write().unwrap();
        *state = hs;
    }

    /// Set conf state (NOW DURABLE!)
    pub fn set_conf_state(&self, cs: ConfState) {
        // Serialize with rkyv
        let data = ConfStateData::from(&cs);
        let bytes = rkyv::to_bytes::<_, 256>(&data)
            .expect("Failed to serialize conf state");

        // Persist to Walrus
        tokio::task::block_in_place(|| {
            self.wal.walrus.append_for_topic(TOPIC_CONF_STATE, &bytes)
        })
        .expect("Failed to persist conf state");

        tracing::debug!("✓ Persisted conf state: voters={:?}, learners={:?}",
            data.voters, data.learners);

        // Update in-memory cache
        let mut state = self.conf_state.write().unwrap();
        *state = cs;
    }

    /// Compact logs by creating a snapshot and trimming old entries
    /// This prevents unbounded log growth and enables Walrus space reclamation
    pub fn compact_logs(&self, applied_index: u64, state_machine_data: Vec<u8>) -> crate::error::Result<()> {
        let entries = self.entries.read().unwrap();
        let snapshot_metadata = self.snapshot.read().unwrap().get_metadata().clone();

        // Check if we've accumulated enough entries since last snapshot
        let entries_since_snapshot = applied_index.saturating_sub(snapshot_metadata.index);

        if entries_since_snapshot < LOG_COMPACTION_THRESHOLD {
            // Not enough entries accumulated, skip compaction
            return Ok(());
        }

        drop(entries); // Release read lock before acquiring write locks

        tracing::info!("Log compaction triggered: {} entries since last snapshot (threshold: {})",
            entries_since_snapshot, LOG_COMPACTION_THRESHOLD);

        // Create new snapshot at applied_index
        let mut snapshot = Snapshot::default();
        snapshot.data = Bytes::from(state_machine_data);

        let metadata = snapshot.mut_metadata();
        metadata.index = applied_index;

        // Get current term from hard state
        let hard_state = self.hard_state.read().unwrap();
        metadata.term = hard_state.term;

        // Get current conf state
        let conf_state = self.conf_state.read().unwrap();
        *metadata.mut_conf_state() = conf_state.clone();

        // Persist snapshot to Walrus
        self.apply_snapshot(snapshot.clone())?;

        // Trim old entries from in-memory cache (keep entries after snapshot)
        let mut entries_mut = self.entries.write().unwrap();
        entries_mut.retain(|e| e.index > applied_index);

        tracing::info!("✓ Log compaction complete: snapshot at index {}, {} entries retained",
            applied_index, entries_mut.len());

        // Note: Old log entries before snapshot are automatically reclaimable by Walrus
        // because we use read_next(TOPIC_LOG, true) during recovery, which checkpoints
        // the cursor. Next recovery will skip old entries, allowing Walrus to reclaim space.

        Ok(())
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
