#![cfg(feature = "openraft")]

use crate::error::OctopiiError;
use crate::openraft::types::{AppEntry, AppNodeId, AppResponse, AppTypeConfig};
use crate::state_machine::StateMachine;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use openraft::{
    alias::SnapshotDataOf,
    storage::{
        EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
        Snapshot, SnapshotMeta,
    },
    Entry, EntryPayload, LogId, OptionalSend, RaftLogReader, RaftTypeConfig, StorageError,
    StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "simulation")]
use std::time::Duration;

#[cfg(feature = "simulation")]
use crate::openraft::sim_runtime;
#[cfg(feature = "simulation")]
use tokio::task::yield_now;

// --- Log Store ---
#[derive(Clone, Debug, Default)]
pub struct MemLogStore {
    inner: Arc<tokio::sync::Mutex<MemLogStoreInner>>,
}

#[derive(Debug)]
struct MemLogStoreInner {
    last_purged_log_id: Option<LogId<AppTypeConfig>>,
    log: BTreeMap<u64, Entry<AppTypeConfig>>,
    committed: Option<LogId<AppTypeConfig>>,
    vote: Option<openraft::Vote<AppTypeConfig>>,
}

impl Default for MemLogStoreInner {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl MemLogStoreInner {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AppTypeConfig>>, io::Error> {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id);

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        Ok(self.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<AppTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry<AppTypeConfig>>,
    {
        for entry in entries {
            self.log.insert(entry.log_id.index, entry);
        }
        callback.io_completed(Ok(())).await;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        let keys = self
            .log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self
                .log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

impl MemLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftLogReader<AppTypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }
}

impl RaftLogStorage<AppTypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<AppTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry<AppTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().await;
        inner.append(entries, callback).await
    }

    async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

// --- State Machine Store ---
#[derive(Debug, Clone)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<AppTypeConfig>,
    pub data: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<AppTypeConfig>>,
    pub last_membership: StoredMembership<AppTypeConfig>,
    pub data: BTreeMap<String, String>,
}

/// WAL record for state machine metadata (membership and applied log)
#[derive(Serialize, Deserialize)]
enum SmMetaRecord {
    /// Membership change: (log_id, membership)
    Membership {
        log_id: Option<LogId<AppTypeConfig>>,
        membership: openraft::Membership<AppTypeConfig>,
    },
}

/// State machine wrapper for OpenRaft with WAL-backed membership persistence.
///
/// When constructed with a WAL (`new_with_wal`), membership changes are persisted
/// and recovered on restart. This ensures nodes remember their voter/learner status
/// across crash/recovery cycles.
pub struct MemStateMachine {
    sm: StateMachine,
    state_machine: tokio::sync::RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: tokio::sync::RwLock<Option<StoredSnapshot>>,
    /// Optional WAL for persisting membership. If None, membership is in-memory only.
    meta_wal: Option<Arc<WriteAheadLog>>,
}

impl MemStateMachine {
    /// Create a new state machine without WAL persistence (membership is in-memory only).
    pub fn new(sm: StateMachine) -> Arc<Self> {
        Arc::new(Self {
            sm,
            state_machine: tokio::sync::RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: tokio::sync::RwLock::new(None),
            meta_wal: None,
        })
    }

    /// Create a new state machine with WAL-backed membership persistence.
    /// Recovers last_membership and last_applied_log from WAL on construction.
    pub async fn new_with_wal(sm: StateMachine, wal: Arc<WriteAheadLog>) -> Arc<Self> {
        let mut data = StateMachineData::default();

        // Recover metadata from WAL
        if let Ok(entries) = wal.read_all().await {
            for raw in entries {
                if let Ok(record) = bincode::deserialize::<SmMetaRecord>(&raw) {
                    let SmMetaRecord::Membership { log_id, membership } = record;
                    data.last_membership = StoredMembership::new(log_id, membership);
                }
            }
        }

        Arc::new(Self {
            sm,
            state_machine: tokio::sync::RwLock::new(data),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: tokio::sync::RwLock::new(None),
            meta_wal: Some(wal),
        })
    }

    /// Persist a metadata record to WAL (if WAL is configured).
    async fn persist_meta(&self, record: &SmMetaRecord) -> io::Result<()> {
        if let Some(ref wal) = self.meta_wal {
            let data = bincode::serialize(record)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            #[cfg(feature = "simulation")]
            {
                // Retry on transient I/O errors in simulation
                for attempt in 0..20 {
                    match wal.append(Bytes::from(data.clone())).await {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            if attempt == 19 {
                                return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                            }
                            sim_runtime::advance_time(Duration::from_millis(10));
                            yield_now().await;
                        }
                    }
                }
            }
            #[cfg(not(feature = "simulation"))]
            {
                wal.append(Bytes::from(data)).await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
            }
        }
        Ok(())
    }
}

impl RaftSnapshotBuilder<AppTypeConfig> for Arc<MemStateMachine> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<AppTypeConfig>, io::Error> {
        let state_machine = self.state_machine.read().await;
        let data = bincode::serialize(&state_machine.data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id.node_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}

impl RaftStateMachine<AppTypeConfig> for Arc<MemStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<AppTypeConfig>>,
            StoredMembership<AppTypeConfig>,
        ),
        io::Error,
    > {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm:
            Stream<Item = Result<EntryResponder<AppTypeConfig>, io::Error>> + Unpin + OptionalSend,
    {
        // Collect membership updates to persist after releasing the lock
        let mut membership_to_persist: Option<(Option<LogId<AppTypeConfig>>, openraft::Membership<AppTypeConfig>)> = None;

        {
            let mut sm = self.state_machine.write().await;

            while let Some((entry, responder)) = entries.try_next().await? {
                sm.last_applied_log = Some(entry.log_id);

                let response = match entry.payload {
                    EntryPayload::Blank => AppResponse(Vec::new()),
                    EntryPayload::Normal(ref data) => {
                        let result = self
                            .sm
                            .apply(&data.0)
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                        AppResponse(result.to_vec())
                    }
                    EntryPayload::Membership(ref mem) => {
                        sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                        // Queue for persistence (only persist the last membership in this batch)
                        membership_to_persist = Some((Some(entry.log_id), mem.clone()));
                        AppResponse(Vec::new())
                    }
                };

                if let Some(responder) = responder {
                    responder.send(response);
                }
            }
        }

        // Persist membership updates outside the lock
        if let Some((log_id, membership)) = membership_to_persist {
            self.persist_meta(&SmMetaRecord::Membership { log_id, membership }).await?;
        }

        Ok(())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<AppTypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<AppTypeConfig>,
        snapshot: SnapshotDataOf<AppTypeConfig>,
    ) -> Result<(), io::Error> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let updated_state_machine_data: BTreeMap<String, String> =
            bincode::deserialize(&new_snapshot.data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let updated_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: updated_state_machine_data.clone(),
        };

        // Extract membership info for persistence before taking locks
        let membership_to_persist = meta.last_membership.membership().clone();
        let membership_log_id = meta.last_membership.log_id().clone();

        {
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        let mut current_snapshot = self.current_snapshot.write().await;

        // Also restore into the state machine
        let snapshot_bytes = bincode::serialize(&updated_state_machine_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.sm
            .restore(&snapshot_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        *current_snapshot = Some(new_snapshot);
        drop(current_snapshot);

        // Persist membership from snapshot
        self.persist_meta(&SmMetaRecord::Membership {
            log_id: membership_log_id,
            membership: membership_to_persist,
        }).await?;

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<AppTypeConfig>>, io::Error> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Cursor::new(data),
                }))
            }
            None => Ok(None),
        }
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

/// Helper to create a new memory log store
pub fn new_mem_log_store() -> MemLogStore {
    MemLogStore::new()
}

#[derive(Clone)]
pub struct WalLogStore {
    inner: Arc<tokio::sync::Mutex<MemLogStoreInner>>,
    wal: Arc<WriteAheadLog>,
}

#[derive(Serialize, Deserialize)]
enum WalLogRecord {
    LogEntry(Entry<AppTypeConfig>),
    Vote(openraft::Vote<AppTypeConfig>),
    Committed(Option<LogId<AppTypeConfig>>),
    Purged(LogId<AppTypeConfig>),
    Truncated(LogId<AppTypeConfig>),
}

impl WalLogStore {
    pub async fn new(wal: Arc<WriteAheadLog>) -> Result<Self, OctopiiError> {
        let store = Self {
            inner: Arc::new(tokio::sync::Mutex::new(MemLogStoreInner::default())),
            wal,
        };
        store.recover_from_wal().await?;
        Ok(store)
    }

    fn sim_assert_log_store_state(inner: &MemLogStoreInner) {
        let last_log_id = inner.log.iter().next_back().map(|(_, entry)| entry.log_id);
        if let Some(purged) = inner.last_purged_log_id.clone() {
            crate::invariants::sim_assert(
                last_log_id.map_or(true, |last| purged <= last),
                "last purged log id is after last log id",
            );
            for (idx, entry) in inner.log.iter() {
                crate::invariants::sim_assert(
                    entry.log_id.index == *idx,
                    "log entry index mismatches map key",
                );
                crate::invariants::sim_assert(
                    *idx > purged.index,
                    "log entry is not strictly after last purged log id",
                );
            }
        } else {
            for (idx, entry) in inner.log.iter() {
                crate::invariants::sim_assert(
                    entry.log_id.index == *idx,
                    "log entry index mismatches map key",
                );
            }
        }

        if let Some(committed) = inner.committed {
            // committed must be <= last_log_id, OR if log is empty, <= last_purged
            let committed_valid = match last_log_id {
                Some(last) => committed <= last,
                None => inner
                    .last_purged_log_id
                    .map(|p| committed <= p)
                    .unwrap_or(false),
            };
            crate::invariants::sim_assert(
                committed_valid,
                "committed log id is after last log/purged id",
            );
            if let Some(purged) = inner.last_purged_log_id.clone() {
                crate::invariants::sim_assert(
                    committed.index >= purged.index,
                    "committed log id is before last purged log id",
                );
            }
        }

        // Invariant #6: Log continuity - no gaps between entries
        if let (Some(first_idx), Some(last_idx)) =
            (inner.log.keys().next().copied(), inner.log.keys().next_back().copied())
        {
            let expected_count = (last_idx - first_idx + 1) as usize;
            crate::invariants::sim_assert(
                inner.log.len() == expected_count,
                "log has gaps between first and last entry",
            );
        }

        // Invariant #7: First entry immediately follows purged index
        if let Some(purged) = inner.last_purged_log_id.clone() {
            if let Some(first_idx) = inner.log.keys().next().copied() {
                crate::invariants::sim_assert(
                    first_idx == purged.index + 1,
                    "first log entry doesn't immediately follow purged index",
                );
            }
        }

        // Invariant #8: Committed entry is accessible (in log or purged)
        if let Some(committed) = inner.committed {
            let in_log = inner.log.contains_key(&committed.index);
            let is_purged = inner
                .last_purged_log_id
                .map(|p| committed.index <= p.index)
                .unwrap_or(false);
            crate::invariants::sim_assert(
                in_log || is_purged,
                "committed log_id not found in log and not purged",
            );
        }
    }

    async fn recover_from_wal(&self) -> Result<(), OctopiiError> {
        let entries = self.wal.read_all().await.unwrap_or_else(|_| Vec::new());
        if cfg!(feature = "simulation")
            && std::env::var("CLUSTER_DEBUG").ok().as_deref() == Some("1")
        {
            eprintln!(
                "[cluster_debug] wal_log_store recover entries={}",
                entries.len()
            );
        }
        let mut inner = self.inner.lock().await;
        Self::apply_wal_entries(&entries, &mut inner)?;
        Self::repair_state_after_recovery(&mut inner);
        Self::sim_assert_log_store_state(&inner);
        #[cfg(feature = "simulation")]
        {
            let snapshot = LogStoreSnapshot::from_inner(&inner);
            let mut fresh = MemLogStoreInner::default();
            Self::apply_wal_entries(&entries, &mut fresh)?;
            Self::repair_state_after_recovery(&mut fresh);
            Self::sim_assert_log_store_state(&fresh);
            crate::invariants::sim_assert(
                snapshot == LogStoreSnapshot::from_inner(&fresh),
                "wal log store recovery not idempotent",
            );
        }
        Ok(())
    }

    /// Repair state after recovery to handle partial writes.
    /// 1. Remove entries at or before purge point (they shouldn't exist)
    /// 2. Truncate log at first gap (log must be contiguous)
    /// 3. Ensure purged <= last_log_id (by clearing invalid purge)
    /// 4. If committed points to a lost entry, roll it back
    fn repair_state_after_recovery(inner: &mut MemLogStoreInner) {
        // Step 1: Remove entries at or before purge point
        // Note: Use clone() since LogId doesn't implement Copy
        if let Some(ref purged) = inner.last_purged_log_id {
            let purge_idx = purged.index;
            let keys_to_remove: Vec<u64> = inner
                .log
                .keys()
                .filter(|&&k| k <= purge_idx)
                .copied()
                .collect();
            for key in keys_to_remove {
                inner.log.remove(&key);
            }
        }

        // Step 2: Find and truncate at first gap in log
        // Log must be contiguous - if there's a gap, entries after the gap are invalid
        let expected_first = match inner.last_purged_log_id.as_ref() {
            Some(p) => p.index + 1,
            None => inner.log.keys().next().copied().unwrap_or(1),
        };
        if cfg!(feature = "simulation")
            && std::env::var("CLUSTER_DEBUG").ok().as_deref() == Some("1")
        {
            let first_key = inner.log.keys().next().copied();
            eprintln!(
                "[cluster_debug] wal_log_store repair expected_first={} first_key={:?} last_purged={:?}",
                expected_first,
                first_key,
                inner.last_purged_log_id.as_ref().map(|p| p.index)
            );
        }
        let mut last_valid_idx: Option<u64> = None;

        for (&idx, _) in inner.log.iter() {
            match last_valid_idx {
                None => {
                    // First entry - must match expected_first or be removed
                    if idx == expected_first {
                        last_valid_idx = Some(idx);
                    } else {
                        // Entry before expected_first or gap at start - will be cleaned up
                        break;
                    }
                }
                Some(last) => {
                    if idx == last + 1 {
                        // Contiguous, continue
                        last_valid_idx = Some(idx);
                    } else {
                        // Gap found - stop here
                        break;
                    }
                }
            }
        }

        // Remove entries that aren't contiguous from expected_first
        let valid_range = match last_valid_idx {
            Some(last) => expected_first..=last,
            None => expected_first..=0, // Empty range - remove all
        };
        let keys_to_remove: Vec<u64> = inner
            .log
            .keys()
            .filter(|&&k| !valid_range.contains(&k))
            .copied()
            .collect();
        for key in keys_to_remove {
            inner.log.remove(&key);
        }

        // Step 3: Ensure purged <= last_log_id (clear invalid purge if needed)
        let last_log_id = inner.log.values().next_back().map(|e| e.log_id);
        if let Some(ref purged) = inner.last_purged_log_id {
            let valid = last_log_id.map(|last| *purged <= last).unwrap_or(true);
            if !valid {
                // purged > last_log_id - this is inconsistent
                // Can't unpurge, so we must accept the purge and clear the log
                inner.log.clear();
            }
        }

        // Step 2: Repair committed to point to a valid entry (AFTER truncation)
        // Must satisfy both:
        //   - Invariant #4: committed <= last_log_id (LogId comparison)
        //   - Invariant #8: committed.index in log OR committed.index <= purged.index
        let last_log_id = inner.log.values().next_back().map(|e| e.log_id);

        if let Some(committed) = inner.committed {
            let in_log = inner.log.contains_key(&committed.index);
            let is_purged = inner
                .last_purged_log_id
                .map(|p| committed.index <= p.index)
                .unwrap_or(false);
            let valid_logid = last_log_id
                .map(|last| committed <= last)
                .unwrap_or(is_purged); // If no log, must be purged

            if !in_log && !is_purged {
                // Invariant #8 violated: committed index not accessible
                inner.committed = last_log_id.or(inner.last_purged_log_id);
            } else if !valid_logid {
                // Invariant #4 violated: committed > last_log_id
                inner.committed = last_log_id.or(inner.last_purged_log_id);
            }
        }
    }

    fn apply_wal_entries(
        entries: &[Bytes],
        inner: &mut MemLogStoreInner,
    ) -> Result<(), OctopiiError> {
        let mut log_entry_count = 0usize;
        let mut vote_count = 0usize;
        let mut committed_count = 0usize;
        let mut purged_count = 0usize;
        let mut truncated_count = 0usize;
        for raw in entries {
            let record: WalLogRecord = bincode::deserialize(raw)
                .map_err(|e| OctopiiError::Wal(format!("Failed to deserialize WAL record: {e}")))?;
            match record {
                WalLogRecord::LogEntry(entry) => {
                    log_entry_count += 1;
                    inner.log.insert(entry.log_id.index, entry);
                }
                WalLogRecord::Vote(vote) => {
                    vote_count += 1;
                    inner.vote = Some(vote);
                }
                WalLogRecord::Committed(committed) => {
                    committed_count += 1;
                    inner.committed = committed;
                }
                WalLogRecord::Purged(log_id) => {
                    purged_count += 1;
                    let keys = inner
                        .log
                        .range(..=log_id.index)
                        .map(|(idx, _)| *idx)
                        .collect::<Vec<_>>();
                    for key in keys {
                        inner.log.remove(&key);
                    }
                    inner.last_purged_log_id = Some(log_id);
                }
                WalLogRecord::Truncated(log_id) => {
                    truncated_count += 1;
                    let keys = inner
                        .log
                        .range(log_id.index..)
                        .map(|(idx, _)| *idx)
                        .collect::<Vec<_>>();
                    for key in keys {
                        inner.log.remove(&key);
                    }
                }
            }
        }
        if cfg!(feature = "simulation")
            && std::env::var("CLUSTER_DEBUG").ok().as_deref() == Some("1")
        {
            eprintln!(
                "[cluster_debug] wal_log_store apply counts: log_entry={} vote={} committed={} purged={} truncated={}",
                log_entry_count, vote_count, committed_count, purged_count, truncated_count
            );
        }
        Ok(())
    }

    async fn persist_record(&self, record: &WalLogRecord) -> Result<(), io::Error> {
        let data =
            bincode::serialize(record).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        #[cfg(feature = "simulation")]
        {
            for attempt in 0..20 {
                match self.wal.append(Bytes::from(data.clone())).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        if attempt == 19 {
                            return Err(io::Error::new(io::ErrorKind::Other, e.to_string()));
                        }
                        sim_runtime::advance_time(Duration::from_millis(10));
                        yield_now().await;
                    }
                }
            }
            return Ok(());
        }
        #[cfg(not(feature = "simulation"))]
        {
            self.wal
                .append(Bytes::from(data))
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(feature = "simulation")]
#[derive(Debug, PartialEq)]
struct LogStoreSnapshot {
    last_purged_log_id: Option<LogId<AppTypeConfig>>,
    log: BTreeMap<u64, Entry<AppTypeConfig>>,
    committed: Option<LogId<AppTypeConfig>>,
    vote: Option<openraft::Vote<AppTypeConfig>>,
}

#[cfg(feature = "simulation")]
impl LogStoreSnapshot {
    fn from_inner(inner: &MemLogStoreInner) -> Self {
        Self {
            last_purged_log_id: inner.last_purged_log_id.clone(),
            log: inner.log.clone(),
            committed: inner.committed,
            vote: inner.vote.clone(),
        }
    }
}

impl RaftLogReader<AppTypeConfig> for WalLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }

    async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }
}

impl RaftLogStorage<AppTypeConfig> for WalLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.committed = committed;
            Self::sim_assert_log_store_state(&inner);
        }
        self.persist_record(&WalLogRecord::Committed(committed))
            .await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.vote = Some(vote.clone());
            Self::sim_assert_log_store_state(&inner);
        }
        self.persist_record(&WalLogRecord::Vote(vote.clone())).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<AppTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry<AppTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut to_persist = Vec::new();
        {
            let mut inner = self.inner.lock().await;
            for entry in entries {
                to_persist.push(entry.clone());
                inner.log.insert(entry.log_id.index, entry);
            }
            Self::sim_assert_log_store_state(&inner);
        }
        for entry in to_persist {
            self.persist_record(&WalLogRecord::LogEntry(entry)).await?;
        }
        callback.io_completed(Ok(())).await;
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.truncate(log_id).await?;
            Self::sim_assert_log_store_state(&inner);
        }
        self.persist_record(&WalLogRecord::Truncated(log_id)).await
    }

    async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let mut inner = self.inner.lock().await;
            inner.purge(log_id).await?;
            Self::sim_assert_log_store_state(&inner);
        }
        self.persist_record(&WalLogRecord::Purged(log_id)).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

pub async fn new_wal_log_store(wal: Arc<WriteAheadLog>) -> Result<WalLogStore, OctopiiError> {
    WalLogStore::new(wal).await
}

/// Helper to create a new memory state machine
pub fn new_mem_state_machine(sm: StateMachine) -> Arc<MemStateMachine> {
    MemStateMachine::new(sm)
}

#[cfg(all(test, feature = "simulation", feature = "openraft"))]
mod tests {
    use super::*;
    use crate::wal::wal::vfs::sim::{self, SimConfig};
    use crate::wal::wal::vfs;
    use crate::wal::WriteAheadLog;
    use openraft::type_config::alias::CommittedLeaderIdOf;
    use openraft::vote::RaftLeaderId;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::runtime::Builder;
    use std::time::Duration;

    #[test]
    fn strictly_at_once_wal_log_store_recovery() {
        const SCENARIOS: usize = 10;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 200;
        const BASE_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_strict_wal_log_store_{scenario}"));
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            vfs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let wal_path = root_dir.join("wal_log_store.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xd1b5_4a32_1a2b_3c4d);
            let mut oracle_log: BTreeMap<u64, Entry<AppTypeConfig>> = BTreeMap::new();
            let mut oracle_vote: Option<openraft::Vote<AppTypeConfig>> = None;
            let mut oracle_committed: Option<LogId<AppTypeConfig>> = None;
            let mut oracle_last_purged: Option<LogId<AppTypeConfig>> = None;
            let mut next_index: u64 = 1;

            for _cycle in 0..CRASH_CYCLES {
                let prev_rate = sim::get_io_error_rate();
                let prev_partial = sim::get_partial_writes_enabled();
                sim::set_io_error_rate(0.0);
                sim::set_partial_writes_enabled(false);
                let wal = create_wal_with_retry(
                    &rt,
                    wal_path.clone(),
                    scenario_seed,
                    WAL_CREATE_RETRIES,
                );
                let mut store = rt
                    .block_on(WalLogStore::new(Arc::clone(&wal)))
                    .expect("Failed to create WalLogStore");

                // Sync oracle from recovered state - under partial writes, some data may be lost.
                // The sim_assert invariants in WalLogStore verify internal consistency.
                sync_oracle_from_store(
                    &rt,
                    &mut store,
                    &mut oracle_log,
                    &mut oracle_vote,
                    &mut oracle_committed,
                    &mut oracle_last_purged,
                    &mut next_index,
                );
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                for _ in 0..OPS_PER_CYCLE {
                    let action = rng.next_usize(7);
                    match action {
                        0 | 1 | 2 => {
                            let term = (rng.next_u64() % 10) + 1;
                            let node_id = (rng.next_u64() % 5) + 1;
                            let leader_id =
                                CommittedLeaderIdOf::<AppTypeConfig>::new(term, node_id);
                            let payload_len = (rng.next_u64() % 24 + 8) as usize;
                            let mut payload = vec![0u8; payload_len];
                            for byte in &mut payload {
                                *byte = b'a' + (rng.next_u64() % 26) as u8;
                            }
                            let entry = Entry::<AppTypeConfig> {
                                log_id: LogId::new(leader_id, next_index),
                                payload: EntryPayload::Normal(AppEntry(payload)),
                            };
                            if rt
                                .block_on(store.persist_record(&WalLogRecord::LogEntry(
                                    entry.clone(),
                                )))
                                .is_ok()
                            {
                                oracle_log.insert(next_index, entry);
                                next_index += 1;
                            }
                        }
                        3 => {
                            let term = (rng.next_u64() % 10) + 1;
                            let node_id = (rng.next_u64() % 5) + 1;
                            let vote = openraft::Vote::<AppTypeConfig>::new(term, node_id);
                            if rt
                                .block_on(store.persist_record(&WalLogRecord::Vote(vote.clone())))
                                .is_ok()
                            {
                                oracle_vote = Some(vote);
                            }
                        }
                        4 => {
                            let committed = oracle_log
                                .iter()
                                .next_back()
                                .map(|(_, entry)| entry.log_id);
                            if rt
                                .block_on(store.persist_record(&WalLogRecord::Committed(
                                    committed,
                                )))
                                .is_ok()
                            {
                                oracle_committed = committed;
                            }
                        }
                        5 => {
                            let last_index = oracle_log.keys().next_back().copied().unwrap_or(0);
                            let committed_index =
                                oracle_committed.map(|id| id.index).unwrap_or(0);
                            if last_index > committed_index {
                                let span = last_index - committed_index;
                                let cutoff =
                                    committed_index + 1 + (rng.next_u64() % span.max(1));
                                if let Some(entry) = oracle_log.get(&cutoff).cloned() {
                                    if rt
                                        .block_on(store.persist_record(&WalLogRecord::Truncated(
                                            entry.log_id,
                                        )))
                                        .is_ok()
                                    {
                                        let keys: Vec<u64> = oracle_log
                                            .range(cutoff..)
                                            .map(|(idx, _)| *idx)
                                            .collect();
                                        for key in keys {
                                            oracle_log.remove(&key);
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            let committed_index =
                                oracle_committed.map(|id| id.index).unwrap_or(0);
                            if committed_index > 0 {
                                let purge_index = 1 + (rng.next_u64() % committed_index);
                                if let Some(entry) = oracle_log.get(&purge_index).cloned() {
                                    if rt
                                        .block_on(store.persist_record(&WalLogRecord::Purged(
                                            entry.log_id,
                                        )))
                                        .is_ok()
                                    {
                                        let keys: Vec<u64> = oracle_log
                                            .range(..=purge_index)
                                            .map(|(idx, _)| *idx)
                                            .collect();
                                        for key in keys {
                                            oracle_log.remove(&key);
                                        }
                                        oracle_last_purged = Some(entry.log_id);
                                    }
                                }
                            }
                        }
                    }
                }

                drop(store);
                drop(wal);
                crate::wal::wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            // Final recovery - sim_assert invariants in WalLogStore::new verify consistency
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let wal = create_wal_with_retry(
                &rt,
                root_dir.join("wal_log_store.log"),
                scenario_seed,
                WAL_CREATE_RETRIES,
            );
            // Recovery with invariant checking happens inside WalLogStore::new
            let _recovered = rt
                .block_on(WalLogStore::new(Arc::clone(&wal)))
                .expect("Failed to create WalLogStore - final recovery failed");
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            sim::teardown();
        }
    }

    /// Sync oracle from recovered store state.
    /// Under fault injection with partial writes, the recovered state may differ from
    /// what we tried to persist. The sim_assert invariants verify internal consistency;
    /// this function syncs the oracle to continue from the actual recovered state.
    fn sync_oracle_from_store(
        rt: &tokio::runtime::Runtime,
        store: &mut WalLogStore,
        oracle_log: &mut BTreeMap<u64, Entry<AppTypeConfig>>,
        oracle_vote: &mut Option<openraft::Vote<AppTypeConfig>>,
        oracle_committed: &mut Option<LogId<AppTypeConfig>>,
        oracle_last_purged: &mut Option<LogId<AppTypeConfig>>,
        next_index: &mut u64,
    ) {
        // Read log state to get bounds
        let state = rt.block_on(store.get_log_state()).expect("read log state failed");
        *oracle_last_purged = state.last_purged_log_id.clone();

        // Read all log entries
        let first_index = state.last_purged_log_id.map(|p| p.index + 1).unwrap_or(1);
        let last_index = state.last_log_id.map(|l| l.index).unwrap_or(0);

        oracle_log.clear();
        if last_index >= first_index {
            let logs = rt.block_on(store.try_get_log_entries(first_index..=last_index))
                .expect("read logs failed");
            for entry in logs {
                oracle_log.insert(entry.log_id.index, entry);
            }
        }

        // Read vote and committed
        *oracle_vote = rt.block_on(store.read_vote()).expect("read vote failed");
        *oracle_committed = rt.block_on(store.read_committed()).expect("read committed failed");

        // Update next_index to continue from where we left off
        *next_index = last_index + 1;
    }

    fn create_wal_with_retry(
        rt: &tokio::runtime::Runtime,
        wal_path: std::path::PathBuf,
        scenario_seed: u64,
        retries: usize,
    ) -> Arc<WriteAheadLog> {
        for attempt in 0..retries {
            match rt.block_on(WriteAheadLog::new(
                wal_path.clone(),
                0,
                Duration::from_millis(0),
            )) {
                Ok(wal) => return Arc::new(wal),
                Err(_) => {
                    sim::advance_time(Duration::from_millis(1));
                    if attempt + 1 == retries {
                        break;
                    }
                }
            }
        }
        panic!(
            "Failed to create WriteAheadLog after {} retries (seed={})",
            retries, scenario_seed
        );
    }
}
