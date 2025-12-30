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

/// In-memory state machine wrapper for OpenRaft.
///
/// **Important:** This state machine does NOT persist Raft metadata (`last_applied_log`,
/// `last_membership`) to WAL. The metadata is held purely in memory and will be lost
/// on process restart.
///
/// Durability depends entirely on the user's `StateMachine` implementation:
/// - The user's `StateMachine` must persist its own state via Walrus (e.g., `KvStateMachine::with_wal()`)
/// - On recovery, OpenRaft will replay committed entries to rebuild `last_applied_log`
/// - Snapshots are rebuilt from the `StateMachine::snapshot()` method
///
/// This design is intentional: the log store (`WalLogStore`) persists all committed entries,
/// so replaying them on startup reconstructs the correct state machine state. The trade-off
/// is slightly slower startup (replay cost) in exchange for simpler state machine code.
pub struct MemStateMachine {
    sm: StateMachine,
    state_machine: tokio::sync::RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: tokio::sync::RwLock<Option<StoredSnapshot>>,
}

impl MemStateMachine {
    pub fn new(sm: StateMachine) -> Arc<Self> {
        Arc::new(Self {
            sm,
            state_machine: tokio::sync::RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: tokio::sync::RwLock::new(None),
        })
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
                    AppResponse(Vec::new())
                }
            };

            if let Some(responder) = responder {
                responder.send(response);
            }
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

        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated_state_machine;

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        // Also restore into the state machine
        let snapshot_bytes = bincode::serialize(&updated_state_machine_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.sm
            .restore(&snapshot_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        *current_snapshot = Some(new_snapshot);
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
            crate::invariants::sim_assert(
                last_log_id.map_or(false, |last| committed <= last),
                "committed log id is after last log id",
            );
            if let Some(purged) = inner.last_purged_log_id.clone() {
                crate::invariants::sim_assert(
                    committed.index >= purged.index,
                    "committed log id is before last purged log id",
                );
            }
        }
    }

    async fn recover_from_wal(&self) -> Result<(), OctopiiError> {
        let entries = self.wal.read_all().await.unwrap_or_else(|_| Vec::new());
        let mut inner = self.inner.lock().await;
        Self::apply_wal_entries(&entries, &mut inner)?;
        Self::sim_assert_log_store_state(&inner);
        #[cfg(feature = "simulation")]
        {
            let snapshot = LogStoreSnapshot::from_inner(&inner);
            let mut fresh = MemLogStoreInner::default();
            Self::apply_wal_entries(&entries, &mut fresh)?;
            Self::sim_assert_log_store_state(&fresh);
            crate::invariants::sim_assert(
                snapshot == LogStoreSnapshot::from_inner(&fresh),
                "wal log store recovery not idempotent",
            );
        }
        Ok(())
    }

    fn apply_wal_entries(
        entries: &[Bytes],
        inner: &mut MemLogStoreInner,
    ) -> Result<(), OctopiiError> {
        for raw in entries {
            let record: WalLogRecord = bincode::deserialize(raw)
                .map_err(|e| OctopiiError::Wal(format!("Failed to deserialize WAL record: {e}")))?;
            match record {
                WalLogRecord::LogEntry(entry) => {
                    inner.log.insert(entry.log_id.index, entry);
                }
                WalLogRecord::Vote(vote) => {
                    inner.vote = Some(vote);
                }
                WalLogRecord::Committed(committed) => {
                    inner.committed = committed;
                }
                WalLogRecord::Purged(log_id) => {
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
        Ok(())
    }

    async fn persist_record(&self, record: &WalLogRecord) -> Result<(), io::Error> {
        let data =
            bincode::serialize(record).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.wal
            .append(Bytes::from(data))
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
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

                verify_wal_log_store(
                    &rt,
                    &mut store,
                    &oracle_log,
                    &oracle_vote,
                    &oracle_committed,
                    &oracle_last_purged,
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
            let mut recovered = rt
                .block_on(WalLogStore::new(Arc::clone(&wal)))
                .expect("Failed to create WalLogStore");

            verify_wal_log_store(
                &rt,
                &mut recovered,
                &oracle_log,
                &oracle_vote,
                &oracle_committed,
                &oracle_last_purged,
            );
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);

            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);
            let _ = vfs::remove_dir_all(&root_dir);
            sim::set_io_error_rate(prev_rate);
            sim::set_partial_writes_enabled(prev_partial);
            sim::teardown();
        }
    }

    fn verify_wal_log_store(
        rt: &tokio::runtime::Runtime,
        store: &mut WalLogStore,
        oracle_log: &BTreeMap<u64, Entry<AppTypeConfig>>,
        oracle_vote: &Option<openraft::Vote<AppTypeConfig>>,
        oracle_committed: &Option<LogId<AppTypeConfig>>,
        oracle_last_purged: &Option<LogId<AppTypeConfig>>,
    ) {
        let last_index = oracle_log.keys().next_back().copied().unwrap_or(0);
        let logs = if last_index == 0 {
            Vec::new()
        } else {
            rt.block_on(store.try_get_log_entries(1..=last_index))
                .expect("read logs failed")
        };
        let recovered_map: BTreeMap<u64, Entry<AppTypeConfig>> =
            logs.into_iter().map(|e| (e.log_id.index, e)).collect();
        assert_eq!(&recovered_map, oracle_log);

        let recovered_vote = rt.block_on(store.read_vote()).expect("read vote failed");
        assert_eq!(&recovered_vote, oracle_vote);

        let recovered_committed = rt
            .block_on(store.read_committed())
            .expect("read committed failed");
        assert_eq!(&recovered_committed, oracle_committed);

        let state = rt.block_on(store.get_log_state()).expect("read log state failed");
        let expected_last_log = oracle_log
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or_else(|| oracle_last_purged.clone());
        assert_eq!(state.last_purged_log_id, *oracle_last_purged);
        assert_eq!(state.last_log_id, expected_last_log);
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
