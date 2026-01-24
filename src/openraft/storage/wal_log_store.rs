#![cfg(feature = "openraft")]

use crate::error::OctopiiError;
use crate::invariants;
use crate::openraft::types::AppTypeConfig;
#[cfg(feature = "simulation")]
use crate::sim_runtime;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use openraft::{
    storage::{IOFlushed, LogState, RaftLogStorage},
    Entry, LogId, OptionalSend, RaftLogReader,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
#[cfg(feature = "simulation")]
use std::time::Duration;
#[cfg(feature = "simulation")]
use tokio::task::yield_now;

#[derive(Clone)]
pub struct WalLogStore {
    pub(crate) inner: Arc<tokio::sync::Mutex<MemLogStoreInner>>,
    pub(crate) wal: Arc<WriteAheadLog>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum WalLogRecord {
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
        Self::check_log_invariants(inner);
        Self::check_committed_invariants(inner);
    }

    fn check_log_invariants(inner: &MemLogStoreInner) {
        let last_log_id = inner.log.iter().next_back().map(|(_, entry)| entry.log_id);

        if let Some(purged) = inner.last_purged_log_id {
            invariants::sim_assert(
                last_log_id.is_none_or(|last| purged <= last),
                "last purged log id is after last log id",
            );
            for (idx, entry) in inner.log.iter() {
                invariants::sim_assert(
                    entry.log_id.index == *idx,
                    "log entry index mismatches map key",
                );
                invariants::sim_assert(
                    *idx > purged.index,
                    "log entry is not strictly after last purged log id",
                );
            }
            if let Some(first_idx) = inner.log.keys().next().copied() {
                invariants::sim_assert(
                    first_idx == purged.index + 1,
                    "first log entry doesn't immediately follow purged index",
                );
            }
        } else {
            for (idx, entry) in inner.log.iter() {
                invariants::sim_assert(
                    entry.log_id.index == *idx,
                    "log entry index mismatches map key",
                );
            }
        }

        if let (Some(first_idx), Some(last_idx)) = (
            inner.log.keys().next().copied(),
            inner.log.keys().next_back().copied(),
        ) {
            let expected_count = (last_idx - first_idx + 1) as usize;
            invariants::sim_assert(
                inner.log.len() == expected_count,
                "log has gaps between first and last entry",
            );
        }
    }

    fn check_committed_invariants(inner: &MemLogStoreInner) {
        if let Some(committed) = inner.committed {
            let last_log_id = inner.log.iter().next_back().map(|(_, entry)| entry.log_id);

            let committed_valid = match last_log_id {
                Some(last) => committed <= last,
                None => inner
                    .last_purged_log_id
                    .map(|p| committed <= p)
                    .unwrap_or(false),
            };
            invariants::sim_assert(
                committed_valid,
                "committed log id is after last log/purged id",
            );

            if let Some(purged) = inner.last_purged_log_id {
                invariants::sim_assert(
                    committed.index >= purged.index,
                    "committed log id is before last purged log id",
                );
            }

            let in_log = inner.log.contains_key(&committed.index);
            let is_purged = inner
                .last_purged_log_id
                .map(|p| committed.index <= p.index)
                .unwrap_or(false);
            invariants::sim_assert(
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
            invariants::sim_assert(
                snapshot == LogStoreSnapshot::from_inner(&fresh),
                "wal log store recovery not idempotent",
            );
        }
        Ok(())
    }

    fn repair_state_after_recovery(inner: &mut MemLogStoreInner) {
        Self::drop_entries_before_purge(inner);
        Self::truncate_at_first_gap(inner);
        Self::drop_log_if_purge_inconsistent(inner);
        Self::repair_committed_after_recovery(inner);
    }

    fn drop_entries_before_purge(inner: &mut MemLogStoreInner) {
        if let Some(ref purged) = inner.last_purged_log_id {
            inner.remove_through(purged.index);
        }
    }

    fn truncate_at_first_gap(inner: &mut MemLogStoreInner) {
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
                    if idx == expected_first {
                        last_valid_idx = Some(idx);
                    } else {
                        break;
                    }
                }
                Some(last) => {
                    if idx == last + 1 {
                        last_valid_idx = Some(idx);
                    } else {
                        break;
                    }
                }
            }
        }

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
    }

    fn drop_log_if_purge_inconsistent(inner: &mut MemLogStoreInner) {
        let last_log_id = inner.log.values().next_back().map(|e| e.log_id);
        if let Some(ref purged) = inner.last_purged_log_id {
            let valid = last_log_id.map(|last| *purged <= last).unwrap_or(true);
            if !valid {
                inner.log.clear();
            }
        }
    }

    fn repair_committed_after_recovery(inner: &mut MemLogStoreInner) {
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

            if (!in_log && !is_purged) || !valid_logid {
                inner.committed = last_log_id.or(inner.last_purged_log_id);
            }
        }
    }

    fn apply_wal_entries(
        entries: &[Bytes],
        inner: &mut MemLogStoreInner,
    ) -> Result<(), OctopiiError> {
        let mut stats = WalReplayStats::default();
        for raw in entries {
            let record: WalLogRecord = bincode::deserialize(raw)
                .map_err(|e| OctopiiError::Wal(format!("Failed to deserialize WAL record: {e}")))?;
            Self::apply_wal_record(inner, record, &mut stats);
        }
        stats.maybe_log();
        Ok(())
    }

    fn apply_wal_record(
        inner: &mut MemLogStoreInner,
        record: WalLogRecord,
        stats: &mut WalReplayStats,
    ) {
        match record {
            WalLogRecord::LogEntry(entry) => {
                stats.log_entry += 1;
                inner.log.insert(entry.log_id.index, entry);
            }
            WalLogRecord::Vote(vote) => {
                stats.vote += 1;
                inner.vote = Some(vote);
            }
            WalLogRecord::Committed(committed) => {
                stats.committed += 1;
                inner.committed = committed;
            }
            WalLogRecord::Purged(log_id) => {
                stats.purged += 1;
                inner.remove_through(log_id.index);
                inner.last_purged_log_id = Some(log_id);
            }
            WalLogRecord::Truncated(log_id) => {
                stats.truncated += 1;
                inner.remove_from(log_id.index);
            }
        }
    }

    pub(crate) async fn persist_record(&self, record: &WalLogRecord) -> Result<(), io::Error> {
        let data = bincode::serialize(record).map_err(io::Error::other)?;
        append_wal_record(&self.wal, Bytes::from(data)).await
    }
}

#[derive(Default)]
struct WalReplayStats {
    log_entry: usize,
    vote: usize,
    committed: usize,
    purged: usize,
    truncated: usize,
}

impl WalReplayStats {
    fn maybe_log(&self) {
        if cfg!(feature = "simulation")
            && std::env::var("CLUSTER_DEBUG").ok().as_deref() == Some("1")
        {
            eprintln!(
                "[cluster_debug] wal_log_store apply counts: log_entry={} vote={} committed={} purged={} truncated={}",
                self.log_entry, self.vote, self.committed, self.purged, self.truncated
            );
        }
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
            last_purged_log_id: inner.last_purged_log_id,
            log: inner.log.clone(),
            committed: inner.committed,
            vote: inner.vote,
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
            inner.vote = Some(*vote);
            Self::sim_assert_log_store_state(&inner);
        }
        self.persist_record(&WalLogRecord::Vote(*vote)).await
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

#[derive(Debug, Default)]
pub(crate) struct MemLogStoreInner {
    pub(crate) last_purged_log_id: Option<LogId<AppTypeConfig>>,
    pub(crate) log: BTreeMap<u64, Entry<AppTypeConfig>>,
    pub(crate) committed: Option<LogId<AppTypeConfig>>,
    pub(crate) vote: Option<openraft::Vote<AppTypeConfig>>,
}

impl MemLogStoreInner {
    pub(crate) fn remove_through(&mut self, end: u64) {
        let keys = self.log.range(..=end).map(|(k, _v)| *k).collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
    }

    pub(crate) fn remove_from(&mut self, start: u64) {
        let keys = self
            .log
            .range(start..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }
    }

    pub(crate) async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
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

    pub(crate) async fn get_log_state(&mut self) -> Result<LogState<AppTypeConfig>, io::Error> {
        let last = self.log.iter().next_back().map(|(_, ent)| ent.log_id);

        let last_purged = self.last_purged_log_id;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    pub(crate) async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.committed = committed;
        Ok(())
    }

    pub(crate) async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        Ok(self.committed)
    }

    pub(crate) async fn save_vote(
        &mut self,
        vote: &openraft::Vote<AppTypeConfig>,
    ) -> Result<(), io::Error> {
        self.vote = Some(*vote);
        Ok(())
    }

    pub(crate) async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        Ok(self.vote)
    }

    pub(crate) async fn append<I>(
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

    pub(crate) async fn truncate(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        self.remove_from(log_id.index);
        Ok(())
    }

    pub(crate) async fn purge(&mut self, log_id: LogId<AppTypeConfig>) -> Result<(), io::Error> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id);
        }

        {
            self.remove_through(log_id.index);
        }

        Ok(())
    }
}

pub(crate) async fn append_wal_record(wal: &WriteAheadLog, data: Bytes) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        for attempt in 0..20 {
            match wal.append(data.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt == 19 {
                        return Err(io::Error::other(e.to_string()));
                    }
                    sim_runtime::advance_time(Duration::from_millis(10));
                    yield_now().await;
                }
            }
        }
        Ok(())
    }
    #[cfg(not(feature = "simulation"))]
    {
        wal.append(data)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        Ok(())
    }
}

pub async fn new_wal_log_store(wal: Arc<WriteAheadLog>) -> Result<WalLogStore, OctopiiError> {
    WalLogStore::new(wal).await
}
