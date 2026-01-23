#![cfg(feature = "openraft")]

use crate::openraft::types::AppTypeConfig;
use openraft::{
    storage::{IOFlushed, LogState, RaftLogStorage},
    Entry, LogId, OptionalSend, RaftLogReader,
};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct MemLogStore {
    pub(crate) inner: Arc<tokio::sync::Mutex<MemLogStoreInner>>,
}

#[derive(Debug)]
pub(crate) struct MemLogStoreInner {
    pub(crate) last_purged_log_id: Option<LogId<AppTypeConfig>>,
    pub(crate) log: BTreeMap<u64, Entry<AppTypeConfig>>,
    pub(crate) committed: Option<LogId<AppTypeConfig>>,
    pub(crate) vote: Option<openraft::Vote<AppTypeConfig>>,
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
    pub(crate) fn remove_through(&mut self, end: u64) {
        let keys = self
            .log
            .range(..=end)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
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

    pub(crate) async fn save_committed(
        &mut self,
        committed: Option<LogId<AppTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.committed = committed;
        Ok(())
    }

    pub(crate) async fn read_committed(&mut self) -> Result<Option<LogId<AppTypeConfig>>, io::Error> {
        Ok(self.committed.clone())
    }

    pub(crate) async fn save_vote(&mut self, vote: &openraft::Vote<AppTypeConfig>) -> Result<(), io::Error> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    pub(crate) async fn read_vote(&mut self) -> Result<Option<openraft::Vote<AppTypeConfig>>, io::Error> {
        Ok(self.vote.clone())
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
            *ld = Some(log_id.clone());
        }

        {
            self.remove_through(log_id.index);
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
