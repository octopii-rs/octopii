#![cfg(feature = "openraft")]

use crate::openraft::storage::wal::append_wal_record;
use crate::openraft::types::{AppResponse, AppTypeConfig};
use crate::state_machine::StateMachine;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use openraft::{
    alias::SnapshotDataOf,
    storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot},
    EntryPayload, LogId, OptionalSend, SnapshotMeta, StoredMembership,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, Cursor};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// In-memory snapshot wrapper.
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

/// WAL record for state machine metadata (membership and snapshot)
#[derive(Serialize, Deserialize)]
enum SmMetaRecord {
    /// Membership change: (log_id, membership)
    Membership {
        log_id: Option<LogId<AppTypeConfig>>,
        membership: openraft::Membership<AppTypeConfig>,
    },
    /// Snapshot data (meta + serialized state)
    Snapshot {
        meta: SnapshotMeta<AppTypeConfig>,
        data: Vec<u8>,
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
        let mut snapshot: Option<StoredSnapshot> = None;
        let mut snapshot_state: Option<BTreeMap<String, String>> = None;

        // Recover metadata from WAL
        if let Ok(entries) = wal.read_all().await {
            for raw in entries {
                if let Ok(record) = bincode::deserialize::<SmMetaRecord>(&raw) {
                    match record {
                        SmMetaRecord::Membership { log_id, membership } => {
                            data.last_membership = StoredMembership::new(log_id, membership);
                        }
                        SmMetaRecord::Snapshot {
                            meta,
                            data: snap_data,
                        } => {
                            if let Ok(restored) =
                                bincode::deserialize::<BTreeMap<String, String>>(&snap_data)
                            {
                                data.last_applied_log = meta.last_log_id;
                                data.last_membership = meta.last_membership.clone();
                                snapshot_state = Some(restored);
                                snapshot = Some(StoredSnapshot {
                                    meta,
                                    data: snap_data,
                                });
                            }
                        }
                    }
                }
            }
        }

        if let Some(state) = snapshot_state {
            if let Ok(snapshot_bytes) = bincode::serialize(&state) {
                if sm.restore(&snapshot_bytes).is_ok() {
                    data.data = state;
                }
            }
        }

        Arc::new(Self {
            sm,
            state_machine: tokio::sync::RwLock::new(data),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: tokio::sync::RwLock::new(snapshot),
            meta_wal: Some(wal),
        })
    }

    /// Persist a metadata record to WAL (if WAL is configured).
    async fn persist_meta(&self, record: &SmMetaRecord) -> io::Result<()> {
        if let Some(ref wal) = self.meta_wal {
            let data = bincode::serialize(record).map_err(io::Error::other)?;
            append_wal_record(wal, Bytes::from(data)).await?;
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

        self.persist_meta(&SmMetaRecord::Snapshot {
            meta: meta.clone(),
            data: data.clone(),
        })
        .await?;

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
        let mut membership_to_persist: Option<(
            Option<LogId<AppTypeConfig>>,
            openraft::Membership<AppTypeConfig>,
        )> = None;

        {
            let mut sm = self.state_machine.write().await;

            while let Some((entry, responder)) = entries.try_next().await? {
                sm.last_applied_log = Some(entry.log_id);

                let response = match entry.payload {
                    EntryPayload::Blank => AppResponse(Vec::new()),
                    EntryPayload::Normal(ref data) => {
                        let result = self.sm.apply(&data.0).map_err(io::Error::other)?;
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
            self.persist_meta(&SmMetaRecord::Membership { log_id, membership })
                .await?;
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
        let membership_log_id = *meta.last_membership.log_id();

        {
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        let mut current_snapshot = self.current_snapshot.write().await;

        // Also restore into the state machine
        let snapshot_bytes = bincode::serialize(&updated_state_machine_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.sm.restore(&snapshot_bytes).map_err(io::Error::other)?;

        *current_snapshot = Some(new_snapshot);
        drop(current_snapshot);

        // Persist membership from snapshot
        self.persist_meta(&SmMetaRecord::Membership {
            log_id: membership_log_id,
            membership: membership_to_persist,
        })
        .await?;

        self.persist_meta(&SmMetaRecord::Snapshot {
            meta: meta.clone(),
            data: snapshot_bytes,
        })
        .await?;

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
