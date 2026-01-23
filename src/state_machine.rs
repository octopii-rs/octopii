use crate::invariants::sim_assert;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const TOPIC_STATE_MACHINE: &str = "state_machine";
const TOPIC_STATE_MACHINE_SNAPSHOT: &str = "state_machine_snapshot";
const STATE_MACHINE_COMPACTION_THRESHOLD: usize = 5000;

pub(crate) enum KvCommand<'a> {
    Set { key: &'a str, value: &'a str },
    Get { key: &'a str },
    Delete { key: &'a str },
}

pub(crate) fn parse_kv_command(command: &str) -> std::result::Result<KvCommand<'_>, String> {
    let mut tokens = command.split_whitespace();
    let op = tokens.next().unwrap_or("");
    match op {
        "SET" => {
            let key = tokens.next().ok_or_else(|| "SET missing key".to_string())?;
            let val = tokens
                .next()
                .ok_or_else(|| "SET missing value".to_string())?;
            Ok(KvCommand::Set { key, value: val })
        }
        "GET" => {
            let key = tokens.next().ok_or_else(|| "GET missing key".to_string())?;
            Ok(KvCommand::Get { key })
        }
        "DELETE" => {
            let key = tokens
                .next()
                .ok_or_else(|| "DELETE missing key".to_string())?;
            Ok(KvCommand::Delete { key })
        }
        _ => Err("unknown op".into()),
    }
}

/// Trait for application state machines.
pub trait StateMachineTrait: Send + Sync {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String>;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&self, data: &[u8]) -> std::result::Result<(), String>;
    fn compact(&self) -> std::result::Result<(), String> {
        Ok(())
    }
}

/// Shared state machine handle type.
pub type StateMachine = Arc<dyn StateMachineTrait>;

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
struct StateMachineEntry {
    key: String,
    value: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
struct StateMachineSnapshot {
    entries: Vec<(String, Vec<u8>)>,
}

/// Replay WAL to recover state machine map. Resets read offsets before replay.
fn replay_wal_to_map(wal: &Arc<WriteAheadLog>) -> HashMap<String, Bytes> {
    let walrus = &wal.walrus;
    let mut recovered = HashMap::new();

    let _ = walrus.reset_read_offset_for_topic(TOPIC_STATE_MACHINE_SNAPSHOT);
    let _ = walrus.reset_read_offset_for_topic(TOPIC_STATE_MACHINE);

    // Replay snapshots first
    loop {
        match walrus.read_next(TOPIC_STATE_MACHINE_SNAPSHOT, true) {
            Ok(Some(entry)) => {
                let archived =
                    unsafe { rkyv::archived_root::<StateMachineSnapshot>(&entry.data) };
                let snapshot: StateMachineSnapshot =
                    match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                recovered.clear();
                for (key, value) in snapshot.entries {
                    recovered.insert(key, Bytes::from(value));
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Replay individual entries
    loop {
        match walrus.read_next(TOPIC_STATE_MACHINE, true) {
            Ok(Some(entry)) => {
                let archived =
                    unsafe { rkyv::archived_root::<StateMachineEntry>(&entry.data) };
                let sm_entry: StateMachineEntry =
                    match archived.deserialize(&mut rkyv::Infallible) {
                        Ok(d) => d,
                        Err(_) => break,
                    };

                if sm_entry.value.is_empty() {
                    recovered.remove(&sm_entry.key);
                } else {
                    recovered.insert(sm_entry.key, Bytes::from(sm_entry.value));
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    recovered
}

/// Simple key-value state machine implementation (durable when WAL is provided).
pub struct KvStateMachine {
    data: RwLock<HashMap<String, Bytes>>,
    wal: Option<Arc<WriteAheadLog>>,
    ops_since_compaction: RwLock<usize>,
}

impl KvStateMachine {
    pub fn in_memory() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            wal: None,
            ops_since_compaction: RwLock::new(0),
        }
    }

    pub fn with_wal(wal: Arc<WriteAheadLog>) -> Self {
        let sm = Self {
            data: RwLock::new(HashMap::new()),
            wal: Some(wal),
            ops_since_compaction: RwLock::new(0),
        };

        sm.recover_from_wal();
        sm
    }

    fn recover_from_wal(&self) {
        if let Some(wal) = &self.wal {
            let recovered = replay_wal_to_map(wal);

            #[cfg(feature = "simulation")]
            {
                // Verify idempotency: replay again and check result matches
                let verify = replay_wal_to_map(wal);
                sim_assert(
                    verify == recovered,
                    "state machine recovery not idempotent across replay",
                );
            }

            if !recovered.is_empty() {
                *self.data.write().unwrap() = recovered;
            }

            *self.ops_since_compaction.write().unwrap() = 0;
        }
    }

    pub fn apply_kv(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd_str =
            String::from_utf8(command.to_vec()).map_err(|e| format!("Invalid UTF-8: {}", e))?;

        let parsed = match parse_kv_command(&cmd_str) {
            Ok(cmd) => cmd,
            Err(_) => return Err(format!("Unknown command: {}", cmd_str)),
        };

        match parsed {
            KvCommand::Set { key, value } => {
                let key_str = key.to_string();
                let value_bytes = Bytes::from(value.to_string());

                if let Some(wal) = &self.wal {
                    let sm_entry = StateMachineEntry {
                        key: key_str.clone(),
                        value: value_bytes.to_vec(),
                    };

                    let bytes = rkyv::to_bytes::<_, 256>(&sm_entry)
                        .map_err(|e| format!("Serialization failed: {:?}", e))?;

                    tokio::task::block_in_place(|| {
                        wal.walrus.append_for_topic(TOPIC_STATE_MACHINE, &bytes)
                    })
                    .map_err(|e| format!("WAL append failed: {}", e))?;
                }

                let mut data = self.data.write().unwrap();
                data.insert(key_str, value_bytes);
                *self.ops_since_compaction.write().unwrap() += 1;

                Ok(Bytes::from("OK"))
            }
            KvCommand::Get { key } => {
                let data = self.data.read().unwrap();
                match data.get(key) {
                    Some(value) => Ok(value.clone()),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            KvCommand::Delete { key } => {
                if let Some(wal) = &self.wal {
                    let sm_entry = StateMachineEntry {
                        key: key.to_string(),
                        value: Vec::new(),
                    };

                    let bytes = rkyv::to_bytes::<_, 256>(&sm_entry)
                        .map_err(|e| format!("Serialization failed: {:?}", e))?;

                    tokio::task::block_in_place(|| {
                        wal.walrus.append_for_topic(TOPIC_STATE_MACHINE, &bytes)
                    })
                    .map_err(|e| format!("WAL append failed: {}", e))?;
                }

                let mut data = self.data.write().unwrap();
                data.remove(key);
                *self.ops_since_compaction.write().unwrap() += 1;

                Ok(Bytes::from("OK"))
            }
        }
    }

    pub fn compact_state_machine(&self) -> Result<(), String> {
        let ops_count = *self.ops_since_compaction.read().unwrap();

        if ops_count < STATE_MACHINE_COMPACTION_THRESHOLD {
            return Ok(());
        }

        if let Some(wal) = &self.wal {
            let data = self.data.read().unwrap();
            let snapshot = StateMachineSnapshot {
                entries: data.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect(),
            };

            let bytes = rkyv::to_bytes::<_, 4096>(&snapshot)
                .map_err(|e| format!("Snapshot serialization failed: {:?}", e))?;

            tokio::task::block_in_place(|| {
                wal.walrus
                    .append_for_topic(TOPIC_STATE_MACHINE_SNAPSHOT, &bytes)
            })
            .map_err(|e| format!("Failed to persist state machine snapshot: {}", e))?;

            *self.ops_since_compaction.write().unwrap() = 0;
        }

        Ok(())
    }

    pub fn snapshot_hashmap(&self) -> HashMap<String, Bytes> {
        let data = self.data.read().unwrap();
        data.clone()
    }

    pub fn restore_hashmap(&self, snapshot: HashMap<String, Bytes>) {
        let mut data = self.data.write().unwrap();
        *data = snapshot;
    }
}

impl Default for KvStateMachine {
    fn default() -> Self {
        Self::in_memory()
    }
}

impl StateMachineTrait for KvStateMachine {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String> {
        self.apply_kv(command)
    }

    fn snapshot(&self) -> Vec<u8> {
        let data = self.data.read().unwrap();
        let snapshot = StateMachineSnapshot {
            entries: data.iter().map(|(k, v)| (k.clone(), v.to_vec())).collect(),
        };

        rkyv::to_bytes::<_, 4096>(&snapshot)
            .map(|bytes| bytes.to_vec())
            .unwrap_or_default()
    }

    fn restore(&self, snapshot: &[u8]) -> std::result::Result<(), String> {
        if snapshot.is_empty() {
            return Ok(());
        }

        let archived = unsafe { rkyv::archived_root::<StateMachineSnapshot>(snapshot) };
        let snapshot: StateMachineSnapshot = archived
            .deserialize(&mut rkyv::Infallible)
            .map_err(|e| format!("Failed to deserialize snapshot: {:?}", e))?;

        let mut data = self.data.write().unwrap();
        data.clear();
        for (key, value) in snapshot.entries {
            data.insert(key, Bytes::from(value));
        }

        Ok(())
    }

    fn compact(&self) -> std::result::Result<(), String> {
        self.compact_state_machine()
    }
}

/// WAL-backed wrapper that replays commands on startup and durably appends writes.
pub struct WalBackedStateMachine {
    inner: StateMachine,
    wal: Arc<WriteAheadLog>,
}

impl WalBackedStateMachine {
    pub fn with_inner(inner: StateMachine, wal: Arc<WriteAheadLog>) -> Arc<Self> {
        let sm = Arc::new(Self { inner, wal });
        Self::replay_wal(&sm);
        sm
    }

    fn replay_wal(this: &Arc<Self>) {
        let wal = Arc::clone(&this.wal);
        let inner = Arc::clone(&this.inner);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                if let Ok(entries) = wal.read_all().await {
                    #[cfg(feature = "simulation")]
                    let pre_snapshot = inner.snapshot();
                    #[cfg(feature = "simulation")]
                    let replay_entries = entries.clone();
                    for entry in &entries {
                        let result = inner.apply(entry);
                        sim_assert(result.is_ok(), "wal replay apply failed");
                    }
                    #[cfg(feature = "simulation")]
                    {
                        let post_snapshot = inner.snapshot();
                        let restore_result = inner.restore(&pre_snapshot);
                        sim_assert(restore_result.is_ok(), "wal replay restore failed");
                        for entry in &replay_entries {
                            let result = inner.apply(entry);
                            sim_assert(
                                result.is_ok(),
                                "wal replay apply failed on second pass",
                            );
                        }
                        let post_snapshot_2 = inner.snapshot();
                        sim_assert(
                            post_snapshot_2 == post_snapshot,
                            "wal replay not idempotent across repeated recovery",
                        );
                        let restore_post = inner.restore(&post_snapshot);
                        sim_assert(
                            restore_post.is_ok(),
                            "wal replay restore to post state failed",
                        );
                    }
                }
            })
        });
    }

    fn append_entry(&self, command: &[u8]) -> std::result::Result<(), String> {
        let data = Bytes::copy_from_slice(command);
        let wal = Arc::clone(&self.wal);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                wal.append(data)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        })
    }
}

impl StateMachineTrait for WalBackedStateMachine {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String> {
        self.append_entry(command)?;
        let result = self.inner.apply(command);
        sim_assert(result.is_ok(), "state machine apply failed");
        result
    }

    fn snapshot(&self) -> Vec<u8> {
        self.inner.snapshot()
    }

    fn restore(&self, data: &[u8]) -> std::result::Result<(), String> {
        self.inner.restore(data)
    }

    fn compact(&self) -> std::result::Result<(), String> {
        self.inner.compact()
    }
}
