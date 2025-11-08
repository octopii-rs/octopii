use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const TOPIC_STATE_MACHINE: &str = "state_machine";

// Serializable types for state machine data
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct StateMachineEntry {
    key: String,
    value: Vec<u8>,
}

/// Simple key-value state machine for demonstration (NOW DURABLE!)
pub struct StateMachine {
    data: RwLock<HashMap<String, Bytes>>,
    wal: Option<Arc<crate::wal::WriteAheadLog>>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            wal: None,
        }
    }

    /// Create state machine with Walrus backing and recovery
    pub fn with_wal(wal: Arc<crate::wal::WriteAheadLog>) -> Self {
        let sm = Self {
            data: RwLock::new(HashMap::new()),
            wal: Some(wal),
        };

        // Recover state from Walrus
        sm.recover_from_walrus();
        sm
    }

    fn recover_from_walrus(&self) {
        if let Some(wal) = &self.wal {
            tracing::info!("Starting state machine recovery from Walrus...");
            let mut recovered = HashMap::new();
            let mut entry_count = 0;

            // Replay all operations WITH checkpointing to rebuild state
            loop {
                match wal.walrus.read_next(TOPIC_STATE_MACHINE, true) {
                    Ok(Some(entry)) => {
                        entry_count += 1;
                        // Zero-copy deserialize with rkyv
                        let archived = unsafe {
                            rkyv::archived_root::<StateMachineEntry>(&entry.data)
                        };
                        let sm_entry: StateMachineEntry = match archived.deserialize(&mut rkyv::Infallible) {
                            Ok(d) => d,
                            Err(_) => {
                                tracing::warn!("Failed to deserialize state machine entry");
                                break;
                            }
                        };

                        if sm_entry.value.is_empty() {
                            // Tombstone - delete the key
                            recovered.remove(&sm_entry.key);
                        } else {
                            // Normal entry - insert/update
                            recovered.insert(sm_entry.key, Bytes::from(sm_entry.value));
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // TODO: Implement compaction for space efficiency
            // Periodically write current state as snapshot, then start fresh topic
            // This would allow reclaiming space from intermediate operations

            if !recovered.is_empty() {
                *self.data.write().unwrap() = recovered.clone();
                tracing::info!("✓ Recovered {} state machine entries from {} operations", recovered.len(), entry_count);
            } else {
                tracing::info!("No state machine entries to recover");
            }
        }
    }

    /// Apply a command to the state machine (NOW DURABLE if WAL is set!)
    pub fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Simple command format: "SET key value" or "GET key"
        let cmd_str = String::from_utf8(command.to_vec())
            .map_err(|e| format!("Invalid UTF-8: {}", e))?;

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();

        match parts.as_slice() {
            ["SET", key, value] => {
                let key_str = key.to_string();
                let value_bytes = Bytes::from(value.to_string());

                // Persist to Walrus BEFORE updating in-memory state
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

                    tracing::trace!("✓ Persisted SET {} = {}", key_str, value);
                }

                // Now safe to update in-memory
                let mut data = self.data.write().unwrap();
                data.insert(key_str, value_bytes);

                Ok(Bytes::from("OK"))
            }
            ["GET", key] => {
                let data = self.data.read().unwrap();
                match data.get(*key) {
                    Some(value) => Ok(value.clone()),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            ["DELETE", key] => {
                // Persist tombstone to Walrus
                if let Some(wal) = &self.wal {
                    let sm_entry = StateMachineEntry {
                        key: key.to_string(),
                        value: vec![], // Empty = tombstone
                    };

                    let bytes = rkyv::to_bytes::<_, 256>(&sm_entry)
                        .map_err(|e| format!("Serialization failed: {:?}", e))?;

                    tokio::task::block_in_place(|| {
                        wal.walrus.append_for_topic(TOPIC_STATE_MACHINE, &bytes)
                    })
                    .map_err(|e| format!("WAL append failed: {}", e))?;

                    tracing::trace!("✓ Persisted DELETE {}", key);
                }

                let mut data = self.data.write().unwrap();
                data.remove(*key);

                Ok(Bytes::from("OK"))
            }
            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
    }

    /// Get a snapshot of the current state
    pub fn snapshot(&self) -> HashMap<String, Bytes> {
        let data = self.data.read().unwrap();
        data.clone()
    }

    /// Restore from a snapshot
    pub fn restore(&self, snapshot: HashMap<String, Bytes>) {
        let mut data = self.data.write().unwrap();
        *data = snapshot;
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}
