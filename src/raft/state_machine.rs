use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const TOPIC_STATE_MACHINE: &str = "state_machine";
const TOPIC_STATE_MACHINE_SNAPSHOT: &str = "state_machine_snapshot";

// State machine compaction threshold: checkpoint after this many operations
const STATE_MACHINE_COMPACTION_THRESHOLD: usize = 5000;

// Serializable types for state machine data
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct StateMachineEntry {
    key: String,
    value: Vec<u8>,
}

// Serializable snapshot type
#[derive(Archive, Deserialize, Serialize, Debug, Clone)]

struct StateMachineSnapshot {
    entries: Vec<(String, Vec<u8>)>,
}

/// Simple key-value state machine for demonstration (NOW DURABLE!)
pub struct StateMachine {
    data: RwLock<HashMap<String, Bytes>>,
    wal: Option<Arc<crate::wal::WriteAheadLog>>,
    // Track operations since last compaction
    ops_since_compaction: RwLock<usize>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            wal: None,
            ops_since_compaction: RwLock::new(0),
        }
    }

    /// Create state machine with Walrus backing and recovery
    pub fn with_wal(wal: Arc<crate::wal::WriteAheadLog>) -> Self {
        let sm = Self {
            data: RwLock::new(HashMap::new()),
            wal: Some(wal),
            ops_since_compaction: RwLock::new(0),
        };

        // Recover state from Walrus
        sm.recover_from_walrus();
        sm
    }

    fn recover_from_walrus(&self) {
        if let Some(wal) = &self.wal {
            tracing::info!("Starting state machine recovery from Walrus...");
            let mut recovered = HashMap::new();

            // Step 1: Check for latest snapshot first
            let mut snapshot_found = false;
            loop {
                match wal.walrus.read_next(TOPIC_STATE_MACHINE_SNAPSHOT, true) {
                    Ok(Some(entry)) => {
                        // Deserialize snapshot with rkyv
                        let archived = unsafe {
                            rkyv::archived_root::<StateMachineSnapshot>(&entry.data)
                        };
                        let snapshot: Result<StateMachineSnapshot, _> = archived.deserialize(&mut rkyv::Infallible);
                        let snapshot = snapshot.expect("Failed to deserialize snapshot");
                        // Load snapshot into recovered map
                        recovered.clear();
                        for (key, value) in snapshot.entries {
                            recovered.insert(key, Bytes::from(value));
                        }
                        snapshot_found = true;
                        tracing::info!("✓ Loaded state machine snapshot with {} entries", recovered.len());
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            // Step 2: Replay operations since snapshot (or all if no snapshot)
            let mut op_count = 0;
            loop {
                match wal.walrus.read_next(TOPIC_STATE_MACHINE, true) {
                    Ok(Some(entry)) => {
                        op_count += 1;
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

            if snapshot_found {
                tracing::info!("✓ Recovery complete: snapshot + {} operations = {} entries",
                    op_count, recovered.len());
            } else if !recovered.is_empty() {
                tracing::info!("✓ Recovered {} entries from {} operations", recovered.len(), op_count);
            } else {
                tracing::info!("No state machine entries to recover");
            }

            if !recovered.is_empty() {
                *self.data.write().unwrap() = recovered;
            }

            // Reset compaction counter
            *self.ops_since_compaction.write().unwrap() = 0;
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

                // Increment ops counter for compaction tracking
                *self.ops_since_compaction.write().unwrap() += 1;

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

                // Increment ops counter for compaction tracking
                *self.ops_since_compaction.write().unwrap() += 1;

                Ok(Bytes::from("OK"))
            }
            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
    }

    /// Compact state machine by writing snapshot and checkpointing old operations
    /// This prevents unbounded WAL growth and enables Walrus space reclamation
    pub fn compact_state_machine(&self) -> Result<(), String> {
        let ops_count = *self.ops_since_compaction.read().unwrap();

        if ops_count < STATE_MACHINE_COMPACTION_THRESHOLD {
            // Not enough operations accumulated, skip compaction
            return Ok(());
        }

        if let Some(wal) = &self.wal {
            tracing::info!("State machine compaction triggered: {} operations since last snapshot (threshold: {})",
                ops_count, STATE_MACHINE_COMPACTION_THRESHOLD);

            // Get current state
            let data = self.data.read().unwrap();
            let snapshot = StateMachineSnapshot {
                entries: data.iter()
                    .map(|(k, v)| (k.clone(), v.to_vec()))
                    .collect(),
            };

            // Serialize and persist snapshot
            let bytes = rkyv::to_bytes::<_, 4096>(&snapshot)
                .map_err(|e| format!("Snapshot serialization failed: {:?}", e))?;

            tokio::task::block_in_place(|| {
                wal.walrus.append_for_topic(TOPIC_STATE_MACHINE_SNAPSHOT, &bytes)
            })
            .map_err(|e| format!("Failed to persist state machine snapshot: {}", e))?;

            tracing::info!("✓ State machine compaction complete: snapshot with {} entries persisted",
                snapshot.entries.len());

            // Reset counter
            *self.ops_since_compaction.write().unwrap() = 0;

            // Note: Old operations are automatically reclaimable by Walrus
            // because we use read_next(TOPIC_STATE_MACHINE, true) during recovery,
            // which checkpoints the cursor. Next recovery loads snapshot first,
            // then only replays operations since snapshot.

            Ok(())
        } else {
            // No WAL, skip compaction
            Ok(())
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
