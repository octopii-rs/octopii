use crate::wal::WriteAheadLog;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

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

/// A minimal in-memory KV state machine used for tests and OpenRaft bootstrapping.
pub struct KvStateMachine {
    map: StdMutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl KvStateMachine {
    pub fn in_memory() -> Self {
        Self {
            map: StdMutex::new(HashMap::new()),
        }
    }
}

impl StateMachineTrait for KvStateMachine {
    fn apply(&self, command: &[u8]) -> std::result::Result<Bytes, String> {
        // Protocol: "SET key value" | "GET key" | "DELETE key"
        let s = std::str::from_utf8(command).map_err(|e| e.to_string())?;
        let mut tokens = s.split_whitespace();
        let op = tokens.next().unwrap_or("");
        match op {
            "SET" => {
                let key = tokens.next().ok_or_else(|| "SET missing key".to_string())?;
                let val = tokens
                    .next()
                    .ok_or_else(|| "SET missing value".to_string())?;
                self.map
                    .lock()
                    .unwrap()
                    .insert(key.as_bytes().to_vec(), val.as_bytes().to_vec());
                Ok(Bytes::from("OK"))
            }
            "GET" => {
                let key = tokens.next().ok_or_else(|| "GET missing key".to_string())?;
                let val_opt = self.map.lock().unwrap().get(key.as_bytes()).cloned();
                match val_opt {
                    Some(v) => Ok(Bytes::from(v)),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            "DELETE" => {
                let key = tokens
                    .next()
                    .ok_or_else(|| "DELETE missing key".to_string())?;
                self.map.lock().unwrap().remove(key.as_bytes());
                Ok(Bytes::from("OK"))
            }
            _ => Err("unknown op".into()),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        // naive bincode for tests
        bincode::serialize(&*self.map.lock().unwrap()).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> std::result::Result<(), String> {
        let map: HashMap<Vec<u8>, Vec<u8>> =
            bincode::deserialize(data).map_err(|e| e.to_string())?;
        *self.map.lock().unwrap() = map;
        Ok(())
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
                    for entry in entries {
                        let _ = inner.apply(&entry);
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
        self.inner.apply(command)
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

#[cfg(all(test, feature = "simulation"))]
mod tests {
    use super::*;
    use crate::wal::wal::vfs::sim::{self, SimConfig};
    use crate::wal::WriteAheadLog;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::runtime::Builder;
    use std::time::Duration;

    #[test]
    fn strictly_at_once_wal_backed_state_machine_recovery() {
        const SCENARIOS: usize = 10;
        const CRASH_CYCLES: usize = 4;
        const OPS_PER_CYCLE: usize = 200;
        const BASE_SEED: u64 = 0x3c6e_f372_fe94_f82b;
        const WAL_CREATE_RETRIES: usize = 32;

        let rt = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        for scenario in 0..SCENARIOS {
            let scenario_seed = BASE_SEED ^ (scenario as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            sim::setup(SimConfig {
                seed: scenario_seed,
                io_error_rate: 0.18,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: true,
            });

            let root_dir = std::env::temp_dir()
                .join(format!("walrus_strict_state_machine_{scenario}"));
            let _ = std::fs::remove_dir_all(&root_dir);
            std::fs::create_dir_all(&root_dir).expect("Failed to create walrus test dir");

            let wal_path = root_dir.join("state_machine.log");
            let mut rng = sim::XorShift128::new(scenario_seed ^ 0xa5a5_a5a5_5a5a_5a5a);
            let mut oracle: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

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
                let (inner, wal, sm) = rt.block_on(async {
                    let inner: StateMachine = Arc::new(KvStateMachine::in_memory());
                    let sm = WalBackedStateMachine::with_inner(Arc::clone(&inner), Arc::clone(&wal));
                    (inner, wal, sm)
                });

                let recovered = inner.snapshot();
                let recovered_map: HashMap<Vec<u8>, Vec<u8>> =
                    bincode::deserialize(&recovered).expect("snapshot deserialize failed");
                assert_eq!(recovered_map, oracle);
                sim::set_io_error_rate(prev_rate);
                sim::set_partial_writes_enabled(prev_partial);

                let keys: [&[u8]; 6] = [
                    b"alpha".as_slice(),
                    b"beta".as_slice(),
                    b"gamma".as_slice(),
                    b"delta".as_slice(),
                    b"epsilon".as_slice(),
                    b"zeta".as_slice(),
                ];
                let _guard = rt.enter();
                for _ in 0..OPS_PER_CYCLE {
                    let key = keys[rng.next_usize(keys.len())];
                    let action = rng.next_usize(3);
                    let (command, value) = if action < 2 {
                        let value = (rng.next_u64() % 1000).to_string();
                        (
                            format!("SET {} {}", std::str::from_utf8(key).unwrap(), value),
                            Some(value),
                        )
                    } else {
                        (
                            format!("DELETE {}", std::str::from_utf8(key).unwrap()),
                            None,
                        )
                    };
                    if sm.apply(command.as_bytes()).is_ok() {
                        if let Some(value) = value {
                            oracle.insert(key.to_vec(), value.into_bytes());
                        } else {
                            oracle.remove(key);
                        }
                    }
                }

                drop(sm);
                drop(inner);
                drop(wal);
                crate::wal::wal::__clear_storage_cache_for_tests();
                sim::advance_time(std::time::Duration::from_secs(1));
            }

            let _ = std::fs::remove_dir_all(&root_dir);
            sim::teardown();
        }
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
