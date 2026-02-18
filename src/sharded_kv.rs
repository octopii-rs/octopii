//! Sharded key-value store with WAL-backed persistence.
//!
//! Keys are distributed across nodes using placement groups. Each key lives on
//! exactly one node (no replication). Local storage is in-memory + WAL for durability.
//! On restart, replays WAL to recover state.

use crate::placement::get_placement_group;
use crate::wal::WriteAheadLog;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

const TOPIC_SHARDED_KV: &str = "sharded_kv";
const TOPIC_SHARDED_KV_SNAPSHOT: &str = "sharded_kv_snapshot";
const COMPACTION_THRESHOLD: usize = 5000;
const ENTRY_BUFFER_SIZE: usize = 512;
const SNAPSHOT_BUFFER_SIZE: usize = 65536;

const OP_GET: u8 = 1;
const OP_PUT: u8 = 2;
const OP_DELETE: u8 = 3;

pub trait RpcClient: Send + Sync {
    fn call(
        &self,
        addr: SocketAddr,
        req: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<Vec<u8>>> + Send + '_>>;
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
struct WalEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
struct WalSnapshot {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct ShardedKV<R: RpcClient> {
    my_id: u64,
    nodes: Vec<u64>,
    node_addrs: HashMap<u64, SocketAddr>,
    local: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    wal: Option<Arc<WriteAheadLog>>,
    ops_since_compaction: RwLock<usize>,
    rpc: R,
}

impl<R: RpcClient> ShardedKV<R> {
    pub fn new(
        my_id: u64,
        nodes: Vec<u64>,
        node_addrs: HashMap<u64, SocketAddr>,
        rpc: R,
    ) -> Self {
        Self {
            my_id,
            nodes,
            node_addrs,
            local: RwLock::new(HashMap::new()),
            wal: None,
            ops_since_compaction: RwLock::new(0),
            rpc,
        }
    }

    pub fn with_wal(
        my_id: u64,
        nodes: Vec<u64>,
        node_addrs: HashMap<u64, SocketAddr>,
        wal: Arc<WriteAheadLog>,
        rpc: R,
    ) -> Self {
        let kv = Self {
            my_id,
            nodes,
            node_addrs,
            local: RwLock::new(HashMap::new()),
            wal: Some(wal),
            ops_since_compaction: RwLock::new(0),
            rpc,
        };
        kv.recover_from_wal();
        kv
    }

    fn recover_from_wal(&self) {
        let Some(wal) = &self.wal else { return };

        let walrus = &wal.walrus;
        let mut recovered: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        let _ = walrus.reset_read_offset_for_topic(TOPIC_SHARDED_KV_SNAPSHOT);
        let _ = walrus.reset_read_offset_for_topic(TOPIC_SHARDED_KV);

        loop {
            match walrus.read_next(TOPIC_SHARDED_KV_SNAPSHOT, true) {
                Ok(Some(entry)) => {
                    let archived = unsafe { rkyv::archived_root::<WalSnapshot>(&entry.data) };
                    let snapshot: WalSnapshot = archived.deserialize(&mut rkyv::Infallible).unwrap();
                    recovered.clear();
                    for (key, value) in snapshot.entries {
                        recovered.insert(key, value);
                    }
                }
                _ => break,
            }
        }

        loop {
            match walrus.read_next(TOPIC_SHARDED_KV, true) {
                Ok(Some(entry)) => {
                    let archived = unsafe { rkyv::archived_root::<WalEntry>(&entry.data) };
                    let wal_entry: WalEntry = archived.deserialize(&mut rkyv::Infallible).unwrap();
                    if wal_entry.value.is_empty() {
                        recovered.remove(&wal_entry.key);
                    } else {
                        recovered.insert(wal_entry.key, wal_entry.value);
                    }
                }
                _ => break,
            }
        }

        if !recovered.is_empty() {
            *self.local.write().unwrap() = recovered;
        }
        *self.ops_since_compaction.write().unwrap() = 0;
    }

    fn append_to_wal(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        let Some(wal) = &self.wal else { return Ok(()) };

        let entry = WalEntry {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let bytes = rkyv::to_bytes::<_, ENTRY_BUFFER_SIZE>(&entry)
            .map_err(|e| format!("Serialization failed: {:?}", e))?;
        tokio::task::block_in_place(|| wal.walrus.append_for_topic(TOPIC_SHARDED_KV, &bytes))
            .map_err(|e| format!("WAL append failed: {}", e))
    }

    pub fn compact(&self) -> Result<(), String> {
        let ops = *self.ops_since_compaction.read().unwrap();
        if ops < COMPACTION_THRESHOLD {
            return Ok(());
        }

        let Some(wal) = &self.wal else { return Ok(()) };

        let data = self.local.read().unwrap();
        let snapshot = WalSnapshot {
            entries: data.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        };

        let bytes = rkyv::to_bytes::<_, SNAPSHOT_BUFFER_SIZE>(&snapshot)
            .map_err(|e| format!("Snapshot serialization failed: {:?}", e))?;

        tokio::task::block_in_place(|| wal.walrus.append_for_topic(TOPIC_SHARDED_KV_SNAPSHOT, &bytes))
            .map_err(|e| format!("Snapshot write failed: {}", e))?;

        *self.ops_since_compaction.write().unwrap() = 0;
        Ok(())
    }

    fn owner(&self, key: &[u8]) -> u64 {
        let hash = hash_key(key);
        get_placement_group(&self.nodes, hash, 1)[0]
    }

    pub async fn get(&self, key: &[u8]) -> crate::error::Result<Option<Vec<u8>>> {
        let owner = self.owner(key);

        if owner == self.my_id {
            Ok(self.local.read().unwrap().get(key).cloned())
        } else {
            let addr = self.node_addrs.get(&owner).ok_or_else(|| {
                crate::error::OctopiiError::Transport(format!("unknown node {}", owner))
            })?;
            let req = encode_get(key);
            let resp = self.rpc.call(*addr, req).await?;
            Ok(decode_get_response(&resp))
        }
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> crate::error::Result<()> {
        let owner = self.owner(key);

        if owner == self.my_id {
            self.append_to_wal(key, value)
                .map_err(|e| crate::error::OctopiiError::Wal(e))?;
            self.local.write().unwrap().insert(key.to_vec(), value.to_vec());
            *self.ops_since_compaction.write().unwrap() += 1;
            Ok(())
        } else {
            let addr = self.node_addrs.get(&owner).ok_or_else(|| {
                crate::error::OctopiiError::Transport(format!("unknown node {}", owner))
            })?;
            let req = encode_put(key, value);
            self.rpc.call(*addr, req).await?;
            Ok(())
        }
    }

    pub async fn delete(&self, key: &[u8]) -> crate::error::Result<bool> {
        let owner = self.owner(key);

        if owner == self.my_id {
            self.append_to_wal(key, &[])
                .map_err(|e| crate::error::OctopiiError::Wal(e))?;
            let existed = self.local.write().unwrap().remove(key).is_some();
            *self.ops_since_compaction.write().unwrap() += 1;
            Ok(existed)
        } else {
            let addr = self.node_addrs.get(&owner).ok_or_else(|| {
                crate::error::OctopiiError::Transport(format!("unknown node {}", owner))
            })?;
            let req = encode_delete(key);
            let resp = self.rpc.call(*addr, req).await?;
            Ok(!resp.is_empty() && resp[0] == 1)
        }
    }

    pub fn handle_request(&self, req: &[u8]) -> Vec<u8> {
        if req.is_empty() {
            return vec![];
        }

        match req[0] {
            OP_GET => {
                let key = &req[1..];
                match self.local.read().unwrap().get(key) {
                    Some(v) => v.clone(),
                    None => vec![],
                }
            }
            OP_PUT => {
                if req.len() < 5 {
                    return vec![];
                }
                let key_len = u32::from_be_bytes([req[1], req[2], req[3], req[4]]) as usize;
                if req.len() < 5 + key_len {
                    return vec![];
                }
                let key = &req[5..5 + key_len];
                let value = &req[5 + key_len..];

                if self.append_to_wal(key, value).is_err() {
                    return vec![0];
                }
                self.local.write().unwrap().insert(key.to_vec(), value.to_vec());
                *self.ops_since_compaction.write().unwrap() += 1;
                vec![1]
            }
            OP_DELETE => {
                let key = &req[1..];
                if self.append_to_wal(key, &[]).is_err() {
                    return vec![0];
                }
                let existed = self.local.write().unwrap().remove(key).is_some();
                *self.ops_since_compaction.write().unwrap() += 1;
                vec![if existed { 1 } else { 0 }]
            }
            _ => vec![],
        }
    }
}

fn hash_key(key: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

pub fn encode_get(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + key.len());
    buf.push(OP_GET);
    buf.extend_from_slice(key);
    buf
}

pub fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + key.len() + value.len());
    buf.push(OP_PUT);
    buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf
}

pub fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + key.len());
    buf.push(OP_DELETE);
    buf.extend_from_slice(key);
    buf
}

fn decode_get_response(resp: &[u8]) -> Option<Vec<u8>> {
    if resp.is_empty() {
        None
    } else {
        Some(resp.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    struct NoopRpc;

    impl RpcClient for NoopRpc {
        fn call(
            &self,
            _addr: SocketAddr,
            _req: Vec<u8>,
        ) -> Pin<Box<dyn Future<Output = crate::error::Result<Vec<u8>>> + Send + '_>> {
            Box::pin(async { Ok(vec![]) })
        }
    }

    fn make_kv(my_id: u64, nodes: Vec<u64>) -> ShardedKV<NoopRpc> {
        ShardedKV::new(my_id, nodes, HashMap::new(), NoopRpc)
    }

    fn make_kv_with_wal(my_id: u64, nodes: Vec<u64>, wal: Arc<WriteAheadLog>) -> ShardedKV<NoopRpc> {
        ShardedKV::with_wal(my_id, nodes, HashMap::new(), wal, NoopRpc)
    }

    #[test]
    fn test_owner_deterministic() {
        let kv = make_kv(1, vec![1, 2, 3, 4, 5]);
        let owner1 = kv.owner(b"test_key");
        let owner2 = kv.owner(b"test_key");
        assert_eq!(owner1, owner2);
    }

    #[test]
    fn test_handle_request_put_get_delete() {
        let kv = make_kv(1, vec![1]);

        let resp = kv.handle_request(&encode_put(b"key1", b"value1"));
        assert_eq!(resp, vec![1]);

        let resp = kv.handle_request(&encode_get(b"key1"));
        assert_eq!(resp, b"value1");

        let resp = kv.handle_request(&encode_delete(b"key1"));
        assert_eq!(resp, vec![1]);

        let resp = kv.handle_request(&encode_get(b"key1"));
        assert!(resp.is_empty());
    }

    #[test]
    fn test_encoding_roundtrip() {
        let key = b"mykey";
        let value = b"myvalue";

        let put = encode_put(key, value);
        assert_eq!(put[0], OP_PUT);
        let key_len = u32::from_be_bytes([put[1], put[2], put[3], put[4]]) as usize;
        assert_eq!(&put[5..5 + key_len], key);
        assert_eq!(&put[5 + key_len..], value);

        let get = encode_get(key);
        assert_eq!(get[0], OP_GET);
        assert_eq!(&get[1..], key);

        let del = encode_delete(key);
        assert_eq!(del[0], OP_DELETE);
        assert_eq!(&del[1..], key);
    }

    #[tokio::test]
    async fn test_local_get_put() {
        let kv = make_kv(1, vec![1]);

        kv.put(b"foo", b"bar").await.unwrap();
        let val = kv.get(b"foo").await.unwrap();
        assert_eq!(val, Some(b"bar".to_vec()));

        let deleted = kv.delete(b"foo").await.unwrap();
        assert!(deleted);

        let val = kv.get(b"foo").await.unwrap();
        assert_eq!(val, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_persistence_and_recovery() {
        use std::time::Duration;

        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("sharded_kv_wal");

        {
            let wal = Arc::new(WriteAheadLog::new(wal_path.clone(), 100, Duration::from_millis(10)).await.unwrap());
            let kv = make_kv_with_wal(1, vec![1], wal);

            kv.put(b"key1", b"value1").await.unwrap();
            kv.put(b"key2", b"value2").await.unwrap();
            kv.put(b"key3", b"value3").await.unwrap();
            kv.delete(b"key2").await.unwrap();

            assert_eq!(kv.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(kv.get(b"key2").await.unwrap(), None);
            assert_eq!(kv.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        }

        {
            let wal = Arc::new(WriteAheadLog::new(wal_path.clone(), 100, Duration::from_millis(10)).await.unwrap());
            let kv = make_kv_with_wal(1, vec![1], wal);

            assert_eq!(kv.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
            assert_eq!(kv.get(b"key2").await.unwrap(), None);
            assert_eq!(kv.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));
        }
    }
}
