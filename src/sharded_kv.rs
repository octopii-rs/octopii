//! Sharded key-value store. Keys are distributed across nodes using consistent
//! hashing via placement groups. No replication - each key lives on exactly one node.

use crate::placement::get_placement_group;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::RwLock;

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

pub struct ShardedKV<R: RpcClient> {
    my_id: u64,
    nodes: Vec<u64>,
    node_addrs: HashMap<u64, SocketAddr>,
    local: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
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
            rpc,
        }
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
            self.local.write().unwrap().insert(key.to_vec(), value.to_vec());
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
            Ok(self.local.write().unwrap().remove(key).is_some())
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
                self.local.write().unwrap().insert(key.to_vec(), value.to_vec());
                vec![1]
            }
            OP_DELETE => {
                let key = &req[1..];
                let existed = self.local.write().unwrap().remove(key).is_some();
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
}
