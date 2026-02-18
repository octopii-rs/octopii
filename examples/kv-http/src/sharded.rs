//! Sharded KV implementation for the example. Uses HTTP forwarding between nodes
//! and shows routing info (owner node, placement group) in responses.

use octopii::get_placement_group;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

pub struct ShardedStore {
    pub my_id: u64,
    pub nodes: Vec<u64>,
    pub node_http_addrs: HashMap<u64, String>,
    pub local: RwLock<HashMap<String, String>>,
    pub http_client: reqwest::Client,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RoutingInfo {
    pub key_hash: String,
    pub owner_node: u64,
    pub placement_group: Vec<u64>,
    pub is_local: bool,
    pub forwarded_to: Option<String>,
}

impl ShardedStore {
    pub fn new(my_id: u64, nodes: Vec<u64>, node_http_addrs: HashMap<u64, String>) -> Self {
        Self {
            my_id,
            nodes,
            node_http_addrs,
            local: RwLock::new(HashMap::new()),
            http_client: reqwest::Client::new(),
        }
    }

    pub fn routing_info(&self, key: &str) -> RoutingInfo {
        let hash = hash_key(key);
        let pg = get_placement_group(&self.nodes, hash, self.nodes.len());
        let owner = pg[0];

        RoutingInfo {
            key_hash: format!("{:016x}", hash),
            owner_node: owner,
            placement_group: pg,
            is_local: owner == self.my_id,
            forwarded_to: None,
        }
    }

    pub async fn get(&self, key: &str) -> (Option<String>, RoutingInfo) {
        let mut info = self.routing_info(key);

        if info.is_local {
            let val = self.local.read().unwrap().get(key).cloned();
            (val, info)
        } else {
            let addr = self.node_http_addrs.get(&info.owner_node);
            if let Some(addr) = addr {
                info.forwarded_to = Some(addr.clone());
                let url = format!("{}/sharded/internal/{}", addr, key);
                match self.http_client.get(&url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            if let Ok(body) = resp.json::<InternalGetResponse>().await {
                                return (body.value, info);
                            }
                        }
                        (None, info)
                    }
                    Err(_) => (None, info),
                }
            } else {
                (None, info)
            }
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> (bool, RoutingInfo) {
        let mut info = self.routing_info(key);

        if info.is_local {
            self.local.write().unwrap().insert(key.to_string(), value.to_string());
            (true, info)
        } else {
            let addr = self.node_http_addrs.get(&info.owner_node);
            if let Some(addr) = addr {
                info.forwarded_to = Some(addr.clone());
                let url = format!("{}/sharded/internal/{}", addr, key);
                let body = InternalPutRequest { value: value.to_string() };
                match self.http_client.put(&url).json(&body).send().await {
                    Ok(resp) => (resp.status().is_success(), info),
                    Err(_) => (false, info),
                }
            } else {
                (false, info)
            }
        }
    }

    pub async fn delete(&self, key: &str) -> (bool, RoutingInfo) {
        let mut info = self.routing_info(key);

        if info.is_local {
            let existed = self.local.write().unwrap().remove(key).is_some();
            (existed, info)
        } else {
            let addr = self.node_http_addrs.get(&info.owner_node);
            if let Some(addr) = addr {
                info.forwarded_to = Some(addr.clone());
                let url = format!("{}/sharded/internal/{}", addr, key);
                match self.http_client.delete(&url).send().await {
                    Ok(resp) => (resp.status().is_success(), info),
                    Err(_) => (false, info),
                }
            } else {
                (false, info)
            }
        }
    }

    pub fn local_get(&self, key: &str) -> Option<String> {
        self.local.read().unwrap().get(key).cloned()
    }

    pub fn local_put(&self, key: &str, value: &str) {
        self.local.write().unwrap().insert(key.to_string(), value.to_string());
    }

    pub fn local_delete(&self, key: &str) -> bool {
        self.local.write().unwrap().remove(key).is_some()
    }
}

fn hash_key(key: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[derive(serde::Deserialize)]
struct InternalGetResponse {
    value: Option<String>,
}

#[derive(serde::Serialize)]
struct InternalPutRequest {
    value: String,
}
