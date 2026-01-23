#![cfg(feature = "openraft")]

use crate::error::{OctopiiError, Result};
use crate::invariants::sim_assert;
use crate::wal::WriteAheadLog;
#[cfg(feature = "simulation")]
use crate::wal::wal::vfs::sim;
use bytes::Bytes;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::RwLock as StdRwLock;
use std::sync::Arc;
use tokio::sync::RwLock as TokioRwLock;

pub(crate) static GLOBAL_PEER_ADDRS: Lazy<StdRwLock<HashMap<String, HashMap<u64, SocketAddr>>>> =
    Lazy::new(|| StdRwLock::new(HashMap::new()));

fn namespace_key_from_path(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

pub(crate) fn cluster_namespace_from_wal_dir(wal_dir: &Path) -> String {
    match wal_dir.parent() {
        Some(parent) => namespace_key_from_path(parent),
        None => namespace_key_from_path(wal_dir),
    }
}

pub fn peer_namespace_from_base(path: &Path) -> String {
    namespace_key_from_path(path)
}

pub(crate) fn register_global_peer_addr(namespace: &str, node_id: u64, addr: SocketAddr) {
    let mut map = GLOBAL_PEER_ADDRS.write().unwrap();
    map.entry(namespace.to_string()).or_default().insert(node_id, addr);
}

pub(crate) fn global_peer_addr(namespace: &str, peer_id: u64) -> Option<SocketAddr> {
    GLOBAL_PEER_ADDRS
        .read()
        .unwrap()
        .get(namespace)
        .and_then(|m| m.get(&peer_id).copied())
}

pub fn clear_global_peer_addrs_for(namespace: &str) {
    GLOBAL_PEER_ADDRS.write().unwrap().remove(namespace);
}

pub fn clear_global_peer_addrs() {
    GLOBAL_PEER_ADDRS.write().unwrap().clear();
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PeerAddrRecord {
    pub(crate) peer_id: u64,
    pub(crate) addr: SocketAddr,
}

pub(crate) async fn load_peer_addr_records(
    wal: &Arc<WriteAheadLog>,
) -> HashMap<u64, SocketAddr> {
    let mut map = HashMap::new();
    if let Ok(entries) = wal.read_all().await {
        for raw in entries {
            if let Ok(record) = bincode::deserialize::<PeerAddrRecord>(&raw) {
                map.insert(record.peer_id, record.addr);
            }
        }
    }
    #[cfg(feature = "simulation")]
    {
        if let Ok(entries) = wal.read_all().await {
            let mut verify = HashMap::new();
            for raw in entries {
                if let Ok(record) = bincode::deserialize::<PeerAddrRecord>(&raw) {
                    verify.insert(record.peer_id, record.addr);
                }
            }
            sim_assert(
                verify == map,
                "peer addr WAL recovery not idempotent across replay",
            );
        }
    }
    map
}

pub(crate) async fn append_peer_addr_record(
    wal: &Arc<WriteAheadLog>,
    peer_id: u64,
    addr: SocketAddr,
) -> Result<()> {
    let bytes = bincode::serialize(&PeerAddrRecord { peer_id, addr })
        .map_err(|e| OctopiiError::Wal(format!("peer addr encode: {e}")))?;
    wal.append(Bytes::from(bytes)).await?;
    Ok(())
}

pub(crate) async fn persist_peer_addr(
    peer_addrs: &TokioRwLock<HashMap<u64, SocketAddr>>,
    wal: &Arc<WriteAheadLog>,
    namespace: &str,
    peer_id: u64,
    addr: SocketAddr,
) -> Result<()> {
    let mut needs_persist = false;
    {
        let mut map = peer_addrs.write().await;
        if map.get(&peer_id).copied() != Some(addr) {
            map.insert(peer_id, addr);
            needs_persist = true;
        }
    }

    register_global_peer_addr(namespace, peer_id, addr);

    if needs_persist {
        let append_res = append_peer_addr_record(wal, peer_id, addr).await;
        if append_res.is_err() {
            sim_assert(false, "peer addr WAL append failed after map update");
        }
        append_res?;
        #[cfg(feature = "simulation")]
        {
            // Disable faults during verification read to avoid false positives
            // from I/O errors causing count mismatches in the WAL invariant check
            let prev_rate = sim::get_io_error_rate();
            let prev_partial = sim::get_partial_writes_enabled();
            sim::set_io_error_rate(0.0);
            sim::set_partial_writes_enabled(false);

            if let Ok(entries) = wal.read_all().await {
                let mut last_addr: Option<SocketAddr> = None;
                for raw in entries {
                    if let Ok(record) = bincode::deserialize::<PeerAddrRecord>(&raw) {
                        if record.peer_id == peer_id {
                            last_addr = Some(record.addr);
                        }
                    }
                }
                sim_assert(
                    last_addr == Some(addr),
                    "peer addr WAL last record mismatch after append",
                );
            }

            sim::set_partial_writes_enabled(prev_partial);
            sim::set_io_error_rate(prev_rate);
        }
    }

    Ok(())
}
