#![cfg(feature = "openraft")]

use crate::openraft::peer_registry::global_peer_addr;
use crate::openraft::types::{AppNodeId, AppTypeConfig};
use crate::rpc::{RequestPayload, ResponsePayload, RpcHandler};
#[cfg(feature = "openraft-filters")]
use crate::sim_time;
use openraft::{
    error::RPCError,
    network::RaftNetworkFactory,
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
};
use serde::{de::DeserializeOwned, Serialize};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

/// QUIC-backed network for OpenRaft messages
pub struct QuinnNetwork {
    rpc: Arc<RpcHandler>,
    peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
    self_id: AppNodeId,
    target: AppNodeId,
    default_addr: Option<SocketAddr>,
    cluster_namespace: Arc<String>,
    filters: Arc<OpenRaftFilters>,
}

impl QuinnNetwork {
    pub fn new(
        rpc: Arc<RpcHandler>,
        peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
        self_id: AppNodeId,
        target: AppNodeId,
        default_addr: Option<SocketAddr>,
        cluster_namespace: Arc<String>,
        filters: Arc<OpenRaftFilters>,
    ) -> Self {
        Self {
            rpc,
            peer_addrs,
            self_id,
            target,
            default_addr,
            cluster_namespace,
            filters,
        }
    }

    async fn peer_addr(&self) -> Option<SocketAddr> {
        let g = self.peer_addrs.read().await;
        if let Some(addr) = g.get(&self.target).copied() {
            return Some(addr);
        }
        drop(g);

        if let Some(addr) = global_peer_addr(self.cluster_namespace.as_str(), self.target) {
            self.peer_addrs.write().await.insert(self.target, addr);
            return Some(addr);
        }
        self.default_addr
    }

    async fn send_openraft(
        &self,
        kind: &str,
        data: Vec<u8>,
    ) -> Result<ResponsePayload, anyhow::Error> {
        // Apply network filters (partitions, drops, delays)
        self.filters.apply(self.self_id, self.target).await?;

        let Some(addr) = self.peer_addr().await else {
            tracing::warn!(
                "QuinnNetwork: no address for peer {} (self={})",
                self.target,
                self.self_id
            );
            anyhow::bail!("no address for peer {}", self.target);
        };

        let payload = RequestPayload::OpenRaft {
            kind: kind.to_string(),
            data: bytes::Bytes::from(data),
        };

        let resp = self
            .rpc
            .request(addr, payload, Duration::from_secs(5))
            .await?;

        Ok(resp.payload)
    }

    async fn rpc_request<Req, Resp>(
        &self,
        kind: &str,
        req: &Req,
    ) -> Result<Resp, RPCError<AppTypeConfig>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let data = bincode::serialize(req)
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        let resp_payload = self.send_openraft(kind, data).await.map_err(|e| {
            RPCError::Unreachable(openraft::error::Unreachable::new(&io::Error::other(e)))
        })?;

        match resp_payload {
            ResponsePayload::OpenRaft {
                kind: resp_kind,
                data,
            } if resp_kind == kind => bincode::deserialize(&data)
                .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e))),
            other => Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                &io::Error::other(format!("unexpected response: {:?}", other)),
            ))),
        }
    }
}

impl openraft::network::v2::RaftNetworkV2<AppTypeConfig> for QuinnNetwork {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<AppTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<AppTypeConfig>, RPCError<AppTypeConfig>> {
        self.rpc_request("append_entries", &req).await
    }

    async fn full_snapshot(
        &mut self,
        _vote: openraft::Vote<AppTypeConfig>,
        _snapshot: openraft::storage::Snapshot<AppTypeConfig>,
        _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::SnapshotResponse<AppTypeConfig>,
        openraft::error::StreamingError<AppTypeConfig>,
    > {
        // For now, return an error - full snapshot streaming not yet implemented
        Err(openraft::error::StreamingError::Unreachable(
            openraft::error::Unreachable::new(&io::Error::other(
                "full_snapshot not yet implemented",
            )),
        ))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<AppTypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<AppTypeConfig>, RPCError<AppTypeConfig>> {
        let resp = match self.rpc_request("vote", &req).await {
            Ok(resp) => resp,
            Err(err) => {
                tracing::error!("Vote RPC {}->{} failed: {}", self.self_id, self.target, err);
                return Err(err);
            }
        };
        Ok(resp)
    }
}

pub struct QuinnNetworkFactory {
    rpc: Arc<RpcHandler>,
    peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
    self_id: AppNodeId,
    cluster_namespace: Arc<String>,
    filters: Arc<OpenRaftFilters>,
}

impl QuinnNetworkFactory {
    pub fn new(
        rpc: Arc<RpcHandler>,
        peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
        self_id: AppNodeId,
        cluster_namespace: Arc<String>,
        filters: Arc<OpenRaftFilters>,
    ) -> Self {
        Self {
            rpc,
            peer_addrs,
            self_id,
            cluster_namespace,
            filters,
        }
    }
}

impl RaftNetworkFactory<AppTypeConfig> for QuinnNetworkFactory {
    type Network = QuinnNetwork;

    async fn new_client(
        &mut self,
        target: AppNodeId,
        node: &openraft::impls::BasicNode,
    ) -> Self::Network {
        let default_addr = node.addr.parse().ok();
        QuinnNetwork::new(
            Arc::clone(&self.rpc),
            Arc::clone(&self.peer_addrs),
            self.self_id,
            target,
            default_addr,
            Arc::clone(&self.cluster_namespace),
            Arc::clone(&self.filters),
        )
    }
}

/// Network filters for testing partition/delay/drop scenarios.
/// Always available but only has effect when openraft-filters feature is enabled.
pub struct OpenRaftFilters {
    #[cfg(feature = "openraft-filters")]
    pub(crate) drop_pairs: tokio::sync::RwLock<std::collections::HashSet<(AppNodeId, AppNodeId)>>,
    #[cfg(feature = "openraft-filters")]
    pub(crate) delay_pairs:
        tokio::sync::RwLock<std::collections::HashMap<(AppNodeId, AppNodeId), Duration>>,
    #[cfg(feature = "openraft-filters")]
    pub(crate) partitions: tokio::sync::RwLock<
        Vec<(
            std::collections::HashSet<AppNodeId>,
            std::collections::HashSet<AppNodeId>,
        )>,
    >,
}

impl OpenRaftFilters {
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "openraft-filters")]
            drop_pairs: tokio::sync::RwLock::new(Default::default()),
            #[cfg(feature = "openraft-filters")]
            delay_pairs: tokio::sync::RwLock::new(Default::default()),
            #[cfg(feature = "openraft-filters")]
            partitions: tokio::sync::RwLock::new(vec![]),
        }
    }

    #[cfg(feature = "openraft-filters")]
    pub async fn clear(&self) {
        self.drop_pairs.write().await.clear();
        self.delay_pairs.write().await.clear();
        self.partitions.write().await.clear();
    }

    #[cfg(not(feature = "openraft-filters"))]
    pub async fn clear(&self) {}

    /// Apply filters before sending. Returns Err if message should be dropped.
    #[cfg(feature = "openraft-filters")]
    pub(crate) async fn apply(&self, from: AppNodeId, to: AppNodeId) -> Result<(), anyhow::Error> {
        // Check partitions
        for (g1, g2) in self.partitions.read().await.iter() {
            if (g1.contains(&from) && g2.contains(&to)) || (g2.contains(&from) && g1.contains(&to))
            {
                anyhow::bail!("openraft-filters: partition drop {}->{}", from, to);
            }
        }
        // Check drop pairs
        if self.drop_pairs.read().await.contains(&(from, to)) {
            anyhow::bail!("openraft-filters: drop pair {}->{}", from, to);
        }
        // Apply delay
        if let Some(d) = self.delay_pairs.read().await.get(&(from, to)).copied() {
            sim_time::sleep(d).await;
        }
        Ok(())
    }

    #[cfg(not(feature = "openraft-filters"))]
    pub(crate) async fn apply(
        &self,
        _from: AppNodeId,
        _to: AppNodeId,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

impl Default for OpenRaftFilters {
    fn default() -> Self {
        Self::new()
    }
}
