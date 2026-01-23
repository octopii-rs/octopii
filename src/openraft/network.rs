#![cfg(feature = "openraft")]

use crate::openraft::peer_registry::global_peer_addr;
use crate::openraft::types::{AppNodeId, AppTypeConfig};
use crate::rpc::{RequestPayload, ResponsePayload, RpcHandler};
use openraft::{
    error::RPCError,
    network::{RaftNetwork, RaftNetworkFactory},
    raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse},
};
use serde::{de::DeserializeOwned, Serialize};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;
use crate::sim_time;

/// QUIC-backed network for OpenRaft messages
pub struct QuinnNetwork {
    rpc: Arc<RpcHandler>,
    peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
    self_id: AppNodeId,
    target: AppNodeId,
    default_addr: Option<SocketAddr>,
    cluster_namespace: Arc<String>,
    #[cfg(feature = "openraft-filters")]
    pub(crate) filters: Arc<OpenRaftFilters>,
}

impl QuinnNetwork {
    pub fn new(
        rpc: Arc<RpcHandler>,
        peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
        self_id: AppNodeId,
        target: AppNodeId,
        default_addr: Option<SocketAddr>,
        cluster_namespace: Arc<String>,
        #[cfg(feature = "openraft-filters")] filters: Arc<OpenRaftFilters>,
    ) -> Self {
        Self {
            rpc,
            peer_addrs,
            self_id,
            target,
            default_addr,
            cluster_namespace,
            #[cfg(feature = "openraft-filters")]
            filters,
        }
    }

    async fn peer_addr(&self) -> Option<SocketAddr> {
        let g = self.peer_addrs.read().await;
        if let Some(addr) = g.get(&self.target).copied() {
            eprintln!(
                "[openraft rpc] {} -> {} using cached addr {}",
                self.self_id, self.target, addr
            );
            return Some(addr);
        }
        drop(g);

        if let Some(addr) = global_peer_addr(self.cluster_namespace.as_str(), self.target) {
            self.peer_addrs.write().await.insert(self.target, addr);
            eprintln!(
                "[openraft rpc] {} -> {} using global addr {}",
                self.self_id, self.target, addr
            );
            return Some(addr);
        }
        if let Some(addr) = self.default_addr {
            eprintln!(
                "[openraft rpc] {} -> {} using default addr {}",
                self.self_id, self.target, addr
            );
        }
        self.default_addr
    }

    async fn send_openraft(
        &self,
        kind: &str,
        data: Vec<u8>,
    ) -> Result<ResponsePayload, anyhow::Error> {
        eprintln!(
            "[openraft rpc] {} -> {} kind={} sending",
            self.self_id, self.target, kind
        );
        #[cfg(feature = "openraft-filters")]
        {
            for (g1, g2) in self.filters.partitions.read().await.iter() {
                if (g1.contains(&self.self_id) && g2.contains(&self.target))
                    || (g2.contains(&self.self_id) && g1.contains(&self.target))
                {
                    anyhow::bail!(
                        "openraft-filters: partition drop {}->{}",
                        self.self_id,
                        self.target
                    );
                }
            }
            if self
                .filters
                .drop_pairs
                .read()
                .await
                .contains(&(self.self_id, self.target))
            {
                anyhow::bail!(
                    "openraft-filters: drop pair {}->{}",
                    self.self_id,
                    self.target
                );
            }
            if let Some(d) = self
                .filters
                .delay_pairs
                .read()
                .await
                .get(&(self.self_id, self.target))
                .copied()
            {
                sim_time::sleep(d).await;
            }
        }

        let Some(addr) = self.peer_addr().await else {
            tracing::warn!(
                "QuinnNetwork: no address for peer {} (self={})",
                self.target,
                self.self_id
            );
            eprintln!(
                "[openraft rpc] {} -> {} kind={} no address",
                self.self_id, self.target, kind
            );
            anyhow::bail!("no address for peer {}", self.target);
        };

        let payload = RequestPayload::OpenRaft {
            kind: kind.to_string(),
            data: bytes::Bytes::from(data),
        };

        let resp = match self
            .rpc
            .request(addr, payload, Duration::from_secs(5))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                eprintln!(
                    "[openraft rpc] {} -> {} kind={} addr={} failed: {}",
                    self.self_id, self.target, kind, addr, e
                );
                return Err(e.into());
            }
        };

        eprintln!(
            "[openraft rpc] {} -> {} kind={} got response payload",
            self.self_id, self.target, kind
        );
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

        let resp_payload = self
            .send_openraft(kind, data)
            .await
            .map_err(|e| {
                RPCError::Unreachable(openraft::error::Unreachable::new(&io::Error::new(
                    io::ErrorKind::Other,
                    e,
                )))
            })?;

        match resp_payload {
            ResponsePayload::OpenRaft { kind: resp_kind, data } if resp_kind == kind => {
                bincode::deserialize(&data).map_err(|e| {
                    eprintln!(
                        "[openraft rpc] {} -> {} kind={} deserialize failed: {} ({} bytes)",
                        self.self_id,
                        self.target,
                        kind,
                        e,
                        data.len()
                    );
                    RPCError::Network(openraft::error::NetworkError::new(&e))
                })
            }
            other => {
                eprintln!(
                    "[openraft rpc] {} -> {} kind={} unexpected response: {:?}",
                    self.self_id, self.target, kind, other
                );
                Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                    &io::Error::new(
                        io::ErrorKind::Other,
                        format!("unexpected response: {:?}", other),
                    ),
                )))
            }
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
            openraft::error::Unreachable::new(&io::Error::new(
                io::ErrorKind::Other,
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
    #[cfg(feature = "openraft-filters")]
    filters: Arc<OpenRaftFilters>,
}

impl QuinnNetworkFactory {
    pub fn new(
        rpc: Arc<RpcHandler>,
        peer_addrs: Arc<tokio::sync::RwLock<std::collections::HashMap<AppNodeId, SocketAddr>>>,
        self_id: AppNodeId,
        cluster_namespace: Arc<String>,
        #[cfg(feature = "openraft-filters")] filters: Arc<OpenRaftFilters>,
    ) -> Self {
        Self {
            rpc,
            peer_addrs,
            self_id,
            cluster_namespace,
            #[cfg(feature = "openraft-filters")]
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
            #[cfg(feature = "openraft-filters")]
            Arc::clone(&self.filters),
        )
    }
}

#[cfg(feature = "openraft-filters")]
pub struct OpenRaftFilters {
    pub(crate) drop_pairs: tokio::sync::RwLock<std::collections::HashSet<(AppNodeId, AppNodeId)>>,
    pub(crate) delay_pairs:
        tokio::sync::RwLock<std::collections::HashMap<(AppNodeId, AppNodeId), Duration>>,
    pub(crate) partitions: tokio::sync::RwLock<
        Vec<(
            std::collections::HashSet<AppNodeId>,
            std::collections::HashSet<AppNodeId>,
        )>,
    >,
}

#[cfg(feature = "openraft-filters")]
impl OpenRaftFilters {
    pub fn new() -> Self {
        Self {
            drop_pairs: tokio::sync::RwLock::new(Default::default()),
            delay_pairs: tokio::sync::RwLock::new(Default::default()),
            partitions: tokio::sync::RwLock::new(vec![]),
        }
    }

    pub async fn clear(&self) {
        self.drop_pairs.write().await.clear();
        self.delay_pairs.write().await.clear();
        self.partitions.write().await.clear();
    }
}
