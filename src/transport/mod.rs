mod peer;
#[cfg(feature = "simulation")]
mod sim;
mod tls;

pub use peer::PeerConnection;
#[cfg(feature = "simulation")]
pub use sim::{SimConfig, SimRouter, SimTransport};

use crate::chunk::ChunkSource;
use crate::error::{OctopiiError, Result};
use bytes::Bytes;
use quinn::Endpoint;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type TransportFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait Transport: Send + Sync {
    fn local_addr(&self) -> Result<SocketAddr>;
    fn close(&self);
    fn is_closed(&self) -> bool;
    fn connect(&self, addr: SocketAddr) -> TransportFut<'_, Arc<dyn Peer>>;
    fn accept(&self) -> TransportFut<'_, (SocketAddr, Arc<dyn Peer>)>;
    fn send(&self, addr: SocketAddr, data: Bytes) -> TransportFut<'_, ()> {
        Box::pin(async move {
            let peer = self.connect(addr).await?;
            peer.send(data).await
        })
    }
}

pub trait Peer: Send + Sync {
    fn send(&self, data: Bytes) -> TransportFut<'_, ()>;
    fn recv(&self) -> TransportFut<'_, Option<Bytes>>;
    fn is_closed(&self) -> bool;

    fn send_chunk_verified(&self, _chunk: ChunkSource) -> TransportFut<'_, u64> {
        Box::pin(async {
            Err(OctopiiError::Transport(
                "send_chunk_verified not supported".to_string(),
            ))
        })
    }

    fn recv_chunk_verified(&self) -> TransportFut<'_, Option<Bytes>> {
        Box::pin(async {
            Err(OctopiiError::Transport(
                "recv_chunk_verified not supported".to_string(),
            ))
        })
    }

    fn recv_chunk_verified_to_file(&self, _path: &Path) -> TransportFut<'_, Option<u64>> {
        Box::pin(async {
            Err(OctopiiError::Transport(
                "recv_chunk_verified_to_file not supported".to_string(),
            ))
        })
    }
}

pub struct QuicTransport {
    endpoint: Endpoint,
    outgoing_peers: Arc<RwLock<HashMap<SocketAddr, Arc<PeerConnection>>>>,
    accepted_peers: Arc<RwLock<HashMap<SocketAddr, Arc<PeerConnection>>>>,
    closed: Arc<AtomicBool>,
}

impl QuicTransport {
    pub async fn new(bind_addr: SocketAddr) -> Result<Self> {
        let (cert, key) = tls::generate_self_signed_cert()?;

        let server_config = tls::create_server_config(cert.clone(), key)?;

        let endpoint = Endpoint::server(server_config, bind_addr)?;

        tracing::info!("QUIC transport listening on {}", bind_addr);

        Ok(Self {
            endpoint,
            outgoing_peers: Arc::new(RwLock::new(HashMap::new())),
            accepted_peers: Arc::new(RwLock::new(HashMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<Arc<PeerConnection>> {
        {
            let peers = self.outgoing_peers.read().await;
            if let Some(peer) = peers.get(&addr) {
                if !peer.is_closed() {
                    tracing::debug!("Reusing existing connection to {}", addr);
                    return Ok(Arc::clone(peer));
                } else {
                    tracing::debug!("Existing connection to {} is closed, reconnecting", addr);
                }
            }
        }

        let mut peers = self.outgoing_peers.write().await;

        if let Some(peer) = peers.get(&addr) {
            if !peer.is_closed() {
                tracing::debug!("Reusing existing connection to {} (after lock)", addr);
                return Ok(Arc::clone(peer));
            }
        }

        tracing::debug!("Creating new QUIC connection to {}", addr);

        let client_config = tls::create_client_config()?;

        let connection = self
            .endpoint
            .connect_with(client_config, addr, "localhost")
            .map_err(|e| {
                tracing::error!("Failed to initiate connection to {}: {}", addr, e);
                OctopiiError::Transport(format!("Connect error: {}", e))
            })?
            .await
            .map_err(|e| {
                tracing::error!("Failed to complete connection to {}: {}", addr, e);
                OctopiiError::Transport(format!("Connection failed: {}", e))
            })?;

        let peer = Arc::new(PeerConnection::new(connection));
        peers.insert(addr, Arc::clone(&peer));

        tracing::info!("Connected to peer {}", addr);

        Ok(peer)
    }

    pub async fn accept(&self) -> Result<(SocketAddr, Arc<PeerConnection>)> {
        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| OctopiiError::Transport("Endpoint closed".to_string()))?;

        let remote_addr = incoming.remote_address();
        let connection = incoming.await?;

        let peer = Arc::new(PeerConnection::new(connection));

        let mut peers = self.accepted_peers.write().await;
        peers.insert(remote_addr, Arc::clone(&peer));

        tracing::info!("Accepted connection from {}", remote_addr);

        Ok((remote_addr, peer))
    }

    pub async fn send(&self, addr: SocketAddr, data: Bytes) -> Result<()> {
        let peer = self.connect(addr).await?;
        peer.send(data).await
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .map_err(|e| OctopiiError::Transport(e.to_string()))
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.endpoint.close(0u32.into(), b"shutdown");
    }

    pub async fn has_active_peer(&self, addr: SocketAddr) -> bool {
        {
            let peers = self.outgoing_peers.read().await;
            if let Some(peer) = peers.get(&addr) {
                if !peer.is_closed() {
                    return true;
                }
            }
        }
        {
            let peers = self.accepted_peers.read().await;
            if let Some(peer) = peers.get(&addr) {
                if !peer.is_closed() {
                    return true;
                }
            }
        }
        false
    }
}

impl Transport for QuicTransport {
    fn local_addr(&self) -> Result<SocketAddr> {
        QuicTransport::local_addr(self)
    }

    fn close(&self) {
        QuicTransport::close(self);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    fn connect(&self, addr: SocketAddr) -> TransportFut<'_, Arc<dyn Peer>> {
        Box::pin(async move {
            let peer = QuicTransport::connect(self, addr).await?;
            Ok(peer as Arc<dyn Peer>)
        })
    }

    fn accept(&self) -> TransportFut<'_, (SocketAddr, Arc<dyn Peer>)> {
        Box::pin(async move {
            let (addr, peer) = QuicTransport::accept(self).await?;
            Ok((addr, peer as Arc<dyn Peer>))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_connect() {
        let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let transport1 = QuicTransport::new(addr1).await.unwrap();
        let transport2 = QuicTransport::new(addr2).await.unwrap();

        let actual_addr2 = transport2.local_addr().unwrap();

        let t2 = Arc::new(transport2);
        let t2_clone = Arc::clone(&t2);
        tokio::spawn(async move {
            let _ = t2_clone.accept().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let peer = transport1.connect(actual_addr2).await.unwrap();
        assert!(!peer.is_closed());

        transport1.close();
        t2.close();
    }
}
