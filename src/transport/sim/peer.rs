use super::router::SimRouter;
use crate::error::Result;
use crate::transport::{Peer, TransportFut};
use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SimTransport {
    addr: SocketAddr,
    router: SimRouter,
    epoch: u64,
    /// Cache of outgoing peer connections (like QUIC transport)
    outgoing_peers: Arc<RwLock<HashMap<SocketAddr, Arc<SimPeer>>>>,
}

impl SimTransport {
    pub fn new(addr: SocketAddr, router: SimRouter) -> Self {
        let epoch = router.register(addr);
        Self {
            addr,
            router,
            epoch,
            outgoing_peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn router(&self) -> SimRouter {
        self.router.clone()
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.addr)
    }

    pub fn close(&self) {
        self.router.close(self.addr, self.epoch);
    }

    pub fn is_closed(&self) -> bool {
        self.router.is_closed(self.addr, self.epoch)
    }

    pub fn connect(&self, addr: SocketAddr) -> TransportFut<'_, Arc<dyn Peer>> {
        let router = self.router.clone();
        let local = self.addr;
        let local_epoch = self.epoch;
        let outgoing_peers = Arc::clone(&self.outgoing_peers);
        Box::pin(async move {
            // Check if we already have a valid cached connection (like QUIC transport)
            {
                let peers = outgoing_peers.read().await;
                if let Some(peer) = peers.get(&addr) {
                    if !peer.is_closed() {
                        return Ok(Arc::clone(peer) as Arc<dyn Peer>);
                    }
                }
            }

            // Create new connection and cache it
            let remote_epoch = router.epoch_of(addr);
            let peer = Arc::new(SimPeer::new(local, local_epoch, addr, remote_epoch, router));

            let mut peers = outgoing_peers.write().await;
            peers.insert(addr, Arc::clone(&peer));

            Ok(peer as Arc<dyn Peer>)
        })
    }

    pub fn accept(&self) -> TransportFut<'_, (SocketAddr, Arc<dyn Peer>)> {
        let router = self.router.clone();
        let local = self.addr;
        let local_epoch = self.epoch;
        Box::pin(async move {
            loop {
                if router.is_closed(local, local_epoch) {
                    return Err(crate::error::OctopiiError::Transport(
                        "sim transport closed".to_string(),
                    ));
                }
                if let Some(peer) = router.accept_peer(local)? {
                    let remote_epoch = router.epoch_of(peer);
                    let sim_peer =
                        SimPeer::new(local, local_epoch, peer, remote_epoch, router.clone());
                    return Ok((peer, Arc::new(sim_peer) as Arc<dyn Peer>));
                }
                if let Some(notify) = router.notify_handle(local, local_epoch) {
                    notify.notified().await;
                }
            }
        })
    }

    pub fn send(&self, addr: SocketAddr, data: Bytes) -> TransportFut<'_, ()> {
        let router = self.router.clone();
        let local = self.addr;
        let local_epoch = self.epoch;
        Box::pin(async move {
            let remote_epoch = router.epoch_of(addr);
            router.enqueue(local, local_epoch, addr, remote_epoch, data)
        })
    }
}

#[derive(Clone)]
pub struct SimPeer {
    local: SocketAddr,
    local_epoch: u64,
    remote: SocketAddr,
    remote_epoch: u64,
    router: SimRouter,
}

impl SimPeer {
    fn new(
        local: SocketAddr,
        local_epoch: u64,
        remote: SocketAddr,
        remote_epoch: u64,
        router: SimRouter,
    ) -> Self {
        Self {
            local,
            local_epoch,
            remote,
            remote_epoch,
            router,
        }
    }
}

impl Peer for SimPeer {
    fn send(&self, data: Bytes) -> TransportFut<'_, ()> {
        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        let local_epoch = self.local_epoch;
        let remote_epoch = self.remote_epoch;
        Box::pin(async move { router.enqueue(local, local_epoch, remote, remote_epoch, data) })
    }

    fn recv(&self) -> TransportFut<'_, Option<Bytes>> {
        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        let local_epoch = self.local_epoch;
        let remote_epoch = self.remote_epoch;
        Box::pin(async move {
            loop {
                if router.is_closed(local, local_epoch) || router.is_closed(remote, remote_epoch) {
                    return Ok(None);
                }
                if let Some(data) = router.recv_from(local, local_epoch, remote)? {
                    return Ok(Some(data));
                }
                if let Some(notify) = router.notify_handle(local, local_epoch) {
                    notify.notified().await;
                }
            }
        })
    }

    fn is_closed(&self) -> bool {
        self.router.is_closed(self.local, self.local_epoch)
            || self.router.is_closed(self.remote, self.remote_epoch)
    }
}
