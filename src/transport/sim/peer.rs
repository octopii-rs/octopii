use super::router::SimRouter;
use crate::chunk::ChunkSource;
use crate::error::{OctopiiError, Result};
use crate::transport::{Peer, TransportFut};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SimTransport {
    addr: SocketAddr,
    router: SimRouter,
    epoch: u64,
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
            {
                let peers = outgoing_peers.read().await;
                if let Some(peer) = peers.get(&addr) {
                    if !peer.is_closed() {
                        return Ok(Arc::clone(peer) as Arc<dyn Peer>);
                    }
                }
            }

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

    fn send_chunk_verified(&self, chunk: ChunkSource) -> TransportFut<'_, u64> {
        use tokio::io::AsyncReadExt;

        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        let local_epoch = self.local_epoch;
        let remote_epoch = self.remote_epoch;

        Box::pin(async move {
            let data = match chunk {
                ChunkSource::File(path) => tokio::fs::read(path)
                    .await
                    .map_err(|e| OctopiiError::Transport(format!("read file: {}", e)))?,
                ChunkSource::Memory(bytes) => bytes.to_vec(),
                ChunkSource::Stream { size, mut reader } => {
                    let mut buf = Vec::with_capacity(size as usize);
                    reader
                        .read_to_end(&mut buf)
                        .await
                        .map_err(|e| OctopiiError::Transport(format!("read stream: {}", e)))?;
                    buf
                }
            };

            let size = data.len() as u64;
            let mut hasher = Sha256::new();
            hasher.update(&data);
            let checksum = hasher.finalize();

            let mut buf = BytesMut::with_capacity(8 + data.len() + 32);
            buf.put_u64(size);
            buf.put_slice(&data);
            buf.put_slice(&checksum);

            router.enqueue(local, local_epoch, remote, remote_epoch, buf.freeze())?;

            loop {
                if router.is_closed(local, local_epoch) || router.is_closed(remote, remote_epoch) {
                    return Err(OctopiiError::Transport("connection closed".to_string()));
                }
                if let Some(ack) = router.recv_from(local, local_epoch, remote)? {
                    if ack.len() == 1 && ack[0] == 0 {
                        return Ok(size);
                    } else {
                        return Err(OctopiiError::Transport(
                            "checksum verification failed on receiver".to_string(),
                        ));
                    }
                }
                if let Some(notify) = router.notify_handle(local, local_epoch) {
                    notify.notified().await;
                }
            }
        })
    }

    fn recv_chunk_verified(&self) -> TransportFut<'_, Option<Bytes>> {
        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        let local_epoch = self.local_epoch;
        let remote_epoch = self.remote_epoch;

        Box::pin(async move {
            let msg = loop {
                if router.is_closed(local, local_epoch) || router.is_closed(remote, remote_epoch) {
                    return Ok(None);
                }
                if let Some(data) = router.recv_from(local, local_epoch, remote)? {
                    break data;
                }
                if let Some(notify) = router.notify_handle(local, local_epoch) {
                    notify.notified().await;
                }
            };

            if msg.len() < 8 + 32 {
                router.enqueue(
                    local,
                    local_epoch,
                    remote,
                    remote_epoch,
                    Bytes::from_static(&[1]),
                )?;
                return Err(OctopiiError::Transport("message too short".to_string()));
            }

            let mut cursor = msg.as_ref();
            let size = cursor.get_u64() as usize;

            if msg.len() != 8 + size + 32 {
                router.enqueue(
                    local,
                    local_epoch,
                    remote,
                    remote_epoch,
                    Bytes::from_static(&[1]),
                )?;
                return Err(OctopiiError::Transport(
                    "invalid message length".to_string(),
                ));
            }

            let data = &cursor[..size];
            let received_checksum = &cursor[size..size + 32];

            let mut hasher = Sha256::new();
            hasher.update(data);
            let computed_checksum = hasher.finalize();

            if &computed_checksum[..] != received_checksum {
                router.enqueue(
                    local,
                    local_epoch,
                    remote,
                    remote_epoch,
                    Bytes::from_static(&[1]),
                )?;
                return Err(OctopiiError::Transport("checksum mismatch".to_string()));
            }

            router.enqueue(
                local,
                local_epoch,
                remote,
                remote_epoch,
                Bytes::from_static(&[0]),
            )?;

            Ok(Some(Bytes::copy_from_slice(data)))
        })
    }

    fn recv_chunk_verified_to_file(&self, path: &Path) -> TransportFut<'_, Option<u64>> {
        let router = self.router.clone();
        let local = self.local;
        let remote = self.remote;
        let local_epoch = self.local_epoch;
        let remote_epoch = self.remote_epoch;
        let path = path.to_path_buf();

        Box::pin(async move {
            let msg = loop {
                if router.is_closed(local, local_epoch) || router.is_closed(remote, remote_epoch) {
                    return Ok(None);
                }
                if let Some(data) = router.recv_from(local, local_epoch, remote)? {
                    break data;
                }
                if let Some(notify) = router.notify_handle(local, local_epoch) {
                    notify.notified().await;
                }
            };

            if msg.len() < 8 + 32 {
                router.enqueue(
                    local,
                    local_epoch,
                    remote,
                    remote_epoch,
                    Bytes::from_static(&[1]),
                )?;
                return Err(OctopiiError::Transport("message too short".to_string()));
            }

            let mut cursor = msg.as_ref();
            let size = cursor.get_u64() as usize;

            if msg.len() != 8 + size + 32 {
                router.enqueue(
                    local,
                    local_epoch,
                    remote,
                    remote_epoch,
                    Bytes::from_static(&[1]),
                )?;
                return Err(OctopiiError::Transport(
                    "invalid message length".to_string(),
                ));
            }

            let data = &cursor[..size];
            let received_checksum = &cursor[size..size + 32];

            let mut hasher = Sha256::new();
            hasher.update(data);
            let computed_checksum = hasher.finalize();

            if &computed_checksum[..] != received_checksum {
                router.enqueue(
                    local,
                    local_epoch,
                    remote,
                    remote_epoch,
                    Bytes::from_static(&[1]),
                )?;
                return Err(OctopiiError::Transport("checksum mismatch".to_string()));
            }

            tokio::fs::write(&path, data)
                .await
                .map_err(|e| OctopiiError::Transport(format!("write file: {}", e)))?;

            router.enqueue(
                local,
                local_epoch,
                remote,
                remote_epoch,
                Bytes::from_static(&[0]),
            )?;

            Ok(Some(size as u64))
        })
    }
}
