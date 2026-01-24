use super::{
    deserialize, serialize, MessageId, ResponsePayload, RpcMessage, RpcRequest, RpcResponse,
};
use crate::error::{OctopiiError, Result};
use crate::sim_time;
use crate::transport::{Peer, Transport};
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::time::timeout;
use tokio::time::Duration;

pub type RequestHandlerFuture = Pin<Box<dyn Future<Output = ResponsePayload> + Send>>;

pub type RequestHandler = Arc<dyn Fn(RpcRequest) -> RequestHandlerFuture + Send + Sync>;

pub struct RpcHandler {
    transport: Arc<dyn Transport>,
    next_id: AtomicU64,
    pending_requests: Arc<RwLock<HashMap<MessageId, oneshot::Sender<RpcResponse>>>>,
    request_handler: Arc<RwLock<Option<RequestHandler>>>,
    peer_receivers: Arc<Mutex<HashMap<SocketAddr, usize>>>,
}

impl RpcHandler {
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self {
            transport: Arc::clone(&transport),
            next_id: AtomicU64::new(1),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_handler: Arc::new(RwLock::new(None)),
            peer_receivers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn set_request_handler<F, Fut>(&self, handler: F)
    where
        F: Fn(RpcRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ResponsePayload> + Send + 'static,
    {
        let mut h = self.request_handler.write().await;
        *h = Some(Arc::new(move |req| Box::pin(handler(req))));
    }

    pub async fn request(
        self: &Arc<Self>,
        addr: SocketAddr,
        payload: super::RequestPayload,
        timeout_duration: Duration,
    ) -> Result<RpcResponse> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = RpcMessage::new_request(id, payload.clone());

        let payload_kind = match &payload {
            #[cfg(feature = "openraft")]
            super::RequestPayload::OpenRaft { kind, .. } => format!("OpenRaft({})", kind),
            _ => "Other".to_string(),
        };

        tracing::debug!("RPC request {} to {}: {:?}", id, addr, payload_kind);

        let (tx, rx) = oneshot::channel();

        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(id, tx);
        }

        let data = serialize(&request)?;

        tracing::debug!("RPC request {}: connecting to {}", id, addr);
        let peer = match self.transport.connect(addr).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("RPC request {}: failed to connect to {}: {}", id, addr, e);
                let mut pending = self.pending_requests.write().await;
                pending.remove(&id);
                return Err(e);
            }
        };

        tracing::debug!("RPC request {}: ensuring peer receiver for {}", id, addr);
        self.ensure_peer_receiver(addr, Arc::clone(&peer)).await;

        tracing::debug!("RPC request {}: sending data to {}", id, addr);
        timeout(timeout_duration, peer.send(data))
            .await
            .map_err(|_| {
                tracing::error!("RPC request {}: send timeout to {}", id, addr);
                OctopiiError::Rpc("Request send timeout".to_string())
            })?
            .map_err(|e| {
                tracing::error!(
                    "RPC request {}: transport send failed to {}: {}",
                    id,
                    addr,
                    e
                );
                OctopiiError::Rpc(format!("Transport send failed: {}", e))
            })?;

        tracing::debug!("RPC request {}: waiting for response from {}", id, addr);
        match timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => {
                tracing::debug!("RPC request {}: received response from {}", id, addr);
                Ok(response)
            }
            Ok(Err(_)) => {
                tracing::error!("RPC request {}: response channel closed for {}", id, addr);
                Err(OctopiiError::Rpc("Response channel closed".to_string()))
            }
            Err(_) => {
                tracing::error!(
                    "RPC request {}: timeout waiting for response from {}",
                    id,
                    addr
                );
                let mut pending = self.pending_requests.write().await;
                pending.remove(&id);
                Err(OctopiiError::Rpc("Request timeout".to_string()))
            }
        }
    }

    pub async fn send_one_way(
        self: &Arc<Self>,
        addr: SocketAddr,
        message: super::OneWayMessage,
    ) -> Result<()> {
        let msg = RpcMessage::new_one_way(message);
        let data = serialize(&msg)?;
        let peer = self.transport.connect(addr).await?;
        self.ensure_peer_receiver(addr, Arc::clone(&peer)).await;

        peer.send(data).await.map_err(|e| {
            OctopiiError::Rpc(format!("Failed to send one-way message to {}: {}", addr, e))
        })
    }

    pub async fn register_peer_receiver(self: &Arc<Self>, addr: SocketAddr, peer: Arc<dyn Peer>) {
        self.ensure_peer_receiver(addr, peer).await;
    }

    pub fn spawn_accept_loop(self: &Arc<Self>, transport: Arc<dyn Transport>) {
        let rpc = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match transport.accept().await {
                    Ok((addr, peer)) => {
                        tracing::debug!("Accepted connection from {}", addr);
                        rpc.register_peer_receiver(addr, peer).await;
                    }
                    Err(e) => {
                        tracing::debug!("Failed to accept connection: {}", e);
                        if cfg!(feature = "simulation") {
                            tokio::task::yield_now().await;
                        } else {
                            sim_time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
        });
    }

    pub async fn notify_message(
        &self,
        addr: SocketAddr,
        msg: RpcMessage,
        peer: Option<Arc<dyn Peer>>,
    ) {
        tracing::debug!(
            "RPC notify_message from {}: {:?}",
            addr,
            match &msg {
                RpcMessage::Request(req) => format!("Request(id={})", req.id),
                RpcMessage::Response(resp) => format!("Response(id={})", resp.id),
                RpcMessage::OneWay(_) => "OneWay".to_string(),
            }
        );

        match msg {
            RpcMessage::Request(req) => {
                self.handle_request(addr, req, peer).await;
            }
            RpcMessage::Response(resp) => {
                tracing::debug!("RPC handler: received response {}", resp.id);
                self.handle_response(resp).await;
            }
            RpcMessage::OneWay(_) => {
                tracing::debug!("Received one-way message from {}", addr);
            }
        }
    }

    async fn handle_request(&self, addr: SocketAddr, req: RpcRequest, peer: Option<Arc<dyn Peer>>) {
        let handler = self.request_handler.read().await;

        let response_payload = match handler.as_ref() {
            Some(h) => h(req.clone()).await,
            None => ResponsePayload::Error {
                message: "No request handler registered".to_string(),
            },
        };

        let response = RpcMessage::new_response(req.id, response_payload);

        if let Ok(data) = serialize(&response) {
            if let Some(peer) = peer {
                if let Err(e) = peer.send(data).await {
                    tracing::error!("Failed to send response via peer: {}", e);
                }
            } else if let Err(e) = self.transport.send(addr, data).await {
                tracing::error!("Failed to send response to {}: {}", addr, e);
            }
        }
    }

    async fn handle_response(&self, resp: RpcResponse) {
        let mut pending = self.pending_requests.write().await;
        if let Some(tx) = pending.remove(&resp.id) {
            let _ = tx.send(resp);
        }
    }

    async fn ensure_peer_receiver(self: &Arc<Self>, addr: SocketAddr, peer: Arc<dyn Peer>) {
        let peer_ptr = Arc::as_ptr(&peer) as *const () as usize;

        let mut receivers = self.peer_receivers.lock().await;
        if let Some(&existing_ptr) = receivers.get(&addr) {
            if existing_ptr == peer_ptr {
                return;
            }
        }
        receivers.insert(addr, peer_ptr);
        drop(receivers);

        let rpc = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                match peer.recv().await {
                    Ok(Some(data)) => match deserialize::<RpcMessage>(&data) {
                        Ok(msg) => {
                            rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to deserialize RPC message from {}: {}",
                                addr,
                                e
                            );
                        }
                    },
                    Ok(None) => {
                        tracing::info!("Peer {} closed connection", addr);
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("Peer {} recv error: {}", addr, e);
                        break;
                    }
                }
            }

            let mut receivers = rpc.peer_receivers.lock().await;
            if let Some(&registered_ptr) = receivers.get(&addr) {
                if registered_ptr == peer_ptr {
                    receivers.remove(&addr);
                }
            }
        });
    }
}

