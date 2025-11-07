use super::{serialize, MessageId, RpcMessage, RpcRequest, RpcResponse, ResponsePayload};
use crate::error::{OctopiiError, Result};
use crate::runtime::OctopiiRuntime;
use crate::transport::QuicTransport;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::time::{timeout, Duration};

/// Callback for handling incoming requests
pub type RequestHandler = Arc<dyn Fn(RpcRequest) -> ResponsePayload + Send + Sync>;

/// RPC handler that manages request/response correlation
pub struct RpcHandler {
    transport: Arc<QuicTransport>,
    next_id: AtomicU64,
    pending_requests: Arc<RwLock<HashMap<MessageId, oneshot::Sender<RpcResponse>>>>,
    request_handler: Arc<RwLock<Option<RequestHandler>>>,
    message_tx: mpsc::UnboundedSender<(SocketAddr, RpcMessage)>,
}

impl RpcHandler {
    /// Create a new RPC handler
    pub fn new(transport: Arc<QuicTransport>, runtime: &OctopiiRuntime) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        let handler = Self {
            transport: Arc::clone(&transport),
            next_id: AtomicU64::new(1),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_handler: Arc::new(RwLock::new(None)),
            message_tx,
        };

        // Spawn message receiver task on the isolated runtime
        let h = handler.clone();
        runtime.spawn(async move {
            h.message_receiver_task(message_rx).await;
        });

        handler
    }

    /// Set the request handler callback
    pub async fn set_request_handler<F>(&self, handler: F)
    where
        F: Fn(RpcRequest) -> ResponsePayload + Send + Sync + 'static,
    {
        let mut h = self.request_handler.write().await;
        *h = Some(Arc::new(handler));
    }

    /// Send a request and wait for response
    pub async fn request(
        &self,
        addr: SocketAddr,
        payload: super::RequestPayload,
        timeout_duration: Duration,
    ) -> Result<RpcResponse> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let request = RpcMessage::new_request(id, payload);

        let (tx, rx) = oneshot::channel();

        // Register pending request
        {
            let mut pending = self.pending_requests.write().await;
            pending.insert(id, tx);
        }

        // Serialize and send
        let data = serialize(&request)?;
        self.transport.send(addr, data).await?;

        // Wait for response with timeout
        match timeout(timeout_duration, rx).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) => Err(OctopiiError::Rpc("Response channel closed".to_string())),
            Err(_) => {
                // Timeout - clean up pending request
                let mut pending = self.pending_requests.write().await;
                pending.remove(&id);
                Err(OctopiiError::Rpc("Request timeout".to_string()))
            }
        }
    }

    /// Send a one-way message (no response expected)
    pub async fn send_one_way(
        &self,
        addr: SocketAddr,
        message: super::OneWayMessage,
    ) -> Result<()> {
        let msg = RpcMessage::new_one_way(message);
        let data = serialize(&msg)?;
        self.transport.send(addr, data).await
    }

    /// Notify the handler of an incoming message
    pub fn notify_message(&self, addr: SocketAddr, msg: RpcMessage) -> Result<()> {
        self.message_tx
            .send((addr, msg))
            .map_err(|_| OctopiiError::Rpc("Message receiver task died".to_string()))
    }

    /// Task that processes incoming messages
    async fn message_receiver_task(&self, mut rx: mpsc::UnboundedReceiver<(SocketAddr, RpcMessage)>) {
        while let Some((addr, msg)) = rx.recv().await {
            match msg {
                RpcMessage::Request(req) => {
                    self.handle_request(addr, req).await;
                }
                RpcMessage::Response(resp) => {
                    self.handle_response(resp).await;
                }
                RpcMessage::OneWay(_) => {
                    // One-way messages would be handled by application logic
                    tracing::debug!("Received one-way message from {}", addr);
                }
            }
        }
    }

    /// Handle an incoming request
    async fn handle_request(&self, addr: SocketAddr, req: RpcRequest) {
        let handler = self.request_handler.read().await;

        let response_payload = match handler.as_ref() {
            Some(h) => h(req.clone()),
            None => ResponsePayload::Error {
                message: "No request handler registered".to_string(),
            },
        };

        let response = RpcMessage::new_response(req.id, response_payload);

        // Send response
        if let Ok(data) = serialize(&response) {
            if let Err(e) = self.transport.send(addr, data).await {
                tracing::error!("Failed to send response to {}: {}", addr, e);
            }
        }
    }

    /// Handle an incoming response
    async fn handle_response(&self, resp: RpcResponse) {
        let mut pending = self.pending_requests.write().await;
        if let Some(tx) = pending.remove(&resp.id) {
            let _ = tx.send(resp);
        }
    }

    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            next_id: AtomicU64::new(self.next_id.load(Ordering::SeqCst)),
            pending_requests: Arc::clone(&self.pending_requests),
            request_handler: Arc::clone(&self.request_handler),
            message_tx: self.message_tx.clone(),
        }
    }
}

// RPC tests are in tests/rpc_test.rs due to bidirectional communication requirements
