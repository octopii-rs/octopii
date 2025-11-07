use super::{serialize, MessageId, RpcMessage, RpcRequest, RpcResponse, ResponsePayload};
use crate::error::{OctopiiError, Result};
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
    pub fn new(transport: Arc<QuicTransport>) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        let handler = Self {
            transport: Arc::clone(&transport),
            next_id: AtomicU64::new(1),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_handler: Arc::new(RwLock::new(None)),
            message_tx,
        };

        // Spawn message receiver task
        let h = handler.clone();
        tokio::spawn(async move {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{deserialize, RequestPayload, ResponsePayload};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_rpc_request_response() {
        let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
        let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

        let actual_addr2 = transport2.local_addr().unwrap();

        let rpc1 = RpcHandler::new(transport1);
        let rpc2 = RpcHandler::new(Arc::clone(&transport2));

        // Set up handler on node 2
        rpc2.set_request_handler(|req: RpcRequest| {
            ResponsePayload::CustomResponse {
                success: true,
                data: Bytes::from("pong"),
            }
        })
        .await;

        // Spawn acceptor for transport2
        let rpc2_clone = rpc2.clone();
        tokio::spawn(async move {
            loop {
                if let Ok((addr, peer)) = transport2.accept().await {
                    let rpc = rpc2_clone.clone();
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                let _ = rpc.notify_message(addr, msg);
                            }
                        }
                    });
                }
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Send request from node 1
        let response = rpc1
            .request(
                actual_addr2,
                RequestPayload::Custom {
                    operation: "ping".to_string(),
                    data: Bytes::from("test"),
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        match response.payload {
            ResponsePayload::CustomResponse { success, data } => {
                assert!(success);
                assert_eq!(data, Bytes::from("pong"));
            }
            _ => panic!("Unexpected response type"),
        }
    }
}
