use crate::chunk::{ChunkSource, TransferResult};
use crate::config::Config;
use crate::error::Result;
use crate::raft::{RaftNode, StateMachine, WalStorage, raft_message_to_rpc, rpc_to_raft_message};
use crate::rpc::{deserialize, RpcHandler, RpcMessage, RpcRequest, RequestPayload, ResponsePayload};
use crate::runtime::OctopiiRuntime;
use crate::transport::QuicTransport;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::future::join_all;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout, Duration};

/// Main Octopii node that orchestrates all components
pub struct OctopiiNode {
    runtime: OctopiiRuntime,
    config: Config,
    transport: Arc<QuicTransport>,
    rpc: Arc<RpcHandler>,
    raft: Arc<RaftNode>,
    state_machine: Arc<StateMachine>,
    /// Map peer ID to socket address
    peer_addrs: Arc<RwLock<HashMap<u64, SocketAddr>>>,
}

impl OctopiiNode {
    /// Create a new Octopii node
    pub fn new(config: Config) -> Result<Self> {
        // Create isolated runtime
        let runtime = OctopiiRuntime::new(config.worker_threads);

        // Initialize components on the isolated runtime
        let transport = Arc::new(runtime.block_on(QuicTransport::new(config.bind_addr))?);
        let rpc = Arc::new(RpcHandler::new(Arc::clone(&transport), &runtime));

        // Create WAL
        let wal_path = config.wal_dir.join(format!("node_{}.wal", config.node_id));
        let wal = Arc::new(
            runtime.block_on(WriteAheadLog::new(
                wal_path,
                config.wal_batch_size,
                Duration::from_millis(config.wal_flush_interval_ms),
            ))?,
        );

        // Create Raft storage and node
        let storage = WalStorage::new(Arc::clone(&wal));

        // Build peer ID mapping (peer ID = index + 1, but skip our own ID)
        let mut peer_addrs_map = HashMap::new();
        for (idx, addr) in config.peers.iter().enumerate() {
            let peer_id = (idx + 1) as u64;
            let peer_id_adjusted = if peer_id >= config.node_id {
                peer_id + 1  // Skip our own ID
            } else {
                peer_id
            };
            peer_addrs_map.insert(peer_id_adjusted, *addr);
            tracing::info!("Mapped peer {} -> {}", peer_id_adjusted, addr);
        }

        let peer_ids: Vec<u64> = peer_addrs_map.keys().copied().collect();
        let raft = Arc::new(runtime.block_on(RaftNode::new(
            config.node_id,
            peer_ids,
            storage,
            config.is_initial_leader,
        ))?);

        // Create state machine
        let state_machine = Arc::new(StateMachine::new());

        let node = Self {
            runtime,
            config,
            transport,
            rpc,
            raft,
            state_machine,
            peer_addrs: Arc::new(RwLock::new(peer_addrs_map)),
        };

        tracing::info!(
            "Octopii node {} created: bind_addr={}, peers={:?}",
            node.config.node_id,
            node.config.bind_addr,
            node.config.peers
        );

        Ok(node)
    }

    /// Start the node
    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting Octopii node {}", self.config.node_id);

        // Set up RPC request handler
        self.setup_rpc_handler().await;

        // Spawn network acceptor task
        self.spawn_network_acceptor();

        // Spawn Raft tick task
        self.spawn_raft_ticker();

        // Spawn Raft ready handler
        self.spawn_raft_ready_handler();

        Ok(())
    }

    /// Propose a change to the distributed state machine
    pub async fn propose(&self, command: Vec<u8>) -> Result<()> {
        tracing::info!("Proposing command to Raft: {} bytes", command.len());
        self.raft.propose(command).await
    }

    /// Execute a read-only query on the state machine
    pub async fn query(&self, command: &[u8]) -> Result<Bytes> {
        self.state_machine
            .apply(command)
            .map_err(|e| crate::error::OctopiiError::Rpc(e))
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        self.raft.is_leader().await
    }

    /// Trigger this node to campaign for leadership
    pub async fn campaign(&self) -> Result<()> {
        self.raft.campaign().await
    }

    /// Get the node ID
    pub fn id(&self) -> u64 {
        self.config.node_id
    }

    /// Transfer a chunk to multiple peers in parallel with verification
    ///
    /// This method sends a chunk (from file or memory) to multiple peers
    /// concurrently. Each transfer is verified with SHA256 checksum and
    /// application-level acknowledgment.
    ///
    /// # Arguments
    /// * `chunk` - Source of the chunk data (file or memory)
    /// * `peers` - List of peer addresses to send to
    /// * `timeout_duration` - Timeout for each individual peer transfer
    ///
    /// # Returns
    /// Vector of `TransferResult`, one per peer, indicating success/failure
    ///
    /// # Example
    /// ```no_run
    /// # use octopii::{OctopiiNode, Config, ChunkSource};
    /// # use std::time::Duration;
    /// # async fn example(node: OctopiiNode) {
    /// let results = node.transfer_chunk_to_peers(
    ///     ChunkSource::File("/data/chunk_1.dat".into()),
    ///     vec!["10.0.0.1:5000".parse().unwrap()],
    ///     Duration::from_secs(60),
    /// ).await;
    /// # }
    /// ```
    pub async fn transfer_chunk_to_peers(
        &self,
        chunk: ChunkSource,
        peers: Vec<SocketAddr>,
        timeout_duration: Duration,
    ) -> Vec<TransferResult> {
        // Create Arc for shared access across tasks
        let chunk = Arc::new(chunk);

        // Spawn one task per peer
        let tasks: Vec<_> = peers
            .into_iter()
            .map(|peer| {
                let transport = Arc::clone(&self.transport);
                let chunk = Arc::clone(&chunk);
                let runtime = self.runtime.clone();

                runtime.spawn(async move {
                    let start = Instant::now();

                    // Apply timeout to the entire transfer
                    let result = timeout(timeout_duration, async {
                        // Get or create connection to peer
                        let peer_conn = transport.connect(peer).await?;

                        // Send chunk with verification
                        let bytes_transferred = peer_conn.send_chunk_verified(&chunk).await?;

                        Ok::<_, crate::error::OctopiiError>(bytes_transferred)
                    })
                    .await;

                    let duration = start.elapsed();

                    match result {
                        Ok(Ok(bytes)) => TransferResult::success(peer, bytes, duration),
                        Ok(Err(e)) => TransferResult::failure(peer, e.to_string()),
                        Err(_) => TransferResult::failure(
                            peer,
                            format!("Transfer timeout after {:?}", timeout_duration),
                        ),
                    }
                })
            })
            .collect();

        // Wait for all tasks to complete
        let results = join_all(tasks).await;

        // Unwrap JoinHandle results (panics are propagated as errors)
        results
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| {
                TransferResult::failure(
                    "0.0.0.0:0".parse().unwrap(),
                    format!("Task panicked: {}", e),
                )
            }))
            .collect()
    }

    /// Set up RPC request handler for application-level RPCs
    /// Note: Raft RPCs are handled directly in the network acceptor for async processing
    async fn setup_rpc_handler(&self) {
        tracing::info!("Setting up application RPC handler");

        self.rpc
            .set_request_handler(move |req: RpcRequest| {
                match &req.payload {
                    RequestPayload::AppendEntries { .. } | RequestPayload::RequestVote { .. } => {
                        // Raft messages are handled in network acceptor, this shouldn't be reached
                        tracing::warn!("Raft message reached RPC handler (should be handled in acceptor)");
                        ResponsePayload::Error {
                            message: "Raft messages handled separately".to_string(),
                        }
                    }
                    RequestPayload::Custom { operation, data } => {
                        tracing::debug!("Handling custom RPC: operation={}, data={} bytes", operation, data.len());
                        ResponsePayload::CustomResponse {
                            success: true,
                            data: Bytes::from("OK"),
                        }
                    }
                }
            })
            .await;
    }

    /// Spawn task to accept incoming network connections
    fn spawn_network_acceptor(&self) {
        let transport = Arc::clone(&self.transport);
        let rpc = Arc::clone(&self.rpc);
        let raft = Arc::clone(&self.raft);
        let node_id = self.config.node_id;

        tracing::info!("Spawning network acceptor for node {}", node_id);

        self.runtime.spawn(async move {
            loop {
                match transport.accept().await {
                    Ok((addr, peer)) => {
                        tracing::debug!("Accepted connection from {}", addr);

                        let rpc_clone = Arc::clone(&rpc);
                        let raft_clone = Arc::clone(&raft);
                        let peer = Arc::new(peer);

                        tokio::spawn(async move {
                            while let Ok(Some(data)) = peer.recv().await {
                                if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                    // Handle Raft messages directly for async processing
                                    match &msg {
                                        RpcMessage::Request(req) => {
                                            match &req.payload {
                                                RequestPayload::AppendEntries { .. } | RequestPayload::RequestVote { .. } => {
                                                    tracing::debug!("Received Raft request from {}: {:?}", addr, req.payload);

                                                    // Convert RPC to Raft message
                                                    if let Some(raft_msg) = rpc_to_raft_message(0, node_id, &req.payload) {
                                                        // Step the Raft state machine
                                                        match raft_clone.step(raft_msg).await {
                                                            Ok(_) => {
                                                                tracing::trace!("Raft message processed successfully");
                                                                // Response will be sent from ready handler
                                                            }
                                                            Err(e) => {
                                                                tracing::error!("Failed to step Raft: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    // Non-Raft messages go through normal RPC handler
                                                    let _ = rpc_clone.notify_message(addr, msg);
                                                }
                                            }
                                        }
                                        _ => {
                                            let _ = rpc_clone.notify_message(addr, msg);
                                        }
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::error!("Failed to accept connection: {}", e);
                    }
                }
            }
        });
    }

    /// Spawn task to tick Raft periodically
    fn spawn_raft_ticker(&self) {
        let raft = Arc::clone(&self.raft);

        tracing::info!("Spawning Raft ticker (100ms interval)");

        self.runtime.spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                ticker.tick().await;
                raft.tick().await;
                tracing::trace!("Raft ticked");
            }
        });
    }

    /// Spawn task to handle Raft ready events
    fn spawn_raft_ready_handler(&self) {
        let raft = Arc::clone(&self.raft);
        let state_machine = Arc::clone(&self.state_machine);
        let rpc = Arc::clone(&self.rpc);
        let peer_addrs = Arc::clone(&self.peer_addrs);

        tracing::info!("Spawning Raft ready handler");

        self.runtime.spawn(async move {
            let mut checker = interval(Duration::from_millis(10));
            let mut check_count = 0;
            loop {
                checker.tick().await;
                check_count += 1;

                if check_count % 100 == 0 {
                    tracing::debug!("Ready handler alive, checked {} times", check_count);
                }

                if let Some(ready) = raft.ready().await {
                    tracing::info!("Raft ready: has {} messages, {} committed entries, {} new entries, snapshot: {}",
                        ready.messages().len(),
                        ready.committed_entries().len(),
                        ready.entries().len(),
                        !ready.snapshot().is_empty()
                    );

                    // Persist hard state if it changed
                    if let Some(hs) = ready.hs() {
                        tracing::debug!("Persisting HardState: term={}, vote={}, commit={}",
                            hs.term, hs.vote, hs.commit);
                        raft.persist_hard_state(hs.clone()).await;
                    }

                    // Persist new entries to storage (must be done before sending messages)
                    if !ready.entries().is_empty() {
                        tracing::debug!("Persisting {} new entries to storage", ready.entries().len());
                        raft.persist_entries(ready.entries()).await;
                    }

                    // Send outgoing Raft messages to peers
                    for msg in ready.messages() {
                        let to = msg.to;
                        let peer_addrs_read = peer_addrs.read().await;

                        if let Some(peer_addr) = peer_addrs_read.get(&to) {
                            if let Some(payload) = raft_message_to_rpc(&msg) {
                                tracing::debug!(
                                    "Sending Raft message to peer {}: {:?} -> {}",
                                    to,
                                    msg.get_msg_type(),
                                    peer_addr
                                );

                                // Send message via RPC
                                let rpc_clone = Arc::clone(&rpc);
                                let peer_addr = *peer_addr;
                                tokio::spawn(async move {
                                    match rpc_clone.request(
                                        peer_addr,
                                        payload,
                                        Duration::from_secs(5),
                                    ).await {
                                        Ok(response) => {
                                            tracing::trace!("Received Raft response from {}: {:?}", peer_addr, response.payload);
                                            // TODO: Feed response back to Raft
                                        }
                                        Err(e) => {
                                            tracing::warn!("Failed to send Raft message to {}: {}", peer_addr, e);
                                        }
                                    }
                                });
                            }
                        } else {
                            tracing::warn!("No address found for peer {}", to);
                        }
                    }

                    // Advance Raft state and get LightReady with committed entries
                    let light_rd = raft.advance(ready).await;

                    tracing::info!("LightReady: {} committed entries, {} messages",
                        light_rd.committed_entries().len(),
                        light_rd.messages().len()
                    );

                    // Apply committed entries to state machine (from LightReady)
                    for entry in light_rd.committed_entries().iter() {
                        if !entry.data.is_empty() {
                            tracing::info!("Applying committed entry: index={}, term={}, {} bytes",
                                entry.index, entry.term, entry.data.len());
                            match state_machine.apply(&entry.data) {
                                Ok(result) => tracing::info!("Entry applied successfully: {:?}", result),
                                Err(e) => tracing::error!("Failed to apply entry: {}", e),
                            }
                        }
                    }

                    // Send messages from LightReady
                    for msg in light_rd.messages() {
                        let to = msg.to;
                        let peer_addrs_read = peer_addrs.read().await;

                        if let Some(peer_addr) = peer_addrs_read.get(&to) {
                            if let Some(payload) = raft_message_to_rpc(&msg) {
                                tracing::debug!(
                                    "Sending LightReady Raft message to peer {}: {:?} -> {}",
                                    to,
                                    msg.get_msg_type(),
                                    peer_addr
                                );

                                let rpc_clone = Arc::clone(&rpc);
                                let peer_addr = *peer_addr;
                                tokio::spawn(async move {
                                    match rpc_clone.request(
                                        peer_addr,
                                        payload,
                                        Duration::from_secs(5),
                                    ).await {
                                        Ok(response) => {
                                            tracing::trace!("Received Raft response from {}: {:?}", peer_addr, response.payload);
                                        }
                                        Err(e) => {
                                            tracing::warn!("Failed to send Raft message to {}: {}", peer_addr, e);
                                        }
                                    }
                                });
                            }
                        }
                    }

                    // Finish advancing (completes the apply phase)
                    raft.advance_apply().await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validation() {
        // Test basic config creation
        let config = Config {
            node_id: 1,
            bind_addr: "127.0.0.1:5000".parse().unwrap(),
            peers: vec![],
            wal_dir: std::env::temp_dir().join("octopii_test"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
        };

        assert_eq!(config.node_id, 1);
        assert_eq!(config.worker_threads, 2);
        assert_eq!(config.wal_batch_size, 10);
    }
}
