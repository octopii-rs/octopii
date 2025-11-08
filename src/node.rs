use crate::chunk::{ChunkSource, TransferResult};
use crate::config::Config;
use crate::error::Result;
use crate::raft::{RaftNode, StateMachine, WalStorage, raft_message_to_rpc, rpc_to_raft_message, raft_response_to_rpc, rpc_response_to_raft};
use crate::rpc::{deserialize, RpcHandler, RpcMessage, RpcRequest, RequestPayload, ResponsePayload, MessageId};
use crate::runtime::OctopiiRuntime;
use crate::transport::QuicTransport;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::future::join_all;
use raft::prelude::MessageType;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tokio::time::{interval, timeout, Duration};

/// Bounded tracker for Raft message types to correlate requests with responses
/// Uses a simple ring buffer to prevent unbounded growth
struct MessageTypeTracker {
    buffer: Mutex<Vec<Option<(MessageId, MessageType)>>>,
    next_slot: Mutex<usize>,
    capacity: usize,
}

impl MessageTypeTracker {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Mutex::new(vec![None; capacity]),
            next_slot: Mutex::new(0),
            capacity,
        }
    }

    /// Track a message ID and its type (overwrites oldest if full)
    fn track(&self, msg_id: MessageId, msg_type: MessageType) {
        let mut slot = self.next_slot.lock().unwrap();
        let mut buffer = self.buffer.lock().unwrap();

        buffer[*slot] = Some((msg_id, msg_type));
        *slot = (*slot + 1) % self.capacity;
    }

    /// Get the message type for a given ID, removing it from tracker
    fn take(&self, msg_id: MessageId) -> Option<MessageType> {
        let mut buffer = self.buffer.lock().unwrap();

        // Linear search - acceptable for small buffers (1024 entries)
        for entry in buffer.iter_mut() {
            if let Some((id, msg_type)) = entry {
                if *id == msg_id {
                    let result = *msg_type;
                    *entry = None;
                    return Some(result);
                }
            }
        }
        None
    }
}

/// Metadata for a pending proposal (bounded queue to prevent unbounded growth)
struct ProposalMetadata {
    responder: oneshot::Sender<Result<Bytes>>,
}

/// Bounded proposal queue using VecDeque with max capacity
struct ProposalQueue {
    queue: Mutex<VecDeque<ProposalMetadata>>,
    max_size: usize,
}

impl ProposalQueue {
    fn new(max_size: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
        }
    }

    /// Push a proposal (returns error if queue is full)
    fn push(&self, metadata: ProposalMetadata) -> std::result::Result<(), ()> {
        let mut queue = self.queue.lock().unwrap();
        if queue.len() >= self.max_size {
            return Err(());  // Queue full
        }
        queue.push_back(metadata);
        Ok(())
    }

    /// Pop the oldest proposal
    fn pop(&self) -> Option<ProposalMetadata> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }

    /// Get current queue size
    fn len(&self) -> usize {
        let queue = self.queue.lock().unwrap();
        queue.len()
    }
}

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
    /// Bounded tracker for correlating Raft message requests with responses
    msg_type_tracker: Arc<MessageTypeTracker>,
    /// Bounded queue for pending proposals (max 10000 in-flight proposals)
    proposal_queue: Arc<ProposalQueue>,
}

impl OctopiiNode {
    /// Create a new Octopii node (async version - call from within async context)
    ///
    /// This is the primary constructor. It accepts an OctopiiRuntime that can be either:
    /// - A dedicated runtime created with `OctopiiRuntime::new()`
    /// - A shared runtime created with `OctopiiRuntime::from_handle()`
    ///
    /// The async version allows multiple nodes to be created in tests without
    /// nested runtime issues.
    ///
    /// # Example
    /// ```no_run
    /// use octopii::{OctopiiNode, Config, OctopiiRuntime};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let runtime = OctopiiRuntime::new(4);
    ///     let config = Config { /* ... */ };
    ///     let node = OctopiiNode::new(config, runtime).await.unwrap();
    /// }
    /// ```
    pub async fn new(config: Config, runtime: OctopiiRuntime) -> Result<Self> {
        // Initialize components (now using .await instead of block_on!)
        let transport = Arc::new(QuicTransport::new(config.bind_addr).await?);
        let rpc = Arc::new(RpcHandler::new(Arc::clone(&transport)));

        // Create WAL
        let wal_path = config.wal_dir.join(format!("node_{}.wal", config.node_id));
        let wal = Arc::new(WriteAheadLog::new(
            wal_path,
            config.wal_batch_size,
            Duration::from_millis(config.wal_flush_interval_ms),
        ).await?);

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
        let raft = Arc::new(RaftNode::new(
            config.node_id,
            peer_ids,
            storage,
            config.is_initial_leader,
        ).await?);

        // Create state machine with WAL backing (enables durability!)
        let state_machine = Arc::new(StateMachine::with_wal(Arc::clone(&wal)));

        // Create bounded message type tracker (1024 in-flight messages max)
        let msg_type_tracker = Arc::new(MessageTypeTracker::new(1024));

        // Create bounded proposal queue (10000 max in-flight proposals)
        let proposal_queue = Arc::new(ProposalQueue::new(10000));

        let node = Self {
            runtime,
            config,
            transport,
            rpc,
            raft,
            state_machine,
            peer_addrs: Arc::new(RwLock::new(peer_addrs_map)),
            msg_type_tracker,
            proposal_queue,
        };

        tracing::info!(
            "Octopii node {} created: bind_addr={}, peers={:?}",
            node.config.node_id,
            node.config.bind_addr,
            node.config.peers
        );

        Ok(node)
    }

    /// Create a new Octopii node with blocking initialization (backward compatibility)
    ///
    /// This creates a dedicated runtime and blocks the current thread until
    /// initialization is complete. Use this for simple use cases where you
    /// don't need to manage the runtime yourself.
    ///
    /// **Note**: This cannot be called from within another async runtime.
    /// For tests and advanced use cases, use `new()` instead.
    ///
    /// # Example
    /// ```no_run
    /// use octopii::{OctopiiNode, Config};
    ///
    /// let config = Config { /* ... */ };
    /// let node = OctopiiNode::new_blocking(config).unwrap();
    /// ```
    pub fn new_blocking(config: Config) -> Result<Self> {
        // Create a dedicated runtime
        let runtime = OctopiiRuntime::new(config.worker_threads);

        // We need to spawn initialization in the runtime
        // Use Handle to spawn since we're not inside the runtime
        let handle = runtime.handle();
        let config_clone = config.clone();
        let runtime_clone = runtime.clone();

        // Block on initialization using the underlying tokio runtime
        // This is safe because we're creating a NEW runtime, not nesting
        let node = std::thread::scope(|s| {
            s.spawn(move || {
                handle.block_on(async move {
                    Self::new(config_clone, runtime_clone).await
                })
            }).join().expect("Thread panicked")
        })?;

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
    /// Returns a future that resolves when the proposal is committed and applied
    pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes> {
        tracing::info!("Proposing command to Raft: {} bytes", command.len());

        // Create oneshot channel for response
        let (tx, rx) = oneshot::channel();

        // Push to proposal queue
        let metadata = ProposalMetadata { responder: tx };
        self.proposal_queue.push(metadata)
            .map_err(|_| crate::error::OctopiiError::Rpc("Proposal queue full".to_string()))?;

        tracing::debug!("Proposal queued, {} proposals pending", self.proposal_queue.len());

        // Propose to Raft
        self.raft.propose(command).await?;

        // Wait for proposal to be committed and applied
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(crate::error::OctopiiError::Rpc("Proposal response channel closed".to_string())),
        }
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

    /// Add a peer to the Raft cluster dynamically
    /// This proposes a ConfChange that will be committed through Raft consensus
    pub async fn add_peer(&self, peer_id: u64, addr: SocketAddr) -> Result<()> {
        tracing::info!("Adding peer {} at {}", peer_id, addr);

        // Update peer address mapping immediately (optimistic)
        self.peer_addrs.write().await.insert(peer_id, addr);

        // Propose ConfChange to Raft
        let mut cc = raft::prelude::ConfChange::default();
        cc.set_change_type(raft::prelude::ConfChangeType::AddNode);
        cc.node_id = peer_id;

        self.raft.propose_conf_change(vec![], cc).await?;
        tracing::info!("Proposed AddNode for peer {}", peer_id);
        Ok(())
    }

    /// Remove a peer from the Raft cluster dynamically
    /// This proposes a ConfChange that will be committed through Raft consensus
    pub async fn remove_peer(&self, peer_id: u64) -> Result<()> {
        tracing::info!("Removing peer {}", peer_id);

        // Propose ConfChange to Raft
        let mut cc = raft::prelude::ConfChange::default();
        cc.set_change_type(raft::prelude::ConfChangeType::RemoveNode);
        cc.node_id = peer_id;

        self.raft.propose_conf_change(vec![], cc).await?;

        // Remove from peer address mapping after proposal
        // (actual removal happens when ConfChange commits)
        self.peer_addrs.write().await.remove(&peer_id);

        tracing::info!("Proposed RemoveNode for peer {}", peer_id);
        Ok(())
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

                                                                // Check for immediate response messages
                                                                if let Some(ready) = raft_clone.ready().await {
                                                                    // Send response messages immediately
                                                                    for resp_msg in ready.messages() {
                                                                        if let Some(resp_payload) = raft_response_to_rpc(resp_msg) {
                                                                            tracing::debug!("Sending immediate Raft response: {:?}", resp_msg.get_msg_type());

                                                                            let response = RpcMessage::new_response(req.id, resp_payload);
                                                                            if let Ok(data) = crate::rpc::serialize(&response) {
                                                                                if let Err(e) = peer.send(data).await {
                                                                                    tracing::error!("Failed to send Raft response: {}", e);
                                                                                }
                                                                            }
                                                                        }
                                                                    }

                                                                    // Advance without processing entries (ready handler will do full processing)
                                                                    let _light_rd = raft_clone.advance(ready).await;
                                                                    raft_clone.advance_apply().await;

                                                                    // Note: We don't apply entries here - the main ready handler does that
                                                                    // This is just for sending immediate responses to avoid latency
                                                                }
                                                            }
                                                            Err(e) => {
                                                                tracing::error!("Failed to step Raft: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    // Non-Raft messages go through normal RPC handler
                                                    rpc_clone.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                                                }
                                            }
                                        }
                                        _ => {
                                            rpc_clone.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
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
        let proposal_queue = Arc::clone(&self.proposal_queue);

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
                        let from = msg.from;
                        let msg_type = msg.get_msg_type();
                        let peer_addrs_read = peer_addrs.read().await;

                        if let Some(peer_addr) = peer_addrs_read.get(&to) {
                            if let Some(payload) = raft_message_to_rpc(&msg) {
                                tracing::debug!(
                                    "Sending Raft message to peer {}: {:?} -> {}",
                                    to,
                                    msg_type,
                                    peer_addr
                                );

                                // Send message via RPC
                                let rpc_clone = Arc::clone(&rpc);
                                let raft_clone = Arc::clone(&raft);
                                let peer_addr = *peer_addr;

                                tokio::spawn(async move {
                                    match rpc_clone.request(
                                        peer_addr,
                                        payload,
                                        Duration::from_secs(5),
                                    ).await {
                                        Ok(response) => {
                                            tracing::trace!("Received Raft response from {}: {:?}", peer_addr, response.payload);

                                            // Convert response back to Raft message and step it
                                            // Use the captured msg_type from the original message
                                            if let Some(raft_resp_msg) = rpc_response_to_raft(to, from, msg_type, &response.payload) {
                                                tracing::debug!("Stepping Raft response back: {:?}", raft_resp_msg.get_msg_type());

                                                if let Err(e) = raft_clone.step(raft_resp_msg).await {
                                                    tracing::error!("Failed to step Raft response: {}", e);
                                                }
                                            }
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
                    let mut last_applied_index = 0u64;
                    for entry in light_rd.committed_entries().iter() {
                        last_applied_index = entry.index;
                        // Check if this is a ConfChange entry
                        if entry.get_entry_type() == raft::prelude::EntryType::EntryConfChange {
                            tracing::info!("Applying ConfChange entry: index={}, term={}",
                                entry.index, entry.term);

                            // Decode and apply ConfChange
                            match protobuf::Message::parse_from_bytes(&entry.data) {
                                Ok(cc) => {
                                    match raft.apply_conf_change(&cc).await {
                                        Ok(conf_state) => {
                                            tracing::info!("✓ ConfChange applied: {:?} for node {}, voters: {:?}",
                                                cc.get_change_type(), cc.node_id, conf_state.voters);
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to apply ConfChange: {}", e);
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to decode ConfChange: {:?}", e);
                                }
                            }
                        } else if !entry.data.is_empty() {
                            tracing::info!("Applying committed entry: index={}, term={}, {} bytes",
                                entry.index, entry.term, entry.data.len());

                            // Apply to state machine
                            let apply_result = state_machine.apply(&entry.data);

                            // If this node is the leader, respond to the client
                            if raft.is_leader().await {
                                if let Some(proposal) = proposal_queue.pop() {
                                    tracing::debug!("Responding to proposal, {} remaining in queue", proposal_queue.len());

                                    let response_result = match apply_result {
                                        Ok(result) => {
                                            tracing::info!("Entry applied successfully: {:?}", result);
                                            Ok(result)
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to apply entry: {}", e);
                                            Err(crate::error::OctopiiError::Rpc(e))
                                        }
                                    };

                                    // Send response to client (ignore if client disconnected)
                                    let _ = proposal.responder.send(response_result);
                                } else {
                                    // This shouldn't happen - we have a committed entry but no pending proposal
                                    tracing::warn!("Committed entry but no pending proposal (possibly from follower sync)");
                                }
                            } else {
                                // Followers just apply without responding to clients
                                match apply_result {
                                    Ok(result) => tracing::info!("Entry applied successfully: {:?}", result),
                                    Err(e) => tracing::error!("Failed to apply entry: {}", e),
                                }
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

                    // Trigger compaction if we applied entries
                    if last_applied_index > 0 {
                        // 1. Try state machine compaction (threshold-based)
                        if let Err(e) = state_machine.compact_state_machine() {
                            tracing::warn!("State machine compaction failed: {}", e);
                        }

                        // 2. Try log compaction (threshold-based)
                        // Get state machine snapshot for Raft snapshot data
                        // Convert HashMap<String, Bytes> to serializable format
                        let sm_snapshot = state_machine.snapshot();
                        let serializable_snapshot: Vec<(String, Vec<u8>)> = sm_snapshot
                            .iter()
                            .map(|(k, v)| (k.clone(), v.to_vec()))
                            .collect();
                        let snapshot_data = rkyv::to_bytes::<_, 4096>(&serializable_snapshot)
                            .unwrap_or_default()
                            .to_vec();

                        // Try to compact logs (will only compact if threshold reached)
                        if let Err(e) = raft.try_compact_logs(last_applied_index, snapshot_data).await {
                            tracing::warn!("Log compaction failed: {}", e);
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
            is_initial_leader: false,
        };

        assert_eq!(config.node_id, 1);
        assert_eq!(config.worker_threads, 2);
        assert_eq!(config.wal_batch_size, 10);
    }
}
