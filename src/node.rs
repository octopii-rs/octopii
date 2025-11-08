use crate::chunk::{ChunkSource, TransferResult};
use crate::config::Config;
use crate::error::{OctopiiError, Result};
use crate::raft::{
    raft_message_to_rpc, raft_response_to_rpc, rpc_response_to_raft, rpc_to_raft_message, RaftNode,
    StateMachine, WalStorage,
};
use crate::rpc::{
    deserialize, MessageId, RequestPayload, ResponsePayload, RpcHandler, RpcMessage, RpcRequest,
};
use crate::runtime::OctopiiRuntime;
use crate::transport::QuicTransport;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::future::join_all;
use raft::prelude::MessageType;
use raft::Storage;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::sync::{broadcast, oneshot, RwLock};
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
            return Err(()); // Queue full
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
    /// Pending ConfChange completions - notifies callers when membership changes commit
    pending_conf_changes: Arc<RwLock<HashMap<u64, oneshot::Sender<raft::prelude::ConfState>>>>,
    /// Broadcast channel used to signal background tasks to shut down gracefully
    shutdown_tx: broadcast::Sender<()>,
    /// Flag ensuring shutdown is triggered at most once
    shutdown_triggered: AtomicBool,
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
    /// ```ignore
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
        let wal = Arc::new(
            WriteAheadLog::new(
                wal_path,
                config.wal_batch_size,
                Duration::from_millis(config.wal_flush_interval_ms),
            )
            .await?,
        );

        // Create Raft storage and node
        let storage = WalStorage::new(Arc::clone(&wal));

        // Build peer ID mapping (peer ID = index + 1, but skip our own ID)
        let mut peer_addrs_map = HashMap::new();
        for (idx, addr) in config.peers.iter().enumerate() {
            let peer_id = (idx + 1) as u64;
            let peer_id_adjusted = if peer_id >= config.node_id {
                peer_id + 1 // Skip our own ID
            } else {
                peer_id
            };
            peer_addrs_map.insert(peer_id_adjusted, *addr);
            tracing::info!("Mapped peer {} -> {}", peer_id_adjusted, addr);
        }

        let mut peer_ids: Vec<u64> = peer_addrs_map.keys().copied().collect();
        peer_ids.sort_unstable();

        // IMPORTANT: Do NOT bootstrap the configuration here!
        // The correct initialization is handled in RaftNode::new() which follows TiKV's pattern:
        // - Leader node: Initializes with snapshot containing only itself as voter
        // - Follower nodes: Start with empty configuration (lazy initialization from leader)
        // Pre-populating followers with voter lists causes "to_commit out of range" panics.

        let raft = Arc::new(
            RaftNode::new(config.node_id, peer_ids, storage, config.is_initial_leader).await?,
        );

        // Create state machine with WAL backing (enables durability!)
        let state_machine = Arc::new(StateMachine::with_wal(Arc::clone(&wal)));

        // Create bounded message type tracker (1024 in-flight messages max)
        let msg_type_tracker = Arc::new(MessageTypeTracker::new(1024));

        // Create bounded proposal queue (10000 max in-flight proposals)
        let proposal_queue = Arc::new(ProposalQueue::new(10000));

        // Create pending ConfChange tracker
        let pending_conf_changes = Arc::new(RwLock::new(HashMap::new()));

        // Channel for signaling graceful shutdown to background tasks
        let (shutdown_tx, _) = broadcast::channel(16);

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
            pending_conf_changes,
            shutdown_tx,
            shutdown_triggered: AtomicBool::new(false),
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
    /// ```ignore
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
                handle.block_on(async move { Self::new(config_clone, runtime_clone).await })
            })
            .join()
            .expect("Thread panicked")
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
        self.proposal_queue
            .push(metadata)
            .map_err(|_| crate::error::OctopiiError::Rpc("Proposal queue full".to_string()))?;

        tracing::debug!(
            "Proposal queued, {} proposals pending",
            self.proposal_queue.len()
        );

        // Propose to Raft
        self.raft.propose(command).await?;

        // Wait for proposal to be committed and applied
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(crate::error::OctopiiError::Rpc(
                "Proposal response channel closed".to_string(),
            )),
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

    /// Check if the cluster has a leader
    pub async fn has_leader(&self) -> bool {
        self.raft.has_leader().await
    }

    /// Trigger this node to campaign for leadership
    pub async fn campaign(&self) -> Result<()> {
        self.raft.campaign().await
    }

    /// Add a peer to the Raft cluster dynamically
    /// This proposes a ConfChange that will be committed through Raft consensus.
    /// Blocks until the ConfChange is committed and applied, or times out after 10 seconds.
    /// Returns the new ConfState after the change is applied.
    pub async fn add_peer(&self, peer_id: u64, addr: SocketAddr) -> Result<raft::prelude::ConfState> {
        tracing::info!("Adding peer {} at {}", peer_id, addr);

        // Update peer address mapping immediately (optimistic)
        self.peer_addrs.write().await.insert(peer_id, addr);

        // Create oneshot channel to wait for completion
        let (tx, rx) = oneshot::channel();
        self.pending_conf_changes.write().await.insert(peer_id, tx);

        // Propose ConfChange to Raft
        let mut cc = raft::prelude::ConfChange::default();
        cc.set_change_type(raft::prelude::ConfChangeType::AddNode);
        cc.node_id = peer_id;

        self.raft.propose_conf_change(vec![], cc).await?;
        tracing::info!("Proposed AddNode for peer {}", peer_id);

        // Wait for ConfChange to commit (with 10 second timeout)
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(conf_state)) => {
                tracing::info!("✓ AddNode completed for peer {}, voters: {:?}", peer_id, conf_state.voters);
                Ok(conf_state)
            }
            Ok(Err(_)) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    "ConfChange notification channel closed".to_string()
                )))
            }
            Err(_) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    format!("ConfChange timeout after 10s for peer {}", peer_id)
                )))
            }
        }
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

    /// Add a peer as a learner (safe membership change)
    /// Learners receive log entries but don't vote, making it safe to add them
    /// without affecting quorum. Promote to voter once caught up.
    /// Blocks until the ConfChange is committed and applied, or times out after 10 seconds.
    /// Returns the new ConfState after the change is applied.
    pub async fn add_learner(&self, peer_id: u64, addr: SocketAddr) -> Result<raft::prelude::ConfState> {
        tracing::info!("Adding peer {} at {} as LEARNER", peer_id, addr);

        // Update peer address mapping
        self.peer_addrs.write().await.insert(peer_id, addr);

        // Create oneshot channel to wait for completion
        let (tx, rx) = oneshot::channel();
        self.pending_conf_changes.write().await.insert(peer_id, tx);

        // Propose ConfChange to add as learner
        let mut cc = raft::prelude::ConfChange::default();
        cc.set_change_type(raft::prelude::ConfChangeType::AddLearnerNode);
        cc.node_id = peer_id;

        self.raft.propose_conf_change(vec![], cc).await?;
        tracing::info!("Proposed AddLearnerNode for peer {}", peer_id);

        // Wait for ConfChange to commit (with 10 second timeout)
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(conf_state)) => {
                tracing::info!("✓ AddLearnerNode completed for peer {}, learners: {:?}", peer_id, conf_state.learners);
                Ok(conf_state)
            }
            Ok(Err(_)) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    "ConfChange notification channel closed".to_string()
                )))
            }
            Err(_) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    format!("ConfChange timeout after 10s for learner {}", peer_id)
                )))
            }
        }
    }

    /// Promote a learner to voter
    /// Should only be called after verifying the learner is caught up.
    /// Blocks until the ConfChange is committed and applied, or times out after 10 seconds.
    /// Returns the new ConfState after the change is applied.
    pub async fn promote_learner(&self, peer_id: u64) -> Result<raft::prelude::ConfState> {
        tracing::info!("Promoting learner {} to voter", peer_id);

        // Check if learner is caught up first
        if !self.is_learner_caught_up(peer_id).await? {
            return Err(crate::error::OctopiiError::Raft(
                raft::Error::ConfChangeError("Learner is not caught up yet, cannot promote".to_string())
            ));
        }

        // Create oneshot channel to wait for completion
        let (tx, rx) = oneshot::channel();
        self.pending_conf_changes.write().await.insert(peer_id, tx);

        // Propose ConfChange to promote learner to voter
        let mut cc = raft::prelude::ConfChange::default();
        cc.set_change_type(raft::prelude::ConfChangeType::AddNode);
        cc.node_id = peer_id;

        self.raft.propose_conf_change(vec![], cc).await?;
        tracing::info!("Proposed promotion of learner {} to voter", peer_id);

        // Wait for ConfChange to commit (with 10 second timeout)
        let conf_state = match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(conf_state)) => {
                tracing::info!("✓ Promotion completed for peer {}, voters: {:?}", peer_id, conf_state.voters);
                conf_state
            }
            Ok(Err(_)) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                return Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    "ConfChange notification channel closed".to_string()
                )));
            }
            Err(_) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                return Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    format!("ConfChange timeout after 10s for promoting learner {}", peer_id)
                )));
            }
        };

        // CRITICAL FIX: Wait for joint consensus to exit before allowing next ConfChange
        // When promoting a learner to voter, raft-rs enters joint consensus with auto_leave=true.
        // The cluster must fully exit joint consensus before the next ConfChange can be proposed.
        // Check if voters_outgoing is non-empty (indicates joint consensus state)
        if !conf_state.voters_outgoing.is_empty() {
            tracing::info!("In joint consensus after promoting learner {} (voters_outgoing: {:?}), waiting for auto-leave...",
                peer_id, conf_state.voters_outgoing);

            // Wait for joint consensus to exit (voters_outgoing becomes empty)
            let start = std::time::Instant::now();
            let joint_timeout = Duration::from_secs(15);

            loop {
                if start.elapsed() > joint_timeout {
                    return Err(OctopiiError::Raft(raft::Error::ConfChangeError(format!(
                        "Joint consensus did not exit after {}s for peer {}",
                        joint_timeout.as_secs(),
                        peer_id
                    ))));
                }

                // Give time for auto-leave to propose and apply the exit from joint consensus
                tokio::time::sleep(Duration::from_millis(500)).await;

                // Check if we've exited joint consensus by getting current raft status
                let raft_read = self.raft.raw_node().read().await;
                let status = raft_read.status();

                // Check if voters_outgoing is empty (exited joint consensus)
                // Status.progress contains ProgressTracker, which has Configuration
                let still_in_joint = if let Some(progress) = status.progress {
                    // ProgressTracker has conf() method that returns Configuration
                    // Configuration.to_conf_state() converts to ConfState with voters_outgoing field
                    let conf_state = progress.conf().to_conf_state();
                    !conf_state.voters_outgoing.is_empty()
                } else {
                    false
                };

                drop(raft_read); // Release lock before continuing

                if !still_in_joint {
                    tracing::info!("✓ Exited joint consensus for peer {} after {:?}", peer_id, start.elapsed());
                    break;
                }

                tracing::trace!("Still in joint consensus for peer {}, waiting... (elapsed: {:?})", peer_id, start.elapsed());
            }
        }

        Ok(conf_state)
    }

    /// Check if a learner is caught up with the leader
    /// A learner is considered caught up if it's within 100 entries of the leader
    pub async fn is_learner_caught_up(&self, learner_id: u64) -> Result<bool> {
        // Check if we're the leader
        if !self.raft.is_leader().await {
            return Err(crate::error::OctopiiError::Raft(
                raft::Error::ConfChangeError("Only leader can check learner progress".to_string())
            ));
        }

        // Get progress for this learner
        if let Some((matched, leader_last_index)) = self.raft.peer_progress(learner_id).await {
            let lag = leader_last_index.saturating_sub(matched);

            tracing::debug!(
                "Learner {} progress: matched={}, leader_last={}, lag={}",
                learner_id,
                matched,
                leader_last_index,
                lag
            );

            // Consider caught up if within 100 entries
            Ok(lag < 100)
        } else {
            tracing::warn!("Learner {} not found in progress tracking", learner_id);
            Ok(false)
        }
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
            .map(|r| {
                r.unwrap_or_else(|e| {
                    TransferResult::failure(
                        "0.0.0.0:0".parse().unwrap(),
                        format!("Task panicked: {}", e),
                    )
                })
            })
            .collect()
    }

    /// Set up RPC request handler for application-level RPCs
    /// Note: Raft RPCs are handled directly in the network acceptor for async processing
    async fn setup_rpc_handler(&self) {
        tracing::info!("Setting up application RPC handler");

        self.rpc
            .set_request_handler(move |req: RpcRequest| {
                match &req.payload {
                    RequestPayload::AppendEntries { .. }
                    | RequestPayload::RequestVote { .. }
                    | RequestPayload::RaftSnapshot { .. } => {
                        // Raft messages are handled in network acceptor, this shouldn't be reached
                        tracing::warn!(
                            "Raft message reached RPC handler (should be handled in acceptor)"
                        );
                        ResponsePayload::Error {
                            message: "Raft messages handled separately".to_string(),
                        }
                    }
                    RequestPayload::Custom { operation, data } => {
                        tracing::debug!(
                            "Handling custom RPC: operation={}, data={} bytes",
                            operation,
                            data.len()
                        );
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
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tracing::info!("Spawning network acceptor for node {}", node_id);

        self.runtime.spawn(async move {
            loop {
                tokio::select! {
                    recv = shutdown_rx.recv() => {
                        let _ = recv;
                        tracing::info!("Network acceptor shutting down");
                        break;
                    }
                    accept_res = transport.accept() => {
                        match accept_res {
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
                                                                // The main ready handler will process the resulting ready state,
                                                                // persist entries, and send response messages
                                                                match raft_clone.step(raft_msg).await {
                                                                    Ok(_) => {
                                                                        tracing::trace!("Raft message processed successfully");
                                                                        // Send immediate ACK to unblock the RPC call
                                                                        // The actual Raft response will be sent by the ready handler
                                                                        let ack = RpcMessage::new_response(
                                                                            req.id,
                                                                            ResponsePayload::CustomResponse {
                                                                                success: true,
                                                                                data: bytes::Bytes::new(),
                                                                            }
                                                                        );
                                                                        if let Ok(data) = crate::rpc::serialize(&ack) {
                                                                            if let Err(e) = peer.send(data).await {
                                                                                tracing::error!("Failed to send ACK: {}", e);
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        tracing::error!("Failed to step Raft: {}", e);
                                                                        // Send error response
                                                                        let err_resp = RpcMessage::new_response(
                                                                            req.id,
                                                                            ResponsePayload::CustomResponse {
                                                                                success: false,
                                                                                data: bytes::Bytes::new(),
                                                                            }
                                                                        );
                                                                        if let Ok(data) = crate::rpc::serialize(&err_resp) {
                                                                            let _ = peer.send(data).await;
                                                                        }
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
                }
            }
        });
    }

    /// Spawn task to tick Raft periodically
    fn spawn_raft_ticker(&self) {
        let raft = Arc::clone(&self.raft);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tracing::info!("Spawning Raft ticker (100ms interval) with automatic leader election");

        // Randomize election timeout between 15-30 ticks (1.5-3 seconds)
        // This prevents split votes when multiple nodes lose leader simultaneously
        use rand::Rng;
        let max_ticks_without_leader: u64 = rand::thread_rng().gen_range(15..=30);

        self.runtime.spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            let mut ticks_without_leader = 0u64;

            loop {
                tokio::select! {
                    recv = shutdown_rx.recv() => {
                        let _ = recv;
                        tracing::info!("Raft ticker shutting down");
                        break;
                    }
                    _ = ticker.tick() => {
                        raft.tick().await;
                        tracing::trace!("Raft ticked");

                        // Automatic leader election with randomized timeout
                        if !raft.has_leader().await {
                            ticks_without_leader += 1;
                            if ticks_without_leader >= max_ticks_without_leader {
                                tracing::warn!(
                                    "No leader detected for {} ticks ({}ms), triggering election",
                                    ticks_without_leader,
                                    ticks_without_leader * 100
                                );
                                if let Err(e) = raft.campaign().await {
                                    tracing::error!("Failed to start election campaign: {}", e);
                                }
                                ticks_without_leader = 0;
                            }
                        } else {
                            ticks_without_leader = 0;
                        }
                    }
                }
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
        let pending_conf_changes = Arc::clone(&self.pending_conf_changes);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tracing::info!("Spawning Raft ready handler");

        self.runtime.spawn(async move {
            let mut checker = interval(Duration::from_millis(10));
            let mut check_count = 0;
            'main: loop {
                tokio::select! {
                    recv = shutdown_rx.recv() => {
                        let _ = recv;
                        tracing::info!("Raft ready handler shutting down");
                        break 'main;
                    }
                    _ = checker.tick() => {
                        check_count += 1;
                        if check_count % 100 == 0 {
                            tracing::debug!("Ready handler alive, checked {} times", check_count);
                        }
                    }
                }

                let ready_opt = tokio::select! {
                    recv = shutdown_rx.recv() => {
                        let _ = recv;
                        tracing::info!("Raft ready handler shutting down (waiting for ready)");
                        break 'main;
                    }
                    ready = raft.ready() => ready,
                };

                if let Some(ready) = ready_opt {
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
                                tracing::info!(
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
                            let parse_result: protobuf::ProtobufResult<raft::prelude::ConfChange> =
                                protobuf::Message::parse_from_bytes(&entry.data);
                            match parse_result {
                                Ok(cc) => {
                                    let node_id = cc.node_id;
                                    match raft.apply_conf_change(&cc).await {
                                        Ok(conf_state) => {
                                            tracing::info!("✓ ConfChange applied: {:?} for node {}, voters: {:?}",
                                                cc.get_change_type(), node_id, conf_state.voters);

                                            // Notify any waiting caller
                                            if let Some(tx) = pending_conf_changes.write().await.remove(&node_id) {
                                                if tx.send(conf_state.clone()).is_err() {
                                                    tracing::warn!("ConfChange waiter dropped for node {}", node_id);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to apply ConfChange: {}", e);
                                            // Remove pending waiter on error
                                            pending_conf_changes.write().await.remove(&node_id);
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

    /// Trigger graceful shutdown for background tasks and transport
    fn initiate_shutdown(&self) {
        if self.shutdown_triggered.swap(true, Ordering::SeqCst) {
            return;
        }

        let _ = self.shutdown_tx.send(());
        self.transport.close();
    }

    /// Public shutdown helper (used in tests)
    pub fn shutdown(&self) {
        self.initiate_shutdown();
    }
}

impl Drop for OctopiiNode {
    fn drop(&mut self) {
        self.initiate_shutdown();
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
