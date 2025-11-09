use crate::chunk::{ChunkSource, TransferResult};
use crate::config::Config;
use crate::error::{OctopiiError, Result};
use crate::raft::{
    raft_message_to_rpc, rpc_to_raft_message, KvStateMachine, RaftNode, StateMachine, WalStorage,
};
use crate::rpc::{
    deserialize, RequestPayload, ResponsePayload, RpcHandler, RpcMessage, RpcRequest,
};
use crate::runtime::OctopiiRuntime;
use crate::transport::{PeerConnection, QuicTransport};
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::future::join_all;
use raft::prelude::{Entry, Message};
use rkyv::Deserialize;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio::task::{yield_now, JoinHandle};
use tokio::time::{interval, timeout, Duration};

/// Filter trait for network simulation (primarily used in testing)
/// This is public but typically only used via test infrastructure
pub trait MessageFilter: Send + Sync {
    fn before(&self, msgs: &mut Vec<Message>) -> std::result::Result<(), String>;
}

/// Metadata for a pending proposal (bounded queue to prevent unbounded growth)
struct ProposalMetadata {
    responder: oneshot::Sender<Result<Bytes>>,
}

/// Bounded proposal queue using VecDeque with max capacity
struct ProposalQueue {
    queue: StdMutex<VecDeque<ProposalMetadata>>,
    max_size: usize,
}

impl ProposalQueue {
    fn new(max_size: usize) -> Self {
        Self {
            queue: StdMutex::new(VecDeque::with_capacity(max_size)),
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
    state_machine: StateMachine,
    raft_msg_tx: mpsc::UnboundedSender<Message>,
    raft_msg_rx: StdMutex<Option<mpsc::UnboundedReceiver<Message>>>,
    /// Map peer ID to socket address
    peer_addrs: Arc<RwLock<HashMap<u64, SocketAddr>>>,
    /// Bounded queue for pending proposals (max 10000 in-flight proposals)
    proposal_queue: Arc<ProposalQueue>,
    /// Pending ConfChange completions - notifies callers when membership changes commit
    pending_conf_changes: Arc<RwLock<HashMap<u64, oneshot::Sender<raft::prelude::ConfState>>>>,
    /// Broadcast channel used to signal background tasks to shut down gracefully
    shutdown_tx: broadcast::Sender<()>,
    /// Flag ensuring shutdown is triggered at most once
    shutdown_triggered: AtomicBool,
    /// Network filters for testing (primarily used in test mode but compiled always for simplicity)
    pub send_filters: Arc<RwLock<Vec<Box<dyn MessageFilter>>>>,
    /// Handle to the Raft event loop task (for graceful shutdown)
    event_loop_handle: StdMutex<Option<JoinHandle<()>>>,
    /// Handle to the network acceptor task (for graceful shutdown)
    network_acceptor_handle: StdMutex<Option<JoinHandle<()>>>,
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

        // Create default KV state machine with WAL backing (enables durability!)
        let state_machine: StateMachine = Arc::new(KvStateMachine::with_wal(Arc::clone(&wal)));

        // Create bounded message type tracker (1024 in-flight messages max)

        // Create bounded proposal queue (10000 max in-flight proposals)
        let proposal_queue = Arc::new(ProposalQueue::new(10000));

        // Create pending ConfChange tracker
        let pending_conf_changes = Arc::new(RwLock::new(HashMap::new()));

        // Channel for inbound Raft messages (delivered to the event loop)
        let (raft_msg_tx, raft_msg_rx) = mpsc::unbounded_channel();

        // Channel for signaling graceful shutdown to background tasks
        let (shutdown_tx, _) = broadcast::channel(16);

        let node = Self {
            runtime,
            config,
            transport,
            rpc,
            raft,
            state_machine,
            raft_msg_tx,
            raft_msg_rx: StdMutex::new(Some(raft_msg_rx)),
            peer_addrs: Arc::new(RwLock::new(peer_addrs_map)),
            proposal_queue,
            pending_conf_changes,
            shutdown_tx,
            shutdown_triggered: AtomicBool::new(false),
            send_filters: Arc::new(RwLock::new(Vec::new())),
            event_loop_handle: StdMutex::new(None),
            network_acceptor_handle: StdMutex::new(None),
        };

        tracing::info!(
            "Octopii node {} created: bind_addr={}, peers={:?}",
            node.config.node_id,
            node.config.bind_addr,
            node.config.peers
        );

        Ok(node)
    }

    /// Create a new Octopii node with a custom state machine (async version)
    ///
    /// This allows you to use your own state machine implementation instead of
    /// the default KV store. Your state machine must implement the StateMachineTrait.
    ///
    /// # Example
    /// ```ignore
    /// use octopii::{OctopiiNode, Config, OctopiiRuntime, StateMachine};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let runtime = OctopiiRuntime::new(4);
    ///     let config = Config { /* ... */ };
    ///     let custom_sm: StateMachine = Arc::new(MyCustomStateMachine::new());
    ///     let node = OctopiiNode::new_with_state_machine(config, runtime, custom_sm).await.unwrap();
    /// }
    /// ```
    pub async fn new_with_state_machine(
        config: Config,
        runtime: OctopiiRuntime,
        state_machine: StateMachine,
    ) -> Result<Self> {
        // Initialize components (same as new() but skip state machine creation)
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

        // Build peer ID mapping
        let mut peer_addrs_map = HashMap::new();
        for (idx, addr) in config.peers.iter().enumerate() {
            let peer_id = (idx + 1) as u64;
            let peer_id_adjusted = if peer_id >= config.node_id {
                peer_id + 1
            } else {
                peer_id
            };
            peer_addrs_map.insert(peer_id_adjusted, *addr);
            tracing::info!("Mapped peer {} -> {}", peer_id_adjusted, addr);
        }

        let mut peer_ids: Vec<u64> = peer_addrs_map.keys().copied().collect();
        peer_ids.sort_unstable();

        let raft = Arc::new(
            RaftNode::new(config.node_id, peer_ids, storage, config.is_initial_leader).await?,
        );

        // Use the provided custom state machine instead of creating KvStateMachine
        // Note: state_machine is already Arc<dyn StateMachineTrait>

        // Create bounded proposal queue
        let proposal_queue = Arc::new(ProposalQueue::new(10000));

        // Create pending ConfChange tracker
        let pending_conf_changes = Arc::new(RwLock::new(HashMap::new()));

        // Channel for inbound Raft messages
        let (raft_msg_tx, raft_msg_rx) = mpsc::unbounded_channel();

        // Channel for signaling graceful shutdown
        let (shutdown_tx, _) = broadcast::channel(16);

        let node = Self {
            runtime,
            config: config.clone(),
            transport,
            rpc,
            raft,
            state_machine,
            raft_msg_tx,
            raft_msg_rx: StdMutex::new(Some(raft_msg_rx)),
            peer_addrs: Arc::new(RwLock::new(peer_addrs_map)),
            proposal_queue,
            pending_conf_changes,
            shutdown_tx,
            shutdown_triggered: AtomicBool::new(false),
            send_filters: Arc::new(RwLock::new(Vec::new())),
            event_loop_handle: StdMutex::new(None),
            network_acceptor_handle: StdMutex::new(None),
        };

        tracing::info!(
            "Octopii node {} created with custom state machine: bind_addr={}, peers={:?}",
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

        // Start the consolidated Raft event loop
        let raft_msg_rx = self
            .raft_msg_rx
            .lock()
            .expect("Raft inbox mutex poisoned")
            .take()
            .ok_or_else(|| {
                OctopiiError::Rpc("Raft event loop already running for this node".to_string())
            })?;
        self.spawn_raft_event_loop(raft_msg_rx);

        // Spawn network acceptor task
        self.spawn_network_acceptor();

        Ok(())
    }

    /// Propose a change to the distributed state machine
    /// Returns a future that resolves when the proposal is committed and applied
    pub async fn propose(&self, command: Vec<u8>) -> Result<Bytes> {
        tracing::info!("Proposing command to Raft: {} bytes", command.len());

        // CRITICAL: Check if this node is the leader before accepting proposals
        // Non-leader nodes cannot commit proposals, which would cause the response
        // channel to hang indefinitely waiting for a commit that will never happen
        if !self.raft.is_leader().await {
            tracing::warn!("Rejecting proposal: node {} is not the leader", self.id());
            return Err(crate::error::OctopiiError::Rpc(
                "Not leader - proposals must be sent to the current leader".to_string(),
            ));
        }

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

    /// Transfer leadership to another node (for planned maintenance or load balancing)
    ///
    /// This is a cooperative operation where the current leader hands off leadership
    /// to the specified target node. The target must be a follower and caught up.
    ///
    /// # Arguments
    /// * `target_id` - The node ID to transfer leadership to
    ///
    /// # Returns
    /// Ok if the transfer command was sent, Err if not currently leader
    pub async fn transfer_leader(&self, target_id: u64) -> Result<()> {
        self.raft.transfer_leader(target_id).await
    }

    /// Request a read index for linearizable reads
    ///
    /// This allows serving reads without going through the Raft log, while still
    /// guaranteeing linearizability. The leader confirms it's still the leader
    /// and returns a read index that can be used to serve consistent reads.
    ///
    /// # Arguments
    /// * `request_ctx` - Optional context bytes to identify this read request
    ///
    /// # Returns
    /// Ok if the read index request was submitted
    pub async fn read_index(&self, request_ctx: Vec<u8>) -> Result<()> {
        self.raft.read_index(request_ctx).await
    }

    /// Add a peer to the Raft cluster dynamically
    /// This proposes a ConfChange that will be committed through Raft consensus.
    /// Blocks until the ConfChange is committed and applied, or times out after 10 seconds.
    /// Returns the new ConfState after the change is applied.
    pub async fn add_peer(
        &self,
        peer_id: u64,
        addr: SocketAddr,
    ) -> Result<raft::prelude::ConfState> {
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
                tracing::info!(
                    "✓ AddNode completed for peer {}, voters: {:?}",
                    peer_id,
                    conf_state.voters
                );
                Ok(conf_state)
            }
            Ok(Err(_)) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    "ConfChange notification channel closed".to_string(),
                )))
            }
            Err(_) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(format!(
                    "ConfChange timeout after 10s for peer {}",
                    peer_id
                ))))
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
    pub async fn add_learner(
        &self,
        peer_id: u64,
        addr: SocketAddr,
    ) -> Result<raft::prelude::ConfState> {
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
                tracing::info!(
                    "✓ AddLearnerNode completed for peer {}, learners: {:?}",
                    peer_id,
                    conf_state.learners
                );
                Ok(conf_state)
            }
            Ok(Err(_)) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    "ConfChange notification channel closed".to_string(),
                )))
            }
            Err(_) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                Err(OctopiiError::Raft(raft::Error::ConfChangeError(format!(
                    "ConfChange timeout after 10s for learner {}",
                    peer_id
                ))))
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
                raft::Error::ConfChangeError(
                    "Learner is not caught up yet, cannot promote".to_string(),
                ),
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
                tracing::info!(
                    "✓ Promotion completed for peer {}, voters: {:?}",
                    peer_id,
                    conf_state.voters
                );
                conf_state
            }
            Ok(Err(_)) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                return Err(OctopiiError::Raft(raft::Error::ConfChangeError(
                    "ConfChange notification channel closed".to_string(),
                )));
            }
            Err(_) => {
                self.pending_conf_changes.write().await.remove(&peer_id);
                return Err(OctopiiError::Raft(raft::Error::ConfChangeError(format!(
                    "ConfChange timeout after 10s for promoting learner {}",
                    peer_id
                ))));
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
                let raft_read = self.raft.raw_node().lock().await;
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
                    tracing::info!(
                        "✓ Exited joint consensus for peer {} after {:?}",
                        peer_id,
                        start.elapsed()
                    );
                    break;
                }

                tracing::trace!(
                    "Still in joint consensus for peer {}, waiting... (elapsed: {:?})",
                    peer_id,
                    start.elapsed()
                );
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
                raft::Error::ConfChangeError("Only leader can check learner progress".to_string()),
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

    /// Trigger log compaction to create a snapshot
    /// This is useful before adding learners to ensure they receive a snapshot
    pub async fn compact_log(&self) -> Result<()> {
        // Get current commit index from Raft
        let hs = self.raft.hard_state().await;
        let commit_index = hs.commit;

        // Get state machine snapshot data
        let snapshot = self.state_machine.snapshot();
        let snapshot_data = bincode::serialize(&snapshot).map_err(|e| {
            crate::error::OctopiiError::Wal(format!("Failed to serialize snapshot: {}", e))
        })?;

        // Compact the Raft log
        self.raft
            .try_compact_logs(commit_index, snapshot_data)
            .await?;

        tracing::info!("Log compacted at commit_index={}", commit_index);
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

        let raft_sender = self.raft_msg_tx.clone();
        let node_id = self.config.node_id;

        self.rpc
            .set_request_handler(move |req: RpcRequest| match &req.payload {
                RequestPayload::RaftMessage { .. } => {
                    if let Some(raft_msg) = rpc_to_raft_message(0, node_id, &req.payload) {
                        match raft_sender.send(raft_msg) {
                            Ok(_) => ResponsePayload::CustomResponse {
                                success: true,
                                data: Bytes::new(),
                            },
                            Err(e) => {
                                tracing::error!("Failed to enqueue Raft message: {}", e);
                                ResponsePayload::CustomResponse {
                                    success: false,
                                    data: Bytes::new(),
                                }
                            }
                        }
                    } else {
                        tracing::error!("Failed to decode Raft message in RPC handler");
                        ResponsePayload::CustomResponse {
                            success: false,
                            data: Bytes::new(),
                        }
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
            })
            .await;
    }

    /// Spawn task to accept incoming network connections
    fn spawn_network_acceptor(&self) {
        let transport = Arc::clone(&self.transport);
        let rpc = Arc::clone(&self.rpc);
        let raft_msg_tx = self.raft_msg_tx.clone();
        let node_id = self.config.node_id;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tracing::info!("Spawning network acceptor for node {}", node_id);

        let handle = self.runtime.spawn(async move {
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
                                let raft_sender = raft_msg_tx.clone();
                                let peer = Arc::new(peer);

                                tokio::spawn(async move {
                                    while let Ok(Some(data)) = peer.recv().await {
                                        if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                            // Handle Raft messages directly for async processing
                                            match &msg {
                                                RpcMessage::Request(req) => {
                                                match &req.payload {
                                                    RequestPayload::RaftMessage { .. } => {
                                                        if let Some(raft_msg) = rpc_to_raft_message(0, node_id, &req.payload) {
                                                            tracing::info!(
                                                                "Received Raft message from {}: {:?}",
                                                                addr,
                                                                raft_msg.get_msg_type()
                                                            );

                                                            match raft_sender.send(raft_msg) {
                                                                Ok(_) => {
                                                                    let ack = RpcMessage::new_response(
                                                                        req.id,
                                                                        ResponsePayload::CustomResponse {
                                                                            success: true,
                                                                            data: bytes::Bytes::new(),
                                                                        }
                                                                    );
                                                                    Self::spawn_rpc_response(Arc::clone(&peer), ack);
                                                                }
                                                                Err(e) => {
                                                                    tracing::error!("Failed to enqueue Raft message: {}", e);
                                                                    let err_resp = RpcMessage::new_response(
                                                                        req.id,
                                                                        ResponsePayload::CustomResponse {
                                                                            success: false,
                                                                            data: bytes::Bytes::new(),
                                                                        }
                                                                    );
                                                                    Self::spawn_rpc_response(Arc::clone(&peer), err_resp);
                                                                }
                                                            }
                                                        } else {
                                                            tracing::error!("Failed to decode Raft message from {}", addr);
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
                                        // Yield to allow ready handler/ticker to run between bursts
                                        yield_now().await;
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

        // Store the handle for graceful shutdown
        *self.network_acceptor_handle.lock().unwrap() = Some(handle);
    }

    /// Spawn the unified Raft event loop that owns the RawNode
    fn spawn_raft_event_loop(&self, mut raft_rx: mpsc::UnboundedReceiver<Message>) {
        let raft = Arc::clone(&self.raft);
        let state_machine = Arc::clone(&self.state_machine);
        let rpc = Arc::clone(&self.rpc);
        let peer_addrs = Arc::clone(&self.peer_addrs);
        let proposal_queue = Arc::clone(&self.proposal_queue);
        let pending_conf_changes = Arc::clone(&self.pending_conf_changes);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let ready_notify = self.raft.ready_notifier();
        let send_filters = Arc::clone(&self.send_filters);

        tracing::info!("Spawning consolidated Raft event loop");

        use rand::Rng;
        let max_ticks_without_leader: u64 = rand::thread_rng().gen_range(15..=30);

        let handle = self.runtime.spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            let mut ticks_without_leader = 0u64;

            loop {
                let mut process_ready = false;

                tokio::select! {
                    recv = shutdown_rx.recv() => {
                        let _ = recv;
                        tracing::info!("Raft event loop shutting down");
                        break;
                    }
                    maybe_msg = raft_rx.recv() => {
                        match maybe_msg {
                            Some(msg) => {
                                let msg_type = msg.get_msg_type();
                                let msg_from = msg.from;
                                let msg_to = msg.to;
                                let msg_index = msg.index;
                                let msg_commit = msg.commit;
                                if let Err(e) = raft.step(msg).await {
                                    tracing::error!("Failed to step Raft message {:?}: {}", msg_type, e);
                                } else {
                                    tracing::info!(
                                        "[Node {}] Stepped {:?} from {} (index={}, commit={}), will process_ready",
                                        msg_to, msg_type, msg_from, msg_index, msg_commit
                                    );
                                    process_ready = true;
                                }
                            }
                            None => {
                                tracing::warn!("Raft message channel closed, stopping event loop");
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        raft.tick().await;
                        tracing::trace!("Raft ticked");
                        process_ready = true;

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
                    _ = ready_notify.notified() => {
                        process_ready = true;
                    }
                }

                if process_ready {
                    Self::handle_raft_ready(
                        &raft,
                        &state_machine,
                        &rpc,
                        &peer_addrs,
                        &proposal_queue,
                        &pending_conf_changes,
                        Some(&send_filters),
                    )
                    .await;
                }
            }

            tracing::info!("Raft event loop exited");
        });

        // Store the handle for graceful shutdown
        *self.event_loop_handle.lock().unwrap() = Some(handle);
    }

    fn spawn_rpc_response(peer: Arc<PeerConnection>, response: RpcMessage) {
        tokio::spawn(async move {
            match crate::rpc::serialize(&response) {
                Ok(data) => match timeout(Duration::from_secs(2), peer.send(data)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::error!("Failed to send RPC response: {}", e),
                    Err(_) => tracing::warn!("Timed out sending RPC response"),
                },
                Err(e) => tracing::error!("Failed to serialize RPC response: {}", e),
            }
        });
    }

    /// Drain and handle all pending Ready states from the RawNode
    async fn handle_raft_ready(
        raft: &Arc<RaftNode>,
        state_machine: &StateMachine,
        rpc: &Arc<RpcHandler>,
        peer_addrs: &Arc<RwLock<HashMap<u64, SocketAddr>>>,
        proposal_queue: &Arc<ProposalQueue>,
        pending_conf_changes: &Arc<RwLock<HashMap<u64, oneshot::Sender<raft::prelude::ConfState>>>>,
        send_filters: Option<&Arc<RwLock<Vec<Box<dyn MessageFilter>>>>>,
    ) {
        loop {
            let ready_opt = raft.ready().await;
            let Some(mut ready) = ready_opt else {
                break;
            };

            let node_id = raft.id();
            tracing::info!(
                "[Node {}] Raft ready: has {} messages, {} committed entries, {} new entries, snapshot: {}",
                node_id,
                ready.messages().len(),
                ready.committed_entries().len(),
                ready.entries().len(),
                !ready.snapshot().is_empty()
            );

            let mut last_applied_index = None;

            // CRITICAL: Send ready.messages() FIRST, before any persistence operations.
            // These are immediate messages (votes, heartbeats, etc.) that must go out immediately.
            // This is the TiKV/raft-rs pattern - see raft-rs/examples/five_mem_node/main.rs:264-267
            Self::send_raft_messages(ready.take_messages(), rpc, peer_addrs, send_filters).await;

            // Apply snapshot if present (must happen before persisting entries)
            if !ready.snapshot().is_empty() {
                let snap = ready.snapshot();
                tracing::info!(
                    "[Node {}] Applying snapshot at index {} (term {})",
                    node_id,
                    snap.get_metadata().index,
                    snap.get_metadata().term
                );

                // Restore state machine from snapshot data
                // Let the state machine handle its own deserialization
                match state_machine.restore(&snap.data) {
                    Ok(()) => {
                        tracing::info!("✓ State machine restored from snapshot");

                        // Apply to Raft storage (persists to Walrus)
                        if let Err(e) = raft
                            .raw_node()
                            .lock()
                            .await
                            .store()
                            .apply_snapshot(snap.clone())
                        {
                            tracing::error!("Failed to apply snapshot to storage: {}", e);
                        } else {
                            last_applied_index = Some(snap.get_metadata().index);
                            tracing::info!(
                                "✓ Snapshot applied successfully: index {}",
                                snap.get_metadata().index
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to deserialize snapshot: {}", e);
                    }
                }
            }

            // Apply committed entries BEFORE persisting new entries
            // This follows the raft-rs example pattern
            let committed_entries = ready.take_committed_entries();
            tracing::info!(
                "[Node {}] Ready has {} committed entries to apply",
                node_id,
                committed_entries.len()
            );
            if let Some(idx) = Self::apply_committed_entries(
                committed_entries,
                raft,
                state_machine,
                proposal_queue,
                pending_conf_changes,
            )
            .await
            {
                last_applied_index = Some(idx);
            }

            // Persist new entries to storage
            if !ready.entries().is_empty() {
                tracing::info!(
                    "[Node {}] Persisting {} new entries to storage",
                    node_id,
                    ready.entries().len()
                );
                raft.persist_entries(ready.entries()).await;
            }

            // Persist HardState
            if let Some(hs) = ready.hs() {
                tracing::debug!(
                    "[Node {}] Persisting HardState: term={}, vote={}, commit={}",
                    node_id,
                    hs.term,
                    hs.vote,
                    hs.commit
                );
                raft.persist_hard_state(hs.clone()).await;
            }

            // CRITICAL: Send persisted_messages() AFTER persistence completes.
            // These messages (like MsgAppend with newly persisted entries) depend on persistence.
            // This unblocks log replication - followers won't hear about new entries until these go out.
            Self::send_raft_messages(
                ready.take_persisted_messages(),
                rpc,
                peer_addrs,
                send_filters,
            )
            .await;

            let mut light_rd = raft.advance(ready).await;

            tracing::info!(
                "LightReady: {} committed entries, {} messages",
                light_rd.committed_entries().len(),
                light_rd.messages().len()
            );

            // Send LightReady messages first (raft-rs pattern: line 341 before 343)
            Self::send_raft_messages(light_rd.take_messages(), rpc, peer_addrs, send_filters).await;

            // Then apply LightReady committed entries
            let light_committed = light_rd.take_committed_entries();
            if !light_committed.is_empty() {
                tracing::info!(
                    "[Node {}] LightReady has {} committed entries to apply",
                    node_id,
                    light_committed.len()
                );
            }
            if let Some(idx) = Self::apply_committed_entries(
                light_committed,
                raft,
                state_machine,
                proposal_queue,
                pending_conf_changes,
            )
            .await
            {
                last_applied_index = Some(idx);
            }

            if let Some(last_applied_index) = last_applied_index {
                if let Err(e) = state_machine.compact() {
                    tracing::warn!("State machine compaction failed: {}", e);
                }

                // Get snapshot from state machine (already serialized)
                let snapshot_data = state_machine.snapshot();

                if let Err(e) = raft
                    .try_compact_logs(last_applied_index, snapshot_data)
                    .await
                {
                    tracing::warn!("Log compaction failed: {}", e);
                }
            }

            raft.advance_apply().await;
        }
    }

    async fn send_raft_messages(
        mut messages: Vec<Message>,
        rpc: &Arc<RpcHandler>,
        peer_addrs: &Arc<RwLock<HashMap<u64, SocketAddr>>>,
        #[allow(unused_variables)] send_filters: Option<&Arc<RwLock<Vec<Box<dyn MessageFilter>>>>>,
    ) {
        if messages.is_empty() {
            return;
        }

        // Apply network filters (primarily used in testing)
        if let Some(filters) = send_filters {
            let filters_guard = filters.read().await;
            for filter in filters_guard.iter() {
                if let Err(e) = filter.before(&mut messages) {
                    tracing::warn!("Filter error: {}", e);
                }
            }
        }

        for msg in messages {
            let to = msg.to;
            let msg_type = msg.get_msg_type();
            let peer_addr = {
                let guard = peer_addrs.read().await;
                guard.get(&to).copied()
            };

            if let Some(peer_addr) = peer_addr {
                if let Some(payload) = raft_message_to_rpc(&msg) {
                    tracing::info!(
                        "Sending Raft message to peer {}: {:?} -> {}",
                        to,
                        msg_type,
                        peer_addr
                    );

                    let rpc_clone = Arc::clone(rpc);
                    tokio::spawn(async move {
                        if let Err(e) = rpc_clone
                            .request(peer_addr, payload, Duration::from_secs(5))
                            .await
                        {
                            tracing::warn!("Failed to send Raft message to {}: {}", peer_addr, e);
                        }
                    });
                }
            } else {
                tracing::warn!("No address found for peer {}", to);
            }
        }
    }

    async fn apply_committed_entries(
        entries: Vec<Entry>,
        raft: &Arc<RaftNode>,
        state_machine: &StateMachine,
        proposal_queue: &Arc<ProposalQueue>,
        pending_conf_changes: &Arc<RwLock<HashMap<u64, oneshot::Sender<raft::prelude::ConfState>>>>,
    ) -> Option<u64> {
        if entries.is_empty() {
            tracing::debug!("[Node {}] No committed entries to apply", raft.id());
            return None;
        }

        tracing::info!(
            "[Node {}] Applying {} committed entries (indices: {} to {})",
            raft.id(),
            entries.len(),
            entries.first().map(|e| e.index).unwrap_or(0),
            entries.last().map(|e| e.index).unwrap_or(0)
        );

        let mut last_applied_index = None;
        let is_leader = raft.is_leader().await;

        for entry in entries {
            last_applied_index = Some(entry.index);

            if entry.get_entry_type() == raft::prelude::EntryType::EntryConfChange {
                tracing::info!(
                    "Applying ConfChange entry: index={}, term={}",
                    entry.index,
                    entry.term
                );

                let parse_result: protobuf::ProtobufResult<raft::prelude::ConfChange> =
                    protobuf::Message::parse_from_bytes(&entry.data);
                match parse_result {
                    Ok(cc) => {
                        let node_id = cc.node_id;
                        match raft.apply_conf_change(&cc).await {
                            Ok(conf_state) => {
                                tracing::info!(
                                    "✓ ConfChange applied: {:?} for node {}, voters: {:?}",
                                    cc.get_change_type(),
                                    node_id,
                                    conf_state.voters
                                );
                                if let Some(tx) =
                                    pending_conf_changes.write().await.remove(&node_id)
                                {
                                    if tx.send(conf_state.clone()).is_err() {
                                        tracing::warn!(
                                            "ConfChange waiter dropped for node {}",
                                            node_id
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to apply ConfChange: {}", e);
                                pending_conf_changes.write().await.remove(&node_id);
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to decode ConfChange: {:?}", e);
                    }
                }
            } else if !entry.data.is_empty() {
                tracing::info!(
                    "Applying committed entry: index={}, term={}, {} bytes",
                    entry.index,
                    entry.term,
                    entry.data.len()
                );

                let apply_result = state_machine.apply(&entry.data);

                if is_leader {
                    if let Some(proposal) = proposal_queue.pop() {
                        tracing::debug!(
                            "Responding to proposal, {} remaining in queue",
                            proposal_queue.len()
                        );

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

                        let _ = proposal.responder.send(response_result);
                    } else {
                        tracing::warn!(
                            "Committed entry but no pending proposal (possibly from follower sync)"
                        );
                    }
                } else {
                    match apply_result {
                        Ok(result) => tracing::info!("Entry applied successfully: {:?}", result),
                        Err(e) => tracing::error!("Failed to apply entry: {}", e),
                    }
                }
            }
        }

        last_applied_index
    }

    /// Trigger graceful shutdown for background tasks and transport
    fn initiate_shutdown(&self) {
        if self.shutdown_triggered.swap(true, Ordering::SeqCst) {
            return;
        }

        tracing::info!(
            "Initiating graceful shutdown for node {}",
            self.config.node_id
        );

        // Signal all background tasks to stop
        let _ = self.shutdown_tx.send(());

        // Close transport to stop accepting new connections
        self.transport.close();

        // Wait for background tasks to exit gracefully
        // This prevents zombie tasks from interfering with node restarts

        if let Some(handle) = self.event_loop_handle.lock().unwrap().take() {
            tracing::debug!("Waiting for event loop to exit...");
            if let Err(e) = futures::executor::block_on(handle) {
                tracing::error!("Event loop task panicked: {:?}", e);
            }
            tracing::debug!("Event loop exited");
        }

        if let Some(handle) = self.network_acceptor_handle.lock().unwrap().take() {
            tracing::debug!("Waiting for network acceptor to exit...");
            if let Err(e) = futures::executor::block_on(handle) {
                tracing::error!("Network acceptor task panicked: {:?}", e);
            }
            tracing::debug!("Network acceptor exited");
        }

        tracing::info!("Node {} shutdown complete", self.config.node_id);
    }

    /// Public shutdown helper (used in tests)
    pub fn shutdown(&self) {
        self.initiate_shutdown();
    }

    /// Add a send filter for network simulation (primarily used in testing)
    pub async fn add_send_filter(&self, filter: Box<dyn MessageFilter>) {
        self.send_filters.write().await.push(filter);
    }

    /// Clear all send filters (primarily used in testing)
    pub async fn clear_send_filters(&self) {
        self.send_filters.write().await.clear();
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
