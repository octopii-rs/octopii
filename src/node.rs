use crate::chunk::{ChunkSource, TransferResult};
use crate::config::Config;
use crate::error::Result;
use crate::raft::{RaftNode, StateMachine, WalStorage};
use crate::rpc::{deserialize, RpcHandler, RpcMessage, RpcRequest, ResponsePayload};
use crate::runtime::OctopiiRuntime;
use crate::transport::QuicTransport;
use crate::wal::WriteAheadLog;
use bytes::Bytes;
use futures::future::join_all;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::time::{interval, timeout, Duration};

/// Main Octopii node that orchestrates all components
pub struct OctopiiNode {
    runtime: OctopiiRuntime,
    config: Config,
    transport: Arc<QuicTransport>,
    rpc: Arc<RpcHandler>,
    raft: Arc<RaftNode>,
    state_machine: Arc<StateMachine>,
}

impl OctopiiNode {
    /// Create a new Octopii node
    pub async fn new(config: Config) -> Result<Self> {
        // Create isolated runtime
        let runtime = OctopiiRuntime::new(config.worker_threads);

        // Initialize components on the isolated runtime
        let transport = Arc::new(runtime.block_on(QuicTransport::new(config.bind_addr))?);
        let rpc = Arc::new(RpcHandler::new(Arc::clone(&transport)));

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
        let peer_ids: Vec<u64> = (1..=config.peers.len() as u64 + 1).collect();
        let raft = Arc::new(runtime.block_on(RaftNode::new(config.node_id, peer_ids, storage))?);

        // Create state machine
        let state_machine = Arc::new(StateMachine::new());

        let node = Self {
            runtime,
            config,
            transport,
            rpc,
            raft,
            state_machine,
        };

        // Set up RPC request handler
        node.setup_rpc_handler().await;

        Ok(node)
    }

    /// Start the node
    pub async fn start(&self) -> Result<()> {
        tracing::info!("Starting Octopii node {}", self.config.node_id);

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

    /// Set up RPC request handler
    async fn setup_rpc_handler(&self) {
        let _raft = Arc::clone(&self.raft);

        self.rpc
            .set_request_handler(move |req: RpcRequest| {
                // Handle Raft RPCs
                match &req.payload {
                    crate::rpc::RequestPayload::AppendEntries { .. } => {
                        // In a full implementation, convert to Raft message and call raft.step()
                        ResponsePayload::AppendEntriesResponse {
                            term: 0,
                            success: true,
                        }
                    }
                    crate::rpc::RequestPayload::RequestVote { .. } => {
                        ResponsePayload::RequestVoteResponse {
                            term: 0,
                            vote_granted: false,
                        }
                    }
                    crate::rpc::RequestPayload::Custom { .. } => ResponsePayload::CustomResponse {
                        success: true,
                        data: Bytes::from("OK"),
                    },
                }
            })
            .await;
    }

    /// Spawn task to accept incoming network connections
    fn spawn_network_acceptor(&self) {
        let transport = Arc::clone(&self.transport);
        let rpc = Arc::clone(&self.rpc);

        self.runtime.spawn(async move {
            loop {
                match transport.accept().await {
                    Ok((addr, peer)) => {
                        let rpc_clone = Arc::clone(&rpc);
                        tokio::spawn(async move {
                            while let Ok(Some(data)) = peer.recv().await {
                                if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                    let _ = rpc_clone.notify_message(addr, msg);
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

        self.runtime.spawn(async move {
            let mut ticker = interval(Duration::from_millis(100));
            loop {
                ticker.tick().await;
                raft.tick().await;
            }
        });
    }

    /// Spawn task to handle Raft ready events
    fn spawn_raft_ready_handler(&self) {
        let raft = Arc::clone(&self.raft);
        let state_machine = Arc::clone(&self.state_machine);

        self.runtime.spawn(async move {
            let mut checker = interval(Duration::from_millis(10));
            loop {
                checker.tick().await;

                if let Some(ready) = raft.ready().await {
                    // Apply committed entries to state machine
                    for entry in ready.committed_entries().iter() {
                        if !entry.data.is_empty() {
                            let _ = state_machine.apply(&entry.data);
                        }
                    }

                    // Advance Raft state
                    raft.advance(ready).await;
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_node_creation() {
        let config = Config {
            node_id: 1,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            peers: vec![],
            wal_dir: std::env::temp_dir().join("octopii_test"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
        };

        let node = OctopiiNode::new(config).await.unwrap();
        assert_eq!(node.id(), 1);

        // Clean up
        let _ = tokio::fs::remove_dir_all(std::env::temp_dir().join("octopii_test")).await;
    }
}
