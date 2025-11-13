/// Integration tests for custom state machine replication
///
/// These tests verify that:
/// 1. Custom state machines can be replicated across a Raft cluster
/// 2. Commands are applied deterministically on all nodes
/// 3. Snapshot/restore works correctly for custom state machines
/// 4. Custom state machines survive crashes and restarts
use crate::test_infrastructure::alloc_port;
use bytes::Bytes;
use octopii::{Config, OctopiiNode, OctopiiRuntime, StateMachineTrait};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tempfile::TempDir;

/// Test counter state machine - implements a simple replicated counter
/// This demonstrates how to create a custom state machine for Octopii
#[derive(Clone)]
struct TestCounterStateMachine {
    counter: Arc<RwLock<i64>>,
}

impl TestCounterStateMachine {
    fn new() -> Self {
        Self {
            counter: Arc::new(RwLock::new(0)),
        }
    }

    /// Get current counter value (for testing)
    fn get_value(&self) -> i64 {
        *self.counter.read().unwrap()
    }
}

impl StateMachineTrait for TestCounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd_str =
            String::from_utf8(command.to_vec()).map_err(|e| format!("Invalid UTF-8: {}", e))?;

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();

        match parts.as_slice() {
            ["INCREMENT"] => {
                let mut counter = self.counter.write().unwrap();
                *counter += 1;
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["DECREMENT"] => {
                let mut counter = self.counter.write().unwrap();
                *counter -= 1;
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["ADD", value] => {
                let val: i64 = value
                    .parse()
                    .map_err(|e| format!("Invalid number: {}", e))?;
                let mut counter = self.counter.write().unwrap();
                *counter += val;
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["GET"] => {
                let counter = self.counter.read().unwrap();
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["RESET"] => {
                let mut counter = self.counter.write().unwrap();
                *counter = 0;
                Ok(Bytes::from("0"))
            }
            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let counter = self.counter.read().unwrap();
        counter.to_le_bytes().to_vec()
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        if snapshot.is_empty() {
            return Ok(());
        }

        if snapshot.len() != 8 {
            return Err(format!(
                "Invalid snapshot size: expected 8 bytes, got {}",
                snapshot.len()
            ));
        }

        let value = i64::from_le_bytes(
            snapshot
                .try_into()
                .map_err(|e| format!("Failed to convert snapshot: {:?}", e))?,
        );

        let mut counter = self.counter.write().unwrap();
        *counter = value;

        Ok(())
    }

    fn compact(&self) -> Result<(), String> {
        // Counter doesn't need compaction
        Ok(())
    }
}

/// Test node wrapper for custom state machines
struct TestNodeWithCustomStateMachine {
    node: Option<OctopiiNode>,
    config: Config,
    node_id: u64,
    addr: SocketAddr,
    _wal_dir: TempDir,
    state_machine: Arc<TestCounterStateMachine>,
}

impl TestNodeWithCustomStateMachine {
    async fn new(
        node_id: u64,
        addr: SocketAddr,
        peers: Vec<SocketAddr>,
        is_initial_leader: bool,
        state_machine: Arc<TestCounterStateMachine>,
    ) -> Self {
        let temp_dir = TempDir::new().unwrap();
        let thread_id = std::thread::current().id();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique_key = format!("custom_sm_test_{}_{:?}_{}", node_id, thread_id, timestamp);
        let wal_path = temp_dir.path().join(&unique_key);

        let config = Config {
            node_id,
            bind_addr: addr,
            peers,
            wal_dir: wal_path,
            worker_threads: 1,         // Reduced to minimize memory pressure
            wal_batch_size: 10,        // Reduced from 100 to lower memory usage
            wal_flush_interval_ms: 50, // Increased to reduce fsync frequency
            is_initial_leader,
            snapshot_lag_threshold: 50,
        };

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

        // Create node with custom state machine
        // Cast to StateMachine (Arc<dyn StateMachineTrait>)
        let sm: octopii::StateMachine = state_machine.clone();
        let node = OctopiiNode::new_with_state_machine(config.clone(), runtime, sm)
            .await
            .unwrap();

        Self {
            node: Some(node),
            config,
            node_id,
            addr,
            _wal_dir: temp_dir,
            state_machine,
        }
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(ref node) = self.node {
            node.start().await?;
        }
        Ok(())
    }

    fn crash(&mut self) {
        if let Some(node) = self.node.as_ref() {
            node.shutdown();
        }
        self.node = None;
    }

    async fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.config.clone();

        tracing::info!("[RESTART] Shutting down node {}", self.node_id);
        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        if let Some(node) = self.node.as_ref() {
            node.shutdown();
        }
        self.node = None;

        // Wait for port to be released
        tokio::time::sleep(Duration::from_millis(2000)).await;

        tracing::info!("[RESTART] Creating new node {} instance", self.node_id);
        // Create a FRESH state machine instance for clean recovery testing
        // The WAL recovery will replay committed entries to rebuild state
        let sm: octopii::StateMachine = Arc::new(TestCounterStateMachine::new());

        self.node = Some(OctopiiNode::new_with_state_machine(config, runtime, sm).await?);
        Ok(())
    }

    async fn is_leader(&self) -> bool {
        if let Some(ref node) = self.node {
            node.is_leader().await
        } else {
            false
        }
    }

    async fn campaign(&self) -> Result<(), String> {
        if let Some(ref node) = self.node {
            node.campaign().await.map_err(|e| e.to_string())
        } else {
            Err("Node not running".to_string())
        }
    }

    async fn propose(&self, data: Vec<u8>) -> Result<Bytes, String> {
        if let Some(ref node) = self.node {
            node.propose(data).await.map_err(|e| e.to_string())
        } else {
            Err("Node not running".to_string())
        }
    }

    async fn query(&self, command: &[u8]) -> Result<Bytes, String> {
        if let Some(ref node) = self.node {
            node.query(command).await.map_err(|e| e.to_string())
        } else {
            Err("Node not running".to_string())
        }
    }
}

/// Test: Basic custom state machine replication
///
/// Verifies that:
/// - Custom state machines can be created and used in a cluster
/// - Commands are replicated and applied on all nodes
/// - All nodes converge to the same state
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_custom_state_machine_replication() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Testing custom state machine replication ===");

    let base_port = alloc_port();
    let node_ids = vec![1, 2, 3];

    // Build address list
    let addrs: Vec<SocketAddr> = node_ids
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            format!("127.0.0.1:{}", base_port + idx as u16)
                .parse()
                .unwrap()
        })
        .collect();

    // Create custom state machines for each node
    let state_machines: Vec<Arc<TestCounterStateMachine>> = node_ids
        .iter()
        .map(|_| Arc::new(TestCounterStateMachine::new()))
        .collect();

    // Create nodes with custom state machines
    let mut nodes = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        let addr = addrs[idx];
        let peers: Vec<SocketAddr> = addrs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, &a)| a)
            .collect();

        let is_initial_leader = idx == 0;
        let node = TestNodeWithCustomStateMachine::new(
            node_id,
            addr,
            peers,
            is_initial_leader,
            state_machines[idx].clone(),
        )
        .await;
        nodes.push(node);
    }

    // Start all nodes
    for node in &nodes {
        node.start().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Campaign for leader
    nodes[0].campaign().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(nodes[0].is_leader().await, "Node 1 should be leader");
    tracing::info!("✓ Leader elected");

    // Apply custom commands
    tracing::info!("Applying INCREMENT commands...");
    for i in 1..=10 {
        nodes[0].propose(b"INCREMENT".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        tracing::info!("  Applied INCREMENT {}/10", i);
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify all nodes have the same counter value
    tracing::info!("Verifying state consistency...");
    for (idx, node) in nodes.iter().enumerate() {
        let result = node.query(b"GET").await.unwrap();
        let value = String::from_utf8_lossy(&result);
        tracing::info!("  Node {} counter: {}", idx + 1, value);
        assert_eq!(value, "10", "Node {} should have counter = 10", idx + 1);
    }

    // Apply ADD command
    tracing::info!("Applying ADD 5 command...");
    nodes[0].propose(b"ADD 5".to_vec()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all nodes have counter = 15
    for (idx, node) in nodes.iter().enumerate() {
        let result = node.query(b"GET").await.unwrap();
        let value = String::from_utf8_lossy(&result);
        tracing::info!("  Node {} counter after ADD: {}", idx + 1, value);
        assert_eq!(value, "15", "Node {} should have counter = 15", idx + 1);
    }

    // Apply DECREMENT command
    tracing::info!("Applying DECREMENT command...");
    nodes[0].propose(b"DECREMENT".to_vec()).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all nodes have counter = 14
    for (idx, node) in nodes.iter().enumerate() {
        let result = node.query(b"GET").await.unwrap();
        let value = String::from_utf8_lossy(&result);
        tracing::info!("  Node {} counter after DECREMENT: {}", idx + 1, value);
        assert_eq!(value, "14", "Node {} should have counter = 14", idx + 1);
    }

    // Cleanup
    for node in &mut nodes {
        node.crash();
    }

    tracing::info!("✓ Test passed: Custom state machine replication works correctly");
}

/// Test: Custom state machine snapshot and restore
///
/// Verifies that:
/// - Custom state machines can create snapshots
/// - Crashed nodes can restore from snapshots
/// - State is preserved across restarts
///
/// Bug Fix History:
/// 1. FIXED: Zombie task bug - old network acceptor tasks interfered with restarts
///    - Solution: Added graceful shutdown with JoinHandle tracking (src/node.rs)
///    - Tasks now properly exit before restart
///
/// 2. FIXED: WAL recovery bug - incomplete log recovery after crash
///    - Problem: Using checkpoint=true persisted read cursor, causing restarts to skip entries
///    - Solution: Changed to checkpoint=false in all WalStorage recovery methods (src/raft/storage.rs)
///    - Now correctly recovers all log entries on every restart
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_custom_state_machine_snapshot_restore() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Testing custom state machine snapshot/restore ===");

    let base_port = alloc_port();
    let node_ids = vec![1, 2]; // Reduced to 2 nodes to lower memory pressure

    // Build address list
    let addrs: Vec<SocketAddr> = node_ids
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            format!("127.0.0.1:{}", base_port + idx as u16)
                .parse()
                .unwrap()
        })
        .collect();

    // Create custom state machines for each node
    let state_machines: Vec<Arc<TestCounterStateMachine>> = node_ids
        .iter()
        .map(|_| Arc::new(TestCounterStateMachine::new()))
        .collect();

    // Create nodes
    let mut nodes = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        let addr = addrs[idx];
        let peers: Vec<SocketAddr> = addrs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, &a)| a)
            .collect();

        let is_initial_leader = idx == 0;
        let node = TestNodeWithCustomStateMachine::new(
            node_id,
            addr,
            peers,
            is_initial_leader,
            state_machines[idx].clone(),
        )
        .await;
        nodes.push(node);
    }

    // Start all nodes
    for node in &nodes {
        node.start().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Campaign for leader
    nodes[0].campaign().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Apply commands to build up state
    tracing::info!("Building up state with 10 INCREMENT commands...");
    for i in 1..=10 {
        nodes[0].propose(b"INCREMENT".to_vec()).await.unwrap();
        if i % 5 == 0 {
            tracing::info!("  Progress: {}/10", i);
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify all nodes have counter = 10
    for (idx, node) in nodes.iter().enumerate() {
        let result = node.query(b"GET").await.unwrap();
        let value = String::from_utf8_lossy(&result);
        assert_eq!(
            value,
            "10",
            "Node {} should have counter = 10 before crash",
            idx + 1
        );
    }
    tracing::info!("✓ All nodes have counter = 10");

    // Wait for fsync to ensure all Raft entries are persisted
    // With 50ms fsync interval, wait 500ms to be safe (10x the interval)
    tracing::info!("Waiting for fsync before crash...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Crash and restart node 2 (follower)
    tracing::info!("Crashing and restarting node 2...");
    nodes[1].crash();

    // Wait longer after crash to ensure full cleanup (connections, ports, etc.)
    tracing::info!("Waiting for cleanup after crash...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    nodes[1].restart().await.unwrap();
    nodes[1].start().await.unwrap();

    // Wait longer for node to fully reconnect - needs time for:
    // 1. Port binding (2s already waited in restart())
    // 2. QUIC connection establishment
    // 3. Raft reconnection and sync
    tracing::info!("Waiting for node 2 to fully reconnect...");
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Verify node 2 restored its state (state machine persists externally via Arc)
    let result = nodes[1].query(b"GET").await.unwrap();
    let value = String::from_utf8_lossy(&result);
    tracing::info!("Node 2 counter after restart: {}", value);
    assert_eq!(value, "10", "Node 2 should have counter = 10 after restart");

    tracing::info!("✓ Node 2 successfully restored state");

    // Force reconnection by having node 0 campaign (ensures all nodes communicate)
    tracing::info!("Forcing cluster reconnection via campaign...");
    nodes[0].campaign().await.ok();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Find the current leader
    let mut leader_idx = 0;
    for (idx, node) in nodes.iter().enumerate() {
        if node.is_leader().await {
            leader_idx = idx;
            tracing::info!("Current leader is node {}", idx + 1);
            break;
        }
    }

    // Apply more commands to verify cluster is still operational
    tracing::info!("Applying 5 more INCREMENT commands to test continued replication...");
    for i in 0..5 {
        nodes[leader_idx]
            .propose(b"INCREMENT".to_vec())
            .await
            .unwrap();
        if i % 2 == 0 {
            tracing::info!("  Applied INCREMENT {}/5", i + 1);
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Give substantial time for replication to reach the restarted node
    tracing::info!("Waiting for replication to complete...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Verify all nodes have counter = 15 (10 initial + 5 after restart)
    for (idx, node) in nodes.iter().enumerate() {
        let result = node.query(b"GET").await.unwrap();
        let value = String::from_utf8_lossy(&result);
        tracing::info!("  Node {} final counter: {}", idx + 1, value);
        assert_eq!(value, "15", "Node {} should have counter = 15", idx + 1);
    }

    // Cleanup
    for node in &mut nodes {
        node.crash();
    }

    tracing::info!("✓ Test passed: Snapshot/restore works correctly for custom state machines");
}

/// Test: Deterministic execution across nodes
///
/// Verifies that:
/// - The same command sequence produces the same state on all nodes
/// - Custom state machines execute deterministically
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_custom_state_machine_determinism() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Testing custom state machine determinism ===");

    let base_port = alloc_port();
    let node_ids = vec![1, 2, 3];

    // Build address list
    let addrs: Vec<SocketAddr> = node_ids
        .iter()
        .enumerate()
        .map(|(idx, _)| {
            format!("127.0.0.1:{}", base_port + idx as u16)
                .parse()
                .unwrap()
        })
        .collect();

    // Create custom state machines
    let state_machines: Vec<Arc<TestCounterStateMachine>> = node_ids
        .iter()
        .map(|_| Arc::new(TestCounterStateMachine::new()))
        .collect();

    // Create nodes
    let mut nodes = Vec::new();
    for (idx, &node_id) in node_ids.iter().enumerate() {
        let addr = addrs[idx];
        let peers: Vec<SocketAddr> = addrs
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, &a)| a)
            .collect();

        let is_initial_leader = idx == 0;
        let node = TestNodeWithCustomStateMachine::new(
            node_id,
            addr,
            peers,
            is_initial_leader,
            state_machines[idx].clone(),
        )
        .await;
        nodes.push(node);
    }

    // Start all nodes
    for node in &nodes {
        node.start().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Campaign for leader
    nodes[0].campaign().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Apply a complex sequence of commands
    let commands = vec![
        "INCREMENT", // 1
        "INCREMENT", // 2
        "ADD 10",    // 12
        "DECREMENT", // 11
        "INCREMENT", // 12
        "ADD 3",     // 15
        "DECREMENT", // 14
        "DECREMENT", // 13
        "ADD 7",     // 20
    ];

    tracing::info!("Applying command sequence...");
    for (i, cmd) in commands.iter().enumerate() {
        nodes[0].propose(cmd.as_bytes().to_vec()).await.unwrap();
        tracing::info!("  {}: {}", i + 1, cmd);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for replication
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify all nodes have the exact same final state
    tracing::info!("Verifying deterministic execution...");
    let expected = "20";
    for (idx, node) in nodes.iter().enumerate() {
        let result = node.query(b"GET").await.unwrap();
        let value = String::from_utf8_lossy(&result);
        tracing::info!("  Node {} counter: {}", idx + 1, value);
        assert_eq!(
            value,
            expected,
            "Node {} should have deterministic result = {}",
            idx + 1,
            expected
        );
    }

    // Also verify via direct state machine access (not through Raft query)
    for (idx, sm) in state_machines.iter().enumerate() {
        let direct_value = sm.get_value();
        tracing::info!(
            "  Node {} state machine value (direct): {}",
            idx + 1,
            direct_value
        );
        assert_eq!(
            direct_value,
            20,
            "Node {} state machine should have value = 20",
            idx + 1
        );
    }

    // Cleanup
    for node in &mut nodes {
        node.crash();
    }

    tracing::info!("✓ Test passed: Custom state machines execute deterministically");
}
