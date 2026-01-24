//! Example demonstrating custom state machine integration with OpenRaft node.
//!
//! This example verifies that custom state machines passed to `new_with_state_machine()`
//! are correctly used for BOTH reads (query) AND writes (propose through Raft consensus).

use bytes::Bytes;
use octopii::{Config, OctopiiNode, OctopiiRuntime, StateMachine, StateMachineTrait};
use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(async {
            let result = run_custom_state_machine_example().await?;
            println!("Custom state machine example completed successfully!");
            println!("  Propose apply count: {}", result.propose_apply_count);
            println!("  Final counter value: {}", result.final_value);
            Ok(())
        })
}

pub struct ExampleResult {
    pub propose_apply_count: usize,
    pub final_value: i64,
}

/// A counter state machine that tracks how many times apply() is called.
/// This lets us verify that propose() actually goes through our custom state machine.
pub struct TrackedCounterStateMachine {
    counter: Mutex<i64>,
    /// Tracks the number of times apply() is called - proves writes go through this SM
    apply_call_count: AtomicUsize,
}

impl TrackedCounterStateMachine {
    pub fn new() -> Self {
        Self {
            counter: Mutex::new(0),
            apply_call_count: AtomicUsize::new(0),
        }
    }

    pub fn apply_count(&self) -> usize {
        self.apply_call_count.load(Ordering::SeqCst)
    }

    pub fn current_value(&self) -> i64 {
        *self.counter.lock().unwrap()
    }
}

impl Default for TrackedCounterStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMachineTrait for TrackedCounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Increment the call counter to prove this method was called
        self.apply_call_count.fetch_add(1, Ordering::SeqCst);

        let cmd = std::str::from_utf8(command).map_err(|e| e.to_string())?;
        let mut tokens = cmd.split_whitespace();
        let op = tokens.next().unwrap_or("");

        match op {
            "INCREMENT" => {
                let mut guard = self.counter.lock().unwrap();
                *guard += 1;
                Ok(Bytes::from(guard.to_string()))
            }
            "DECREMENT" => {
                let mut guard = self.counter.lock().unwrap();
                *guard -= 1;
                Ok(Bytes::from(guard.to_string()))
            }
            "ADD" => {
                let amount: i64 = tokens
                    .next()
                    .ok_or("ADD missing amount")?
                    .parse()
                    .map_err(|e: std::num::ParseIntError| e.to_string())?;
                let mut guard = self.counter.lock().unwrap();
                *guard += amount;
                Ok(Bytes::from(guard.to_string()))
            }
            "GET" => {
                let guard = self.counter.lock().unwrap();
                Ok(Bytes::from(guard.to_string()))
            }
            "RESET" => {
                let mut guard = self.counter.lock().unwrap();
                *guard = 0;
                Ok(Bytes::from("0"))
            }
            _ => Err(format!("Unknown command: {}", op)),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        self.counter.lock().unwrap().to_le_bytes().to_vec()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        if data.is_empty() {
            return Ok(());
        }
        if data.len() != 8 {
            return Err("snapshot should be 8 bytes".into());
        }
        let value = i64::from_le_bytes(data.try_into().unwrap());
        *self.counter.lock().unwrap() = value;
        Ok(())
    }
}

pub async fn run_custom_state_machine_example() -> Result<ExampleResult, Box<dyn Error>> {
    let data_dir = tempfile::tempdir()?;
    let wal_dir = data_dir.path().join("custom_sm_node");
    std::fs::create_dir_all(&wal_dir)?;

    let config = Config {
        bind_addr: "127.0.0.1:0".parse()?,
        peers: Vec::new(),
        wal_dir,
        is_initial_leader: true,
        worker_threads: 2,
        ..Default::default()
    };

    // Create our custom state machine
    let custom_sm = Arc::new(TrackedCounterStateMachine::new());
    let sm_for_node: StateMachine = custom_sm.clone();

    let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

    // Use new_with_state_machine to create node with our custom SM
    let node = OctopiiNode::new_with_state_machine(config, runtime, sm_for_node).await?;

    node.start().await?;
    node.campaign().await?;

    // Wait for leader election
    for _ in 0..20 {
        if node.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if !node.is_leader().await {
        return Err("Node failed to become leader".into());
    }

    // Record apply count before propose
    let apply_count_before = custom_sm.apply_count();

    // Propose commands through Raft consensus
    // If the bug exists (custom SM not wired correctly), these would go to the default KvStateMachine
    node.propose(b"INCREMENT".to_vec()).await?;
    node.propose(b"INCREMENT".to_vec()).await?;
    node.propose(b"ADD 10".to_vec()).await?;

    // Small delay to ensure commands are applied
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check that our custom state machine's apply() was actually called
    let apply_count_after = custom_sm.apply_count();
    let propose_apply_count = apply_count_after - apply_count_before;

    // Get the final value directly from our custom state machine
    let final_value = custom_sm.current_value();

    node.shutdown().await;

    Ok(ExampleResult {
        propose_apply_count,
        final_value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a node with custom state machine
    async fn setup_node_with_custom_sm(
        test_name: &str,
    ) -> (
        OctopiiNode,
        Arc<TrackedCounterStateMachine>,
        tempfile::TempDir,
    ) {
        let data_dir = tempfile::tempdir().unwrap();
        let wal_dir = data_dir.path().join(test_name);
        std::fs::create_dir_all(&wal_dir).unwrap();

        let config = Config {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            peers: Vec::new(),
            wal_dir,
            is_initial_leader: true,
            worker_threads: 2,
            ..Default::default()
        };

        let custom_sm = Arc::new(TrackedCounterStateMachine::new());
        let sm_for_node: StateMachine = custom_sm.clone();

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let node = OctopiiNode::new_with_state_machine(config, runtime, sm_for_node)
            .await
            .unwrap();

        node.start().await.unwrap();
        node.campaign().await.unwrap();

        // Wait for leader
        for _ in 0..20 {
            if node.is_leader().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(node.is_leader().await, "Node should become leader");

        (node, custom_sm, data_dir)
    }

    /// INVARIANT 1: propose() MUST route writes through the custom state machine.
    /// This was the core bug - propose() went to default KvStateMachine instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_propose_uses_custom_state_machine() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("propose_test").await;

        let apply_count_before = custom_sm.apply_count();

        // Each propose MUST trigger exactly one apply() on our custom SM
        node.propose(b"INCREMENT".to_vec()).await.unwrap();
        node.propose(b"INCREMENT".to_vec()).await.unwrap();
        node.propose(b"INCREMENT".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let apply_count_after = custom_sm.apply_count();
        let applies_from_propose = apply_count_after - apply_count_before;

        node.shutdown().await;

        // EXACT count - not "at least", because we need to ensure no double-applies
        // and no missed applies
        assert_eq!(
            applies_from_propose, 3,
            "propose() must call custom SM apply() exactly once per proposal. \
             Expected 3, got {}. If < 3: writes going to wrong SM (the bug). \
             If > 3: duplicate applies (different bug).",
            applies_from_propose
        );
    }

    /// INVARIANT 2: query() MUST read from the custom state machine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_query_uses_custom_state_machine() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("query_test").await;

        let apply_count_before = custom_sm.apply_count();

        // Query should invoke apply() on our custom SM
        let result = node.query(b"GET").await.unwrap();

        let apply_count_after = custom_sm.apply_count();

        node.shutdown().await;

        // query() should have called apply() on our SM
        assert_eq!(
            apply_count_after - apply_count_before,
            1,
            "query() must call custom SM apply()"
        );

        // Initial value should be 0
        assert_eq!(
            String::from_utf8(result.to_vec()).unwrap(),
            "0",
            "Initial counter value should be 0"
        );
    }

    /// INVARIANT 3: propose() and query() MUST use the SAME state machine instance.
    /// The bug caused them to use different instances (propose->default, query->custom).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_propose_and_query_share_same_instance() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("same_instance_test").await;

        // Write via propose
        node.propose(b"ADD 42".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Read via query
        let query_result = node.query(b"GET").await.unwrap();
        let queried_value: i64 = String::from_utf8(query_result.to_vec())
            .unwrap()
            .parse()
            .unwrap();

        // Read directly from our SM instance
        let direct_value = custom_sm.current_value();

        node.shutdown().await;

        // All three views MUST be consistent
        assert_eq!(queried_value, 42, "query() should see proposed value");
        assert_eq!(
            direct_value, 42,
            "Direct SM access should see proposed value"
        );
        assert_eq!(
            queried_value, direct_value,
            "query() and direct access must return same value - proves same instance"
        );
    }

    /// INVARIANT 4: State must be consistent after EVERY operation, not just at the end.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_state_consistent_after_each_operation() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("consistency_test").await;

        let ops = [
            ("INCREMENT", 1i64),
            ("INCREMENT", 2),
            ("ADD 10", 12),
            ("DECREMENT", 11),
            ("ADD -5", 6),
        ];

        for (cmd, expected_value) in ops {
            node.propose(cmd.as_bytes().to_vec()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Check consistency after EACH operation
            let query_result = node.query(b"GET").await.unwrap();
            let queried: i64 = String::from_utf8(query_result.to_vec())
                .unwrap()
                .parse()
                .unwrap();
            let direct = custom_sm.current_value();

            assert_eq!(
                queried, expected_value,
                "After '{}': query() returned {}, expected {}",
                cmd, queried, expected_value
            );
            assert_eq!(
                direct, expected_value,
                "After '{}': direct SM returned {}, expected {}",
                cmd, direct, expected_value
            );
            assert_eq!(
                queried, direct,
                "After '{}': query() and direct access diverged ({} vs {})",
                cmd, queried, direct
            );
        }

        node.shutdown().await;
    }

    /// INVARIANT 5: Snapshot must capture state from custom state machine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_snapshot_uses_custom_state_machine() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("snapshot_test").await;

        // Set up some state
        node.propose(b"ADD 100".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Take snapshot directly from our SM
        let snapshot = custom_sm.snapshot();

        // Snapshot should contain our value (100 as i64 little-endian)
        assert_eq!(snapshot.len(), 8, "Snapshot should be 8 bytes (i64)");
        let snapshot_value = i64::from_le_bytes(snapshot.try_into().unwrap());
        assert_eq!(snapshot_value, 100, "Snapshot must capture custom SM state");

        node.shutdown().await;
    }

    /// INVARIANT 6: Restore must restore state to custom state machine.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_restore_uses_custom_state_machine() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("restore_test").await;

        // Set initial state
        node.propose(b"ADD 50".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Take snapshot
        let snapshot = custom_sm.snapshot();
        assert_eq!(custom_sm.current_value(), 50);

        // Modify state
        node.propose(b"ADD 25".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(custom_sm.current_value(), 75);

        // Restore from snapshot
        custom_sm.restore(&snapshot).unwrap();

        // Verify restore affected our custom SM
        assert_eq!(
            custom_sm.current_value(),
            50,
            "Restore must affect custom SM state"
        );

        // Verify query sees restored state
        let query_result = node.query(b"GET").await.unwrap();
        let queried: i64 = String::from_utf8(query_result.to_vec())
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            queried, 50,
            "query() must see restored state from custom SM"
        );

        node.shutdown().await;
    }

    /// INVARIANT 7: Apply count must match total operations (propose + query).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invariant_apply_count_matches_operations() {
        let (node, custom_sm, _dir) = setup_node_with_custom_sm("apply_count_test").await;

        let initial_count = custom_sm.apply_count();

        // 3 proposes + 2 queries = 5 apply() calls
        node.propose(b"INCREMENT".to_vec()).await.unwrap();
        node.propose(b"INCREMENT".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        node.query(b"GET").await.unwrap();

        node.propose(b"INCREMENT".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        node.query(b"GET").await.unwrap();

        let final_count = custom_sm.apply_count();
        let total_applies = final_count - initial_count;

        node.shutdown().await;

        assert_eq!(
            total_applies, 5,
            "Total apply() calls should equal propose count + query count. \
             Expected 5 (3 proposes + 2 queries), got {}",
            total_applies
        );
    }
}
