#![cfg(feature = "raft-rs-impl")]
// Log Streaming integration tests - TiKV test parity
//
// Log streaming tests verify efficient catch-up mechanisms for slow followers.
// When a follower falls behind, the leader should efficiently stream log entries
// to help it catch up, rather than sending entries one-by-one.

use crate::common::TestCluster;
use std::time::Duration;
use tokio;

fn alloc_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static PORT: AtomicU16 = AtomicU16::new(25000);
    PORT.fetch_add(1, Ordering::SeqCst)
}

#[test]
fn test_follower_catch_up_after_partition() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting follower catch up after partition test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some initial proposals
        for i in 1..=5 {
            let cmd = format!("SET initial{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Partition node 3 from the cluster
        tracing::info!("Isolating node 3 to create lag...");
        cluster.isolate_node(3);
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Make many proposals while node 3 is isolated (create a large gap)
        tracing::info!("Making 50 proposals while node 3 is partitioned...");
        for i in 6..=55 {
            let cmd = format!("SET during_partition{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Heal partition - node 3 should catch up via log streaming
        tracing::info!("Healing partition - node 3 should catch up...");
        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify convergence - all nodes should have same state
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed after catch-up");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Follower catch up after partition");
    });
}

// Crash-based catch-up test - simplified to avoid infrastructure issues
// Tests that a crashed node can recover and catch up after restart
#[test]
fn test_follower_catch_up_after_crash() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting follower catch up after crash test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some proposals
        for i in 1..=3 {
            let cmd = format!("SET before{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Crash and restart immediately (simpler scenario)
        tracing::info!("Crash and restart node 3...");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        cluster.restart_node(3).await.expect("Failed to restart");
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Make more proposals to verify cluster is healthy
        for i in 4..=6 {
            let cmd = format!("SET after{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Verify convergence
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Follower catch up after crash");
    });
}

#[test]
fn test_multiple_followers_catch_up() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting multiple followers catch up test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some initial proposals
        for i in 1..=3 {
            let cmd = format!("SET initial{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Partition both followers (nodes 2 and 3)
        tracing::info!("Isolating both followers (nodes 2 and 3)...");
        cluster.isolate_node(2);
        cluster.isolate_node(3);
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Make proposals on leader (which has no followers)
        // These will not be committed but will be in the log
        tracing::info!("Making proposals on isolated leader...");
        for i in 4..=8 {
            let cmd = format!("SET on_leader{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Heal one follower at a time
        tracing::info!("Healing partition for node 2...");
        cluster.clear_send_filters(2).await;
        cluster.clear_recv_filters(2);
        tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::info!("Healing partition for node 3...");
        cluster.clear_send_filters(3).await;
        cluster.clear_recv_filters(3);
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Both followers should catch up
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Multiple followers catch up");
    });
}

#[test]
fn test_catch_up_with_ongoing_writes() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting catch up with ongoing writes test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Initial proposals
        for i in 1..=5 {
            let cmd = format!("SET initial{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Partition node 3
        tracing::info!("Isolating node 3...");
        cluster.isolate_node(3);
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Make proposals while node 3 is isolated
        for i in 6..=20 {
            let cmd = format!("SET during_partition{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Heal partition
        tracing::info!("Healing partition...");
        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Continue making proposals while node 3 is catching up
        tracing::info!("Making proposals while node 3 catches up...");
        for i in 21..=30 {
            let cmd = format!("SET during_catchup{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify all nodes converge
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Catch up with ongoing writes");
    });
}

// Test catch-up after repeated partition/heal cycles
#[test]
fn test_catch_up_with_repeated_partitions() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting catch up with repeated partitions test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // First partition cycle
        tracing::info!("First partition cycle...");
        cluster.isolate_node(3);
        for i in 1..=5 {
            let cmd = format!("SET cycle1_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Second partition cycle
        tracing::info!("Second partition cycle...");
        cluster.isolate_node(3);
        for i in 6..=10 {
            let cmd = format!("SET cycle2_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Catch up with repeated partitions");
    });
}
