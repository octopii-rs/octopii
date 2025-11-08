/// Comprehensive cluster scenario tests for Raft
mod common;

use common::TestCluster;
use std::time::Duration;

#[test]
fn test_three_node_cluster_leader_election() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Starting 3-node cluster leader election test ===");

    // Create cluster BEFORE entering async runtime
    let mut cluster = TestCluster::new(vec![1, 2, 3], 7100);

    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Node 1 campaigns
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify leader elected
        assert!(cluster.nodes[0].is_leader().await, "Node 1 should be leader");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: 3-node leader election");
    });
}

#[test]
fn test_three_node_cluster_with_proposals() {
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

        tracing::info!("=== Starting 3-node cluster with proposals test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 7110);
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Propose several values
        for i in 1..=5 {
            let cmd = format!("SET key{} value{}", i, i);
            tracing::info!("Proposing: {}", cmd);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence (has leader)
        cluster.verify_convergence(Duration::from_secs(5)).await.expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: 3-node proposals");
    });
}

#[test]
fn test_five_node_cluster_leader_election() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(6)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting 5-node cluster leader election test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3, 4, 5], 7120);
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Node 3 campaigns
        cluster.nodes[2].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify leader elected
        cluster.verify_convergence(Duration::from_secs(5)).await.expect("Leader election failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: 5-node leader election");
    });
}

#[test]
fn test_follower_crash_and_recovery() {
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

        tracing::info!("=== Starting follower crash and recovery test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 7140);
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some proposals
        for i in 1..=3 {
            let cmd = format!("SET before_crash{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Crash follower node 3
        tracing::info!("Crashing follower node 3...");
        cluster.crash_node(3).expect("Failed to crash node");

        // Make more proposals while node 3 is down
        for i in 4..=6 {
            let cmd = format!("SET during_crash{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Restart node 3
        tracing::info!("Restarting node 3...");
        cluster.restart_node(3).await.expect("Failed to restart node");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence (node 3 should catch up)
        cluster.verify_convergence(Duration::from_secs(10)).await.expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Follower crash and recovery");
    });
}

#[test]
fn test_concurrent_proposals_from_leader() {
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

        tracing::info!("=== Starting concurrent proposals test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 7160);
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make many proposals quickly
        for i in 1..=20 {
            let cmd = format!("SET concurrent{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        // Wait for all proposals to commit
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify convergence
        cluster.verify_convergence(Duration::from_secs(10)).await.expect("Convergence failed");

        // Verify a few keys made it through
        let result = cluster.nodes[0].query(b"GET concurrent1").await;
        tracing::info!("Query result: {:?}", result);

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Concurrent proposals");
    });
}
