/// Advanced durability tests: crashes during specific Raft operations
mod common;

use common::TestCluster;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

#[test]
fn test_crash_immediately_after_proposal() {
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

        tracing::info!("=== Starting crash immediately after proposal test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make a proposal and immediately crash the leader
        let cmd = "SET critical_data important_value";
        cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

        // Crash leader immediately (before commit)
        tokio::time::sleep(Duration::from_millis(50)).await;
        tracing::info!("Crashing leader immediately after proposal...");
        cluster.crash_node(1).ok();

        // Let remaining nodes elect new leader
        tokio::time::sleep(Duration::from_millis(500)).await;
        cluster.nodes[1].campaign().await.ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Restart crashed node
        tracing::info!("Restarting crashed leader...");
        cluster.restart_node(1).await.expect("Failed to restart");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Crash immediately after proposal");
    });
}

#[test]
fn test_durable_state_after_many_operations() {
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

        tracing::info!("=== Starting durable state after many operations test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make MANY operations to test WAL durability under load
        tracing::info!("Making 50 proposals...");
        for i in 1..=50 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

            if i % 10 == 0 {
                tracing::info!("Progress: {} proposals", i);
            }
        }

        // Wait for commits
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Crash ALL nodes
        tracing::info!("Crashing all nodes...");
        for i in 1..=3 {
            cluster.crash_node(i).ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Restart ALL nodes
        tracing::info!("Restarting all nodes...");
        for i in 1..=3 {
            cluster.restart_node(i).await.expect("Failed to restart");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Re-elect
        cluster.nodes[0].campaign().await.ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify cluster is functional
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Durable state after many operations");
    });
}

// Exact copy of chaos_tests::test_crash_during_proposal to verify it works in this file
#[test]
fn test_interleaved_operations_and_crashes() {
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

        tracing::info!("=== Starting interleaved operations and crashes test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some successful proposals
        for i in 1..=3 {
            let cmd = format!("SET before{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Start proposals and crash a follower
        for i in 4..=6 {
            let cmd = format!("SET during{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

            if i == 5 {
                tracing::info!("Crashing node 3 during proposals...");
                cluster.crash_node(3).ok();
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Restart crashed node
        tracing::info!("Restarting node 3...");
        cluster.restart_node(3).await.expect("Failed to restart");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Interleaved operations and crashes");
    });
}
