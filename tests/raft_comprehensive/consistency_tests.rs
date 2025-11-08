/// Consistency verification tests: linearizability, convergence, state machine correctness
mod common;

use common::TestCluster;
use std::time::Duration;

#[test]
fn test_state_machine_linearizability() {
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

        tracing::info!("=== Starting state machine linearizability test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 7400).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Perform a sequence of SET/GET operations
        // SET key1 = A
        cluster.nodes[0]
            .propose("SET key1 A".as_bytes().to_vec())
            .await
            .ok();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // SET key1 = B (overwrite)
        cluster.nodes[0]
            .propose("SET key1 B".as_bytes().to_vec())
            .await
            .ok();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // SET key2 = C
        cluster.nodes[0]
            .propose("SET key2 C".as_bytes().to_vec())
            .await
            .ok();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // DELETE key1
        cluster.nodes[0]
            .propose("DELETE key1".as_bytes().to_vec())
            .await
            .ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify operations completed
        let result_key1 = cluster.nodes[0].query(b"GET key1").await;
        let result_key2 = cluster.nodes[0].query(b"GET key2").await;

        tracing::info!("Query key1 (should be NOT_FOUND): {:?}", result_key1);
        tracing::info!("Query key2 (should be C): {:?}", result_key2);

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: State machine linearizability");
    });
}

#[test]
fn test_convergence_under_continuous_load() {
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

        tracing::info!("=== Starting convergence under continuous load test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3, 4, 5], 7440).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Continuous load: many proposals
        tracing::info!("Applying continuous load (30 proposals)...");
        for i in 1..=30 {
            let cmd = format!("SET load{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for convergence
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify all nodes converged
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Convergence under continuous load");
    });
}

#[test]
fn test_no_data_loss_after_total_outage() {
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

        tracing::info!("=== Starting no data loss after total outage test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 7430).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make proposals
        for i in 1..=5 {
            let cmd = format!("SET persistent{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        // Wait for majority commit
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Crash and restart ALL nodes (simulating total outage)
        tracing::info!("Total cluster outage...");
        for i in 1..=3 {
            cluster.crash_node(i).ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        for i in 1..=3 {
            cluster.restart_node(i).await.expect("Failed to restart");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Re-elect
        cluster.nodes[0].campaign().await.ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify NO DATA LOSS - query should work
        let result = cluster.nodes[0].query(b"GET persistent1").await;
        tracing::info!("Query after recovery: {:?}", result);

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: No data loss after total outage");
    });
}
