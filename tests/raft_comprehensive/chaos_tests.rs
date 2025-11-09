/// Chaos tests: random crashes, failures, and edge cases
mod common;

use common::TestCluster;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

#[test]
fn test_crash_during_proposal() {
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

        tracing::info!("=== Starting crash during proposal test ===");

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
        tracing::info!("✓ Test passed: Crash during proposal");
    });
}

#[test]
fn test_all_nodes_crash_and_recover() {
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

        tracing::info!("=== Starting all nodes crash and recover test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader and make proposals
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        for i in 1..=5 {
            let cmd = format!("SET persistent{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Crash ALL nodes
        tracing::info!("Crashing all nodes...");
        for i in 1..=3 {
            cluster.crash_node(i).ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Restart ALL nodes
        tracing::info!("Restarting all nodes...");
        for i in 1..=3 {
            cluster
                .restart_node(i)
                .await
                .expect("Failed to restart node");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Re-elect leader
        cluster.nodes[0].campaign().await.ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make new proposals to verify cluster is functional
        for i in 6..=8 {
            let cmd = format!("SET after_crash{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: All nodes crash and recover");
    });
}

#[test]
fn test_rolling_restarts() {
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

        tracing::info!("=== Starting rolling restarts test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make initial proposals
        for i in 1..=3 {
            let cmd = format!("SET initial{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Rolling restart: crash and restart each node one at a time
        for node_id in 1..=3 {
            tracing::info!("Rolling restart: node {}...", node_id);

            cluster.crash_node(node_id).ok();
            tokio::time::sleep(Duration::from_millis(500)).await;

            cluster
                .restart_node(node_id)
                .await
                .expect("Failed to restart");
            tokio::time::sleep(Duration::from_secs(2)).await;

            // Make a proposal after each restart
            let cmd = format!("SET after_restart{} node{}", node_id, node_id);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Final convergence check
        tokio::time::sleep(Duration::from_secs(3)).await;

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Rolling restarts");
    });
}

#[test]
fn test_rapid_crash_recovery_cycles() {
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

        tracing::info!("=== Starting rapid crash/recovery cycles test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Rapid crash/recovery cycles on node 3
        for cycle in 0..3 {
            tracing::info!("Crash/recovery cycle {} on node 3", cycle);

            // Make a proposal
            let cmd = format!("SET cycle{} value{}", cycle, cycle);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

            // Crash node 3
            cluster.crash_node(3).ok();
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Restart node 3
            cluster.restart_node(3).await.expect("Failed to restart");
            tokio::time::sleep(Duration::from_millis(800)).await;
        }

        // Final proposals
        for i in 10..=12 {
            let cmd = format!("SET final{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Rapid crash/recovery cycles");
    });
}
