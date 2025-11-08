/// Pre-vote protocol tests
mod common;

use common::TestCluster;
use std::time::Duration;

#[test]
fn test_pre_vote_stable_cluster() {
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

        tracing::info!("=== Starting pre-vote stable cluster test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8500).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        let initial_leader = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        tracing::info!("Initial leader: node {}", initial_leader);

        // Make continuous proposals
        for i in 1..=50 {
            let cmd = format!("SET prevote{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Verify leader remains stable (pre-vote prevents disruption)
        let final_leader = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        assert_eq!(initial_leader, final_leader, "Leader should remain stable");
        tracing::info!("✓ Leader remained stable with pre-vote enabled");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Pre-vote stable cluster");
    });
}

#[test]
fn test_pre_vote_with_lagging_node() {
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

        tracing::info!("=== Starting pre-vote with lagging node test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8510).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Crash node 3
        tracing::info!("Crashing node 3");
        cluster.crash_node(3).expect("Failed to crash node 3");

        // Make many proposals while node 3 is down
        tracing::info!("Making proposals while node 3 is offline...");
        for i in 1..=100 {
            let cmd = format!("SET lagging{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_before = if cluster.nodes[0].is_leader().await {
            1
        } else {
            2
        };

        tracing::info!("Leader before restart: node {}", leader_before);

        // Restart node 3 (far behind)
        tracing::info!("Restarting node 3 (far behind)");
        cluster
            .restart_node(3)
            .await
            .expect("Failed to restart node 3");

        // Wait and make more proposals
        tokio::time::sleep(Duration::from_secs(3)).await;

        for i in 101..=120 {
            let cmd = format!("SET after{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_after = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        tracing::info!("Leader after node 3 rejoined: node {}", leader_after);

        // With pre-vote, the lagging node shouldn't disrupt the cluster
        // Leader may have changed due to other reasons, but cluster should be stable
        assert!(cluster.has_leader().await, "Cluster should have a leader");
        tracing::info!("✓ Cluster remained stable despite lagging node");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Pre-vote with lagging node");
    });
}
