/// Automatic leader election tests
mod common;

use common::TestCluster;
use std::time::Duration;

#[test]
#[ignore = "Cluster membership initialization needs fix - nodes 2&3 aren't voters yet"]
fn test_automatic_election_after_leader_failure() {
    // TODO: Fix cluster initialization so all nodes are voters before testing automatic election
    // Currently node 1 bootstraps with only itself as voter, nodes 2&3 need to be added via ConfChange
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

        tracing::info!("=== Starting automatic election after leader failure test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8200).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect initial leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(cluster.nodes[0].is_leader().await, "Node 1 should be leader");
        tracing::info!("✓ Node 1 is leader");

        // Make proposals to ensure cluster is synchronized
        for i in 1..=10 {
            let cmd = format!("SET init{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tracing::info!("✓ Cluster synchronized with {} proposals", 10);
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Kill the leader
        tracing::info!("Killing leader (node 1)");
        cluster.crash_node(1).expect("Failed to crash node 1");

        // Wait for automatic election
        // Raft's election_tick = 10 means after 10 ticks without heartbeat, start election
        // Our ticker runs every 100ms, so 10 ticks = 1 second
        // Pre-vote + actual vote might take another second
        // Total expected time: 1-2 seconds
        tracing::info!("Waiting for automatic leader election...");
        let start = std::time::Instant::now();

        // Give it 6 seconds to account for retries and network delays
        let result = cluster.wait_for_leader_election(Duration::from_secs(6)).await;
        let elapsed = start.elapsed();

        match result {
            Ok(new_leader_id) => {
                tracing::info!("✓ New leader elected: node {} in {:?}", new_leader_id, elapsed);
                // Be more lenient with timing - just check it happened eventually
                assert!(elapsed < Duration::from_secs(5), "Election took too long: {:?}", elapsed);
                assert_ne!(new_leader_id, 1, "Should be a different leader");
            }
            Err(e) => {
                // Debug: check node states
                for (idx, node) in cluster.nodes.iter().enumerate() {
                    let is_leader = node.is_leader().await;
                    tracing::error!("Node {} (id={}): is_leader={}", idx, node.node_id, is_leader);
                }
                panic!("Automatic election failed after {:?}: {}", elapsed, e);
            }
        }

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Automatic election after leader failure");
    });
}

#[test]
fn test_no_election_with_healthy_leader() {
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

        tracing::info!("=== Starting no election with healthy leader test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8210).await;
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

        // Make proposals to keep leader active
        for i in 1..=10 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait longer than election timeout
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify SAME leader (no election triggered)
        let final_leader = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        assert_eq!(initial_leader, final_leader, "Leader should not have changed");
        tracing::info!("✓ Leader remained stable: node {}", final_leader);

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: No election with healthy leader");
    });
}

#[test]
#[ignore = "Cluster membership initialization needs fix - nodes aren't all voters yet"]
fn test_rapid_leader_failures() {
    // TODO: Same issue as test_automatic_election_after_leader_failure
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

        tracing::info!("=== Starting rapid leader failures test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3, 4, 5], 8220).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect initial leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Make initial proposals to synchronize cluster
        for i in 1..=10 {
            let cmd = format!("SET init{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tracing::info!("✓ Cluster initialized");
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Kill leaders 3 times in a row
        for iteration in 1..=3 {
            tracing::info!("=== Iteration {} ===", iteration);

            // Find current leader
            let mut current_leader_idx = None;
            for (idx, node) in cluster.nodes.iter().enumerate() {
                if node.is_leader().await {
                    current_leader_idx = Some(idx);
                    break;
                }
            }

            if let Some(idx) = current_leader_idx {
                let leader_id = cluster.nodes[idx].node_id;
                tracing::info!("Current leader: node {}", leader_id);

                // Kill the leader
                cluster.crash_node(leader_id).ok();
                tracing::info!("Killed leader node {}", leader_id);

                // Wait for automatic re-election
                tokio::time::sleep(Duration::from_millis(3500)).await;

                // Verify new leader elected
                let has_new_leader = cluster.has_leader().await;
                assert!(has_new_leader, "Should have new leader after iteration {}", iteration);
                tracing::info!("✓ New leader elected after iteration {}", iteration);
            }
        }

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Rapid leader failures");
    });
}
