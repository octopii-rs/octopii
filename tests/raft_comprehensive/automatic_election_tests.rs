/// Automatic leader election tests
mod common;

use common::TestCluster;
use std::time::Duration;

#[test]
fn test_automatic_election_after_leader_failure() {
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

        // Create cluster with only leader initially (following TiKV pattern)
        // Followers will be added as learners dynamically
        let mut cluster = TestCluster::new(vec![1], 8200).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Leader campaigns (node 1 is configured as initial leader)
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(cluster.nodes[0].is_leader().await, "Node 1 should be leader");
        tracing::info!("✓ Node 1 is leader");

        // CRITICAL: Make proposals FIRST to advance the leader's log
        // This ensures followers will receive a snapshot when added
        tracing::info!("Making initial proposals to advance log...");
        for i in 1..=20 {
            let cmd = format!("SET bootstrap{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await
                .expect("Failed to propose");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        tracing::info!("✓ Leader log advanced");

        // Now add peers as learners first (they can have empty logs)
        // Then promote to voters once caught up
        tracing::info!("Adding node 2 as learner");
        cluster.add_learner(2).await.expect("Failed to add learner 2");
        cluster.nodes[1].start().await.expect("Failed to start learner 2");
        tokio::time::sleep(Duration::from_secs(3)).await; // Give more time for network setup

        tracing::info!("Adding node 3 as learner");
        cluster.add_learner(3).await.expect("Failed to add learner 3");
        cluster.nodes[2].start().await.expect("Failed to start learner 3");
        tokio::time::sleep(Duration::from_secs(3)).await; // Give more time for network setup

        // Make more proposals for learners to catch up on
        tracing::info!("Making proposals for learners to replicate...");
        for i in 1..=10 {
            let cmd = format!("SET learner_catchup{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await
                .expect("Failed to propose");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Wait for learners to catch up
        for node_id in 2..=3 {
            let start = std::time::Instant::now();
            loop {
                if start.elapsed() > Duration::from_secs(15) {
                    panic!("Learner {} did not catch up after 15s", node_id);
                }
                if cluster.is_learner_caught_up(node_id).await.unwrap_or(false) {
                    tracing::info!("✓ Learner {} caught up", node_id);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }

        // Extra stabilization time
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Promote learners to voters (one at a time with enough time between)
        tracing::info!("Promoting learner 2 to voter");
        cluster.promote_learner(2).await.expect("Failed to promote learner 2");
        tokio::time::sleep(Duration::from_secs(3)).await; // Give cluster time to stabilize

        tracing::info!("Promoting learner 3 to voter");
        cluster.promote_learner(3).await.expect("Failed to promote learner 3");
        tokio::time::sleep(Duration::from_secs(3)).await; // Give cluster time to stabilize

        tracing::info!("✓ All nodes added as voters: [1, 2, 3]");

        // Make some more proposals to ensure cluster is working
        for i in 1..=10 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        tracing::info!("✓ Made additional proposals");

        // Kill the leader (node 1)
        tracing::info!("Killing leader (node 1)");
        cluster.crash_node(1).expect("Failed to crash leader");

        // Wait for automatic election
        tracing::info!("Waiting for automatic leader election...");
        let start = std::time::Instant::now();

        let result = cluster.wait_for_leader_election(Duration::from_secs(6)).await;
        let elapsed = start.elapsed();

        match result {
            Ok(new_leader_id) => {
                tracing::info!("✓ New leader elected: node {} in {:?}", new_leader_id, elapsed);
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
fn test_rapid_leader_failures() {
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

        // Create 5-node cluster (all nodes start together)
        let mut cluster = TestCluster::new(vec![1, 2, 3, 4, 5], 8220).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect initial leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add peers via ConfChange (following TiKV pattern)
        for peer_id in 2..=5 {
            tracing::info!("Adding peer {} via ConfChange", peer_id);
            let addr = format!("127.0.0.1:{}", 8220 + peer_id - 1).parse().unwrap();
            cluster.nodes[0].get_node().unwrap()
                .add_peer(peer_id, addr).await
                .expect(&format!("Failed to add peer {}", peer_id));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        tracing::info!("✓ All nodes added as voters: [1, 2, 3, 4, 5]");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some initial proposals
        for i in 1..=10 {
            let cmd = format!("SET init{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tracing::info!("✓ All nodes ready");
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
