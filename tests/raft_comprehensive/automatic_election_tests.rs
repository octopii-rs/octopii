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

        let mut cluster = TestCluster::new(vec![1, 2, 3], 8200).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect initial leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(cluster.nodes[0].is_leader().await, "Node 1 should be leader");
        tracing::info!("✓ Node 1 is leader");

        // Add nodes 2 and 3 as voters (now properly waits for completion!)
        tracing::info!("Adding node 2 as voter");
        let conf_state_2 = cluster.nodes[0]
            .get_node()
            .unwrap()
            .add_peer(2, cluster.nodes[1].addr)
            .await
            .expect("Failed to add node 2");
        tracing::info!("Node 2 added, voters: {:?}", conf_state_2.voters);

        tracing::info!("Adding node 3 as voter");
        let conf_state_3 = cluster.nodes[0]
            .get_node()
            .unwrap()
            .add_peer(3, cluster.nodes[2].addr)
            .await
            .expect("Failed to add node 3");
        tracing::info!("Node 3 added, voters: {:?}", conf_state_3.voters);

        tracing::info!("✓ All nodes added as voters");

        // Make proposals to ensure cluster is fully synchronized
        for i in 1..=20 {
            let cmd = format!("SET init{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tracing::info!("✓ Cluster synchronized with 20 proposals");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Kill the leader
        tracing::info!("Killing leader (node 1)");
        cluster.crash_node(1).expect("Failed to crash node 1");

        // Wait for automatic election
        // Raft's election_tick = 10 ticks * 100ms = 1 second
        // MAX_TICKS_WITHOUT_LEADER = 20 ticks * 100ms = 2 seconds for our automatic campaign
        // Pre-vote + actual vote might take another 1-2 seconds
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

        let mut cluster = TestCluster::new(vec![1, 2, 3, 4, 5], 8220).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect initial leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add all other nodes as voters (now blocks until complete!)
        for node_id in 2..=5 {
            let idx = (node_id - 1) as usize;
            tracing::info!("Adding node {} as voter", node_id);
            let conf_state = cluster.nodes[0]
                .get_node()
                .unwrap()
                .add_peer(node_id, cluster.nodes[idx].addr)
                .await
                .expect(&format!("Failed to add node {}", node_id));
            tracing::info!("Node {} added, voters: {:?}", node_id, conf_state.voters);
        }

        tracing::info!("✓ All nodes added as voters");

        // Make initial proposals to synchronize cluster
        for i in 1..=20 {
            let cmd = format!("SET init{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tracing::info!("✓ Cluster initialized");
        tokio::time::sleep(Duration::from_secs(2)).await;

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
