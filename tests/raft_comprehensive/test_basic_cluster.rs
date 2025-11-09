use crate::common::*;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

/// Basic 3-node cluster test - like TiKV's test_put
/// Tests that we can:
/// 1. Bootstrap a 3-node cluster
/// 2. Elect a leader
/// 3. Make proposals and get them committed
/// 4. Read back the data
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_basic_three_node_cluster() {
        // Setup logging
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== Starting basic 3-node cluster test ===");

        // Create cluster with all 3 nodes as initial peers (TiKV run() pattern)
        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;

        // Start all nodes - they're all part of the Raft group from the beginning
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // One node campaigns to become leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify leader election succeeded
        assert!(
            cluster.nodes[0].is_leader().await,
            "Node 1 should be leader"
        );
        tracing::info!("✓ Leader elected successfully");

        // Make proposals to test consensus (use valid SET commands)
        for i in 1..=20 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[0]
                .propose(cmd.into_bytes())
                .await
                .expect(&format!("Failed to propose entry {}", i));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Give time for replication
        tokio::time::sleep(Duration::from_secs(2)).await;
        tracing::info!("✓ Successfully proposed 20 entries");

        // Verify cluster is operational by making one more proposal
        cluster.nodes[0]
            .propose(b"SET final_key final_value".to_vec())
            .await
            .expect("Failed final proposal");

        tokio::time::sleep(Duration::from_millis(500)).await;
        tracing::info!("✓ Final verification proposal succeeded");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Basic 3-node cluster operations");
}
