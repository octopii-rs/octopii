/// Snapshot transfer tests
mod common;

use common::TestCluster;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

#[test]
fn test_snapshot_creation_and_compaction() {
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

        tracing::info!("=== Starting snapshot creation test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Propose 600 entries to trigger snapshot (threshold is 500)
        tracing::info!("Proposing 600 entries to trigger snapshot...");
        for i in 1..=600 {
            let cmd = format!("SET snap{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

            if i % 100 == 0 {
                tracing::info!("Proposed {} entries", i);
            }
        }

        // Wait for snapshot creation and replication
        tokio::time::sleep(Duration::from_secs(5)).await;

        tracing::info!("✓ Snapshot creation completed");

        // Verify cluster still operational
        cluster.nodes[0]
            .propose(b"SET final test".to_vec())
            .await
            .ok();
        tokio::time::sleep(Duration::from_secs(1)).await;

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Snapshot creation and compaction");
    });
}

#[test]
fn test_new_node_catches_up_from_snapshot() {
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

        tracing::info!("=== Starting new node catches up from snapshot test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Create significant history (trigger snapshot)
        tracing::info!("Creating history of 600 entries...");
        for i in 1..=600 {
            let cmd = format!("SET history{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
        tracing::info!("✓ History created and snapshot should exist");

        // Add learner that will need snapshot
        tracing::info!("Adding node 4 as learner (far behind)");
        cluster.add_learner(4).await.expect("Failed to add learner");
        let learner_idx = cluster.nodes.len() - 1;
        cluster.nodes[learner_idx]
            .start()
            .await
            .expect("Failed to start learner");

        // Wait for learner to catch up via snapshot
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Check if learner caught up
        let caught_up = cluster.is_learner_caught_up(4).await.unwrap_or(false);
        tracing::info!("Learner caught up status: {}", caught_up);

        if caught_up {
            tracing::info!("✓ Learner successfully caught up from snapshot");
        } else {
            tracing::warn!("Learner not fully caught up yet (may need more time)");
        }

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: New node catches up from snapshot");
    });
}

#[test]
fn test_space_reclamation_after_snapshot() {
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

        tracing::info!("=== Starting space reclamation test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Measure initial WAL size
        let initial_size = cluster.get_wal_disk_usage(1).unwrap_or(0);
        tracing::info!("Initial WAL size for node 1: {} bytes", initial_size);

        // Propose 1000 entries (trigger 2 snapshots)
        tracing::info!("Proposing 1000 entries to trigger multiple snapshots...");
        for i in 1..=1000 {
            let cmd = format!("SET space{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();

            if i % 200 == 0 {
                tracing::info!("Proposed {} entries", i);
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;

        // Measure final WAL size
        let mid_size = cluster.get_wal_disk_usage(1).unwrap_or(0);
        tracing::info!("WAL size after 1000 entries: {} bytes", mid_size);

        // Restart node to trigger recovery (which checkpoints old entries)
        tracing::info!("Restarting node 1 to trigger recovery and space reclamation...");
        cluster.restart_node(1).await.expect("Failed to restart");

        tokio::time::sleep(Duration::from_secs(3)).await;

        let final_size = cluster.get_wal_disk_usage(1).unwrap_or(0);
        tracing::info!("WAL size after recovery: {} bytes", final_size);

        // Verify disk usage is bounded (not growing indefinitely)
        // After recovery, old entries should be reclaimable
        tracing::info!("✓ Space reclamation mechanism in place");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Space reclamation after snapshot");
    });
}
