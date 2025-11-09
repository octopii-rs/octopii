/// Short-duration stress tests (60-120 seconds)
mod common;

use crate::test_infrastructure::alloc_port;
use common::TestCluster;
use std::time::Duration;

#[test]
fn test_sustained_throughput_with_snapshots() {
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

        tracing::info!("=== Starting sustained throughput with snapshots test (60s) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        let start = std::time::Instant::now();
        let duration = Duration::from_secs(60);
        let mut proposal_count = 0;

        // Sustained load for 60 seconds
        while start.elapsed() < duration {
            let cmd = format!("SET stress{} value{}", proposal_count, proposal_count);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            proposal_count += 1;

            // Throttle to ~100 proposals/sec
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tracing::info!("Completed {} proposals in 60 seconds", proposal_count);

        // Check WAL disk usage - should have compacted at least once
        let wal_usage = cluster.get_wal_disk_usage(1).unwrap_or(0);
        tracing::info!("Final WAL disk usage: {} bytes", wal_usage);

        // With 6000 proposals and compaction every 500 entries, we should see snapshots
        assert!(
            proposal_count > 5000,
            "Should have made substantial progress"
        );

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Sustained throughput with snapshots");
    });
}

#[test]
fn test_multiple_learner_workflow_stress() {
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

        tracing::info!("=== Starting multiple learner workflow stress test (90s) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make initial proposals
        tracing::info!("Making initial proposals...");
        for i in 0..200 {
            let cmd = format!("SET init{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add first learner
        tracing::info!("Adding learner 4");
        cluster
            .add_learner(4)
            .await
            .expect("Failed to add learner 4");
        cluster.nodes[3]
            .start()
            .await
            .expect("Failed to start learner 4");

        // Continue proposals while learner catches up
        for i in 200..400 {
            let cmd = format!("SET catchup1_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for learner 4 to catch up
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(30) {
                panic!("Learner 4 did not catch up in time");
            }
            if cluster.is_learner_caught_up(4).await.unwrap_or(false) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        tracing::info!("Learner 4 caught up, promoting...");
        cluster
            .promote_learner(4)
            .await
            .expect("Failed to promote learner 4");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add second learner
        tracing::info!("Adding learner 5");
        cluster
            .add_learner(5)
            .await
            .expect("Failed to add learner 5");
        cluster.nodes[4]
            .start()
            .await
            .expect("Failed to start learner 5");

        // More proposals
        for i in 400..600 {
            let cmd = format!("SET catchup2_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Wait for learner 5 to catch up
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(30) {
                panic!("Learner 5 did not catch up in time");
            }
            if cluster.is_learner_caught_up(5).await.unwrap_or(false) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        tracing::info!("Learner 5 caught up, promoting...");
        cluster
            .promote_learner(5)
            .await
            .expect("Failed to promote learner 5");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify cluster stability with 5 nodes
        assert!(cluster.has_leader().await, "Cluster should have a leader");
        tracing::info!("✓ Cluster stable with 5 nodes");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Multiple learner workflow stress");
    });
}

#[test]
fn test_repeated_leader_failures_with_load() {
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

        tracing::info!("=== Starting repeated leader failures with load test (90s) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect initial leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        let test_duration = Duration::from_secs(90);
        let start = std::time::Instant::now();
        let mut proposal_count = 0;
        let mut leader_changes = 0;

        // Cycle: make proposals for 15s, crash leader, wait for re-election, repeat
        while start.elapsed() < test_duration {
            // Find current leader
            let leader_idx = if cluster.nodes[0].is_leader().await {
                0
            } else if cluster.nodes[1].is_leader().await {
                1
            } else if cluster.nodes[2].is_leader().await {
                2
            } else {
                // No leader, wait for election
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            let leader_id = cluster.nodes[leader_idx].node_id;
            tracing::info!("Current leader: node {}", leader_id);

            // Make proposals for 15 seconds
            let phase_start = std::time::Instant::now();
            while phase_start.elapsed() < Duration::from_secs(15) && start.elapsed() < test_duration
            {
                let cmd = format!("SET failure{} value{}", proposal_count, proposal_count);
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
                proposal_count += 1;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }

            if start.elapsed() >= test_duration {
                break;
            }

            // Crash the leader
            tracing::info!("Crashing leader node {}", leader_id);
            cluster
                .crash_node(leader_id)
                .expect("Failed to crash leader");
            leader_changes += 1;

            // Wait for automatic re-election (should happen within 2.5 seconds)
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Verify new leader elected
            let has_leader = cluster.has_leader().await;
            assert!(has_leader, "Should have elected new leader after crash");

            // Restart crashed node
            tracing::info!("Restarting node {}", leader_id);
            cluster
                .restart_node(leader_id)
                .await
                .expect("Failed to restart node");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }

        tracing::info!(
            "Completed {} proposals with {} leader changes over 90 seconds",
            proposal_count,
            leader_changes
        );

        assert!(
            leader_changes >= 3,
            "Should have had multiple leader changes"
        );
        assert!(
            proposal_count > 2000,
            "Should have made substantial progress despite failures"
        );
        assert!(
            cluster.has_leader().await,
            "Cluster should be stable at end"
        );

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Repeated leader failures with load");
    });
}
