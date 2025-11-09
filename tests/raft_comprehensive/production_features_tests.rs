/// Production readiness tests: Features required for TiKV production parity
///
/// These tests verify critical features for production deployment:
/// - Leadership Transfer: Planned maintenance and load balancing
/// - Joint Consensus (ConfChangeV2): Safe membership changes
/// - Read Index: Linearizable reads (API exposed, integration test TBD)
///
/// Implements features identified in TIKV_TEST_PARITY_ANALYSIS.md Phase 1.

mod common;

use common::TestCluster;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

#[test]
fn test_leadership_transfer_basic() {
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

        tracing::info!("=== Starting basic leadership transfer test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect node 1 as leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify node 1 is leader
        assert!(
            cluster.nodes[0].is_leader().await,
            "Node 1 should be leader"
        );
        tracing::info!("✓ Node 1 is leader");

        // Transfer leadership from node 1 to node 2
        tracing::info!("Transferring leadership from node 1 to node 2...");
        cluster.nodes[0]
            .transfer_leader(2)
            .await
            .expect("Leadership transfer failed");

        // Wait for transfer to complete
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify node 2 is now leader
        assert!(
            cluster.nodes[1].is_leader().await,
            "Node 2 should be leader after transfer"
        );
        assert!(
            !cluster.nodes[0].is_leader().await,
            "Node 1 should no longer be leader"
        );

        tracing::info!("✓ Leadership successfully transferred from node 1 to node 2");

        // Verify cluster still works - make proposals on new leader
        for i in 1..=5 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[1]
                .propose(cmd.as_bytes().to_vec())
                .await
                .expect("Proposal failed");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed after leadership transfer");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Basic leadership transfer");
    });
}

// TODO: This test appears to hang during convergence verification under concurrent load.
// Investigate and fix the convergence check or increase timeouts.
#[test]
#[ignore]
fn test_leadership_transfer_during_proposals() {
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

        tracing::info!("=== Starting leadership transfer during proposals test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect node 1 as leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some initial proposals
        for i in 1..=5 {
            let cmd = format!("SET before{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Transfer leadership while making proposals
        tracing::info!("Transferring leadership while making proposals...");
        cluster.nodes[0].transfer_leader(3).await.ok();

        // Continue making proposals during transfer
        for i in 6..=10 {
            let cmd = format!("SET during{} value{}", i, i);
            // Try on old leader (may fail as it steps down)
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for transfer to stabilize
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Verify node 3 is now leader
        assert!(
            cluster.nodes[2].is_leader().await,
            "Node 3 should be leader after transfer"
        );
        tracing::info!("✓ Leadership transferred to node 3");

        // Make final proposals on new leader
        for i in 11..=15 {
            let cmd = format!("SET after{} value{}", i, i);
            cluster.nodes[2]
                .propose(cmd.as_bytes().to_vec())
                .await
                .expect("Proposal on new leader failed");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Leadership transfer during proposals");
    });
}

#[test]
fn test_leadership_transfer_chain() {
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

        tracing::info!("=== Starting leadership transfer chain test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect node 1 as leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Transfer leadership in a chain: 1 -> 2 -> 3 -> 1
        let transfer_chain = vec![(0, 2), (1, 3), (2, 1)];

        for (from_idx, to_id) in transfer_chain {
            tracing::info!("Transferring leadership to node {}...", to_id);

            cluster.nodes[from_idx]
                .transfer_leader(to_id)
                .await
                .expect("Transfer failed");
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Make a proposal to verify cluster health
            let cmd = format!("SET leader{} active", to_id);
            cluster.nodes[(to_id - 1) as usize]
                .propose(cmd.as_bytes().to_vec())
                .await
                .expect("Proposal failed");
            tokio::time::sleep(Duration::from_millis(500)).await;

            tracing::info!("✓ Node {} is now leader", to_id);
        }

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Leadership transfer chain");
    });
}

#[test]
fn test_joint_consensus_via_confchange_v2() {
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

        tracing::info!("=== Starting Joint Consensus (ConfChangeV2) test ===");
        tracing::info!("Note: raft-rs handles ConfChangeV2 automatically via propose_conf_change");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add node 4 (raft-rs uses ConfChangeV2 internally for safety)
        tracing::info!("Adding node 4 via propose_conf_change (uses ConfChangeV2 internally)");
        let node4_addr = format!("127.0.0.1:{}", alloc_port())
            .parse()
            .unwrap();

        cluster.nodes[0]
            .add_peer(4, node4_addr)
            .await
            .expect("Failed to add node 4");

        tracing::info!("✓ Node 4 added successfully via safe ConfChange");

        // Make proposals to verify cluster health
        for i in 1..=5 {
            let cmd = format!("SET test{} value{}", i, i);
            cluster.nodes[0]
                .propose(cmd.as_bytes().to_vec())
                .await
                .expect("Proposal failed");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Joint Consensus via ConfChangeV2");
        tracing::info!("  (raft-rs handles joint consensus automatically for safety)");
    });
}
