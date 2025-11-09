// Investigation into rapid restart hanging issue
//
// This test file is dedicated to understanding why multiple rapid crash/restart
// cycles cause tests to hang. We'll add extensive logging and try various
// timing strategies to identify the root cause.

use crate::common::TestCluster;
use std::time::Duration;
use tokio;

fn alloc_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static PORT: AtomicU16 = AtomicU16::new(35000);
    PORT.fetch_add(1, Ordering::SeqCst)
}

#[test]
fn test_single_restart_baseline() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== BASELINE: Single restart (this works) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some proposals
        tracing::info!("[BASELINE] Making 3 proposals before crash");
        for i in 1..=3 {
            let cmd = format!("SET before{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Single crash/restart
        tracing::info!("[BASELINE] Crashing node 3...");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("[BASELINE] Restarting node 3...");
        cluster.restart_node(3).await.expect("Failed to restart");
        tokio::time::sleep(Duration::from_secs(4)).await;

        tracing::info!("[BASELINE] Waiting a bit longer for node 3 to fully rejoin...");
        tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::info!("[BASELINE] Checking cluster state before proposals...");
        for (idx, node) in cluster.nodes.iter().enumerate() {
            let is_leader = node.is_leader().await;
            tracing::info!("[BASELINE] Node {} is_leader: {}", idx + 1, is_leader);
        }

        tracing::info!("[BASELINE] Making 3 proposals after restart - dynamically finding leader");
        for i in 4..=6 {
            let cmd = format!("SET after{} value{}", i, i);

            // Find current leader before each proposal
            let leader_idx = cluster.find_leader_index().await.expect("No leader found!");
            tracing::info!(
                "[BASELINE] Current leader is node {}, proposing: SET after{} value{}",
                leader_idx + 1,
                i,
                i
            );

            // Propose to the current leader
            cluster.nodes[leader_idx]
                .propose(cmd.as_bytes().to_vec())
                .await
                .expect("Proposal failed");
            tracing::info!("[BASELINE] Proposal {} completed successfully", i);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("[BASELINE] Checking if we have a leader...");
        let has_leader = cluster.has_leader().await;
        tracing::info!("[BASELINE] Has leader: {}", has_leader);

        if !has_leader {
            tracing::warn!("[BASELINE] NO LEADER! This is the problem!");
        }

        tracing::info!("[BASELINE] Skipping convergence check to isolate issue");
        //cluster
        //    .verify_convergence(Duration::from_secs(10))
        //    .await
        //    .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ BASELINE PASSED (without convergence check)");
    });
}

#[test]
fn test_double_restart_with_extended_delays() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== TEST 1: Double restart with EXTENDED delays ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Phase 1
        tracing::info!("[TEST1] Phase 1: Making 2 proposals");
        for i in 1..=2 {
            let cmd = format!("SET phase1_{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        // First crash/restart cycle
        tracing::info!("[TEST1] CYCLE 1: Crashing node 3...");
        cluster.crash_node(3).ok();
        tracing::info!("[TEST1] CYCLE 1: Waiting 3s after crash...");
        tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::info!("[TEST1] CYCLE 1: Making 2 proposals during crash");
        for i in 3..=4 {
            let cmd = format!("SET during1_{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST1] CYCLE 1: Restarting node 3...");
        cluster.restart_node(3).await.expect("Failed first restart");
        tracing::info!("[TEST1] CYCLE 1: Waiting 5s after restart for full recovery...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Phase 2: Proposals after first restart
        tracing::info!("[TEST1] Phase 2: Making 2 proposals after first restart");
        for i in 5..=6 {
            let cmd = format!("SET phase2_{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Second crash/restart cycle
        tracing::info!("[TEST1] CYCLE 2: Crashing node 3 again...");
        cluster.crash_node(3).ok();
        tracing::info!("[TEST1] CYCLE 2: Waiting 3s after second crash...");
        tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::info!("[TEST1] CYCLE 2: Making 2 proposals during second crash");
        for i in 7..=8 {
            let cmd = format!("SET during2_{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST1] CYCLE 2: Restarting node 3 second time...");
        cluster
            .restart_node(3)
            .await
            .expect("Failed second restart");
        tracing::info!("[TEST1] CYCLE 2: Waiting 5s after second restart for full recovery...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Final proposals
        tracing::info!("[TEST1] Final: Making 2 proposals");
        for i in 9..=10 {
            let cmd = format!("SET final_{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST1] Verifying convergence...");
        cluster
            .verify_convergence(Duration::from_secs(15))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ TEST1 PASSED: Double restart with extended delays works!");
    });
}

#[test]
fn test_rapid_restart_minimal_scenario() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== TEST 2: Minimal rapid restart (no proposals between cycles) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("[TEST2] Making initial proposals");
        for i in 1..=2 {
            let cmd = format!("SET initial{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Rapid cycle 1
        tracing::info!("[TEST2] RAPID CYCLE 1: Crash -> wait 2s -> restart -> wait 3s");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_secs(2)).await;
        cluster.restart_node(3).await.expect("Failed first restart");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Rapid cycle 2 (no proposals in between)
        tracing::info!("[TEST2] RAPID CYCLE 2: Crash -> wait 2s -> restart -> wait 3s");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_secs(2)).await;
        cluster
            .restart_node(3)
            .await
            .expect("Failed second restart");
        tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::info!("[TEST2] Making final proposals");
        for i in 3..=4 {
            let cmd = format!("SET final{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST2] Verifying convergence...");
        cluster
            .verify_convergence(Duration::from_secs(15))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ TEST2 PASSED: Rapid restart without proposals works!");
    });
}

#[test]
fn test_different_node_rapid_restarts() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== TEST 3: Restart DIFFERENT nodes (not same node twice) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("[TEST3] Making initial proposals");
        for i in 1..=2 {
            let cmd = format!("SET initial{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Crash/restart node 3
        tracing::info!("[TEST3] Cycle 1: Crash/restart node 3");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_secs(2)).await;
        cluster
            .restart_node(3)
            .await
            .expect("Failed to restart node 3");
        tokio::time::sleep(Duration::from_secs(4)).await;

        tracing::info!("[TEST3] Making middle proposals");
        for i in 3..=4 {
            let cmd = format!("SET middle{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Crash/restart node 2 (DIFFERENT node)
        tracing::info!("[TEST3] Cycle 2: Crash/restart node 2 (different node)");
        cluster.crash_node(2).ok();
        tokio::time::sleep(Duration::from_secs(2)).await;
        cluster
            .restart_node(2)
            .await
            .expect("Failed to restart node 2");
        tokio::time::sleep(Duration::from_secs(4)).await;

        tracing::info!("[TEST3] Making final proposals");
        for i in 5..=6 {
            let cmd = format!("SET final{} value{}", i, i);
            if let Some(leader_idx) = cluster.find_leader_index().await {
                cluster.nodes[leader_idx]
                    .propose(cmd.as_bytes().to_vec())
                    .await
                    .ok();
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST3] Verifying convergence...");
        cluster
            .verify_convergence(Duration::from_secs(15))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ TEST3 PASSED: Different nodes restart works!");
    });
}

#[test]
#[ignore] // Run this manually to see if it hangs
fn test_original_problematic_pattern() {
    let test_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    test_runtime.block_on(async {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init()
            .ok();

        tracing::info!("=== TEST 4: Original problematic pattern (may hang) ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Original pattern: many proposals, short waits
        for i in 1..=5 {
            let cmd = format!("SET phase1_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST4] First crash");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_millis(500)).await; // SHORT WAIT

        for i in 6..=10 {
            let cmd = format!("SET phase2_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST4] First restart");
        cluster.restart_node(3).await.expect("Failed to restart");
        tokio::time::sleep(Duration::from_secs(3)).await;

        for i in 11..=15 {
            let cmd = format!("SET phase3_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST4] Second crash");
        cluster.crash_node(3).ok();
        tokio::time::sleep(Duration::from_millis(500)).await; // SHORT WAIT

        for i in 16..=20 {
            let cmd = format!("SET phase4_{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        tracing::info!("[TEST4] Second restart (this is where it might hang)");
        cluster
            .restart_node(3)
            .await
            .expect("Failed second restart");
        tokio::time::sleep(Duration::from_secs(5)).await;

        tracing::info!("[TEST4] Verifying convergence (or hang here)...");
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ TEST4 PASSED (unexpected!)");
    });
}
