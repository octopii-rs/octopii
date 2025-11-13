#![cfg(feature = "raft-rs-impl")]
/// Message chaos tests: Testing Raft under adverse network conditions
///
/// These tests verify that Raft correctly handles:
/// - Duplicate messages (network retransmissions)
/// - Out-of-order message delivery
/// - Slow followers (network throttling)
///
/// Inspired by TiKV's comprehensive network chaos testing.
mod common;

use crate::test_infrastructure::{
    alloc_port, MessageDuplicationFilter, MessageReorderFilter, ThrottleFilter,
};
use common::TestCluster;
use std::time::Duration;

#[test]
fn test_message_duplication_idempotency() {
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

        tracing::info!("=== Starting message duplication test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add message duplication filter on all nodes (50% duplication rate)
        cluster
            .add_send_filter(1, Box::new(MessageDuplicationFilter::new(50)))
            .await;
        cluster
            .add_send_filter(2, Box::new(MessageDuplicationFilter::new(50)))
            .await;
        cluster
            .add_send_filter(3, Box::new(MessageDuplicationFilter::new(50)))
            .await;

        tracing::info!("Duplication filters active - making proposals");

        // Make proposals with duplicated messages
        for i in 1..=10 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence despite duplicates
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed with message duplication");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Message duplication handled correctly");
    });
}

#[test]
fn test_message_reordering() {
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

        tracing::info!("=== Starting message reordering test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add message reordering filter on all nodes
        cluster
            .add_send_filter(1, Box::new(MessageReorderFilter::new()))
            .await;
        cluster
            .add_send_filter(2, Box::new(MessageReorderFilter::new()))
            .await;
        cluster
            .add_send_filter(3, Box::new(MessageReorderFilter::new()))
            .await;

        tracing::info!("Reordering filters active - making proposals");

        // Make sequential proposals
        for i in 1..=10 {
            let cmd = format!("SET seq{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify convergence despite message reordering
        cluster
            .verify_convergence(Duration::from_secs(10))
            .await
            .expect("Convergence failed with message reordering");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Message reordering handled correctly");
    });
}

#[test]
fn test_slow_follower_with_throttling() {
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

        tracing::info!("=== Starting slow follower test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Throttle node 3 to make it a slow follower
        // Allow only 5 messages per 200ms
        cluster.add_recv_filter(
            3,
            Box::new(ThrottleFilter::new(Duration::from_millis(200), 5)),
        );

        tracing::info!("Throttle filter active on node 3 - making many proposals");

        // Make many proposals to trigger lag on throttled follower
        for i in 1..=50 {
            let cmd = format!("SET item{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Give time for the slow follower to catch up
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify all nodes eventually converge
        cluster
            .verify_convergence(Duration::from_secs(15))
            .await
            .expect("Slow follower failed to converge");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Slow follower eventually caught up");
    });
}

#[test]
fn test_combined_network_chaos() {
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

        tracing::info!("=== Starting combined network chaos test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Apply different chaos filters to different nodes
        // Node 1: message duplication
        cluster
            .add_send_filter(1, Box::new(MessageDuplicationFilter::new(30)))
            .await;

        // Node 2: message reordering
        cluster
            .add_send_filter(2, Box::new(MessageReorderFilter::new()))
            .await;

        // Node 3: throttling (slow follower)
        cluster.add_recv_filter(
            3,
            Box::new(ThrottleFilter::new(Duration::from_millis(200), 10)),
        );

        tracing::info!("All chaos filters active - testing under maximum adversity");

        // Make proposals under full chaos
        for i in 1..=30 {
            let cmd = format!("SET chaos{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Give plenty of time for convergence under chaos
        tokio::time::sleep(Duration::from_secs(8)).await;

        // Verify convergence despite all network chaos
        cluster
            .verify_convergence(Duration::from_secs(15))
            .await
            .expect("Failed to converge under network chaos");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Raft survives combined network chaos");
    });
}
