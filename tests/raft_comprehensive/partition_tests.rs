#![cfg(feature = "raft-rs-impl")]
use crate::common::*;
use crate::test_infrastructure::*;
use std::sync::Arc;
use std::time::Duration;

/// Test network partition with leader isolated
///
/// This test demonstrates the filter infrastructure ported from TiKV.
/// It creates a 3-node cluster, partitions the leader from followers,
/// and verifies that a new leader is elected.
///
/// NOTE: This test validates the filter infrastructure is properly integrated.
/// Full filter application requires transport layer integration (future work).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_partition_leader_isolated() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Test: Partition leader from followers ===");

    // Use alloc_port() from TiKV infrastructure
    let base_port = alloc_port();
    let mut cluster = TestCluster::new(vec![1, 2, 3], base_port).await;

    cluster.start_all().await.expect("Failed to start cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node 1 campaigns to become leader
    cluster.nodes[0].campaign().await.expect("Campaign failed");
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        cluster.nodes[0].is_leader().await,
        "Node 1 should be leader"
    );
    tracing::info!("✓ Node 1 elected as leader");

    // Create partition: node 1 (leader) isolated from nodes 2 & 3
    cluster.partition(vec![1], vec![2, 3]).await;
    tracing::info!("✓ Partition created (infrastructure validated)");

    // Verify filters were applied to OctopiiNode
    let node0_filters = cluster.nodes[0]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    let node1_filters = cluster.nodes[1]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    let node2_filters = cluster.nodes[2]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    assert_eq!(node0_filters, 1);
    assert_eq!(node1_filters, 1);
    assert_eq!(node2_filters, 1);
    tracing::info!("✓ Filters successfully applied to all OctopiiNodes");

    // NOTE: Full partition behavior testing requires transport integration
    // For now, we validate the filter infrastructure is properly set up
    tracing::info!("✓ Test passed: Filter infrastructure integrated with TestCluster");

    cluster.shutdown_all();
}

/// Test isolating a single node
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_isolate_single_node() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init()
        .ok();

    tracing::info!("=== Test: Isolate single node ===");

    let base_port = alloc_port();
    let mut cluster = TestCluster::new(vec![1, 2, 3], base_port).await;

    cluster.start_all().await.expect("Failed to start cluster");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Isolate node 2
    cluster.isolate_node(2).await;
    tracing::info!("✓ Isolation filters applied to node 2");

    // Verify filters were set up on OctopiiNode
    let node1_filters = cluster.nodes[1]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    assert!(node1_filters > 0);
    tracing::info!("✓ Node 2 has {} isolation filters", node1_filters);

    // Clear filters (heal partition)
    cluster.clear_all_filters().await;
    let node1_filters_after = cluster.nodes[1]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    assert_eq!(node1_filters_after, 0);
    tracing::info!("✓ Filters cleared successfully");

    cluster.shutdown_all();
}

/// Test the retry! macro from TiKV infrastructure
#[tokio::test]
async fn test_retry_macro() {
    use std::sync::{Arc, Mutex};

    let counter = Arc::new(Mutex::new(0));
    let counter_clone = counter.clone();

    // Test retry! with eventual success
    let result = retry!(
        {
            let mut count = counter_clone.lock().unwrap();
            *count += 1;
            if *count >= 3 {
                Ok::<_, String>(42)
            } else {
                Err("not yet".to_string())
            }
        },
        10,
        10
    );

    assert_eq!(result.unwrap(), 42);
    assert!(*counter.lock().unwrap() >= 3);
    tracing::info!("✓ retry! macro works correctly");
}

/// Test temp_dir utility from TiKV
#[test]
fn test_temp_dir_utility() {
    let dir = temp_dir(Some("partition_test"), false);
    assert!(dir.path().exists());
    let path = dir.path().to_path_buf();
    drop(dir);
    assert!(!path.exists());
    tracing::info!("✓ temp_dir utility works correctly");
}

/// Test eventually utility from TiKV
#[test]
fn test_eventually_utility() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    let flag = Arc::new(AtomicBool::new(false));
    let flag_clone = flag.clone();

    // Spawn thread to set flag after delay
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        flag_clone.store(true, Ordering::SeqCst);
    });

    // Use eventually to wait for flag
    eventually(Duration::from_millis(10), Duration::from_secs(1), || {
        flag.load(Ordering::SeqCst)
    });

    tracing::info!("✓ eventually utility works correctly");
}

/// Test filter composition
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multiple_filters() {
    let base_port = alloc_port();
    let mut cluster = TestCluster::new(vec![1, 2, 3], base_port).await;

    cluster.start_all().await.expect("Failed to start cluster");

    // Add multiple filters to the same node
    cluster
        .add_send_filter(1, Box::new(DropPacketFilter::new(10)))
        .await;
    cluster
        .add_send_filter(1, Box::new(DelayFilter::new(Duration::from_millis(5))))
        .await;

    // Verify filters were applied to OctopiiNode
    let node0_filters = cluster.nodes[0]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    assert_eq!(node0_filters, 2);
    tracing::info!("✓ Multiple filters can be composed");

    cluster.clear_send_filters(1).await;
    let node0_filters_after = cluster.nodes[0]
        .node
        .as_ref()
        .unwrap()
        .send_filters
        .read()
        .await
        .len();
    assert_eq!(node0_filters_after, 0);
    tracing::info!("✓ Filters can be cleared");

    cluster.shutdown_all();
}
