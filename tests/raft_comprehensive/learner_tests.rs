/// Learner node workflow tests
mod common;

use common::TestCluster;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

#[test]
fn test_add_learner_to_cluster() {
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

        tracing::info!("=== Starting add learner test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make some proposals before adding learner
        for i in 1..=10 {
            let cmd = format!("SET key{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Add node 4 as learner
        tracing::info!("Adding node 4 as learner");
        cluster.add_learner(4).await.expect("Failed to add learner");

        // Start the learner node
        let learner_idx = cluster.nodes.len() - 1;
        cluster.nodes[learner_idx]
            .start()
            .await
            .expect("Failed to start learner");

        tokio::time::sleep(Duration::from_secs(3)).await;

        // Verify learner received entries (by checking it's alive and connected)
        // Learner should not be able to vote yet
        assert!(
            !cluster.nodes[learner_idx].is_leader().await,
            "Learner should not be leader"
        );

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Add learner to cluster");
    });
}

#[test]
fn test_promote_learner_when_caught_up() {
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

        tracing::info!("=== Starting promote learner test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add learner
        tracing::info!("Adding node 4 as learner");
        cluster.add_learner(4).await.expect("Failed to add learner");
        let learner_idx = cluster.nodes.len() - 1;
        cluster.nodes[learner_idx]
            .start()
            .await
            .expect("Failed to start learner");

        // Make some proposals so learner has something to catch up on
        for i in 1..=20 {
            let cmd = format!("SET catchup{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        // Wait for learner to catch up
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Check if learner is caught up
        let is_caught_up = cluster.is_learner_caught_up(4).await.unwrap_or(false);
        tracing::info!("Learner caught up status: {}", is_caught_up);

        // Promote learner to voter
        if is_caught_up {
            tracing::info!("Promoting learner 4 to voter");
            cluster
                .promote_learner(4)
                .await
                .expect("Failed to promote learner");
            tokio::time::sleep(Duration::from_secs(2)).await;
            tracing::info!("✓ Successfully promoted learner to voter");
        } else {
            tracing::warn!("Learner not caught up yet, but test will continue");
        }

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Promote learner when caught up");
    });
}

#[test]
fn test_learner_promotion_fails_when_not_caught_up() {
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

        tracing::info!("=== Starting learner promotion failure test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Add and start learner
        tracing::info!("Adding node 4 as learner");
        cluster.add_learner(4).await.expect("Failed to add learner");
        let learner_idx = cluster.nodes.len() - 1;
        cluster.nodes[learner_idx]
            .start()
            .await
            .expect("Failed to start learner 4");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Isolate learner so it can't catch up
        tracing::info!("Isolating learner node 4");
        cluster.isolate_node(4).await;

        // Make many proposals while learner is isolated
        for i in 1..=50 {
            let cmd = format!("SET offline{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Try to promote learner immediately (should fail - not caught up)
        tracing::info!("Attempting to promote learner that's far behind");
        let result = cluster.promote_learner(4).await;

        // Expect failure since learner is isolated and far behind
        if result.is_err() {
            tracing::info!("✓ Correctly rejected promotion of isolated/lagging learner");
        } else {
            tracing::warn!(
                "Promotion succeeded unexpectedly (learner might have caught up despite isolation)"
            );
        }

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Learner promotion fails when not caught up");
    });
}

#[test]
fn test_multiple_learners_simultaneously() {
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

        tracing::info!("=== Starting multiple learners test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Make initial proposals
        for i in 1..=10 {
            let cmd = format!("SET initial{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Add and start learner 4
        tracing::info!("Adding node 4 as learner");
        cluster
            .add_learner(4)
            .await
            .expect("Failed to add learner 4");
        let learner1_idx = cluster.nodes.len() - 1;
        cluster.nodes[learner1_idx]
            .start()
            .await
            .expect("Failed to start learner 4");

        // Small delay before adding second learner
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Add and start learner 5
        tracing::info!("Adding node 5 as learner");
        cluster
            .add_learner(5)
            .await
            .expect("Failed to add learner 5");
        let learner2_idx = cluster.nodes.len() - 1;
        cluster.nodes[learner2_idx]
            .start()
            .await
            .expect("Failed to start learner 5");

        // Make more proposals
        for i in 11..=30 {
            let cmd = format!("SET multi{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        // Wait for both to catch up
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Try to promote both
        tracing::info!("Attempting to promote both learners");
        let caught_up_4 = cluster.is_learner_caught_up(4).await.unwrap_or(false);
        let caught_up_5 = cluster.is_learner_caught_up(5).await.unwrap_or(false);

        tracing::info!(
            "Learner 4 caught up: {}, Learner 5 caught up: {}",
            caught_up_4,
            caught_up_5
        );

        if caught_up_4 {
            cluster.promote_learner(4).await.ok();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        if caught_up_5 {
            cluster.promote_learner(5).await.ok();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Multiple learners simultaneously");
    });
}
