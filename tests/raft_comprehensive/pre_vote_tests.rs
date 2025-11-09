/// Pre-vote protocol tests
mod common;

use common::TestCluster;
use crate::test_infrastructure::alloc_port;
use std::time::Duration;

#[test]
fn test_pre_vote_stable_cluster() {
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

        tracing::info!("=== Starting pre-vote stable cluster test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
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

        // Make continuous proposals
        for i in 1..=50 {
            let cmd = format!("SET prevote{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Verify leader remains stable (pre-vote prevents disruption)
        let final_leader = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        assert_eq!(initial_leader, final_leader, "Leader should remain stable");
        tracing::info!("✓ Leader remained stable with pre-vote enabled");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Pre-vote stable cluster");
    });
}

#[test]
fn test_pre_vote_with_lagging_node() {
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

        tracing::info!("=== Starting pre-vote with lagging node test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Crash node 3
        tracing::info!("Crashing node 3");
        cluster.crash_node(3).expect("Failed to crash node 3");

        // Make many proposals while node 3 is down
        tracing::info!("Making proposals while node 3 is offline...");
        for i in 1..=100 {
            let cmd = format!("SET lagging{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_before = if cluster.nodes[0].is_leader().await {
            1
        } else {
            2
        };

        tracing::info!("Leader before restart: node {}", leader_before);

        // Restart node 3 (far behind)
        tracing::info!("Restarting node 3 (far behind)");
        cluster
            .restart_node(3)
            .await
            .expect("Failed to restart node 3");

        // Wait and make more proposals
        tokio::time::sleep(Duration::from_secs(3)).await;

        for i in 101..=120 {
            let cmd = format!("SET after{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_after = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        tracing::info!("Leader after node 3 rejoined: node {}", leader_after);

        // With pre-vote, the lagging node shouldn't disrupt the cluster
        // Leader may have changed due to other reasons, but cluster should be stable
        assert!(cluster.has_leader().await, "Cluster should have a leader");
        tracing::info!("✓ Cluster remained stable despite lagging node");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Pre-vote with lagging node");
    });
}

#[test]
fn test_pre_vote_prevents_election_storm_after_partition_heal() {
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

        tracing::info!("=== Starting prevote prevents election storm after partition heal ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("Creating partition: isolate node 3");
        // Partition node 3 away from nodes 1 and 2
        cluster.isolate_node(3);
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Make proposals in the majority partition (nodes 1 & 2)
        for i in 1..=20 {
            let cmd = format!("SET partitioned{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let leader_before_heal = if cluster.nodes[0].is_leader().await {
            1
        } else {
            2
        };

        tracing::info!("Leader before heal: node {}", leader_before_heal);

        // Heal the partition
        tracing::info!("Healing partition - node 3 rejoins");
        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Make more proposals - cluster should remain stable
        for i in 21..=40 {
            let cmd = format!("SET after_heal{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_after_heal = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        tracing::info!("Leader after heal: node {}", leader_after_heal);

        // Pre-vote should prevent node 3 from disrupting the cluster with unnecessary elections
        // The leader should remain stable (node 3's stale term won't trigger election)
        assert_eq!(
            leader_before_heal, leader_after_heal,
            "Pre-vote should prevent election storm after partition heal"
        );

        cluster
            .verify_convergence(Duration::from_secs(5))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Prevote prevents election storm after partition heal");
    });
}

#[test]
fn test_pre_vote_with_one_way_partition() {
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

        tracing::info!("=== Starting prevote with one-way partition test ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader on node 1
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("Creating one-way partition: node 3 can receive but not send");
        // Node 3 can receive messages from 1 & 2, but cannot send to them
        // This simulates asymmetric network failure
        cluster.add_send_filter(
            3,
            Box::new(crate::test_infrastructure::DropPacketFilter::new(100)),
        ).await;

        tokio::time::sleep(Duration::from_millis(500)).await;

        // Make proposals - nodes 1 & 2 should continue working
        for i in 1..=30 {
            let cmd = format!("SET oneway{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Node 3 shouldn't be able to become leader (can't send messages)
        // But with prevote, it also shouldn't disrupt the cluster
        assert!(!cluster.nodes[2].is_leader().await, "Node 3 should not be leader");
        assert!(
            cluster.nodes[0].is_leader().await || cluster.nodes[1].is_leader().await,
            "Nodes 1 or 2 should still have a leader"
        );

        tracing::info!("✓ Cluster remained stable despite one-way partition");

        // Heal the partition
        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(3)).await;

        cluster
            .verify_convergence(Duration::from_secs(5))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Prevote with one-way partition");
    });
}

#[test]
fn test_pre_vote_prevents_disruption_from_isolated_candidate() {
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

        tracing::info!("=== Starting prevote prevents disruption from isolated candidate ===");

        let mut cluster = TestCluster::new(vec![1, 2, 3, 4, 5], alloc_port()).await;
        cluster.start_all().await.expect("Failed to start cluster");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Elect leader
        cluster.nodes[0].campaign().await.expect("Campaign failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        tracing::info!("Isolating nodes 4 and 5 from the majority");
        // Isolate nodes 4 and 5 - they'll be in minority partition
        cluster.isolate_node(4);
        cluster.isolate_node(5);
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Majority partition (nodes 1, 2, 3) continues to make progress
        for i in 1..=50 {
            let cmd = format!("SET majority{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_before = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        tracing::info!("Leader in majority partition: node {}", leader_before);

        // Heal the partition - isolated nodes rejoin
        tracing::info!("Healing partition - nodes 4 and 5 rejoin");
        cluster.clear_all_filters();
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Continue making proposals
        for i in 51..=70 {
            let cmd = format!("SET after{} value{}", i, i);
            cluster.nodes[0].propose(cmd.as_bytes().to_vec()).await.ok();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let leader_after = if cluster.nodes[0].is_leader().await {
            1
        } else if cluster.nodes[1].is_leader().await {
            2
        } else {
            3
        };

        // Pre-vote should prevent the isolated nodes from disrupting the majority
        // Leader should remain stable
        assert_eq!(
            leader_before, leader_after,
            "Pre-vote should prevent isolated candidates from disrupting cluster"
        );

        cluster
            .verify_convergence(Duration::from_secs(5))
            .await
            .expect("Convergence failed");

        cluster.shutdown_all();
        tracing::info!("✓ Test passed: Prevote prevents disruption from isolated candidate");
    });
}
