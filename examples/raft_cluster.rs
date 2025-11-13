//! Example demonstrating a 3-node Raft cluster
//!
//! Run with: RUST_LOG=info cargo run --example raft_cluster

use octopii::{Config, OctopiiNode};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("octopii=debug".parse().unwrap())
                .add_directive("raft=debug".parse().unwrap()),
        )
        .init();

    println!("Starting 3-node Raft cluster...\n");

    // Node 1 configuration (initial leader)
    let config1 = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5001".parse()?,
        peers: vec!["127.0.0.1:5002".parse()?, "127.0.0.1:5003".parse()?],
        wal_dir: PathBuf::from("/tmp/octopii_node1"),
        worker_threads: 2,
        wal_batch_size: 100,
        wal_flush_interval_ms: 100,
        is_initial_leader: true,
        snapshot_lag_threshold: 200,
    };

    // Node 2 configuration (follower)
    let config2 = Config {
        node_id: 2,
        bind_addr: "127.0.0.1:5002".parse()?,
        peers: vec!["127.0.0.1:5001".parse()?, "127.0.0.1:5003".parse()?],
        wal_dir: PathBuf::from("/tmp/octopii_node2"),
        worker_threads: 2,
        wal_batch_size: 100,
        wal_flush_interval_ms: 100,
        is_initial_leader: false,
        snapshot_lag_threshold: 200,
    };

    // Node 3 configuration (follower)
    let config3 = Config {
        node_id: 3,
        bind_addr: "127.0.0.1:5003".parse()?,
        peers: vec!["127.0.0.1:5001".parse()?, "127.0.0.1:5002".parse()?],
        wal_dir: PathBuf::from("/tmp/octopii_node3"),
        worker_threads: 2,
        wal_batch_size: 100,
        wal_flush_interval_ms: 100,
        is_initial_leader: false,
        snapshot_lag_threshold: 200,
    };

    // Create nodes (must be done outside of tokio runtime to avoid nested runtime panic)
    println!("Creating nodes...");
    let node1 = OctopiiNode::new_blocking(config1)?;
    let node2 = OctopiiNode::new_blocking(config2)?;
    let node3 = OctopiiNode::new_blocking(config3)?;

    // Create a tokio runtime for async operations
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move { run_cluster(node1, node2, node3).await })
}

async fn run_cluster(
    node1: OctopiiNode,
    node2: OctopiiNode,
    node3: OctopiiNode,
) -> Result<(), Box<dyn std::error::Error>> {
    // Start nodes
    println!("Starting nodes...");
    node1.start().await?;
    node2.start().await?;
    node3.start().await?;

    println!("\nNodes started! Triggering election on node 1...");
    node1.campaign().await?;

    println!("Waiting for leader election...");
    sleep(Duration::from_secs(2)).await;

    // Check which node is the leader
    println!("\n=== Checking leader status ===");
    let is_leader1 = node1.is_leader().await;
    let is_leader2 = node2.is_leader().await;
    let is_leader3 = node3.is_leader().await;

    println!("Node 1 is leader: {}", is_leader1);
    println!("Node 2 is leader: {}", is_leader2);
    println!("Node 3 is leader: {}", is_leader3);

    // Propose some commands
    let leader_node = if is_leader1 {
        Some(&node1)
    } else if is_leader2 {
        Some(&node2)
    } else if is_leader3 {
        Some(&node3)
    } else {
        None
    };

    if let Some(leader) = leader_node {
        println!("\n=== Proposing commands via leader ===");

        let commands = vec![
            ("SET foo bar", "Setting foo=bar"),
            ("SET hello world", "Setting hello=world"),
            ("GET foo", "Getting foo"),
            ("SET count 42", "Setting count=42"),
            ("GET hello", "Getting hello"),
        ];

        for (cmd, desc) in commands {
            println!("Proposing: {}", desc);
            leader.propose(cmd.as_bytes().to_vec()).await?;
            sleep(Duration::from_millis(500)).await;
        }

        println!("\nCommands proposed! Waiting for replication...");
        sleep(Duration::from_secs(2)).await;
    } else {
        println!("\nNo leader elected yet (this is normal for initial startup)");
        println!("In a real deployment, you would wait longer for leader election");
    }

    println!("\n=== Cluster Demo Complete ===");
    println!("Check the logs to see:");
    println!("  - Leader election messages");
    println!("  - Raft heartbeats between nodes");
    println!("  - Log replication messages");
    println!("  - State machine updates");

    println!("\nPress Ctrl+C to exit...");
    sleep(Duration::from_secs(3600)).await; // Keep running

    Ok(())
}
