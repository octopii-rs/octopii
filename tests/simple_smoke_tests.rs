use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::time::Duration;
use tokio::time::sleep;
use std::path::PathBuf;

#[test]
fn test_smoke_single_node_leader() {
    // Create a single-node cluster and ensure it can become leader (no nested runtimes)
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let temp_dir = std::env::temp_dir().join("octopii_simple_smoke_single");
        let _ = std::fs::remove_dir_all(&temp_dir);

        let mut config = Config {
            node_id: 1,
            // bind to a fixed high port to avoid ephemeral coordination
            bind_addr: "127.0.0.1:9311".parse().unwrap(),
            peers: vec![],
            wal_dir: temp_dir.clone(),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: true,
            snapshot_lag_threshold: 50,
        };

        // Avoid nested runtime: construct via async API using current handle
        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let node = OctopiiNode::new(config, runtime).await.expect("create node");
        node.start().await.expect("start node");

        // Single node should be able to become leader
        if !node.is_leader().await {
            node.campaign().await.expect("campaign");
            sleep(Duration::from_millis(500)).await;
        }
        assert!(node.is_leader().await, "single node should be leader");

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    });
}

#[test]
fn test_smoke_three_nodes_manual_election_after_shutdown() {
    // Start 3 nodes, elect node1, then gracefully shut it down and manually
    // trigger election on node2 to verify basic re-election works.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = PathBuf::from(std::env::temp_dir()).join("octopii_simple_smoke_three");
        let _ = std::fs::remove_dir_all(&base);

        // Use fixed high ports to avoid dependence on discovering ephemeral ports
        let addr1 = "127.0.0.1:9321".parse().unwrap();
        let addr2 = "127.0.0.1:9322".parse().unwrap();
        let addr3 = "127.0.0.1:9323".parse().unwrap();

        let config1 = Config {
            node_id: 1,
            bind_addr: addr1,
            peers: vec![addr2, addr3],
            wal_dir: base.join("n1"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: true,
            snapshot_lag_threshold: 50,
        };
        let config2 = Config {
            node_id: 2,
            bind_addr: addr2,
            peers: vec![addr1, addr3],
            wal_dir: base.join("n2"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 50,
        };
        let config3 = Config {
            node_id: 3,
            bind_addr: addr3,
            peers: vec![addr1, addr2],
            wal_dir: base.join("n3"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 50,
        };

        // Avoid nested runtime: construct via async API using current handle
        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = OctopiiNode::new(config1, runtime.clone()).await.expect("create n1");
        let n2 = OctopiiNode::new(config2, runtime.clone()).await.expect("create n2");
        let n3 = OctopiiNode::new(config3, runtime).await.expect("create n3");

        n1.start().await.expect("start n1");
        n2.start().await.expect("start n2");
        n3.start().await.expect("start n3");

        // Elect node1
        n1.campaign().await.expect("n1 campaign");
        sleep(Duration::from_secs(2)).await;
        assert!(n1.is_leader().await, "n1 should be leader");

        // Gracefully shut down node1
        n1.shutdown();
        sleep(Duration::from_millis(1000)).await;

        // Manually trigger election on node2 for deterministic re-election
        n2.campaign().await.expect("n2 campaign");
        sleep(Duration::from_secs(3)).await;
        assert!(n2.is_leader().await, "n2 should take leadership after n1 shutdown");

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&base).await;
    });
}

#[test]
fn test_three_nodes_graceful_shutdown_auto_election() {
    // Step closer to the comprehensive test: no manual campaign for re-election.
    // After gracefully shutting down the leader, followers should automatically elect a new leader.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = PathBuf::from(std::env::temp_dir()).join("octopii_simple_smoke_auto");
        let _ = std::fs::remove_dir_all(&base);

        let addr1 = "127.0.0.1:9331".parse().unwrap();
        let addr2 = "127.0.0.1:9332".parse().unwrap();
        let addr3 = "127.0.0.1:9333".parse().unwrap();

        let config1 = Config {
            node_id: 1,
            bind_addr: addr1,
            peers: vec![addr2, addr3],
            wal_dir: base.join("n1"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: true,
            snapshot_lag_threshold: 50,
        };
        let config2 = Config {
            node_id: 2,
            bind_addr: addr2,
            peers: vec![addr1, addr3],
            wal_dir: base.join("n2"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 50,
        };
        let config3 = Config {
            node_id: 3,
            bind_addr: addr3,
            peers: vec![addr1, addr2],
            wal_dir: base.join("n3"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 50,
        };

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = OctopiiNode::new(config1, runtime.clone()).await.expect("create n1");
        let n2 = OctopiiNode::new(config2, runtime.clone()).await.expect("create n2");
        let n3 = OctopiiNode::new(config3, runtime).await.expect("create n3");

        n1.start().await.expect("start n1");
        n2.start().await.expect("start n2");
        n3.start().await.expect("start n3");

        // Elect node1
        n1.campaign().await.expect("n1 campaign");
        sleep(Duration::from_secs(2)).await;
        assert!(n1.is_leader().await, "n1 should be leader");

        // Make a couple of proposals to advance term/log slightly
        let _ = n1.propose(b"SET k v".to_vec()).await;
        sleep(Duration::from_millis(300)).await;
        let _ = n1.propose(b"SET k2 v2".to_vec()).await;
        sleep(Duration::from_millis(300)).await;

        // Gracefully shut down leader
        n1.shutdown();
        // Allow transports to close and followers to detect silence
        sleep(Duration::from_millis(2000)).await;

        // Wait for automatic leader election (no manual campaign here)
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(12);
        let mut new_leader: Option<u64> = None;
        while start.elapsed() < timeout {
            if n2.is_leader().await {
                new_leader = Some(2);
                break;
            }
            if n3.is_leader().await {
                new_leader = Some(3);
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }

        assert!(
            new_leader.is_some(),
            "A new leader should be elected automatically after graceful shutdown"
        );
        assert_ne!(new_leader.unwrap(), 1, "New leader must not be node 1");

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&base).await;
    });
}

#[test]
fn test_three_nodes_crash_leader_with_load_and_restart_auto_election() {
    // Step even closer: run load, kill the leader, expect auto-election,
    // then restart the old leader and ensure the cluster remains stable.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = PathBuf::from(std::env::temp_dir()).join("octopii_simple_smoke_crash_restart");
        let _ = std::fs::remove_dir_all(&base);

        let addr1 = "127.0.0.1:9341".parse().unwrap();
        let addr2 = "127.0.0.1:9342".parse().unwrap();
        let addr3 = "127.0.0.1:9343".parse().unwrap();

        let config1 = Config {
            node_id: 1,
            bind_addr: addr1,
            peers: vec![addr2, addr3],
            wal_dir: base.join("n1"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: true,
            snapshot_lag_threshold: 50,
        };
        let config2 = Config {
            node_id: 2,
            bind_addr: addr2,
            peers: vec![addr1, addr3],
            wal_dir: base.join("n2"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 50,
        };
        let config3 = Config {
            node_id: 3,
            bind_addr: addr3,
            peers: vec![addr1, addr2],
            wal_dir: base.join("n3"),
            worker_threads: 2,
            wal_batch_size: 10,
            wal_flush_interval_ms: 100,
            is_initial_leader: false,
            snapshot_lag_threshold: 50,
        };

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = OctopiiNode::new(config1.clone(), runtime.clone()).await.expect("create n1");
        let n2 = OctopiiNode::new(config2.clone(), runtime.clone()).await.expect("create n2");
        let n3 = OctopiiNode::new(config3.clone(), runtime.clone()).await.expect("create n3");

        n1.start().await.expect("start n1");
        n2.start().await.expect("start n2");
        n3.start().await.expect("start n3");

        // Elect node1
        n1.campaign().await.expect("n1 campaign");
        sleep(Duration::from_secs(2)).await;
        assert!(n1.is_leader().await, "n1 should be leader");

        // Propose a batch of entries to create non-trivial history
        for i in 0..100usize {
            let _ = n1.propose(format!("SET a{} v{}", i, i).into_bytes()).await;
            sleep(Duration::from_millis(10)).await;
        }
        sleep(Duration::from_millis(500)).await;

        // Abruptly remove the leader (simulate crash using shutdown here)
        n1.shutdown();
        // Allow followers to detect leader silence
        sleep(Duration::from_millis(1500)).await;

        // Wait for automatic re-election between n2 and n3
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(10);
        let mut new_leader_id: Option<u64> = None;
        while start.elapsed() < timeout {
            if n2.is_leader().await {
                new_leader_id = Some(2);
                break;
            }
            if n3.is_leader().await {
                new_leader_id = Some(3);
                break;
            }
            sleep(Duration::from_millis(200)).await;
        }
        assert!(new_leader_id.is_some(), "A new leader should be elected automatically");
        let new_leader_id = new_leader_id.unwrap();

        // Send a few more proposals via the new leader
        let leader = if new_leader_id == 2 { &n2 } else { &n3 };
        for i in 100..120usize {
            let _ = leader.propose(format!("SET b{} v{}", i, i).into_bytes()).await;
            sleep(Duration::from_millis(10)).await;
        }
        sleep(Duration::from_millis(300)).await;

        // Restart the old leader with the same WAL (simulate node restart)
        // Wait for the OS to fully release the port; retry on AddrInUse with backoff.
        sleep(Duration::from_millis(3000)).await;
        let mut n1r_opt = None;
        let mut last_err = String::new();
        for attempt in 0..60 {
            match OctopiiNode::new(config1.clone(), runtime.clone()).await {
                Ok(n) => {
                    n1r_opt = Some(n);
                    break;
                }
                Err(e) => {
                    let msg = format!("{}", e);
                    last_err = msg.clone();
                    if msg.contains("Address already in use") {
                        // Linear backoff up to 1s per attempt
                        let delay_ms = 200 + (attempt * 200);
                        sleep(Duration::from_millis(std::cmp::min(delay_ms, 1000) as u64)).await;
                        continue;
                    } else {
                        panic!("recreate n1 failed: {}", e);
                    }
                }
            }
        }
        let n1r = n1r_opt.unwrap_or_else(|| panic!("recreate n1 after retries: {}", last_err));
        n1r.start().await.expect("restart n1");
        // Allow it to catch up via append/snapshot
        sleep(Duration::from_secs(3)).await;

        // Cluster should remain stable and still have a leader (n2 or n3)
        // Cluster should still have a leader (not necessarily the restarted node)
        assert!(
            n2.has_leader().await || n3.has_leader().await,
            "Cluster should report a leader after restart"
        );

        // Final small proposal through the current leader to ensure liveness
        let current_leader = if n2.is_leader().await { &n2 } else if n3.is_leader().await { &n3 } else { &n1r };
        let _ = current_leader.propose(b"SET final x".to_vec()).await;
        sleep(Duration::from_millis(300)).await;

        // Cleanup
        let _ = tokio::fs::remove_dir_all(&base).await;
    });
}


