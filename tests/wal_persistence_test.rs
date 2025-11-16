use bytes::Bytes;
use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

static NEXT_PORT_BLOCK: AtomicU16 = AtomicU16::new(9600);

fn next_port_block() -> u16 {
    NEXT_PORT_BLOCK.fetch_add(10, Ordering::SeqCst)
}

fn addr_for(block: u16, node_id: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], block + node_id))
}

fn single_node_config(base: &TempDir, port: u16) -> Config {
    Config {
        node_id: 1,
        bind_addr: SocketAddr::from(([127, 0, 0, 1], port)),
        peers: Vec::new(),
        wal_dir: base.path().to_path_buf(),
        worker_threads: 2,
        wal_batch_size: 16,
        wal_flush_interval_ms: 50,
        is_initial_leader: true,
        snapshot_lag_threshold: 50,
    }
}

fn cluster_config(
    base: &PathBuf,
    node_id: u64,
    addr: SocketAddr,
    peers: Vec<SocketAddr>,
    is_initial_leader: bool,
) -> Config {
    Config {
        node_id,
        bind_addr: addr,
        peers,
        wal_dir: base.join(format!("n{}", node_id)),
        worker_threads: 2,
        wal_batch_size: 32,
        wal_flush_interval_ms: 50,
        is_initial_leader,
        snapshot_lag_threshold: 50,
    }
}

#[test]
fn test_single_node_wal_persists_across_restart() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let wal_dir = tempfile::tempdir().unwrap();
        // Use port 0 so OS assigns a free port on each start (no fixed port requirement).
        let mut config = single_node_config(&wal_dir, 0);
        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

        let node = OctopiiNode::new(config.clone(), runtime.clone())
            .await
            .expect("create node");
        node.start().await.expect("start node");
        node.campaign().await.expect("campaign");
        tokio::time::sleep(Duration::from_millis(500)).await;

        node.propose(b"SET alpha one".to_vec()).await.unwrap();
        node.propose(b"SET beta two".to_vec()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let val = node.query(b"GET beta").await.unwrap();
        assert_eq!(val, Bytes::from("two"));

        node.shutdown();
        drop(node);
        tokio::time::sleep(Duration::from_millis(500)).await;

        config.is_initial_leader = false;
        let restarted = OctopiiNode::new(config, runtime)
            .await
            .expect("restart node");
        restarted.start().await.expect("start restarted node");
        restarted.campaign().await.expect("restarted node election");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let val = restarted.query(b"GET alpha").await.unwrap();
        assert_eq!(val, Bytes::from("one"));
        let val = restarted.query(b"GET beta").await.unwrap();
        assert_eq!(val, Bytes::from("two"));

        let metrics = restarted.raft_metrics();
        assert!(metrics.last_log_index.unwrap() >= 2);

        restarted.shutdown();
    });
}

async fn wait_for_value(node: &OctopiiNode, key: &[u8], expected: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(value) = node.query(key).await {
            if value == Bytes::from(expected.to_string()) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

#[test]
fn test_three_node_leader_restart_preserves_wal() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = PathBuf::from(std::env::temp_dir()).join("octopii_wal_cluster");
        let _ = std::fs::remove_dir_all(&base);

        let port_block = next_port_block();
        let addr1 = addr_for(port_block, 1);
        let addr2 = addr_for(port_block, 2);
        let addr3 = addr_for(port_block, 3);

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = Arc::new(
            OctopiiNode::new(
                cluster_config(&base, 1, addr1, vec![addr2, addr3], true),
                runtime.clone(),
            )
            .await
            .expect("create n1"),
        );
        let n2 = Arc::new(
            OctopiiNode::new(
                cluster_config(&base, 2, addr2, vec![addr1, addr3], false),
                runtime.clone(),
            )
            .await
            .expect("create n2"),
        );
        let n3 = Arc::new(
            OctopiiNode::new(
                cluster_config(&base, 3, addr3, vec![addr1, addr2], false),
                runtime.clone(),
            )
            .await
            .expect("create n3"),
        );

        for node in [&n1, &n2, &n3] {
            node.start().await.expect("start node");
        }

        n1.campaign().await.expect("n1 campaign");
        tokio::time::sleep(Duration::from_secs(2)).await;

        n1.add_learner(2, addr2).await.expect("add learner 2");
        n1.add_learner(3, addr3).await.expect("add learner 3");
        tokio::time::sleep(Duration::from_secs(2)).await;
        n1.promote_learner(2).await.expect("promote 2");
        n1.promote_learner(3).await.expect("promote 3");
        tokio::time::sleep(Duration::from_secs(2)).await;

        for i in 0..40 {
            let _ = n1
                .propose(format!("SET persist{} {}", i, i).into_bytes())
                .await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        n1.shutdown();
        // Drop the original leader to free its transport socket before restart.
        drop(n1);
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut new_leader_id = None;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            if n2.is_leader().await {
                new_leader_id = Some(2);
                break;
            }
            if n3.is_leader().await {
                new_leader_id = Some(3);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if new_leader_id.is_none() {
            n2.campaign().await.ok();
            tokio::time::sleep(Duration::from_secs(2)).await;
            if n2.is_leader().await {
                new_leader_id = Some(2);
            }
        }
        if new_leader_id.is_none() {
            n3.campaign().await.ok();
            tokio::time::sleep(Duration::from_secs(2)).await;
            if n3.is_leader().await {
                new_leader_id = Some(3);
            }
        }
        let new_leader = new_leader_id.expect("cluster failed to elect new leader");
        let leader = if new_leader == 2 {
            Arc::clone(&n2)
        } else {
            Arc::clone(&n3)
        };
        for i in 100..110 {
            let _ = leader
                .propose(format!("SET post{} {}", i, i).into_bytes())
                .await;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;

        let restart_block = next_port_block();
        let addr1_restart = addr_for(restart_block, 1);
        let mut restart_config = cluster_config(&base, 1, addr1_restart, vec![addr2, addr3], false);
        restart_config.is_initial_leader = false;
        tokio::time::sleep(Duration::from_secs(1)).await;

        let n1_restart = Arc::new(
            OctopiiNode::new(restart_config, runtime.clone())
                .await
                .expect("restart n1"),
        );
        n2.update_peer_addr(1, addr1_restart).await;
        n3.update_peer_addr(1, addr1_restart).await;
        n1_restart.start().await.expect("start restart");
        tokio::time::sleep(Duration::from_secs(2)).await;

        let start_wait = Instant::now();
        while start_wait.elapsed() < Duration::from_secs(10) {
            if n1_restart.has_leader().await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        assert!(
            n1_restart.has_leader().await,
            "restarted node should observe a leader"
        );

        assert!(
            wait_for_value(&n1_restart, b"GET persist0", "0", Duration::from_secs(10)).await,
            "restarted node should retain old entries"
        );
        assert!(
            wait_for_value(&n1_restart, b"GET post100", "100", Duration::from_secs(10)).await,
            "restarted node should learn new entries committed while it was down"
        );

        n1_restart.shutdown();
        n2.shutdown();
        n3.shutdown();
        let _ = tokio::fs::remove_dir_all(&base).await;
    });
}
