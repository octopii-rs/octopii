use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const LEADER_WAIT: Duration = Duration::from_secs(20);
const FOLLOWER_WAIT: Duration = Duration::from_secs(20);

async fn wait_for_leader(node: &OctopiiNode, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if node.is_leader().await {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

async fn wait_for_peer_leader(nodes: &[Arc<OctopiiNode>], timeout: Duration) -> Option<usize> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        for (idx, node) in nodes.iter().enumerate() {
            if node.is_leader().await {
                return Some(idx);
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    None
}

use std::sync::Arc;

#[test]
fn test_read_index_enforces_leadership() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = PathBuf::from(std::env::temp_dir()).join("octopii_read_index");
        let _ = std::fs::remove_dir_all(&base);

        let addr1 = "127.0.0.1:9461".parse().unwrap();
        let addr2 = "127.0.0.1:9462".parse().unwrap();
        let addr3 = "127.0.0.1:9463".parse().unwrap();

        let config = |node_id, addr, peers, initial| Config {
            node_id,
            bind_addr: addr,
            peers,
            wal_dir: base.join(format!("n{}", node_id)),
            worker_threads: 2,
            wal_batch_size: 16,
            wal_flush_interval_ms: 50,
            is_initial_leader: initial,
            snapshot_lag_threshold: 50,
        };

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = Arc::new(
            OctopiiNode::new(config(1, addr1, vec![addr2, addr3], true), runtime.clone())
                .await
                .expect("create n1"),
        );
        let n2 = Arc::new(
            OctopiiNode::new(config(2, addr2, vec![addr1, addr3], false), runtime.clone())
                .await
                .expect("create n2"),
        );
        let n3 = Arc::new(
            OctopiiNode::new(config(3, addr3, vec![addr1, addr2], false), runtime.clone())
                .await
                .expect("create n3"),
        );

        for node in [&n1, &n2, &n3] {
            node.start().await.expect("start node");
        }

        n1.campaign().await.expect("n1 campaign");
        assert!(
            wait_for_leader(&n1, LEADER_WAIT).await,
            "n1 never became leader"
        );

        for i in 0..10 {
            let _ = n1.propose(format!("SET li{} {}", i, i).into_bytes()).await;
        }

        let nodes = vec![Arc::clone(&n1), Arc::clone(&n2), Arc::clone(&n3)];
        let leader_idx = wait_for_peer_leader(&nodes, FOLLOWER_WAIT)
            .await
            .expect("no leader elected");
        let leader = &nodes[leader_idx];
        let follower = nodes
            .iter()
            .enumerate()
            .find_map(|(idx, node)| if idx != leader_idx { Some(node) } else { None })
            .expect("missing follower reference");

        leader.read_index(b"leader-read".to_vec()).await.unwrap();
        let err = follower
            .read_index(b"follower-read".to_vec())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not leader"),
            "expected not leader error, got {err}"
        );

        for node in nodes {
            node.shutdown();
        }
        let _ = tokio::fs::remove_dir_all(&base).await;
    });
}
