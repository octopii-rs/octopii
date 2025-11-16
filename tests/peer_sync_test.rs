use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::time::sleep;

static NEXT_PORT: AtomicU16 = AtomicU16::new(9600);

fn next_port() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::SeqCst)
}

fn node_config(
    base: &PathBuf,
    node_id: u64,
    addr: SocketAddr,
    peers: Vec<SocketAddr>,
    leader: bool,
) -> Config {
    Config {
        node_id,
        bind_addr: addr,
        peers,
        wal_dir: base.join(format!("n{}", node_id)),
        worker_threads: 2,
        wal_batch_size: 16,
        wal_flush_interval_ms: 50,
        is_initial_leader: leader,
        snapshot_lag_threshold: 50,
    }
}

#[test]
fn test_peer_address_auto_distribution() {
    octopii::openraft::node::clear_global_peer_addrs();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = tempfile::tempdir().unwrap();
        let port1 = next_port();
        let port2 = next_port();
        let port3 = next_port();
        let addr1: SocketAddr = format!("127.0.0.1:{port1}").parse().unwrap();
        let addr2: SocketAddr = format!("127.0.0.1:{port2}").parse().unwrap();
        let addr3: SocketAddr = format!("127.0.0.1:{port3}").parse().unwrap();

        let base_path = base.path().to_path_buf();
        let config1 = node_config(&base_path, 1, addr1, vec![addr2, addr3], true);
        let config2 = node_config(&base_path, 2, addr2, vec![addr1], false);
        let config3 = node_config(&base_path, 3, addr3, vec![addr1], false);

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = OctopiiNode::new(config1, runtime.clone()).await.unwrap();
        let n2 = OctopiiNode::new(config2, runtime.clone()).await.unwrap();
        let n3 = OctopiiNode::new(config3, runtime.clone()).await.unwrap();

        n1.start().await.unwrap();
        n2.start().await.unwrap();
        n3.start().await.unwrap();

        n1.campaign().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        n1.add_learner(2, addr2).await.unwrap();
        n1.add_learner(3, addr3).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        let addr_known = n2.peer_addr_for(3).await;
        assert_eq!(addr_known, Some(addr3));
        let addr_known = n3.peer_addr_for(2).await;
        assert_eq!(addr_known, Some(addr2));

        n1.promote_learner(2).await.unwrap();
        n1.promote_learner(3).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        n1.shutdown();
        drop(n1);
        tokio::time::sleep(Duration::from_secs(1)).await;

        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(15);
        let mut new_leader = None;
        while start.elapsed() < timeout {
            if n2.is_leader().await {
                new_leader = Some(2);
                break;
            }
            if n3.is_leader().await {
                new_leader = Some(3);
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        if new_leader.is_none() {
            for node in [(&n2, 2u64), (&n3, 3u64)] {
                let _ = node.0.campaign().await;
                tokio::time::sleep(Duration::from_secs(2)).await;
                if node.0.is_leader().await {
                    new_leader = Some(node.1);
                    break;
                }
            }
        }

        assert!(
            new_leader.is_some(),
            "followers should elect a new leader without manual peer address updates"
        );

        n2.shutdown();
        n3.shutdown();
    });
}

async fn wait_for_peer_address(
    node: &OctopiiNode,
    peer_id: u64,
    addr: SocketAddr,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if node.peer_addr_for(peer_id).await == Some(addr) {
            return true;
        }
        sleep(Duration::from_millis(200)).await;
    }
    false
}

#[test]
fn test_peer_address_distribution_without_initial_peers() {
    octopii::openraft::node::clear_global_peer_addrs();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let base = tempfile::tempdir().unwrap();
        let port1 = next_port();
        let port2 = next_port();
        let port3 = next_port();
        let port4 = next_port();
        let addr1: SocketAddr = format!("127.0.0.1:{port1}").parse().unwrap();
        let addr2: SocketAddr = format!("127.0.0.1:{port2}").parse().unwrap();
        let addr3: SocketAddr = format!("127.0.0.1:{port3}").parse().unwrap();
        let addr4: SocketAddr = format!("127.0.0.1:{port4}").parse().unwrap();

        let base_path = base.path().to_path_buf();
        let config = |node_id, addr, peers, leader| Config {
            node_id,
            bind_addr: addr,
            peers,
            wal_dir: base_path.join(format!("n{}", node_id)),
            worker_threads: 2,
            wal_batch_size: 16,
            wal_flush_interval_ms: 50,
            is_initial_leader: leader,
            snapshot_lag_threshold: 50,
        };

        let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
        let n1 = OctopiiNode::new(config(1, addr1, vec![addr2, addr3], true), runtime.clone())
            .await
            .unwrap();
        let n2 = OctopiiNode::new(config(2, addr2, vec![addr1], false), runtime.clone())
            .await
            .unwrap();
        let n3 = OctopiiNode::new(config(3, addr3, vec![addr1], false), runtime.clone())
            .await
            .unwrap();

        n1.start().await.unwrap();
        n2.start().await.unwrap();
        n3.start().await.unwrap();

        n1.campaign().await.unwrap();
        sleep(Duration::from_secs(1)).await;

        n1.add_learner(2, addr2).await.unwrap();
        n1.add_learner(3, addr3).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        n1.promote_learner(2).await.unwrap();
        n1.promote_learner(3).await.unwrap();
        sleep(Duration::from_secs(1)).await;

        // Start node 4 with no peers configured; it should still learn everyone via peer sync.
        let config4 = config(4, addr4, vec![], false);
        let n4 = OctopiiNode::new(config4, runtime.clone()).await.unwrap();
        n4.start().await.unwrap();

        n1.add_learner(4, addr4).await.unwrap();
        sleep(Duration::from_secs(1)).await;

        assert!(
            wait_for_peer_address(&n4, 1, addr1, Duration::from_secs(20)).await,
            "n4 should learn leader address automatically"
        );
        assert!(
            wait_for_peer_address(&n4, 2, addr2, Duration::from_secs(20)).await,
            "n4 should learn peer 2 address automatically"
        );
        assert!(
            wait_for_peer_address(&n2, 4, addr4, Duration::from_secs(20)).await,
            "existing voters should learn new learner address automatically"
        );

        n4.shutdown();
        n3.shutdown();
        n2.shutdown();
        n1.shutdown();
    });
}
