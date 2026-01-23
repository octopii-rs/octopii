#![cfg(feature = "openraft")]

use octopii::{Config, OctopiiNode, OctopiiRuntime};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::{sleep, Instant};

static NEXT_PORT_GROUP: AtomicU16 = AtomicU16::new(4000);

fn next_addr_with_suffix(suffix: u16) -> SocketAddr {
    loop {
        // Allocate ports in predictable groups of 10 to guarantee the desired suffix.
        let group = NEXT_PORT_GROUP.fetch_add(1, Ordering::SeqCst);
        let port = group.saturating_mul(10).saturating_add(suffix);
        if port < 1024 || port > 65000 {
            NEXT_PORT_GROUP.store(4000, Ordering::SeqCst);
            continue;
        }
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            let addr = listener.local_addr().expect("local addr");
            drop(listener);
            return addr;
        }
    }
}

fn node_config(
    node_id: u64,
    bind: SocketAddr,
    peers: Vec<SocketAddr>,
    wal_dir: PathBuf,
    is_initial_leader: bool,
) -> Config {
    Config {
        node_id,
        bind_addr: bind,
        peers,
        wal_dir,
        worker_threads: 2,
        wal_batch_size: 32,
        wal_flush_interval_ms: 0,
        is_initial_leader,
        snapshot_lag_threshold: 64,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn openraft_three_node_cluster_replicates_commands() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = tempdir()?;
    let base = temp.path();

    let addr1 = next_addr_with_suffix(1);
    let addr2 = next_addr_with_suffix(2);
    let addr3 = next_addr_with_suffix(3);
    eprintln!("using addrs: n1={} n2={} n3={}", addr1, addr2, addr3);

    let peers1 = vec![addr2, addr3];
    let peers2 = vec![addr1, addr3];
    let peers3 = vec![addr1, addr2];

    let shared_runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

    let node1 = OctopiiNode::new(
        node_config(
            1,
            addr1,
            peers1,
            base.join("node1"),
            true, /* initial leader */
        ),
        shared_runtime.clone(),
    )
    .await?;
    let node2 = OctopiiNode::new(
        node_config(2, addr2, peers2, base.join("node2"), false),
        shared_runtime.clone(),
    )
    .await?;
    let node3 = OctopiiNode::new(
        node_config(3, addr3, peers3, base.join("node3"), false),
        shared_runtime,
    )
    .await?;

    node1.start().await?;
    node2.start().await?;
    node3.start().await?;

    node1.campaign().await?;

    let leader_wait_start = Instant::now();
    let leader_timeout = Duration::from_secs(30);
    let leader_id = loop {
        if node1.is_leader().await {
            break 1;
        }
        if node2.is_leader().await {
            break 2;
        }
        if node3.is_leader().await {
            break 3;
        }
        if leader_wait_start.elapsed() > leader_timeout {
            eprintln!(
                "metrics: n1={:?} n2={:?} n3={:?}",
                node1.raft_metrics(),
                node2.raft_metrics(),
                node3.raft_metrics()
            );
            panic!(
                "leader election did not complete within {:?}",
                leader_timeout
            );
        }
        sleep(Duration::from_millis(200)).await;
    };

    let leader = match leader_id {
        1 => &node1,
        2 => &node2,
        3 => &node3,
        _ => unreachable!(),
    };

    leader.propose(b"SET cluster_key value".to_vec()).await?;
    sleep(Duration::from_secs(2)).await;

    let expected = "value".to_string();
    for (node, id) in [(&node1, 1_u64), (&node2, 2_u64), (&node3, 3_u64)] {
        let mut observed = String::new();
        let mut attempts = 0;
        while attempts < 50 {
            let response = node.query(b"GET cluster_key").await?;
            observed = String::from_utf8(response.to_vec())?;
            if observed == expected {
                break;
            }
            sleep(Duration::from_millis(200)).await;
            attempts += 1;
        }
        assert_eq!(
            observed, expected,
            "node {} should observe replicated value",
            id
        );
    }

    node1.shutdown();
    node2.shutdown();
    node3.shutdown();

    Ok(())
}
