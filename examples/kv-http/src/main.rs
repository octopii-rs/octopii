mod api;
mod sharded;
mod types;

use api::AppState;
use axum::{
    routing::{delete, get, put},
    Router,
};
use octopii::{Config, OctopiiNode, OctopiiRuntime};
use sharded::ShardedStore;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

fn parse_peers(s: &str) -> Vec<SocketAddr> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',')
        .filter_map(|p| p.trim().parse().ok())
        .collect()
}

fn config_from_env() -> Config {
    let node_id: u64 = env::var("NODE_ID")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .expect("NODE_ID must be a number");

    let bind_addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:5001".into())
        .parse()
        .expect("BIND_ADDR must be a valid socket address");

    let peers = parse_peers(&env::var("PEERS").unwrap_or_default());

    let wal_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "./data".into()));

    let is_initial_leader = env::var("INITIAL_LEADER")
        .unwrap_or_else(|_| "false".into())
        .to_lowercase()
        == "true";

    Config {
        node_id,
        bind_addr,
        peers,
        wal_dir,
        is_initial_leader,
        worker_threads: 4,
        ..Default::default()
    }
}

fn http_addr_from_env() -> SocketAddr {
    env::var("HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8001".into())
        .parse()
        .expect("HTTP_ADDR must be a valid socket address")
}

fn cluster_nodes_from_env() -> Vec<u64> {
    env::var("CLUSTER_NODES")
        .unwrap_or_else(|_| "1".into())
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect()
}

fn cluster_http_from_env() -> HashMap<u64, String> {
    let mut map = HashMap::new();
    let s = env::var("CLUSTER_HTTP").unwrap_or_default();
    for part in s.split(',') {
        if let Some((id, addr)) = part.split_once('=') {
            if let Ok(id) = id.trim().parse::<u64>() {
                map.insert(id, addr.trim().to_string());
            }
        }
    }
    map
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let config = config_from_env();
    let http_addr = http_addr_from_env();
    let cluster_nodes = cluster_nodes_from_env();
    let cluster_http = cluster_http_from_env();

    tracing::info!(
        "Starting node {} at {} (HTTP: {})",
        config.node_id,
        config.bind_addr,
        http_addr
    );
    tracing::info!("Peers: {:?}", config.peers);
    tracing::info!("Cluster nodes: {:?}", cluster_nodes);
    tracing::info!("Cluster HTTP: {:?}", cluster_http);
    tracing::info!("Initial leader: {}", config.is_initial_leader);

    std::fs::create_dir_all(&config.wal_dir)?;

    let runtime = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());
    let peers = config.peers.clone();
    let node = Arc::new(OctopiiNode::new(config.clone(), runtime).await?);

    let sharded = Arc::new(ShardedStore::new(config.node_id, cluster_nodes, cluster_http));

    node.start().await?;

    if config.is_initial_leader {
        tracing::info!("Triggering leader election...");
        node.campaign().await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Add and promote peer nodes
        for peer_addr in &peers {
            let peer_id = (peer_addr.port() % 10) as u64;
            tracing::info!("Adding learner {} at {}", peer_id, peer_addr);

            for attempt in 1..=10 {
                match node.add_learner(peer_id, *peer_addr).await {
                    Ok(_) => {
                        tracing::info!("Added learner {}", peer_id);
                        break;
                    }
                    Err(e) => {
                        if attempt == 10 {
                            tracing::warn!("Failed to add learner {} after 10 attempts: {}", peer_id, e);
                        } else {
                            tracing::debug!("Attempt {} to add learner {} failed: {}", attempt, peer_id, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        for peer_addr in &peers {
            let peer_id = (peer_addr.port() % 10) as u64;
            tracing::info!("Promoting learner {} to voter", peer_id);
            if let Err(e) = node.promote_learner(peer_id).await {
                tracing::warn!("Failed to promote learner {}: {}", peer_id, e);
            }
        }
    }

    let state = Arc::new(AppState {
        node: node.clone(),
        sharded,
    });

    let app = Router::new()
        // Replicated KV (Raft consensus)
        .route("/kv/:key", get(api::get_key))
        .route("/kv/:key", put(api::put_key))
        .route("/kv/:key", delete(api::delete_key))
        // Sharded KV (no consensus, horizontal scaling)
        .route("/sharded/:key", get(api::sharded_get))
        .route("/sharded/:key", put(api::sharded_put))
        .route("/sharded/:key", delete(api::sharded_delete))
        // Internal endpoints for node-to-node forwarding
        .route("/sharded/internal/:key", get(api::sharded_internal_get))
        .route("/sharded/internal/:key", put(api::sharded_internal_put))
        .route("/sharded/internal/:key", delete(api::sharded_internal_delete))
        .route("/health", get(api::health))
        .with_state(state);

    tracing::info!("HTTP server listening on {}", http_addr);

    let listener = tokio::net::TcpListener::bind(http_addr).await?;
    axum::serve(listener, app).await?;

    node.shutdown().await;
    Ok(())
}
