use bytes::Bytes;
use octopii::{Config, OctopiiNode};

#[test]
fn test_single_node_startup() {
    // Create a runtime for the test
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Clean up before test
    let temp_dir = std::env::temp_dir().join("octopii_integration_test_1");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        peers: vec![],
        wal_dir: temp_dir.clone(),
        worker_threads: 2,
        wal_batch_size: 10,
        wal_flush_interval_ms: 100,
        is_initial_leader: false,
    };

    // Create node OUTSIDE of async context to avoid nested runtime issue
    let node = OctopiiNode::new_blocking(config).unwrap();
    assert_eq!(node.id(), 1);

    rt.block_on(async {
        // Start the node
        node.start().await.unwrap();

        // Give it time to initialize
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Clean up
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    });
}

#[test]
fn test_node_state_machine_query() {
    // Create a runtime for the test
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Clean up before test
    let temp_dir = std::env::temp_dir().join("octopii_integration_test_2");
    let _ = std::fs::remove_dir_all(&temp_dir);

    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        peers: vec![],
        wal_dir: temp_dir.clone(),
        worker_threads: 2,
        wal_batch_size: 10,
        wal_flush_interval_ms: 100,
        is_initial_leader: false,
    };

    // Create node OUTSIDE of async context to avoid nested runtime issue
    let node = OctopiiNode::new_blocking(config).unwrap();

    rt.block_on(async {
        // Test state machine operations
        let result = node.query(b"SET foo bar").await.unwrap();
        assert_eq!(result, Bytes::from("OK"));

        let result = node.query(b"GET foo").await.unwrap();
        assert_eq!(result, Bytes::from("bar"));

        let result = node.query(b"DELETE foo").await.unwrap();
        assert_eq!(result, Bytes::from("OK"));

        let result = node.query(b"GET foo").await.unwrap();
        assert_eq!(result, Bytes::from("NOT_FOUND"));

        // Clean up
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    });
}

#[test]
fn test_multiple_nodes_communication() {
    // Create a runtime for the test
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Clean up before test
    let temp_dir = std::env::temp_dir().join("octopii_integration_test_3");
    let _ = std::fs::remove_dir_all(&temp_dir);

    // Create two nodes
    let config1 = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        peers: vec!["127.0.0.1:6001".parse().unwrap()],
        wal_dir: temp_dir.join("node1"),
        worker_threads: 2,
        wal_batch_size: 10,
        wal_flush_interval_ms: 100,
        is_initial_leader: false,
    };

    let config2 = Config {
        node_id: 2,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        peers: vec!["127.0.0.1:6000".parse().unwrap()],
        wal_dir: temp_dir.join("node2"),
        worker_threads: 2,
        wal_batch_size: 10,
        wal_flush_interval_ms: 100,
        is_initial_leader: false,
    };

    // Create nodes OUTSIDE of async context to avoid nested runtime issue
    let node1 = OctopiiNode::new_blocking(config1).unwrap();
    let node2 = OctopiiNode::new_blocking(config2).unwrap();

    rt.block_on(async {
        // Start both nodes
        node1.start().await.unwrap();
        node2.start().await.unwrap();

        // Give them time to initialize
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(node1.id(), 1);
        assert_eq!(node2.id(), 2);

        // Clean up
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    });
}
