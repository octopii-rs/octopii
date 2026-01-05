use bytes::Bytes;
use octopii::rpc::{
    deserialize, RequestPayload, ResponsePayload, RpcHandler, RpcMessage, RpcRequest,
};
use octopii::transport::{QuicTransport, Transport};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc_request_response() {
    // Use ephemeral ports to avoid conflicts when tests run in parallel.
    let transport1 = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let transport2 = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let addr1 = transport1.local_addr().unwrap();
    let addr2 = transport2.local_addr().unwrap();

    let rpc1 = Arc::new(RpcHandler::new(
        Arc::clone(&transport1) as Arc<dyn Transport>
    ));
    let rpc2 = Arc::new(RpcHandler::new(
        Arc::clone(&transport2) as Arc<dyn Transport>
    ));

    // Set up handler on node 2
    rpc2.set_request_handler(|_req: RpcRequest| async {
        ResponsePayload::CustomResponse {
            success: true,
            data: Bytes::from("response_data"),
        }
    })
    .await;

    // Spawn acceptor for transport2 (server) - proper pattern from node.rs
    let t2_clone = Arc::clone(&transport2);
    let rpc2_clone = Arc::clone(&rpc2);
    tokio::spawn(async move {
        loop {
            match t2_clone.accept().await {
                Ok((addr, peer)) => {
                    let rpc = Arc::clone(&rpc2_clone);
                    let peer = peer as Arc<dyn octopii::transport::Peer>;
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                            }
                        }
                    });
                }
                Err(_) => break, // Endpoint closed
            }
        }
    });

    // Spawn acceptor for transport1 (client) - not needed for this test but good practice
    let t1_clone = Arc::clone(&transport1);
    let rpc1_clone = Arc::clone(&rpc1);
    tokio::spawn(async move {
        loop {
            match t1_clone.accept().await {
                Ok((addr, peer)) => {
                    let rpc = Arc::clone(&rpc1_clone);
                    let peer = peer as Arc<dyn octopii::transport::Peer>;
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                            }
                        }
                    });
                }
                Err(_) => break, // Endpoint closed
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Client needs to establish connection and spawn recv loop on it to receive responses
    let client_peer = transport1.connect(addr2).await.unwrap() as Arc<dyn octopii::transport::Peer>;
    let rpc1_for_recv = Arc::clone(&rpc1);
    let client_peer_for_recv = Arc::clone(&client_peer);
    tokio::spawn(async move {
        while let Ok(Some(data)) = client_peer_for_recv.recv().await {
            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                rpc1_for_recv
                    .notify_message(addr2, msg, Some(Arc::clone(&client_peer_for_recv)))
                    .await;
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Send request from node 1
    let response = rpc1
        .request(
            addr2,
            RequestPayload::Custom {
                operation: "test".to_string(),
                data: Bytes::from("request_data"),
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap();

    match response.payload {
        ResponsePayload::CustomResponse { success, data } => {
            assert!(success);
            assert_eq!(data, Bytes::from("response_data"));
        }
        _ => panic!("Unexpected response type"),
    }

    transport1.close();
    transport2.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc_one_way_message() {
    let transport1 = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let transport2 = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let addr1 = transport1.local_addr().unwrap();
    let addr2 = transport2.local_addr().unwrap();

    let rpc1 = Arc::new(RpcHandler::new(
        Arc::clone(&transport1) as Arc<dyn Transport>
    ));
    let rpc2 = Arc::new(RpcHandler::new(
        Arc::clone(&transport2) as Arc<dyn Transport>
    ));

    // Spawn acceptor for transport2 to receive the one-way message
    let t2_clone = Arc::clone(&transport2);
    let rpc2_clone = Arc::clone(&rpc2);
    tokio::spawn(async move {
        loop {
            match t2_clone.accept().await {
                Ok((addr, peer)) => {
                    let rpc = Arc::clone(&rpc2_clone);
                    let peer = peer as Arc<dyn octopii::transport::Peer>;
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                            }
                        }
                    });
                }
                Err(_) => break, // Endpoint closed
            }
        }
    });

    // Allow server acceptor to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Send one-way message
    let result = rpc1
        .send_one_way(
            addr2,
            octopii::rpc::OneWayMessage::Heartbeat {
                node_id: 1,
                timestamp: 123456,
            },
        )
        .await;

    assert!(result.is_ok());

    // Give time for message to be received
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    transport1.close();
    transport2.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc_request_timeout_and_recovery() {
    let transport1 = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let transport2 = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let addr1 = transport1.local_addr().unwrap();
    let addr2 = transport2.local_addr().unwrap();

    let rpc1 = Arc::new(RpcHandler::new(
        Arc::clone(&transport1) as Arc<dyn Transport>
    ));
    let rpc2 = Arc::new(RpcHandler::new(
        Arc::clone(&transport2) as Arc<dyn Transport>
    ));

    let drop_first = Arc::new(AtomicBool::new(true));

    // Server acceptor: first inbound request is dropped on purpose, subsequent ones are handled normally.
    let t2_clone = Arc::clone(&transport2);
    let rpc2_clone = Arc::clone(&rpc2);
    let drop_clone = Arc::clone(&drop_first);
    tokio::spawn(async move {
        loop {
            match t2_clone.accept().await {
                Ok((addr, peer_raw)) => {
                    let rpc = Arc::clone(&rpc2_clone);
                    let peer = peer_raw as Arc<dyn octopii::transport::Peer>;
                    let drop_flag = Arc::clone(&drop_clone);
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                if drop_flag.swap(false, Ordering::SeqCst) {
                                    // Simulate a peer that accepts the message but never responds.
                                    tokio::time::sleep(Duration::from_millis(500)).await;
                                    continue;
                                } else {
                                    rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                                }
                            }
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    // Client acceptor (not strictly required but mirrors production usage)
    let t1_clone = Arc::clone(&transport1);
    let rpc1_clone = Arc::clone(&rpc1);
    tokio::spawn(async move {
        loop {
            match t1_clone.accept().await {
                Ok((addr, peer_raw)) => {
                    let rpc = Arc::clone(&rpc1_clone);
                    let peer = peer_raw as Arc<dyn octopii::transport::Peer>;
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                            }
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // First request should time out because server intentionally dropped it.
    let err = rpc1
        .request(
            addr2,
            RequestPayload::Custom {
                operation: "slow".into(),
                data: Bytes::from_static(b"payload"),
            },
            Duration::from_millis(300),
        )
        .await
        .expect_err("first request should time out");
    assert!(
        err.to_string().contains("timeout") || err.to_string().contains("closed"),
        "unexpected error message: {err}"
    );

    // Now install a real handler and ensure subsequent requests succeed.
    rpc2.set_request_handler(|_req| async {
        ResponsePayload::CustomResponse {
            success: true,
            data: Bytes::from_static(b"ok"),
        }
    })
    .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Establish a client-side receive loop for responses
    let client_peer = transport1.connect(addr2).await.unwrap() as Arc<dyn octopii::transport::Peer>;
    let rpc1_for_recv = Arc::clone(&rpc1);
    let client_peer_for_recv = Arc::clone(&client_peer);
    tokio::spawn(async move {
        while let Ok(Some(data)) = client_peer_for_recv.recv().await {
            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                rpc1_for_recv
                    .notify_message(addr2, msg, Some(Arc::clone(&client_peer_for_recv)))
                    .await;
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let response = rpc1
        .request(
            addr2,
            RequestPayload::Custom {
                operation: "fast".into(),
                data: Bytes::from("world"),
            },
            Duration::from_secs(6),
        )
        .await
        .expect("second request should succeed");

    match response.payload {
        ResponsePayload::CustomResponse { success, data } => {
            assert!(success);
            assert_eq!(data, Bytes::from_static(b"ok"));
        }
        other => panic!("unexpected response: {:?}", other),
    }

    transport1.close();
    transport2.close();
}
