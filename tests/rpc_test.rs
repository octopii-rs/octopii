use bytes::Bytes;
use octopii::rpc::{deserialize, RpcHandler, RpcMessage, RpcRequest, RequestPayload, ResponsePayload};
use octopii::runtime::OctopiiRuntime;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[test]
fn test_rpc_request_response() {
    // Create test runtime
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create Octopii runtimes OUTSIDE async context
    // Leak them to avoid drop-in-async-context error (test only)
    let runtime1 = Box::leak(Box::new(OctopiiRuntime::new(2)));
    let runtime2 = Box::leak(Box::new(OctopiiRuntime::new(2)));

    rt.block_on(async {
        // Use specific ports so nodes can connect back to each other
        let addr1: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:7002".parse().unwrap();

        let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
        let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

        let rpc1 = Arc::new(RpcHandler::new(Arc::clone(&transport1), &runtime1));
        let rpc2 = Arc::new(RpcHandler::new(Arc::clone(&transport2), &runtime2));

    // Set up handler on node 2
    rpc2.set_request_handler(|_req: RpcRequest| {
        println!("Request handler called!");
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
                    println!("Server accepted connection from {}", addr);
                    let rpc = Arc::clone(&rpc2_clone);
                    let peer = Arc::new(peer);
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            println!("Server received {} bytes", data.len());
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                println!("Server deserialized message");
                                if let Err(e) = rpc.notify_message(addr, msg) {
                                    println!("Server notify_message error: {}", e);
                                }
                            }
                        }
                    });
                }
                Err(e) => println!("Server accept error: {}", e),
            }
        }
    });

    // Spawn acceptor for transport1 (client) to receive responses
    let t1_clone = Arc::clone(&transport1);
    let rpc1_clone = Arc::clone(&rpc1);
    tokio::spawn(async move {
        loop {
            match t1_clone.accept().await {
                Ok((addr, peer)) => {
                    println!("Client accepted connection from {}", addr);
                    let rpc = Arc::clone(&rpc1_clone);
                    let peer = Arc::new(peer);
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            println!("Client received {} bytes", data.len());
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                println!("Client deserialized message");
                                if let Err(e) = rpc.notify_message(addr, msg) {
                                    println!("Client notify_message error: {}", e);
                                }
                            }
                        }
                    });
                }
                Err(e) => println!("Client accept error: {}", e),
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Send request from node 1
    println!("Client sending request to {}", addr2);
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
    println!("Client received response");

        match response.payload {
            ResponsePayload::CustomResponse { success, data } => {
                assert!(success);
                assert_eq!(data, Bytes::from("response_data"));
            }
            _ => panic!("Unexpected response type"),
        }

        transport1.close();
        transport2.close();
    });
}

#[test]
fn test_rpc_one_way_message() {
    // Create test runtime
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Create Octopii runtimes OUTSIDE async context
    // Leak them to avoid drop-in-async-context error (test only)
    let runtime1 = Box::leak(Box::new(OctopiiRuntime::new(2)));
    let runtime2 = Box::leak(Box::new(OctopiiRuntime::new(2)));

    rt.block_on(async {
        // Use specific ports so nodes can connect back to each other
        let addr1: SocketAddr = "127.0.0.1:7003".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:7004".parse().unwrap();

        let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
        let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

        let rpc1 = Arc::new(RpcHandler::new(Arc::clone(&transport1), &runtime1));
        let rpc2 = Arc::new(RpcHandler::new(Arc::clone(&transport2), &runtime2));

    // Spawn acceptor for transport2 to receive the one-way message
    let t2_clone = Arc::clone(&transport2);
    let rpc2_clone = Arc::clone(&rpc2);
    tokio::spawn(async move {
        loop {
            if let Ok((addr, peer)) = t2_clone.accept().await {
                let rpc = Arc::clone(&rpc2_clone);
                let peer = Arc::new(peer);
                tokio::spawn(async move {
                    while let Ok(Some(data)) = peer.recv().await {
                        if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                            let _ = rpc.notify_message(addr, msg);
                        }
                    }
                });
            }
        }
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

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
    });
}
