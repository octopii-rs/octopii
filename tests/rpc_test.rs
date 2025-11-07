use bytes::Bytes;
use octopii::rpc::{deserialize, RpcHandler, RpcMessage, RpcRequest, RequestPayload, ResponsePayload};
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test]
async fn test_rpc_request_response() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    let rpc1 = Arc::new(RpcHandler::new(Arc::clone(&transport1)));
    let rpc2 = Arc::new(RpcHandler::new(Arc::clone(&transport2)));

    // Set up handler on node 2
    rpc2.set_request_handler(|_req: RpcRequest| ResponsePayload::CustomResponse {
        success: true,
        data: Bytes::from("response_data"),
    })
    .await;

    // Spawn acceptor for transport2 (server) - sends response back on same connection
    let t2_clone = Arc::clone(&transport2);
    tokio::spawn(async move {
        loop {
            if let Ok((_addr, peer)) = t2_clone.accept().await {
                let peer = Arc::new(peer);
                tokio::spawn(async move {
                    while let Ok(Some(data)) = peer.recv().await {
                        if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                            match msg {
                                RpcMessage::Request(req) => {
                                    // Handle request and send response back on same connection
                                    use octopii::rpc::serialize;
                                    let response_payload = ResponsePayload::CustomResponse {
                                        success: true,
                                        data: Bytes::from("response_data"),
                                    };
                                    let response = RpcMessage::new_response(req.id, response_payload);
                                    if let Ok(data) = serialize(&response) {
                                        let _ = peer.send(data).await;
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                });
            }
        }
    });

    // Spawn acceptor for transport1 (client) to receive responses
    let t1_clone = Arc::clone(&transport1);
    let rpc1_clone = Arc::clone(&rpc1);
    tokio::spawn(async move {
        loop {
            if let Ok((addr, peer)) = t1_clone.accept().await {
                let rpc = Arc::clone(&rpc1_clone);
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

    // Send request from node 1
    let response = rpc1
        .request(
            actual_addr2,
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

#[tokio::test]
async fn test_rpc_one_way_message() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    let rpc1 = RpcHandler::new(Arc::clone(&transport1));

    // Spawn acceptor
    let t2_clone = Arc::clone(&transport2);
    tokio::spawn(async move {
        let _ = t2_clone.accept().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Send one-way message
    let result = rpc1
        .send_one_way(
            actual_addr2,
            octopii::rpc::OneWayMessage::Heartbeat {
                node_id: 1,
                timestamp: 123456,
            },
        )
        .await;

    assert!(result.is_ok());

    transport1.close();
    transport2.close();
}
