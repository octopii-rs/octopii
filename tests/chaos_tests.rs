use bytes::Bytes;
use octopii::rpc::{
    deserialize, RequestPayload, ResponsePayload, RpcHandler, RpcMessage, RpcRequest,
};
use octopii::transport::QuicTransport;
use octopii::ShippingLane;
use rand::Rng;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

const LONG_TIMEOUT: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "chaos test - slow and flaky"]
async fn test_rpc_storm_with_handler_flaps() {
    let transport1 = Arc::new(QuicTransport::new("127.0.0.1:0".parse().unwrap()).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new("127.0.0.1:0".parse().unwrap()).await.unwrap());
    let addr1 = transport1.local_addr().unwrap();
    let addr2 = transport2.local_addr().unwrap();

    let rpc1 = Arc::new(RpcHandler::new(Arc::clone(&transport1)));
    let rpc2 = Arc::new(RpcHandler::new(Arc::clone(&transport2)));

    let drop_rate = Arc::new(AtomicBool::new(true));
    rpc2
        .set_request_handler({
            let drop_rate = Arc::clone(&drop_rate);
            move |req: RpcRequest| {
                if drop_rate.load(Ordering::SeqCst) {
                    ResponsePayload::CustomResponse {
                        success: false,
                        data: Bytes::from_static(b"dropped"),
                    }
                } else {
                    let payload = match &req.payload {
                        RequestPayload::Custom { operation, .. } => {
                            Bytes::from(format!("ack-{operation}"))
                        }
                        _ => Bytes::from_static(b"ack"),
                    };
                    ResponsePayload::CustomResponse {
                        success: true,
                        data: payload,
                    }
                }
            }
        })
        .await;

    let t2_clone = Arc::clone(&transport2);
    let rpc2_clone = Arc::clone(&rpc2);
    tokio::spawn(async move {
        loop {
            match t2_clone.accept().await {
                Ok((addr, peer)) => {
                    let rpc = Arc::clone(&rpc2_clone);
                    let peer = Arc::new(peer);
                    tokio::spawn(async move {
                        while let Ok(Some(data)) = peer.recv().await {
                            if let Ok(msg) = deserialize::<RpcMessage>(&data) {
                                if rand::random::<u8>() % 5 == 0 {
                                    continue;
                                }
                                rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                            }
                        }
                    });
                }
                Err(_) => break,
            }
        }
    });

    let t1_clone = Arc::clone(&transport1);
    let rpc1_clone = Arc::clone(&rpc1);
    tokio::spawn(async move {
        loop {
            match t1_clone.accept().await {
                Ok((addr, peer)) => {
                    let rpc = Arc::clone(&rpc1_clone);
                    let peer = Arc::new(peer);
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

    let stop_flag = Arc::new(AtomicBool::new(false));
    let drop_rate_clone = Arc::clone(&drop_rate);
    let stop_flag_clone = Arc::clone(&stop_flag);
    tokio::spawn(async move {
        while !stop_flag_clone.load(Ordering::SeqCst) {
            drop_rate_clone.store(true, Ordering::SeqCst);
            sleep(Duration::from_secs(1)).await;
            drop_rate_clone.store(false, Ordering::SeqCst);
            sleep(Duration::from_secs(1)).await;
        }
    });

    let client_peer = Arc::new(transport1.connect(addr2).await.unwrap());
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

    let success_counter = Arc::new(AtomicUsize::new(0));
    let failure_counter = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::new();
    for worker_id in 0..8 {
        let rpc1 = Arc::clone(&rpc1);
        let success_counter = Arc::clone(&success_counter);
        let failure_counter = Arc::clone(&failure_counter);
        workers.push(tokio::spawn(async move {
            for seq in 0..30 {
                let resp = rpc1
                    .request(
                        addr2,
                        RequestPayload::Custom {
                            operation: format!("storm-{worker_id}-{seq}"),
                            data: Bytes::from_static(b"rpc-chaos"),
                        },
                        Duration::from_secs(4),
                    )
                    .await;
                match resp {
                    Ok(resp_msg) => match resp_msg.payload {
                        ResponsePayload::CustomResponse { success, .. } if success => {
                            success_counter.fetch_add(1, Ordering::SeqCst);
                        }
                        _ => {
                            failure_counter.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                    Err(_) => {
                        failure_counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    for worker in workers {
        worker.await.unwrap();
    }
    stop_flag.store(true, Ordering::SeqCst);
    sleep(Duration::from_millis(100)).await;

    assert!(
        success_counter.load(Ordering::SeqCst) >= 50,
        "expected many successful RPCs despite chaos; successes={}",
        success_counter.load(Ordering::SeqCst)
    );
    assert!(
        failure_counter.load(Ordering::SeqCst) > 0,
        "failures should occur under chaos"
    );

    transport1.close();
    transport2.close();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "chaos test - slow and flaky"]
async fn test_shipping_lane_retries_under_flaky_receiver() {
    let sender = Arc::new(QuicTransport::new("127.0.0.1:0".parse().unwrap()).await.unwrap());
    let receiver = Arc::new(QuicTransport::new("127.0.0.1:0".parse().unwrap()).await.unwrap());
    let receiver_addr = receiver.local_addr().unwrap();

    let fail_counter = Arc::new(AtomicUsize::new(0));
    let total_successes = 6usize;
    let (done_tx, done_rx) = oneshot::channel();
    let accept_loop = {
        let transport = Arc::clone(&receiver);
        let fail_counter = Arc::clone(&fail_counter);
        tokio::spawn(async move {
            let mut collected = Vec::new();
            while collected.len() < total_successes {
                let (_, peer) = transport.accept().await.unwrap();
                let attempt = fail_counter.fetch_add(1, Ordering::SeqCst);
                if attempt % 3 == 0 {
                    // Drop connection without reading to force the sender to retry.
                    drop(peer);
                    continue;
                }
                if let Ok(Some(data)) = peer.recv_chunk_verified().await {
                    collected.push(data);
                }
            }
            let _ = done_tx.send(collected);
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let lane = ShippingLane::new(Arc::clone(&sender));
    let payloads: Vec<Bytes> = (0..total_successes)
        .map(|i| Bytes::from(vec![i as u8; 128 * 1024]))
        .collect();
    let expected_payloads = payloads.clone();

    for payload in payloads.clone() {
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = lane.send_memory(receiver_addr, payload.clone()).await.unwrap();
            if result.success {
                break;
            }
            assert!(attempt < 6, "too many retries for payload");
            sleep(Duration::from_millis(200)).await;
        }
    }

    let received = done_rx.await.unwrap();
    assert_eq!(received.len(), total_successes);
    let mut expected_sorted: Vec<Vec<u8>> = expected_payloads.into_iter().map(|b| b.to_vec()).collect();
    let mut actual_sorted: Vec<Vec<u8>> = received.into_iter().map(|b| b.to_vec()).collect();
    expected_sorted.sort();
    actual_sorted.sort();
    assert_eq!(expected_sorted, actual_sorted);
    accept_loop.await.unwrap();
    sender.close();
    receiver.close();
}
