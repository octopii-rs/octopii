use bytes::Bytes;
use futures::future;
use octopii::transport::QuicTransport;
use octopii::{ChunkSource, ShippingLane};
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test]
async fn test_shipping_lane_send_file_to_peer() {
    let sender = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );

    let receiver_addr = receiver.local_addr().unwrap();
    let dest_dir = tempfile::tempdir().unwrap();
    let dest_path = dest_dir.path().join("received.bin");

    let accept_handle = {
        let transport = Arc::clone(&receiver);
        let dest_path = dest_path.clone();
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_to_path(&dest_path).await.unwrap().unwrap();
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let src_dir = tempfile::tempdir().unwrap();
    let src_path = src_dir.path().join("source.bin");
    let payload: Vec<u8> = (0..8 * 1024).map(|i| (i % 251) as u8).collect();
    tokio::fs::write(&src_path, &payload).await.unwrap();

    let lane = ShippingLane::new(Arc::clone(&sender));
    let result = lane.send_file(receiver_addr, &src_path).await.unwrap();
    assert!(result.success);
    assert_eq!(result.bytes_transferred, payload.len() as u64);
    assert!(result.checksum_verified);

    accept_handle.await.unwrap();
    let received = tokio::fs::read(&dest_path).await.unwrap();
    assert_eq!(received, payload);

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_shipping_lane_receive_file_from_peer() {
    let client = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let server = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );

    let server_addr = server.local_addr().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let source_path = source_dir.path().join("payload.bin");
    let payload: Vec<u8> = (0..4096).map(|i| (i % 127) as u8).collect();
    tokio::fs::write(&source_path, &payload).await.unwrap();

    let send_handle = {
        let transport = Arc::clone(&server);
        let source_path = source_path.clone();
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.send_chunk_verified(&ChunkSource::File(source_path))
                .await
                .unwrap();
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let dest_dir = tempfile::tempdir().unwrap();
    let dest_path = dest_dir.path().join("download.bin");

    let lane = ShippingLane::new(Arc::clone(&client));
    let result = lane.receive_file(server_addr, &dest_path).await.unwrap();
    send_handle.await.unwrap();

    assert!(result.success);
    assert_eq!(result.bytes_transferred, payload.len() as u64);
    let received = tokio::fs::read(&dest_path).await.unwrap();
    assert_eq!(received, payload);

    client.close();
    server.close();
}

#[tokio::test]
async fn test_shipping_lane_send_memory() {
    let sender = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );

    let receiver_addr = receiver.local_addr().unwrap();
    let payload = Bytes::from_static(b"shipping-lane-mem-payload");

    let receive_handle = {
        let transport = Arc::clone(&receiver);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_verified().await.unwrap().unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let lane = ShippingLane::new(Arc::clone(&sender));
    let result = lane
        .send_memory(receiver_addr, payload.clone())
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(result.bytes_transferred, payload.len() as u64);

    let received = receive_handle.await.unwrap();
    assert_eq!(received, payload);

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_shipping_lane_receive_memory() {
    let client = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let server = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );

    let server_addr = server.local_addr().unwrap();
    let payload = Bytes::from(vec![42u8; 2048]);
    let expected = payload.clone();

    let send_handle = {
        let transport = Arc::clone(&server);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.send_chunk_verified(&ChunkSource::Memory(payload))
                .await
                .unwrap();
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let lane = ShippingLane::new(Arc::clone(&client));
    let (result, data) = lane.receive_memory(server_addr).await.unwrap();
    send_handle.await.unwrap();

    assert!(result.success);
    assert_eq!(result.bytes_transferred, expected.len() as u64);
    let received = data.expect("payload should be returned");
    assert_eq!(received, expected);

    client.close();
    server.close();
}

#[tokio::test]
async fn test_shipping_lane_send_memory_reports_failure_on_aborted_receiver() {
    let sender = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );

    let receiver_addr = receiver.local_addr().unwrap();

    let drop_handle = {
        let transport = Arc::clone(&receiver);
        tokio::spawn(async move {
            if let Ok((_, peer)) = transport.accept().await {
                tokio::time::sleep(Duration::from_millis(200)).await;
                drop(peer);
            }
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let lane = ShippingLane::new(Arc::clone(&sender));
    let payload = Bytes::from_static(b"should-fail");
    let result = lane.send_memory(receiver_addr, payload).await.unwrap();
    drop_handle.await.unwrap();

    assert!(
        !result.success,
        "shipping lane should surface failure when receiver aborts"
    );
    assert!(
        !result.checksum_verified,
        "failure path should report checksum not verified"
    );
    assert!(
        result
            .error
            .as_ref()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "failure path should include error message"
    );

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_shipping_lane_large_file_transfer() {
    let sender = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver_addr = receiver.local_addr().unwrap();

    let dest_dir = tempfile::tempdir().unwrap();
    let dest_path = dest_dir.path().join("large.bin");
    let recv_handle = {
        let transport = Arc::clone(&receiver);
        let dest_path = dest_path.clone();
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_to_path(&dest_path).await.unwrap().unwrap();
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let src_dir = tempfile::tempdir().unwrap();
    let src_path = src_dir.path().join("huge.bin");
    let payload: Vec<u8> = (0..(2 * 1024 * 1024)).map(|i| (i % 251) as u8).collect();
    tokio::fs::write(&src_path, &payload).await.unwrap();

    let lane = ShippingLane::new(Arc::clone(&sender));
    let result = lane.send_file(receiver_addr, &src_path).await.unwrap();
    assert!(result.success);
    assert_eq!(result.bytes_transferred, payload.len() as u64);

    recv_handle.await.unwrap();
    let received = tokio::fs::read(&dest_path).await.unwrap();
    assert_eq!(received, payload);

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_shipping_lane_concurrent_memory_transfers() {
    let sender = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver = Arc::new(
        QuicTransport::new("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let receiver_addr = receiver.local_addr().unwrap();

    let payloads: Vec<Bytes> = (0..5)
        .map(|i| Bytes::from(vec![i as u8; 32 * 1024]))
        .collect();

    let recv_handle = {
        let transport = Arc::clone(&receiver);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            let mut received = Vec::new();
            for _ in 0..5 {
                let data = peer.recv_chunk_verified().await.unwrap().unwrap();
                received.push(data);
            }
            received
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut handles = Vec::new();
    for payload in payloads.clone() {
        let transport = Arc::clone(&sender);
        handles.push(tokio::spawn(async move {
            let lane = ShippingLane::new(transport);
            lane.send_memory(receiver_addr, payload).await.unwrap()
        }));
    }

    let results = future::join_all(handles).await;
    for res in results {
        let transfer = res.unwrap();
        assert!(transfer.success);
        assert_eq!(transfer.bytes_transferred, 32 * 1024);
    }

    let received = recv_handle.await.unwrap();
    let mut expected: Vec<Vec<u8>> = payloads.into_iter().map(|b| b.to_vec()).collect();
    let mut actual: Vec<Vec<u8>> = received.into_iter().map(|b| b.to_vec()).collect();
    expected.sort();
    actual.sort();
    assert_eq!(expected, actual);

    sender.close();
    receiver.close();
}
