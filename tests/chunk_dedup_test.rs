use bytes::Bytes;
use octopii::chunk::ChunkSource;
use octopii::transport::QuicTransport;
use octopii::BlobStore;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test]
async fn test_send_chunk_dedup_new_data() {
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
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::new(store_dir.path()).unwrap());

    let payload = Bytes::from(vec![42u8; 1024]);
    let expected_hash: [u8; 32] = Sha256::digest(&payload).into();

    let recv_handle = {
        let transport = Arc::clone(&receiver);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_dedup(&store).await.unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let peer = sender.connect(receiver_addr).await.unwrap();
    let (bytes_sent, was_needed) = peer
        .send_chunk_dedup(ChunkSource::Memory(payload.clone()))
        .await
        .unwrap();

    assert!(was_needed, "new data should be needed");
    assert_eq!(bytes_sent, 1024);

    let received_hash = recv_handle.await.unwrap().unwrap();
    assert_eq!(received_hash, expected_hash);

    // Verify data was stored
    let stored = store.get(&received_hash).unwrap();
    assert_eq!(stored, payload.as_ref());

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_send_chunk_dedup_already_exists() {
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
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::new(store_dir.path()).unwrap());

    let payload = Bytes::from(vec![99u8; 2048]);

    // Pre-store the data
    let existing_hash = store.put(&payload).unwrap();

    let recv_handle = {
        let transport = Arc::clone(&receiver);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_dedup(&store).await.unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let peer = sender.connect(receiver_addr).await.unwrap();
    let (bytes_sent, was_needed) = peer
        .send_chunk_dedup(ChunkSource::Memory(payload))
        .await
        .unwrap();

    assert!(!was_needed, "existing data should not be needed");
    assert_eq!(bytes_sent, 0, "no bytes should be transferred");

    let received_hash = recv_handle.await.unwrap().unwrap();
    assert_eq!(received_hash, existing_hash);

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_dedup_saves_bandwidth() {
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
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::new(store_dir.path()).unwrap());

    // 5MB payload
    let large_payload = Bytes::from(vec![0xABu8; 5 * 1024 * 1024]);

    // Receiver accepts once and receives twice on the same peer
    let recv_handle = {
        let transport = Arc::clone(&receiver);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            // Receive twice on the same connection
            let hash1 = peer.recv_chunk_dedup(&store).await.unwrap();
            let hash2 = peer.recv_chunk_dedup(&store).await.unwrap();
            (hash1, hash2)
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Sender sends twice on the same connection
    let peer = sender.connect(receiver_addr).await.unwrap();

    // First transfer - should transfer data
    let (bytes1, needed1) = peer
        .send_chunk_dedup(ChunkSource::Memory(large_payload.clone()))
        .await
        .unwrap();

    assert!(needed1, "first transfer should be needed");
    assert_eq!(bytes1, 5 * 1024 * 1024);

    // Second transfer - should skip data (dedup)
    let (bytes2, needed2) = peer
        .send_chunk_dedup(ChunkSource::Memory(large_payload))
        .await
        .unwrap();

    assert!(!needed2, "second transfer should be deduped");
    assert_eq!(bytes2, 0, "no bytes transferred on dedup");

    let (hash1, hash2) = recv_handle.await.unwrap();
    assert_eq!(hash1.unwrap(), hash2.unwrap(), "hashes should match");

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_send_chunk_dedup_file_source() {
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

    // Create source file
    let src_dir = tempfile::tempdir().unwrap();
    let src_path = src_dir.path().join("source.bin");
    let payload: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    tokio::fs::write(&src_path, &payload).await.unwrap();

    // Create blob store
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::new(store_dir.path()).unwrap());

    let recv_handle = {
        let transport = Arc::clone(&receiver);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_dedup(&store).await.unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let peer = sender.connect(receiver_addr).await.unwrap();
    let (bytes_sent, was_needed) = peer
        .send_chunk_dedup(ChunkSource::File(src_path))
        .await
        .unwrap();

    assert!(was_needed);
    assert_eq!(bytes_sent, 4096);

    let hash = recv_handle.await.unwrap().unwrap();

    // Verify hash matches
    let expected_hash: [u8; 32] = Sha256::digest(&payload).into();
    assert_eq!(hash, expected_hash);

    sender.close();
    receiver.close();
}

#[tokio::test]
async fn test_shipping_lane_dedup() {
    use octopii::transport::Transport;
    use octopii::ShippingLane;

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
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(BlobStore::new(store_dir.path()).unwrap());

    let payload = Bytes::from(vec![77u8; 8192]);

    // Pre-store the data to trigger dedup
    let existing_hash = store.put(&payload).unwrap();

    let recv_handle = {
        let transport = Arc::clone(&receiver);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let (_, peer) = transport.accept().await.unwrap();
            peer.recv_chunk_dedup(&store).await.unwrap()
        })
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    let lane = ShippingLane::new(Arc::clone(&sender) as Arc<dyn Transport>);
    let (result, was_needed) = lane
        .send_chunk_dedup(receiver_addr, ChunkSource::Memory(payload))
        .await
        .unwrap();

    assert!(result.success);
    assert!(!was_needed, "should be deduped");
    assert_eq!(result.bytes_transferred, 0);

    let received_hash = recv_handle.await.unwrap().unwrap();
    assert_eq!(received_hash, existing_hash);

    sender.close();
    receiver.close();
}
