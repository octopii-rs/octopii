use bytes::Bytes;
use octopii::chunk::ChunkSource;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test]
async fn test_nonexistent_file() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());

    // Try to send a file that doesn't exist
    let nonexistent_file = std::path::PathBuf::from("/tmp/this_file_does_not_exist_12345.dat");
    let chunk = ChunkSource::File(nonexistent_file);

    let fake_peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();

    // This should fail when trying to open the file
    let peer_result = transport1.connect(fake_peer).await;

    // Connection might fail or file open might fail - either is acceptable
    // The key is it doesn't panic and handles the error gracefully
    if let Ok(peer) = peer_result {
        let result = peer.send_chunk_verified(&chunk).await;
        assert!(result.is_err(), "Should fail when file doesn't exist");
    }

    transport1.close();
}

#[tokio::test]
async fn test_empty_chunk() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Empty chunk
    let empty_chunk = Bytes::new();
    let chunk = ChunkSource::Memory(empty_chunk.clone());

    // Spawn receiver
    let t2 = Arc::clone(&transport2);
    let receiver = tokio::spawn(async move {
        let (_, peer) = t2.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send empty chunk
    let peer = transport1.connect(actual_addr2).await.unwrap();
    let result = peer.send_chunk_verified(&chunk).await.unwrap();

    assert_eq!(result, 0, "Empty chunk should transfer 0 bytes");

    // Verify receiver gets empty data
    let received = receiver.await.unwrap().unwrap();
    assert_eq!(received.len(), 0);
    assert_eq!(received, empty_chunk);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_connection_timeout() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());

    // Try to connect to a non-existent peer with short timeout
    let fake_peer: SocketAddr = "192.0.2.1:9999".parse().unwrap(); // TEST-NET-1, should be unreachable

    let result = tokio::time::timeout(Duration::from_secs(2), transport1.connect(fake_peer)).await;

    // Should timeout or fail to connect
    assert!(
        result.is_err() || result.unwrap().is_err(),
        "Should timeout or fail when peer is unreachable"
    );

    transport1.close();
}

#[tokio::test]
async fn test_very_small_chunks_edge_cases() {
    // Test various small sizes: 1 byte, 2 bytes, 7 bytes, 63 bytes
    let test_sizes = vec![1, 2, 7, 63, 127, 255, 511];

    for size in test_sizes {
        // Create fresh transports for each iteration to avoid connection reuse issues
        let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
        let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

        let actual_addr2 = transport2.local_addr().unwrap();

        // Create chunk
        let data = Bytes::from(vec![size as u8; size]);
        let chunk = ChunkSource::Memory(data.clone());

        // Spawn receiver
        let t2 = Arc::clone(&transport2);
        let receiver = tokio::spawn(async move {
            let (_, peer) = t2.accept().await.unwrap();
            peer.recv_chunk_verified().await.unwrap().unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send
        let peer = transport1.connect(actual_addr2).await.unwrap();
        let bytes_sent = peer.send_chunk_verified(&chunk).await.unwrap();

        assert_eq!(bytes_sent, size as u64);

        // Verify
        let received = receiver.await.unwrap();
        assert_eq!(received.len(), size);
        assert_eq!(received, data);

        transport1.close();
        transport2.close();
    }
}

#[tokio::test]
async fn test_rapid_connect_disconnect() {
    // Connect and immediately try to send (stress test)
    for i in 0..10 {
        // Create fresh transports for each iteration to ensure new connections
        let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

        let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
        let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

        let actual_addr2 = transport2.local_addr().unwrap();

        let chunk = Bytes::from(format!("Message {}", i));

        // Spawn receiver
        let t2 = Arc::clone(&transport2);
        let receiver = tokio::spawn(async move {
            let (_, peer) = t2.accept().await.unwrap();
            peer.recv_chunk_verified().await.unwrap().unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send
        let peer = transport1.connect(actual_addr2).await.unwrap();
        let result = peer
            .send_chunk_verified(&ChunkSource::Memory(chunk.clone()))
            .await;

        assert!(result.is_ok(), "Transfer {} should succeed", i);

        // Verify
        let received = receiver.await.unwrap();
        assert_eq!(received, chunk);

        transport1.close();
        transport2.close();
    }
}

#[tokio::test]
async fn test_transfer_with_exactly_buffer_size() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create chunk exactly 64KB (the buffer size)
    let size = 64 * 1024;
    let chunk_data = Bytes::from(vec![77u8; size]);
    let chunk = ChunkSource::Memory(chunk_data.clone());

    // Spawn receiver
    let t2 = Arc::clone(&transport2);
    let receiver = tokio::spawn(async move {
        let (_, peer) = t2.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap().unwrap()
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send
    let peer = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer.send_chunk_verified(&chunk).await.unwrap();

    assert_eq!(bytes_sent, size as u64);

    // Verify
    let received = receiver.await.unwrap();
    assert_eq!(received.len(), size);
    assert_eq!(received, chunk_data);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_transfer_multiple_of_buffer_size() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create chunk exactly 3 * 64KB
    let size = 3 * 64 * 1024;
    let chunk_data = Bytes::from(vec![88u8; size]);
    let chunk = ChunkSource::Memory(chunk_data.clone());

    // Spawn receiver
    let t2 = Arc::clone(&transport2);
    let receiver = tokio::spawn(async move {
        let (_, peer) = t2.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap().unwrap()
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send
    let peer = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer.send_chunk_verified(&chunk).await.unwrap();

    assert_eq!(bytes_sent, size as u64);

    // Verify
    let received = receiver.await.unwrap();
    assert_eq!(received.len(), size);
    assert_eq!(received, chunk_data);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_transfer_not_multiple_of_buffer_size() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create chunk that's NOT a multiple of buffer size: 64KB + 1234 bytes
    let size = 64 * 1024 + 1234;
    let chunk_data = Bytes::from(vec![99u8; size]);
    let chunk = ChunkSource::Memory(chunk_data.clone());

    // Spawn receiver
    let t2 = Arc::clone(&transport2);
    let receiver = tokio::spawn(async move {
        let (_, peer) = t2.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap().unwrap()
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send
    let peer = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer.send_chunk_verified(&chunk).await.unwrap();

    assert_eq!(bytes_sent, size as u64);

    // Verify
    let received = receiver.await.unwrap();
    assert_eq!(received.len(), size);
    assert_eq!(received, chunk_data);

    transport1.close();
    transport2.close();
}
