use bytes::Bytes;
use octopii::chunk::ChunkSource;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test]
async fn test_chunk_transfer_memory() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Spawn receiver
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send chunk
    let test_data = Bytes::from("Hello, this is a test chunk!");
    let chunk = ChunkSource::Memory(test_data.clone());

    let peer1 = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer1.send_chunk_verified(&chunk).await.unwrap();

    assert_eq!(bytes_sent, test_data.len() as u64);

    // Verify receiver got the data
    let received = receiver_handle.await.unwrap().unwrap();
    assert_eq!(received, test_data);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_chunk_transfer_file() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create test file
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("octopii_test_chunk.dat");
    let test_data = b"This is test file data for chunk transfer!";
    tokio::fs::write(&test_file, test_data).await.unwrap();

    // Spawn receiver
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send chunk from file
    let chunk = ChunkSource::File(test_file.clone());
    let peer1 = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer1.send_chunk_verified(&chunk).await.unwrap();

    assert_eq!(bytes_sent, test_data.len() as u64);

    // Verify receiver got the data
    let received = receiver_handle.await.unwrap().unwrap();
    assert_eq!(received.as_ref(), test_data);

    // Cleanup
    let _ = tokio::fs::remove_file(&test_file).await;

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_chunk_transfer_checksum_verification() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create test data
    let test_data = Bytes::from(vec![0u8; 1024 * 1024]); // 1MB of zeros

    // Spawn receiver
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send chunk
    let chunk = ChunkSource::Memory(test_data.clone());
    let peer1 = transport1.connect(actual_addr2).await.unwrap();
    let result = peer1.send_chunk_verified(&chunk).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1024 * 1024);

    // Verify receiver got correct data
    let received = receiver_handle.await.unwrap().unwrap();
    assert_eq!(received.len(), test_data.len());
    assert_eq!(received, test_data);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_chunk_transfer_stream_to_disk() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    let temp_dir = tempfile::tempdir().unwrap();
    let dest_path = temp_dir.path().join("streamed.bin");

    let t2_clone = Arc::clone(&transport2);
    let dest_clone = dest_path.clone();
    let receiver = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_to_path(dest_clone).await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let payload = Bytes::from(vec![5u8; 8 * 1024]);
    let chunk = ChunkSource::Memory(payload.clone());
    let sender = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = sender.send_chunk_verified(&chunk).await.unwrap();
    assert_eq!(bytes_sent, payload.len() as u64);

    let written = receiver.await.unwrap().unwrap();
    assert_eq!(written, payload.len() as u64);

    let on_disk = tokio::fs::read(&dest_path).await.unwrap();
    assert_eq!(on_disk, payload.as_ref());

    transport1.close();
    transport2.close();
}
