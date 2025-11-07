use bytes::Bytes;
use octopii::chunk::ChunkSource;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

/// Helper to create a large test file
async fn create_large_test_file(path: &std::path::Path, size_mb: usize) -> Vec<u8> {
    let size = size_mb * 1024 * 1024;
    // Create pseudo-random data for better realism
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push((i % 256) as u8);
    }
    tokio::fs::write(path, &data).await.unwrap();
    data
}

#[tokio::test]
async fn test_large_chunk_10mb() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create 10MB test file
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("octopii_large_10mb.dat");
    let original_data = create_large_test_file(&test_file, 10).await;

    // Spawn receiver
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send large chunk
    let start = std::time::Instant::now();
    let chunk = ChunkSource::File(test_file.clone());
    let peer1 = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer1.send_chunk_verified(&chunk).await.unwrap();
    let duration = start.elapsed();

    println!("Transferred 10MB in {:?} ({:.2} MB/s)",
        duration,
        10.0 / duration.as_secs_f64()
    );

    assert_eq!(bytes_sent, 10 * 1024 * 1024);

    // Verify data integrity
    let received = receiver_handle.await.unwrap().unwrap();
    assert_eq!(received.len(), original_data.len());
    assert_eq!(&received[..], &original_data[..]);

    // Cleanup
    let _ = tokio::fs::remove_file(&test_file).await;
    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_large_chunk_100mb() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create 100MB test file
    let temp_dir = std::env::temp_dir();
    let test_file = temp_dir.join("octopii_large_100mb.dat");
    println!("Creating 100MB test file...");
    let original_data = create_large_test_file(&test_file, 100).await;

    // Spawn receiver
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send large chunk
    println!("Starting transfer...");
    let start = std::time::Instant::now();
    let chunk = ChunkSource::File(test_file.clone());
    let peer1 = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer1.send_chunk_verified(&chunk).await.unwrap();
    let duration = start.elapsed();

    println!("Transferred 100MB in {:?} ({:.2} MB/s)",
        duration,
        100.0 / duration.as_secs_f64()
    );

    assert_eq!(bytes_sent, 100 * 1024 * 1024);

    // Verify data integrity
    let received = receiver_handle.await.unwrap().unwrap();
    assert_eq!(received.len(), original_data.len());

    // For large data, just verify checksums match
    use sha2::{Sha256, Digest};
    let mut hasher1 = Sha256::new();
    hasher1.update(&original_data);
    let hash1 = hasher1.finalize();

    let mut hasher2 = Sha256::new();
    hasher2.update(&received);
    let hash2 = hasher2.finalize();

    assert_eq!(hash1, hash2, "Data checksums don't match!");

    // Cleanup
    let _ = tokio::fs::remove_file(&test_file).await;
    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_large_memory_chunk_50mb() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create 50MB in-memory chunk
    println!("Creating 50MB in-memory chunk...");
    let size = 50 * 1024 * 1024;
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push((i % 256) as u8);
    }
    let test_data = Bytes::from(data);

    // Spawn receiver
    let t2_clone = Arc::clone(&transport2);
    let receiver_handle = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap()
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send large memory chunk
    println!("Starting transfer...");
    let start = std::time::Instant::now();
    let chunk = ChunkSource::Memory(test_data.clone());
    let peer1 = transport1.connect(actual_addr2).await.unwrap();
    let bytes_sent = peer1.send_chunk_verified(&chunk).await.unwrap();
    let duration = start.elapsed();

    println!("Transferred 50MB from memory in {:?} ({:.2} MB/s)",
        duration,
        50.0 / duration.as_secs_f64()
    );

    assert_eq!(bytes_sent, 50 * 1024 * 1024);

    // Verify data
    let received = receiver_handle.await.unwrap().unwrap();
    assert_eq!(received.len(), test_data.len());
    assert_eq!(received, test_data);

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_sequential_large_transfers() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create test files
    let temp_dir = std::env::temp_dir();
    let test_files: Vec<_> = (0..5)
        .map(|i| {
            let path = temp_dir.join(format!("octopii_seq_{}.dat", i));
            (path, 10) // 10MB each
        })
        .collect();

    for (path, size) in &test_files {
        create_large_test_file(path, *size).await;
    }

    // Connect once
    let peer1 = transport1.connect(actual_addr2).await.unwrap();

    println!("Sending 5 x 10MB chunks sequentially on same connection...");
    let total_start = std::time::Instant::now();

    for (i, (path, _)) in test_files.iter().enumerate() {
        // Spawn receiver for this transfer
        let t2_clone = Arc::clone(&transport2);
        let receiver_handle = tokio::spawn(async move {
            let (_, peer) = t2_clone.accept().await.unwrap();
            peer.recv_chunk_verified().await.unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let chunk = ChunkSource::File(path.clone());
        let bytes_sent = peer1.send_chunk_verified(&chunk).await.unwrap();
        assert_eq!(bytes_sent, 10 * 1024 * 1024);

        let _received = receiver_handle.await.unwrap().unwrap();
        println!("  Chunk {} transferred", i + 1);
    }

    let total_duration = total_start.elapsed();
    println!("Total: 50MB in {:?} ({:.2} MB/s average)",
        total_duration,
        50.0 / total_duration.as_secs_f64()
    );

    // Cleanup
    for (path, _) in &test_files {
        let _ = tokio::fs::remove_file(path).await;
    }

    transport1.close();
    transport2.close();
}
