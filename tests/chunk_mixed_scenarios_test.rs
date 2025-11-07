use bytes::Bytes;
use futures::future::join_all;
use octopii::chunk::ChunkSource;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

/// Helper to create test file
async fn create_test_file(path: &std::path::Path, size_mb: usize) {
    let size = size_mb * 1024 * 1024;
    let mut data = Vec::with_capacity(size);
    for i in 0..size {
        data.push((i % 256) as u8);
    }
    tokio::fs::write(path, &data).await.unwrap();
}

#[tokio::test]
async fn test_mixed_file_and_memory_transfers() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create mixed sources
    let temp_dir = std::env::temp_dir();

    // 3 file chunks
    let file1 = temp_dir.join("mixed_1.dat");
    let file2 = temp_dir.join("mixed_2.dat");
    let file3 = temp_dir.join("mixed_3.dat");

    create_test_file(&file1, 5).await; // 5MB
    create_test_file(&file2, 8).await; // 8MB
    create_test_file(&file3, 3).await; // 3MB

    // 3 memory chunks
    let mem1 = Bytes::from(vec![1u8; 4 * 1024 * 1024]); // 4MB
    let mem2 = Bytes::from(vec![2u8; 6 * 1024 * 1024]); // 6MB
    let mem3 = Bytes::from(vec![3u8; 2 * 1024 * 1024]); // 2MB

    let sources = vec![
        ChunkSource::File(file1.clone()),
        ChunkSource::Memory(mem1.clone()),
        ChunkSource::File(file2.clone()),
        ChunkSource::Memory(mem2.clone()),
        ChunkSource::File(file3.clone()),
        ChunkSource::Memory(mem3.clone()),
    ];

    println!("Transferring 6 mixed chunks (files + memory) in parallel...");

    // Spawn receivers
    let receiver_tasks: Vec<_> = (0..6)
        .map(|_| {
            let t2 = Arc::clone(&transport2);
            tokio::spawn(async move {
                let (_, peer) = t2.accept().await.unwrap();
                peer.recv_chunk_verified().await.unwrap().unwrap()
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send all in parallel
    let start = std::time::Instant::now();

    let sender_tasks: Vec<_> = sources
        .iter()
        .map(|source| {
            let t1 = Arc::clone(&transport1);
            let source = source.clone();
            tokio::spawn(async move {
                let peer = t1.connect(actual_addr2).await.unwrap();
                peer.send_chunk_verified(&source).await.unwrap()
            })
        })
        .collect();

    let sent_results = join_all(sender_tasks).await;
    let duration = start.elapsed();

    println!(
        "✓ Mixed transfers completed in {:?} ({:.2} MB/s)",
        duration,
        28.0 / duration.as_secs_f64() // 5+4+8+6+3+2 = 28MB total
    );

    // Verify
    for result in sent_results {
        result.unwrap();
    }

    join_all(receiver_tasks).await;

    // Cleanup
    let _ = tokio::fs::remove_file(&file1).await;
    let _ = tokio::fs::remove_file(&file2).await;
    let _ = tokio::fs::remove_file(&file3).await;

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_varying_chunk_sizes_parallel() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create chunks of varying sizes: 1KB, 10KB, 100KB, 1MB, 5MB, 10MB
    let sizes = vec![
        1 * 1024,           // 1KB
        10 * 1024,          // 10KB
        100 * 1024,         // 100KB
        1 * 1024 * 1024,    // 1MB
        5 * 1024 * 1024,    // 5MB
        10 * 1024 * 1024,   // 10MB
    ];

    println!("Creating chunks of varying sizes: 1KB to 10MB...");
    let chunks: Vec<Bytes> = sizes
        .iter()
        .enumerate()
        .map(|(i, &size)| {
            let mut data = vec![0u8; size];
            for j in 0..size {
                data[j] = ((i + j) % 256) as u8;
            }
            Bytes::from(data)
        })
        .collect();

    // Spawn receivers
    let receiver_tasks: Vec<_> = (0..chunks.len())
        .map(|_| {
            let t2 = Arc::clone(&transport2);
            tokio::spawn(async move {
                let (_, peer) = t2.accept().await.unwrap();
                peer.recv_chunk_verified().await.unwrap().unwrap()
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send all sizes in parallel
    println!("Sending all sizes in parallel...");
    let start = std::time::Instant::now();

    let sender_tasks: Vec<_> = chunks
        .iter()
        .map(|chunk| {
            let t1 = Arc::clone(&transport1);
            let chunk = chunk.clone();
            tokio::spawn(async move {
                let peer = t1.connect(actual_addr2).await.unwrap();
                peer.send_chunk_verified(&ChunkSource::Memory(chunk))
                    .await
                    .unwrap()
            })
        })
        .collect();

    let sent_results = join_all(sender_tasks).await;
    let received_results = join_all(receiver_tasks).await;
    let duration = start.elapsed();

    println!("✓ All varying sizes transferred in {:?}", duration);

    // Verify
    for (result, expected_size) in sent_results.iter().zip(sizes.iter()) {
        assert_eq!(result.as_ref().unwrap(), expected_size);
    }

    for (received, expected) in received_results.iter().zip(chunks.iter()) {
        assert_eq!(received.as_ref().unwrap(), expected);
    }

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_rapid_small_chunks() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    let num_chunks = 100;
    let chunk_size = 64 * 1024; // 64KB each

    println!("Rapid fire: {} x 64KB chunks in parallel", num_chunks);

    let chunks: Vec<Bytes> = (0..num_chunks)
        .map(|i| {
            let mut data = vec![0u8; chunk_size];
            for j in 0..chunk_size {
                data[j] = ((i + j) % 256) as u8;
            }
            Bytes::from(data)
        })
        .collect();

    // Spawn receivers
    let receiver_tasks: Vec<_> = (0..num_chunks)
        .map(|_| {
            let t2 = Arc::clone(&transport2);
            tokio::spawn(async move {
                let (_, peer) = t2.accept().await.unwrap();
                peer.recv_chunk_verified().await.unwrap().unwrap()
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fire all at once
    let start = std::time::Instant::now();

    let sender_tasks: Vec<_> = chunks
        .iter()
        .map(|chunk| {
            let t1 = Arc::clone(&transport1);
            let chunk = chunk.clone();
            tokio::spawn(async move {
                let peer = t1.connect(actual_addr2).await.unwrap();
                peer.send_chunk_verified(&ChunkSource::Memory(chunk))
                    .await
                    .unwrap()
            })
        })
        .collect();

    join_all(sender_tasks).await;
    join_all(receiver_tasks).await;

    let duration = start.elapsed();
    let total_mb = (num_chunks * 64) as f64 / 1024.0;

    println!(
        "✓ {} chunks ({:.1}MB) in {:?} ({:.2} chunks/sec, {:.2} MB/s)",
        num_chunks,
        total_mb,
        duration,
        num_chunks as f64 / duration.as_secs_f64(),
        total_mb / duration.as_secs_f64()
    );

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_interleaved_rpc_and_chunk_transfer() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    println!("Testing RPC and chunk transfer interleaving...");

    // Prepare a 10MB chunk
    let chunk_data = Bytes::from(vec![42u8; 10 * 1024 * 1024]);
    let chunk = ChunkSource::Memory(chunk_data.clone());

    // Spawn chunk receiver
    let t2_chunk = Arc::clone(&transport2);
    let chunk_receiver = tokio::spawn(async move {
        let (_, peer) = t2_chunk.accept().await.unwrap();
        peer.recv_chunk_verified().await.unwrap().unwrap()
    });

    // Spawn RPC receivers (simulate 10 RPCs)
    let rpc_receivers: Vec<_> = (0..10)
        .map(|_| {
            let t2_rpc = Arc::clone(&transport2);
            tokio::spawn(async move {
                let (_, peer) = t2_rpc.accept().await.unwrap();
                peer.recv().await.unwrap().unwrap()
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect once
    let peer = transport1.connect(actual_addr2).await.unwrap();

    // Start chunk transfer in background
    let chunk_sender = {
        let peer = Arc::new(peer);
        let chunk = chunk.clone();
        let peer_clone = Arc::clone(&peer);
        tokio::spawn(async move {
            peer_clone.send_chunk_verified(&chunk).await.unwrap()
        })
    };

    // While chunk is transferring, send RPCs
    println!("  Chunk transfer started, now sending 10 RPCs...");
    let peer_arc = Arc::new(transport1.connect(actual_addr2).await.unwrap());

    let rpc_senders: Vec<_> = (0..10)
        .map(|i| {
            let peer = Arc::clone(&peer_arc);
            let msg = Bytes::from(format!("RPC message {}", i));
            tokio::spawn(async move {
                peer.send(msg).await.unwrap();
            })
        })
        .collect();

    // Wait for all to complete
    chunk_sender.await.unwrap();
    join_all(rpc_senders).await;
    chunk_receiver.await.unwrap();
    join_all(rpc_receivers).await;

    println!("✓ Chunk transfer and RPCs completed without blocking each other");

    transport1.close();
    transport2.close();
}
