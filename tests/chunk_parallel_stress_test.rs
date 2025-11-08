use bytes::Bytes;
use futures::future::join_all;
use octopii::chunk::ChunkSource;
use octopii::transport::QuicTransport;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

#[tokio::test]
async fn test_parallel_transfers_to_single_peer() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    // Create 10 chunks of 5MB each
    let num_chunks = 10;
    let chunk_size = 5 * 1024 * 1024;

    println!("Creating {} x 5MB chunks...", num_chunks);
    let chunks: Vec<Bytes> = (0..num_chunks)
        .map(|i| {
            let mut data = vec![0u8; chunk_size];
            for j in 0..chunk_size {
                data[j] = ((i + j) % 256) as u8;
            }
            Bytes::from(data)
        })
        .collect();

    // Accept connection once
    let t2_clone = Arc::clone(&transport2);
    let receiver_peer_task = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        Arc::new(peer)
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect once
    let peer1 = Arc::new(transport1.connect(actual_addr2).await.unwrap());

    // Wait for receiver to accept
    let peer2 = receiver_peer_task.await.unwrap();

    // Send all chunks in parallel
    println!("Starting {} parallel transfers...", num_chunks);
    let start = std::time::Instant::now();

    let sender_tasks: Vec<_> = chunks
        .iter()
        .map(|chunk| {
            let peer = Arc::clone(&peer1);
            let chunk = chunk.clone();
            tokio::spawn(async move {
                peer.send_chunk_verified(&ChunkSource::Memory(chunk))
                    .await
                    .unwrap()
            })
        })
        .collect();

    // Receive all chunks in parallel
    let receiver_tasks: Vec<_> = (0..num_chunks)
        .map(|_| {
            let peer = Arc::clone(&peer2);
            tokio::spawn(async move { peer.recv_chunk_verified().await.unwrap().unwrap() })
        })
        .collect();

    let sent_results = join_all(sender_tasks).await;
    let duration = start.elapsed();

    let total_mb = (num_chunks * 5) as f64;
    println!(
        "Transferred {}MB in parallel in {:?} ({:.2} MB/s aggregate)",
        total_mb,
        duration,
        total_mb / duration.as_secs_f64()
    );

    // Verify all sends succeeded
    for result in sent_results {
        assert_eq!(result.unwrap(), chunk_size as u64);
    }

    // Verify all receives completed (order may not match due to concurrent streams)
    let received_results = join_all(receiver_tasks).await;
    let mut received_set: std::collections::HashSet<_> = received_results
        .iter()
        .map(|r| r.as_ref().unwrap())
        .collect();
    for expected in &chunks {
        assert!(
            received_set.remove(expected),
            "Expected chunk not found in received data"
        );
    }
    assert!(received_set.is_empty(), "Received unexpected data");

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_parallel_transfers_to_multiple_peers() {
    let num_peers = 5;
    let chunk_size_mb = 10;

    // Create sender
    let sender_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let sender_transport = Arc::new(QuicTransport::new(sender_addr).await.unwrap());

    // Create multiple peer transports
    println!("Creating {} peers...", num_peers);
    let peer_transports: Vec<Arc<QuicTransport>> = {
        let mut transports = Vec::new();
        for _ in 0..num_peers {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            transports.push(Arc::new(QuicTransport::new(addr).await.unwrap()));
        }
        transports
    };

    let peer_addrs: Vec<SocketAddr> = peer_transports
        .iter()
        .map(|t| t.local_addr().unwrap())
        .collect();

    // Create test chunks (one per peer)
    println!("Creating {} x {}MB chunks...", num_peers, chunk_size_mb);
    let chunks: Vec<Bytes> = (0..num_peers)
        .map(|i| {
            let size = chunk_size_mb * 1024 * 1024;
            let mut data = vec![0u8; size];
            for j in 0..size {
                data[j] = ((i * 1000 + j) % 256) as u8;
            }
            Bytes::from(data)
        })
        .collect();

    // Spawn receiver on each peer
    let receiver_tasks: Vec<_> = peer_transports
        .iter()
        .map(|transport| {
            let t = Arc::clone(transport);
            tokio::spawn(async move {
                let (_, peer) = t.accept().await.unwrap();
                peer.recv_chunk_verified().await.unwrap().unwrap()
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send to all peers in parallel
    println!(
        "Sending {}MB to {} peers in parallel...",
        chunk_size_mb, num_peers
    );
    let start = std::time::Instant::now();

    let sender_tasks: Vec<_> = peer_addrs
        .iter()
        .zip(chunks.iter())
        .map(|(addr, chunk)| {
            let transport = Arc::clone(&sender_transport);
            let chunk = chunk.clone();
            let addr = *addr;
            tokio::spawn(async move {
                let peer = transport.connect(addr).await.unwrap();
                peer.send_chunk_verified(&ChunkSource::Memory(chunk))
                    .await
                    .unwrap()
            })
        })
        .collect();

    let sent_results = join_all(sender_tasks).await;
    let duration = start.elapsed();

    let total_mb = (num_peers * chunk_size_mb) as f64;
    println!(
        "Transferred {}MB total across {} peers in {:?} ({:.2} MB/s aggregate)",
        total_mb,
        num_peers,
        duration,
        total_mb / duration.as_secs_f64()
    );

    // Verify all sends
    for result in sent_results {
        assert_eq!(result.unwrap(), (chunk_size_mb * 1024 * 1024) as u64);
    }

    // Verify all receives
    let received_results = join_all(receiver_tasks).await;
    for (i, result) in received_results.iter().enumerate() {
        let received = result.as_ref().unwrap();
        assert_eq!(received.len(), chunks[i].len());
        assert_eq!(received, &chunks[i]);
    }

    // Cleanup
    sender_transport.close();
    for transport in peer_transports {
        transport.close();
    }
}

#[tokio::test]
async fn test_high_concurrency_stress() {
    let num_concurrent = 20; // 20 concurrent transfers
    let chunk_size = 2 * 1024 * 1024; // 2MB each

    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    println!(
        "Stress test: {} concurrent transfers of 2MB each",
        num_concurrent
    );

    // Create chunks
    let chunks: Vec<Bytes> = (0..num_concurrent)
        .map(|i| {
            let mut data = vec![0u8; chunk_size];
            for j in 0..chunk_size {
                data[j] = ((i * 100 + j) % 256) as u8;
            }
            Bytes::from(data)
        })
        .collect();

    // Accept connection once
    let t2_clone = Arc::clone(&transport2);
    let receiver_peer_task = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        Arc::new(peer)
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect once
    let peer1 = Arc::new(transport1.connect(actual_addr2).await.unwrap());

    // Wait for receiver to accept
    let peer2 = receiver_peer_task.await.unwrap();

    // Launch all transfers at once
    let start = std::time::Instant::now();

    let sender_tasks: Vec<_> = chunks
        .iter()
        .map(|chunk| {
            let peer = Arc::clone(&peer1);
            let chunk = chunk.clone();
            tokio::spawn(async move {
                peer.send_chunk_verified(&ChunkSource::Memory(chunk))
                    .await
                    .unwrap()
            })
        })
        .collect();

    // Receive all in parallel
    let receiver_tasks: Vec<_> = (0..num_concurrent)
        .map(|_| {
            let peer = Arc::clone(&peer2);
            tokio::spawn(async move { peer.recv_chunk_verified().await.unwrap().unwrap() })
        })
        .collect();

    // Wait for all to complete
    let sent_results = join_all(sender_tasks).await;
    let received_results = join_all(receiver_tasks).await;
    let duration = start.elapsed();

    let total_mb = (num_concurrent * 2) as f64;
    println!(
        "✓ {}MB across {} concurrent streams in {:?} ({:.2} MB/s)",
        total_mb,
        num_concurrent,
        duration,
        total_mb / duration.as_secs_f64()
    );

    // Verify all succeeded
    for result in sent_results {
        assert_eq!(result.unwrap(), chunk_size as u64);
    }

    // Verify all chunks received (order may not match)
    let mut received_set: std::collections::HashSet<_> = received_results
        .iter()
        .map(|r| r.as_ref().unwrap())
        .collect();
    for expected in &chunks {
        assert!(received_set.remove(expected), "Expected chunk not found");
    }
    assert!(received_set.is_empty());

    transport1.close();
    transport2.close();
}

#[tokio::test]
async fn test_burst_transfers() {
    let addr1: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let actual_addr2 = transport2.local_addr().unwrap();

    let bursts = 3;
    let chunks_per_burst = 5;
    let chunk_size = 3 * 1024 * 1024; // 3MB

    println!(
        "Burst test: {} bursts of {} x 3MB chunks",
        bursts, chunks_per_burst
    );

    let mut all_chunks = Vec::new();
    for burst in 0..bursts {
        for i in 0..chunks_per_burst {
            let mut data = vec![0u8; chunk_size];
            for j in 0..chunk_size {
                data[j] = ((burst * 1000 + i * 100 + j) % 256) as u8;
            }
            all_chunks.push(Bytes::from(data));
        }
    }

    // Accept connection once
    let t2_clone = Arc::clone(&transport2);
    let receiver_peer_task = tokio::spawn(async move {
        let (_, peer) = t2_clone.accept().await.unwrap();
        Arc::new(peer)
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect once
    let peer1 = Arc::new(transport1.connect(actual_addr2).await.unwrap());

    // Wait for receiver to accept
    let peer2 = receiver_peer_task.await.unwrap();

    let total_start = std::time::Instant::now();

    for burst_num in 0..bursts {
        println!("  Burst {}...", burst_num + 1);
        let burst_start = burst_num * chunks_per_burst;
        let burst_end = burst_start + chunks_per_burst;
        let burst_chunks = &all_chunks[burst_start..burst_end];

        // Send this burst
        let sender_tasks: Vec<_> = burst_chunks
            .iter()
            .map(|chunk| {
                let peer = Arc::clone(&peer1);
                let chunk = chunk.clone();
                tokio::spawn(async move {
                    peer.send_chunk_verified(&ChunkSource::Memory(chunk))
                        .await
                        .unwrap()
                })
            })
            .collect();

        // Receive this burst
        let receiver_tasks: Vec<_> = (0..chunks_per_burst)
            .map(|_| {
                let peer = Arc::clone(&peer2);
                tokio::spawn(async move { peer.recv_chunk_verified().await.unwrap().unwrap() })
            })
            .collect();

        join_all(sender_tasks).await;
        join_all(receiver_tasks).await;

        // Small delay between bursts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let total_duration = total_start.elapsed();
    let total_mb = (bursts * chunks_per_burst * 3) as f64;

    println!(
        "✓ Total {}MB in {} bursts completed in {:?} ({:.2} MB/s average)",
        total_mb,
        bursts,
        total_duration,
        total_mb / total_duration.as_secs_f64()
    );

    transport1.close();
    transport2.close();
}
