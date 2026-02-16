use bytes::Bytes;
use octopii::transport::QuicTransport;
use octopii::BlobStore;
use octopii::ChunkSource;
use std::error::Error;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("=== Blob Store Dedup Example ===\n");

    // Setup sender and receiver transports
    let sender = Arc::new(QuicTransport::new("127.0.0.1:0".parse()?).await?);
    let receiver = Arc::new(QuicTransport::new("127.0.0.1:0".parse()?).await?);
    let receiver_addr = receiver.local_addr()?;

    // Receiver has a BlobStore
    let store_dir = tempfile::tempdir()?;
    let store = Arc::new(BlobStore::new(store_dir.path())?);

    println!("Sender:   {}", sender.local_addr()?);
    println!("Receiver: {}", receiver_addr);
    println!("Store:    {}\n", store_dir.path().display());

    // Create a 1MB payload
    let payload = Bytes::from(vec![0xAB; 1024 * 1024]);
    println!("Payload size: {} bytes\n", payload.len());

    // Spawn receiver task that accepts and receives twice
    let recv_handle = {
        let receiver = Arc::clone(&receiver);
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            let (addr, peer) = receiver.accept().await?;
            println!("[Receiver] Accepted connection from {}", addr);

            // First receive
            println!("[Receiver] Waiting for first chunk...");
            let hash1 = peer.recv_chunk_dedup(&store).await?;
            println!("[Receiver] First chunk received: {:?}", hash1.map(hex));

            // Second receive
            println!("[Receiver] Waiting for second chunk...");
            let hash2 = peer.recv_chunk_dedup(&store).await?;
            println!("[Receiver] Second chunk received: {:?}", hash2.map(hex));

            Ok::<_, octopii::OctopiiError>((hash1, hash2))
        })
    };

    // Give receiver time to start accepting
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Connect sender to receiver
    let peer = sender.connect(receiver_addr).await?;
    println!("[Sender] Connected to receiver\n");

    // First send - should transfer data
    println!("[Sender] Sending first chunk...");
    let (bytes1, needed1) = peer
        .send_chunk_dedup(ChunkSource::Memory(payload.clone()))
        .await?;
    println!("[Sender] First send complete:");
    println!("         bytes_transferred: {}", bytes1);
    println!("         was_needed: {}\n", needed1);

    // Second send - should be deduped
    println!("[Sender] Sending second chunk (same data)...");
    let (bytes2, needed2) = peer
        .send_chunk_dedup(ChunkSource::Memory(payload.clone()))
        .await?;
    println!("[Sender] Second send complete:");
    println!("         bytes_transferred: {}", bytes2);
    println!("         was_needed: {}\n", needed2);

    // Wait for receiver
    let (hash1, hash2) = recv_handle.await??;

    // Summary
    println!("=== Summary ===");
    println!("First transfer:  {} bytes sent (needed: {})", bytes1, needed1);
    println!("Second transfer: {} bytes sent (needed: {})", bytes2, needed2);
    println!("Bandwidth saved: {} bytes", payload.len());
    println!("Hashes match:    {}", hash1 == hash2);

    // Verify data in store
    if let Some(hash) = hash1 {
        let stored = store.get(&hash)?;
        println!("Data integrity:  {}", stored == payload.as_ref());
    }

    sender.close();
    receiver.close();

    Ok(())
}

fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|b| format!("{:02x}", b)).collect::<String>()[..16].to_string() + "..."
}
