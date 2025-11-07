# API Documentation

## Table of Contents

1. [Getting Started](#getting-started)
2. [Runtime Configuration](#runtime-configuration)
3. [Chunk Transfer API](#chunk-transfer-api)
4. [RPC API](#rpc-api)
5. [Transport API](#transport-api)

---

## Getting Started

Add to your `Cargo.toml`:

```toml
[dependencies]
octopii = { path = "../octopii" }  # or version when published
tokio = { version = "1", features = ["rt-multi-thread"] }
bytes = "1"
```

Basic example:

```rust
use octopii::{OctopiiRuntime, OctopiiNode, Config};

fn main() {
    // Create isolated runtime with 2 worker threads
    let runtime = OctopiiRuntime::new(2);

    // Run octopii operations in isolated runtime
    runtime.block_on(async {
        let config = Config::default();
        let node = OctopiiNode::new(config).await.unwrap();

        // Your code here...
    });
}
```

---

## Runtime Configuration

### Creating an Isolated Runtime

```rust
use octopii::OctopiiRuntime;

// Minimal setup: 2 threads
let runtime = OctopiiRuntime::new(2);

// Moderate setup: 4 threads for higher concurrency
let runtime = OctopiiRuntime::new(4);

// Heavy load: 8 threads
let runtime = OctopiiRuntime::new(8);
```

**When to use how many threads**:
- 1-2 threads: Light usage, occasional transfers
- 3-4 threads: Moderate concurrent transfers (5-10 peers)
- 6-8 threads: Heavy load (20+ concurrent transfers)

### Running Code in Isolated Runtime

```rust
// Block on async code (from sync context)
let result = runtime.block_on(async {
    // All this runs in octopii's thread pool
    node.transfer_chunk(...).await
});

// Spawn background task
runtime.spawn(async {
    // This runs in background within octopii's thread pool
    loop {
        // Background processing...
    }
});
```

---

## Chunk Transfer API

### Overview

Transfer large files or memory chunks with SHA256 checksum verification and application-level ACK.

### Basic File Transfer (Single Peer)

```rust
use octopii::{ChunkSource, OctopiiNode, Config};
use std::path::PathBuf;
use std::time::Duration;

// Initialize node
let config = Config {
    listen_addr: "127.0.0.1:5000".parse().unwrap(),
    ..Default::default()
};
let node = OctopiiNode::new(config).await?;

// Prepare chunk
let file_path = PathBuf::from("/path/to/large_file.dat");
let chunk = ChunkSource::File(file_path);

// Send to single peer
let peer = "192.168.1.100:5000".parse().unwrap();
let peers = vec![peer];

let results = node.transfer_chunk_to_peers(
    chunk,
    peers,
    Duration::from_secs(300),  // 5 minute timeout
).await;

// Check results
for result in results {
    if result.success {
        println!("✓ Sent {} bytes to {} in {:?}",
            result.bytes_transferred,
            result.peer,
            result.duration
        );
    } else {
        eprintln!("✗ Failed to send to {}: {}",
            result.peer,
            result.error.unwrap_or_default()
        );
    }
}
```

### Parallel Transfer to Multiple Peers

```rust
use octopii::ChunkSource;
use bytes::Bytes;

// In-memory chunk
let data = Bytes::from(vec![0u8; 10 * 1024 * 1024]);  // 10MB
let chunk = ChunkSource::Memory(data);

// Multiple peers
let peers = vec![
    "192.168.1.100:5000".parse().unwrap(),
    "192.168.1.101:5000".parse().unwrap(),
    "192.168.1.102:5000".parse().unwrap(),
];

// Parallel transfer (3 concurrent streams)
let results = node.transfer_chunk_to_peers(
    chunk,
    peers,
    Duration::from_secs(600),
).await;

// All transfers happen in parallel!
let successful = results.iter().filter(|r| r.success).count();
println!("Successfully sent to {}/{} peers", successful, results.len());
```

### Receiving Chunks (Server Side)

```rust
use octopii::transport::QuicTransport;
use std::sync::Arc;

// Create transport
let addr = "0.0.0.0:5000".parse().unwrap();
let transport = Arc::new(QuicTransport::new(addr).await?);

// Accept connections in a loop
loop {
    let (peer_addr, peer_conn) = transport.accept().await?;
    println!("Connection from: {}", peer_addr);

    // Spawn handler for this peer
    tokio::spawn(async move {
        loop {
            match peer_conn.recv_chunk_verified().await {
                Ok(Some(chunk_data)) => {
                    println!("Received {} bytes", chunk_data.len());
                    // Process chunk_data (Bytes)
                    // Save to disk, process, etc.
                }
                Ok(None) => break,  // Connection closed
                Err(e) => {
                    eprintln!("Error receiving: {}", e);
                    break;
                }
            }
        }
    });
}
```

### File Source vs Memory Source

```rust
use std::path::PathBuf;
use bytes::Bytes;

// File Source (for large files - streams from disk)
let chunk = ChunkSource::File(PathBuf::from("/data/bigfile.dat"));
// Memory efficient: uses 64KB buffers, doesn't load whole file

// Memory Source (for data already in memory)
let data = Bytes::from(vec![1, 2, 3, 4, 5]);
let chunk = ChunkSource::Memory(data);
// Fast for smaller data, but entire chunk must fit in RAM
```

---

## RPC API

### Simple Request/Response

**Sender**:
```rust
use serde::{Serialize, Deserialize};
use bytes::Bytes;

#[derive(Serialize, Deserialize)]
struct PingRequest {
    id: u64,
    timestamp: u64,
}

// Connect to peer
let peer_addr = "192.168.1.100:5000".parse().unwrap();
let peer = transport.connect(peer_addr).await?;

// Send RPC message
let request = PingRequest {
    id: 42,
    timestamp: std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs(),
};

let message = bincode::serialize(&request)?;
peer.send(Bytes::from(message)).await?;

println!("RPC sent!");
```

**Receiver**:
```rust
#[derive(Serialize, Deserialize)]
struct PingRequest {
    id: u64,
    timestamp: u64,
}

// Accept connection
let (peer_addr, peer_conn) = transport.accept().await?;

// Receive RPC message
match peer_conn.recv().await? {
    Some(message) => {
        let request: PingRequest = bincode::deserialize(&message)?;
        println!("Received ping {} from {}", request.id, peer_addr);

        // Process and respond...
    }
    None => {
        // Connection closed
    }
}
```

### Concurrent RPC and Chunk Transfer

```rust
// Same connection, multiple concurrent operations!

let peer = transport.connect(peer_addr).await?;

// Spawn RPC handler
let peer_clone = peer.clone();  // Error: PeerConnection doesn't impl Clone!
// Instead, wrap in Arc:
let peer = Arc::new(transport.connect(peer_addr).await?);

// Spawn RPC sender
let peer_rpc = Arc::clone(&peer);
tokio::spawn(async move {
    loop {
        let msg = Bytes::from("ping");
        peer_rpc.send(msg).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
});

// Simultaneously send large chunk (doesn't block RPC!)
let chunk = ChunkSource::File(PathBuf::from("/large_file.dat"));
peer.send_chunk_verified(&chunk).await?;

// RPCs continued working throughout the chunk transfer!
```

---

## Transport API

### Creating a Transport

```rust
use octopii::transport::QuicTransport;
use std::sync::Arc;

// Bind to specific address
let transport = QuicTransport::new("127.0.0.1:5000".parse()?).await?;

// Bind to any available port
let transport = QuicTransport::new("127.0.0.1:0".parse()?).await?;
let actual_addr = transport.local_addr()?;
println!("Listening on: {}", actual_addr);

// Wrap in Arc for sharing across threads
let transport = Arc::new(transport);
```

### Client-Server Pattern

**Server**:
```rust
use octopii::transport::QuicTransport;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(QuicTransport::new("0.0.0.0:5000".parse()?).await?);
    println!("Server listening on port 5000");

    loop {
        let (peer_addr, peer_conn) = transport.accept().await?;
        println!("New connection from: {}", peer_addr);

        // Handle this peer in background
        tokio::spawn(async move {
            handle_peer(peer_conn).await;
        });
    }
}

async fn handle_peer(peer: octopii::transport::PeerConnection) {
    // Handle RPC, chunk transfers, etc.
}
```

**Client**:
```rust
use octopii::transport::QuicTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = QuicTransport::new("127.0.0.1:0".parse()?).await?;

    // Connect to server
    let server = "192.168.1.100:5000".parse()?;
    let peer = transport.connect(server).await?;

    // Use the connection
    // ...

    transport.close();
    Ok(())
}
```

### Connection Statistics

```rust
let peer = transport.connect(peer_addr).await?;

// Get connection stats
let stats = peer.stats();
println!("Bytes sent: {}", stats.udp_tx.bytes);
println!("Bytes received: {}", stats.udp_rx.bytes);
println!("Lost packets: {}", stats.path.lost_packets);

// Check if connection is closed
if peer.is_closed() {
    println!("Connection closed");
}
```

---

## Complete Example: P2P File Sharing

### Sender Node

```rust
use octopii::{OctopiiRuntime, ChunkSource};
use octopii::transport::QuicTransport;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OctopiiRuntime::new(4);

    runtime.block_on(async {
        // Setup transport
        let transport = Arc::new(QuicTransport::new("0.0.0.0:0".parse()?).await?);

        // File to send
        let file = PathBuf::from("./large_file.dat");
        let chunk = ChunkSource::File(file);

        // Target peers
        let peers = vec![
            "192.168.1.100:5000".parse()?,
            "192.168.1.101:5000".parse()?,
        ];

        // Send to each peer
        for peer_addr in peers {
            println!("Connecting to {}...", peer_addr);
            let peer = transport.connect(peer_addr).await?;

            println!("Sending file...");
            let bytes_sent = peer.send_chunk_verified(&chunk).await?;
            println!("✓ Sent {} bytes to {}", bytes_sent, peer_addr);
        }

        transport.close();
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
```

### Receiver Node

```rust
use octopii::OctopiiRuntime;
use octopii::transport::QuicTransport;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OctopiiRuntime::new(4);

    runtime.block_on(async {
        // Setup transport
        let transport = Arc::new(QuicTransport::new("0.0.0.0:5000".parse()?).await?);
        println!("Listening on port 5000...");

        // Accept connections
        let (peer_addr, peer) = transport.accept().await?;
        println!("Connection from: {}", peer_addr);

        // Receive chunk
        match peer.recv_chunk_verified().await? {
            Some(data) => {
                println!("Received {} bytes", data.len());

                // Save to disk
                let mut file = File::create("received_file.dat").await?;
                file.write_all(&data).await?;
                println!("✓ Saved to received_file.dat");
            }
            None => println!("Connection closed"),
        }

        transport.close();
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
```

---

## Type Reference

### ChunkSource

```rust
pub enum ChunkSource {
    File(PathBuf),      // Stream file from disk (memory efficient)
    Memory(Bytes),      // Use data already in memory
}
```

### TransferResult

```rust
pub struct TransferResult {
    pub peer: SocketAddr,           // Which peer
    pub success: bool,              // Transfer succeeded?
    pub bytes_transferred: u64,     // How many bytes
    pub checksum_verified: bool,    // Checksum matched?
    pub duration: Duration,         // How long it took
    pub error: Option<String>,      // Error if failed
}
```

### Config

```rust
pub struct Config {
    pub listen_addr: SocketAddr,    // Where to listen
    // Other config options...
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
        }
    }
}
```
