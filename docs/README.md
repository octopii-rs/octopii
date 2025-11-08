# Octopii Documentation

Welcome to the octopii documentation.

## Quick Links

- **[API Reference](API.md)** - How to use octopii in your application
- **[Architecture](ARCHITECTURE.md)** - Internal design and how things work
- **[Raft Consensus](RAFT.md)** - Raft integration and usage
- **[Write-Ahead Log](WAL.md)** - WAL/Walrus usage in Octopii
- **[RPC & Transport](RPC_AND_TRANSPORT.md)** - Network layer details
- **[Limitations](LIMITATIONS.md)** - Current constraints and known issues

## What is Octopii?

Octopii is a minimal distributed file transfer and RPC system built on QUIC transport. It provides:

- **Isolated Runtime**: Dedicated tokio thread pool that won't interfere with your app
- **Chunk Transfer**: Send large files (1GB+) with SHA256 verification and application-level ACK
- **Simple RPC**: Request/response messaging with bincode serialization
- **QUIC Transport**: Multiplexed streams, connection pooling, TLS 1.3 encryption
- **Durable Storage**: Production-grade WAL (Walrus) with crash recovery and topic isolation
- **Raft Integration**: Distributed consensus with fully durable state machine and storage

## Getting Started

1. Read [API.md](API.md) for code examples
2. Check [ARCHITECTURE.md](ARCHITECTURE.md) to understand how it works
3. Review [LIMITATIONS.md](LIMITATIONS.md) to know what's supported

## Key Features

### Isolated Thread Pool

```rust
let runtime = OctopiiRuntime::new(4);  // 4 worker threads
runtime.block_on(async {
    // All operations run in isolated pool
});
```

### Large File Transfer

```rust
let chunk = ChunkSource::File(PathBuf::from("bigfile.dat"));
let results = node.transfer_chunk_to_peers(chunk, peers, timeout).await;
```

### Parallel Transfers

```rust
// Sends to all peers concurrently
let peers = vec![peer1, peer2, peer3];
node.transfer_chunk_to_peers(chunk, peers, timeout).await;
```

### Stream Multiplexing

```rust
// RPC and chunk transfer don't block each other
peer.send(rpc_message).await;          // Fast!
peer.send_chunk_verified(&chunk).await; // Concurrent!
```

## Documentation Structure

```
docs/
├── README.md             (This file)
├── API.md                (How to use octopii)
├── ARCHITECTURE.md       (How octopii works)
├── RAFT.md               (Raft consensus integration)
├── WAL.md                (Write-Ahead Log / Walrus)
├── RPC_AND_TRANSPORT.md  (Network layer)
└── LIMITATIONS.md        (What doesn't work yet)
```

## When to Use Octopii

**Good for**:
- P2P file sharing in trusted networks
- Internal data transfer between services
- Large file synchronization
- Chunk distribution in distributed systems

**Not good for**:
- Public internet (no real authentication)
- Production systems (alpha quality)
- Mission-critical transfers (no resume support)

## Performance Characteristics

- **Memory**: 64KB buffers for file streaming (sender), full chunk in RAM (receiver)
- **CPU**: SHA256 checksum computed incrementally
- **Network**: QUIC multiplexing allows concurrent transfers
- **Throughput**: Tested up to 100MB chunks, 20 concurrent streams

See [LIMITATIONS.md](LIMITATIONS.md) for details.
