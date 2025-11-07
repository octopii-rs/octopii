# Internal Architecture

## Overview

Octopii is a minimal distributed file transfer and RPC system built on QUIC transport. It provides an isolated tokio runtime to avoid interfering with other performance-critical code in your application.

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Your Application                         │
│  (Uses octopii as a library with isolated runtime)          │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ├─── OctopiiRuntime (Isolated thread pool)
                      │
         ┌────────────┴────────────┐
         │     OctopiiNode         │
         │   (Main Entry Point)    │
         └────────┬────────────────┘
                  │
      ┌───────────┼───────────┐
      │           │           │
      ▼           ▼           ▼
┌──────────┐ ┌────────┐ ┌─────────┐
│   RPC    │ │  WAL   │ │ Transport│
│  Layer   │ │Storage │ │  (QUIC)  │
└──────────┘ └────────┘ └────┬─────┘
                              │
                    ┌─────────┴──────────┐
                    │  Chunk Transfer    │
                    │  (File/Memory)     │
                    │  + Checksum        │
                    └────────────────────┘
```

## Component Breakdown

### 1. Runtime Isolation

**Purpose**: Prevents octopii's async operations from interfering with your app's performance-critical code.

**Implementation**:
```
┌─────────────────────────────────────┐
│      OctopiiRuntime                 │
│                                      │
│   ┌──────────────────────────┐     │
│   │ Tokio Multi-Thread       │     │
│   │ Runtime                  │     │
│   │                          │     │
│   │  Thread 1 ┃ Thread 2    │     │
│   │  ...      ┃ ...         │     │
│   │  Thread N ┃ Thread N+1  │     │
│   └──────────────────────────┘     │
│                                      │
│   Configurable thread count          │
└─────────────────────────────────────┘
```

**Key points**:
- Dedicated thread pool separate from your app
- All async operations run within this pool
- Thread count configurable at creation
- Uses `Runtime::block_on()` for sync/async boundary

**Code location**: `src/runtime.rs`

### 2. Transport Layer (QUIC)

**Purpose**: Reliable, multiplexed, encrypted transport using UDP.

```
Client                                    Server
  │                                         │
  │  1. TLS Handshake (Self-signed)        │
  │────────────────────────────────────────>│
  │                                         │
  │  2. QUIC Connection Established        │
  │<───────────────────────────────────────>│
  │                                         │
  │  3a. Stream 1: RPC Call                │
  │──────────────────────>                 │
  │                                         │
  │  3b. Stream 2: Chunk Transfer          │
  │──────────────────────────────────────> │
  │                       (Concurrent!)     │
  │  3c. Stream 3: Another RPC             │
  │──────────────────────>                 │
  │                                         │
```

**Key Features**:
- Stream multiplexing: Multiple transfers don't block each other
- Connection pooling: Reuses connections to same peer
- TLS 1.3 encryption (self-signed certs for simplicity)
- ALPN protocol: "octopii"

**Code location**: `src/transport/mod.rs`, `src/transport/tls.rs`, `src/transport/peer.rs`

### 3. Chunk Transfer Protocol

**Purpose**: Transfer large files (1GB+) with checksum verification and application-level ACK.

```
Sender                                Receiver
  │                                     │
  │  1. [8 bytes: size]                │
  │────────────────────────────────────>│
  │                                     │
  │  2. [64KB buffer chunk 1]          │
  │────────────────────────────────────>│
  │                                     │  Compute
  │  3. [64KB buffer chunk 2]          │  SHA256
  │────────────────────────────────────>│  incrementally
  │                                     │
  │  ...  (N chunks)                   │
  │                                     │
  │  N. [Remaining bytes]              │
  │────────────────────────────────────>│
  │                                     │
  │  N+1. [32 bytes: SHA256 hash]      │
  │────────────────────────────────────>│
  │                                     │  Verify
  │                                     │  checksum
  │  N+2. [1 byte: status]             │
  │<────────────────────────────────────│
  │     0 = OK                          │
  │     1 = Checksum mismatch           │
  │     2 = Error                       │
```

**Buffer Strategy**:
- 64KB buffers for streaming
- Avoids loading entire file into memory
- Incremental checksum computation (no double-pass)

**Sources**:
- `ChunkSource::File(PathBuf)` - Stream from disk
- `ChunkSource::Memory(Bytes)` - Already in memory

**Code location**: `src/chunk.rs`, `src/transport/peer.rs` (methods: `send_chunk_verified`, `recv_chunk_verified`)

### 4. RPC Layer

**Purpose**: Simple request/response messaging with bincode serialization.

```
┌─────────────────────────────────────────┐
│            RPC Message                  │
│                                          │
│  [4 bytes: length] [message bytes]      │
│                                          │
│  Serialization: bincode                 │
│  (Any type implementing Serialize)       │
└─────────────────────────────────────────┘

Flow:
  1. Sender serializes message to bytes
  2. Sends [length][message]
  3. Waits for [ACK]
  4. Receiver reads length
  5. Reads exact bytes
  6. Deserializes
  7. Sends ACK
```

**Code location**: `src/rpc/mod.rs`, `src/transport/peer.rs` (methods: `send`, `recv`)

### 5. Write-Ahead Log (WAL)

**Purpose**: Durable storage with batched writes for performance.

```
┌───────────────────────────────────┐
│         WAL Structure             │
│                                   │
│  [Entry 1][Entry 2][Entry 3]...  │
│                                   │
│  Each entry:                      │
│    [4 bytes: length]              │
│    [entry data]                   │
└───────────────────────────────────┘

Write Strategy:
  - Batches writes every 100ms
  - Reduces fsync calls
  - Buffered writes for performance
```

**Code location**: `src/wal/mod.rs`

### 6. Raft Integration

**Purpose**: State machine replication using raft-rs (currently stub).

```
┌─────────┐     ┌─────────┐     ┌─────────┐
│ Node 1  │────>│ Node 2  │<───>│ Node 3  │
│(Leader) │     │(Follower)│    │(Follower)│
└─────────┘     └─────────┘     └─────────┘
     │               │               │
     │               │               │
     ▼               ▼               ▼
  [WAL]          [WAL]          [WAL]
```

**Code location**: `src/raft/mod.rs` (minimal implementation)

## Data Flow Examples

### Example 1: Chunk Transfer to Multiple Peers

```
Application
    │
    │ transfer_chunk_to_peers(chunk, [peer1, peer2, peer3])
    │
    ▼
OctopiiNode
    │
    │ Spawns 3 parallel tasks
    ├─────────┬─────────┬─────────┐
    ▼         ▼         ▼         ▼
  Peer1    Peer2    Peer3      ...
  [64KB]   [64KB]   [64KB]    (concurrent)
  [64KB]   [64KB]   [64KB]
   ...      ...      ...
  [SHA256] [SHA256] [SHA256]
  [ACK]    [ACK]    [ACK]
    │        │        │
    └────────┴────────┴───> Results aggregated
                             │
                             ▼
                      Application receives
                      Vec<TransferResult>
```

### Example 2: RPC + Chunk Transfer on Same Connection

```
Time ──────────────────────────────────────>

Connection to Peer:
├─ Stream 1: RPC Call ───────────────> [ACK]
├─ Stream 2: 100MB Chunk ──────────────────────────────────────────> [ACK]
│               (doesn't block RPC!)
├─ Stream 3: RPC Call ───────────────> [ACK]
└─ Stream 4: Another RPC ────────────> [ACK]

All concurrent, no blocking!
```

## Thread Safety

- **QuicTransport**: Wrapped in `Arc` for shared ownership
- **PeerConnection**: Uses quinn's `Connection` (internally Arc-wrapped)
- **Channels**: Used for cross-thread communication
- **Mutex**: Used sparingly (WAL batching)

## Performance Considerations

1. **Memory Efficiency**:
   - 64KB streaming buffers (not full file)
   - `bytes::Bytes` for zero-copy
   - Controlled allocation (cap at 10MB for receive buffer)

2. **Network Efficiency**:
   - Connection pooling
   - Stream multiplexing
   - Single ACK per chunk (not per buffer)

3. **CPU Efficiency**:
   - Incremental SHA256 (computed during streaming)
   - Batched WAL writes (reduced fsync)
   - Isolated runtime (doesn't steal from app threads)

## Error Handling

All operations return `Result<T, OctopiiError>`:

```
OctopiiError
├─ Io(std::io::Error)
├─ QuicConnection(quinn::ConnectionError)
├─ QuicTransport(quinn::TransportError)
├─ Serialization(bincode::Error)
├─ Transport(String)
└─ Wal(String)
```

**Strategy**: Fail fast, propagate errors to caller, no silent failures.
