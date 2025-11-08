# RPC and Transport Architecture

This document explains how Octopii's bidirectional RPC system and chunk transfer protocol work over QUIC.

## Table of Contents
- [Overview](#overview)
- [QUIC Transport Layer](#quic-transport-layer)
- [Bidirectional RPC Protocol](#bidirectional-rpc-protocol)
- [Chunk Transfer Protocol](#chunk-transfer-protocol)
- [Architectural Decisions](#architectural-decisions)

## Overview

Octopii uses **Quinn QUIC** for all network communication. QUIC provides:
- **Multiplexed streams**: Multiple concurrent streams over a single connection
- **Built-in encryption**: TLS 1.3 by default
- **Connection pooling**: Reuse connections to the same peer
- **Low latency**: 0-RTT connection resumption

## QUIC Transport Layer

### Connection Architecture

```
┌─────────────┐                    ┌─────────────┐
│   Node A    │                    │   Node B    │
│             │                    │             │
│  Transport  │◄──── Connection ───►│  Transport  │
│   Endpoint  │       (pooled)     │   Endpoint  │
│             │                    │             │
└─────────────┘                    └─────────────┘
       │                                  │
       │  Bi-Stream 1: RPC Request       │
       │───────────────────────────────►  │
       │                                  │
       │  ◄─────────────────────────────  │
       │    Bi-Stream 2: RPC Response    │
       │                                  │
       │  Bi-Stream 3: Chunk Transfer    │
       │───────────────────────────────►  │
```

### Key Components

#### QuicTransport
- **Purpose**: Manages QUIC endpoint and connection pool
- **Location**: `src/transport/mod.rs`
- **Connection Pooling**: One connection per peer address

```rust
pub struct QuicTransport {
    endpoint: Endpoint,
    connections: Arc<RwLock<HashMap<SocketAddr, PeerConnection>>>,
}
```

#### PeerConnection
- **Purpose**: Wrapper around Quinn `Connection` for bi-directional streams
- **Location**: `src/transport/peer.rs`
- **Operations**: `send()`, `recv()`, `send_chunk_verified()`, `recv_chunk_verified()`

```rust
pub struct PeerConnection {
    connection: Connection,  // Quinn QUIC connection
}
```

## Bidirectional RPC Protocol

### RPC Message Flow (Request-Response)

The RPC protocol uses **bidirectional QUIC streams** for request-response communication:

```
Client                                          Server
  │                                               │
  │  1. Connect to server (if not connected)      │
  ├──────────────── QUIC Handshake ──────────────►│
  │                                               │
  │  2. Client spawns recv loop on connection     │
  │     to handle incoming bi-streams             │
  │                                               │
  │  3. Server accepts connection and spawns      │
  │     recv loop to handle incoming bi-streams   │
  │                                               │
  │  4. Client opens bi-stream #1                 │
  │     and sends RPC request                     │
  ├────────────────► [Bi-Stream #1] ─────────────►│
  │                  { Request }                  │
  │                                               │
  │  5. Server recv loop receives request         │
  │                                               │
  │  6. Server processes request inline           │
  │     (no spawned task!)                        │
  │                                               │
  │  7. Server opens bi-stream #2 on SAME         │
  │     connection and sends response             │
  │ ◄──────────────── [Bi-Stream #2] ─────────────┤
  │                  { Response }                 │
  │                                               │
  │  8. Client recv loop receives response        │
  │                                               │
  │  9. Response routed to waiting request()      │
  │     via oneshot channel                       │
  │                                               │
  │ 10. request() returns response to caller      │
  │                                               │
```

### Critical Architectural Detail

**Why responses go via a separate bi-stream instead of the same bi-stream:**

The initial design had responses going back on the same bi-stream as the request. However, this created coupling between `send()` and `recv()` operations. The current design uses **separate bi-streams** for requests and responses, which provides:

1. **Simplicity**: Each bi-stream has one clear purpose
2. **Flexibility**: Can send multiple requests without waiting for responses
3. **Compatibility**: Works with existing `peer.send()` and `peer.recv()` primitives

### RPC Handler Architecture

```
┌───────────────────────────────────────────────────────┐
│                    RpcHandler                         │
│                                                       │
│  ┌─────────────────────────────────────────────────┐ │
│  │  pending_requests:                              │ │
│  │    HashMap<MessageId, oneshot::Sender>          │ │
│  └─────────────────────────────────────────────────┘ │
│                                                       │
│  async fn notify_message(msg, peer)                  │
│     │                                                │
│     ├──► Request?  ──► handle_request(peer)         │
│     │                      │                         │
│     │                      └──► peer.send(response)  │
│     │                                                │
│     └──► Response? ──► handle_response()            │
│                            │                         │
│                            └──► Send via oneshot     │
│                                 to waiting request() │
└───────────────────────────────────────────────────────┘
```

### Key Design: Inline Message Processing

**CRITICAL**: `notify_message()` is now **async** and processes messages **inline** instead of queuing to a channel and spawning a background task.

**Before (Broken - caused starvation):**
```rust
// OLD DESIGN - DO NOT USE
pub fn notify_message(&self, addr: SocketAddr, msg: RpcMessage) {
    // Send to channel
    self.message_tx.send((addr, msg)).unwrap();
}

// Background task (spawned in new())
async fn message_receiver_task(&self, rx: Receiver) {
    while let Some((addr, msg)) = rx.recv().await {
        self.handle_request(addr, req).await;  // ← HANGS HERE
        self.transport.send(addr, response).await;
    }
}
```

**Why this caused starvation:**
- Spawning `message_receiver_task` creates a task trampoline
- Quinn's connection driver needs to be polled to make progress
- The spawned task prevents proper cooperative scheduling
- Result: `transport.send()` hangs waiting for Quinn drivers

**After (Fixed - direct await chain):**
```rust
// NEW DESIGN - WORKS CORRECTLY
pub async fn notify_message(&self, addr: SocketAddr, msg: RpcMessage, peer: Option<Arc<PeerConnection>>) {
    match msg {
        RpcMessage::Request(req) => {
            self.handle_request(addr, req, peer).await;  // Direct .await
        }
        RpcMessage::Response(resp) => {
            self.handle_response(resp).await;  // Direct .await
        }
    }
}
```

**Why this works:**
- No spawned background task
- Direct `.await` chain allows Quinn drivers to be polled
- Responses sent via `peer.send()` instead of creating new connection
- Proper cooperative scheduling with tokio runtime

## Chunk Transfer Protocol

### Verified Chunk Transfer

The chunk transfer protocol provides **cryptographic verification** using SHA-256 checksums:

```
Sender                                              Receiver
  │                                                    │
  │  1. Open bi-directional stream                    │
  ├──────────────────────────────────────────────────►│
  │                                                    │
  │  2. Send chunk metadata                           │
  ├─────────► [8 bytes: chunk size] ─────────────────►│
  │                                                    │
  │  3. Stream chunk data (64KB buffers)              │
  ├─────────► [N bytes: chunk data] ─────────────────►│
  │           (compute SHA-256 while streaming)       │
  │                                                    │ (compute SHA-256
  │                                                    │  while receiving)
  │  4. Send checksum                                 │
  ├─────────► [32 bytes: SHA-256 hash] ──────────────►│
  │                                                    │
  │  5. Wait for verification result                  │ ──► Compare checksums
  │                                                    │
  │  6. Receive ACK                                   │
  │ ◄───────── [1 byte: 0=OK, 1=fail, 2=error] ──────┤
  │                                                    │
  │  7. Return bytes_transferred or error             │
  │                                                    │
```

### Chunk Transfer Implementation

**Location**: `src/transport/peer.rs`

```rust
pub async fn send_chunk_verified(&self, chunk: &ChunkSource) -> Result<u64> {
    const BUFFER_SIZE: usize = 64 * 1024; // 64KB buffer

    let (mut send_stream, mut recv_stream) = self.connection.open_bi().await?;

    // Send size
    send_stream.write_all(&size.to_le_bytes()).await?;

    // Stream data and compute checksum
    let mut hasher = Sha256::new();
    // ... read from ChunkSource::File or ChunkSource::Memory
    // ... write to stream while updating hasher

    // Send checksum
    send_stream.write_all(&checksum).await?;

    // Wait for verification result
    let mut ack_buf = [0u8; 1];
    recv_stream.read_exact(&mut ack_buf).await?;

    match ack_buf[0] {
        0 => Ok(size),  // Success
        1 => Err("Checksum failed"),
        _ => Err("Unknown error"),
    }
}
```

### Parallel Chunk Transfers

QUIC's **stream multiplexing** enables massive parallelism:

```
Sender                                              Receiver
  │                                                    │
  │         Single QUIC Connection                    │
  │◄──────────────────────────────────────────────────►│
  │                                                    │
  │  Stream 1: Chunk A (5MB) ─────────────────────────►│
  │  Stream 2: Chunk B (5MB) ─────────────────────────►│
  │  Stream 3: Chunk C (5MB) ─────────────────────────►│
  │      ...                                           │
  │  Stream N: Chunk N (5MB) ─────────────────────────►│
  │                                                    │
  │  All streams concurrent, no head-of-line blocking │
  │                                                    │
```

**Example**: Transferring 10 x 5MB chunks in parallel achieves ~50MB total with near-linear scaling on a single connection!

See `tests/chunk_parallel_stress_test.rs` for benchmarks.

## Architectural Decisions

### Why Bidirectional Streams?

**Advantages:**
1. **Reduced latency**: Responses go back on same connection
2. **Connection reuse**: No need to establish new connections
3. **Simpler state**: Single connection per peer
4. **Better performance**: Quinn optimized for this pattern

**Disadvantages:**
1. **Complexity**: Both sides need recv loops
2. **Ordering**: Responses may arrive out of order (handled by request IDs)

### Why Inline Processing (No Spawned Tasks)?

**Critical for Quinn QUIC compatibility:**

Quinn requires the **endpoint and connection drivers** to be polled regularly. Spawning background tasks that do I/O creates a task trampoline that prevents proper cooperative scheduling.

**Best practice**:
```rust
// ✅ GOOD: Direct await chain
async fn handle_message(&self, msg: Message, peer: Arc<PeerConnection>) {
    let response = self.process(msg).await;
    peer.send(response).await;  // Yields to scheduler
}

// ❌ BAD: Spawned task doing I/O
fn handle_message(&self, msg: Message) {
    tokio::spawn(async move {
        let response = self.process(msg).await;
        transport.send(addr, response).await;  // May hang!
    });
}
```

See [Quinn Issue #867](https://github.com/quinn-rs/quinn/issues/867) for details.

### Why Pass Peer to notify_message()?

The `peer` parameter allows responses to be sent back on the **same connection** that received the request:

```rust
pub async fn notify_message(
    &self,
    addr: SocketAddr,
    msg: RpcMessage,
    peer: Option<Arc<PeerConnection>>  // ← Send response via this peer
)
```

**Without peer**: Server tries to create NEW connection to client → hangs
**With peer**: Server sends response via existing connection → works

### Thread Model

**Multi-threaded runtime required:**

Quinn QUIC requires a **multi-threaded tokio runtime** to avoid starvation:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc_request_response() {
    // Single-threaded runtime would cause deadlock!
}
```

**Why?** Quinn's I/O drivers need to run concurrently with application tasks.

### Connection Lifecycle

```
Client                          Server
  │                               │
  │  transport.connect(addr)      │
  ├──────────────────────────────►│  transport.accept()
  │       QUIC Handshake          │        │
  │◄──────────────────────────────┤        │
  │                               │        │
  │  Store in connection pool     │  Store PeerConnection
  │                               │        │
  │  Spawn recv loop ────┐        │  Spawn recv loop
  │                      │        │        │
  │                      ▼        │        ▼
  │  ┌────────────────────────┐   │  ┌────────────────────────┐
  │  │ while peer.recv()      │   │  │ while peer.recv()      │
  │  │   notify_message(msg)  │   │  │   notify_message(msg)  │
  │  └────────────────────────┘   │  └────────────────────────┘
  │                               │
  │  Both sides can send/recv     │
  │  on same connection           │
  │                               │
```

## Testing Patterns

### Proper RPC Test Setup

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc() {
    let transport1 = Arc::new(QuicTransport::new(addr1).await.unwrap());
    let transport2 = Arc::new(QuicTransport::new(addr2).await.unwrap());

    let rpc1 = Arc::new(RpcHandler::new(Arc::clone(&transport1)));
    let rpc2 = Arc::new(RpcHandler::new(Arc::clone(&transport2)));

    // Server: spawn acceptor and recv loop
    tokio::spawn(async move {
        loop {
            if let Ok((addr, peer)) = transport2.accept().await {
                let peer = Arc::new(peer);
                tokio::spawn(async move {
                    while let Ok(Some(data)) = peer.recv().await {
                        if let Ok(msg) = deserialize(&data) {
                            rpc2.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                        }
                    }
                });
            }
        }
    });

    // Client: connect and spawn recv loop
    let client_peer = Arc::new(transport1.connect(addr2).await.unwrap());
    tokio::spawn(async move {
        while let Ok(Some(data)) = client_peer.recv().await {
            if let Ok(msg) = deserialize(&data) {
                rpc1.notify_message(addr2, msg, Some(Arc::clone(&client_peer))).await;
            }
        }
    });

    // Now RPC works bidirectionally!
    let response = rpc1.request(addr2, payload, timeout).await.unwrap();
}
```

## Performance Characteristics

### RPC Latency
- **Local loopback**: ~1-2ms per request-response
- **Network**: Depends on RTT + processing time
- **Concurrent requests**: Limited only by QUIC stream limits (~100s concurrent)

### Chunk Transfer Throughput
- **Single chunk**: ~100-200 MB/s (local loopback)
- **Parallel chunks (10x)**: ~500-1000 MB/s aggregate (local loopback)
- **Network**: Limited by bandwidth and QUIC congestion control

### Memory Usage
- **Connection overhead**: ~50KB per connection
- **Stream overhead**: ~1KB per stream
- **Chunk buffers**: 64KB per concurrent transfer

## Future Improvements

1. **Connection migration**: QUIC supports seamless handoff between networks
2. **0-RTT resumption**: Reduce reconnection latency
3. **Adaptive chunk sizes**: Tune based on network conditions
4. **Flow control**: Backpressure for slow receivers
5. **Stream prioritization**: Prioritize critical RPCs over bulk transfers
