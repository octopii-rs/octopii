# RPC and Transport Architecture

This document explains how Octopii's RPC system and chunk transfer protocol work over QUIC.

## Table of Contents
- [QUIC Transport Basics](#quic-transport-basics)
- [Bi-Stream Protocol (send/recv)](#bi-stream-protocol-sendrecv)
- [RPC Request-Response Pattern](#rpc-request-response-pattern)
- [Chunk Transfer Protocol](#chunk-transfer-protocol)
- [Architectural Decisions](#architectural-decisions)
- [Quinn QUIC Compatibility](#quinn-quic-compatibility)

## QUIC Transport Basics

### What is QUIC?

QUIC is a modern transport protocol that provides:
- **Multiple concurrent streams** over a single UDP connection
- **Built-in TLS 1.3 encryption** (no plaintext)
- **Connection migration** (can change IP/port without reconnecting)
- **0-RTT connection resumption** (fast reconnects)

### Connection Pooling

Octopii maintains **one QUIC connection per peer address**:

```
Node A (127.0.0.1:5000)          Node B (127.0.0.1:6000)
        │                                  │
        │        QUIC Connection           │
        │◄────────────────────────────────►│
        │      (pooled, reused)            │
        │                                  │
        │  Stream 1 ──────────────────────►│
        │  Stream 2 ──────────────────────►│
        │  Stream 3 ◄──────────────────────│
        │     ...                          │
```

**Key point**: Connections are pooled and reused. Streams are NOT - each `send()` or `recv()` opens a NEW bi-stream.

**Code**: `src/transport/mod.rs` - `QuicTransport.connections` HashMap

## Bi-Stream Protocol (send/recv)

### What is a Bi-Stream?

A **bi-directional stream** has two independent halves:
- **Send half**: Write data, close with `finish()`
- **Recv half**: Read data until EOF

Each `send()` and `recv()` operation opens a **NEW** bi-stream. The bi-directional nature is used ONLY for transport-level ACKs, not for RPC request-response correlation.

### peer.send() - Opens NEW Bi-Stream

**Source**: `src/transport/peer.rs:23-36`

```rust
pub async fn send(&self, data: Bytes) -> Result<()> {
    // 1. Open NEW bi-stream
    let (mut send, mut recv) = self.connection.open_bi().await?;

    // 2. Send framed message: [4-byte length][data]
    let len = data.len() as u32;
    send.write_all(&len.to_le_bytes()).await?;
    send.write_all(&data).await?;
    send.finish()?;  // Close send half

    // 3. Wait for ACK on recv half (empty read = ACK)
    let _ = recv.read_to_end(0).await?;

    Ok(())
}
```

**Protocol**:
```
Sender                           Receiver
  │                                 │
  │  Open bi-stream #N              │
  ├────────────────────────────────►│
  │                                 │
  │  Send: [4 bytes: len][data]    │
  ├────────────────────────────────►│
  │                                 │
  │  Close send half                │
  │                                 │
  │  Wait for ACK...                │
  │                                 │
  │◄────────────────────────────────┤
  │  Recv: EOF (empty = ACK)        │
  │                                 │
  │  send() returns Ok(())          │
  │                                 │
```

### peer.recv() - Accepts NEW Bi-Stream

**Source**: `src/transport/peer.rs:41-62`

```rust
pub async fn recv(&self) -> Result<Option<Bytes>> {
    // 1. Accept NEW bi-stream (blocks until sender opens one)
    let (mut send, mut recv) = self.connection.accept_bi().await?;

    // 2. Read framed message: [4-byte length][data]
    let mut len_buf = [0u8; 4];
    recv.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    let mut data = BytesMut::with_capacity(len);
    data.resize(len, 0);
    recv.read_exact(&mut data).await?;

    // 3. Send ACK by closing send half (empty write = ACK)
    send.finish()?;

    Ok(Some(data.freeze()))
}
```

**Protocol**:
```
Receiver                         Sender
  │                                 │
  │  Accept bi-stream (blocks)      │
  │◄────────────────────────────────┤
  │                                 │
  │  Recv: [4 bytes: len][data]    │
  │◄────────────────────────────────┤
  │                                 │
  │  Close send half (ACK)          │
  ├────────────────────────────────►│
  │                                 │
  │  recv() returns Ok(Some(data))  │
  │                                 │
```

### Key Insight: Each Message = New Bi-Stream

**NOT bidirectional RPC!** Each message opens a separate bi-stream:

```
Connection (reused)
├─ Bi-Stream 1: Client sends message A ────► Server
│                Client waits for ACK  ◄──── Server sends ACK
│
├─ Bi-Stream 2: Server sends message B ────► Client
│                Server waits for ACK  ◄──── Client sends ACK
│
├─ Bi-Stream 3: Client sends message C ────► Server
│                ...
```

The "bi" in bi-stream is for **ACKs only**, not for RPC response correlation.

## RPC Request-Response Pattern

### Overview

RPC request-response uses **TWO separate bi-streams**:
1. Client opens bi-stream #1, sends request, gets ACK
2. Server opens bi-stream #2, sends response, gets ACK

Request and response correlation is handled by **request IDs** in the message payload, not by stream reuse.

### RPC Message Flow

**Source**: `src/rpc/handler.rs`, `tests/rpc_test.rs`

```
Client                                          Server
  │                                               │
  │  1. Both sides spawn recv loops               │
  │                                               │
  │  Client: tokio::spawn(                        │
  │    while peer.recv() { notify_message() }     │
  │  )                                            │
  │                                               │
  │  Server: tokio::spawn(                        │
  │    while peer.recv() { notify_message() }     │
  │  )                                            │
  │                                               │
  │  2. Client calls rpc.request()                │
  │     - Generates request ID: 1                 │
  │     - Stores oneshot channel in HashMap       │
  │                                               │
  │  3. Client calls peer.send(request)           │
  │     - Opens bi-stream #1                      │
  ├────────── [Bi-Stream #1] ────────────────────►│
  │     Request { id: 1, payload: ... }          │
  │                                               │
  │◄────────────── [ACK] ──────────────────────────┤
  │                                               │
  │  4. Server's recv loop gets request           │
  │     - recv() accepts bi-stream #1             │
  │     - Deserializes RpcMessage                 │
  │     - Calls notify_message(msg, peer)         │
  │                                               │
  │  5. notify_message() calls handle_request()   │
  │     - Processes request inline (NO spawn!)    │
  │     - Calls request_handler callback          │
  │     - Creates response: Response { id: 1 }    │
  │                                               │
  │  6. Server calls peer.send(response)          │
  │     - Opens bi-stream #2                      │
  │◄────────── [Bi-Stream #2] ────────────────────┤
  │     Response { id: 1, payload: ... }         │
  │                                               │
  ├───────────────► [ACK] ────────────────────────►│
  │                                               │
  │  7. Client's recv loop gets response          │
  │     - recv() accepts bi-stream #2             │
  │     - Deserializes RpcMessage                 │
  │     - Calls notify_message(msg, peer)         │
  │                                               │
  │  8. notify_message() calls handle_response()  │
  │     - Looks up request ID in HashMap          │
  │     - Sends response via oneshot channel      │
  │                                               │
  │  9. request() receives response and returns   │
  │                                               │
```

### Critical Implementation Details

**1. Both sides need recv loops**

Each side must spawn a recv loop to accept incoming bi-streams:

```rust
// Client recv loop (accepts responses)
let client_peer = Arc::new(transport.connect(server_addr).await?);
tokio::spawn(async move {
    while let Ok(Some(data)) = client_peer.recv().await {
        if let Ok(msg) = deserialize(&data) {
            rpc.notify_message(server_addr, msg, Some(Arc::clone(&client_peer))).await;
        }
    }
});

// Server recv loop (accepts requests)
tokio::spawn(async move {
    loop {
        if let Ok((addr, peer)) = transport.accept().await {
            let peer = Arc::new(peer);
            tokio::spawn(async move {
                while let Ok(Some(data)) = peer.recv().await {
                    if let Ok(msg) = deserialize(&data) {
                        rpc.notify_message(addr, msg, Some(Arc::clone(&peer))).await;
                    }
                }
            });
        }
    }
});
```

**2. notify_message() is async and handles messages inline**

**Source**: `src/rpc/handler.rs:107-127`

```rust
pub async fn notify_message(
    &self,
    addr: SocketAddr,
    msg: RpcMessage,
    peer: Option<Arc<PeerConnection>>  // Used to send response
) {
    match msg {
        RpcMessage::Request(req) => {
            self.handle_request(addr, req, peer).await;  // Inline, no spawn!
        }
        RpcMessage::Response(resp) => {
            self.handle_response(resp).await;  // Routes to waiting request()
        }
        RpcMessage::OneWay(_) => {
            // One-way messages (no response expected)
        }
    }
}
```

**3. handle_request() sends response via peer.send()**

**Source**: `src/rpc/handler.rs:130-155`

```rust
async fn handle_request(
    &self,
    addr: SocketAddr,
    req: RpcRequest,
    peer: Option<Arc<PeerConnection>>
) {
    let handler = self.request_handler.read().await;
    let response_payload = match handler.as_ref() {
        Some(h) => h(req.clone()),
        None => ResponsePayload::Error { ... },
    };

    let response = RpcMessage::new_response(req.id, response_payload);
    let data = serialize(&response)?;

    if let Some(peer) = peer {
        // Send response on SAME connection (but NEW bi-stream!)
        peer.send(data).await?;
    } else {
        // Fallback: create new connection (slow!)
        self.transport.send(addr, data).await?;
    }
}
```

**4. request() waits for response via oneshot channel**

**Source**: `src/rpc/handler.rs:56-88`

```rust
pub async fn request(
    &self,
    addr: SocketAddr,
    payload: RequestPayload,
    timeout_duration: Duration,
) -> Result<RpcResponse> {
    let id = self.next_id.fetch_add(1, Ordering::SeqCst);
    let request = RpcMessage::new_request(id, payload);

    let (tx, rx) = oneshot::channel();

    // Store oneshot sender in HashMap
    {
        let mut pending = self.pending_requests.write().await;
        pending.insert(id, tx);
    }

    // Send request
    let data = serialize(&request)?;
    self.transport.send(addr, data).await?;

    // Wait for response (from handle_response via oneshot)
    match timeout(timeout_duration, rx).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => Err(OctopiiError::Rpc("Channel closed".to_string())),
        Err(_) => Err(OctopiiError::Rpc("Timeout".to_string())),
    }
}
```

## Chunk Transfer Protocol

### Protocol Overview

Chunk transfer uses **one bi-stream** with SHA-256 verification:

**Source**: `src/transport/peer.rs:83-161` (send), `173-250` (recv)

```
Sender                                Receiver
  │                                     │
  │  1. Open bi-stream                 │
  ├────────────────────────────────────►│
  │                                     │
  │  2. Send: [8 bytes: total_size]    │
  ├────────────────────────────────────►│
  │                                     │
  │  3. Stream data in 64KB chunks     │
  ├────► [64KB buffer] ─────────────────►│  Compute
  ├────► [64KB buffer] ─────────────────►│  SHA-256
  ├────► [64KB buffer] ─────────────────►│  incrementally
  │       ...                            │
  ├────► [remaining bytes] ─────────────►│
  │                                     │
  │  4. Send: [32 bytes: SHA-256]      │
  ├────────────────────────────────────►│
  │                                     │
  │  5. Close send half                 │
  │                                     │
  │  6. Wait for verification result    │  Verify checksum
  │                                     │
  │◄────── [1 byte: status] ────────────┤
  │        0 = OK                        │
  │        1 = Checksum mismatch         │
  │        2 = Error                     │
  │                                     │
  │  send_chunk_verified() returns      │
  │  Ok(bytes_transferred)              │
  │                                     │
```

### Key Features

1. **Streaming**: 64KB buffers, doesn't load full file into memory
2. **Incremental hashing**: SHA-256 computed during streaming (no double-pass)
3. **Verification**: Receiver verifies checksum before ACKing
4. **Sources**: `ChunkSource::File(path)` or `ChunkSource::Memory(bytes)`

### Parallel Chunk Transfers

QUIC allows **multiple concurrent chunk transfers** on the same connection:

```
Sender                                Receiver
  │                                     │
  │      Single QUIC Connection         │
  │◄───────────────────────────────────►│
  │                                     │
  │  Bi-Stream 1: Chunk A (5MB)        │
  ├────────────────────────────────────►│
  │  Bi-Stream 2: Chunk B (5MB)        │
  ├────────────────────────────────────►│
  │  Bi-Stream 3: Chunk C (5MB)        │
  ├────────────────────────────────────►│
  │     ...                             │
  │  Bi-Stream N: Chunk N (5MB)        │
  ├────────────────────────────────────►│
  │                                     │
  │  All streams concurrent!            │
  │  No head-of-line blocking           │
  │                                     │
```

**Benchmark**: `tests/chunk_parallel_stress_test.rs` - 10 x 5MB chunks in parallel

## Architectural Decisions

### Why Separate Bi-Streams for Request and Response?

**Current design**: Request uses bi-stream #1, response uses bi-stream #2

**Alternative**: Use same bi-stream for request + response

**Tradeoffs**:

| Aspect | Separate Bi-Streams (Current) | Same Bi-Stream (Alternative) |
|--------|-------------------------------|------------------------------|
| Simplicity | ✅ Each stream has one purpose | ❌ Mixed send/recv on both halves |
| Flexibility | ✅ Multiple requests without waiting | ❌ Must wait for response before next request |
| Compatibility | ✅ Works with existing send()/recv() | ❌ Requires new send_and_recv() primitive |
| Efficiency | ❌ More streams | ✅ Fewer streams |

**Why we chose separate bi-streams:**
1. Works with existing `peer.send()` and `peer.recv()` primitives
2. Allows concurrent requests without blocking
3. Simpler mental model (each message = one stream)
4. QUIC streams are cheap (minimal overhead)

### Why Pass peer to notify_message()?

**Source**: `src/rpc/handler.rs:107`

```rust
pub async fn notify_message(
    &self,
    addr: SocketAddr,
    msg: RpcMessage,
    peer: Option<Arc<PeerConnection>>  // ← Critical!
)
```

**Purpose**: Allows server to send response on the **SAME connection** instead of creating a new one.

**Without peer**:
```
Server receives request on connection A
Server tries transport.send(client_addr, response)
  → Calls transport.connect(client_addr)
  → Tries to create NEW connection
  → Client must accept() this new connection
  → HANGS if client has no accept() loop
```

**With peer**:
```
Server receives request on connection A with peer
Server calls peer.send(response)
  → Opens new bi-stream on SAME connection A
  → Client's recv() loop accepts bi-stream
  → Response delivered successfully
```

### Why Inline Processing (No Spawned Tasks)?

**Critical for Quinn QUIC compatibility!**

**Source**: `src/rpc/handler.rs:115-116`

```rust
match msg {
    RpcMessage::Request(req) => {
        self.handle_request(addr, req, peer).await;  // ← Direct await, NO spawn!
    }
    // ...
}
```

**Bad pattern (causes starvation)**:
```rust
// DON'T DO THIS!
tokio::spawn(async move {
    let response = handle_request(req).await;
    peer.send(response).await;  // ← HANGS!
});
```

**Why spawned tasks cause hangs**:
1. Quinn needs its connection/endpoint drivers to be polled regularly
2. Spawned tasks create a "task trampoline" that prevents proper scheduling
3. `peer.send().await` inside spawned task can't yield properly
4. Result: send() hangs waiting for Quinn driver that never runs

**See**: [Quinn Issue #867](https://github.com/quinn-rs/quinn/issues/867) - "Task switching overhead causes 80ms+ latency"

**Good pattern (direct await chain)**:
```rust
// ✅ CORRECT!
pub async fn notify_message(&self, msg: RpcMessage) {
    self.handle_request(req).await;  // Direct await
    peer.send(response).await;       // Yields properly to scheduler
}
```

**Why this works**:
- Direct await chain allows tokio to poll Quinn drivers between awaits
- Proper cooperative scheduling
- Quinn can make progress

### Why Multi-Threaded Runtime?

**Source**: `tests/rpc_test.rs:8`

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc_request_response() {
    // ...
}
```

**Quinn requires multi-threaded runtime** to avoid deadlocks.

**Why?** Quinn's I/O drivers need to run concurrently with application tasks. Single-threaded runtime can cause starvation.

## Quinn QUIC Compatibility

### Best Practices

1. **Use direct await chains**: No spawned tasks doing Quinn I/O
2. **Multi-threaded runtime**: Minimum 2 worker threads
3. **Pass peer connections**: Don't create new connections for responses
4. **Recv loops on both sides**: Both client and server need `while peer.recv()`

### Common Pitfalls

❌ **Spawning tasks that do Quinn I/O**
```rust
tokio::spawn(async move {
    peer.send(data).await;  // HANGS!
});
```

✅ **Direct await in async function**
```rust
async fn handler(&self, peer: Arc<PeerConnection>) {
    peer.send(data).await;  // Works!
}
```

❌ **Creating new connections for responses**
```rust
async fn respond(&self, addr: SocketAddr) {
    self.transport.send(addr, response).await;  // Slow + may hang!
}
```

✅ **Using existing peer connection**
```rust
async fn respond(&self, peer: Arc<PeerConnection>) {
    peer.send(response).await;  // Fast + reliable!
}
```

❌ **No recv loop on client**
```rust
// Client sends request but never calls peer.recv()
let response = rpc.request(addr, payload).await;  // HANGS!
```

✅ **Recv loop on both sides**
```rust
// Client spawns recv loop
tokio::spawn(async move {
    while let Ok(Some(data)) = peer.recv().await {
        rpc.notify_message(addr, deserialize(&data), peer).await;
    }
});

// Now requests work!
let response = rpc.request(addr, payload).await;  // Success!
```

## Performance Characteristics

### RPC Latency
- **Local loopback**: ~1-2ms per request-response
- **Network**: Depends on RTT + processing time
- **Overhead**: 2 bi-streams per request-response (8 bytes frame header each)

### Chunk Transfer Throughput
- **Single 5MB chunk**: ~100-200 MB/s (local loopback)
- **10 parallel 5MB chunks**: ~500-1000 MB/s aggregate (local loopback)
- **Network**: Limited by bandwidth and QUIC congestion control

### Memory Usage
- **Connection**: ~50KB per QUIC connection
- **Bi-stream**: ~1KB per active bi-stream
- **Chunk buffer**: 64KB per concurrent transfer

## Testing Patterns

### Example: RPC Request-Response Test

**Source**: `tests/rpc_test.rs:8-110`

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_rpc_request_response() {
    let transport1 = Arc::new(QuicTransport::new(addr1).await?);
    let transport2 = Arc::new(QuicTransport::new(addr2).await?);

    let rpc1 = Arc::new(RpcHandler::new(transport1.clone()));
    let rpc2 = Arc::new(RpcHandler::new(transport2.clone()));

    // Set up request handler on server
    rpc2.set_request_handler(|req| {
        ResponsePayload::CustomResponse { success: true, data: ... }
    }).await;

    // Server recv loop
    tokio::spawn(async move {
        loop {
            if let Ok((addr, peer)) = transport2.accept().await {
                let peer = Arc::new(peer);
                tokio::spawn(async move {
                    while let Ok(Some(data)) = peer.recv().await {
                        if let Ok(msg) = deserialize(&data) {
                            rpc2.notify_message(addr, msg, Some(peer.clone())).await;
                        }
                    }
                });
            }
        }
    });

    // Client connects and spawns recv loop
    let client_peer = Arc::new(transport1.connect(addr2).await?);
    tokio::spawn(async move {
        while let Ok(Some(data)) = client_peer.recv().await {
            if let Ok(msg) = deserialize(&data) {
                rpc1.notify_message(addr2, msg, Some(client_peer.clone())).await;
            }
        }
    });

    // Send request
    let response = rpc1.request(addr2, payload, Duration::from_secs(5)).await?;

    assert!(response.success);
}
```

## Summary

**Key Takeaways**:

1. **Each message opens a NEW bi-stream** - not true bidirectional RPC
2. **Bi-streams used for ACKs only** - request/response correlation via IDs
3. **Both sides need recv loops** - to accept incoming bi-streams
4. **Pass peer to notify_message()** - send responses on same connection
5. **Direct await chains, no spawned I/O** - required for Quinn compatibility
6. **Multi-threaded runtime** - minimum 2 workers for Quinn
7. **Connections are pooled** - one per peer, streams are not pooled

**The bi-directional in bi-stream is for transport ACKs, not for RPC bidirectionality!**
