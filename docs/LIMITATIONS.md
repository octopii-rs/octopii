# Current Limitations

This document outlines the current limitations, constraints, and known issues with octopii.

---

## Security Limitations

### 1. Self-Signed Certificates

**Limitation**: Uses self-signed TLS certificates with no verification.

```
Server generates:  [Self-signed cert]
Client accepts:    Any certificate (no verification!)
```

**Impact**:
- Vulnerable to man-in-the-middle attacks
- No peer authentication
- Encryption provided, but no identity verification

**Not suitable for**:
- Public internet deployments
- Untrusted networks
- Production systems requiring authentication

**Current code**:
```rust
// src/transport/tls.rs
struct SkipServerVerification;  // Accepts ANY certificate!
```

### 2. No Authorization

**Limitation**: Any peer can connect and transfer data.

**Impact**:
- No access control
- No rate limiting
- No peer allowlist/blocklist

---

## Protocol Limitations

### 1. No Resume Support

**Limitation**: If a transfer fails partway through, you must restart from beginning.

```
Transfer: [====>    ] 40% complete
Network failure!
Restart:  [>        ] 0%  (lost all progress)
```

**Impact**:
- Large file transfers vulnerable to network hiccups
- Wasted bandwidth on retry
- No partial transfer recovery

### 2. No Compression

**Limitation**: Data is sent as-is, no compression.

**Impact**:
- Higher bandwidth usage for compressible data
- Slower transfers on limited networks
- More expensive on metered connections

### 3. No Deduplication

**Limitation**: Sending same file twice transfers all bytes twice.

**Impact**:
- Redundant transfers waste bandwidth
- No content-addressed storage
- No delta transfers

### 4. Fixed Buffer Size

**Limitation**: 64KB buffer hardcoded, not tunable.

```rust
const BUFFER_SIZE: usize = 64 * 1024;  // Hardcoded!
```

**Impact**:
- May not be optimal for all network conditions
- Can't tune for high-latency networks
- Can't reduce for memory-constrained systems

---

## Scalability Limitations

### 1. In-Memory Receive Buffer

**Limitation**: Received chunks are held entirely in memory.

```rust
// receiver allocates up to 10MB initially
let mut data = BytesMut::with_capacity(
    std::cmp::min(total_size as usize, 10 * 1024 * 1024)
);
```

**Impact**:
- Large transfers (1GB+) require 1GB+ of RAM on receiver
- OOM risk on memory-constrained systems
- Not suitable for streaming to disk

**Current max tested**: 100MB chunks

### 2. No Streaming to Disk (Receiver)

**Limitation**: Receiver must buffer entire chunk in memory before writing.

**Impact**:
- Can't receive multi-GB files on low-RAM systems
- Higher memory pressure
- Slower time-to-first-byte for consumers

**Workaround**: Would need to implement streaming `recv_chunk_verified_to_file()`

### 3. Linear Peer Scaling

**Limitation**: Each peer connection uses dedicated resources.

**Impact**:
- 1000 concurrent peers = 1000 open connections
- Memory grows with peer count
- File descriptor limits apply

### 4. No Connection Limit

**Limitation**: No max connection limit enforced.

**Impact**:
- Can be overwhelmed by connection flood
- Resource exhaustion possible
- No backpressure mechanism

---

## Reliability Limitations

### 1. No Automatic Retry

**Limitation**: Failed transfers are not automatically retried.

```rust
let result = peer.send_chunk_verified(&chunk).await;
if result.is_err() {
    // You must manually retry
}
```

**Impact**:
- Application must implement retry logic
- Transient failures require manual handling
- No exponential backoff built-in

### 2. Synchronous Checksumming

**Limitation**: SHA256 computed synchronously during send/receive.

**Impact**:
- CPU-bound on fast networks
- Can bottleneck on large transfers
- No GPU acceleration
- Blocks other operations on same stream

### 3. No Corruption Recovery

**Limitation**: Checksum mismatch = entire transfer failed.

```
Transfer:  [=========] 100% complete
Checksum:  MISMATCH!
Result:    Error (all 1GB wasted)
```

**Impact**:
- Single bit flip fails entire transfer
- No per-block verification
- No erasure coding

---

## Feature Limitations

### 1. Basic RPC Only

**Limitation**: RPC is simple request/response, no advanced features.

**Missing**:
- No request routing
- No service discovery
- No load balancing
- No multiplexing awareness
- No backpressure
- No streaming RPC
- No bidirectional streams
- No protobuf/gRPC compatibility

### 2. Minimal Raft Integration

**Limitation**: Raft integration is a stub, not functional.

```rust
// src/raft/mod.rs - currently does nothing useful
```

**Impact**:
- No state machine replication
- No leader election
- No distributed consensus
- Not production-ready for distributed systems

### 3. No Metrics/Observability

**Limitation**: No built-in metrics, tracing, or monitoring.

**Missing**:
- Transfer progress callbacks
- Bandwidth monitoring
- Connection health checks
- Error rate tracking
- No Prometheus/OpenTelemetry integration

### 4. No Configuration File

**Limitation**: All configuration is programmatic.

**Impact**:
- No runtime configuration
- Can't change settings without recompile
- No environment variable support

---

## Platform Limitations

### 1. Not Tested on Windows

**Status**: Developed and tested on Linux only.

**Impact**:
- May not work on Windows
- Path handling may break
- TLS provider compatibility unknown

### 2. Requires Tokio

**Limitation**: Tightly coupled to tokio runtime.

**Impact**:
- Can't use with async-std
- Can't use with smol
- Requires multi-threaded tokio runtime

---

## Memory Limitations

### 1. No Memory Pooling

**Limitation**: Buffers allocated/freed for each transfer.

**Impact**:
- Allocator pressure on high-throughput
- No buffer reuse
- Potential fragmentation

### 2. Arc Everywhere

**Limitation**: Heavy use of Arc for sharing.

**Impact**:
- Atomic reference counting overhead
- Memory overhead of control block
- Cache line contention on counters

---

## Network Limitations

### 1. No NAT Traversal

**Limitation**: Both peers must be directly routable.

**Impact**:
- Won't work behind NAT without port forwarding
- No STUN/TURN support
- No hole punching

### 2. No IPv6

**Status**: Only tested with IPv4.

**Impact**:
- May not work on IPv6-only networks
- No dual-stack testing

### 3. No Bandwidth Limiting

**Limitation**: Transfers use all available bandwidth.

**Impact**:
- Can starve other applications
- No QoS support
- No fair sharing between peers

---

## Testing Limitations

### 1. Limited Large-Scale Testing

**Current test coverage**:
- ✓ Up to 100MB transfers
- ✓ Up to 20 concurrent streams
- ✓ Up to 3 peers in parallel

**Not tested**:
- Multi-GB transfers (1GB+)
- Hundreds of concurrent connections
- High packet loss scenarios
- Extreme latency (satellite links)
- Long-running transfers (hours)

### 2. No Chaos Testing

**Missing**:
- Network partition testing
- Random disconnection testing
- Packet corruption testing
- Resource exhaustion testing

---

## Performance Limitations

### 1. Single Stream per Transfer

**Limitation**: Each chunk transfer uses one QUIC stream.

**Impact**:
- Can't parallelize single large file across multiple streams
- Head-of-line blocking within a stream
- Can't utilize full bandwidth on high-BDP networks

### 2. No Zero-Copy Send (Files)

**Limitation**: File data is read into buffer, then sent.

```rust
file.read(&mut buffer).await?;  // Copy to buffer
send_stream.write_all(&buffer).await?;  // Copy to kernel
```

**Impact**:
- Extra memory copy
- CPU overhead
- Not using sendfile()/splice()

### 3. Synchronous Accept Loop

**Limitation**: `accept()` must be called in a loop.

**Impact**:
- Application must manage accept loop
- Can't accept on multiple addresses
- No automatic connection handling

---

## API Limitations

### 1. PeerConnection Not Clone

**Limitation**: Can't clone PeerConnection.

```rust
let peer = transport.connect(addr).await?;
let peer2 = peer.clone();  // Error!
```

**Workaround**: Wrap in `Arc<PeerConnection>`

**Impact**:
- More verbose API
- Manual Arc management
- Easy to misuse

### 2. Blocking Runtime API

**Limitation**: Must use `block_on()` from sync context.

```rust
runtime.block_on(async { ... });  // Blocks calling thread
```

**Impact**:
- Can't integrate with existing async runtimes
- Must manage runtime lifetime carefully
- Potential deadlocks if misused

### 3. No Async File Read API

**Limitation**: Applications must use tokio::fs for file operations.

```rust
// Must use tokio::fs, not std::fs
use tokio::fs::File;  // Required
use std::fs::File;    // Won't work with ChunkSource::File
```

---

## Known Issues

### 1. Test Hang on Interleaved RPC

**Status**: `test_interleaved_rpc_and_chunk_transfer` may hang.

**Impact**: CI may timeout

**Workaround**: Run with timeout

### 2. Connection Reuse Edge Cases

**Issue**: Rapid connection open/close may trigger QUIC errors.

**Impact**: Some stress tests need fresh transports per iteration.

---

## Not Limitations (Things that Work)

For clarity, these DO work correctly:

- ✓ Parallel transfers to multiple peers
- ✓ Concurrent RPC and chunk transfer on same connection
- ✓ SHA256 checksum verification
- ✓ Application-level ACK
- ✓ Memory-efficient file streaming (sender side)
- ✓ Isolated tokio runtime
- ✓ Connection pooling
- ✓ Stream multiplexing
