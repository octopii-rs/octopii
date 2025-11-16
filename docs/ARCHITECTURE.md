# Octopii Internal Architecture

## Overview

This document provides a comprehensive view of Octopii's internal architecture, data flows, component interactions, and implementation details. It includes ASCII diagrams to visualize the system structure.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Module Organization](#module-organization)
3. [Core Components](#core-components)
4. [Data Flow Diagrams](#data-flow-diagrams)
5. [Consensus Layer](#consensus-layer)
6. [Network Layer](#network-layer)
7. [Persistence Layer](#persistence-layer)
8. [Concurrency Model](#concurrency-model)
9. [Key Algorithms](#key-algorithms)
10. [Serialization Strategy](#serialization-strategy)
11. [Error Propagation](#error-propagation)
12. [Testing Infrastructure](#testing-infrastructure)

---

## System Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         User Application                         │
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ propose()    │  │ query()      │  │ Membership Mgmt      │  │
│  │ campaign()   │  │ read_index() │  │ add_learner()        │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
└─────────┼──────────────────┼──────────────────────┼──────────────┘
          │                  │                      │
          └──────────────────┴──────────────────────┘
                             │
                   ┌─────────▼─────────┐
                   │   OctopiiNode     │  ◄── Public API Layer
                   │  (OpenRaft Node)  │
                   └─────────┬─────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
   ┌────▼─────┐      ┌──────▼──────┐      ┌─────▼──────┐
   │ OpenRaft │      │ RpcHandler  │      │   State    │
   │ (Raft    │      │ (Messaging) │      │  Machine   │
   │ Protocol)│      └──────┬──────┘      └─────┬──────┘
   └────┬─────┘             │                   │
        │                   │                   │
        │         ┌─────────▼──────────┐        │
        │         │  QuicTransport     │        │
        │         │  (QUIC Network)    │        │
        │         └─────────┬──────────┘        │
        │                   │                   │
        │           ┌───────▼────────┐          │
        │           │  PeerConnection │         │
        │           │  (Connection    │         │
        │           │   Management)   │         │
        │           └────────────────┘          │
        │                                       │
   ┌────▼────────────────────────────────┐     │
   │     WriteAheadLog (Walrus)          │◄────┘
   │  ┌───────┐ ┌────────┐ ┌──────────┐ │
   │  │  Logs │ │ Config │ │ Snapshot │ │
   │  └───────┘ └────────┘ └──────────┘ │
   └─────────────────────────────────────┘
              │
   ┌──────────▼────────────┐
   │   Persistent Storage  │
   │    (Disk/SSD/NVMe)    │
   └───────────────────────┘
```

### Component Layers

```
┌────────────────────────────────────────────────────────┐
│                    Application Layer                    │  ← User Code
├────────────────────────────────────────────────────────┤
│                      API Layer                          │  ← OctopiiNode
├────────────────────────────────────────────────────────┤
│                   Consensus Layer                       │  ← OpenRaft/Raft
├────────────────────────────────────────────────────────┤
│                    RPC Layer                            │  ← RpcHandler
├────────────────────────────────────────────────────────┤
│                  Transport Layer                        │  ← QuicTransport
├────────────────────────────────────────────────────────┤
│                 Persistence Layer                       │  ← WAL
├────────────────────────────────────────────────────────┤
│                   Storage Layer                         │  ← Filesystem
└────────────────────────────────────────────────────────┘
```

---

## Module Organization

### Directory Structure

```
octopii/
├── src/
│   ├── lib.rs                    # Public API exports
│   │                               - Re-exports public types
│   │                               - Feature gate management
│   │
│   ├── config.rs                 # Configuration
│   │                               - Config struct
│   │                               - Default values
│   │
│   ├── error.rs                  # Error types
│   │                               - OctopiiError enum
│   │                               - Result type alias
│   │                               - Error conversions
│   │
│   ├── runtime.rs                # Tokio runtime wrapper
│   │                               - OctopiiRuntime
│   │                               - Handle vs owned runtime
│   │
│   ├── chunk.rs                  # Data transfer
│   │                               - ChunkSource enum
│   │                               - TransferResult
│   │
│   ├── state_machine.rs          # State machine trait
│   │                               - StateMachineTrait
│   │                               - KvStateMachine
│   │
│   ├── rpc/
│   │   ├── mod.rs                # RPC utilities
│   │   │                           - serialize/deserialize
│   │   │                           - Re-exports
│   │   │
│   │   ├── message.rs            # Message types
│   │   │                           - RpcMessage
│   │   │                           - RequestPayload
│   │   │                           - ResponsePayload
│   │   │                           - OneWayMessage
│   │   │
│   │   └── handler.rs            # RPC handler
│   │                               - RpcHandler
│   │                               - Request/response correlation
│   │                               - Timeout management
│   │
│   ├── transport/
│   │   ├── mod.rs                # QUIC transport
│   │   │                           - QuicTransport
│   │   │                           - Connection pooling
│   │   │
│   │   ├── peer.rs               # Peer connection
│   │   │                           - PeerConnection
│   │   │                           - Chunk protocols
│   │   │                           - send/recv
│   │   │
│   │   └── tls.rs                # TLS configuration
│   │                               - Self-signed certs
│   │                               - Skip verification
│   │
│   ├── wal/
│   │   ├── mod.rs                # WAL wrapper
│   │   │                           - WriteAheadLog
│   │   │                           - Walrus integration
│   │   │
│   │   └── wal/                  # Walrus implementation
│   │       ├── mod.rs
│   │       ├── topic.rs
│   │       └── cursor.rs
│   │
│   ├── raft/                     # Raft-rs integration (legacy)
│   │   ├── mod.rs                # RaftNode
│   │   ├── storage.rs            # WalStorage
│   │   ├── state_machine.rs      # RaftStateMachine wrapper
│   │   └── rpc.rs                # Message serialization
│   │
│   └── openraft/                 # OpenRaft integration
│       ├── mod.rs                # Re-exports
│       ├── types.rs              # Type definitions
│       │                           - AppEntry
│       │                           - AppResponse
│       │                           - TypeConfig
│       │
│       ├── node.rs               # OctopiiNode
│       │                           - Main node implementation
│       │                           - Leadership management
│       │                           - Membership API
│       │
│       ├── storage.rs            # Storage implementations
│       │                           - MemLogStore
│       │                           - MemStateMachine
│       │
│       └── network.rs            # Network layer
│                                   - QuinnNetwork
│                                   - RaftNetworkFactory
│
└── Cargo.toml                    # Dependencies and features
```

---

## Core Components

### 1. OctopiiNode

**Location:** `src/openraft/node.rs`

**Purpose:** Main entry point for Raft operations

**Internal Structure:**

```
┌──────────────────────────────────────────────────┐
│              OctopiiNode                         │
├──────────────────────────────────────────────────┤
│                                                  │
│  config: Config                                  │
│  raft: Arc<Raft<TypeConfig>>                     │
│  rpc_handler: Arc<RpcHandler>                    │
│  transport: Arc<QuicTransport>                   │
│  network: Arc<QuinnNetwork>                      │
│  peer_addrs: Arc<RwLock<HashMap<u64, Addr>>>     │
│  runtime: OctopiiRuntime                         │
│                                                  │
└──────────────────────────────────────────────────┘
         │
         ├─► Spawns consensus loop
         ├─► Manages RPC handlers
         └─► Coordinates state machine
```

**Key Responsibilities:**
- Initialize OpenRaft with storage and network
- Coordinate between consensus, RPC, and state machine
- Manage cluster membership
- Handle leadership transitions

---

### 2. RpcHandler

**Location:** `src/rpc/handler.rs`

**Purpose:** Request/response correlation and message routing

**Internal Structure:**

```
┌────────────────────────────────────────────────────┐
│              RpcHandler                            │
├────────────────────────────────────────────────────┤
│                                                    │
│  transport: Arc<QuicTransport>                     │
│  pending_requests:                                 │
│    Arc<RwLock<HashMap<MessageId, Sender>>>         │
│  request_handler:                                  │
│    Arc<RwLock<Option<RequestHandlerFn>>>           │
│  next_message_id: AtomicU64                        │
│                                                    │
└────────────────────────────────────────────────────┘
```

**Message Flow:**

```
Request:
  user → request() → assign message_id
                  ↓
         serialize RequestPayload
                  ↓
         send via QuicTransport
                  ↓
         register in pending_requests
                  ↓
         await response (with timeout)


Response (from peer):
  QUIC recv → deserialize RpcMessage
           ↓
       match message_id in pending_requests
           ↓
       send response to waiting request
```

**Critical Details:**
- Each peer has a dedicated receiver task to avoid blocking
- Monotonic message IDs ensure no collisions
- Timeouts prevent indefinite waiting
- Double-checked locking for request handlers

---

### 3. QuicTransport

**Location:** `src/transport/mod.rs`

**Purpose:** QUIC connection management and pooling

**Connection Pool Architecture:**

```
┌────────────────────────────────────────────┐
│          QuicTransport                     │
├────────────────────────────────────────────┤
│                                            │
│  endpoint: Arc<Endpoint>                   │
│  connections:                              │
│    Arc<RwLock<HashMap<Addr, PeerConn>>>    │
│  bind_addr: SocketAddr                     │
│                                            │
└────────────────────────────────────────────┘
         │
         ├─► connect(addr) ────┐
         │                     │
         │   ┌─────────────────▼──────────────┐
         │   │ Check pool for existing conn   │
         │   │ If exists: return              │
         │   │ If not: create new connection  │
         │   │         add to pool            │
         │   │         return                 │
         │   └────────────────────────────────┘
         │
         └─► accept() ──────┐
                            │
             ┌──────────────▼───────────────┐
             │ Wait for incoming connection │
             │ Add to pool                  │
             │ Return (addr, connection)    │
             └──────────────────────────────┘
```

**Connection Lifecycle:**

```
   New Connection Request
           │
           ▼
   ┌───────────────┐
   │ Check Pool    │
   └───────┬───────┘
           │
     ┌─────┴─────┐
     │           │
  Exists      Not Exists
     │           │
     ▼           ▼
  Return    ┌─────────────┐
  Cached    │   Connect   │
  Conn      │   via QUIC  │
            └──────┬──────┘
                   │
                   ▼
            ┌──────────────┐
            │  TLS Handshake│
            └──────┬────────┘
                   │
                   ▼
            ┌──────────────┐
            │ Add to Pool  │
            └──────┬───────┘
                   │
                   ▼
               Return Conn
```

---

### 4. WriteAheadLog

**Location:** `src/wal/mod.rs`

**Purpose:** Durable persistence using Walrus

**Internal Organization:**

```
┌─────────────────────────────────────────┐
│         WriteAheadLog                   │
├─────────────────────────────────────────┤
│                                         │
│  walrus: Arc<Walrus>                    │
│  topics:                                │
│    - logs                               │
│    - hard_state                         │
│    - config_state                       │
│    - snapshots                          │
│  batch_size: usize                      │
│  flush_interval: Duration               │
│  offset_counter: AtomicU64              │
│                                         │
└─────────────────────────────────────────┘
```

**Write Path:**

```
   append(data)
       │
       ▼
   ┌──────────────┐
   │ Generate     │
   │ offset       │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │ Compute      │
   │ checksum     │
   │ (FNV-1a)     │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐      ┌────────────┐
   │ Add to batch │─────►│ Batch full?│
   └──────────────┘      └──────┬─────┘
                                │
                    ┌───────────┴────────────┐
                    │                        │
                  Yes                       No
                    │                        │
                    ▼                        ▼
          ┌─────────────────┐         Wait for next
          │ Write to Walrus │         append or
          │ (fsync)         │         flush timeout
          └─────────────────┘
```

**Read Path:**

```
   read_all()
       │
       ▼
   ┌──────────────────┐
   │ Create cursor    │
   │ at beginning     │
   └────────┬─────────┘
            │
            ▼
   ┌────────────────────┐
   │ Read entry         │
   └────────┬───────────┘
            │
            ▼
   ┌────────────────────┐
   │ Verify checksum    │
   └────────┬───────────┘
            │
      ┌─────┴─────┐
      │           │
   Valid      Invalid
      │           │
      ▼           ▼
   Collect    Log Error
   Entry      & Skip
      │
      └──► More entries? ──┐
                           │
                     ┌─────┴──────┐
                     │            │
                    Yes          No
                     │            │
                     ▼            ▼
              Loop back      Return all
```

---

### 5. State Machine

**Location:** `src/state_machine.rs`

**Purpose:** Apply committed log entries

**Interaction with Consensus:**

```
   Raft commits entry (index N)
            │
            ▼
   ┌────────────────────┐
   │ OpenRaft calls     │
   │ apply_to_state_    │
   │ machine()          │
   └────────┬───────────┘
            │
            ▼
   ┌────────────────────┐
   │ Extract AppEntry   │
   │ from log           │
   └────────┬───────────┘
            │
            ▼
   ┌────────────────────┐
   │ Call user's        │
   │ StateMachine::     │
   │ apply()            │
   └────────┬───────────┘
            │
            ▼
   ┌────────────────────┐
   │ Return AppResponse │
   └────────┬───────────┘
            │
            ▼
   Response sent to client
```

---

## Data Flow Diagrams

### Write Operation (propose)

```
┌──────┐
│Client│
└──┬───┘
   │ propose(command)
   │
   ▼
┌─────────────┐
│OctopiiNode  │
│(is_leader?) │
└──────┬──────┘
       │ Yes
       ▼
┌──────────────────┐
│ OpenRaft::       │
│ client_write()   │
└────────┬─────────┘
         │
         ├──────────────┐
         │              │
         ▼              ▼
┌─────────────┐   ┌──────────────┐
│ Append to   │   │ Replicate to │
│ local log   │   │ followers    │
│ (WAL)       │   │ (via RPC)    │
└─────────────┘   └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ Serialize as │
                  │ OpenRaft msg │
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ RpcHandler:: │
                  │ request()    │
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ QuicTransport│
                  │ ::send()     │
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │ QUIC stream  │
                  │ to followers │
                  └──────┬───────┘
                         │
         ┌───────────────┴────────────────┐
         │                                │
         ▼                                ▼
   ┌──────────┐                    ┌──────────┐
   │Follower 1│                    │Follower 2│
   └────┬─────┘                    └────┬─────┘
        │                               │
        │ Append to local WAL           │
        └───────────────┬───────────────┘
                        │
                        ▼
                ┌───────────────┐
                │ Send AppendEntr│
                │ iesResponse   │
                └───────┬───────┘
                        │
                        ▼ Majority ACK
                ┌───────────────┐
                │ Leader commits│
                │ entry         │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Apply to      │
                │ StateMachine  │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Return response│
                │ to client     │
                └───────────────┘
```

### Read Operation (query)

```
┌──────┐
│Client│
└──┬───┘
   │ query(command)
   │
   ▼
┌─────────────┐
│OctopiiNode  │
└──────┬──────┘
       │
       ▼
┌──────────────┐
│ StateMachine │
│ ::apply()    │
│ (read-only)  │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Return result│
└──────────────┘

Note: For linearizable reads, use read_index() first:

┌──────┐
│Client│
└──┬───┘
   │ read_index(ctx)
   │
   ▼
┌─────────────────┐
│ Confirm current │
│ leader with     │
│ heartbeat round │
└────────┬────────┘
         │
         ▼ Confirmed
┌─────────────────┐
│ query(command)  │
└─────────────────┘
```

### Leader Election

```
   Election Timeout
   (or campaign())
         │
         ▼
   ┌──────────────┐
   │ Increment    │
   │ term         │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │ Transition to│
   │ Candidate    │
   └──────┬───────┘
          │
          ▼
   ┌──────────────┐
   │ Vote for self│
   └──────┬───────┘
          │
          ▼
   ┌──────────────────┐
   │ Send VoteRequest │
   │ to all peers     │
   └────────┬─────────┘
            │
    ┌───────┴────────┐
    │                │
    ▼                ▼
┌────────┐      ┌────────┐
│ Peer 1 │      │ Peer 2 │
└───┬────┘      └───┬────┘
    │               │
    │ Check:        │
    │ - term        │
    │ - log up to   │
    │   date?       │
    │               │
    ▼               ▼
┌────────┐      ┌────────┐
│Vote Yes│      │Vote Yes│
└───┬────┘      └───┬────┘
    │               │
    └───────┬───────┘
            │
            ▼ Majority votes
   ┌─────────────────┐
   │ Become Leader   │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ Send heartbeats │
   │ to all peers    │
   └─────────────────┘
```

### Membership Change (Add Learner → Promote)

```
┌──────┐
│Client│
└──┬───┘
   │ add_learner(id, addr)
   │
   ▼
┌────────────────┐
│ Leader adds    │
│ learner to     │
│ config         │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ Start          │
│ replicating    │
│ logs to learner│
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ Learner catches│
│ up (receives   │
│ all logs)      │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ Client checks  │
│ is_learner_    │
│ caught_up()    │
└───────┬────────┘
        │ Returns true
        ▼
┌────────────────┐
│ promote_learner│
│ (id)           │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ Leader creates │
│ config change  │
│ entry          │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ Replicate and  │
│ commit config  │
│ change         │
└───────┬────────┘
        │
        ▼
┌────────────────┐
│ Learner becomes│
│ voting member  │
└────────────────┘
```

### Snapshot Transfer

```
   Leader detects peer lag
   (> snapshot_lag_threshold)
            │
            ▼
   ┌─────────────────┐
   │ Create snapshot │
   │ at commit index │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ StateMachine::  │
   │ snapshot()      │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ Send snapshot   │
   │ via chunk       │
   │ protocol        │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ send_chunk_     │
   │ verified()      │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ Peer receives   │
   │ and verifies    │
   │ checksum        │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ StateMachine::  │
   │ restore()       │
   └────────┬────────┘
            │
            ▼
   ┌─────────────────┐
   │ Peer continues  │
   │ with log        │
   │ replication     │
   └─────────────────┘
```

---

## Consensus Layer

### OpenRaft Integration

**Architecture:**

```
┌────────────────────────────────────────┐
│           OctopiiNode                  │
├────────────────────────────────────────┤
│                                        │
│  ┌──────────────────────────────┐     │
│  │  Raft<TypeConfig>            │     │
│  │  ┌────────────────────────┐  │     │
│  │  │ RaftCore               │  │     │
│  │  │  - Leader election     │  │     │
│  │  │  - Log replication     │  │     │
│  │  │  - Membership changes  │  │     │
│  │  └────────────────────────┘  │     │
│  └────────┬─────────────────────┘     │
│           │                           │
│  ┌────────▼─────────────────────┐     │
│  │  QuinnNetwork                │     │
│  │  - send_append_entries       │     │
│  │  - send_vote                 │     │
│  │  - send_snapshot             │     │
│  └────────┬─────────────────────┘     │
│           │                           │
│  ┌────────▼─────────────────────┐     │
│  │  MemLogStore                 │     │
│  │  - In-memory log cache       │     │
│  │  - WAL backing               │     │
│  └──────────────────────────────┘     │
│                                        │
│  ┌────────────────────────────────┐   │
│  │  MemStateMachine               │   │
│  │  - Wraps user StateMachine     │   │
│  │  - Apply/snapshot/restore      │   │
│  └────────────────────────────────┘   │
│                                        │
└────────────────────────────────────────┘
```

### Type Configuration

```rust
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type NodeId = u64;
    type Node = SocketAddr;
    type Entry = AppEntry;
    type SnapshotData = Bytes;
    type AsyncRuntime = TokioRuntime;
}

pub struct AppEntry(pub Vec<u8>);
pub struct AppResponse(pub Bytes);
```

**Purpose:**
- `NodeId`: Unique identifier (u64)
- `Node`: Network address (SocketAddr)
- `Entry`: User command bytes
- `SnapshotData`: Serialized state
- `AsyncRuntime`: Tokio for async operations

---

## Network Layer

### QuinnNetwork

**Location:** `src/openraft/network.rs`

**Purpose:** Bridge between OpenRaft and QUIC transport

**RPC Types:**

```
AppendEntriesRequest
  ├─► Serialize via bincode
  ├─► Wrap in RequestPayload::OpenRaft
  ├─► Send via RpcHandler::request()
  └─► Wait for ResponsePayload

VoteRequest
  ├─► Serialize via bincode
  ├─► Wrap in RequestPayload::OpenRaft
  ├─► Send via RpcHandler::request()
  └─► Wait for ResponsePayload

InstallSnapshotRequest
  ├─► Send metadata
  ├─► Stream snapshot via chunk protocol *(planned; current builds trigger a Raft
      snapshot but do not yet stream the payload over Shipping Lane)*
  └─► Wait for completion
```

**Network Flow:**

```
OpenRaft                    QuinnNetwork                 RpcHandler
   │                             │                             │
   │ send_append_entries()       │                             │
   ├────────────────────────────►│                             │
   │                             │ Serialize request           │
   │                             ├────────────────────────────►│
   │                             │                             │ QUIC send
   │                             │                             ├──────────►
   │                             │                             │
   │                             │                             │◄──────────
   │                             │                             │ QUIC recv
   │                             │ Deserialize response        │
   │                             │◄────────────────────────────┤
   │                             │                             │
   │◄────────────────────────────┤                             │
   │ Return response             │                             │
```

---

## Persistence Layer

### WAL Topics

```
WriteAheadLog
    │
    ├─► logs_topic
    │     ├─ Entry 1 [offset=1, checksum, data]
    │     ├─ Entry 2 [offset=2, checksum, data]
    │     └─ Entry 3 [offset=3, checksum, data]
    │
    ├─► hard_state_topic
    │     ├─ term: u64
    │     ├─ vote: Option<NodeId>
    │     └─ commit: u64
    │
    ├─► config_state_topic
    │     ├─ voters: Vec<NodeId>
    │     └─ learners: Vec<NodeId>
    │
    └─► snapshots_topic
          ├─ Snapshot 1 [index, term, data]
          └─ Snapshot 2 [index, term, data]
```

### Recovery Process

```
Node Restart
     │
     ▼
┌──────────────┐
│ Open WAL     │
│ directory    │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Create cursor│
│ at beginning │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│ Read hard_state  │
│ (term, vote)     │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Read config_state│
│ (voters,learners)│
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Read all logs    │
│ into memory      │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Find last        │
│ snapshot (if any)│
└────────┬─────────┘
         │
   ┌─────┴──────┐
   │            │
 Found      Not Found
   │            │
   ▼            │
┌─────────┐     │
│ Restore │     │
│ snapshot│     │
└────┬────┘     │
     │          │
     └────┬─────┘
          │
          ▼
┌─────────────────┐
│ Apply remaining │
│ logs after      │
│ snapshot        │
└────────┬────────┘
         │
         ▼
   ┌──────────┐
   │ Ready to │
   │ serve    │
   └──────────┘
```

---

## Concurrency Model

### Threading Architecture

```
┌────────────────────────────────────────────────────────┐
│                  OctopiiRuntime                        │
│              (Tokio Thread Pool)                       │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐                  │
│  │Thread│ │Thread│ │Thread│ │Thread│  ... (N workers) │
│  └───┬──┘ └───┬──┘ └───┬──┘ └───┬──┘                  │
└──────┼────────┼────────┼────────┼─────────────────────┘
       │        │        │        │
       │   Tasks scheduled by runtime
       │        │        │        │
   ┌───▼────────▼────────▼────────▼─────┐
   │                                     │
   │  ┌────────────────────────────┐    │
   │  │ Consensus Loop (async)     │    │
   │  │  - Tick                    │    │
   │  │  - Process Ready           │    │
   │  │  - Apply committed entries │    │
   │  └────────────────────────────┘    │
   │                                     │
   │  ┌────────────────────────────┐    │
   │  │ RPC Receiver Tasks         │    │
   │  │  - One per peer            │    │
   │  │  - Process incoming msgs   │    │
   │  └────────────────────────────┘    │
   │                                     │
   │  ┌────────────────────────────┐    │
   │  │ QUIC Acceptor Loop         │    │
   │  │  - Accept connections      │    │
   │  │  - Spawn handlers          │    │
   │  └────────────────────────────┘    │
   │                                     │
   │  ┌────────────────────────────┐    │
   │  │ WAL Flush Timer            │    │
   │  │  - Periodic fsync          │    │
   │  └────────────────────────────┘    │
   │                                     │
   └─────────────────────────────────────┘
```

### Lock Hierarchy

```
To avoid deadlocks, locks are acquired in this order:

1. RpcHandler::pending_requests (RwLock)
   ├─ Short-lived reads for lookup
   └─ Short-lived writes for insert/remove

2. QuicTransport::connections (RwLock)
   ├─ Read: Check if connection exists
   └─ Write: Add/remove connections

3. OctopiiNode::peer_addrs (RwLock)
   ├─ Read: Lookup peer address
   └─ Write: Update peer address

4. StateMachine::data (RwLock or Mutex)
   ├─ User-defined locking
   └─ Should be short-lived

Critical: NEVER hold multiple locks simultaneously
         unless absolutely necessary
```

### Synchronization Points

```
┌──────────────────────────────────────┐
│  Consensus Loop                      │
│  ┌────────────────────────────────┐  │
│  │ Loop:                          │  │
│  │   1. Tick Raft                 │  │
│  │   2. Check ready               │  │
│  │   3. Process messages          │  │
│  │   4. Apply committed           │  │
│  │   5. Send to peers             │  │
│  │   6. Advance Raft              │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
           │
           │ Spawned as separate task
           │ No shared mutable state except:
           │   - Raft (behind Arc<Mutex>)
           │   - StateMachine (user-defined)
           │

┌──────────────────────────────────────┐
│  RPC Receiver (per peer)             │
│  ┌────────────────────────────────┐  │
│  │ Loop:                          │  │
│  │   1. Receive message           │  │
│  │   2. Deserialize               │  │
│  │   3. Match on type:            │  │
│  │      - Request → call handler  │  │
│  │      - Response → wake waiter  │  │
│  │      - OneWay → process        │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
           │
           │ Spawned per peer
           │ Shared state:
           │   - RpcHandler (Arc)
           │
```

---

## Key Algorithms

### 1. Snapshot Catch-Up

**Location:** `src/openraft/node.rs` (conceptual, implemented in OpenRaft)

**Purpose:** Fast catch-up for lagging peers

```
Algorithm: check_peer_lag()

Input: peer_id, peer_progress
Output: trigger_snapshot if needed

1. Get peer's matched_index
2. Get leader's commit_index
3. Calculate lag = commit_index - matched_index

4. If lag > snapshot_lag_threshold:
     a. Create snapshot at commit_index
     b. Call StateMachine::snapshot()
     c. Send via chunk protocol
     d. Peer calls StateMachine::restore()
     e. Continue with normal log replication

5. Else:
     a. Continue with normal log replication
```

**Benefit:**
- Lagging peer: 1,000,000 log entries behind
- Snapshot size: 100KB
- Transfer: 100KB vs 100MB+ of logs

---

### 2. Log Compaction

**Purpose:** Prevent unbounded log growth

```
Algorithm: compact_logs()

Input: logs, snapshots, compact_threshold
Output: compacted logs

1. Check if len(logs) > compact_threshold

2. If yes:
     a. Create snapshot at commit_index:
          - Call StateMachine::snapshot()
          - Store snapshot with index/term
     b. Trim logs:
          - Remove all entries <= snapshot_index
          - Keep only uncommitted entries
     c. Update first_index = snapshot_index + 1

3. Else:
     - No action needed
```

**Visual:**

```
Before compaction:
┌────────────────────────────────────────┐
│ Logs: [1, 2, 3, ..., 10000, 10001]    │
│ Snapshot: None                         │
└────────────────────────────────────────┘

After compaction (at index 9000):
┌────────────────────────────────────────┐
│ Logs: [9001, 9002, ..., 10001]        │
│ Snapshot: [index=9000, term=5, data]  │
└────────────────────────────────────────┘
```

---

### 3. Request/Response Correlation

**Location:** `src/rpc/handler.rs`

**Purpose:** Match responses to pending requests

```
Algorithm: request(addr, payload, timeout)

1. Generate unique message_id:
     message_id = next_message_id.fetch_add(1)

2. Create oneshot channel:
     (tx, rx) = oneshot::channel()

3. Register pending request:
     pending_requests.insert(message_id, tx)

4. Serialize request:
     msg = RpcMessage::Request {
         message_id,
         payload,
     }
     data = bincode::serialize(msg)

5. Send via QUIC:
     transport.send(addr, data)

6. Wait for response with timeout:
     select! {
         response = rx.recv() => Ok(response),
         _ = sleep(timeout) => {
             pending_requests.remove(message_id)
             Err(Timeout)
         }
     }

---

Algorithm: handle_response(response)

1. Extract message_id from response

2. Lookup in pending_requests:
     if let Some(tx) = pending_requests.remove(message_id):
         tx.send(response)  // Wakes waiting request

3. If not found:
     log warning (late or duplicate response)
```

---

### 4. Connection Pooling

**Location:** `src/transport/mod.rs`

**Purpose:** Reuse connections, avoid reconnection overhead

```
Algorithm: connect(addr)

1. Acquire read lock on connections:
     if connections.contains_key(addr):
         return connections.get(addr).clone()

2. Release read lock

3. Acquire write lock on connections:
     // Double-check (another thread may have connected)
     if connections.contains_key(addr):
         return connections.get(addr).clone()

     // Create new connection
     conn = endpoint.connect(addr).await?

     // Check if still alive
     if !conn.is_closed():
         connections.insert(addr, conn.clone())
         return conn
     else:
         return Err(ConnectionFailed)

4. Release write lock
```

**Double-Checked Locking:**
```
Thread 1                    Thread 2
   │                           │
   ├─ Read lock                ├─ Read lock
   ├─ Key not found            ├─ Key not found
   ├─ Release read             ├─ Release read
   │                           │
   ├─ Write lock               │ (waits for lock)
   ├─ Double-check             │
   ├─ Connect                  │
   ├─ Insert                   │
   ├─ Release write            │
   │                           ├─ Write lock acquired
   │                           ├─ Double-check
   │                           ├─ Key FOUND!
   │                           └─ Return existing
```

---

## Serialization Strategy

### Protocol Layers

```
┌────────────────────────────────────────┐
│         Application Data               │
│         (user commands)                │
└──────────────┬─────────────────────────┘
               │
               ▼
┌────────────────────────────────────────┐
│      AppEntry (Vec<u8>)                │  ← User command wrapped
└──────────────┬─────────────────────────┘
               │
               ▼ bincode::serialize
┌────────────────────────────────────────┐
│      RaftMessage (protobuf)            │  ← OpenRaft messages
│   - AppendEntriesRequest               │
│   - VoteRequest                        │
│   - InstallSnapshotRequest             │
└──────────────┬─────────────────────────┘
               │
               ▼ bincode::serialize
┌────────────────────────────────────────┐
│      RpcMessage (bincode)              │  ← RPC layer envelope
│   - Request                            │
│   - Response                           │
│   - OneWay                             │
└──────────────┬─────────────────────────┘
               │
               ▼
┌────────────────────────────────────────┐
│      Bytes                             │  ← Raw bytes
└──────────────┬─────────────────────────┘
               │
               ▼
┌────────────────────────────────────────┐
│      QUIC Stream                       │  ← Network transport
└────────────────────────────────────────┘
```

### Format Comparison

| Layer | Format | Reason |
|-------|--------|--------|
| WAL entries | rkyv | Zero-copy deserialization, fast recovery |
| Raft messages | protobuf | OpenRaft compatibility |
| RPC messages | bincode | Fast, compact, Rust-native |
| User state | User choice | KvStateMachine uses bincode |

---

## Error Propagation

### Error Flow

```
   Error occurs at transport layer
              │
              ▼
   ┌──────────────────────┐
   │ QuicConnectionError  │
   └──────────┬───────────┘
              │ From trait
              ▼
   ┌──────────────────────┐
   │  OctopiiError::      │
   │  QuicConnection      │
   └──────────┬───────────┘
              │ Propagate up
              ▼
   ┌──────────────────────┐
   │  OctopiiError::      │
   │  Transport           │
   └──────────┬───────────┘
              │ Propagate up
              ▼
   ┌──────────────────────┐
   │  OctopiiError::Rpc   │
   └──────────┬───────────┘
              │ Return to user
              ▼
   ┌──────────────────────┐
   │  User handles error  │
   │  - Retry             │
   │  - Failover          │
   │  - Log and continue  │
   └──────────────────────┘
```

### Error Context

```rust
// Low-level error
quinn::ConnectionError::LocallyClosed
    ↓
// Wrapped with context
OctopiiError::QuicConnection(quinn::ConnectionError)
    ↓
// Further wrapped
OctopiiError::Transport("Failed to connect to peer 192.168.1.100:5000")
    ↓
// Returned to user
propose() returns Err(OctopiiError::Transport(...))
```

---

## Testing Infrastructure

### Test Patterns

#### 1. Smoke Test

```rust
#[tokio::test]
async fn test_single_node_propose() {
    // Create node
    let runtime = OctopiiRuntime::new(2);
    let config = Config {
        node_id: 1,
        is_initial_leader: true,
        ..Default::default()
    };
    let node = OctopiiNode::new(config, runtime).await.unwrap();
    node.start().await.unwrap();

    // Propose
    let response = node.propose(b"SET k v".to_vec()).await.unwrap();
    assert_eq!(response, Bytes::from("OK"));

    // Query
    let value = node.query(b"GET k").await.unwrap();
    assert_eq!(value, Bytes::from("v"));
}
```

#### 2. Cluster Test

```rust
#[tokio::test]
async fn test_three_node_replication() {
    // Create 3 nodes
    let nodes = vec![
        create_node(1, true).await.unwrap(),
        create_node(2, false).await.unwrap(),
        create_node(3, false).await.unwrap(),
    ];

    // Wait for cluster
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Propose on leader
    nodes[0].propose(b"SET k v".to_vec()).await.unwrap();

    // Verify on all nodes
    for node in &nodes {
        let value = node.query(b"GET k").await.unwrap();
        assert_eq!(value, Bytes::from("v"));
    }
}
```

#### 3. Chunk Transfer Test

```rust
#[tokio::test]
async fn test_large_chunk_transfer() {
    // Create transport
    let transport = QuicTransport::new("127.0.0.1:0".parse().unwrap())
        .await.unwrap();

    // Create large data
    let data = vec![0u8; 10_000_000]; // 10MB
    let chunk = ChunkSource::Memory(Bytes::from(data.clone()));

    // Send and verify
    let peer = transport.connect("127.0.0.1:5001".parse().unwrap())
        .await.unwrap();
    let bytes_sent = peer.send_chunk_verified(&chunk).await.unwrap();
    assert_eq!(bytes_sent, 10_000_000);
}
```

### Test Utilities

```rust
// Temporary directory for WAL
fn temp_wal_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("octopii_test_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// Fixed ports to avoid conflicts
fn test_ports() -> Vec<u16> {
    vec![15001, 15002, 15003]
}

// Logging setup
fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("octopii=debug")
        .try_init();
}
```

---

## Performance Characteristics

### Throughput

```
Factors:
  - WAL batch size (larger = higher throughput)
  - WAL flush interval (longer = higher throughput)
  - Worker threads (more = higher parallelism)
  - Network bandwidth
  - Disk write speed

Typical: 10,000 - 100,000 ops/sec (3-node cluster, SSD)
```

### Latency

```
Factors:
  - WAL fsync time (dominant factor)
  - Network RTT
  - Serialization overhead
  - State machine apply time

Breakdown (typical):
  - Serialization: <1ms
  - Network (local): 1-2ms
  - WAL fsync: 1-10ms (depends on batch size/interval)
  - State machine: <1ms
  - Total: 5-20ms
```

### Memory

```
Components:
  - In-memory log cache: ~100 bytes per entry
  - State machine state: User-defined
  - Connection pool: ~10KB per peer
  - Pending requests: ~1KB per in-flight request

Example (10,000 uncommitted entries):
  - Logs: 1MB
  - State machine (KV): Variable
  - Connections (5 peers): 50KB
  - Total: ~1-2MB + state machine
```

### Network

```
Per-peer overhead:
  - 1 QUIC connection (bidirectional)
  - Heartbeats: ~100 bytes every 500ms
  - Log replication: Proportional to write rate

Example (1000 writes/sec):
  - Log entries: ~100KB/sec/peer
  - Heartbeats: ~200 bytes/sec/peer
  - Total: ~100KB/sec/peer
```

---

## Deployment Patterns

### Single Datacenter

```
┌────────────────────────────────────────┐
│          Datacenter                    │
│                                        │
│  ┌──────┐  ┌──────┐  ┌──────┐         │
│  │Node 1│  │Node 2│  │Node 3│         │
│  │Leader│  │Follow│  │Follow│         │
│  └──┬───┘  └───┬──┘  └───┬──┘         │
│     │          │          │            │
│     └──────────┴──────────┘            │
│            Low latency                 │
│            (< 1ms RTT)                 │
└────────────────────────────────────────┘
```

### Multi-Region (Read Replicas)

```
┌─────────────────────┐        ┌─────────────────────┐
│   Region 1 (US)     │        │   Region 2 (EU)     │
│                     │        │                     │
│  ┌──────┐ ┌──────┐ │        │  ┌──────┐           │
│  │Node 1│ │Node 2│ │        │  │Node 4│           │
│  │Leader│ │Voter │ │        │  │Learner│          │
│  └──┬───┘ └───┬──┘ │        │  └───┬──┘           │
│     │         │     │        │      │              │
│     └─────────┘     │        │      │              │
└──────────┬──────────┘        └──────┬──────────────┘
           │                          │
           │    Cross-region          │
           │    (50-100ms RTT)        │
           └──────────────────────────┘
```

---

## Summary

Octopii's architecture is built around:

1. **Separation of concerns**: Consensus, RPC, transport, persistence
2. **Pluggability**: Custom state machines, configurable storage
3. **Performance**: Connection pooling, batched WAL, async I/O
4. **Reliability**: Checksummed transfers, durable persistence, crash recovery
5. **Scalability**: Dynamic membership, snapshot catch-up

The system achieves consensus through the Raft algorithm, communicates via QUIC for low-latency RPC, persists data through a write-ahead log, and provides a clean API for building distributed applications.

---

**Version:** 0.1.0
**Last Updated:** 2025-11-13
