# Raft Implementation Internals

This document explains how Octopii's Raft implementation works internally, including the event loop, Ready/LightReady pattern, message flow, and testing infrastructure.

## Table of Contents
- [Overview](#overview)
- [Core Components](#core-components)
- [Event Loop Architecture](#event-loop-architecture)
- [Ready and LightReady Pattern](#ready-and-lightready-pattern)
- [Message Flow](#message-flow)
- [Persistence Layer](#persistence-layer)
- [Test Infrastructure](#test-infrastructure)

## Overview

Octopii uses the `raft-rs` library (from TiKV) for the core Raft consensus algorithm, wrapped with:
- **Walrus** for durable Write-Ahead Logging (WAL)
- **QUIC** for low-latency network transport
- **Asynchronous I/O** with Tokio for high performance

```
┌──────────────────────────────────────────────────────────────────┐
│                        OctopiiNode                               │
│  ┌────────────┐  ┌─────────────┐  ┌──────────────┐             │
│  │  RaftNode  │  │   Storage   │  │     State    │             │
│  │  (raft-rs) │──│  (Walrus)   │  │    Machine   │             │
│  └──────┬─────┘  └──────┬──────┘  └──────┬───────┘             │
│         │               │                 │                      │
│         └───────────────┴─────────────────┘                      │
│                         │                                        │
│         ┌───────────────┴───────────────┐                        │
│         │     Event Loop (async)        │                        │
│         │  - ready() processing         │                        │
│         │  - LightReady processing      │                        │
│         │  - Message sending/receiving  │                        │
│         └───────────────┬───────────────┘                        │
│                         │                                        │
│         ┌───────────────┴───────────────┐                        │
│         │   QUIC Transport Layer        │                        │
│         │  - Peer connections           │                        │
│         │  - Network filters (testing)  │                        │
│         └───────────────────────────────┘                        │
└──────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. RaftNode
Wraps `raft-rs`'s RawNode and manages:
- Raft state machine transitions
- Proposal handling
- Message stepping
- Ready state notifications

### 2. WalStorage
Implements raft-rs's `Storage` trait backed by Walrus:
- Persistent hard state (term, vote, commit_index)
- Raft log entries
- Snapshots
- Configuration state

### 3. StateMachine
User-facing state machine:
- Applies committed entries
- Handles queries
- Provides linearizable reads

### 4. QuicTransport
Network layer using QUIC protocol:
- Peer-to-peer connections
- Message routing
- Connection management

## Event Loop Architecture

The heart of Octopii is the **async event loop** that runs in `run_raft_loop`:

```
┌─────────────────────────────────────────────────────────────┐
│                    Event Loop Cycle                         │
│                                                             │
│  1. Wait for trigger (timer, message, or proposal)         │
│                  │                                          │
│                  ▼                                          │
│  2. raft.ready() ──► Ready available?                      │
│                  │                                          │
│                  ▼                                          │
│  3. Process Ready:                                          │
│     - Persist hard_state to WAL                            │
│     - Persist entries to WAL                               │
│     - Send messages to peers                               │
│     - Apply committed entries to state machine             │
│                  │                                          │
│                  ▼                                          │
│  4. raft.advance(ready) ──► Returns LightReady             │
│                  │                                          │
│                  ▼                                          │
│  5. Process LightReady:                                     │
│     - Send additional messages                              │
│     - Apply additional committed entries                    │
│                  │                                          │
│                  ▼                                          │
│  6. Loop back to step 1                                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Event Loop Triggers

The loop wakes up when any of these occur:
1. **Tick timer** (150ms): Drives election timeout and heartbeats
2. **Incoming Raft message**: From other nodes via QUIC
3. **Proposal request**: Client wants to replicate data
4. **Ready notification**: Raft has work to do

## Ready and LightReady Pattern

This is the key pattern from `raft-rs` that you see in logs:

### What is Ready?

**Ready** represents a batch of work that needs to be done:

```rust
Ready {
    ss: Option<SoftState>,           // Leadership/term changes (not persisted)
    hs: Option<HardState>,            // Term, vote, commit (must persist!)
    entries: Vec<Entry>,              // Log entries to persist
    snapshot: Snapshot,               // Snapshot to apply
    committed_entries: Vec<Entry>,    // Entries to apply to state machine
    messages: Vec<Message>,           // Messages to send to peers
}
```

**Processing order (critical for correctness):**

```
Step 1: Persist HardState to WAL
        │
        ▼
Step 2: Persist Entries to WAL
        │
        ▼
Step 3: Send Messages to peers
        │
        ▼
Step 4: Apply committed entries to state machine
        │
        ▼
Step 5: Call raft.advance(ready) ──► Returns LightReady
```

### What is LightReady?

**LightReady** is additional work that becomes available AFTER calling `advance()`:

```rust
LightReady {
    committed_entries: Vec<Entry>,    // Additional entries to apply
    messages: Vec<Message>,           // Additional messages to send
}
```

**Why two phases?**

This design allows raft-rs to:
1. Make progress while persisting data
2. Generate new messages after advancing state
3. Apply more committed entries in the same event loop iteration

**Example log sequence:**
```
INFO Raft ready: has 2 messages, 5 committed entries, 3 new entries
INFO LightReady: 2 committed entries, 1 messages
```

This means:
- Initial Ready: 2 messages sent, 5 entries applied, 3 entries persisted
- After advance(): 1 more message sent, 2 more entries applied

## Message Flow

### Client Proposal Flow

```
Client                    OctopiiNode              RaftNode              Peers
  │                            │                       │                   │
  │  propose(data)            │                       │                   │
  ├──────────────────────────►│                       │                   │
  │                            │  step(MsgPropose)    │                   │
  │                            ├──────────────────────►│                   │
  │                            │                       │                   │
  │                            │    ready()            │                   │
  │                            │◄──────────────────────┤                   │
  │                            │                       │                   │
  │                            │  Persist entries      │                   │
  │                            │  to WAL               │                   │
  │                            ├─────────┐             │                   │
  │                            │         │             │                   │
  │                            │◄────────┘             │                   │
  │                            │                       │                   │
  │                            │  Send MsgAppend       │                   │
  │                            ├───────────────────────┼──────────────────►│
  │                            │                       │                   │
  │                            │                       │   MsgAppendResp   │
  │                            │                       │◄──────────────────┤
  │                            │    ready()            │                   │
  │                            │◄──────────────────────┤                   │
  │                            │                       │                   │
  │                            │  Apply to state       │                   │
  │                            │  machine              │                   │
  │                            ├─────────┐             │                   │
  │                            │         │             │                   │
  │                            │◄────────┘             │                   │
  │                            │                       │                   │
  │  Result(committed)        │                       │                   │
  │◄──────────────────────────┤                       │                   │
  │                            │                       │                   │
```

### Heartbeat Flow

```
Leader                    Follower
  │                          │
  │  Tick timer fires        │
  ├────────┐                 │
  │        │                 │
  │◄───────┘                 │
  │                          │
  │  ready() available       │
  │  (has MsgHeartbeat)      │
  │                          │
  │   MsgHeartbeat          │
  ├─────────────────────────►│
  │                          │
  │                          │  step(MsgHeartbeat)
  │                          ├────────┐
  │                          │        │
  │                          │◄───────┘
  │                          │
  │                          │  ready() available
  │                          │  (has MsgHeartbeatResponse)
  │                          │
  │  MsgHeartbeatResponse   │
  │◄─────────────────────────┤
  │                          │
  │  step(MsgHeartbeatResp) │
  ├────────┐                 │
  │        │                 │
  │◄───────┘                 │
  │                          │
```

### Leader Election Flow

```
Node 1 (Candidate)        Node 2              Node 3
  │                          │                   │
  │  Election timeout        │                   │
  ├────────┐                 │                   │
  │        │                 │                   │
  │◄───────┘                 │                   │
  │                          │                   │
  │  campaign()              │                   │
  ├────────┐                 │                   │
  │        │                 │                   │
  │◄───────┘                 │                   │
  │                          │                   │
  │  ready() available       │                   │
  │  (has MsgRequestVote)    │                   │
  │                          │                   │
  │   MsgRequestVote        │                   │
  ├─────────────────────────►│                   │
  ├───────────────────────────────────────────────►│
  │                          │                   │
  │                          │  step(MsgVote)    │
  │                          ├────────┐          │
  │                          │        │          │
  │                          │◄───────┘          │
  │                          │                   │
  │  MsgRequestVoteResponse │                   │
  │◄─────────────────────────┤                   │
  │◄─────────────────────────────────────────────┤
  │                          │                   │
  │  step(VoteResp x2)       │                   │
  │  Becomes LEADER          │                   │
  ├────────┐                 │                   │
  │        │                 │                   │
  │◄───────┘                 │                   │
  │                          │                   │
  │  ready() available       │                   │
  │  (SoftState: is_leader)  │                   │
  │                          │                   │
```

## Persistence Layer

### Walrus Integration

Octopii uses **Walrus** for all durable storage. Walrus organizes data into topics (like Kafka):

```
Walrus WAL Directory
└── node_1.wal/
    ├── raft_hard_state      ← term, vote, commit_index
    ├── raft_entries         ← log entries (index, term, data)
    ├── raft_conf_state      ← cluster membership
    ├── raft_snapshot        ← raft snapshot metadata
    ├── state_machine        ← applied commands
    └── state_machine_snapshot ← state machine snapshot
```

### Write Path

When Raft has entries to persist:

```
1. ready.entries available
        │
        ▼
2. Batch entries together
        │
        ▼
3. wal.append_batch("raft_entries", entries)
        │
        ▼
4. Walrus writes to log file
        │
        ▼
5. Periodic fsync (every 100ms by default)
        │
        ▼
6. Entry durable on disk
```

### Recovery Path

When a node restarts:

```
1. Open Walrus WAL
        │
        ▼
2. Read raft_hard_state topic
   └─► Get last term, vote, commit
        │
        ▼
3. Read raft_entries topic
   └─► Rebuild log from last snapshot
        │
        ▼
4. Read raft_conf_state topic
   └─► Get current cluster membership
        │
        ▼
5. Read state_machine topic
   └─► Rebuild key-value state
        │
        ▼
6. Resume Raft from recovered state
```

## Test Infrastructure

Octopii has comprehensive test infrastructure (1,095 LOC ported from TiKV):

### Network Simulation

```
┌──────────────────────────────────────────────────────────────┐
│                    Network Filter Layer                       │
│                                                               │
│  Node 1              Node 2              Node 3              │
│    │                   │                   │                 │
│    │  MsgAppend       │                   │                 │
│    ├─────────────────►│                   │                 │
│    │                  │                   │                 │
│    │   [Filter Applied]                   │                 │
│    │    - Drop?                           │                 │
│    │    - Delay?                          │                 │
│    │    - Modify?                         │                 │
│    │                  │                   │                 │
│    ├──────────────────X   (dropped)       │                 │
│    │                                      │                 │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

### Filter Types

1. **PartitionFilter**: Blocks messages between node groups
2. **DropPacketFilter**: Randomly drops messages (simulates packet loss)
3. **DelayFilter**: Delays messages (simulates network latency)
4. **MessageTypeFilter**: Filters by Raft message type
5. **ConditionalFilter**: Custom predicate-based filtering

### Test Cluster Architecture

```
TestCluster {
  nodes: Vec<TestNode>,
  base_port: u16,
  filters: HashMap<NodeId, Vec<Filter>>
}

TestNode {
  node: OctopiiNode,
  addr: SocketAddr,
  state: NodeState (Running | Crashed)
}
```

### Common Test Patterns

#### 1. Partition Testing
```rust
// Isolate leader from followers
cluster.partition(vec![1], vec![2, 3]).await;

// Result: Nodes 2,3 elect new leader
```

#### 2. Crash Recovery
```rust
// Crash node 2
cluster.crash_node(2)?;

// Make proposals
for i in 0..10 {
    cluster.nodes[0].propose(data).await?;
}

// Restart node 2
cluster.restart_node(2).await?;

// Node 2 catches up from WAL
```

#### 3. Learner Testing
```rust
// Add learner (receives entries but doesn't vote)
cluster.add_learner(4).await?;

// Learner catches up
tokio::time::sleep(Duration::from_secs(2)).await;

// Promote to full voter
cluster.promote_learner(4).await?;
```

## Performance Characteristics

### Latency Breakdown (typical)

```
Client Proposal → Committed
│
├─ Step 1: RPC to leader                    ~0.5ms (QUIC)
├─ Step 2: Leader persists to WAL          ~0.3ms (Walrus)
├─ Step 3: Leader sends MsgAppend          ~0.5ms (QUIC)
├─ Step 4: Follower persists               ~0.3ms (Walrus)
├─ Step 5: Follower sends MsgAppendResp    ~0.5ms (QUIC)
├─ Step 6: Leader commits entry            ~0.1ms
└─ Step 7: Apply to state machine          ~0.2ms
                                           ─────────
                                           ~2.4ms (3-node cluster)
```

### Throughput Characteristics

- **Single node**: ~50,000 proposals/sec (limited by WAL fsync)
- **Batching enabled**: ~200,000 proposals/sec
- **Network bound**: At ~10KB entries, limited by QUIC throughput

## Debugging Tips

### Understanding Log Messages

```
INFO Raft ready: has 2 messages, 5 committed entries, 3 new entries, snapshot: false
```
Means:
- **2 messages**: Will send 2 Raft messages to peers
- **5 committed entries**: Will apply 5 entries to state machine
- **3 new entries**: Will persist 3 entries to WAL
- **snapshot: false**: No snapshot to apply

```
INFO LightReady: 2 committed entries, 1 messages
```
Means:
- After advancing Raft, 2 more entries became committed
- 1 more message needs to be sent

```
WARN No address found for peer 4
```
Means:
- Raft wants to send message to peer 4
- But peer 4's address isn't in peer_addrs map
- Usually means learner not started yet or removed from cluster

### Common Issues

**Issue**: ConfChange timeout
```
Error: "ConfChange timeout after 10s for learner 4"
```
**Cause**: Adding node to config but not starting it
**Fix**: Start node immediately after adding to cluster

**Issue**: Split brain
```
Node 1: "I'm leader at term 5"
Node 2: "I'm leader at term 5"
```
**Cause**: Network partition, both think they have majority
**Fix**: This shouldn't happen with correct quorum! Check test setup.

**Issue**: No leader
```
All nodes: state=Candidate
```
**Cause**: No node can get majority of votes
**Fix**: Check that cluster has odd number of nodes and all are reachable

## Further Reading

- [raft-rs documentation](https://docs.rs/raft/)
- [Raft paper](https://raft.github.io/raft.pdf)
- [Walrus documentation](https://walrus.nubskr.com/)
- [QUIC protocol](https://www.rfc-editor.org/rfc/rfc9000.html)
