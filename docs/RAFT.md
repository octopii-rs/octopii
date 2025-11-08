# Raft Consensus in Octopii

## Overview

Octopii provides a Raft consensus implementation using `raft-rs` with full durability via Walrus WAL.

## Basic Usage

### Creating a Raft Cluster

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};

let config = Config {
    node_id: 1,
    bind_addr: "127.0.0.1:7000".parse().unwrap(),
    peers: vec![
        "127.0.0.1:7001".parse().unwrap(),
        "127.0.0.1:7002".parse().unwrap(),
    ],
    wal_dir: PathBuf::from("/tmp/raft_node_1"),
    worker_threads: 4,
    wal_batch_size: 100,
    wal_flush_interval_ms: 100,
    is_initial_leader: true,  // Only one node should be true
};

let runtime = OctopiiRuntime::new(4);
let node = OctopiiNode::new(config, runtime).await?;
node.start().await?;
```

### Proposing Values

```rust
// Only leader can propose
if node.is_leader().await {
    let command = b"SET key value";
    let result = node.propose(command.to_vec()).await?;
}
```

### Querying State

```rust
// Any node can query
let result = node.query(b"GET key").await?;
```

### Leader Election

```rust
// Trigger election if no leader
node.campaign().await?;
```

### Adding/Removing Peers

```rust
// Add a new peer (must be leader)
node.add_peer(4, "127.0.0.1:7003".parse()?).await?;

// Remove a peer (must be leader)
node.remove_peer(4).await?;
```

## Architecture

### Cluster Topology

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   Node 1    │◄───────►│   Node 2    │◄───────►│   Node 3    │
│  (Leader)   │  QUIC   │ (Follower)  │  QUIC   │ (Follower)  │
└──────┬──────┘         └──────┬──────┘         └──────┬──────┘
       │                       │                        │
       ▼                       ▼                        ▼
   ┌────────┐             ┌────────┐              ┌────────┐
   │ Walrus │             │ Walrus │              │ Walrus │
   │  WAL   │             │  WAL   │              │  WAL   │
   └────────┘             └────────┘              └────────┘
```

### Storage Layer

All Raft state is persisted to Walrus via topic isolation:

```
Walrus WAL
├── hard_state topic
│   └── (term, vote, commit_index)
├── entries topic
│   └── (index, term, data, entry_type)
└── snapshot topic
    └── (state_machine_data, metadata)
```

### State Machine

Built-in key-value state machine:

```
┌─────────────────────────────────────┐
│       State Machine (KV Store)      │
├─────────────────────────────────────┤
│ Operations:                         │
│  • SET key value → Apply to store   │
│  • GET key → Read from store        │
│  • DELETE key → Remove from store   │
└─────────────────────────────────────┘
```

### Durability Guarantees

1. **Crash Recovery**: All Raft state persisted to WAL
2. **Log Compaction**: Automatic snapshot creation at 1000 entries
3. **Space Reclamation**: Old entries deleted after snapshot

## Implementation Details

### Message Flow

```
     Client                 Leader Node              Follower Nodes
       │                         │                          │
       │  propose(SET x=1)       │                          │
       ├────────────────────────►│                          │
       │                         │                          │
       │                   ┌─────┴─────┐                    │
       │                   │   Raft    │                    │
       │                   │  propose  │                    │
       │                   └─────┬─────┘                    │
       │                         │                          │
       │                   ┌─────▼─────┐                    │
       │                   │  Persist  │                    │
       │                   │  to WAL   │                    │
       │                   └─────┬─────┘                    │
       │                         │                          │
       │                         │  AppendEntries RPC       │
       │                         ├─────────────────────────►│
       │                         │                          │
       │                         │      ACK (success)       │
       │                         │◄─────────────────────────┤
       │                         │                          │
       │                   ┌─────▼─────┐                    │
       │                   │  Commit   │                    │
       │                   │   Entry   │                    │
       │                   └─────┬─────┘                    │
       │                         │                          │
       │                   ┌─────▼──────┐                   │
       │                   │   Apply    │                   │
       │                   │ to StateMachine               │
       │                   └─────┬──────┘                   │
       │                         │                          │
       │   Result (success)      │                          │
       │◄────────────────────────┤                          │
       │                         │                          │
```

### Background Tasks

Node starts 2 background tasks:
1. **Tick loop**: Runs every 100ms, drives heartbeats and elections
2. **Ready loop**: Processes Raft ready states (persist, send messages, apply)

### Persistence

```rust
// Entries are persisted synchronously
node.store().append_entries_sync(&entries);

// Hard state updates
node.store().set_hard_state(hard_state);

// Snapshots for compaction
node.store().apply_snapshot(snapshot)?;
```

### Recovery

On startup, WAL automatically recovers:
1. Read hard state (term, vote, commit)
2. Read all log entries
3. Read latest snapshot if exists
4. Initialize Raft with recovered state

## Configuration

```rust
pub struct Config {
    pub node_id: u64,                    // Unique node ID
    pub bind_addr: SocketAddr,           // Listen address
    pub peers: Vec<SocketAddr>,          // Other nodes
    pub wal_dir: PathBuf,                // WAL storage directory
    pub worker_threads: usize,           // Runtime threads
    pub wal_batch_size: usize,           // WAL batching (unused with Walrus)
    pub wal_flush_interval_ms: u64,      // Fsync interval (100-200ms recommended)
    pub is_initial_leader: bool,         // Bootstrap as leader
}
```

## Cluster Bootstrap

**Leader node** (one node only):
```rust
Config { is_initial_leader: true, .. }
```

**Follower nodes**:
```rust
Config { is_initial_leader: false, .. }
```

The leader initializes the cluster and followers join via Raft messages.

## Advanced Features

### Pre-Vote Protocol

Octopii implements the pre-vote protocol to prevent election storms when a partitioned node rejoins the cluster:

```rust
// Automatically enabled in RaftConfig
config.pre_vote = true;  // Prevents unnecessary elections
```

**How it works**: Before starting an election, a candidate asks peers if they would vote for it. Only if enough peers respond positively does it start a real election.

### Automatic Leader Election

The cluster automatically triggers elections when no leader is detected for 2 seconds:

```rust
// No manual intervention needed
// Cluster self-heals automatically
```

**Configuration**: Adjust `MAX_TICKS_WITHOUT_LEADER` in `src/node.rs` to tune election timeout (default: 20 ticks = 2 seconds).

### Learner Nodes (Safe Membership Changes)

Add new nodes as learners first to avoid quorum issues:

```rust
// Add node as learner (doesn't affect quorum)
node.add_learner(4, "127.0.0.1:7003".parse()?).await?;

// Check if learner is caught up
if node.is_learner_caught_up(4).await? {
    // Promote to voter when safe
    node.promote_learner(4).await?;
}
```

**Why learners**: Adding a voter directly can cause quorum loss if the new node is slow to catch up. Learners receive logs but don't vote, making membership changes safe.

### Snapshot Transfer

Leaders automatically send snapshots to followers that are too far behind:

```rust
// Happens automatically when follower is > LOG_COMPACTION_THRESHOLD entries behind
// No manual intervention needed
```

**Performance**: Snapshots are transferred via RPC with efficient serialization. Large snapshots are sent in batches for memory efficiency.

### Batch Operations

Log entries are written in batches for improved performance:

```rust
// Up to 2000 entries written atomically per batch
// 2-5x throughput improvement over individual writes
// Leverages io_uring on Linux for maximum performance
```

## Performance Optimizations

### Walrus Batch Operations

- **Batch writes**: Up to 2000 log entries written atomically
- **Batch reads**: Recovery is 10-50x faster with batch reads
- **Space reclamation**: Old log entries automatically reclaimable after snapshot

### Aggressive Log Compaction

- **Threshold**: Snapshot created every 500 entries (reduced from 1000)
- **Space-efficient**: Old entries discarded during recovery, blocks reclaimable by Walrus
- **Fast recovery**: Only entries after snapshot are kept in memory

## Limitations

- Fixed tick intervals (100ms tick, 300ms heartbeat)
- No joint consensus for membership changes (coming soon)

## Example: 3-Node Cluster

```rust
// Node 1 (leader)
let node1 = OctopiiNode::new(Config {
    node_id: 1,
    bind_addr: "127.0.0.1:7000".parse()?,
    peers: vec!["127.0.0.1:7001".parse()?, "127.0.0.1:7002".parse()?],
    wal_dir: PathBuf::from("/tmp/node1"),
    is_initial_leader: true,
    ..Default::default()
}, runtime1).await?;

// Node 2 (follower)
let node2 = OctopiiNode::new(Config {
    node_id: 2,
    bind_addr: "127.0.0.1:7001".parse()?,
    peers: vec!["127.0.0.1:7000".parse()?, "127.0.0.1:7002".parse()?],
    wal_dir: PathBuf::from("/tmp/node2"),
    is_initial_leader: false,
    ..Default::default()
}, runtime2).await?;

// Node 3 (follower)
let node3 = OctopiiNode::new(Config {
    node_id: 3,
    bind_addr: "127.0.0.1:7002".parse()?,
    peers: vec!["127.0.0.1:7000".parse()?, "127.0.0.1:7001".parse()?],
    wal_dir: PathBuf::from("/tmp/node3"),
    is_initial_leader: false,
    ..Default::default()
}, runtime3).await?;

// Start all nodes
node1.start().await?;
node2.start().await?;
node3.start().await?;

// Propose on leader
node1.propose(b"SET x 42".to_vec()).await?;

// Query on any node
let value = node2.query(b"GET x").await?;
```
