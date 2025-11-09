# Custom State Machines in Octopii

Octopii supports custom replicated state machines through the `StateMachineTrait`. This allows you to implement your own application logic that will be consistently replicated across the Raft cluster.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [The StateMachineTrait](#the-statemachinetrait)
- [Implementation Guide](#implementation-guide)
- [Persistence Patterns](#persistence-patterns)
- [Integration with OctopiiNode](#integration-with-octopiinode)
- [Best Practices](#best-practices)
- [Example: Counter State Machine](#example-counter-state-machine)
- [Advanced Topics](#advanced-topics)

## Overview

A state machine in Raft is a deterministic component that processes commands in a specific order. The Raft consensus algorithm ensures that all nodes in the cluster apply the same commands in the same order, guaranteeing consistency.

Octopii provides:
- **Default Implementation**: `KvStateMachine` - a key-value store with WAL-backed durability
- **Custom Implementation**: Implement `StateMachineTrait` for your own application logic

## Quick Start

Here's a minimal custom state machine:

```rust
use bytes::Bytes;
use octopii::StateMachineTrait;
use std::sync::RwLock;

struct CounterStateMachine {
    counter: RwLock<i64>,
}

impl StateMachineTrait for CounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd = String::from_utf8_lossy(command);
        match cmd.as_ref() {
            "INCREMENT" => {
                let mut counter = self.counter.write().unwrap();
                *counter += 1;
                Ok(Bytes::from(format!("{}", *counter)))
            }
            "GET" => {
                let counter = self.counter.read().unwrap();
                Ok(Bytes::from(format!("{}", *counter)))
            }
            _ => Err(format!("Unknown command: {}", cmd)),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let counter = self.counter.read().unwrap();
        counter.to_le_bytes().to_vec()
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        if snapshot.len() != 8 {
            return Err("Invalid snapshot size".to_string());
        }
        let value = i64::from_le_bytes(snapshot.try_into().unwrap());
        *self.counter.write().unwrap() = value;
        Ok(())
    }
}
```

## The StateMachineTrait

```rust
pub trait StateMachineTrait: Send + Sync {
    /// Apply a command to the state machine
    fn apply(&self, command: &[u8]) -> Result<Bytes, String>;

    /// Create a snapshot of the current state
    fn snapshot(&self) -> Vec<u8>;

    /// Restore state from a snapshot
    fn restore(&self, snapshot: &[u8]) -> Result<(), String>;

    /// Compact the state machine (optional)
    fn compact(&self) -> Result<(), String> {
        Ok(())
    }
}
```

### Method Responsibilities

#### `apply(command: &[u8]) -> Result<Bytes, String>`

**Purpose**: Process a committed command and update state

**Requirements**:
- MUST be deterministic - same command always produces same result
- MUST be thread-safe (trait requires `Send + Sync`)
- SHOULD return meaningful results for queries
- SHOULD validate commands and return errors for invalid input

**Example**:
```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let cmd_str = String::from_utf8(command.to_vec())
        .map_err(|e| format!("Invalid UTF-8: {}", e))?;

    let parts: Vec<&str> = cmd_str.split_whitespace().collect();

    match parts.as_slice() {
        ["SET", key, value] => {
            let mut data = self.data.write().unwrap();
            data.insert(key.to_string(), value.to_string());
            Ok(Bytes::from("OK"))
        }
        ["GET", key] => {
            let data = self.data.read().unwrap();
            match data.get(*key) {
                Some(value) => Ok(Bytes::from(value.clone())),
                None => Ok(Bytes::from("NOT_FOUND")),
            }
        }
        _ => Err(format!("Unknown command: {}", cmd_str)),
    }
}
```

#### `snapshot() -> Vec<u8>`

**Purpose**: Serialize the current state for persistence and transfer

**Requirements**:
- MUST capture complete current state
- SHOULD be compact and efficient
- SHOULD use a stable serialization format
- Called periodically by Raft for log compaction

**Example**:
```rust
fn snapshot(&self) -> Vec<u8> {
    let data = self.data.read().unwrap();
    // Use your preferred serialization format
    rkyv::to_bytes::<_, 4096>(&*data)
        .map(|bytes| bytes.to_vec())
        .unwrap_or_else(|_| vec![])
}
```

#### `restore(snapshot: &[u8]) -> Result<(), String>`

**Purpose**: Deserialize and restore state from a snapshot

**Requirements**:
- MUST handle empty snapshots gracefully
- MUST validate snapshot data
- MUST completely replace current state
- Called during crash recovery and when catching up

**Example**:
```rust
fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
    if snapshot.is_empty() {
        return Ok(());
    }

    let data = rkyv::from_bytes::<HashMap<String, String>>(snapshot)
        .map_err(|e| format!("Failed to deserialize: {:?}", e))?;

    *self.data.write().unwrap() = data;
    Ok(())
}
```

#### `compact() -> Result<(), String>`

**Purpose**: Optional method for state machine compaction

**Requirements**:
- Default implementation does nothing
- Override if your state machine needs periodic cleanup
- Called periodically by the compaction process

**Example**:
```rust
fn compact(&self) -> Result<(), String> {
    // Example: Remove tombstones older than 1 hour
    let mut data = self.data.write().unwrap();
    let now = SystemTime::now();
    data.retain(|_, entry| {
        !entry.is_tombstone() || entry.age(now) < Duration::from_secs(3600)
    });
    Ok(())
}
```

## Implementation Guide

### Step 1: Define Your State

```rust
use std::sync::RwLock;
use std::collections::HashMap;

struct MyStateMachine {
    // Use RwLock for thread-safe access
    data: RwLock<HashMap<String, String>>,
    // Add any other state you need
}

impl MyStateMachine {
    fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}
```

### Step 2: Implement StateMachineTrait

```rust
use octopii::StateMachineTrait;
use bytes::Bytes;

impl StateMachineTrait for MyStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Parse command and update state
        // Return result
    }

    fn snapshot(&self) -> Vec<u8> {
        // Serialize state
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        // Deserialize and restore state
    }
}
```

### Step 3: Design Your Command Format

Choose a command format that works for your application:

**Simple Text Commands**:
```rust
"SET key value"
"GET key"
"DELETE key"
```

**Binary Protocol**:
```rust
// [command_type: u8][payload_len: u32][payload: bytes]
let command_type = command[0];
let payload_len = u32::from_le_bytes(command[1..5].try_into().unwrap());
let payload = &command[5..];
```

**Structured with Serde**:
```rust
#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

let cmd: Command = bincode::deserialize(command)?;
```

### Step 4: Ensure Determinism

**Critical Rules**:
- NO random number generation
- NO system time (unless from command input)
- NO external network calls
- NO non-deterministic operations

**Good**:
```rust
// Deterministic - same input always gives same output
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let value = compute_hash(command);  // ✓ Deterministic
    // ...
}
```

**Bad**:
```rust
// Non-deterministic - different nodes get different results!
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let random_id = rand::random::<u64>();  // ✗ NON-DETERMINISTIC!
    let timestamp = SystemTime::now();       // ✗ NON-DETERMINISTIC!
    // ...
}
```

## Persistence Patterns

### Pattern 1: External Persistence (Recommended for Simple Cases)

The state machine relies on Raft's log and snapshots for persistence:

```rust
struct SimpleStateMachine {
    data: RwLock<HashMap<String, String>>,
}

// State persists through:
// 1. Raft log (automatic)
// 2. Snapshots (via snapshot()/restore())
```

**Pros**:
- Simple to implement
- Raft handles all persistence
- Works well for small-to-medium state

**Cons**:
- Full state in memory
- Snapshot/restore on every restart

### Pattern 2: WAL-Backed Persistence (Recommended for Production)

Like `KvStateMachine`, persist operations independently:

```rust
struct DurableStateMachine {
    data: RwLock<HashMap<String, String>>,
    wal: Arc<WriteAheadLog>,
}

impl StateMachineTrait for DurableStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // 1. Persist to WAL FIRST
        self.wal.append(command)?;

        // 2. Apply to in-memory state
        let mut data = self.data.write().unwrap();
        // ... update data ...

        Ok(result)
    }
}
```

**Pros**:
- Fast recovery (replay WAL)
- No full snapshots needed
- Durable across crashes

**Cons**:
- More complex to implement
- Need to manage WAL compaction

### Pattern 3: External Database

State machine as a thin wrapper over a database:

```rust
struct DbStateMachine {
    db: Arc<Database>,
}

impl StateMachineTrait for DbStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Parse command
        // Execute database transaction
        self.db.execute(sql, params)?;
        Ok(result)
    }

    fn snapshot(&self) -> Vec<u8> {
        // Export database state
        self.db.export_snapshot()
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        // Import database state
        self.db.import_snapshot(snapshot)
    }
}
```

**Pros**:
- Leverage existing database features
- Can handle large state
- Transaction support

**Cons**:
- Database must be deterministic
- Snapshot/restore can be slow
- Additional dependency

## Integration with OctopiiNode

### Using the Default KV Store

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5000".parse()?,
        peers: vec!["127.0.0.1:5001".parse()?],
        // ... other config
        ..Default::default()
    };

    let runtime = OctopiiRuntime::new(4);
    let node = OctopiiNode::new(config, runtime).await?;
    node.start().await?;

    // Uses default KvStateMachine
    let result = node.propose(b"SET key value".to_vec()).await?;
    println!("Result: {}", String::from_utf8_lossy(&result));

    Ok(())
}
```

### Using a Custom State Machine

```rust
use octopii::{Config, OctopiiNode, OctopiiRuntime, StateMachine};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config {
        node_id: 1,
        bind_addr: "127.0.0.1:5000".parse()?,
        peers: vec!["127.0.0.1:5001".parse()?],
        // ... other config
        ..Default::default()
    };

    // Create your custom state machine
    let my_sm = Arc::new(MyStateMachine::new());

    // Cast to trait object
    let state_machine: StateMachine = my_sm;

    let runtime = OctopiiRuntime::new(4);
    let node = OctopiiNode::new_with_state_machine(
        config,
        runtime,
        state_machine,
    ).await?;

    node.start().await?;

    // Use your custom commands
    let result = node.propose(b"MY_CUSTOM_COMMAND data".to_vec()).await?;
    println!("Result: {}", String::from_utf8_lossy(&result));

    Ok(())
}
```

## Best Practices

### 1. Thread Safety

Always use appropriate synchronization:

```rust
use std::sync::RwLock;  // Prefer RwLock for read-heavy workloads
use std::sync::Mutex;    // Use Mutex for simple cases

struct MyStateMachine {
    data: RwLock<HashMap<String, String>>,  // ✓ Thread-safe
}
```

### 2. Error Handling

Return descriptive errors:

```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    let cmd_str = String::from_utf8(command.to_vec())
        .map_err(|e| format!("Invalid UTF-8 in command: {}", e))?;  // ✓ Descriptive

    if cmd_str.is_empty() {
        return Err("Empty command not allowed".to_string());  // ✓ Clear error
    }

    // ...
}
```

### 3. Command Validation

Validate early and thoroughly:

```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    // Validate size
    if command.len() > MAX_COMMAND_SIZE {
        return Err(format!("Command too large: {} bytes", command.len()));
    }

    // Validate format
    let parts: Vec<&str> = cmd_str.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty command".to_string());
    }

    // Validate command type
    match parts[0] {
        "SET" | "GET" | "DELETE" => {},  // Valid
        _ => return Err(format!("Unknown command: {}", parts[0])),
    }

    // ... process command
}
```

### 4. Efficient Serialization

Choose appropriate serialization:

```rust
// For simple types
fn snapshot(&self) -> Vec<u8> {
    let counter = self.counter.read().unwrap();
    counter.to_le_bytes().to_vec()  // Efficient for primitives
}

// For complex types
use rkyv;  // Zero-copy deserialization
use bincode;  // Simple and fast
use serde_json;  // Human-readable (slower)
```

### 5. Snapshot Size Management

Keep snapshots reasonable:

```rust
fn snapshot(&self) -> Vec<u8> {
    let data = self.data.read().unwrap();

    // Option 1: Compress large snapshots
    let serialized = bincode::serialize(&*data).unwrap();
    compress(&serialized)

    // Option 2: Only snapshot active data
    let active_data: HashMap<_, _> = data.iter()
        .filter(|(_, v)| !v.is_deleted())
        .collect();
    bincode::serialize(&active_data).unwrap()
}
```

### 6. Testing

Test your state machine thoroughly:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determinism() {
        let sm1 = MyStateMachine::new();
        let sm2 = MyStateMachine::new();

        let commands = vec![b"SET a 1", b"SET b 2", b"GET a"];

        for cmd in commands {
            let result1 = sm1.apply(cmd).unwrap();
            let result2 = sm2.apply(cmd).unwrap();
            assert_eq!(result1, result2, "Non-deterministic behavior!");
        }
    }

    #[test]
    fn test_snapshot_restore() {
        let sm1 = MyStateMachine::new();

        // Apply some commands
        sm1.apply(b"SET key1 value1").unwrap();
        sm1.apply(b"SET key2 value2").unwrap();

        // Take snapshot
        let snapshot = sm1.snapshot();

        // Create new state machine and restore
        let sm2 = MyStateMachine::new();
        sm2.restore(&snapshot).unwrap();

        // Verify state matches
        assert_eq!(sm1.apply(b"GET key1").unwrap(), sm2.apply(b"GET key1").unwrap());
    }
}
```

## Example: Counter State Machine

See `examples/custom_state_machine.rs` for a complete working example:

```bash
cargo run --example custom_state_machine
```

This example demonstrates:
- Basic command parsing
- State management with RwLock
- Snapshot/restore implementation
- All four StateMachineTrait methods

## Advanced Topics

### Handling Large State

For state machines with large datasets:

1. **Incremental Snapshots**: Only snapshot changes
2. **External Storage**: Store large blobs outside the state machine
3. **Lazy Loading**: Load data on demand
4. **Compression**: Compress snapshots

### Command Batching

Process multiple commands in one apply call:

```rust
fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
    // Parse batch format: [count: u32][cmd1_len: u32][cmd1][cmd2_len: u32][cmd2]...
    let count = u32::from_le_bytes(command[0..4].try_into().unwrap());

    let mut results = Vec::new();
    let mut offset = 4;

    for _ in 0..count {
        let len = u32::from_le_bytes(command[offset..offset+4].try_into().unwrap()) as usize;
        offset += 4;

        let cmd = &command[offset..offset+len];
        results.push(self.apply_single(cmd)?);
        offset += len;
    }

    Ok(Bytes::from(serialize_results(results)))
}
```

### Read-Only Queries

Use `OctopiiNode::query()` for reads that don't need to go through Raft:

```rust
// Write (goes through Raft)
node.propose(b"SET key value".to_vec()).await?;

// Read (local, no Raft)
let value = node.query(b"GET key").await?;
```

**Note**: Queries are not linearizable by default. For linearizable reads, use Read Index (see `docs/read_index.md`).

### Migration and Versioning

Version your snapshot format:

```rust
#[derive(Serialize, Deserialize)]
struct SnapshotV1 {
    version: u32,  // Always 1
    data: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotV2 {
    version: u32,  // Always 2
    data: HashMap<String, String>,
    metadata: HashMap<String, String>,  // New field
}

fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
    let version = u32::from_le_bytes(snapshot[0..4].try_into().unwrap());

    match version {
        1 => {
            let snap: SnapshotV1 = deserialize(snapshot)?;
            // Migrate to current format
            self.load_v1(snap)
        }
        2 => {
            let snap: SnapshotV2 = deserialize(snapshot)?;
            self.load_v2(snap)
        }
        _ => Err(format!("Unknown snapshot version: {}", version)),
    }
}
```

## Troubleshooting

### State Divergence

If nodes have different state:

1. **Check Determinism**: Ensure apply() is truly deterministic
2. **Check Serialization**: Verify snapshot/restore roundtrips correctly
3. **Check Ordering**: Ensure no race conditions in concurrent access

### Performance Issues

If apply() is slow:

1. **Profile**: Use `cargo flamegraph` to find bottlenecks
2. **Optimize Locks**: Minimize lock hold time
3. **Batch Operations**: Process multiple commands together
4. **Async I/O**: Use tokio for I/O operations (but maintain determinism!)

### Memory Usage

If memory grows unbounded:

1. **Implement compact()**: Clean up old data
2. **Use Weak References**: For caches
3. **Limit State Size**: Reject operations that would exceed limits

## Additional Resources

- [Raft Paper](https://raft.github.io/raft.pdf) - Original Raft consensus algorithm
- [KvStateMachine Source](../src/raft/state_machine.rs) - Production example
- [Example Code](../examples/custom_state_machine.rs) - Working example
- [Integration Tests](../tests/raft_comprehensive/custom_state_machine_tests.rs) - Test patterns

## Support

For questions or issues:
- GitHub Issues: https://github.com/anthropics/octopii/issues
- Documentation: https://docs.octopii.io

---

*Last updated: 2025-11-09*
