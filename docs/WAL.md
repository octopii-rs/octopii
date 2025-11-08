# Write-Ahead Log in Octopii

## Overview

Octopii uses Walrus as its Write-Ahead Log (WAL) for durable storage. The WAL provides crash recovery and persistence for Raft state.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│              OctopiiNode                             │
│                                                      │
│  ┌────────────┐              ┌─────────────────┐   │
│  │    Raft    │─────────────►│  WriteAheadLog  │   │
│  │   Storage  │   persist    │   (WAL wrapper) │   │
│  └────────────┘              └────────┬────────┘   │
│                                       │             │
│                                       │             │
│                              ┌────────▼────────┐    │
│                              │     Walrus      │    │
│                              │  (sync engine)  │    │
│                              └────────┬────────┘    │
└───────────────────────────────────────┼─────────────┘
                                        │
                                        ▼
                              ┌──────────────────┐
                              │   Disk Storage   │
                              │  (mmap files)    │
                              └──────────────────┘
```

## Usage in Octopii

### Initialization

Each Raft node creates its own isolated WAL instance:

```rust
// Configured via OctopiiNode
let config = Config {
    wal_dir: PathBuf::from("/tmp/node_1"),  // Unique per node
    wal_flush_interval_ms: 100,              // Fsync every 100ms
    ..
};

let node = OctopiiNode::new(config, runtime).await?;
```

### Directory Structure

```
/tmp/node_1/
└── node_1.wal/              # WAL directory (auto-created)
    ├── 1731059832437        # Block file (timestamped)
    ├── 1731059832556        # Block file
    ├── read_offset_idx_index.db  # Reader position index
    └── ...
```

### Topic Isolation

Walrus uses topics to isolate different data streams:

```
Node WAL Instance
├── hard_state_topic      (Raft term, vote, commit)
├── entries_topic         (Raft log entries)
└── snapshot_topic        (Compacted snapshots)
```

Each topic maintains independent:
- Write position
- Read position (checkpointed to disk)
- Block allocation

## WriteAheadLog API

### Creating a WAL

```rust
use octopii::wal::WriteAheadLog;

let wal = WriteAheadLog::new(
    PathBuf::from("/tmp/my_wal/data.wal"),
    100,                              // batch_size (unused with Walrus)
    Duration::from_millis(100),       // flush_interval
).await?;
```

### Appending Data

```rust
let data = Bytes::from("some data");
let offset = wal.append(data).await?;
// Returns monotonic offset (0, 1, 2, ...)
```

### Flushing

```rust
// Async flush (waits for background fsync)
wal.flush().await?;
```

### Reading All Entries

```rust
// For crash recovery
let entries = wal.read_all().await?;
// Returns Vec<Bytes> of all entries
```

## How It Works

### Write Path

```
Client
  │
  │ wal.append(data)
  ▼
┌─────────────────────────┐
│  WriteAheadLog wrapper  │
│  (async → sync bridge)  │
└───────────┬─────────────┘
            │ block_in_place()
            ▼
┌─────────────────────────┐
│  Walrus (sync engine)   │
│  • Serialize with rkyv  │
│  • Write to mmap block  │
│  • Schedule fsync       │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│  Background fsync       │
│  (native OS thread)     │
└─────────────────────────┘
```

### Read Path

```
wal.read_all()
  │
  ▼
┌──────────────────────────┐
│ Walrus batch_read        │
│ • Read from mmap blocks  │
│ • Deserialize entries    │
│ • Checkpoint position    │
└──────────┬───────────────┘
           │
           ▼
Vec<Bytes> (all entries)
```

### Thread Model

```
┌────────────────────────────────────────────────┐
│            Tokio Runtime                       │
│  ┌──────────────────────────────────────┐     │
│  │  Async task                          │     │
│  │    wal.append(data).await            │     │
│  └───────────────┬──────────────────────┘     │
│                  │                             │
│                  │ block_in_place()            │
│                  ▼                             │
│  ┌─────────────────────────────────────┐      │
│  │  Blocking context (dedicated thread)│      │
│  │    walrus.append_for_topic()        │      │
│  └─────────────────────────────────────┘      │
└────────────────────────────────────────────────┘
                   │
                   ▼
┌────────────────────────────────────────────────┐
│  Walrus Native Threads (outside Tokio)        │
│  • Background fsync                            │
│  • File deletion                               │
└────────────────────────────────────────────────┘
```

## Configuration

### Fsync Schedule

Controls durability vs performance:

```rust
// Fast (100ms fsync)
Duration::from_millis(100)

// Conservative (immediate fsync every write)
Duration::from_millis(0)  // Converts to FsyncSchedule::SyncEach

// Default (200ms fsync)
Duration::from_millis(200)
```

### Directory Isolation

Each WAL instance gets its own directory to prevent conflicts:

```rust
// Bad: All nodes share /tmp/wal
wal_dir: PathBuf::from("/tmp/wal")

// Good: Each node has unique directory
wal_dir: PathBuf::from("/tmp/node_1")
wal_dir: PathBuf::from("/tmp/node_2")
```

### WAL Creation Mutex

To prevent race conditions when setting `WALRUS_DATA_DIR`, WAL creation is serialized:

```rust
static WAL_CREATION_LOCK: Mutex<()> = Mutex::new(());

// Multiple concurrent WAL::new() calls are safe
let wal1 = WriteAheadLog::new(path1, ...).await?;  // Gets lock
let wal2 = WriteAheadLog::new(path2, ...).await?;  // Waits for lock
```

## Raft Integration

### Storage Topics

Raft uses three Walrus topics:

```rust
// Hard state (term, vote, commit index)
walrus.append_for_topic("hard_state", rkyv_data);

// Log entries
walrus.append_for_topic("entries", entry_data);

// Snapshots
walrus.append_for_topic("snapshot", snapshot_data);
```

### Crash Recovery

On node restart:

```rust
1. Read hard_state topic → Restore (term, vote, commit_index)
2. Read entries topic → Restore log entries
3. Read snapshot topic → Restore compacted state
4. Initialize Raft with recovered state
```

### Log Compaction

When log grows beyond 1000 entries:

```rust
1. Create snapshot of state machine
2. Write snapshot to snapshot topic
3. Checkpoint old entry reads
4. Walrus reclaims space from old blocks
```

## Performance Characteristics

### Memory

- **Write buffering**: Minimal (direct to mmap)
- **Read buffering**: 10MB batches
- **Mmap overhead**: Per-file overhead (typically 10MB blocks)

### Disk I/O

```
Write:
  ├── Immediate: mmap write (memory-backed)
  └── Deferred: fsync every 100-200ms

Read:
  └── Direct from mmap (OS page cache)
```

### Fsync Overhead

```
FsyncSchedule::SyncEach        → Every write (slow, max durability)
FsyncSchedule::Milliseconds(N) → Every N ms (fast, batched)
FsyncSchedule::NoFsync         → Never (fastest, no durability)
```

## Block File Management

### File Creation

```
1. Walrus creates 1GB pre-allocated files
2. Files are mmapped for zero-copy I/O
3. Files have 10MB blocks
4. Blocks are allocated sequentially
```

### File Deletion

```
1. File is fully allocated (no more space)
2. All blocks are checkpointed (read complete)
3. Background thread deletes file
4. Space is reclaimed
```

### File Naming

```
Timestamp-based:
  1731059832437  (ms since epoch)
  1731059832556
  1731059833102
```

## Limitations

- **No compression**: Walrus doesn't compress data
- **No encryption**: Data stored in plaintext
- **No replication**: Each node has independent WAL
- **No streaming reads**: Must read in batches
- **Fixed block size**: 10MB blocks, 1GB files
