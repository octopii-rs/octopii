# Raft Log Replication Bug - Diagnostic Report

**Date**: 2025-11-08
**Branch**: `claude/raft-improvements-analysis-011CUvozkUzpxyut8m7n8J3L`
**Status**: ❌ BLOCKED - Critical bug preventing automatic election tests

## Failing Test

**Test File**: `tests/raft_comprehensive/automatic_election_tests.rs`
**Test Name**: `test_automatic_election_after_leader_failure`
**Command**: `cargo test --test raft_comprehensive_test test_automatic_election_after_leader_failure -- --nocapture`

## The Core Issue

**Followers never receive or persist log entries from the leader, despite network connectivity being established.**

### Symptom
- Leader sends ONLY empty AppendEntries messages (heartbeats) to followers
- All messages show `entries: []` even though leader has 20+ entries to send
- Followers respond successfully to heartbeats
- Leader's progress tracker thinks followers are "caught up"
- Followers remain at `last_index=0` permanently
- Test crashes with: `panic: to_commit 23 is out of range [last_index 0]`

### Evidence from Logs
```
[Node 1] Persisting 1 new entries to storage   // Leader persists 22 entries
[Node 2] Raft ready: 0 new entries              // Follower NEVER persists
[Node 3] Raft ready: 0 new entries              // Follower NEVER persists

Received Raft request: AppendEntries {
    term: 2,
    leader_id: 1,
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [],        // ⚠️ ALWAYS EMPTY
    leader_commit: 0
}
```

## Root Cause Hypothesis

**State Mismatch Between Leader and Followers**

1. **Leader State**:
   - Bootstraps with snapshot at index 1 containing `voters: [1]`
   - Creates entries at indices 2-22 (20 proposals)
   - Has `first_index=2, last_index=22, snapshot_index=1`

2. **Follower State**:
   - Starts with COMPLETELY EMPTY log
   - No snapshot, no entries
   - Has `first_index=0, last_index=0, snapshot_index=0`

3. **raft-rs Behavior**:
   - When adding a peer, raft-rs initializes their `next_index` in the progress tracker
   - **BUG**: The `next_index` appears to be set incorrectly (possibly to last_index+1)
   - raft-rs thinks follower already has the entries, only sends heartbeats
   - **SHOULD** send MsgSnapshot when peer is too far behind, but doesn't

## What Has Been Fixed

### ✅ Critical Bugs Resolved

1. **Incorrect Cluster Bootstrap** (`src/node.rs:191-195`)
   - BEFORE: All nodes pre-populated with full voter list via `storage.set_conf_state()`
   - AFTER: Removed this code entirely
   - Now only leader bootstraps (in `src/raft/mod.rs`)

2. **Message Handling Bug** (`src/node.rs:755-790`)
   - BEFORE: Network handler called `ready()` and `advance()` without persisting entries
   - AFTER: Send immediate ACK response, let main ready handler process everything
   - Entries are now properly persisted... on the leader only

3. **Joint Consensus Wait** (`src/node.rs:515-559`)
   - Added logic to wait for `voters_outgoing.is_empty()` before next ConfChange
   - Uses `Configuration.to_conf_state()` API

### ✅ Diagnostics Added

- Node ID in all ready handler logs: `[Node X]`
- INFO level logging for entry persistence
- RPC message content logging
- "Received Raft request" shows full AppendEntries details

## What Hasn't Worked

❌ **Tried but failed**:
1. Learner-first pattern (add_learner → promote_learner)
2. Direct voter add pattern (add_peer)
3. Log compaction before adding peers (skipped due to threshold)
4. Removing leader bootstrap snapshot (causes campaign failure)

## Instructions for Future Investigation

### Key Files to Review

1. **Initialization**:
   - `src/raft/mod.rs:32-45` - Leader bootstrap with snapshot
   - `src/node.rs:156-199` - OctopiiNode initialization
   - `tests/raft_comprehensive/automatic_election_tests.rs:24-68` - Test setup

2. **Message Handling**:
   - `src/node.rs:765-792` - Incoming Raft message processing
   - `src/node.rs:907-1000` - Ready handler (outgoing messages)
   - `src/raft/rpc.rs:6-104` - Raft↔RPC message conversion

3. **Storage**:
   - `src/raft/storage.rs:469-516` - Log compaction
   - `src/raft/storage.rs:342-377` - Snapshot application
   - `src/raft/storage.rs:520-528` - initial_state()

### Online Resources to Study

#### raft-rs Documentation
- **Main docs**: https://docs.rs/raft/latest/raft/
- **RawNode API**: https://docs.rs/raft/latest/raft/raw_node/struct.RawNode.html
- **Storage trait**: https://docs.rs/raft/latest/raft/trait.Storage.html
- **ProgressTracker**: https://docs.rs/raft/latest/raft/struct.ProgressTracker.html
- **Configuration**: https://docs.rs/raft/latest/src/raft/tracker.rs.html

#### TiKV Examples (CRITICAL!)
- **five_mem_node**: https://github.com/tikv/raft-rs/blob/master/examples/five_mem_node/main.rs
  - Shows proper follower initialization pattern
  - Look for `initialize_raft_from_message()` function
  - Shows how followers start with `raft_group: None`
  - Shows snapshot handling

- **How to add nodes**: https://github.com/tikv/raft-rs/blob/master/examples/five_mem_node/README.md

#### Walrus (WAL) Documentation
- **Main docs**: https://docs.rs/walrus/latest/walrus/
- We use Walrus for: `TOPIC_LOG`, `TOPIC_SNAPSHOT`, `TOPIC_HARD_STATE`, `TOPIC_CONF_STATE`

#### Relevant Issues
- **five_mem_node panics**: https://github.com/tikv/raft-rs/issues/205
- **Joint consensus**: https://github.com/tikv/raft-rs/issues/378

### Specific Things to Investigate

1. **Progress Tracker Initialization** ⭐
   ```
   QUESTION: When leader calls add_peer(), what does raft-rs set as the peer's next_index?
   LOCATION: Look in raft-rs source for AddNode handling in tracker.rs
   EXPECTED: Should be set to leader's first_index (2) or snapshot_index+1
   ACTUAL: Appears to be set to last_index+1 (23), causing raft-rs to think peer is caught up
   ```

2. **Snapshot Transfer Trigger** ⭐⭐
   ```
   QUESTION: When should raft-rs send MsgSnapshot instead of MsgAppend?
   LOCATION: raft-rs RawNode::ready() logic for new peers
   KEY: If peer's next_index < leader's first_index, should send snapshot
   BUG: This condition never triggers, even though follower is at index 0
   ```

3. **Follower Initialization from Leader** ⭐⭐⭐
   ```
   CRITICAL QUESTION: How should followers initialize when receiving first message?

   TiKV Pattern (five_mem_node):
   - Followers start with raft_group: None
   - initialize_raft_from_message() creates RawNode when first message arrives
   - Receives snapshot from leader FIRST, then regular entries

   Our Pattern:
   - Followers start with RawNode already created
   - Empty storage (no snapshot)
   - Never receive initial snapshot

   ACTION: Compare our initialization with five_mem_node line-by-line
   ```

4. **Leader's apply_snapshot Behavior**
   ```
   QUESTION: Does the leader's bootstrap snapshot affect how it replicates?

   Current leader state:
   - snapshot_metadata.index = 1
   - entries[0].index = 2 (first actual entry)

   When adding peer at index 0:
   - Should send snapshot first?
   - Or should send entries starting from index 1?

   ACTION: Check RaftLog::maybe_send_snapshot() conditions in raft-rs source
   ```

## Debugging Commands

```bash
# Run failing test with full output
cargo test --test raft_comprehensive_test test_automatic_election_after_leader_failure -- --nocapture

# Check which nodes persist entries
cargo test --test raft_comprehensive_test test_automatic_election_after_leader_failure -- --nocapture 2>&1 | grep "Persisting.*entries"

# Check what messages followers receive
cargo test --test raft_comprehensive_test test_automatic_election_after_leader_failure -- --nocapture 2>&1 | grep "Received Raft request"

# Check if snapshots are sent
cargo test --test raft_comprehensive_test test_automatic_election_after_leader_failure -- --nocapture 2>&1 | grep -i snapshot
```

## Potential Solutions to Try

### Option 1: Match TiKV Lazy Initialization ⭐⭐⭐
**Most Promising**

Study `five_mem_node/main.rs` carefully:
- Create followers with `raft_group: None`
- Implement `initialize_raft_from_message()`
- Let followers bootstrap from leader's first message
- This may automatically handle snapshot transfer

### Option 2: Force Snapshot Send
```rust
// In add_peer(), after ConfChange:
let snapshot = create_snapshot_at_current_state();
manually_send_snapshot_to_peer(peer_id, snapshot);
```

### Option 3: Initialize Followers with Leader's Bootstrap Snapshot
```rust
// In follower initialization:
if !is_leader {
    // Start with a minimal snapshot at index 0
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().index = 0;
    snapshot.mut_metadata().term = 0;
    storage.apply_snapshot(snapshot)?;
}
```

### Option 4: Use ConfChangeV2 with Learner-First
Study how TiKV does membership changes:
- Use `ConfChangeV2` instead of `ConfChange`
- Explicitly handle learner → voter transition
- May have better snapshot handling

## Git Information

**Branch**: `claude/raft-improvements-analysis-011CUvozkUzpxyut8m7n8J3L`

**Key Commits**:
- `f2a8763` - Diagnose log replication issue (this document's state)
- `d5e43de` - Add log compaction diagnostics
- `7f2aab3` - Add RPC response after stepping
- `b3ea664` - Fix cluster init and message handling bugs

**To Resume**:
```bash
git checkout claude/raft-improvements-analysis-011CUvozkUzpxyut8m7n8J3L
cargo test --test raft_comprehensive_test test_automatic_election_after_leader_failure -- --nocapture
```

## Success Criteria

Test passes when:
1. ✅ Leader creates 20+ log entries
2. ✅ Nodes 2 and 3 added to cluster
3. ✅ **Nodes 2 and 3 receive and persist all 20+ entries from leader**
4. ✅ All nodes have `last_index >= 22`
5. ✅ Leader failover triggers automatic election
6. ✅ New leader elected from nodes 2 or 3
7. ✅ Cluster continues operating with new leader

Currently stuck at step 3 - followers never persist entries.

---

**TLDR for Future You**: The fundamental problem is that raft-rs won't send actual log entries to new peers that start with empty logs. Leader only sends heartbeats. Study the TiKV five_mem_node example's lazy initialization pattern - that's likely the key. The issue is NOT network, NOT message handling, NOT configuration - it's how we're initializing the Raft state for new peers.
