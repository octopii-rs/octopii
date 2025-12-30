# Simulation Walrus Coverage Plan (StrictlyAtOnce + SyncEach)

Scope: cover all Octopii production Walrus usage paths with deterministic simulation tests.

## Remaining Coverage Gaps
- Raft storage recovery topics: `raft_conf_state(_recovery)` and `raft_snapshot`.
- Raft log recovery exact flow: `batch_read_for_topic("raft_log_recovery", checkpoint=true)` with snapshot index filtering.
- Hard/conf state recovery flow: `batch_read_for_topic(..., checkpoint=false)` and “latest wins”.
- State machine snapshot + replay flow: snapshot read (checkpoint=true) then log replay (checkpoint=true).
- WriteAheadLog `read_all` behavior: `batch_read_for_topic("wal_data", checkpoint=true)` loop and empty-read termination.

## Variants / Invariants to Enforce
- **StrictlyAtOnce read semantics**
  - `read_next(checkpoint=true)` advances cursor; entries do not repeat after restart.
  - `read_next(checkpoint=false)` does not advance cursor; entries repeat after restart.
- **Batch reads**
  - `batch_read_for_topic(checkpoint=true)` advances cursor; second recovery does not replay.
  - `batch_read_for_topic(checkpoint=false)` does not advance cursor; recovery can replay.
- **Dual-topic durability**
  - For any successful append, both primary and recovery topics are readable after crash.
  - On failure, neither topic should expose partial or torn entries.
- **Snapshot precedence**
  - Latest snapshot wins.
  - Replay only entries after snapshot index.
- **Latest-wins semantics**
  - Hard/conf state recovery picks the last valid entry.

## Planned Test Additions
- [x] **Raft hard/conf state recovery test**
  - Simulate multiple writes to `raft_hard_state(_recovery)` and `raft_conf_state(_recovery)`.
  - Use `batch_read_for_topic(..., checkpoint=false)` and assert “latest wins”.
- [x] **Raft snapshot recovery test**
  - Append multiple snapshots, simulate crash, recover with `read_next(checkpoint=true)` loop.
  - Assert last snapshot is used and older ones are ignored.
- [x] **Raft log recovery with snapshot index**
  - Append log entries + a snapshot; recover via `batch_read_for_topic("raft_log_recovery", true)`.
  - Assert entries ≤ snapshot index are discarded; > snapshot index are kept.
- [x] **State machine snapshot + replay test**
  - Append snapshot + operations, recover via snapshot then op log.
  - Assert replay applies only after snapshot state.
- [x] **WriteAheadLog read_all test**
  - Append N entries, crash, recover using `read_all`.
  - Assert all entries are returned exactly once and cursor is advanced.

## CI Integration
- [x] Add the new tests to `tests/simulation.rs` or a dedicated simulation test file.
- [ ] Keep non-stress tests in `.github/workflows/ci.yml`.
- [ ] Keep stress variants (long iterations) in `.github/workflows/simulation-stress.yml`.

## Execution Checklist
- [x] Implement tests with deterministic seeds.
- [x] Verify per-test unique WAL roots.
- [ ] Run each new test locally once (single test target) for sanity.
- [ ] Confirm CI picks up new tests.
