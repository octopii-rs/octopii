# Cluster Simulation Coverage Plan (StrictlyAtOnce + SyncEach)

Scope: deterministic simulation of full Octopii cluster behavior (no shipping lane/chunk transfer yet), covering the same breadth as the Walrus simulation suites and aligned with production invariants.

## Current Coverage (as of now)
- 3-node cluster sim with deterministic fault plan and WAL/VFS error injection.
- Stress suite with seed sweeps, deterministic fault profiles, and parallel seed runs.
- Seed-driven VFS error rates, deterministic transport faults (reorder/timeout/bandwidth/partition).
- CI workflows run cluster tests on push and stress runs.

## Gaps vs Walrus Simulation Suites
- No larger cluster sizes (5/7/9).
- No recovery/crash cycles (cold restart, double-crash, staged recovery) for cluster.
- No long-running stress tiers (10k+ ops) with higher fault rates.
- No deterministic seed sweep matching Walrus `stress_random_seeds_*` pattern (pseudo-random seed generator loop).
- No partial write injection variants.
- No explicit invariants for cluster state transitions beyond existing sim_asserts.
- No cluster-specific “strict” workflow mirroring Walrus strict tests (serial, low parallelism).

## Invariants to Enforce (Production `sim_assert`)
Use the same pattern as existing production asserts (panics in simulation):
- **Leadership invariants**
  - `propose()` only allowed on `state == Leader`.
  - `current_leader` in membership when present.
  - `state == Leader` implies `current_leader == self` (best-effort, allow brief metric skew).
- **Log invariants**
  - `committed` <= `last_log_id` (already in log store checks; keep).
  - `last_purged_log_id` monotonic.
- **State machine invariants**
  - `last_applied` monotonic and <= `committed`.
  - Snapshot index never decreases.
- **Transport invariants**
  - Deterministic fault plan events applied in timestamp order (no time travel).
- **WAL invariants**
  - Existing `read_all` and topic count assertions are preserved; do not disable.

## Deterministic Seed Strategy (Parity with Walrus)
Adopt the same deterministic pseudo-random seed loop as `stress_random_seeds_10_15pct` and `stress_random_seeds_15_20pct_long`:
- Generate seeds via the xorshift loop.
- Map error rates to 10–15% (short) and 15–20% (long) using `seed % 6`.
- Use the seed as the single source of truth for:
  - VFS `io_error_rate`
  - Transport fault schedule
  - Partition churn plan
  - Bandwidth/reorder/timeout parameters

## Cluster Size Matrix
Introduce parameterized cluster builders:
- **Sizes**: 3, 5, 7 (optionally 9 once 7 is stable).
- **Topologies**: full mesh peer list (all-to-all) for deterministic coverage.
- **Leadership**: single initial leader (node 1) per run.

## Test Matrix (to match Walrus coverage depth)
### 1) Deterministic correctness (no faults beyond VFS error rate)
- 3/5/7 nodes, seed sweep (pseudo-random seed loop, 20 seeds).
- Operations: propose N writes, read on all nodes, assert equality.

### 2) Fault injection (network + VFS)
- 3/5/7 nodes, seed sweep with:
  - Reorder/timeout/bandwidth + partition churn.
  - VFS error rate 10–15% (short) and 15–20% (long).
- Assertions:
  - Leader eventually elected.
  - All nodes converge on the same key/value state.
  - Read index and query results match across nodes.

### 3) Recovery and crash cycles (cluster-level)
- **Single node crash**: kill leader, restart, re-elect, verify data.
- **Double crash**: crash/restart cycles during writes.
- **Crash during recovery**: restart during snapshot/replication.
- **Staged recovery**: bring up minority, then full cluster.

### 4) Snapshot/compaction scenarios
- Force snapshot via `snapshot_lag_threshold` + heavy log writes.
- Verify snapshot application and log truncation across nodes.

### 5) Partial write injection
- Enable VFS `enable_partial_writes` for some runs.
- Use same error-rate bands as Walrus partial-write tests.

## Harness Work Needed
1) **Parameterized cluster builder**
   - Build N nodes, deterministic address allocation, unique WAL roots.
   - Encapsulate start/restart logic and cleanup.
2) **Seed runner (cluster-specific)**
   - Mirror `sim_runner` behavior: deterministic seed list, parallel jobs.
3) **Fault plan generator**
   - Extend current plan to support arbitrary N, using seed to pick:
     - Pairwise reorder/timeout/bandwidth
     - Partition groups (minority/majority splits)
     - Fault schedule times

## CI Parity (match Walrus workflows)
- **Strict suite**: serial runs of recovery/crash/partial-write cluster tests.
- **Stress suite**: `stress_random_seeds_*` cluster variants.
- **Parallel sweep**: seed range split across threads.
- **Push coverage**: deterministic core cluster tests on every push.

## Step-by-Step Plan
1) **Audit & gap list** against Walrus sim suites; confirm missing coverage areas.
2) **Add parameterized cluster harness** (N nodes) + seed-based fault plan.
3) **Implement cluster recovery/crash tests** (single, double, staged).
4) **Implement snapshot/compaction tests** with forced thresholds.
5) **Add partial-write variants** (VFS partial writes + error-rate bands).
6) **Add deterministic seed sweep tests** matching Walrus patterns.
7) **Wire CI** with strict/stress/parallel workflows + cleanup.
8) **Run locally**: short seed sweeps + single recovery test per size.

## Execution Checklist
- [ ] Cluster sizes 3/5/7 supported with shared harness.
- [ ] Deterministic seed sweeps (20 seeds) using Walrus seed generator.
- [ ] Fault profiles for reorder/timeout/bandwidth + partition churn.
- [ ] Crash/recovery tests for leader and follower.
- [ ] Snapshot/compaction tests.
- [ ] Partial write tests.
- [ ] CI workflows updated to include all variants.
- [ ] Local sanity runs done for each new suite.
