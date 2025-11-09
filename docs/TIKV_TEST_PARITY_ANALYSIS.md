# TiKV Test Parity Analysis

**Status:** Comparing Octopii's Raft cluster tests against TiKV's test suite to identify gaps

**Date:** 2025-11-09

## Current Test Coverage (Octopii)

### ✅ What We Have (65 tests)

1. **Batch Operations** (3 tests)
   - Batch append correctness
   - Batch recovery performance
   - High throughput proposals

2. **Chaos Testing** (4 tests)
   - Crash during proposal
   - All nodes crash and recover
   - Rolling restarts
   - Rapid crash recovery cycles

3. **Message Chaos** (4 tests) ⭐ NEW
   - Message duplication idempotency
   - Out-of-order message delivery
   - Slow follower with throttling
   - Combined network chaos

4. **Cluster Scenarios** (4 tests)
   - Single node cluster
   - Three node cluster formation
   - Five node cluster
   - Leader re-election after crash

5. **Consistency** (4 tests)
   - Basic read-write consistency
   - Concurrent writes
   - All nodes crash and recover state
   - State machine consistency

6. **Durability Edge Cases** (4 tests)
   - Recovery after unclean shutdown
   - Multiple sequential restarts
   - Concurrent proposals during recovery
   - Partial replication before crash

7. **Learner Tests** (4 tests)
   - Basic learner addition
   - Multiple learners simultaneously
   - Learner promotion
   - Promotion fails when not caught up

8. **Partition Tests** (7 tests)
   - Basic partition
   - Leader isolation
   - Minority partition
   - Heal partition
   - Asymmetric partition
   - Multiple filters
   - Partition leader isolated

9. **Real Partition Behavior** (6 tests)
   - Quorum loss detection
   - Split brain prevention
   - Leader in minority partition
   - Partition healing
   - Message loss
   - One-way partition

10. **Pre-Vote Tests** (3 tests)
   - Prevents disruption
   - Stale node doesn't disrupt
   - Restarted node doesn't disrupt

11. **Short Duration Stress** (4 tests)
    - 100 proposals in 10 seconds
    - Concurrent client requests
    - Leadership churn
    - Rapid leader failures

12. **Snapshot Transfer** (3 tests)
    - Snapshot creation and compaction
    - New node catches up from snapshot
    - Space reclamation after snapshot

13. **Automatic Elections** (6 tests)
    - Basic election
    - Timeout randomization
    - Multiple candidates
    - Simultaneous elections
    - Leader step down
    - Election with partitions

14. **Basic Cluster** (9 tests)
    - Leader election
    - Proposal replication
    - Raft state persistence
    - Log compaction
    - Configuration changes
    - Learner integration
    - Network partitions
    - Node recovery
    - State machine consistency

## ❌ Missing Compared to TiKV

### High Priority

1. **Joint Consensus (ConfChangeV2)** ❌
   - TiKV extensively tests joint consensus for safer configuration changes
   - We only have basic ConfChange (V1)
   - **Needed for:** Production-safe membership changes

2. **Read Index / Linearizable Reads** ❌
   - TiKV tests read_index for linearizable reads without going through Raft log
   - We don't test read consistency guarantees
   - **Needed for:** Strong consistency guarantees

3. **Transfer Leadership** ❌
   - TiKV tests explicit leadership transfer
   - We only have implicit leader changes via elections
   - **Needed for:** Planned maintenance, load balancing

4. **Log Streaming** ❌
   - TiKV tests streaming log entries to slow followers
   - We don't have optimized catch-up mechanisms
   - **Needed for:** Efficient follower recovery

5. **Prevote Safety** ⚠️ (Partial)
   - We have basic prevote tests
   - Missing: prevote + partition + asymmetric network scenarios
   - **Needed for:** Election storm prevention

### Medium Priority

6. **Message Reordering** ✅ DONE
   - TiKV tests message delivery out of order
   - ✅ Now have MessageReorderFilter + tests
   - **Needed for:** Real-world network behavior

7. **Message Duplication** ✅ DONE
   - TiKV tests duplicate message handling
   - ✅ Now have MessageDuplicationFilter + tests
   - **Needed for:** Network reliability

8. **Slow Followers** ✅ DONE
   - TiKV tests followers that lag behind significantly
   - ✅ Now have ThrottleFilter + slow follower test
   - **Needed for:** Performance degradation handling

9. **Priority-based Elections** ❌
   - TiKV supports priority for leadership preference
   - We don't have priority mechanisms
   - **Needed for:** Datacenter locality, resource optimization

10. **Witness Nodes** ❌
    - TiKV has witness nodes (non-voting, no data)
    - We only have learners (non-voting, with data)
    - **Needed for:** Quorum without full replicas

### Low Priority (Nice to Have)

11. **Prometheus Metrics Validation** ❌
    - TiKV validates metrics during tests
    - We don't validate observability
    - **Needed for:** Production monitoring

12. **Disk Full Scenarios** ❌
    - TiKV tests behavior when disk fills up
    - We don't test resource exhaustion
    - **Needed for:** Graceful degradation

13. **Clock Skew** ❌
    - TiKV tests with simulated clock drift
    - We don't simulate time anomalies
    - **Needed for:** Multi-datacenter deployments

14. **Quiesce/Resume** ❌
    - TiKV tests pausing/resuming Raft groups
    - We don't have quiesce support
    - **Needed for:** Resource efficiency

15. **Flow Control** ❌
    - TiKV tests backpressure mechanisms
    - We don't test flow control
    - **Needed for:** Preventing OOM

## 🔧 Infrastructure Gaps

### What TiKV Has That We Don't

1. **Deterministic Testing Framework** ❌
   - TiKV uses deterministic simulation testing
   - We use real time and real network
   - **Impact:** Harder to reproduce bugs

2. **Property-Based Testing** ❌
   - TiKV uses QuickCheck for property testing
   - We only have example-based tests
   - **Impact:** Miss edge cases

3. **Jepsen-style Linearizability Checker** ❌
   - TiKV validates linearizability formally
   - We manually verify consistency
   - **Impact:** Can't prove safety

4. **Fault Injection Framework** ⚠️ (Partial)
   - We have network filters
   - Missing: disk errors, OOM, CPU throttling
   - **Impact:** Limited chaos testing

5. **Benchmark Suite** ❌
   - TiKV has extensive benchmarks
   - We don't measure performance systematically
   - **Impact:** No performance regression detection

## 📊 Coverage Comparison

| Category | TiKV | Octopii | Gap |
|----------|------|---------|-----|
| Basic Raft | ✅ | ✅ | None |
| Learners | ✅ | ✅ | None |
| Snapshots | ✅ | ✅ | None |
| Partitions | ✅ | ✅ | None |
| Prevote | ✅ | ⚠️ | Minor |
| Joint Consensus | ✅ | ❌ | **Major** |
| Read Index | ✅ | ❌ | **Major** |
| Leadership Transfer | ✅ | ❌ | **Major** |
| Message Chaos | ✅ | ✅ | None |
| Resource Limits | ✅ | ❌ | Medium |
| Deterministic Tests | ✅ | ❌ | **Major** |
| Formal Verification | ✅ | ❌ | **Major** |

## 🎯 Recommendations (Priority Order)

### Phase 1: Critical Safety (Production Blocker)
1. **Implement Joint Consensus tests** - Safest membership changes
2. **Add Read Index tests** - Linearizable read guarantees
3. **Implement Leadership Transfer** - Planned maintenance support
4. **Add Linearizability Checker** - Prove safety formally

### Phase 2: Operational Maturity
5. **Message reordering/duplication tests** - Real network behavior
6. **Slow follower handling** - Performance degradation
7. **Flow control tests** - Prevent OOM
8. **Add Prometheus metrics validation** - Observability

### Phase 3: Advanced Features
9. **Witness nodes** - Efficient quorum
10. **Priority elections** - Datacenter awareness
11. **Deterministic testing framework** - Reproducible bugs
12. **Property-based testing** - Edge case discovery

### Phase 4: Polish
13. **Disk full scenarios** - Graceful degradation
14. **Clock skew simulation** - Multi-DC
15. **Quiesce/Resume** - Resource efficiency
16. **Benchmark suite** - Performance tracking

## 💡 Quick Wins Completed ✅

1. ✅ **Message Duplication Filter** - DONE
   - Added MessageDuplicationFilter to filter infrastructure
   - Tests idempotency of message handling

2. ✅ **Message Reordering Filter** - DONE
   - Added MessageReorderFilter for out-of-order delivery
   - Tests Raft robustness to message ordering

3. ✅ **Slow Follower Test** - DONE
   - Added ThrottleFilter for network throttling
   - Verifies followers can catch up despite lag

## 💡 Next Quick Wins

1. **Prometheus Test Helper** - 2 days
   - Add metric collection to test infrastructure
   - Validate key metrics (leader changes, proposals, etc.)

4. **Disk Full Simulation** - 2 days
   - Mock WAL append errors
   - Verify graceful handling

5. **Basic Linearizability Checker** - 3 days
   - Record all operations and results
   - Verify linearizability post-test
   - Use existing algorithms (e.g., Knossos)

## 📈 Current Status

**Test Count:** 65 tests (+4 message chaos tests)
**Coverage:** ~65% of TiKV's core scenarios
**Major Gaps:** 4 (Joint Consensus, Read Index, Transfer Leadership, Formal Verification)
**Recent Improvements:** ✅ Message duplication/reordering/throttling (2025-11-09)
**Maturity:** **Beta** - Good for experimentation, needs work for production

## 🚀 To Reach Production Parity

**Estimated Work:**
- Phase 1 (Critical): 3-4 weeks
- Phase 2 (Operational): 2-3 weeks
- Phase 3 (Advanced): 4-6 weeks
- Phase 4 (Polish): 2-3 weeks

**Total:** ~3 months of focused development

## Notes

- Our test infrastructure (filters, crash/restart, learners) is solid
- We have good coverage of basic Raft scenarios
- Main gaps are advanced features (joint consensus, read index) and formal verification
- Quick wins can improve confidence significantly without major refactoring
