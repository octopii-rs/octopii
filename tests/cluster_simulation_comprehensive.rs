//! Comprehensive Cluster Simulation Tests
//!
//! This test suite provides thorough coverage of cluster behavior under various
//! fault conditions, matching the exhaustiveness of the Walrus simulation tests.
//!
//! Test categories:
//! - Basic correctness (no faults)
//! - I/O fault injection
//! - Network fault injection
//! - Combined faults
//! - Crash recovery
//! - Membership changes
//! - Snapshot & log truncation
//! - Stress tests

mod common;

#[cfg(all(feature = "simulation", feature = "openraft"))]
mod comprehensive_tests {
    use crate::common::cluster_sim::{
        walrus_error_rate, walrus_seed_sequence, ClusterHarness, ClusterParams, CrashReason,
        FaultProfile, ValidationMode,
    };
    use futures::FutureExt;
    use octopii::wal::wal::vfs::sim::{self, RecoveryCrashPoint};

    // =========================================================================
    // Configuration helpers
    // =========================================================================
    //
    // Env knobs:
    // - CLUSTER_SEED_START / CLUSTER_SEED_COUNT
    // - CLUSTER_OPS / CLUSTER_STRESS_OPS
    // - CLUSTER_STRESS_SEED_COUNT / CLUSTER_WALRUS_SEED_COUNT

    fn seed_range() -> (u64, u64) {
        let start = std::env::var("CLUSTER_SEED_START")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let count = std::env::var("CLUSTER_SEED_COUNT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20);
        (start, count)
    }

    fn ops_count() -> usize {
        std::env::var("CLUSTER_OPS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1000)
    }

    fn stress_ops() -> usize {
        std::env::var("CLUSTER_STRESS_OPS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(5000)
    }

    fn enable_full_verification(params: &mut ClusterParams) {
        params.verify_full = true;
        params.verify_invariants = true;
        params.invariant_interval = 25;
        params.verify_logs = true;
        params.verify_each_op = true;
    }

    fn enable_durability(params: &mut ClusterParams) {
        params.verify_durability = true;
    }

    fn stress_seed_count() -> usize {
        std::env::var("CLUSTER_STRESS_SEED_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(5)
    }

    fn walrus_seed_count() -> usize {
        std::env::var("CLUSTER_WALRUS_SEED_COUNT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8)
    }

    /// Smaller ops count for tests with high overhead (membership, 7-node clusters)
    fn small_ops_count() -> usize {
        ops_count() / 4
    }

    // =========================================================================
    // 4.1 Basic Correctness (No Faults)
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn basic_correctness_3_nodes_no_faults() {
        let mut params = ClusterParams::new(3, 100001, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(ops_count()).await;

        let (commits, failures, verifies) = harness.oracle_stats();
        assert_eq!(failures, 0, "no failures expected without faults");
        assert_eq!(commits, verifies, "all commits should be verified");

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn basic_correctness_5_nodes_no_faults() {
        let mut params = ClusterParams::new(5, 100002, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(ops_count()).await;

        let (commits, failures, _) = harness.oracle_stats();
        assert_eq!(failures, 0, "no failures expected without faults");
        assert!(commits >= ops_count() as u64);

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn basic_correctness_7_nodes_no_faults() {
        let mut params = ClusterParams::new(7, 100003, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await; // Smaller workload for 7 nodes

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn basic_correctness_seed_sweep_no_faults() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::None);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    // =========================================================================
    // 4.2 I/O Fault Injection
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn io_faults_5pct_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsLight);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn io_faults_10pct_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.10);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsMedium);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn io_faults_partial_writes_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::PartialWrites);
            params.enable_partial_writes = true;
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn io_faults_15pct_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.15);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsHeavy);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn io_faults_20pct_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count.min(10)) {
            let error_rate = walrus_error_rate(seed, 0.20);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsHeavy);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn io_faults_25pct_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count.min(5)) {
            let error_rate = walrus_error_rate(seed, 0.25);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsHeavy);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    // =========================================================================
    // 4.3 Network Fault Injection
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn network_faults_partition_churn() {
        let mut params = ClusterParams::new(5, 200001, 0.0, FaultProfile::PartitionChurn);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(ops_count()).await;
        harness.cleanup();
    }

    // SplitBrain / leader isolation case (historically flaky under OpenRaft assertions)
    #[tokio::test(flavor = "current_thread")]
    async fn network_faults_leader_isolation() {
        let mut params = ClusterParams::new(5, 200002, 0.0, FaultProfile::SplitBrain);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run some operations before partition
        harness.run_oracle_workload(small_ops_count()).await;

        // Isolate the leader
        harness.isolate_leader().await;
        harness.tick(100, 50).await; // Let new leader be elected

        // Run more operations (should succeed with new leader)
        harness.run_oracle_workload(small_ops_count()).await;

        // Heal and verify convergence
        harness.heal_partitions();
        harness.tick(100, 50).await;

        assert!(harness.verify_no_divergence().await, "log divergence detected");
        harness.cleanup();
    }

    // Rapid partition/heal cycles (historically flaky under OpenRaft assertions)
    #[tokio::test(flavor = "current_thread")]
    async fn network_faults_flapping_connectivity() {
        let mut params = ClusterParams::new(5, 200003, 0.0, FaultProfile::FlappingConnectivity);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn network_faults_reorder_timeout_bandwidth() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::ReorderTimeoutBandwidth);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    // =========================================================================
    // 4.4 Combined Faults
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn combined_faults_io_and_network() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.08);
            let mut params = ClusterParams::new(5, seed, error_rate, FaultProfile::CombinedNetworkAndIo);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn combined_faults_partition_with_partial_writes() {
        let (start, count) = seed_range();
        for seed in start..(start + count.min(10)) {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(5, seed, error_rate, FaultProfile::PartitionChurn);
            params.enable_partial_writes = true;
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(small_ops_count()).await;
            harness.cleanup();
        }
    }

    // =========================================================================
    // 4.5 Crash Recovery (Conservative - CI)
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn crash_recovery_single_follower() {
        let mut params = ClusterParams::new(5, 300001, 0.05, FaultProfile::None);
        params.require_all_nodes = false; // Only require quorum during validation
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run some operations
        harness.run_oracle_workload(small_ops_count()).await;

        // Crash a follower (not the leader)
        let leader_id = leader.unwrap();
        let follower_idx = harness.nodes.iter()
            .position(|n| n.id() != leader_id)
            .unwrap();
        harness.crash_and_recover_node(follower_idx, CrashReason::Scheduled).await;

        // Run more operations - give extra time for recovery
        harness.tick(100, 50).await;
        harness.run_oracle_workload(small_ops_count()).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crash_recovery_leader() {
        let mut params = ClusterParams::new(5, 300002, 0.05, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run some operations
        harness.run_oracle_workload(small_ops_count()).await;

        // Crash the leader
        let leader_id = leader.unwrap();
        let leader_idx = harness.nodes.iter()
            .position(|n| n.id() == leader_id)
            .unwrap();
        harness.crash_and_recover_node(leader_idx, CrashReason::Scheduled).await;

        // New leader should be elected
        let new_leader = harness.wait_for_leader().await;
        assert!(new_leader.is_some(), "new leader election failed");

        // Run more operations
        harness.run_oracle_workload(small_ops_count()).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crash_recovery_cycles_3() {
        let mut params = ClusterParams::new(5, 300003, 0.05, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;

        for cycle in 0..3 {
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed in cycle {}", cycle);

            harness.run_oracle_workload(small_ops_count() / 2).await;

            // Crash a random node
            let crash_idx = (cycle % harness.nodes.len()) as usize;
            harness.crash_and_recover_node(crash_idx, CrashReason::Scheduled).await;
        }

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crash_recovery_cycles_5() {
        let mut params = ClusterParams::new(5, 300004, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;

        for cycle in 0..5 {
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed in cycle {}", cycle);

            harness.run_oracle_workload(small_ops_count() / 2).await;

            let crash_idx = ((cycle * 3) % harness.nodes.len()) as usize;
            harness.crash_and_recover_node(crash_idx, CrashReason::Scheduled).await;
        }

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crash_recovery_cycles_8() {
        let mut params = ClusterParams::new(5, 300005, 0.05, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;

        for cycle in 0..8 {
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed in cycle {}", cycle);

            harness.run_oracle_workload(small_ops_count() / 3).await;

            // Crash a random node
            let crash_idx = ((cycle * 7) % harness.nodes.len()) as usize;
            harness.crash_and_recover_node(crash_idx, CrashReason::Scheduled).await;
        }

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crash_during_recovery_single_node() {
        let mut params = ClusterParams::new(3, 300006, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 2).await;

        harness.crash_node(1, CrashReason::Scheduled).await;
        harness.tick(50, 50).await;

        let crash_points = [
            RecoveryCrashPoint::AfterFileList,
            RecoveryCrashPoint::AfterBlockHeader,
            RecoveryCrashPoint::AfterChainRebuild,
        ];

        for point in crash_points {
            sim::set_recovery_crash_point(point);
            let result = std::panic::AssertUnwindSafe(harness.recover_node(1))
                .catch_unwind()
                .await;
            assert!(result.is_err(), "expected crash at {:?}", point);
        }

        sim::set_recovery_crash_point(RecoveryCrashPoint::None);
        harness.recover_node(1).await;
        harness.tick(100, 50).await;
        harness.run_oracle_workload(small_ops_count() / 2).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn double_crash_recovery_cycles() {
        let mut params = ClusterParams::new(3, 300007, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        let crash_points = [
            RecoveryCrashPoint::AfterBlockHeader,
            RecoveryCrashPoint::AfterChainRebuild,
            RecoveryCrashPoint::AfterEntryCount(15),
        ];

        for cycle in 0..3 {
            harness.run_oracle_workload(small_ops_count() / 2).await;
            harness.crash_node(1, CrashReason::Scheduled).await;
            harness.tick(50, 50).await;

            let point = crash_points[cycle % crash_points.len()];
            sim::set_recovery_crash_point(point);
            let result = std::panic::AssertUnwindSafe(harness.recover_node(1))
                .catch_unwind()
                .await;
            assert!(result.is_err(), "expected crash at {:?}", point);
        }

        sim::set_recovery_crash_point(RecoveryCrashPoint::None);
        harness.recover_node(1).await;
        harness.tick(100, 50).await;
        harness.run_oracle_workload(small_ops_count() / 2).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durability_must_survive_after_crash() {
        let mut params = ClusterParams::new(3, 300008, 0.0, FaultProfile::None);
        enable_durability(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 2).await;
        harness.crash_node(1, CrashReason::Scheduled).await;
        harness.tick(50, 50).await;
        harness.recover_node(1).await;
        harness.tick(100, 50).await;

        harness.verify_must_survive().await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn durability_partial_writes_crash_cycles_smoke() {
        let seed = 300009;
        let error_rate = walrus_error_rate(seed, 0.08);
        let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::PartialWrites);
        enable_durability(&mut params);
        params.enable_partial_writes = true;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        for _ in 0..3 {
            harness.run_oracle_workload(small_ops_count() / 4).await;
            harness.crash_and_recover_node(1, CrashReason::Scheduled).await;
            harness.verify_must_survive().await;
        }

        harness.run_oracle_workload(small_ops_count() / 4).await;
        harness.verify_must_survive().await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_seed_smoke() {
        let seed = 500010;
        let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::None);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 2).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_io_faults_smoke() {
        let seed = 500011;
        let error_rate = walrus_error_rate(seed, 0.05);
        let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsLight);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_partial_writes_smoke() {
        let seed = 500012;
        let error_rate = walrus_error_rate(seed, 0.05);
        let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::PartialWrites);
        enable_full_verification(&mut params);
        params.enable_partial_writes = true;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_crash_recovery_smoke() {
        let seed = 500013;
        let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::None);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;
        harness.crash_and_recover_node(1, CrashReason::Scheduled).await;
        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_seed_sweep_smoke() {
        let seeds = walrus_seed_sequence(0x9e3779b97f4a7c15, 20);
        for seed in seeds {
            let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::None);
            enable_full_verification(&mut params);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);
            harness.run_oracle_workload(small_ops_count() / 3).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_fault_seed_sweep_smoke() {
        let seeds = walrus_seed_sequence(0xd1b54a32d192ed03, 20);
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsLight);
            enable_full_verification(&mut params);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);
            harness.run_oracle_workload(small_ops_count() / 3).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_combined_faults_smoke() {
        let seed = 500014;
        let error_rate = walrus_error_rate(seed, 0.05);
        let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::CombinedNetworkAndIo);
        enable_full_verification(&mut params);
        params.enable_partial_writes = true;
        params.require_all_nodes = false;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_membership_smoke() {
        let mut params = ClusterParams::new(3, 500015, 0.0, FaultProfile::None);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;
        let new_idx = harness.add_node().await.expect("add node failed");
        harness.promote_node(new_idx).await.expect("promote failed");
        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_membership_io_faults_smoke() {
        let seed = 500019;
        let error_rate = walrus_error_rate(seed, 0.05);
        let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsLight);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;
        let new_idx = harness.add_node().await.expect("add node failed");
        harness.promote_node(new_idx).await.expect("promote failed");
        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_network_faults_smoke() {
        let seed = 500016;
        let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::ReorderTimeoutBandwidth);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_crash_recovery_io_faults_smoke() {
        let seed = 500020;
        let error_rate = walrus_error_rate(seed, 0.05);
        let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsLight);
        enable_full_verification(&mut params);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 3).await;
        harness.crash_and_recover_node(1, CrashReason::Scheduled).await;
        harness.run_oracle_workload(small_ops_count() / 3).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_snapshot_smoke() {
        let mut params = ClusterParams::new(3, 500017, 0.0, FaultProfile::None);
        enable_full_verification(&mut params);
        params.snapshot_lag_threshold = 20;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_verification_snapshot_partition_smoke() {
        let mut params = ClusterParams::new(5, 500018, 0.0, FaultProfile::PartitionChurn);
        enable_full_verification(&mut params);
        params.snapshot_lag_threshold = 20;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count() / 2).await;

        harness.cleanup();
    }

    // =========================================================================
    // 4.6 Crash Recovery (Aggressive)
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn stress_crash_multiple_nodes() {
        let mut params = ClusterParams::new(5, 400001, 0.05, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        // Crash 2 nodes (still have quorum with 3)
        harness.crash_multiple(&[1, 2], CrashReason::Scheduled);
        harness.tick(50, 50).await;

        // Should still be able to make progress
        harness.run_oracle_workload(small_ops_count()).await;

        // Recover nodes
        harness.recover_node(1).await;
        harness.recover_node(2).await;
        harness.tick(50, 50).await;

        harness.run_oracle_workload(small_ops_count()).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_crash_majority_and_recover() {
        let mut params = ClusterParams::new(5, 400002, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        // Crash majority (3 nodes) - cluster should stall
        harness.crash_multiple(&[0, 1, 2], CrashReason::Scheduled);
        harness.tick(50, 50).await;

        // Cluster cannot make progress without quorum
        // Recover all nodes
        harness.recover_node(0).await;
        harness.recover_node(1).await;
        harness.recover_node(2).await;
        harness.tick(100, 50).await;

        // Now should be able to continue
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed after recovery");

        harness.run_oracle_workload(small_ops_count()).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_crash_recovery_cycles_10() {
        let mut params = ClusterParams::new(5, 400003, 0.05, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;

        for cycle in 0..10 {
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed in cycle {}", cycle);

            harness.run_oracle_workload(small_ops_count() / 2).await;

            // Crash a random node
            let crash_idx = (cycle * 7) % harness.nodes.len();
            harness.crash_and_recover_node(crash_idx, CrashReason::Scheduled).await;
        }

        harness.cleanup();
    }

    // =========================================================================
    // 4.7 Membership Changes
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn membership_add_learner() {
        let mut params = ClusterParams::new(3, 500001, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run initial workload
        harness.run_oracle_workload(small_ops_count()).await;

        // Add a new node
        let new_idx = harness.add_node().await.expect("add node failed");
        assert_eq!(new_idx, 3);
        assert_eq!(harness.nodes.len(), 4);

        harness.tick(50, 50).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn membership_promote_learner() {
        let mut params = ClusterParams::new(3, 500002, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        // Add and promote a new node
        let new_idx = harness.add_node().await.expect("add node failed");
        harness.promote_node(new_idx).await.expect("promote failed");

        // Should now have 4 voters
        harness.run_oracle_workload(small_ops_count()).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn membership_remove_follower() {
        let mut params = ClusterParams::new(5, 500003, 0.0, FaultProfile::None);
        params.require_all_nodes = false; // Only require quorum
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        // Remove a follower
        let leader_id = leader.unwrap();
        let follower_idx = harness.nodes.iter()
            .position(|n| n.id() != leader_id)
            .unwrap();
        harness.remove_node(follower_idx).await.expect("remove failed");

        // Cluster should continue operating
        harness.run_oracle_workload(small_ops_count()).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn membership_scale_3_to_5() {
        let mut params = ClusterParams::new(3, 500004, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        // Scale up to 5 nodes
        harness.scale_cluster(5).await.expect("scale up failed");
        assert_eq!(harness.nodes.len(), 5);

        harness.run_oracle_workload(small_ops_count()).await;
        harness.cleanup();
    }

    // =========================================================================
    // 4.8 Snapshot & Log Truncation
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_under_load() {
        let mut params = ClusterParams::new(3, 600001, 0.0, FaultProfile::None);
        params.snapshot_lag_threshold = 50; // Trigger snapshots more often
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run enough operations to trigger snapshots
        harness.run_oracle_workload(ops_count()).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn snapshot_during_partition() {
        let mut params = ClusterParams::new(5, 600002, 0.0, FaultProfile::PartitionChurn);
        params.snapshot_lag_threshold = 30;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(small_ops_count()).await;

        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn follower_catches_up_via_snapshot() {
        let mut params = ClusterParams::new(5, 600003, 0.0, FaultProfile::None);
        params.snapshot_lag_threshold = 20;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Crash a follower
        harness.crash_node(1, CrashReason::Scheduled).await;

        // Run lots of operations so log is truncated
        harness.run_oracle_workload(small_ops_count()).await;

        // Recover the follower - should catch up via snapshot
        harness.recover_node(1).await;
        harness.tick(200, 50).await;

        // Verify the recovered node has caught up
        harness.run_oracle_workload(small_ops_count() / 2).await;

        harness.cleanup();
    }

    // =========================================================================
    // 4.9 Stress Tests (Long-Running)
    // =========================================================================

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_seed_sweep() {
        let seeds = walrus_seed_sequence(0x9e3779b97f4a7c15, stress_seed_count());
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(5, seed, error_rate, FaultProfile::ReorderTimeoutBandwidth);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(stress_ops()).await;

            let (commits, failures, _) = harness.oracle_stats();
            eprintln!("Seed {}: {} commits, {} failures", seed, commits, failures);

            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_io_errors_high() {
        let seeds = walrus_seed_sequence(0xd1b54a32d192ed03, stress_seed_count());
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.15);
            let mut params = ClusterParams::new(5, seed, error_rate, FaultProfile::IoErrorsHeavy);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(stress_ops() / 2).await;
            harness.cleanup();
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_full_chaos() {
        // Everything at once: I/O errors, partial writes, network faults, crashes
        let seed = 0x517c_c1b7_2722_0a95;
        let error_rate = 0.10;
        let mut params = ClusterParams::new(5, seed, error_rate, FaultProfile::CombinedNetworkAndIo);
        params.enable_partial_writes = true;
        params.require_all_nodes = false;
        params.snapshot_lag_threshold = 50;

        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run with periodic crashes
        for cycle in 0..5 {
            harness.run_oracle_workload(stress_ops() / 10).await;

            // Crash and recover a node each cycle
            let crash_idx = (cycle * 3) % harness.nodes.len();
            harness.crash_and_recover_node(crash_idx, CrashReason::Scheduled).await;
        }

        let (commits, failures, _) = harness.oracle_stats();
        eprintln!("Full chaos: {} commits, {} failures", commits, failures);

        harness.cleanup();
    }

    // =========================================================================
    // 4.10 Walrus-Style Seed Sweeps (Matching simulation.rs patterns)
    // =========================================================================

    /// 8 random seeds with 1000+ ops each, no faults - matches Walrus pattern
    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_random_seeds_8_no_faults() {
        let seeds = walrus_seed_sequence(0x9e3779b97f4a7c15, walrus_seed_count());
        for seed in seeds {
            let mut params = ClusterParams::new(3, seed, 0.0, FaultProfile::None);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(ops_count()).await;

            let (commits, failures, verifies) = harness.oracle_stats();
            assert_eq!(failures, 0, "seed {}: unexpected failures", seed);
            assert_eq!(commits, verifies, "seed {}: verification mismatch", seed);

            harness.cleanup();
        }
    }

    /// 8 random seeds with I/O errors - matches Walrus fault injection pattern
    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_random_seeds_8_io_errors() {
        let seeds = walrus_seed_sequence(0xd1b54a32d192ed03, walrus_seed_count());
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::IoErrorsLight);
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(ops_count()).await;

            let (commits, failures, _) = harness.oracle_stats();
            eprintln!("Seed {}: {} commits, {} failures", seed, commits, failures);

            harness.cleanup();
        }
    }

    /// 8 random seeds with partial writes - matches Walrus partial write pattern
    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_random_seeds_8_partial_writes() {
        let seeds = walrus_seed_sequence(0x517cc1b727220a95, walrus_seed_count());
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.05);
            let mut params = ClusterParams::new(3, seed, error_rate, FaultProfile::PartialWrites);
            params.enable_partial_writes = true;
            let mut harness = ClusterHarness::new(params).await;
            let leader = harness.wait_for_leader().await;
            assert!(leader.is_some(), "leader election failed for seed {}", seed);

            harness.run_oracle_workload(ops_count()).await;

            let (commits, failures, _) = harness.oracle_stats();
            eprintln!("Seed {}: {} commits, {} failures", seed, commits, failures);

            harness.cleanup();
        }
    }

    /// Long-running no-faults stress test - matches Walrus stress_ tests
    #[tokio::test(flavor = "current_thread")]
    async fn stress_no_faults_long() {
        let mut params = ClusterParams::new(5, 0x1234567890abcdef, 0.0, FaultProfile::None);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        // Run many operations to find rare bugs
        harness.run_oracle_workload(stress_ops() * 2).await;

        let (commits, failures, verifies) = harness.oracle_stats();
        assert_eq!(failures, 0, "unexpected failures in stress test");
        assert_eq!(commits, verifies, "verification mismatch in stress test");

        harness.cleanup();
    }

    /// Long-running stress with I/O faults
    #[tokio::test(flavor = "current_thread")]
    async fn stress_io_faults_long() {
        let seed = 0xfedcba0987654321;
        let error_rate = walrus_error_rate(seed, 0.07);
        let mut params = ClusterParams::new(5, seed, error_rate, FaultProfile::IoErrorsMedium);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election failed");

        harness.run_oracle_workload(stress_ops()).await;

        let (commits, failures, _) = harness.oracle_stats();
        eprintln!("Stress I/O faults: {} commits, {} failures", commits, failures);

        harness.cleanup();
    }
}
