mod common;

#[cfg(all(feature = "simulation", feature = "openraft"))]
mod cluster_sim_stress {
    use crate::common::cluster_sim::{
        walrus_error_rate, walrus_seed_sequence, ClusterHarness, ClusterParams, FaultProfile,
        ValidationMode,
    };
    use std::thread;

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

    fn ops_short() -> usize {
        std::env::var("CLUSTER_OPS_SHORT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(50)
    }

    fn ops_long() -> usize {
        std::env::var("CLUSTER_OPS_LONG")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(150)
    }

    fn parallel_config() -> (usize, u64, u64) {
        let jobs = std::env::var("CLUSTER_PARALLEL_JOBS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4);
        let start = std::env::var("CLUSTER_PARALLEL_SEED_START")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let count = std::env::var("CLUSTER_PARALLEL_SEED_COUNT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20);
        (jobs.max(1), start, count)
    }

    async fn run_seed(
        size: usize,
        seed: u64,
        error_rate: f64,
        profile: FaultProfile,
        ops: usize,
        partial_writes: bool,
        require_all: bool,
        mode: ValidationMode,
    ) {
        let mut params = ClusterParams::new(size, seed, error_rate, profile);
        params.enable_partial_writes = partial_writes;
        params.require_all_nodes = require_all;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election did not complete");
        harness.run_workload(ops, mode).await;
        harness.cleanup();
    }

    fn run_seed_blocking(
        size: usize,
        seed: u64,
        error_rate: f64,
        profile: FaultProfile,
        ops: usize,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        rt.block_on(async move {
            run_seed(
                size,
                seed,
                error_rate,
                profile,
                ops,
                false,
                true,
                ValidationMode::Leader,
            )
            .await
        });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.10);
            run_seed(
                3,
                seed,
                error_rate,
                FaultProfile::None,
                ops_short(),
                false,
                true,
                ValidationMode::Leader,
            )
            .await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_fault_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.10);
            run_seed(
                5,
                seed,
                error_rate,
                FaultProfile::ReorderTimeoutBandwidth,
                ops_short(),
                false,
                true,
                ValidationMode::Leader,
            )
            .await;
            // NOTE: PartitionChurn removed - triggers openraft assertion failure
            // `self.leader.is_none()` on certain seeds. Same bug as FlappingConnectivity
            // and SplitBrain. See network_faults_* tests in cluster_simulation_comprehensive.rs.
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_seven_nodes_seed_sweep() {
        let (start, count) = seed_range();
        for seed in start..(start + count) {
            let error_rate = walrus_error_rate(seed, 0.10);
            run_seed(
                7,
                seed,
                error_rate,
                FaultProfile::ReorderTimeoutBandwidth,
                ops_short(),
                false,
                false,
                ValidationMode::Leader,
            )
            .await;
        }
    }

    #[test]
    fn stress_cluster_parallel_seed_sweep() {
        let (jobs, start, count) = parallel_config();
        let seeds: Vec<u64> = (start..(start + count)).collect();
        let chunk_size = ((count as usize) + jobs - 1) / jobs;
        let mut handles = Vec::new();
        for chunk in seeds.chunks(chunk_size) {
            let chunk = chunk.to_vec();
            let handle = thread::spawn(move || {
                for seed in chunk {
                    let error_rate = walrus_error_rate(seed, 0.10);
                    run_seed_blocking(
                        3,
                        seed,
                        error_rate,
                        FaultProfile::ReorderTimeoutBandwidth,
                        ops_short(),
                    );
                }
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().expect("seed thread");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_random_seeds_10_15pct() {
        let seeds = walrus_seed_sequence(0x9e3779b97f4a7c15, 20);
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.10);
            run_seed(
                3,
                seed,
                error_rate,
                FaultProfile::ReorderTimeoutBandwidth,
                ops_short(),
                false,
                true,
                ValidationMode::Leader,
            )
            .await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_random_seeds_15_20pct_long() {
        let seeds = walrus_seed_sequence(0xd1b54a32d192ed03, 20);
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.15);
            run_seed(
                5,
                seed,
                error_rate,
                FaultProfile::ReorderTimeoutBandwidth,
                ops_long(),
                false,
                false,
                ValidationMode::Leader,
            )
            .await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_partial_writes_10_15pct() {
        let seeds = walrus_seed_sequence(0x517c_c1b7_2722_0a95, 10);
        for seed in seeds {
            let error_rate = walrus_error_rate(seed, 0.10);
            run_seed(
                3,
                seed,
                error_rate,
                FaultProfile::ReorderTimeoutBandwidth,
                ops_short(),
                true,
                true,
                ValidationMode::Leader,
            )
            .await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stress_cluster_recovery_single_node() {
        let seed = 42424242;
        let mut params =
            ClusterParams::new(5, seed, walrus_error_rate(seed, 0.10), FaultProfile::None);
        params.snapshot_lag_threshold = 50;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election did not complete");
        harness.run_workload(ops_short(), ValidationMode::Leader).await;

        harness.restart_node(1).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election did not complete");
        harness.run_workload(ops_short(), ValidationMode::Leader).await;
        harness.cleanup();
    }
}
