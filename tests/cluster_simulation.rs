mod common;

#[cfg(all(feature = "simulation", feature = "openraft"))]
mod cluster_sim_tests {
    use crate::common::cluster_sim::{
        ClusterHarness, ClusterParams, FaultProfile, ValidationMode,
    };

    #[tokio::test(flavor = "current_thread")]
    async fn cluster_three_nodes_faults_10pct() {
        let params = ClusterParams::new(3, 424242, 0.10, FaultProfile::ReorderTimeoutBandwidth);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election did not complete");
        harness.run_workload(5, ValidationMode::Cluster).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cluster_five_nodes_faults_10pct() {
        let mut params =
            ClusterParams::new(5, 271828, 0.10, FaultProfile::ReorderTimeoutBandwidth);
        params.require_all_nodes = false;
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election did not complete");
        harness.run_workload(5, ValidationMode::Cluster).await;
        harness.cleanup();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cluster_seven_nodes_partition_15pct() {
        let params = ClusterParams::new(7, 161803, 0.10, FaultProfile::ReorderTimeoutBandwidth);
        let mut harness = ClusterHarness::new(params).await;
        let leader = harness.wait_for_leader().await;
        assert!(leader.is_some(), "leader election did not complete");
        harness.run_workload(5, ValidationMode::Cluster).await;
        harness.cleanup();
    }
}
