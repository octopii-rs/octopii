#![cfg(all(feature = "simulation", feature = "openraft"))]

use crate::common::cluster_invariants::InvariantChecker;
use crate::common::cluster_oracle::ClusterOracle;
use octopii::config::Config;
use octopii::openraft::node::OpenRaftNode;
use octopii::openraft::sim_runtime;
use octopii::runtime::OctopiiRuntime;
use octopii::transport::{SimConfig, SimRouter, SimTransport, Transport};
use octopii::wal::wal::vfs::sim::{self, SimConfig as VfsSimConfig};
use octopii::wal::wal;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::yield_now;
use tokio::time::Duration;

#[derive(Clone, Copy, Debug)]
pub enum FaultProfile {
    None,
    ReorderTimeoutBandwidth,
    PartitionChurn,
    // Extended fault profiles
    IoErrorsLight,      // 5% I/O error rate
    IoErrorsMedium,     // 10% I/O error rate
    IoErrorsHeavy,      // 15-20% I/O error rate
    PartialWrites,      // Enable torn writes
    CombinedNetworkAndIo, // Network faults + I/O errors
    SplitBrain,         // Partition isolating leader
    FlappingConnectivity, // Rapid partition/heal cycles
}

#[derive(Clone, Debug)]
pub struct CrashEvent {
    pub node_idx: usize,
    pub node_id: u64,
    pub tick: u64,
    pub reason: CrashReason,
}

#[derive(Clone, Copy, Debug)]
pub enum CrashReason {
    Scheduled,
    IoError,
    Partition,
    MembershipChange,
}

pub struct ClusterParams {
    pub size: usize,
    pub seed: u64,
    pub io_error_rate: f64,
    pub profile: FaultProfile,
    pub enable_partial_writes: bool,
    pub snapshot_lag_threshold: u64,
    pub require_all_nodes: bool,
    pub verify_invariants: bool,
    pub invariant_interval: u64,
    pub verify_full: bool,
    pub verify_each_op: bool,
    pub verify_durability: bool,
    pub verify_logs: bool,
}

impl ClusterParams {
    pub fn new(size: usize, seed: u64, io_error_rate: f64, profile: FaultProfile) -> Self {
        let verify_invariants = std::env::var("CLUSTER_VERIFY_INVARIANTS")
            .ok()
            .as_deref()
            == Some("1");
        let invariant_interval = std::env::var("CLUSTER_INVARIANT_INTERVAL")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(50);
        let verify_full = std::env::var("CLUSTER_VERIFY_FULL")
            .ok()
            .as_deref()
            == Some("1");
        let verify_each_op = std::env::var("CLUSTER_VERIFY_EACH_OP")
            .ok()
            .as_deref()
            == Some("1");
        let verify_durability = std::env::var("CLUSTER_VERIFY_DURABILITY")
            .ok()
            .as_deref()
            == Some("1");
        let verify_logs = std::env::var("CLUSTER_VERIFY_LOGS")
            .ok()
            .as_deref()
            == Some("1");
        Self {
            size,
            seed,
            io_error_rate,
            profile,
            enable_partial_writes: false,
            snapshot_lag_threshold: 500,
            require_all_nodes: true,
            verify_invariants,
            invariant_interval,
            verify_full,
            verify_each_op,
            verify_durability,
            verify_logs,
        }
    }
}

pub struct ClusterHarness {
    pub router: SimRouter,
    pub nodes: Vec<Arc<OpenRaftNode>>,
    pub addrs: Vec<SocketAddr>,
    pub base: PathBuf,
    fault_plan: FaultPlan,
    pub seed: u64,
    pub io_error_rate: f64,
    require_all_nodes: bool,
    transports: Vec<Arc<SimTransport>>,
    configs: Vec<Config>,
    rt: OctopiiRuntime,
    // New fields for comprehensive testing
    pub oracle: ClusterOracle,
    pub crash_history: Vec<CrashEvent>,
    pub tick_count: u64,
    enable_partial_writes: bool,
    next_node_id: u64,
    invariants: InvariantChecker,
    invariants_enabled: bool,
    invariant_interval: u64,
    full_verify_enabled: bool,
    durability_enabled: bool,
    log_verify_enabled: bool,
    per_op_verify_enabled: bool,
}

static NEXT_CLUSTER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
pub enum ValidationMode {
    Cluster,
    Leader,
    Full,
}

impl ClusterHarness {
    fn debug_enabled() -> bool {
        std::env::var("CLUSTER_DEBUG").ok().as_deref() == Some("1")
    }

    fn debug_dump_metrics(&self, label: &str) {
        if !Self::debug_enabled() {
            return;
        }
        eprintln!("\n[cluster_debug] {label}");
        for (idx, node) in self.nodes.iter().enumerate() {
            let metrics = node.raft_metrics();
            eprintln!(
                "  node[{}] id={} state={:?} leader={:?} term={:?} last_log={:?} last_applied={:?} membership={:?}",
                idx,
                node.id(),
                metrics.state,
                metrics.current_leader,
                metrics.current_term,
                metrics.last_log_index,
                metrics.last_applied,
                metrics.membership_config.membership().get_joint_config(),
            );
            if metrics.state == openraft::ServerState::Leader {
                if let Some(replication) = metrics.replication.as_ref() {
                    let mut peers = replication.keys().copied().collect::<Vec<_>>();
                    peers.sort_unstable();
                    eprintln!("    replication peers={:?}", peers);
                }
            }
        }
    }
    pub async fn new(params: ClusterParams) -> Self {
        let run_id = NEXT_CLUSTER_ID.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "octopii_sim_cluster_{}_n{}_{}",
            params.seed, params.size, run_id
        ));
        let _ = std::fs::remove_dir_all(&base);

        std::env::set_var("WALRUS_QUIET", "1");
        sim::setup(VfsSimConfig {
            seed: params.seed,
            io_error_rate: params.io_error_rate,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: params.enable_partial_writes,
        });
        sim_runtime::reset(params.seed, 1700000000_000_000_000);

        let router = SimRouter::new(SimConfig {
            seed: params.seed ^ 0xdead_beef,
            drop_rate: 0.0,
            min_delay_ms: 0,
            max_delay_ms: 0,
        });

        // Port must end in node_id for the port % 10 hack in openraft/node.rs
        // Node IDs are 1, 2, 3, ..., so ports must end in 1, 2, 3, ...
        // Use base_port that ends in 0 (e.g., 9320, 9330, ...) so port+1 ends in 1
        let base_port = 9320 + ((params.seed % 100) * 10) as u16;
        let mut addrs = Vec::with_capacity(params.size);
        for i in 0..params.size {
            let port = base_port + (i + 1) as u16; // +1 so node 1 gets port ending in 1
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            addrs.push(addr);
        }

        let transports = addrs
            .iter()
            .map(|addr| Arc::new(SimTransport::new(*addr, router.clone())))
            .collect::<Vec<_>>();

        let rt = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

        let configs = addrs
            .iter()
            .enumerate()
            .map(|(idx, addr)| Config {
                node_id: (idx + 1) as u64,
                bind_addr: *addr,
                peers: addrs
                    .iter()
                    .filter(|a| *a != addr)
                    .copied()
                    .collect(),
                wal_dir: base.join(format!("n{}", idx + 1)),
                worker_threads: 1,
                wal_batch_size: 10,
                wal_flush_interval_ms: 50,
                is_initial_leader: idx == 0,
                snapshot_lag_threshold: params.snapshot_lag_threshold,
            })
            .collect::<Vec<_>>();

        let mut nodes = Vec::with_capacity(params.size);
        for (idx, cfg) in configs.iter().cloned().enumerate() {
            let transport = Arc::clone(&transports[idx]) as Arc<dyn Transport>;
            let node = new_node_with_retry(cfg, rt.clone(), transport).await;
            nodes.push(node);
        }

        let fault_plan = FaultPlan::new(params.seed, &addrs, params.profile, router.now_ms());

        Self {
            router,
            nodes,
            addrs,
            base,
            fault_plan,
            seed: params.seed,
            io_error_rate: params.io_error_rate,
            require_all_nodes: params.require_all_nodes,
            transports,
            configs,
            rt,
            oracle: ClusterOracle::new(),
            crash_history: Vec::new(),
            tick_count: 0,
            enable_partial_writes: params.enable_partial_writes,
            next_node_id: (params.size + 1) as u64,
            invariants: InvariantChecker::new(),
            invariants_enabled: params.verify_invariants,
            invariant_interval: params.invariant_interval,
            full_verify_enabled: params.verify_full,
            durability_enabled: params.verify_durability,
            log_verify_enabled: params.verify_logs,
            per_op_verify_enabled: params.verify_each_op,
        }
    }

    pub async fn tick(&mut self, steps: usize, step_ms: u64) {
        for _ in 0..steps {
            self.router.advance_time(step_ms);
            self.fault_plan.apply(&self.router);
            self.router.deliver_ready();
            sim_runtime::advance_time(Duration::from_millis(step_ms));
            self.tick_count += 1;
            self.oracle.set_tick(self.tick_count);
            if self.invariants_enabled {
                self.invariants.set_tick(self.tick_count);
                if self.tick_count % self.invariant_interval == 0 {
                    self.check_invariants_light();
                }
            }
            yield_now().await;
        }
    }

    /// Get the current tick count
    pub fn current_tick(&self) -> u64 {
        self.tick_count
    }

    pub async fn wait_for_leader(&mut self) -> Option<u64> {
        for i in 0..2000 {
            self.tick(5, 50).await;
            for node in &self.nodes {
                if node.is_leader().await {
                    return Some(node.id());
                }
            }
            if i % 200 == 0 {
                self.debug_dump_metrics("wait_for_leader progress");
            }
        }
        self.debug_dump_metrics("wait_for_leader exhausted");
        None
    }

    pub async fn propose_with_leader(&mut self, command: Vec<u8>) -> octopii::Result<()> {
        for attempt in 0..20 {
            let leader_id = self.wait_for_leader().await;
            let Some(leader_id) = leader_id else {
                self.tick(5, 50).await;
                continue;
            };
            let leader = self
                .nodes
                .iter()
                .find(|n| n.id() == leader_id)
                .cloned()
                .expect("leader node");
            let propose_task = {
                let leader = Arc::clone(&leader);
                let data = command.clone();
                tokio::spawn(async move { leader.propose(data).await })
            };
            match drive_join(&self.router, &mut self.fault_plan, propose_task, 50).await {
                Ok(_) => return Ok(()),
                Err(_) => {
                    self.tick(5, 50).await;
                }
            }
            if attempt % 5 == 0 {
                self.debug_dump_metrics("propose_with_leader retry");
            }
        }
        self.debug_dump_metrics("propose_with_leader exhausted");
        Err(octopii::error::OctopiiError::Rpc(
            "failed to propose after retries".to_string(),
        ))
    }

    pub async fn run_workload(&mut self, ops: usize, mode: ValidationMode) {
        for i in 0..ops {
            let cmd = format!("SET key{i} val{i}");
            self.propose_with_leader(cmd.into_bytes())
                .await
                .expect("propose");
        }

        for i in 0..ops {
            let key = format!("GET key{i}");
            let expected = format!("val{i}");
            match mode {
                ValidationMode::Cluster => {
                    self.wait_for_value(key.as_bytes(), expected.as_bytes())
                        .await;
                }
                ValidationMode::Leader => {
                    self.wait_for_leader_value(key.as_bytes(), expected.as_bytes())
                        .await;
                }
                ValidationMode::Full => {
                    self.wait_for_value(key.as_bytes(), expected.as_bytes())
                        .await;
                }
            }
        }

        if matches!(mode, ValidationMode::Full) || self.full_verify_enabled {
            self.verify_oracle_full().await;
        }
    }

    async fn wait_for_value(&mut self, key: &[u8], expected: &[u8]) {
        let required = if self.require_all_nodes {
            self.nodes.len()
        } else {
            (self.nodes.len() / 2) + 1
        };
        for _ in 0..2000 {
            let mut match_count = 0usize;
            for node in &self.nodes {
                let val = node.query(key).await.expect("read");
                if val.as_ref() != expected {
                    continue;
                }
                match_count += 1;
            }
            if match_count >= required {
                return;
            }
            self.tick(10, 50).await;
        }
        panic!("cluster did not converge for key");
    }

    async fn wait_for_leader_value(&mut self, key: &[u8], expected: &[u8]) {
        for _ in 0..2000 {
            if let Some(leader_id) = self.wait_for_leader().await {
                if let Some(leader) = self.nodes.iter().find(|n| n.id() == leader_id) {
                    let val = leader.query(key).await.expect("read");
                    if val.as_ref() == expected {
                        return;
                    }
                }
            }
            self.tick(10, 50).await;
        }
        panic!("leader did not apply key");
    }

    pub async fn restart_node(&mut self, idx: usize) {
        if let Some(node) = self.nodes.get(idx) {
            node.shutdown().await;
        }
        let mut cfg = self.configs[idx].clone();
        cfg.is_initial_leader = false;
        let transport = Arc::clone(&self.transports[idx]) as Arc<dyn Transport>;
        let node = new_node_with_retry(cfg, self.rt.clone(), transport).await;
        self.nodes[idx] = node;
    }

    pub fn cleanup(&self) {
        if Self::debug_enabled() {
            eprintln!(
                "\n[cluster_debug] skipping cleanup for inspection: {}",
                self.base.display()
            );
            return;
        }
        let _ = std::fs::remove_dir_all(&self.base);
        wal::__clear_storage_cache_for_tests();
        sim::teardown();
    }

    // =========================================================================
    // Crash and Recovery Methods
    // =========================================================================

    /// Crash a node (shutdown without restart)
    pub async fn crash_node(&mut self, idx: usize, reason: CrashReason) {
        if let Some(node) = self.nodes.get(idx) {
            let node_id = node.id();
            node.shutdown().await;
            self.crash_history.push(CrashEvent {
                node_idx: idx,
                node_id,
                tick: self.tick_count,
                reason,
            });
        }
    }

    /// Crash multiple nodes simultaneously
    pub async fn crash_multiple(&mut self, indices: &[usize], reason: CrashReason) {
        for &idx in indices {
            self.crash_node(idx, reason).await;
        }
    }

    /// Crash majority of nodes (n/2 + 1)
    pub async fn crash_majority(&mut self, reason: CrashReason) {
        let majority = (self.nodes.len() / 2) + 1;
        let indices: Vec<usize> = (0..majority).collect();
        self.crash_multiple(&indices, reason).await;
    }

    /// Restart a crashed node
    pub async fn recover_node(&mut self, idx: usize) {
        if idx >= self.configs.len() {
            return;
        }
        let mut cfg = self.configs[idx].clone();
        cfg.is_initial_leader = false;
        let addr = self.addrs[idx];
        let transport = Arc::new(SimTransport::new(addr, self.router.clone()));
        self.transports[idx] = Arc::clone(&transport);
        let transport = transport as Arc<dyn Transport>;
        let node = new_node_with_retry(cfg, self.rt.clone(), transport).await;
        node.set_election_enabled(false);
        self.nodes[idx] = node;
    }

    /// Crash and recover a node in sequence
    pub async fn crash_and_recover_node(&mut self, idx: usize, reason: CrashReason) {
        if Self::debug_enabled() {
            eprintln!(
                "\n[cluster_debug] crash_and_recover_node idx={} reason={:?} tick={}",
                idx, reason, self.tick_count
            );
        }
        self.crash_node(idx, reason).await;
        if Self::debug_enabled() {
            eprintln!("[cluster_debug] crash_node complete idx={}", idx);
        }
        // Wait for election timeout (800-1600ms) and new leader election
        // At 50ms per tick, need ~32 ticks for min election timeout
        self.tick(100, 50).await; // 5 seconds - should be enough for election
        self.recover_node(idx).await;
        self.tick(100, 50).await; // Let the node catch up and rejoin cluster
        if let Some(node) = self.nodes.get(idx) {
            node.set_election_enabled(true);
        }
        if Self::debug_enabled() {
            self.debug_dump_metrics("after crash_and_recover_node");
        }
    }

    // =========================================================================
    // Partition Methods
    // =========================================================================

    /// Isolate the current leader from all followers
    pub async fn isolate_leader(&mut self) -> Option<u64> {
        let leader_id = self.wait_for_leader().await?;
        let leader_idx = self.nodes.iter().position(|n| n.id() == leader_id)?;
        let leader_addr = self.addrs[leader_idx];
        let other_addrs: Vec<SocketAddr> = self.addrs.iter()
            .filter(|&&a| a != leader_addr)
            .copied()
            .collect();
        self.router.add_partition(vec![leader_addr], other_addrs);
        Some(leader_id)
    }

    /// Heal all partitions
    pub fn heal_partitions(&self) {
        self.router.clear_faults();
    }

    /// Create a partition between two groups of nodes
    pub fn partition_nodes(&self, group_a: &[usize], group_b: &[usize]) {
        let addrs_a: Vec<SocketAddr> = group_a.iter()
            .filter_map(|&i| self.addrs.get(i).copied())
            .collect();
        let addrs_b: Vec<SocketAddr> = group_b.iter()
            .filter_map(|&i| self.addrs.get(i).copied())
            .collect();
        self.router.add_partition(addrs_a, addrs_b);
    }

    // =========================================================================
    // Log and State Inspection Methods
    // =========================================================================

    /// Get the committed index for a node
    pub fn get_committed_index(&self, idx: usize) -> Option<u64> {
        self.nodes.get(idx).and_then(|n| {
            let metrics = n.raft_metrics();
            metrics.last_applied.as_ref().map(|log_id| log_id.index)
        })
    }

    /// Get the last log index for a node
    pub fn get_last_log_index(&self, idx: usize) -> Option<u64> {
        self.nodes.get(idx).and_then(|n| {
            let metrics = n.raft_metrics();
            metrics.last_log_index
        })
    }

    /// Get the current term for a node
    pub fn get_current_term(&self, idx: usize) -> Option<u64> {
        self.nodes.get(idx).map(|n| {
            let metrics = n.raft_metrics();
            metrics.current_term
        })
    }

    /// Check if all nodes have converged to the same committed index
    pub fn check_commit_convergence(&self) -> bool {
        let indices: Vec<Option<u64>> = (0..self.nodes.len())
            .map(|i| self.get_committed_index(i))
            .collect();

        let first = indices.first().and_then(|o| *o);
        first.is_some() && indices.iter().all(|o| *o == first)
    }

    /// Verify no log divergence (all nodes agree on log entries up to min committed)
    pub async fn verify_no_divergence(&mut self) -> bool {
        // Get the minimum committed index across all nodes
        let min_committed = (0..self.nodes.len())
            .filter_map(|i| self.get_committed_index(i))
            .min();

        let Some(min_idx) = min_committed else {
            return true; // No commits yet
        };

        // For simplicity, we verify by reading values from the state machine
        // In a more thorough check, we'd compare log entries directly
        for i in 0..min_idx.min(100) {
            let key = format!("GET key{i}");
            let mut values = Vec::new();
            for node in &self.nodes {
                if let Ok(val) = node.query(key.as_bytes()).await {
                    values.push(val);
                }
            }
            // All values should be the same
            if !values.is_empty() && !values.windows(2).all(|w| w[0] == w[1]) {
                return false;
            }
        }
        true
    }

    // =========================================================================
    // Membership Change Methods
    // =========================================================================

    /// Add a new learner node to the cluster
    pub async fn add_node(&mut self) -> octopii::Result<usize> {
        let new_idx = self.nodes.len();
        let new_node_id = self.next_node_id;
        self.next_node_id += 1;

        let base_port = 9700 + (self.seed % 1000) as u16;
        let new_port = base_port + new_idx as u16;
        let new_addr: SocketAddr = format!("127.0.0.1:{new_port}").parse().unwrap();

        // Create transport for new node
        let transport = Arc::new(SimTransport::new(new_addr, self.router.clone()));

        // Create config for new node
        let config = Config {
            node_id: new_node_id,
            bind_addr: new_addr,
            peers: self.addrs.clone(),
            wal_dir: self.base.join(format!("n{}", new_node_id)),
            worker_threads: 1,
            wal_batch_size: 10,
            wal_flush_interval_ms: 50,
            is_initial_leader: false,
            snapshot_lag_threshold: 500,
        };

        // Start the new node
        let node = new_node_with_retry(config.clone(), self.rt.clone(), Arc::clone(&transport) as Arc<dyn Transport>).await;

        // Add to cluster tracking
        self.addrs.push(new_addr);
        self.transports.push(transport);
        self.configs.push(config);
        self.nodes.push(node);

        // Tell the leader to add this node as a learner
        let leader_id = self.wait_for_leader().await
            .ok_or_else(|| octopii::error::OctopiiError::Rpc("no leader".to_string()))?;
        let leader = self.nodes.iter()
            .find(|n| n.id() == leader_id)
            .ok_or_else(|| octopii::error::OctopiiError::Rpc("leader not found".to_string()))?;

        let add_task = {
            let leader = Arc::clone(leader);
            tokio::spawn(async move { leader.add_learner(new_node_id, new_addr).await })
        };
        drive_join(&self.router, &mut self.fault_plan, add_task, 50).await?;

        Ok(new_idx)
    }

    /// Promote a learner to voter
    pub async fn promote_node(&mut self, idx: usize) -> octopii::Result<()> {
        let node_id = self.nodes.get(idx)
            .map(|n| n.id())
            .ok_or_else(|| octopii::error::OctopiiError::Rpc("node not found".to_string()))?;

        // Wait for the learner to catch up
        for _ in 0..100 {
            let leader_id = self.wait_for_leader().await
                .ok_or_else(|| octopii::error::OctopiiError::Rpc("no leader".to_string()))?;
            let leader = self.nodes.iter()
                .find(|n| n.id() == leader_id)
                .ok_or_else(|| octopii::error::OctopiiError::Rpc("leader not found".to_string()))?;

            if leader.is_learner_caught_up(node_id).await.unwrap_or(false) {
                let promote_task = {
                    let leader = Arc::clone(leader);
                    tokio::spawn(async move { leader.promote_learner(node_id).await })
                };
                drive_join(&self.router, &mut self.fault_plan, promote_task, 50).await?;
                return Ok(());
            }
            self.tick(10, 50).await;
        }

        Err(octopii::error::OctopiiError::Rpc("learner did not catch up".to_string()))
    }

    /// Remove a node from the cluster (by crashing it and removing from membership)
    pub async fn remove_node(&mut self, idx: usize) -> octopii::Result<()> {
        if idx >= self.nodes.len() {
            return Err(octopii::error::OctopiiError::Rpc("invalid node index".to_string()));
        }

        // Crash the node
        self.crash_node(idx, CrashReason::MembershipChange).await;
        self.tick(20, 50).await;

        // Note: Full membership change (removing from Raft config) would require
        // change_membership on the leader. For now, we just crash the node.
        // The cluster should continue operating with the remaining nodes.

        Ok(())
    }

    /// Scale cluster up or down
    pub async fn scale_cluster(&mut self, target_size: usize) -> octopii::Result<()> {
        let current_size = self.nodes.len();

        if target_size > current_size {
            // Scale up
            for _ in current_size..target_size {
                let idx = self.add_node().await?;
                self.tick(20, 50).await;
                self.promote_node(idx).await?;
                self.tick(10, 50).await;
            }
        } else if target_size < current_size {
            // Scale down - remove from the end
            for idx in (target_size..current_size).rev() {
                self.remove_node(idx).await?;
            }
        }

        Ok(())
    }

    // =========================================================================
    // Oracle-Integrated Workload Methods
    // =========================================================================

    /// Propose a SET command and track in oracle
    pub async fn propose_set(&mut self, key: &str, value: &str) -> octopii::Result<()> {
        let cmd = format!("SET {key} {value}");
        let partial_before = sim::get_partial_write_count();
        match self.propose_with_leader(cmd.into_bytes()).await {
            Ok(()) => {
                let partial_after = sim::get_partial_write_count();
                if self.durability_enabled {
                    if partial_before == partial_after {
                        self.oracle.record_must_survive(key, value);
                    } else {
                        self.oracle.record_may_be_lost(key, value);
                    }
                } else {
                    self.oracle.record_commit(key, value);
                }
                Ok(())
            }
            Err(e) => {
                self.oracle.record_failure(key, value, &e.to_string());
                Err(e)
            }
        }
    }

    /// Query a value and verify against oracle
    pub async fn query_and_verify(&mut self, key: &str) -> octopii::Result<()> {
        let leader_id = self.wait_for_leader().await
            .ok_or_else(|| octopii::error::OctopiiError::Rpc("no leader".to_string()))?;
        let leader = self.nodes.iter()
            .find(|n| n.id() == leader_id)
            .ok_or_else(|| octopii::error::OctopiiError::Rpc("leader not found".to_string()))?;

        let query = format!("GET {key}");
        let result = leader.query(query.as_bytes()).await?;
        let actual = std::str::from_utf8(&result).ok();

        self.oracle.assert_read(key, actual);
        Ok(())
    }

    /// Run a workload with oracle tracking
    pub async fn run_oracle_workload(&mut self, ops: usize) {
        if Self::debug_enabled() {
            eprintln!(
                "\n[cluster_debug] run_oracle_workload start ops={} tick={}",
                ops, self.tick_count
            );
            self.debug_dump_metrics("run_oracle_workload start");
        }
        for i in 0..ops {
            let key = format!("key{i}");
            let value = format!("val{i}");
            self.propose_set(&key, &value).await.expect("propose");
            if self.per_op_verify_enabled {
                let query = format!("GET {key}");
                self.wait_for_leader_value(query.as_bytes(), value.as_bytes())
                    .await;
                self.query_and_verify(&key).await.expect("query");
            }
        }

        // Verify all values
        self.tick(50, 50).await; // Let cluster converge
        for i in 0..ops {
            let key = format!("key{i}");
            self.query_and_verify(&key).await.expect("query");
        }
        if self.full_verify_enabled {
            self.verify_oracle_full().await;
        }
        if Self::debug_enabled() {
            eprintln!(
                "\n[cluster_debug] run_oracle_workload end ops={} tick={}",
                ops, self.tick_count
            );
        }
    }

    /// Verify must-survive keys after crash recovery
    pub async fn verify_must_survive(&mut self) {
        if !self.durability_enabled {
            return;
        }
        let required = if self.require_all_nodes {
            self.nodes.len()
        } else {
            (self.nodes.len() / 2) + 1
        };
        for (key, value) in self.oracle.must_survive_snapshot() {
            let mut match_count = 0usize;
            for node in &self.nodes {
                let cmd = format!("GET {key}");
                if let Ok(bytes) = node.query(cmd.as_bytes()).await {
                    if let Ok(actual) = String::from_utf8(bytes.to_vec()) {
                        if actual == value {
                            match_count += 1;
                        }
                    }
                }
            }
            if match_count < required {
                panic!(
                    "must-survive verification failed for key '{}': only {} nodes matched (required {})",
                    key, match_count, required
                );
            }
        }

        if let Some(leader_id) = self.wait_for_leader().await {
            if let Some(leader) = self.nodes.iter().find(|n| n.id() == leader_id) {
                for (key, value) in self.oracle.may_be_lost_snapshot() {
                    let cmd = format!("GET {key}");
                    if let Ok(bytes) = leader.query(cmd.as_bytes()).await {
                        if let Ok(actual) = String::from_utf8(bytes.to_vec()) {
                            if actual == value {
                                self.oracle.promote_may_be_lost(&key, &value);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Get oracle statistics
    pub fn oracle_stats(&self) -> (u64, usize, u64) {
        self.oracle.stats()
    }

    /// Get crash history
    pub fn crash_history(&self) -> &[CrashEvent] {
        &self.crash_history
    }

    fn check_invariants_light(&mut self) {
        for node in &self.nodes {
            let metrics = node.raft_metrics();
            if metrics.state == openraft::ServerState::Leader {
                self.invariants
                    .record_leader(node.id(), metrics.current_term);
            }
            if let Err(e) = self
                .invariants
                .check_term_monotonicity(node.id(), metrics.current_term)
            {
                self.invariants.reset();
                panic!("{e}");
            }
            if let Some(log_id) = metrics.last_applied {
                if let Err(e) = self
                    .invariants
                    .check_commit_monotonicity(node.id(), log_id.index)
                {
                    self.invariants.reset();
                    panic!("{e}");
                }
            }
        }
        if let Err(e) = self.invariants.check_single_leader() {
            self.invariants.reset();
            panic!("{e}");
        }
    }

    async fn verify_oracle_full(&mut self) {
        let expected = self.oracle.expected_state_snapshot();
        let required = if self.require_all_nodes {
            self.nodes.len()
        } else {
            (self.nodes.len() / 2) + 1
        };

        for (key, value) in &expected {
            let mut match_count = 0usize;
            for node in &self.nodes {
                let cmd = format!("GET {key}");
                let result = node.query(cmd.as_bytes()).await;
                let actual = result
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok());
                if actual.as_deref() == Some(value.as_str()) {
                    match_count += 1;
                } else if actual.is_some() {
                    self.oracle
                        .assert_read_with_node(node.id(), key, actual.as_deref());
                }
            }
            if match_count < required {
                panic!(
                    "full verification failed for key '{}': only {} nodes matched (required {})",
                    key, match_count, required
                );
            }
        }

        self.check_membership_consistency(required);
        self.check_state_machine_consistency(&expected).await;
        self.check_log_consistency().await;
    }

    fn check_membership_consistency(&self, required: usize) {
        let mut counts: HashMap<Vec<Vec<u64>>, usize> = HashMap::new();
        for node in &self.nodes {
            let metrics = node.raft_metrics();
            let mut joint = Vec::new();
            for cfg in metrics.membership_config.membership().get_joint_config() {
                let mut members = cfg.iter().copied().collect::<Vec<_>>();
                members.sort_unstable();
                joint.push(members);
            }
            joint.sort();
            *counts.entry(joint).or_insert(0) += 1;
        }

        let max = counts.values().copied().max().unwrap_or(0);
        if max < required {
            panic!(
                "membership consistency failed: only {} nodes share the same membership (required {})",
                max, required
            );
        }
    }

    async fn check_state_machine_consistency(&mut self, expected: &[(String, String)]) {
        if !self.require_all_nodes {
            return;
        }
        let mut node_states = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let mut state = HashMap::new();
            for (key, _) in expected {
                let cmd = format!("GET {key}");
                if let Ok(bytes) = node.query(cmd.as_bytes()).await {
                    if let Ok(val) = String::from_utf8(bytes.to_vec()) {
                        state.insert(key.clone(), val);
                    }
                }
            }
            node_states.push((node.id(), state));
        }

        if let Err(e) = self
            .invariants
            .check_state_machine_consistency(&node_states)
        {
            self.invariants.reset();
            panic!("{e}");
        }
    }

    async fn check_log_consistency(&mut self) {
        if !self.log_verify_enabled {
            return;
        }

        let active_nodes: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| n.raft_metrics().state != openraft::ServerState::Shutdown)
            .collect();
        if active_nodes.len() < 2 {
            return;
        }

        let mut min_applied: Option<u64> = None;
        for node in &active_nodes {
            let metrics = node.raft_metrics();
            let Some(log_id) = metrics.last_applied else {
                return;
            };
            min_applied = Some(match min_applied {
                Some(cur) => cur.min(log_id.index),
                None => log_id.index,
            });
        }

        let Some(end_idx) = min_applied else {
            return;
        };

        let mut start_idx = 1u64;
        for node in &active_nodes {
            let state = node.log_state().await.expect("log_state");
            if let Some(purged) = state.last_purged_log_id {
                start_idx = start_idx.max(purged.index + 1);
            }
        }

        if start_idx > end_idx {
            return;
        }

        let expected_count = (end_idx - start_idx + 1) as usize;
        let mut baseline: Option<Vec<(u64, u64, u64)>> = None;

        for node in &active_nodes {
            let entries = node
                .log_entries(start_idx..=end_idx)
                .await
                .expect("log_entries");

            if entries.len() != expected_count {
                panic!(
                    "log contiguity failed for node {}: expected {} entries ({}..={}), got {}",
                    node.id(),
                    expected_count,
                    start_idx,
                    end_idx,
                    entries.len()
                );
            }

            let mut fingerprint = Vec::with_capacity(entries.len());
            for (i, entry) in entries.iter().enumerate() {
                let expected_idx = start_idx + i as u64;
                if entry.log_id.index != expected_idx {
                    panic!(
                        "log index gap for node {}: expected {}, got {}",
                        node.id(),
                        expected_idx,
                        entry.log_id.index
                    );
                }
                let term = entry.log_id.leader_id.term as u64;
                let payload_hash = hash_payload(&entry.payload);
                fingerprint.push((entry.log_id.index, term, payload_hash));
            }

            if let Some(ref base) = baseline {
                if base != &fingerprint {
                    panic!(
                        "log equivalence failed: node {} diverged from baseline in {}..={}",
                        node.id(),
                        start_idx,
                        end_idx
                    );
                }
            } else {
                baseline = Some(fingerprint);
            }
        }
    }
}

fn hash_payload(payload: &openraft::EntryPayload<octopii::openraft::types::AppTypeConfig>) -> u64 {
    let encoded = bincode::serialize(payload).unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    encoded.hash(&mut hasher);
    hasher.finish()
}

pub fn walrus_seed_sequence(init: u64, count: usize) -> Vec<u64> {
    let mut seeds = Vec::with_capacity(count);
    let mut s = init;
    for _ in 0..count {
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        seeds.push(s.wrapping_mul(0x2545f4914f6cdd1d));
    }
    seeds
}

pub fn walrus_error_rate(seed: u64, base: f64) -> f64 {
    base + ((seed % 6) as f64) * 0.01
}

struct FaultPlan {
    events: Vec<FaultEvent>,
    next_idx: usize,
}

impl FaultPlan {
    fn new(seed: u64, addrs: &[SocketAddr], profile: FaultProfile, start_ms: u64) -> Self {
        let mut rng = SimpleRng::new(seed ^ 0x55aa_55aa_0f0f_f0f0);
        let mut events = Vec::new();
        let n = addrs.len();

        match profile {
            FaultProfile::None => {}
            FaultProfile::ReorderTimeoutBandwidth => {
                if n >= 2 {
                    let t1 = 500 + rng.range(0, 300);
                    let t2 = t1 + 400 + rng.range(0, 200);
                    let t3 = t2 + 400 + rng.range(0, 200);
                    let t4 = t3 + 1000 + rng.range(0, 300);

                    let jitter = 50 + rng.range(0, 200);
                    let prob = 0.4 + (rng.range(0, 40) as f64 / 100.0);
                    let timeout_ms = 80 + rng.range(0, 200);
                    let bytes_per_ms = 5 + rng.range(0, 30);
                    let burst = bytes_per_ms * 20;

                    let (a, b) = pick_pair(&mut rng, n);
                    let (c, d) = pick_pair(&mut rng, n);
                    let (e, f) = pick_pair(&mut rng, n);

                    events.push(FaultEvent::Reorder {
                        at_ms: start_ms + t1,
                        from: addrs[a],
                        to: addrs[b],
                        max_jitter_ms: jitter,
                        probability: prob,
                    });
                    events.push(FaultEvent::Timeout {
                        at_ms: start_ms + t2,
                        from: addrs[c],
                        to: addrs[d],
                        timeout_ms,
                    });
                    events.push(FaultEvent::Bandwidth {
                        at_ms: start_ms + t3,
                        from: addrs[e],
                        to: addrs[f],
                        bytes_per_ms,
                        burst_bytes: burst,
                    });
                    events.push(FaultEvent::Clear {
                        at_ms: start_ms + t4,
                    });
                }
            }
            FaultProfile::PartitionChurn => {
                if n >= 2 {
                    let t1 = 600 + rng.range(0, 300);
                    let t2 = t1 + 500 + rng.range(0, 300);
                    let t3 = t2 + 500 + rng.range(0, 300);

                    let (group_a, group_b) = pick_partition(&mut rng, n);
                    events.push(FaultEvent::Partition {
                        at_ms: start_ms + t1,
                        group_a: group_a
                            .iter()
                            .map(|i| addrs[*i])
                            .collect::<Vec<_>>(),
                        group_b: group_b
                            .iter()
                            .map(|i| addrs[*i])
                            .collect::<Vec<_>>(),
                    });
                    events.push(FaultEvent::Clear {
                        at_ms: start_ms + t2,
                    });
                    let (group_c, group_d) = pick_partition(&mut rng, n);
                    events.push(FaultEvent::Partition {
                        at_ms: start_ms + t3,
                        group_a: group_c
                            .iter()
                            .map(|i| addrs[*i])
                            .collect::<Vec<_>>(),
                        group_b: group_d
                            .iter()
                            .map(|i| addrs[*i])
                            .collect::<Vec<_>>(),
                    });
                    events.push(FaultEvent::Clear {
                        at_ms: start_ms + t3 + 600,
                    });
                }
            }
            // Extended fault profiles - these primarily affect I/O error rates
            // which are set via ClusterParams, not network events
            FaultProfile::IoErrorsLight
            | FaultProfile::IoErrorsMedium
            | FaultProfile::IoErrorsHeavy
            | FaultProfile::PartialWrites => {
                // No network events - these profiles rely on VFS fault injection
            }
            FaultProfile::CombinedNetworkAndIo => {
                // Combine reorder/timeout with I/O errors
                if n >= 2 {
                    let t1 = 400 + rng.range(0, 200);
                    let t2 = t1 + 600 + rng.range(0, 300);

                    let jitter = 30 + rng.range(0, 100);
                    let prob = 0.3 + (rng.range(0, 30) as f64 / 100.0);

                    let (a, b) = pick_pair(&mut rng, n);
                    events.push(FaultEvent::Reorder {
                        at_ms: start_ms + t1,
                        from: addrs[a],
                        to: addrs[b],
                        max_jitter_ms: jitter,
                        probability: prob,
                    });
                    events.push(FaultEvent::Clear {
                        at_ms: start_ms + t2,
                    });
                }
            }
            FaultProfile::SplitBrain => {
                // Create a partition that isolates node 0 (typically the initial leader)
                if n >= 3 {
                    let t1 = 500 + rng.range(0, 200);
                    let t2 = t1 + 800 + rng.range(0, 400);

                    events.push(FaultEvent::Partition {
                        at_ms: start_ms + t1,
                        group_a: vec![addrs[0]],
                        group_b: addrs[1..].to_vec(),
                    });
                    events.push(FaultEvent::Clear {
                        at_ms: start_ms + t2,
                    });
                }
            }
            FaultProfile::FlappingConnectivity => {
                // Rapid partition/heal cycles
                if n >= 2 {
                    let mut t = start_ms + 300;
                    for _ in 0..5 {
                        let (group_a, group_b) = pick_partition(&mut rng, n);
                        events.push(FaultEvent::Partition {
                            at_ms: t,
                            group_a: group_a.iter().map(|i| addrs[*i]).collect(),
                            group_b: group_b.iter().map(|i| addrs[*i]).collect(),
                        });
                        t += 200 + rng.range(0, 100);
                        events.push(FaultEvent::Clear { at_ms: t });
                        t += 100 + rng.range(0, 100);
                    }
                }
            }
        }

        Self {
            events,
            next_idx: 0,
        }
    }

    fn apply(&mut self, router: &SimRouter) {
        let now = router.now_ms();
        while let Some(event) = self.events.get(self.next_idx) {
            if event.at_ms() > now {
                break;
            }
            match event.clone() {
                FaultEvent::Reorder {
                    from,
                    to,
                    max_jitter_ms,
                    probability,
                    ..
                } => {
                    router.set_reorder_pair(from, to, max_jitter_ms, probability);
                    router.set_reorder_pair(to, from, max_jitter_ms, probability);
                }
                FaultEvent::Timeout {
                    from,
                    to,
                    timeout_ms,
                    ..
                } => {
                    router.set_timeout_pair(from, to, timeout_ms);
                    router.set_timeout_pair(to, from, timeout_ms);
                }
                FaultEvent::Bandwidth {
                    from,
                    to,
                    bytes_per_ms,
                    burst_bytes,
                    ..
                } => {
                    router.set_bandwidth_pair(from, to, bytes_per_ms, burst_bytes);
                    router.set_bandwidth_pair(to, from, bytes_per_ms, burst_bytes);
                }
                FaultEvent::Partition {
                    group_a,
                    group_b,
                    ..
                } => {
                    router.add_partition(group_a, group_b);
                }
                FaultEvent::Clear { .. } => {
                    router.clear_faults();
                }
            }
            self.next_idx += 1;
        }
    }
}

#[derive(Clone)]
enum FaultEvent {
    Reorder {
        at_ms: u64,
        from: SocketAddr,
        to: SocketAddr,
        max_jitter_ms: u64,
        probability: f64,
    },
    Timeout {
        at_ms: u64,
        from: SocketAddr,
        to: SocketAddr,
        timeout_ms: u64,
    },
    Bandwidth {
        at_ms: u64,
        from: SocketAddr,
        to: SocketAddr,
        bytes_per_ms: u64,
        burst_bytes: u64,
    },
    Partition {
        at_ms: u64,
        group_a: Vec<SocketAddr>,
        group_b: Vec<SocketAddr>,
    },
    Clear {
        at_ms: u64,
    },
}

impl FaultEvent {
    fn at_ms(&self) -> u64 {
        match self {
            FaultEvent::Reorder { at_ms, .. } => *at_ms,
            FaultEvent::Timeout { at_ms, .. } => *at_ms,
            FaultEvent::Bandwidth { at_ms, .. } => *at_ms,
            FaultEvent::Partition { at_ms, .. } => *at_ms,
            FaultEvent::Clear { at_ms } => *at_ms,
        }
    }
}

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        let mut rng = Self { state: 0 };
        rng.state = seed.wrapping_add(0x9E3779B97F4A7C15);
        rng.next_u64();
        rng
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn range(&mut self, min: u64, max_exclusive: u64) -> u64 {
        if max_exclusive <= min + 1 {
            return min;
        }
        min + (self.next_u64() % (max_exclusive - min))
    }
}

fn pick_pair(rng: &mut SimpleRng, n: usize) -> (usize, usize) {
    let a = rng.range(0, n as u64) as usize;
    let mut b = rng.range(0, n as u64) as usize;
    if b == a {
        b = (b + 1) % n;
    }
    (a, b)
}

fn pick_partition(rng: &mut SimpleRng, n: usize) -> (Vec<usize>, Vec<usize>) {
    let mut idxs = (0..n).collect::<Vec<_>>();
    for i in (1..n).rev() {
        let j = rng.range(0, (i + 1) as u64) as usize;
        idxs.swap(i, j);
    }
    let split = 1 + rng.range(0, (n - 1) as u64) as usize;
    let group_a = idxs[..split].to_vec();
    let group_b = idxs[split..].to_vec();
    (group_a, group_b)
}

async fn drive_join<T>(
    router: &SimRouter,
    plan: &mut FaultPlan,
    mut handle: tokio::task::JoinHandle<T>,
    step_ms: u64,
) -> T {
    yield_now().await;
    while !handle.is_finished() {
        router.advance_time(step_ms);
        plan.apply(router);
        router.deliver_ready();
        sim_runtime::advance_time(Duration::from_millis(step_ms));
        yield_now().await;
    }
    handle.await.expect("task join")
}

async fn new_node_with_retry(
    config: Config,
    rt: OctopiiRuntime,
    transport: Arc<dyn Transport>,
) -> Arc<OpenRaftNode> {
    for attempt in 0..10 {
        let prev_rate = sim::get_io_error_rate();
        sim::set_io_error_rate(0.0);
        let node_res = OpenRaftNode::new_sim(config.clone(), rt.clone(), Arc::clone(&transport))
            .await;
        let node_err = node_res.as_ref().err().map(|e| e.to_string());
        let node = node_res.ok().map(Arc::new);
        let started = if let Some(node) = node.as_ref() {
            node.start().await.is_ok()
        } else {
            false
        };
        sim::set_io_error_rate(prev_rate);
        if let (Some(node), true) = (node, started) {
            return node;
        }
        if ClusterHarness::debug_enabled() {
            eprintln!(
                "[cluster_debug] new_node_with_retry attempt={} failed: new_res={:?} started={}",
                attempt,
                node_err.as_deref(),
                started
            );
        }
        let _ = std::fs::remove_dir_all(&config.wal_dir);
        sim_runtime::advance_time(Duration::from_millis(50));
        yield_now().await;
        if attempt == 9 {
            panic!("failed to create node after retries");
        }
    }
    unreachable!()
}
