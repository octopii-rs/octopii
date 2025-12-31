#![cfg(all(feature = "simulation", feature = "openraft"))]

use octopii::config::Config;
use octopii::openraft::node::OpenRaftNode;
use octopii::openraft::sim_runtime;
use octopii::runtime::OctopiiRuntime;
use octopii::transport::{SimConfig, SimRouter, SimTransport, Transport};
use octopii::wal::wal::vfs::sim::{self, SimConfig as VfsSimConfig};
use octopii::wal::wal;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::yield_now;
use tokio::time::Duration;

#[derive(Clone, Copy)]
pub enum FaultProfile {
    None,
    ReorderTimeoutBandwidth,
    PartitionChurn,
}

pub struct ClusterParams {
    pub size: usize,
    pub seed: u64,
    pub io_error_rate: f64,
    pub profile: FaultProfile,
    pub enable_partial_writes: bool,
    pub snapshot_lag_threshold: u64,
    pub require_all_nodes: bool,
}

impl ClusterParams {
    pub fn new(size: usize, seed: u64, io_error_rate: f64, profile: FaultProfile) -> Self {
        Self {
            size,
            seed,
            io_error_rate,
            profile,
            enable_partial_writes: false,
            snapshot_lag_threshold: 500,
            require_all_nodes: true,
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
}

#[derive(Clone, Copy)]
pub enum ValidationMode {
    Cluster,
    Leader,
}

impl ClusterHarness {
    pub async fn new(params: ClusterParams) -> Self {
        let base = std::env::temp_dir().join(format!(
            "octopii_sim_cluster_{}_n{}",
            params.seed, params.size
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

        let base_port = 9700 + (params.seed % 1000) as u16;
        let mut addrs = Vec::with_capacity(params.size);
        for i in 0..params.size {
            let port = base_port + i as u16;
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
        }
    }

    pub async fn tick(&mut self, steps: usize, step_ms: u64) {
        for _ in 0..steps {
            self.router.advance_time(step_ms);
            self.fault_plan.apply(&self.router);
            self.router.deliver_ready();
            sim_runtime::advance_time(Duration::from_millis(step_ms));
            yield_now().await;
        }
    }

    pub async fn wait_for_leader(&mut self) -> Option<u64> {
        for _ in 0..2000 {
            self.tick(5, 50).await;
            for node in &self.nodes {
                if node.is_leader().await {
                    return Some(node.id());
                }
            }
        }
        None
    }

    pub async fn propose_with_leader(&mut self, command: Vec<u8>) -> octopii::Result<()> {
        for _ in 0..20 {
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
        }
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
            }
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
            node.shutdown();
        }
        let mut cfg = self.configs[idx].clone();
        cfg.is_initial_leader = false;
        let transport = Arc::clone(&self.transports[idx]) as Arc<dyn Transport>;
        let node = new_node_with_retry(cfg, self.rt.clone(), transport).await;
        self.nodes[idx] = node;
    }

    pub fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.base);
        wal::__clear_storage_cache_for_tests();
        sim::teardown();
    }
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
        let node = OpenRaftNode::new_sim(config.clone(), rt.clone(), Arc::clone(&transport))
            .await
            .ok()
            .map(Arc::new);
        let started = if let Some(node) = node.as_ref() {
            node.start().await.is_ok()
        } else {
            false
        };
        sim::set_io_error_rate(prev_rate);
        if let (Some(node), true) = (node, started) {
            return node;
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
