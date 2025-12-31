#[cfg(all(feature = "simulation", feature = "openraft"))]
mod cluster_sim_tests {
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

    async fn tick(router: &SimRouter, plan: &mut FaultPlan, steps: usize, step_ms: u64) {
        for _ in 0..steps {
            router.advance_time(step_ms);
            plan.apply(router);
            router.deliver_ready();
            sim_runtime::advance_time(Duration::from_millis(step_ms));
            yield_now().await;
        }
    }

    async fn wait_for_leader(
        nodes: &[Arc<OpenRaftNode>],
        router: &SimRouter,
        plan: &mut FaultPlan,
    ) -> Option<u64> {
        for _ in 0..2000 {
            tick(router, plan, 5, 50).await;
            for node in nodes {
                if node.is_leader().await {
                    return Some(node.id());
                }
            }
        }
        None
    }

    async fn drive_join<T>(
        router: &SimRouter,
        plan: &mut FaultPlan,
        mut handle: tokio::task::JoinHandle<T>,
        step_ms: u64,
    ) -> T {
        yield_now().await;
        while !handle.is_finished() {
            tick(router, plan, 1, step_ms).await;
        }
        handle.await.expect("task join")
    }

    async fn propose_with_leader(
        nodes: &[Arc<OpenRaftNode>],
        router: &SimRouter,
        plan: &mut FaultPlan,
        command: Vec<u8>,
    ) -> octopii::Result<()> {
        for _ in 0..20 {
            let leader_id = wait_for_leader(nodes, router, plan).await;
            let Some(leader_id) = leader_id else {
                tick(router, plan, 5, 50).await;
                continue;
            };
            let leader = nodes
                .iter()
                .find(|n| n.id() == leader_id)
                .cloned()
                .expect("leader node");
            let propose_task = {
                let leader = Arc::clone(&leader);
                let data = command.clone();
                tokio::spawn(async move { leader.propose(data).await })
            };
            match drive_join(router, plan, propose_task, 50).await {
                Ok(_) => return Ok(()),
                Err(_) => {
                    tick(router, plan, 5, 50).await;
                }
            }
        }
        Err(octopii::error::OctopiiError::Rpc(
            "failed to propose after retries".to_string(),
        ))
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

    struct FaultPlan {
        events: Vec<FaultEvent>,
        next_idx: usize,
    }

    impl FaultPlan {
        fn empty() -> Self {
            Self {
                events: Vec::new(),
                next_idx: 0,
            }
        }

        fn new(seed: u64, addrs: &[SocketAddr], start_ms: u64) -> Self {
            let mut rng = SimpleRng::new(seed);
            let a = addrs[0];
            let b = addrs[1];
            let c = addrs[2];

            let t1 = 500 + rng.range(0, 200);
            let t2 = t1 + 500 + rng.range(0, 200);
            let t3 = t2 + 500 + rng.range(0, 200);
            let t4 = t3 + 1000 + rng.range(0, 200);

            let jitter = 100 + rng.range(0, 200);
            let prob = 0.5 + (rng.range(0, 50) as f64 / 100.0);
            let timeout_ms = 80 + rng.range(0, 120);
            let bytes_per_ms = 10 + rng.range(0, 40);
            let burst = bytes_per_ms * 10;

            let events = vec![
                FaultEvent::Reorder {
                    at_ms: start_ms + t1,
                    from: a,
                    to: b,
                    max_jitter_ms: jitter,
                    probability: prob,
                },
                FaultEvent::Timeout {
                    at_ms: start_ms + t2,
                    from: b,
                    to: c,
                    timeout_ms,
                },
                FaultEvent::Bandwidth {
                    at_ms: start_ms + t3,
                    from: a,
                    to: c,
                    bytes_per_ms,
                    burst_bytes: burst,
                },
                FaultEvent::Clear {
                    at_ms: start_ms + t4,
                },
            ];

            Self { events, next_idx: 0 }
        }

        fn apply(&mut self, router: &SimRouter) {
            let now = router.now_ms();
            while let Some(event) = self.events.get(self.next_idx) {
                if event.at_ms() > now {
                    break;
                }
                match *event {
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
                    FaultEvent::Clear { .. } => {
                        router.clear_faults();
                    }
                }
                self.next_idx += 1;
            }
        }
    }

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
        Clear {
            at_ms: u64,
        },
    }

    impl FaultEvent {
        fn at_ms(&self) -> u64 {
            match *self {
                FaultEvent::Reorder { at_ms, .. } => at_ms,
                FaultEvent::Timeout { at_ms, .. } => at_ms,
                FaultEvent::Bandwidth { at_ms, .. } => at_ms,
                FaultEvent::Clear { at_ms } => at_ms,
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

    fn make_config(
        node_id: u64,
        bind_addr: SocketAddr,
        peers: Vec<SocketAddr>,
        wal_dir: PathBuf,
        is_initial_leader: bool,
    ) -> Config {
        Config {
            node_id,
            bind_addr,
            peers,
            wal_dir,
            worker_threads: 1,
            wal_batch_size: 10,
            wal_flush_interval_ms: 50,
            is_initial_leader,
            snapshot_lag_threshold: 500,
        }
    }

    async fn run_cluster_with_config(seed: u64, io_error_rate: f64) {
        let base = std::env::temp_dir().join(format!("octopii_sim_cluster_{seed}"));
        let _ = std::fs::remove_dir_all(&base);

        std::env::set_var("WALRUS_QUIET", "1");
        sim::setup(VfsSimConfig {
            seed,
            io_error_rate,
            initial_time_ns: 1700000000_000_000_000,
            enable_partial_writes: false,
        });
        sim_runtime::reset(seed, 1700000000_000_000_000);

        let router = SimRouter::new(SimConfig {
            seed: seed ^ 0xdead_beef,
            drop_rate: 0.0,
            min_delay_ms: 0,
            max_delay_ms: 0,
        });

        let addr1: SocketAddr = "127.0.0.1:9711".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9712".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:9713".parse().unwrap();

        let transport1 = Arc::new(SimTransport::new(addr1, router.clone()));
        let transport2 = Arc::new(SimTransport::new(addr2, router.clone()));
        let transport3 = Arc::new(SimTransport::new(addr3, router.clone()));

        let rt = OctopiiRuntime::from_handle(tokio::runtime::Handle::current());

        let config1 = make_config(
            1,
            addr1,
            vec![addr2, addr3],
            base.join("n1"),
            true,
        );
        let config2 = make_config(2, addr2, vec![addr1, addr3], base.join("n2"), false);
        let config3 = make_config(3, addr3, vec![addr1, addr2], base.join("n3"), false);

        let mut fault_plan = FaultPlan::empty();

        let n1 = new_node_with_retry(
            config1,
            rt.clone(),
            Arc::clone(&transport1) as Arc<dyn Transport>,
        )
        .await;
        let n2 = new_node_with_retry(
            config2,
            rt.clone(),
            Arc::clone(&transport2) as Arc<dyn Transport>,
        )
        .await;
        let n3 = new_node_with_retry(
            config3,
            rt.clone(),
            Arc::clone(&transport3) as Arc<dyn Transport>,
        )
        .await;

        let nodes = vec![Arc::clone(&n1), Arc::clone(&n2), Arc::clone(&n3)];
        let leader = wait_for_leader(&nodes, &router, &mut fault_plan).await;
        assert!(leader.is_some(), "leader election did not complete");

        let mut fault_plan = FaultPlan::new(seed, &[addr1, addr2, addr3], router.now_ms());

        propose_with_leader(&nodes, &router, &mut fault_plan, b"SET alpha one".to_vec())
            .await
            .expect("propose");
        tick(&router, &mut fault_plan, 50, 50).await;

        let read1 = n1.query(b"GET alpha").await.expect("read n1");
        let read2 = n2.query(b"GET alpha").await.expect("read n2");
        let read3 = n3.query(b"GET alpha").await.expect("read n3");

        assert_eq!(read1, read2);
        assert_eq!(read2, read3);

        let _ = std::fs::remove_dir_all(&base);
        wal::__clear_storage_cache_for_tests();
        sim::teardown();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn deterministic_cluster_three_nodes() {
        run_cluster_with_config(424242, 0.10).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fault_injection_cluster_seed_424242() {
        run_cluster_with_config(424242, 0.15).await;
    }
}
