#[cfg(feature = "simulation")]
mod sim_isolation_tests {
    use octopii::wal::wal::vfs;
    use octopii::wal::wal::vfs::sim::{self, SimConfig, SimStats};
    use octopii::wal::wal::{FsyncSchedule, ReadConsistency, Walrus};
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;

    fn run_deterministic_probe(seed: u64) -> (Vec<u8>, SimStats) {
        sim::setup(SimConfig {
            seed,
            io_error_rate: 0.0,
            initial_time_ns: 1_700_000_000_000_000_000,
            enable_partial_writes: false,
        });

        let root = PathBuf::from("/tmp/sim_vfs_probe");
        let _ = vfs::remove_dir_all(&root);
        vfs::create_dir_all(&root).expect("create dir");

        let file_path = root.join("probe.bin");
        let mut f = vfs::File::create(&file_path).expect("create file");
        f.write_all(b"deterministic-payload").expect("write payload");
        f.sync_all().expect("sync payload");

        let mut f = vfs::File::open(&file_path).expect("open file");
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).expect("read file");

        let stats = sim::stats().expect("stats available");
        sim::teardown();
        (buf, stats)
    }

    #[test]
    fn deterministic_vfs_replay_matches() {
        let (buf_a, stats_a) = run_deterministic_probe(4242);
        let (buf_b, stats_b) = run_deterministic_probe(4242);

        assert_eq!(buf_a, buf_b);
        assert_eq!(stats_a.io_op_count, stats_b.io_op_count);
        assert_eq!(stats_a.io_fail_count, stats_b.io_fail_count);
        assert_eq!(stats_a.current_time_ns, stats_b.current_time_ns);
    }

    #[test]
    fn thread_isolation_blocks_cross_visibility() {
        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx) = mpsc::channel::<bool>();
        let shared_root = PathBuf::from("/tmp/sim_thread_isolation");
        let shared_key = "sim_isolation_key";

        let barrier_a = Arc::clone(&barrier);
        let tx_a = tx.clone();
        let root_a = shared_root.clone();
        let key_a = shared_key.to_string();
        let thread_a = thread::spawn(move || {
            sim::setup(SimConfig {
                seed: 1,
                io_error_rate: 0.0,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: false,
            });
            octopii::wal::wal::__set_thread_wal_data_dir_for_tests(root_a.clone());
            vfs::create_dir_all(&root_a).expect("create root dir");

            let wal = Walrus::with_consistency_and_schedule_for_key(
                &key_a,
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("walrus init");
            wal.append_for_topic("orders", b"thread-a")
                .expect("append a");

            barrier_a.wait();
            octopii::wal::wal::__clear_thread_wal_data_dir_for_tests();
            sim::teardown();
            tx_a.send(true).ok();
        });

        let barrier_b = Arc::clone(&barrier);
        let root_b = shared_root.clone();
        let key_b = shared_key.to_string();
        let thread_b = thread::spawn(move || {
            sim::setup(SimConfig {
                seed: 2,
                io_error_rate: 0.0,
                initial_time_ns: 1_700_000_000_000_000_000,
                enable_partial_writes: false,
            });
            octopii::wal::wal::__set_thread_wal_data_dir_for_tests(root_b.clone());
            vfs::create_dir_all(&root_b).expect("create root dir");

            let wal = Walrus::with_consistency_and_schedule_for_key(
                &key_b,
                ReadConsistency::StrictlyAtOnce,
                FsyncSchedule::SyncEach,
            )
            .expect("walrus init");

            barrier_b.wait();

            let read = wal.read_next("orders", true).expect("read");
            assert!(read.is_none(), "thread B saw thread A data");

            wal.append_for_topic("orders", b"thread-b")
                .expect("append b");
            let read = wal.read_next("orders", true).expect("read");
            assert!(read.is_some(), "thread B could not read its own write");

            octopii::wal::wal::__clear_thread_wal_data_dir_for_tests();
            sim::teardown();
        });

        thread_a.join().expect("thread a");
        thread_b.join().expect("thread b");
        assert!(rx.recv().unwrap_or(false));
    }
}
