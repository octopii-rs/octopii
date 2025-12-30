use crate::wal::wal::config::{debug_print, is_io_uring_enabled, FsyncSchedule};
use crate::wal::wal::storage::{open_storage_for_path, StorageImpl};
use crate::wal::wal::vfs as fs;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[cfg(not(feature = "simulation"))]
use super::DELETION_TX;
#[cfg(feature = "simulation")]
use super::set_deletion_tx;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
use io_uring;

/// State machine struct for background work.
///
/// In simulation mode, this is owned by `Walrus` and ticked manually.
/// In production mode, this runs in a background thread.
pub struct BackgroundWorker {
    rx: mpsc::Receiver<String>,
    del_rx: mpsc::Receiver<String>,
    pool: HashMap<String, StorageImpl>,
    tick_count: u64,
    delete_pending: HashSet<String>,
    #[cfg(target_os = "linux")]
    ring: Option<io_uring::IoUring>,
}

// Safety: StorageImpl is Send, io_uring is Send, mpsc::Receiver is Send.
unsafe impl Send for BackgroundWorker {}

impl BackgroundWorker {
    fn new(rx: mpsc::Receiver<String>, del_rx: mpsc::Receiver<String>) -> Self {
        #[cfg(target_os = "linux")]
        let ring = io_uring::IoUring::new(2048).ok();

        Self {
            rx,
            del_rx,
            pool: HashMap::new(),
            tick_count: 0,
            delete_pending: HashSet::new(),
            #[cfg(target_os = "linux")]
            ring,
        }
    }

    /// Deterministic step function - performs one iteration of background work.
    ///
    /// This was formerly the loop body in the background thread. By extracting it
    /// into a method, we can call it manually in simulation mode for deterministic
    /// control over when fsyncs and cleanups happen.
    pub fn tick(&mut self) {
        // Phase 1: Collect unique paths to flush
        let mut unique = HashSet::new();
        while let Ok(path) = self.rx.try_recv() {
            unique.insert(path);
        }

        if !unique.is_empty() {
            debug_print!("[flush] scheduling {} paths", unique.len());
        }

        // Phase 2: Open/map files if needed
        for path in unique.iter() {
            // Skip if file doesn't exist
            if !fs::exists(&path) {
                debug_print!("[flush] file does not exist, skipping: {}", path);
                continue;
            }

            if !self.pool.contains_key(path) {
                match open_storage_for_path(path) {
                    Ok(storage) => {
                        self.pool.insert(path.clone(), storage);
                    }
                    Err(e) => {
                        debug_print!("[flush] failed to open storage for {}: {}", path, e);
                    }
                }
            }
        }

        // Phase 3: Flush operations
        #[cfg(target_os = "linux")]
        {
            if is_io_uring_enabled() {
                // FD backend: Use io_uring for batched fsync
                let mut fsync_batch = Vec::new();

                for path in unique.iter() {
                    if let Some(storage) = self.pool.get(path) {
                        if let Some(fd_backend) = storage.as_fd() {
                            let raw_fd = fd_backend.file().as_raw_fd();
                            fsync_batch.push((raw_fd, path.clone()));
                        }
                    }
                }

                if !fsync_batch.is_empty() {
                    // Try io_uring if available, otherwise fallback to sync flush
                    if let Some(ring) = self.ring.as_mut() {
                        debug_print!(
                            "[flush] batching {} fsync operations (io_uring)",
                            fsync_batch.len()
                        );

                        // Push all fsync operations to submission queue
                        for (i, (raw_fd, _path)) in fsync_batch.iter().enumerate() {
                            let fd = io_uring::types::Fd(*raw_fd);

                            let fsync_op =
                                io_uring::opcode::Fsync::new(fd).build().user_data(i as u64);

                            unsafe {
                                if ring.submission().push(&fsync_op).is_err() {
                                    // Submission queue full, submit current batch
                                    ring.submit().expect("Failed to submit fsync batch");
                                    ring.submission()
                                        .push(&fsync_op)
                                        .expect("Failed to push fsync op");
                                }
                            }
                        }

                        // Single syscall to submit all fsync operations!
                        match ring.submit_and_wait(fsync_batch.len()) {
                            Ok(submitted) => {
                                debug_print!(
                                    "[flush] submitted {} fsync ops in one syscall",
                                    submitted
                                );
                            }
                            Err(e) => {
                                debug_print!("[flush] failed to submit fsync batch: {}", e);
                            }
                        }

                        // Process completions
                        for _ in 0..fsync_batch.len() {
                            if let Some(cqe) = ring.completion().next() {
                                let idx = cqe.user_data() as usize;
                                let result = cqe.result();

                                if result < 0 {
                                    let (_fd, path) = &fsync_batch[idx];
                                    debug_print!(
                                        "[flush] fsync error for {}: error code {}",
                                        path,
                                        result
                                    );
                                }
                            }
                        }
                    } else {
                        // Fallback: No io_uring, use sync flush
                        debug_print!(
                            "[flush] fallback: syncing {} files without io_uring",
                            fsync_batch.len()
                        );
                        for (_fd, path) in fsync_batch.iter() {
                            if let Some(storage) = self.pool.get_mut(path) {
                                if let Err(e) = storage.flush() {
                                    debug_print!("[flush] flush error for {}: {}", path, e);
                                }
                            }
                        }
                    }
                }
            } else {
                for path in unique.iter() {
                    if let Some(storage) = self.pool.get_mut(path) {
                        if let Err(e) = storage.flush() {
                            debug_print!("[flush] flush error for {}: {}", path, e);
                        }
                    }
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            for path in unique.iter() {
                if let Some(storage) = self.pool.get_mut(path) {
                    if let Err(e) = storage.flush() {
                        debug_print!("[flush] flush error for {}: {}", path, e);
                    }
                }
            }
        }

        // Phase 4: Handle deletion requests
        while let Ok(path) = self.del_rx.try_recv() {
            debug_print!("[reclaim] deletion requested: {}", path);
            self.delete_pending.insert(path);
        }

        // Phase 5: Periodic cleanup (every 1000 ticks)
        self.tick_count += 1;
        if self.tick_count >= 1000 {
            self.tick_count = 0;

            let mut empty: HashMap<String, StorageImpl> = HashMap::new();
            std::mem::swap(&mut self.pool, &mut empty); // reset map every 1000 ticks

            // Perform batched deletions now that mmaps/fds are dropped
            for path in self.delete_pending.drain() {
                match fs::remove_file(&path) {
                    Ok(_) => debug_print!("[reclaim] deleted file {}", path),
                    Err(e) => {
                        debug_print!("[reclaim] delete failed for {}: {}", path, e)
                    }
                }
            }
        }
    }
}

/// Result type for start_background_workers - contains sender and optional worker handle
pub struct BackgroundHandle {
    pub tx: Arc<mpsc::Sender<String>>,
    pub worker: Option<Arc<Mutex<BackgroundWorker>>>,
}

/// Start background workers for fsync and cleanup.
///
/// In simulation mode (`--features simulation`), returns the worker handle so it can
/// be ticked manually by `Walrus::tick_background()`.
///
/// In production mode, spawns a background thread and returns None for the worker.
pub(super) fn start_background_workers(fsync_schedule: FsyncSchedule) -> BackgroundHandle {
    let (tx, rx) = mpsc::channel::<String>();
    let tx_arc = Arc::new(tx);
    let (del_tx, del_rx) = mpsc::channel::<String>();
    let del_tx_arc = Arc::new(del_tx);
    #[cfg(feature = "simulation")]
    {
        set_deletion_tx(del_tx_arc.clone());
    }
    #[cfg(not(feature = "simulation"))]
    {
        let _ = DELETION_TX.set(del_tx_arc.clone());
    }

    let sleep_millis = match fsync_schedule {
        FsyncSchedule::Milliseconds(ms) => ms.max(1),
        FsyncSchedule::SyncEach => 5000,
        FsyncSchedule::NoFsync => 10000,
    };

    let worker = BackgroundWorker::new(rx, del_rx);

    #[cfg(feature = "simulation")]
    {
        // In simulation mode, return the worker to the caller (Walrus) so it can be ticked manually
        return BackgroundHandle {
            tx: tx_arc,
            worker: Some(Arc::new(Mutex::new(worker))),
        };
    }

    #[cfg(not(feature = "simulation"))]
    {
        // In production mode, spawn the background thread as before
        let mut worker = worker;
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(sleep_millis));
            worker.tick();
        });
        return BackgroundHandle {
            tx: tx_arc,
            worker: None,
        };
    }
}
