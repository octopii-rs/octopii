//! Virtual File System (VFS) Abstraction Layer for Deterministic Simulation Testing
//!
//! This module provides a drop-in replacement for `std::fs` and `std::time` that operates
//! in two modes:
//!
//! 1. **Default mode**: Zero-cost pass-through to the standard library
//! 2. **Simulation mode** (`--features simulation`): Deterministic control over time and
//!    injectable I/O failures for testing crash recovery and fault tolerance
//!
//! # Usage
//!
//! Replace `std::fs::File` with `vfs::File`, `std::fs::OpenOptions` with `vfs::OpenOptions`,
//! and `std::time::SystemTime::now()` with `vfs::now()`.
//!
//! # Simulation Mode
//!
//! When compiled with `--features simulation`, you must call `vfs::sim::setup()` before
//! any I/O operations. This initializes the deterministic RNG and virtual clock.
//!
//! ```ignore
//! #[cfg(feature = "simulation")]
//! vfs::sim::setup(SimConfig {
//!     seed: 42,
//!     io_error_rate: 0.01,  // 1% chance of I/O failure
//!     initial_time_ns: 0,
//! });
//! ```

use std::io::{self, Read, Write};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// ============================================================================
// SIMULATION CONTEXT (only compiled with `simulation` feature)
// ============================================================================

#[cfg(feature = "simulation")]
pub mod sim {
    use super::*;
    use std::cell::RefCell;
    use std::collections::{BTreeSet, HashMap};
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};

    #[derive(Debug, Default)]
    struct SimFile {
        data: Vec<u8>,
        len: usize,
    }

    #[derive(Debug, Default)]
    struct SimFs {
        files: HashMap<PathBuf, SimFile>,
        dirs: BTreeSet<PathBuf>,
    }

    impl SimFs {
        fn new() -> Self {
            Self {
                files: HashMap::new(),
                dirs: BTreeSet::new(),
            }
        }

        fn ensure_dir_all(&mut self, path: &Path) {
            let mut cur = PathBuf::new();
            for comp in path.components() {
                cur.push(comp.as_os_str());
                self.dirs.insert(cur.clone());
            }
        }

        fn ensure_parent_dirs(&mut self, path: &Path) {
            if let Some(parent) = path.parent() {
                self.ensure_dir_all(parent);
            }
        }

        fn file_len(&self, path: &Path) -> Option<usize> {
            self.files.get(path).map(|f| f.len)
        }

        fn create_file(&mut self, path: &Path, truncate: bool) {
            self.ensure_parent_dirs(path);
            let entry = self.files.entry(path.to_path_buf()).or_default();
            if truncate {
                entry.data.clear();
                entry.len = 0;
            }
        }

        fn set_len(&mut self, path: &Path, size: usize) -> io::Result<()> {
            let file = self
                .files
                .get_mut(path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            file.len = size;
            if file.data.len() > size {
                file.data.truncate(size);
            }
            Ok(())
        }

        fn read_at(&self, path: &Path, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
            let file = self
                .files
                .get(path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            if offset >= file.len {
                return Ok(0);
            }
            let end = (offset + buf.len()).min(file.len);
            let len = end - offset;
            buf[..len].fill(0);
            if offset < file.data.len() {
                let data_end = end.min(file.data.len());
                let data_len = data_end - offset;
                buf[..data_len].copy_from_slice(&file.data[offset..data_end]);
            }
            Ok(len)
        }

        fn write_at(&mut self, path: &Path, offset: usize, buf: &[u8]) -> io::Result<usize> {
            let file = self
                .files
                .get_mut(path)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))?;
            let end = offset.saturating_add(buf.len());
            if end > file.data.len() {
                file.data.resize(end, 0);
            }
            file.data[offset..end].copy_from_slice(buf);
            if end > file.len {
                file.len = end;
            }
            Ok(buf.len())
        }

        fn remove_file(&mut self, path: &Path) -> io::Result<()> {
            if self.files.remove(path).is_some() {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "file not found"))
            }
        }

        fn remove_dir(&mut self, path: &Path) -> io::Result<()> {
            if self
                .dirs
                .iter()
                .any(|dir| dir.parent() == Some(path))
                || self.files.keys().any(|file| file.parent() == Some(path))
            {
                return Err(io::Error::new(
                    io::ErrorKind::DirectoryNotEmpty,
                    "directory not empty",
                ));
            }
            if self.dirs.remove(path) {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "dir not found"))
            }
        }

        fn remove_dir_all(&mut self, path: &Path) -> io::Result<()> {
            let prefix = path.to_path_buf();
            let exists = self.dirs.contains(path) || self.files.contains_key(path);
            if !exists {
                return Err(io::Error::new(io::ErrorKind::NotFound, "path not found"));
            }
            self.files.retain(|p, _| !p.starts_with(&prefix));
            self.dirs.retain(|p| !p.starts_with(&prefix));
            Ok(())
        }

        fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
            if let Some(file) = self.files.remove(from) {
                self.ensure_parent_dirs(to);
                self.files.insert(to.to_path_buf(), file);
                return Ok(());
            }

            if self.dirs.contains(from) {
                let from_prefix = from.to_path_buf();
                let to_prefix = to.to_path_buf();
                let mut new_dirs = BTreeSet::new();
                let old_dirs = std::mem::take(&mut self.dirs);
                for dir in old_dirs {
                    if dir.starts_with(&from_prefix) {
                        let suffix = dir.strip_prefix(&from_prefix).unwrap_or(Path::new(""));
                        let mut new_path = to_prefix.clone();
                        if !suffix.as_os_str().is_empty() {
                            new_path.push(suffix);
                        }
                        new_dirs.insert(new_path);
                    } else {
                        new_dirs.insert(dir);
                    }
                }
                self.dirs = new_dirs;

                let mut new_files = HashMap::new();
                for (path, file) in self.files.drain() {
                    if path.starts_with(&from_prefix) {
                        let suffix = path.strip_prefix(&from_prefix).unwrap_or(Path::new(""));
                        let mut new_path = to_prefix.clone();
                        if !suffix.as_os_str().is_empty() {
                            new_path.push(suffix);
                        }
                        new_files.insert(new_path, file);
                    } else {
                        new_files.insert(path, file);
                    }
                }
                self.files = new_files;
                return Ok(());
            }

            Err(io::Error::new(io::ErrorKind::NotFound, "path not found"))
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<SimDirEntry>> {
            if !self.dirs.contains(path) && !path.as_os_str().is_empty() {
                return Err(io::Error::new(io::ErrorKind::NotFound, "dir not found"));
            }
            let mut entries = Vec::new();
            for dir in &self.dirs {
                if dir.parent() == Some(path) {
                    entries.push(SimDirEntry::new(dir.clone(), true));
                }
            }
            for file in self.files.keys() {
                if file.parent() == Some(path) {
                    entries.push(SimDirEntry::new(file.clone(), false));
                }
            }
            entries.sort_by_key(|entry| entry.file_name().to_string_lossy().into_owned());
            Ok(entries)
        }
    }

    #[derive(Debug, Clone)]
    pub(crate) struct SimFileHandle {
        pub(crate) path: PathBuf,
        pub(crate) cursor: usize,
        pub(crate) readable: bool,
        pub(crate) writable: bool,
        pub(crate) append: bool,
    }

    #[derive(Debug, Clone, Default)]
    pub(crate) struct SimOpenOptions {
        pub(crate) read: bool,
        pub(crate) write: bool,
        pub(crate) append: bool,
        pub(crate) truncate: bool,
        pub(crate) create: bool,
        pub(crate) create_new: bool,
    }

    #[derive(Debug, Clone)]
    pub(crate) struct SimDirEntry {
        path: PathBuf,
        is_dir: bool,
    }

    impl SimDirEntry {
        fn new(path: PathBuf, is_dir: bool) -> Self {
            Self { path, is_dir }
        }

        pub(crate) fn path(&self) -> PathBuf {
            self.path.clone()
        }

        pub(crate) fn file_name(&self) -> OsString {
            self.path
                .file_name()
                .map(|s| s.to_os_string())
                .unwrap_or_default()
        }

        pub(crate) fn is_dir(&self) -> bool {
            self.is_dir
        }
    }

    /// XorShift128+ PRNG - fast, reproducible, no dependencies
    ///
    /// This is not cryptographically secure, but provides excellent statistical
    /// properties for simulation testing with minimal overhead.
    #[derive(Debug, Clone)]
    pub struct XorShift128 {
        s0: u64,
        s1: u64,
    }

    impl XorShift128 {
        /// Create a new RNG from a 64-bit seed
        pub fn new(seed: u64) -> Self {
            // Use splitmix64 to initialize state from a single seed
            // This ensures good initial state even with poor seeds
            let mut state = seed;
            let s0 = Self::splitmix64(&mut state);
            let s1 = Self::splitmix64(&mut state);
            Self {
                s0: if s0 == 0 { 1 } else { s0 },
                s1: if s1 == 0 { 1 } else { s1 },
            }
        }

        fn splitmix64(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^ (z >> 31)
        }

        /// Generate next u64
        pub fn next_u64(&mut self) -> u64 {
            let s0 = self.s0;
            let mut s1 = self.s1;
            let result = s0.wrapping_add(s1);

            s1 ^= s0;
            self.s0 = s0.rotate_left(24) ^ s1 ^ (s1 << 16);
            self.s1 = s1.rotate_left(37);

            result
        }

        /// Generate a random f64 in [0.0, 1.0)
        pub fn next_f64(&mut self) -> f64 {
            // Use upper 53 bits for maximum precision in f64 mantissa
            (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
        }

        /// Generate a random usize in [0, max)
        pub fn next_usize(&mut self, max: usize) -> usize {
            if max == 0 {
                return 0;
            }
            (self.next_u64() as usize) % max
        }
    }

    /// Configuration for simulation mode
    #[derive(Debug, Clone)]
    pub struct SimConfig {
        /// Seed for deterministic RNG
        pub seed: u64,
        /// Probability of I/O operation failing (0.0 = never, 1.0 = always)
        pub io_error_rate: f64,
        /// Initial virtual time in nanoseconds since UNIX_EPOCH
        pub initial_time_ns: u64,
        /// Whether to enable partial write simulation
        pub enable_partial_writes: bool,
    }

    impl Default for SimConfig {
        fn default() -> Self {
            Self {
                seed: 0,
                io_error_rate: 0.0,
                initial_time_ns: 1_700_000_000_000_000_000, // ~2023
                enable_partial_writes: false,
            }
        }
    }

    /// Internal simulation context state
    #[derive(Debug)]
    struct Context {
        rng: XorShift128,
        clock_ns: u64,
        config: SimConfig,
        io_op_count: u64,
        io_fail_count: u64,
        partial_write_count: u64,
        fs: SimFs,
    }

    impl Context {
        fn new(config: SimConfig) -> Self {
            let rng = XorShift128::new(config.seed);
            let clock_ns = config.initial_time_ns;
            Self {
                rng,
                clock_ns,
                config,
                io_op_count: 0,
                io_fail_count: 0,
                partial_write_count: 0,
                fs: SimFs::new(),
            }
        }
    }

    thread_local! {
        static CONTEXT: RefCell<Option<Context>> = const { RefCell::new(None) };
    }

    /// Initialize the simulation context for a new test run
    ///
    /// # Panics
    ///
    /// Panics if called from a multi-threaded tokio runtime in simulation mode.
    /// Simulation requires single-threaded execution for determinism.
    pub fn setup(config: SimConfig) {
        // Warn if running in multi-threaded mode
        if let Ok(workers) = std::env::var("TOKIO_WORKER_THREADS") {
            if workers != "1" {
                eprintln!(
                    "[vfs::sim] WARNING: TOKIO_WORKER_THREADS={} but simulation mode \
                     requires single-threaded execution for determinism",
                    workers
                );
            }
        }

        CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = Some(Context::new(config));
        });
    }

    /// Tear down the simulation context
    pub fn teardown() {
        CONTEXT.with(|ctx| {
            *ctx.borrow_mut() = None;
        });
    }

    /// Check if simulation is currently active
    pub fn is_active() -> bool {
        CONTEXT.with(|ctx| ctx.borrow().is_some())
    }

    /// Advance the virtual clock by the given duration
    pub fn advance_time(duration: Duration) {
        CONTEXT.with(|ctx| {
            if let Some(ref mut c) = *ctx.borrow_mut() {
                c.clock_ns = c.clock_ns.saturating_add(duration.as_nanos() as u64);
            }
        });
    }

    /// Dynamically update the I/O error rate without resetting RNG state
    ///
    /// This allows temporarily disabling faults during critical phases
    /// (e.g., recovery) while keeping the same random sequence for
    /// reproducibility.
    pub fn set_io_error_rate(rate: f64) {
        CONTEXT.with(|ctx| {
            if let Some(ref mut c) = *ctx.borrow_mut() {
                c.config.io_error_rate = rate;
            }
        });
    }

    /// Get the current I/O error rate
    ///
    /// Returns the configured error rate, or 0.0 if simulation context is not initialized.
    pub fn get_io_error_rate() -> f64 {
        CONTEXT.with(|ctx| {
            if let Some(ref c) = *ctx.borrow() {
                c.config.io_error_rate
            } else {
                0.0
            }
        })
    }

    /// Enable or disable partial write simulation without resetting RNG state.
    pub fn set_partial_writes_enabled(enabled: bool) {
        CONTEXT.with(|ctx| {
            if let Some(ref mut c) = *ctx.borrow_mut() {
                c.config.enable_partial_writes = enabled;
            }
        });
    }

    /// Get whether partial write simulation is currently enabled.
    pub fn get_partial_writes_enabled() -> bool {
        CONTEXT.with(|ctx| {
            if let Some(ref c) = *ctx.borrow() {
                c.config.enable_partial_writes
            } else {
                false
            }
        })
    }

    /// Advance the virtual clock by a small amount to simulate I/O latency
    /// Default: 1ms per I/O operation
    pub(super) fn tick_time() {
        advance_time(Duration::from_millis(1));
    }

    /// Get the current virtual time
    pub(super) fn current_time() -> SystemTime {
        CONTEXT.with(|ctx| {
            if let Some(ref c) = *ctx.borrow() {
                UNIX_EPOCH + Duration::from_nanos(c.clock_ns)
            } else {
                // Fallback if not initialized (shouldn't happen in proper usage)
                UNIX_EPOCH
            }
        })
    }

    /// Roll the dice to determine if an I/O operation should fail
    ///
    /// Returns `Ok(())` if the operation should proceed, or `Err` with a
    /// simulated I/O error if it should fail.
    pub(super) fn should_fail_io() -> io::Result<()> {
        CONTEXT.with(|ctx| {
            let mut ctx_ref = ctx.borrow_mut();
            if let Some(ref mut c) = *ctx_ref {
                c.io_op_count += 1;
                let roll = c.rng.next_f64();
                if roll < c.config.io_error_rate {
                    c.io_fail_count += 1;
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "simulated I/O failure (op #{}, roll={:.4}, threshold={:.4})",
                            c.io_op_count, roll, c.config.io_error_rate
                        ),
                    ));
                }
            }
            Ok(())
        })
    }

    /// Determine how many bytes of a write should succeed before failure
    /// Returns None if the full write should succeed, Some(n) if only n bytes should be written
    /// Increments partial_write_count when a partial write is triggered.
    pub(super) fn partial_write_amount(requested: usize) -> Option<usize> {
        CONTEXT.with(|ctx| {
            let mut ctx_ref = ctx.borrow_mut();
            if let Some(ref mut c) = *ctx_ref {
                if c.config.enable_partial_writes && requested > 0 {
                    // 10% chance of partial write when enabled
                    if c.rng.next_f64() < 0.1 {
                        c.partial_write_count += 1;
                        return Some(c.rng.next_usize(requested));
                    }
                }
            }
            None
        })
    }

    /// Get statistics about the simulation run
    #[derive(Debug, Clone)]
    pub struct SimStats {
        pub io_op_count: u64,
        pub io_fail_count: u64,
        pub partial_write_count: u64,
        pub current_time_ns: u64,
    }

    pub fn stats() -> Option<SimStats> {
        CONTEXT.with(|ctx| {
            ctx.borrow().as_ref().map(|c| SimStats {
                io_op_count: c.io_op_count,
                io_fail_count: c.io_fail_count,
                partial_write_count: c.partial_write_count,
                current_time_ns: c.clock_ns,
            })
        })
    }

    /// Get the current partial write count
    pub fn get_partial_write_count() -> u64 {
        CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .map(|c| c.partial_write_count)
                .unwrap_or(0)
        })
    }

    /// Reset the partial write count to zero and return the old value
    pub fn reset_partial_write_count() -> u64 {
        CONTEXT.with(|ctx| {
            let mut ctx_ref = ctx.borrow_mut();
            if let Some(ref mut c) = *ctx_ref {
                let old = c.partial_write_count;
                c.partial_write_count = 0;
                old
            } else {
                0
            }
        })
    }

    // ========================================================================
    // Recovery Crash Point Injection
    // ========================================================================

    /// Points during recovery where we can inject a simulated crash
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum RecoveryCrashPoint {
        /// No crash injection
        None,
        /// Crash after scanning file list
        AfterFileList,
        /// Crash after reading a block header
        AfterBlockHeader,
        /// Crash after rebuilding topic chains
        AfterChainRebuild,
        /// Crash after recovering N entries (for partial recovery testing)
        AfterEntryCount(usize),
    }

    thread_local! {
        static RECOVERY_CRASH_POINT: std::cell::Cell<RecoveryCrashPoint> =
            const { std::cell::Cell::new(RecoveryCrashPoint::None) };
        static RECOVERY_ENTRY_COUNT: std::cell::Cell<usize> =
            const { std::cell::Cell::new(0) };
    }

    /// Set the crash point for recovery testing
    pub fn set_recovery_crash_point(point: RecoveryCrashPoint) {
        RECOVERY_CRASH_POINT.with(|p| p.set(point));
        RECOVERY_ENTRY_COUNT.with(|c| c.set(0));
    }

    /// Get the current recovery crash point
    pub fn get_recovery_crash_point() -> RecoveryCrashPoint {
        RECOVERY_CRASH_POINT.with(|p| p.get())
    }

    /// Check if we should crash at the given point. Panics if crash point matches.
    pub fn check_recovery_crash(current: RecoveryCrashPoint) {
        let target = RECOVERY_CRASH_POINT.with(|p| p.get());
        if target == current {
            panic!("SIMULATED CRASH DURING RECOVERY at {:?}", current);
        }
    }

    /// Increment recovery entry count and check if we should crash
    pub fn recovery_entry_recovered() {
        let count = RECOVERY_ENTRY_COUNT.with(|c| {
            let new = c.get() + 1;
            c.set(new);
            new
        });

        let target = RECOVERY_CRASH_POINT.with(|p| p.get());
        if let RecoveryCrashPoint::AfterEntryCount(n) = target {
            if count >= n {
                panic!(
                    "SIMULATED CRASH DURING RECOVERY after {} entries",
                    count
                );
            }
        }
    }

    /// Get direct access to the RNG for test-specific randomness
    pub fn with_rng<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut XorShift128) -> R,
    {
        CONTEXT.with(|ctx| {
            let mut ctx_ref = ctx.borrow_mut();
            ctx_ref.as_mut().map(|c| f(&mut c.rng))
        })
    }

    fn with_fs_mut<F, R>(f: F) -> io::Result<R>
    where
        F: FnOnce(&mut SimFs) -> io::Result<R>,
    {
        CONTEXT.with(|ctx| {
            let mut ctx_ref = ctx.borrow_mut();
            let sim = ctx_ref
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "simulation not active"))?;
            f(&mut sim.fs)
        })
    }

    fn with_fs<F, R>(f: F) -> io::Result<R>
    where
        F: FnOnce(&SimFs) -> io::Result<R>,
    {
        CONTEXT.with(|ctx| {
            let ctx_ref = ctx.borrow();
            let sim = ctx_ref
                .as_ref()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "simulation not active"))?;
            f(&sim.fs)
        })
    }

    pub(crate) fn open_with_options(path: &Path, opts: &SimOpenOptions) -> io::Result<SimFileHandle> {
        with_fs_mut(|fs| {
            let is_dir = fs.dirs.contains(path);
            let exists = fs.files.contains_key(path) || is_dir;
            if opts.create_new && exists {
                return Err(io::Error::new(io::ErrorKind::AlreadyExists, "file exists"));
            }
            if !exists && !opts.create && !opts.create_new {
                return Err(io::Error::new(io::ErrorKind::NotFound, "file not found"));
            }

            if is_dir && (opts.create || opts.create_new || opts.truncate || opts.write || opts.append)
            {
                return Err(io::Error::new(io::ErrorKind::Other, "path is a directory"));
            }

            if opts.create || opts.create_new {
                fs.create_file(path, opts.truncate);
            }

            if opts.truncate && exists {
                fs.create_file(path, true);
            }

            let len = if is_dir {
                0
            } else {
                fs.file_len(path).unwrap_or(0)
            };
            let cursor = if opts.append { len } else { 0 };
            Ok(SimFileHandle {
                path: path.to_path_buf(),
                cursor,
                readable: opts.read,
                writable: opts.write || opts.append,
                append: opts.append,
            })
        })
    }

    pub(crate) fn create_file(path: &Path) -> io::Result<SimFileHandle> {
        let opts = SimOpenOptions {
            write: true,
            truncate: true,
            create: true,
            ..SimOpenOptions::default()
        };
        open_with_options(path, &opts)
    }

    pub(crate) fn open_file(path: &Path) -> io::Result<SimFileHandle> {
        let opts = SimOpenOptions {
            read: true,
            ..SimOpenOptions::default()
        };
        open_with_options(path, &opts)
    }

    pub(crate) fn file_len(path: &Path) -> io::Result<u64> {
        with_fs(|fs| {
            fs.file_len(path)
                .map(|len| len as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
        })
    }

    pub(crate) fn set_len(path: &Path, size: u64) -> io::Result<()> {
        with_fs_mut(|fs| fs.set_len(path, size as usize))
    }

    pub(crate) fn read_at(path: &Path, offset: usize, buf: &mut [u8]) -> io::Result<usize> {
        with_fs(|fs| fs.read_at(path, offset, buf))
    }

    pub(crate) fn write_at(path: &Path, offset: usize, buf: &[u8]) -> io::Result<usize> {
        with_fs_mut(|fs| fs.write_at(path, offset, buf))
    }

    pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
        with_fs_mut(|fs| fs.remove_file(path))
    }

    pub(crate) fn remove_dir(path: &Path) -> io::Result<()> {
        with_fs_mut(|fs| fs.remove_dir(path))
    }

    pub(crate) fn remove_dir_all(path: &Path) -> io::Result<()> {
        with_fs_mut(|fs| fs.remove_dir_all(path))
    }

    pub(crate) fn rename(from: &Path, to: &Path) -> io::Result<()> {
        with_fs_mut(|fs| fs.rename(from, to))
    }

    pub(crate) fn create_dir_all(path: &Path) -> io::Result<()> {
        with_fs_mut(|fs| {
            fs.ensure_dir_all(path);
            Ok(())
        })
    }

    pub(crate) fn read_dir(path: &Path) -> io::Result<Vec<SimDirEntry>> {
        with_fs(|fs| fs.read_dir(path))
    }

    pub(crate) fn exists(path: &Path) -> io::Result<bool> {
        with_fs(|fs| {
            if fs.files.contains_key(path) || fs.dirs.contains(path) {
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }
}

// ============================================================================
// TIME VIRTUALIZATION
// ============================================================================

/// Get the current time (virtualized in simulation mode)
///
/// In default mode, this is equivalent to `std::time::SystemTime::now()`.
/// In simulation mode, this returns the virtual clock time.
#[inline]
pub fn now() -> SystemTime {
    #[cfg(feature = "simulation")]
    {
        if sim::is_active() {
            return sim::current_time();
        }
    }
    SystemTime::now()
}

// ============================================================================
// FILE WRAPPER
// ============================================================================

/// A virtualized file handle that wraps `std::fs::File`
///
/// In default mode, all operations pass through directly to the underlying file.
/// In simulation mode, operations use the in-memory SimFS.
#[derive(Debug)]
pub struct File {
    inner: FileInner,
}

#[derive(Debug)]
enum FileInner {
    Real(std::fs::File),
    #[cfg(feature = "simulation")]
    Sim(sim::SimFileHandle),
}

impl File {
    /// Opens a file in read-only mode
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                let handle = sim::open_file(path.as_ref())?;
                return Ok(Self {
                    inner: FileInner::Sim(handle),
                });
            }
        }
        let inner = std::fs::File::open(path)?;
        Ok(Self {
            inner: FileInner::Real(inner),
        })
    }

    /// Opens a file in write-only mode, creating it if it doesn't exist
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                let handle = sim::create_file(path.as_ref())?;
                return Ok(Self {
                    inner: FileInner::Sim(handle),
                });
            }
        }
        let inner = std::fs::File::create(path)?;
        Ok(Self {
            inner: FileInner::Real(inner),
        })
    }

    /// Attempts to sync all OS-internal metadata to disk
    pub fn sync_all(&self) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                return Ok(());
            }
        }
        match &self.inner {
            FileInner::Real(inner) => inner.sync_all(),
            #[cfg(feature = "simulation")]
            FileInner::Sim(_) => Ok(()),
        }
    }

    /// Attempts to sync file data to disk (not metadata)
    pub fn sync_data(&self) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                return Ok(());
            }
        }
        match &self.inner {
            FileInner::Real(inner) => inner.sync_data(),
            #[cfg(feature = "simulation")]
            FileInner::Sim(_) => Ok(()),
        }
    }

    /// Truncates or extends the underlying file
    pub fn set_len(&self, size: u64) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                if let FileInner::Sim(handle) = &self.inner {
                    return sim::set_len(&handle.path, size);
                }
            }
        }
        match &self.inner {
            FileInner::Real(inner) => inner.set_len(size),
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => sim::set_len(&handle.path, size),
        }
    }

    /// Queries metadata about the underlying file
    pub fn metadata(&self) -> io::Result<std::fs::Metadata> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "metadata not supported in simulation mode",
                ));
            }
        }
        match &self.inner {
            FileInner::Real(inner) => inner.metadata(),
            #[cfg(feature = "simulation")]
            FileInner::Sim(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "metadata not supported in simulation mode",
            )),
        }
    }

    /// Creates a new independently owned handle to the underlying file
    pub fn try_clone(&self) -> io::Result<Self> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
        }
        match &self.inner {
            FileInner::Real(inner) => {
                let inner = inner.try_clone()?;
                Ok(Self {
                    inner: FileInner::Real(inner),
                })
            }
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => Ok(Self {
                inner: FileInner::Sim(handle.clone()),
            }),
        }
    }

    /// Returns the file length in bytes.
    pub fn len(&self) -> io::Result<u64> {
        match &self.inner {
            FileInner::Real(inner) => Ok(inner.metadata()?.len()),
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => sim::file_len(&handle.path),
        }
    }

    /// Get a reference to the underlying std::fs::File
    ///
    /// Use sparingly - this bypasses simulation fault injection
    pub fn inner(&self) -> Option<&std::fs::File> {
        match &self.inner {
            FileInner::Real(inner) => Some(inner),
            #[cfg(feature = "simulation")]
            FileInner::Sim(_) => None,
        }
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        #[cfg(feature = "simulation")]
        {
            sim::tick_time();
            // Note: We don't fail reads by default (most systems handle read errors differently)
            // but we do advance virtual time
        }
        match &mut self.inner {
            FileInner::Real(inner) => inner.read(buf),
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => {
                if !handle.readable {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "read not permitted",
                    ));
                }
                let read = sim::read_at(&handle.path, handle.cursor, buf)?;
                handle.cursor += read;
                Ok(read)
            }
        }
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();

            // Check for partial write simulation
            if let Some(partial_len) = sim::partial_write_amount(buf.len()) {
                if partial_len == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "simulated write interruption (0 bytes written)",
                    ));
                }
                match &mut self.inner {
                    FileInner::Real(inner) => {
                        let written = inner.write(&buf[..partial_len])?;
                        return Ok(written);
                    }
                    #[cfg(feature = "simulation")]
                    FileInner::Sim(handle) => {
                        if !handle.writable {
                            return Err(io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "write not permitted",
                            ));
                        }
                        if handle.append {
                            handle.cursor = sim::file_len(&handle.path)? as usize;
                        }
                        let written = sim::write_at(&handle.path, handle.cursor, &buf[..partial_len])?;
                        handle.cursor += written;
                        return Ok(written);
                    }
                }
            }
        }
        match &mut self.inner {
            FileInner::Real(inner) => inner.write(buf),
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => {
                if !handle.writable {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "write not permitted",
                    ));
                }
                if handle.append {
                    handle.cursor = sim::file_len(&handle.path)? as usize;
                }
                let written = sim::write_at(&handle.path, handle.cursor, buf)?;
                handle.cursor += written;
                Ok(written)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                return Ok(());
            }
        }
        match &mut self.inner {
            FileInner::Real(inner) => inner.flush(),
            #[cfg(feature = "simulation")]
            FileInner::Sim(_) => Ok(()),
        }
    }
}

// Unix-specific extensions for pread/pwrite
#[cfg(unix)]
impl std::os::unix::io::AsRawFd for File {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        use std::os::unix::io::AsRawFd;
        match &self.inner {
            FileInner::Real(inner) => inner.as_raw_fd(),
            #[cfg(feature = "simulation")]
            FileInner::Sim(_) => -1,
        }
    }
}

#[cfg(unix)]
impl std::os::unix::fs::FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(feature = "simulation")]
        {
            sim::tick_time();
        }
        match &self.inner {
            FileInner::Real(inner) => {
                use std::os::unix::fs::FileExt;
                inner.read_at(buf, offset)
            }
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => {
                if !handle.readable {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "read not permitted",
                    ));
                }
                sim::read_at(&handle.path, offset as usize, buf)
            }
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();

            if let Some(partial_len) = sim::partial_write_amount(buf.len()) {
                if partial_len == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "simulated write_at interruption (0 bytes written)",
                    ));
                }
                match &self.inner {
                    FileInner::Real(inner) => {
                        use std::os::unix::fs::FileExt;
                        return inner.write_at(&buf[..partial_len], offset);
                    }
                    #[cfg(feature = "simulation")]
                    FileInner::Sim(handle) => {
                        if !handle.writable {
                            return Err(io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "write not permitted",
                            ));
                        }
                        return sim::write_at(&handle.path, offset as usize, &buf[..partial_len]);
                    }
                }
            }
        }
        match &self.inner {
            FileInner::Real(inner) => {
                use std::os::unix::fs::FileExt;
                inner.write_at(buf, offset)
            }
            #[cfg(feature = "simulation")]
            FileInner::Sim(handle) => {
                if !handle.writable {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "write not permitted",
                    ));
                }
                sim::write_at(&handle.path, offset as usize, buf)
            }
        }
    }
}

// ============================================================================
// OPEN OPTIONS WRAPPER
// ============================================================================

/// A virtualized builder for opening files with various options
#[derive(Debug, Clone)]
pub struct OpenOptions {
    inner: std::fs::OpenOptions,
    #[cfg(feature = "simulation")]
    sim: sim::SimOpenOptions,
}

impl OpenOptions {
    /// Creates a blank new set of options
    pub fn new() -> Self {
        Self {
            inner: std::fs::OpenOptions::new(),
            #[cfg(feature = "simulation")]
            sim: sim::SimOpenOptions::default(),
        }
    }

    /// Sets the option for read access
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.inner.read(read);
        #[cfg(feature = "simulation")]
        {
            self.sim.read = read;
        }
        self
    }

    /// Sets the option for write access
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.inner.write(write);
        #[cfg(feature = "simulation")]
        {
            self.sim.write = write;
        }
        self
    }

    /// Sets the option for append mode
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.inner.append(append);
        #[cfg(feature = "simulation")]
        {
            self.sim.append = append;
        }
        self
    }

    /// Sets the option for truncating a file
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.inner.truncate(truncate);
        #[cfg(feature = "simulation")]
        {
            self.sim.truncate = truncate;
        }
        self
    }

    /// Sets the option for creating a new file
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.inner.create(create);
        #[cfg(feature = "simulation")]
        {
            self.sim.create = create;
        }
        self
    }

    /// Sets the option for creating a new file exclusively
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.inner.create_new(create_new);
        #[cfg(feature = "simulation")]
        {
            self.sim.create_new = create_new;
        }
        self
    }

    /// Opens a file at `path` with the options specified
    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
            if sim::is_active() {
                let handle = sim::open_with_options(path.as_ref(), &self.sim)?;
                return Ok(File {
                    inner: FileInner::Sim(handle),
                });
            }
        }
        let inner = self.inner.open(path)?;
        Ok(File {
            inner: FileInner::Real(inner),
        })
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

// Unix-specific OpenOptions extensions
#[cfg(unix)]
impl OpenOptions {
    /// Sets custom flags for the file open operation (e.g., O_SYNC)
    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        use std::os::unix::fs::OpenOptionsExt;
        self.inner.custom_flags(flags);
        self
    }

    /// Sets the file mode bits for new files
    pub fn mode(&mut self, mode: u32) -> &mut Self {
        use std::os::unix::fs::OpenOptionsExt;
        self.inner.mode(mode);
        self
    }
}

// ============================================================================
// DIRECTORY OPERATIONS
// ============================================================================

pub struct ReadDir {
    inner: ReadDirInner,
}

enum ReadDirInner {
    Real(std::fs::ReadDir),
    #[cfg(feature = "simulation")]
    Sim(Vec<sim::SimDirEntry>, usize),
}

pub struct DirEntry {
    inner: DirEntryInner,
}

enum DirEntryInner {
    Real(std::fs::DirEntry),
    #[cfg(feature = "simulation")]
    Sim(sim::SimDirEntry),
}

impl DirEntry {
    pub fn path(&self) -> std::path::PathBuf {
        match &self.inner {
            DirEntryInner::Real(inner) => inner.path(),
            #[cfg(feature = "simulation")]
            DirEntryInner::Sim(inner) => inner.path(),
        }
    }

    pub fn file_name(&self) -> std::ffi::OsString {
        match &self.inner {
            DirEntryInner::Real(inner) => inner.file_name(),
            #[cfg(feature = "simulation")]
            DirEntryInner::Sim(inner) => inner.file_name(),
        }
    }

    pub fn is_dir(&self) -> io::Result<bool> {
        match &self.inner {
            DirEntryInner::Real(inner) => inner.file_type().map(|ft| ft.is_dir()),
            #[cfg(feature = "simulation")]
            DirEntryInner::Sim(inner) => Ok(inner.is_dir()),
        }
    }
}

impl Iterator for ReadDir {
    type Item = io::Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            ReadDirInner::Real(inner) => inner
                .next()
                .map(|res| res.map(|entry| DirEntry {
                    inner: DirEntryInner::Real(entry),
                })),
            #[cfg(feature = "simulation")]
            ReadDirInner::Sim(entries, idx) => {
                if *idx >= entries.len() {
                    return None;
                }
                let entry = entries[*idx].clone();
                *idx += 1;
                Some(Ok(DirEntry {
                    inner: DirEntryInner::Sim(entry),
                }))
            }
        }
    }
}

/// Creates a directory and all of its parent components if missing
pub fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            return sim::create_dir_all(path.as_ref());
        }
    }
    std::fs::create_dir_all(path)
}

/// Removes a file from the filesystem
pub fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            return sim::remove_file(path.as_ref());
        }
    }
    std::fs::remove_file(path)
}

/// Removes an empty directory
pub fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            return sim::remove_dir(path.as_ref());
        }
    }
    std::fs::remove_dir(path)
}

/// Removes a directory and all its contents
pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            return sim::remove_dir_all(path.as_ref());
        }
    }
    std::fs::remove_dir_all(path)
}

/// Returns an iterator over the entries within a directory
pub fn read_dir<P: AsRef<Path>>(path: P) -> io::Result<ReadDir> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
            sim::tick_time();
        if sim::is_active() {
            let entries = sim::read_dir(path.as_ref())?;
            return Ok(ReadDir {
                inner: ReadDirInner::Sim(entries, 0),
            });
        }
    }
    std::fs::read_dir(path).map(|inner| ReadDir {
        inner: ReadDirInner::Real(inner),
    })
}

/// Renames a file or directory
pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            return sim::rename(from.as_ref(), to.as_ref());
        }
    }
    std::fs::rename(from, to)
}

/// Read the entire contents of a file into a bytes vector
pub fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            let mut buf = Vec::new();
            let handle = sim::open_file(path.as_ref())?;
            let len = sim::file_len(&handle.path)? as usize;
            buf.resize(len, 0);
            let _ = sim::read_at(&handle.path, 0, &mut buf)?;
            return Ok(buf);
        }
    }
    std::fs::read(path)
}

/// Write a slice as the entire contents of a file
pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            let handle = sim::create_file(path.as_ref())?;
            let data = contents.as_ref();
            sim::set_len(&handle.path, 0)?;
            let _ = sim::write_at(&handle.path, 0, data)?;
            return Ok(());
        }
    }
    std::fs::write(path, contents)
}

// ============================================================================
// PATH HELPERS
// ============================================================================

/// Returns `true` if the path exists on disk
pub fn exists<P: AsRef<Path>>(path: P) -> bool {
    #[cfg(feature = "simulation")]
    {
        sim::tick_time();
        // Note: exists() doesn't typically fail, just returns false if inaccessible
        if sim::is_active() {
            return sim::exists(path.as_ref()).unwrap_or(false);
        }
    }
    path.as_ref().exists()
}

/// Given a reference to a Path, return the file's metadata
pub fn metadata<P: AsRef<Path>>(path: P) -> io::Result<std::fs::Metadata> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
        if sim::is_active() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "metadata not supported in simulation mode",
            ));
        }
    }
    std::fs::metadata(path)
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_returns_valid_time() {
        let time = now();
        assert!(time > UNIX_EPOCH);
    }

    #[test]
    fn test_file_create_and_write() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("vfs_test_{}.tmp", std::process::id()));

        // Create and write
        {
            let mut file = File::create(&path).expect("create failed");
            file.write_all(b"hello vfs").expect("write failed");
            file.sync_all().expect("sync failed");
        }

        // Read back
        {
            let mut file = File::open(&path).expect("open failed");
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).expect("read failed");
            assert_eq!(buf, b"hello vfs");
        }

        // Cleanup
        remove_file(&path).expect("remove failed");
    }

    #[test]
    fn test_open_options() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("vfs_opts_test_{}.tmp", std::process::id()));

        // Create with OpenOptions
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .expect("open failed");

            file.write_all(b"test data").expect("write failed");
        }

        // Verify
        assert!(exists(&path));

        // Cleanup
        remove_file(&path).expect("remove failed");
    }

    #[test]
    fn test_directory_operations() {
        let dir = std::env::temp_dir();
        let test_dir = dir.join(format!("vfs_dir_test_{}", std::process::id()));

        // Create nested dirs
        let nested = test_dir.join("a").join("b").join("c");
        create_dir_all(&nested).expect("create_dir_all failed");
        assert!(exists(&nested));

        // Cleanup
        remove_dir_all(&test_dir).expect("remove_dir_all failed");
        assert!(!exists(&test_dir));
    }

    #[cfg(unix)]
    #[test]
    fn test_file_ext_pwrite_pread() {
        use std::os::unix::fs::FileExt;

        let dir = std::env::temp_dir();
        let path = dir.join(format!("vfs_pwrite_test_{}.tmp", std::process::id()));

        // Must open with read+write for pread to work
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .expect("open failed");
        file.set_len(100).expect("set_len failed");

        // Write at offset
        let data = b"hello at offset 50";
        file.write_at(data, 50).expect("write_at failed");

        // Read back
        let mut buf = vec![0u8; data.len()];
        file.read_at(&mut buf, 50).expect("read_at failed");
        assert_eq!(&buf, data);

        drop(file);
        remove_file(&path).expect("remove failed");
    }

    // ===== Simulation-specific tests =====

    #[cfg(feature = "simulation")]
    mod sim_tests {
        use super::*;

        #[test]
        fn test_xorshift_reproducibility() {
            let mut rng1 = sim::XorShift128::new(42);
            let mut rng2 = sim::XorShift128::new(42);

            for _ in 0..1000 {
                assert_eq!(rng1.next_u64(), rng2.next_u64());
            }
        }

        #[test]
        fn test_xorshift_distribution() {
            let mut rng = sim::XorShift128::new(12345);
            let mut sum = 0.0;

            for _ in 0..10000 {
                let val = rng.next_f64();
                assert!(val >= 0.0 && val < 1.0);
                sum += val;
            }

            // Mean should be close to 0.5
            let mean = sum / 10000.0;
            assert!((mean - 0.5).abs() < 0.05);
        }

        #[test]
        fn test_sim_context_setup_teardown() {
            assert!(!sim::is_active());

            sim::setup(sim::SimConfig::default());
            assert!(sim::is_active());

            sim::teardown();
            assert!(!sim::is_active());
        }

        #[test]
        fn test_virtual_time() {
            sim::setup(sim::SimConfig {
                seed: 42,
                io_error_rate: 0.0,
                initial_time_ns: 1_000_000_000_000_000_000, // 1e18 ns
                enable_partial_writes: false,
            });

            let time1 = now();
            sim::advance_time(Duration::from_secs(10));
            let time2 = now();

            let diff = time2.duration_since(time1).unwrap();
            assert_eq!(diff.as_secs(), 10);

            sim::teardown();
        }

        #[test]
        fn test_io_failure_injection() {
            sim::setup(sim::SimConfig {
                seed: 42,
                io_error_rate: 1.0, // 100% failure rate
                initial_time_ns: 0,
                enable_partial_writes: false,
            });

            let dir = std::env::temp_dir();
            let path = dir.join("vfs_sim_fail_test.tmp");

            // This should fail due to 100% error rate
            let result = File::create(&path);
            assert!(result.is_err());

            sim::teardown();
        }

        #[test]
        fn test_io_success_with_zero_error_rate() {
            sim::setup(sim::SimConfig {
                seed: 42,
                io_error_rate: 0.0, // 0% failure rate
                initial_time_ns: 0,
                enable_partial_writes: false,
            });

            let dir = std::env::temp_dir();
            let path = dir.join(format!("vfs_sim_success_{}.tmp", std::process::id()));

            // This should succeed
            {
                let mut file = File::create(&path).expect("create should succeed");
                file.write_all(b"test").expect("write should succeed");
            }

            remove_file(&path).expect("cleanup");
            sim::teardown();
        }

        #[test]
        fn test_stats_tracking() {
            sim::setup(sim::SimConfig {
                seed: 42,
                io_error_rate: 0.0,
                initial_time_ns: 0,
                enable_partial_writes: false,
            });

            let dir = std::env::temp_dir();
            let path = dir.join(format!("vfs_sim_stats_{}.tmp", std::process::id()));

            // Perform some I/O
            {
                let file = File::create(&path).expect("create");
                file.sync_all().expect("sync");
            }

            let stats = sim::stats().expect("stats should be available");
            assert!(stats.io_op_count >= 2); // At least create + sync
            assert_eq!(stats.io_fail_count, 0);

            remove_file(&path).ok();
            sim::teardown();
        }
    }
}
