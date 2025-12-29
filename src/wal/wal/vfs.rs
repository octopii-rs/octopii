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
    pub(super) fn partial_write_amount(requested: usize) -> Option<usize> {
        CONTEXT.with(|ctx| {
            let mut ctx_ref = ctx.borrow_mut();
            if let Some(ref mut c) = *ctx_ref {
                if c.config.enable_partial_writes && requested > 0 {
                    // 10% chance of partial write when enabled
                    if c.rng.next_f64() < 0.1 {
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
        pub current_time_ns: u64,
    }

    pub fn stats() -> Option<SimStats> {
        CONTEXT.with(|ctx| {
            ctx.borrow().as_ref().map(|c| SimStats {
                io_op_count: c.io_op_count,
                io_fail_count: c.io_fail_count,
                current_time_ns: c.clock_ns,
            })
        })
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
/// In simulation mode, operations may fail based on the configured error rate.
#[derive(Debug)]
pub struct File {
    inner: std::fs::File,
}

impl File {
    /// Opens a file in read-only mode
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        let inner = std::fs::File::open(path)?;
        Ok(Self { inner })
    }

    /// Opens a file in write-only mode, creating it if it doesn't exist
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        let inner = std::fs::File::create(path)?;
        Ok(Self { inner })
    }

    /// Attempts to sync all OS-internal metadata to disk
    pub fn sync_all(&self) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        self.inner.sync_all()
    }

    /// Attempts to sync file data to disk (not metadata)
    pub fn sync_data(&self) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        self.inner.sync_data()
    }

    /// Truncates or extends the underlying file
    pub fn set_len(&self, size: u64) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        self.inner.set_len(size)
    }

    /// Queries metadata about the underlying file
    pub fn metadata(&self) -> io::Result<std::fs::Metadata> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        self.inner.metadata()
    }

    /// Creates a new independently owned handle to the underlying file
    pub fn try_clone(&self) -> io::Result<Self> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
        }
        let inner = self.inner.try_clone()?;
        Ok(Self { inner })
    }

    /// Get a reference to the underlying std::fs::File
    ///
    /// Use sparingly - this bypasses simulation fault injection
    pub fn inner(&self) -> &std::fs::File {
        &self.inner
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
        self.inner.read(buf)
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
                let written = self.inner.write(&buf[..partial_len])?;
                return Ok(written);
            }
        }
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        self.inner.flush()
    }
}

// Unix-specific extensions for pread/pwrite
#[cfg(unix)]
impl std::os::unix::io::AsRawFd for File {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        use std::os::unix::io::AsRawFd;
        self.inner.as_raw_fd()
    }
}

#[cfg(unix)]
impl std::os::unix::fs::FileExt for File {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        #[cfg(feature = "simulation")]
        {
            sim::tick_time();
        }
        use std::os::unix::fs::FileExt;
        self.inner.read_at(buf, offset)
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
                use std::os::unix::fs::FileExt;
                return self.inner.write_at(&buf[..partial_len], offset);
            }
        }
        use std::os::unix::fs::FileExt;
        self.inner.write_at(buf, offset)
    }
}

// ============================================================================
// OPEN OPTIONS WRAPPER
// ============================================================================

/// A virtualized builder for opening files with various options
#[derive(Debug, Clone)]
pub struct OpenOptions {
    inner: std::fs::OpenOptions,
}

impl OpenOptions {
    /// Creates a blank new set of options
    pub fn new() -> Self {
        Self {
            inner: std::fs::OpenOptions::new(),
        }
    }

    /// Sets the option for read access
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.inner.read(read);
        self
    }

    /// Sets the option for write access
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.inner.write(write);
        self
    }

    /// Sets the option for append mode
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.inner.append(append);
        self
    }

    /// Sets the option for truncating a file
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.inner.truncate(truncate);
        self
    }

    /// Sets the option for creating a new file
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.inner.create(create);
        self
    }

    /// Sets the option for creating a new file exclusively
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.inner.create_new(create_new);
        self
    }

    /// Opens a file at `path` with the options specified
    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        #[cfg(feature = "simulation")]
        {
            sim::should_fail_io()?;
            sim::tick_time();
        }
        let inner = self.inner.open(path)?;
        Ok(File { inner })
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

/// Creates a directory and all of its parent components if missing
pub fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::create_dir_all(path)
}

/// Removes a file from the filesystem
pub fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::remove_file(path)
}

/// Removes an empty directory
pub fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::remove_dir(path)
}

/// Removes a directory and all its contents
pub fn remove_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::remove_dir_all(path)
}

/// Returns an iterator over the entries within a directory
pub fn read_dir<P: AsRef<Path>>(path: P) -> io::Result<std::fs::ReadDir> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::read_dir(path)
}

/// Renames a file or directory
pub fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::rename(from, to)
}

/// Read the entire contents of a file into a bytes vector
pub fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
    }
    std::fs::read(path)
}

/// Write a slice as the entire contents of a file
pub fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
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
    }
    path.as_ref().exists()
}

/// Given a reference to a Path, return the file's metadata
pub fn metadata<P: AsRef<Path>>(path: P) -> io::Result<std::fs::Metadata> {
    #[cfg(feature = "simulation")]
    {
        sim::should_fail_io()?;
        sim::tick_time();
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
