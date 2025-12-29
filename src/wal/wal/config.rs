use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::wal::wal::vfs::now;

// Global flag to choose backend
pub(crate) static USE_FD_BACKEND: AtomicBool = AtomicBool::new(true);

// Public function to enable FD backend
pub fn enable_fd_backend() {
    USE_FD_BACKEND.store(true, Ordering::Relaxed);
}

// Public function to disable FD backend (use mmap instead)
pub fn disable_fd_backend() {
    USE_FD_BACKEND.store(false, Ordering::Relaxed);
}

// Helper to check backend status, strictly enforcing FD backend in simulation
pub(crate) fn is_fd_backend_enabled() -> bool {
    #[cfg(feature = "simulation")]
    {
        // Memory mapped I/O cannot be fault-injected easily, so we force
        // the FD backend which uses pwrite/pread through our VFS layer.
        return true;
    }
    #[cfg(not(feature = "simulation"))]
    {
        USE_FD_BACKEND.load(Ordering::Relaxed)
    }
}

pub(crate) fn is_io_uring_enabled() -> bool {
    #[cfg(feature = "simulation")]
    {
        // io_uring bypasses the VFS simulation layer, breaking fault injection.
        return false;
    }
    #[cfg(not(feature = "simulation"))]
    {
        is_fd_backend_enabled()
    }
}

// Macro to conditionally print debug messages
macro_rules! debug_print {
    ($($arg:tt)*) => {
        if std::env::var("WALRUS_QUIET").is_err() {
            println!($($arg)*);
        }
    };
}

pub(crate) use debug_print;

#[derive(Clone, Copy, Debug)]
pub enum FsyncSchedule {
    Milliseconds(u64),
    SyncEach, // fsync after every single entry
    NoFsync,  // disable fsyncing entirely (maximum throughput, no durability)
}

pub(crate) const DEFAULT_BLOCK_SIZE: u64 = 10 * 1024 * 1024; // 10mb
pub(crate) const BLOCKS_PER_FILE: u64 = 100;
pub(crate) const MAX_ALLOC: u64 = 1 * 1024 * 1024 * 1024; // 1 GiB cap per block
pub(crate) const PREFIX_META_SIZE: usize = 64;
pub(crate) const MAX_FILE_SIZE: u64 = DEFAULT_BLOCK_SIZE * BLOCKS_PER_FILE;
pub(crate) const MAX_BATCH_ENTRIES: usize = 2000;
pub(crate) const MAX_BATCH_BYTES: u64 = 10 * 1024 * 1024 * 1024; // 10 GiB total payload limit

static LAST_MILLIS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn now_millis_str() -> String {
    // Use vfs::now() for deterministic time in simulation mode
    let system_ms = now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_millis();

    let mut observed = LAST_MILLIS.load(Ordering::Relaxed);
    loop {
        let system_ms_u64 = system_ms.try_into().unwrap_or(u64::MAX);
        let candidate = if system_ms_u64 <= observed {
            observed.saturating_add(1)
        } else {
            system_ms_u64
        };

        match LAST_MILLIS.compare_exchange(observed, candidate, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return candidate.to_string(),
            Err(actual) => observed = actual,
        }
    }
}

pub(crate) fn checksum64(data: &[u8]) -> u64 {
    // FNV-1a 64-bit checksum
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001B3;
    let mut hash = FNV_OFFSET;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(crate) fn wal_data_dir() -> PathBuf {
    std::env::var_os("WALRUS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("_octopii_wal_files"))
}

pub(crate) fn sanitize_namespace(key: &str) -> String {
    let mut sanitized: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.trim_matches('_').is_empty() {
        sanitized = format!("ns_{:x}", checksum64(key.as_bytes()));
    }
    sanitized
}
