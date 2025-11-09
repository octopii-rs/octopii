// Test utilities ported from TiKV
// Source: tikv/components/test_util/src/lib.rs
// Licensed under Apache-2.0

use rand::Rng;
use std::{
    env,
    fmt::Debug,
    sync::atomic::{AtomicU16, Ordering},
    time::Duration,
};

static INITIAL_PORT: AtomicU16 = AtomicU16::new(0);
/// Linux by default uses [32768, 61000] for local port.
const MIN_LOCAL_PORT: u16 = 32767;

/// Allocates a port for testing purpose.
///
/// This ensures each test gets a unique port, avoiding conflicts.
/// Ported from TiKV test_util.
pub fn alloc_port() -> u16 {
    let p = INITIAL_PORT.load(Ordering::Relaxed);
    if p == 0 {
        let _ = INITIAL_PORT.compare_exchange(
            0,
            rand::thread_rng().gen_range(10240..MIN_LOCAL_PORT),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
    let mut p = INITIAL_PORT.load(Ordering::SeqCst);
    loop {
        let next = if p >= MIN_LOCAL_PORT { 10240 } else { p + 1 };
        match INITIAL_PORT.compare_exchange_weak(p, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return next,
            Err(e) => p = e,
        }
    }
}

static MEM_DISK: &str = "TIKV_TEST_MEMORY_DISK_MOUNT_POINT";

/// Gets a temporary path. The directory will be removed when dropped.
///
/// The returned path will point to memory only when memory disk is available
/// and specified.
/// Ported from TiKV test_util.
pub fn temp_dir(prefix: impl Into<Option<&'static str>>, prefer_mem: bool) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    if let Some(prefix) = prefix.into() {
        builder.prefix(prefix);
    }
    match env::var(MEM_DISK) {
        Ok(dir) if prefer_mem => builder.tempdir_in(dir).unwrap(),
        _ => builder.tempdir().unwrap(),
    }
}

/// Compare two structs and provide more helpful debug difference.
///
/// This is better than assert_eq! because it shows exactly where the
/// difference is in the debug output.
/// Ported from TiKV test_util.
#[track_caller]
pub fn assert_eq_debug<C: PartialEq + Debug>(lhs: &C, rhs: &C) {
    if lhs == rhs {
        return;
    }
    let lhs_str = format!("{:?}", lhs);
    let rhs_str = format!("{:?}", rhs);

    fn find_index(l: impl Iterator<Item = (u8, u8)>) -> usize {
        let it = l
            .enumerate()
            .take_while(|(_, (l, r))| l == r)
            .filter(|(_, (l, _))| *l == b' ');
        let mut last = None;
        let mut second = None;
        for a in it {
            second = last;
            last = Some(a);
        }
        second.map_or(0, |(i, _)| i)
    }
    let cpl = find_index(lhs_str.bytes().zip(rhs_str.bytes()));
    let csl = find_index(lhs_str.bytes().rev().zip(rhs_str.bytes().rev()));
    if cpl + csl > lhs_str.len() || cpl + csl > rhs_str.len() {
        assert_eq!(lhs, rhs);
    }
    let lhs_diff = String::from_utf8_lossy(&lhs_str.as_bytes()[cpl..lhs_str.len() - csl]);
    let rhs_diff = String::from_utf8_lossy(&rhs_str.as_bytes()[cpl..rhs_str.len() - csl]);
    panic!(
        "config not matched:\nlhs: ...{}...,\nrhs: ...{}...",
        lhs_diff, rhs_diff
    );
}

/// Eventually check a condition with retries.
///
/// Polls `check` every `tick` duration until it returns true or `total` time elapses.
/// Panics if the condition doesn't become true within the timeout.
/// Ported from TiKV test_util.
///
/// # Example
/// ```
/// eventually(
///     Duration::from_millis(100),
///     Duration::from_secs(5),
///     || cluster.has_leader()
/// );
/// ```
#[track_caller]
pub fn eventually(tick: Duration, total: Duration, mut check: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    loop {
        if check() {
            return;
        }
        if start.elapsed() < total {
            std::thread::sleep(tick);
            continue;
        }
        panic!(
            "failed to pass the check after {:?} elapsed",
            start.elapsed()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_port() {
        let port1 = alloc_port();
        let port2 = alloc_port();
        assert_ne!(port1, port2, "Ports should be unique");
        assert!(port1 >= 10240 && port1 < MIN_LOCAL_PORT);
        assert!(port2 >= 10240 && port2 < MIN_LOCAL_PORT);
    }

    #[test]
    fn test_temp_dir() {
        let dir = temp_dir(Some("test_prefix"), false);
        assert!(dir.path().exists());
        let path = dir.path().to_path_buf();
        drop(dir);
        // Directory should be cleaned up
        assert!(!path.exists());
    }

    #[test]
    fn test_eventually_success() {
        let mut counter = 0;
        eventually(Duration::from_millis(10), Duration::from_secs(1), || {
            counter += 1;
            counter >= 5
        });
        assert!(counter >= 5);
    }

    #[test]
    #[should_panic(expected = "failed to pass the check")]
    fn test_eventually_timeout() {
        eventually(Duration::from_millis(10), Duration::from_millis(50), || {
            false
        });
    }

    #[test]
    fn test_assert_eq_debug_equal() {
        let a = vec![1, 2, 3];
        let b = vec![1, 2, 3];
        assert_eq_debug(&a, &b); // Should not panic
    }

    #[test]
    #[should_panic(expected = "config not matched")]
    fn test_assert_eq_debug_different() {
        let a = vec![1, 2, 3];
        let b = vec![1, 2, 4];
        assert_eq_debug(&a, &b);
    }
}
