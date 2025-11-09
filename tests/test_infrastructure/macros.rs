// Test macros ported from TiKV
// Source: tikv/components/test_util/src/macros.rs
// Licensed under Apache-2.0

/// Retry an expression multiple times with delays between attempts.
///
/// This macro is useful for testing eventually-consistent operations.
/// It will retry the expression up to `count` times, sleeping `interval`
/// milliseconds between each attempt.
///
/// # Examples
///
/// ```
/// // Retry with defaults (10 times, 100ms intervals)
/// retry!(cluster.has_leader());
///
/// // Retry 20 times with 100ms intervals
/// retry!(cluster.has_leader(), 20);
///
/// // Retry 20 times with 50ms intervals
/// retry!(cluster.has_leader(), 20, 50);
/// ```
///
/// The expression should return a Result type. The macro will return Ok
/// on the first success, or the last Err if all attempts fail.
#[macro_export]
macro_rules! retry {
    ($expr:expr) => {
        retry!($expr, 10)
    };
    ($expr:expr, $count:expr) => {
        retry!($expr, $count, 100)
    };
    ($expr:expr, $count:expr, $interval:expr) => {{
        use std::thread;
        use std::time::Duration;
        let mut res = $expr;
        if !res.is_ok() {
            for _ in 0..$count {
                thread::sleep(Duration::from_millis($interval));
                res = $expr;
                if res.is_ok() {
                    break;
                }
            }
        }
        res
    }};
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_retry_success_first_time() {
        let result = retry!(Ok::<_, String>(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_retry_success_after_retries() {
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = counter.clone();

        let result = retry!(
            {
                let mut count = counter_clone.lock().unwrap();
                *count += 1;
                if *count >= 3 {
                    Ok::<_, String>(42)
                } else {
                    Err("not yet".to_string())
                }
            },
            10,
            10
        );

        assert_eq!(result.unwrap(), 42);
        assert!(*counter.lock().unwrap() >= 3);
    }

    #[test]
    fn test_retry_failure_after_all_attempts() {
        let result = retry!(Err::<i32, _>("always fails"), 3, 10);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "always fails");
    }

    #[test]
    fn test_retry_with_default_params() {
        let result = retry!(Ok::<_, String>(100));
        assert_eq!(result.unwrap(), 100);
    }

    #[test]
    fn test_retry_with_count_only() {
        let result = retry!(Ok::<_, String>(200), 5);
        assert_eq!(result.unwrap(), 200);
    }
}
