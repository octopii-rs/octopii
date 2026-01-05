//! ClusterOracle - Simple KV state tracking for cluster simulation tests
//!
//! Tracks expected state after commits and verifies reads match expectations.
//! This is a simpler oracle than the full linearizability checker - it just
//! tracks the expected KV state after all committed operations.

use std::collections::HashMap;

/// A failed operation record for debugging
#[derive(Debug, Clone)]
pub struct FailedOp {
    pub key: String,
    pub value: String,
    pub error: String,
    pub tick: u64,
}

/// Verification failure details
#[derive(Debug)]
pub struct VerificationFailure {
    pub key: String,
    pub expected: Option<String>,
    pub actual: Option<String>,
    pub message: String,
}

/// Simple KV oracle that tracks expected state after commits
#[derive(Debug)]
pub struct ClusterOracle {
    /// Expected KV state after all commits
    expected_state: HashMap<String, String>,
    /// Committed operation count
    commit_count: u64,
    /// Verified read count
    verify_count: u64,
    /// Failed operations (for debugging)
    failed_ops: Vec<FailedOp>,
    /// Current tick for timing info
    current_tick: u64,
    /// Keys written without partial writes (must survive crashes)
    must_survive: HashMap<String, String>,
    /// Keys written during partial writes (may be lost after crash)
    may_be_lost: HashMap<String, String>,
    /// Recovery cycle counter for durability checks
    recovery_cycle: u64,
}

impl Default for ClusterOracle {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterOracle {
    /// Create a new empty oracle
    pub fn new() -> Self {
        Self {
            expected_state: HashMap::new(),
            commit_count: 0,
            verify_count: 0,
            failed_ops: Vec::new(),
            current_tick: 0,
            must_survive: HashMap::new(),
            may_be_lost: HashMap::new(),
            recovery_cycle: 0,
        }
    }

    /// Update the current tick (for timing info in failures)
    pub fn set_tick(&mut self, tick: u64) {
        self.current_tick = tick;
    }

    /// Record a successful commit - updates expected state
    pub fn record_commit(&mut self, key: &str, value: &str) {
        self.expected_state.insert(key.to_string(), value.to_string());
        self.commit_count += 1;
    }

    /// Record a must-survive commit (written without partial writes)
    pub fn record_must_survive(&mut self, key: &str, value: &str) {
        self.expected_state.insert(key.to_string(), value.to_string());
        self.must_survive.insert(key.to_string(), value.to_string());
        self.commit_count += 1;
    }

    /// Record a commit that may be lost after crash (partial write observed)
    pub fn record_may_be_lost(&mut self, key: &str, value: &str) {
        self.expected_state.insert(key.to_string(), value.to_string());
        self.may_be_lost.insert(key.to_string(), value.to_string());
        self.commit_count += 1;
    }

    /// Record a delete operation
    pub fn record_delete(&mut self, key: &str) {
        self.expected_state.remove(key);
        self.commit_count += 1;
    }

    /// Record a failed operation (for debugging)
    pub fn record_failure(&mut self, key: &str, value: &str, error: &str) {
        self.failed_ops.push(FailedOp {
            key: key.to_string(),
            value: value.to_string(),
            error: error.to_string(),
            tick: self.current_tick,
        });
    }

    /// Get the expected value for a key
    pub fn expected_value(&self, key: &str) -> Option<&String> {
        self.expected_state.get(key)
    }

    /// Snapshot expected state for full verification
    pub fn expected_state_snapshot(&self) -> Vec<(String, String)> {
        self.expected_state
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Verify a read matches expected state
    /// Returns Ok if the value matches, Err with details if not
    pub fn verify_read(&mut self, key: &str, actual: Option<&str>) -> Result<(), VerificationFailure> {
        let expected = self.expected_state.get(key);

        let matches = match (expected, actual) {
            (None, None) => true,
            (Some(e), Some(a)) => e == a,
            _ => false,
        };

        if matches {
            self.verify_count += 1;
            Ok(())
        } else {
            Err(VerificationFailure {
                key: key.to_string(),
                expected: expected.cloned(),
                actual: actual.map(|s| s.to_string()),
                message: format!(
                    "Value mismatch for key '{}': expected {:?}, got {:?}",
                    key, expected, actual
                ),
            })
        }
    }

    /// Verify a read and panic on failure (convenience method for tests)
    pub fn assert_read(&mut self, key: &str, actual: Option<&str>) {
        if let Err(failure) = self.verify_read(key, actual) {
            self.dump_diagnostics();
            panic!(
                "ORACLE VERIFICATION FAILED at tick {}:\n  Key: '{}'\n  Expected: {:?}\n  Actual: {:?}\n  {}",
                self.current_tick, failure.key, failure.expected, failure.actual, failure.message
            );
        }
    }

    /// Verify a read and panic on failure, with node context
    pub fn assert_read_with_node(&mut self, node_id: u64, key: &str, actual: Option<&str>) {
        if let Err(failure) = self.verify_read(key, actual) {
            self.dump_diagnostics();
            panic!(
                "ORACLE VERIFICATION FAILED at tick {} (node {}):\n  Key: '{}'\n  Expected: {:?}\n  Actual: {:?}\n  {}",
                self.current_tick,
                node_id,
                failure.key,
                failure.expected,
                failure.actual,
                failure.message
            );
        }
    }

    /// Get statistics (committed, failed, verified)
    pub fn stats(&self) -> (u64, usize, u64) {
        (self.commit_count, self.failed_ops.len(), self.verify_count)
    }

    /// Get all failed operations
    pub fn failed_ops(&self) -> &[FailedOp] {
        &self.failed_ops
    }

    /// Get the number of keys in expected state
    pub fn key_count(&self) -> usize {
        self.expected_state.len()
    }

    /// Reset the oracle for a new test run
    pub fn reset(&mut self) {
        self.expected_state.clear();
        self.commit_count = 0;
        self.verify_count = 0;
        self.failed_ops.clear();
        self.current_tick = 0;
        self.must_survive.clear();
        self.may_be_lost.clear();
        self.recovery_cycle = 0;
    }

    /// Dump diagnostic information (useful when debugging failures)
    pub fn dump_diagnostics(&self) {
        eprintln!("\n=== CLUSTER ORACLE DIAGNOSTICS ===");
        eprintln!("Current tick: {}", self.current_tick);
        eprintln!("Committed ops: {}", self.commit_count);
        eprintln!("Verified reads: {}", self.verify_count);
        eprintln!("Failed ops: {}", self.failed_ops.len());
        eprintln!("Must-survive keys: {}", self.must_survive.len());
        eprintln!("May-be-lost keys: {}", self.may_be_lost.len());
        eprintln!("Recovery cycle: {}", self.recovery_cycle);
        eprintln!("Keys in state: {}", self.expected_state.len());

        if !self.failed_ops.is_empty() {
            eprintln!("\nRecent failures (last 10):");
            for (i, op) in self.failed_ops.iter().rev().take(10).enumerate() {
                eprintln!(
                    "  [{}] tick={}: SET {} = {} -> error: {}",
                    i, op.tick, op.key, op.value, op.error
                );
            }
        }

        if self.expected_state.len() <= 20 {
            eprintln!("\nExpected state:");
            for (k, v) in &self.expected_state {
                eprintln!("  {} = {}", k, v);
            }
        } else {
            eprintln!("\nExpected state (first 20 keys):");
            for (k, v) in self.expected_state.iter().take(20) {
                eprintln!("  {} = {}", k, v);
            }
            eprintln!("  ... and {} more keys", self.expected_state.len() - 20);
        }
        eprintln!("==================================\n");
    }

    /// Snapshot must-survive keys for post-recovery verification
    pub fn must_survive_snapshot(&self) -> Vec<(String, String)> {
        self.must_survive
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Snapshot may-be-lost keys for post-recovery promotion
    pub fn may_be_lost_snapshot(&self) -> Vec<(String, String)> {
        self.may_be_lost
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Promote a may-be-lost key to must-survive if the expected value is present
    pub fn promote_may_be_lost(&mut self, key: &str, value: &str) {
        if self.may_be_lost.get(key).map(String::as_str) != Some(value) {
            return;
        }
        self.may_be_lost.remove(key);
        self.must_survive.insert(key.to_string(), value.to_string());
    }

    /// Verify all keys in expected state exist on a node
    /// Returns list of mismatches
    pub fn verify_all_keys<F>(&mut self, read_fn: F) -> Vec<VerificationFailure>
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut failures = Vec::new();

        for (key, expected) in &self.expected_state {
            let actual = read_fn(key);
            if actual.as_ref() != Some(expected) {
                failures.push(VerificationFailure {
                    key: key.clone(),
                    expected: Some(expected.clone()),
                    actual,
                    message: format!("Key '{}' mismatch during full verification", key),
                });
            } else {
                self.verify_count += 1;
            }
        }

        failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_commit_and_verify() {
        let mut oracle = ClusterOracle::new();

        oracle.record_commit("key1", "value1");
        oracle.record_commit("key2", "value2");

        assert!(oracle.verify_read("key1", Some("value1")).is_ok());
        assert!(oracle.verify_read("key2", Some("value2")).is_ok());
        assert!(oracle.verify_read("key3", None).is_ok());

        let (commits, failures, verifies) = oracle.stats();
        assert_eq!(commits, 2);
        assert_eq!(failures, 0);
        assert_eq!(verifies, 3);
    }

    #[test]
    fn test_verification_failure() {
        let mut oracle = ClusterOracle::new();

        oracle.record_commit("key1", "value1");

        let result = oracle.verify_read("key1", Some("wrong_value"));
        assert!(result.is_err());

        let failure = result.unwrap_err();
        assert_eq!(failure.key, "key1");
        assert_eq!(failure.expected, Some("value1".to_string()));
        assert_eq!(failure.actual, Some("wrong_value".to_string()));
    }

    #[test]
    fn test_delete() {
        let mut oracle = ClusterOracle::new();

        oracle.record_commit("key1", "value1");
        assert!(oracle.verify_read("key1", Some("value1")).is_ok());

        oracle.record_delete("key1");
        assert!(oracle.verify_read("key1", None).is_ok());
    }

    #[test]
    fn test_overwrite() {
        let mut oracle = ClusterOracle::new();

        oracle.record_commit("key1", "value1");
        oracle.record_commit("key1", "value2");

        assert!(oracle.verify_read("key1", Some("value2")).is_ok());
        assert!(oracle.verify_read("key1", Some("value1")).is_err());
    }

    #[test]
    fn test_failed_ops_tracking() {
        let mut oracle = ClusterOracle::new();
        oracle.set_tick(100);

        oracle.record_failure("key1", "value1", "connection timeout");
        oracle.record_failure("key2", "value2", "leader changed");

        let (_, failures, _) = oracle.stats();
        assert_eq!(failures, 2);

        let ops = oracle.failed_ops();
        assert_eq!(ops[0].tick, 100);
        assert_eq!(ops[0].error, "connection timeout");
    }
}
