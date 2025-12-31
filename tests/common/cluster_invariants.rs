//! Cluster Invariant Checker
//!
//! Automated verification of Raft properties during simulation tests.
//! These invariants are checked periodically to catch violations early.

#![cfg(all(feature = "simulation", feature = "openraft"))]

use std::collections::HashMap;

/// Result of invariant checking
#[derive(Debug)]
pub struct InvariantViolation {
    pub invariant: &'static str,
    pub message: String,
    pub details: HashMap<String, String>,
}

impl std::fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "INVARIANT VIOLATION [{}]: {}", self.invariant, self.message)?;
        if !self.details.is_empty() {
            write!(f, "\nDetails:")?;
            for (k, v) in &self.details {
                write!(f, "\n  {}: {}", k, v)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for InvariantViolation {}

/// Tracks leader state for single-leader invariant checking
#[derive(Debug, Clone)]
pub struct LeaderRecord {
    pub node_id: u64,
    pub term: u64,
    pub tick: u64,
}

/// Invariant checker for cluster simulation tests
pub struct InvariantChecker {
    /// Leaders observed at each term
    leaders_by_term: HashMap<u64, Vec<LeaderRecord>>,
    /// Committed entries we've observed (index -> (term, data_hash))
    committed_entries: HashMap<u64, (u64, u64)>,
    /// Last known committed index per node
    node_committed: HashMap<u64, u64>,
    /// Current tick
    current_tick: u64,
}

impl Default for InvariantChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl InvariantChecker {
    pub fn new() -> Self {
        Self {
            leaders_by_term: HashMap::new(),
            committed_entries: HashMap::new(),
            node_committed: HashMap::new(),
            current_tick: 0,
        }
    }

    /// Update the current tick
    pub fn set_tick(&mut self, tick: u64) {
        self.current_tick = tick;
    }

    /// Record a leader observation
    pub fn record_leader(&mut self, node_id: u64, term: u64) {
        let record = LeaderRecord {
            node_id,
            term,
            tick: self.current_tick,
        };
        self.leaders_by_term
            .entry(term)
            .or_default()
            .push(record);
    }

    /// Record a committed entry observation
    pub fn record_committed(&mut self, node_id: u64, index: u64, term: u64, data_hash: u64) {
        // Track committed index per node
        self.node_committed.insert(node_id, index);

        // Track entry by index
        self.committed_entries.insert(index, (term, data_hash));
    }

    /// Check the single leader per term invariant
    ///
    /// Raft Invariant: At most one leader can be elected in a given term.
    pub fn check_single_leader(&self) -> Result<(), InvariantViolation> {
        for (term, records) in &self.leaders_by_term {
            // Get unique leader IDs for this term
            let unique_leaders: std::collections::HashSet<u64> =
                records.iter().map(|r| r.node_id).collect();

            if unique_leaders.len() > 1 {
                let mut details = HashMap::new();
                details.insert("term".to_string(), term.to_string());
                details.insert(
                    "leaders".to_string(),
                    format!("{:?}", unique_leaders),
                );
                details.insert(
                    "observations".to_string(),
                    format!("{:?}", records),
                );

                return Err(InvariantViolation {
                    invariant: "SingleLeaderPerTerm",
                    message: format!(
                        "Multiple leaders observed in term {}: {:?}",
                        term, unique_leaders
                    ),
                    details,
                });
            }
        }
        Ok(())
    }

    /// Check the commit safety invariant
    ///
    /// Raft Invariant: If a log entry is committed, it will never be overwritten
    /// with a different entry at the same index.
    pub fn check_commit_safety(
        &self,
        entries: &[(u64, u64, u64)], // (index, term, data_hash)
    ) -> Result<(), InvariantViolation> {
        for (index, term, data_hash) in entries {
            if let Some((prev_term, prev_hash)) = self.committed_entries.get(index) {
                if *prev_term != *term || *prev_hash != *data_hash {
                    let mut details = HashMap::new();
                    details.insert("index".to_string(), index.to_string());
                    details.insert("previous_term".to_string(), prev_term.to_string());
                    details.insert("new_term".to_string(), term.to_string());
                    details.insert("previous_hash".to_string(), prev_hash.to_string());
                    details.insert("new_hash".to_string(), data_hash.to_string());

                    return Err(InvariantViolation {
                        invariant: "CommitSafety",
                        message: format!(
                            "Committed entry at index {} was overwritten",
                            index
                        ),
                        details,
                    });
                }
            }
        }
        Ok(())
    }

    /// Check state machine consistency across nodes
    ///
    /// All nodes should have the same state machine state for entries
    /// up to the minimum committed index.
    pub fn check_state_machine_consistency(
        &self,
        node_states: &[(u64, HashMap<String, String>)], // (node_id, kv_state)
    ) -> Result<(), InvariantViolation> {
        if node_states.len() < 2 {
            return Ok(());
        }

        let (first_id, first_state) = &node_states[0];

        for (node_id, state) in node_states.iter().skip(1) {
            // Find keys that differ
            let mut diffs = Vec::new();

            for (key, value) in first_state {
                match state.get(key) {
                    Some(other_value) if other_value != value => {
                        diffs.push(format!(
                            "key '{}': node {} has '{}', node {} has '{}'",
                            key, first_id, value, node_id, other_value
                        ));
                    }
                    None => {
                        diffs.push(format!(
                            "key '{}': present in node {}, missing in node {}",
                            key, first_id, node_id
                        ));
                    }
                    _ => {}
                }
            }

            // Check for keys in other node but not in first
            for key in state.keys() {
                if !first_state.contains_key(key) {
                    diffs.push(format!(
                        "key '{}': missing in node {}, present in node {}",
                        key, first_id, node_id
                    ));
                }
            }

            if !diffs.is_empty() {
                let mut details = HashMap::new();
                details.insert("node_a".to_string(), first_id.to_string());
                details.insert("node_b".to_string(), node_id.to_string());
                details.insert("differences".to_string(), diffs.join("; "));

                return Err(InvariantViolation {
                    invariant: "StateMachineConsistency",
                    message: format!(
                        "State machines diverged between nodes {} and {}",
                        first_id, node_id
                    ),
                    details,
                });
            }
        }

        Ok(())
    }

    /// Check log matching invariant
    ///
    /// Raft Invariant: If two logs contain an entry with the same index and term,
    /// then the logs are identical in all entries up through the given index.
    pub fn check_log_matching(
        &self,
        log_a: &[(u64, u64)], // [(index, term), ...]
        log_b: &[(u64, u64)],
        node_a: u64,
        node_b: u64,
    ) -> Result<(), InvariantViolation> {
        // Build index -> term maps
        let map_a: HashMap<u64, u64> = log_a.iter().copied().collect();
        let map_b: HashMap<u64, u64> = log_b.iter().copied().collect();

        // Find common indices
        for (index, term_a) in &map_a {
            if let Some(term_b) = map_b.get(index) {
                if term_a == term_b {
                    // Same index and term - verify all prior entries match
                    for prior_idx in 1..*index {
                        let prior_a = map_a.get(&prior_idx);
                        let prior_b = map_b.get(&prior_idx);

                        if prior_a != prior_b {
                            let mut details = HashMap::new();
                            details.insert("matching_index".to_string(), index.to_string());
                            details.insert("matching_term".to_string(), term_a.to_string());
                            details.insert("divergent_index".to_string(), prior_idx.to_string());
                            details.insert(
                                "term_in_node_a".to_string(),
                                format!("{:?}", prior_a),
                            );
                            details.insert(
                                "term_in_node_b".to_string(),
                                format!("{:?}", prior_b),
                            );

                            return Err(InvariantViolation {
                                invariant: "LogMatching",
                                message: format!(
                                    "Logs match at index {} term {} but diverge at prior index {}",
                                    index, term_a, prior_idx
                                ),
                                details,
                            });
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check commit monotonicity
    ///
    /// A node's committed index should never decrease.
    pub fn check_commit_monotonicity(
        &mut self,
        node_id: u64,
        new_committed: u64,
    ) -> Result<(), InvariantViolation> {
        if let Some(&prev_committed) = self.node_committed.get(&node_id) {
            if new_committed < prev_committed {
                let mut details = HashMap::new();
                details.insert("node_id".to_string(), node_id.to_string());
                details.insert("previous_committed".to_string(), prev_committed.to_string());
                details.insert("new_committed".to_string(), new_committed.to_string());
                details.insert("tick".to_string(), self.current_tick.to_string());

                return Err(InvariantViolation {
                    invariant: "CommitMonotonicity",
                    message: format!(
                        "Node {}'s committed index decreased from {} to {}",
                        node_id, prev_committed, new_committed
                    ),
                    details,
                });
            }
        }

        self.node_committed.insert(node_id, new_committed);
        Ok(())
    }

    /// Run all invariant checks
    pub fn check_all(
        &mut self,
        entries: &[(u64, u64, u64)],
        node_states: &[(u64, HashMap<String, String>)],
    ) -> Result<(), InvariantViolation> {
        self.check_single_leader()?;
        self.check_commit_safety(entries)?;
        self.check_state_machine_consistency(node_states)?;
        Ok(())
    }

    /// Reset the checker state
    pub fn reset(&mut self) {
        self.leaders_by_term.clear();
        self.committed_entries.clear();
        self.node_committed.clear();
        self.current_tick = 0;
    }

    /// Get statistics
    pub fn stats(&self) -> (usize, usize, usize) {
        (
            self.leaders_by_term.len(),
            self.committed_entries.len(),
            self.node_committed.len(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_leader_ok() {
        let mut checker = InvariantChecker::new();
        checker.record_leader(1, 1);
        checker.record_leader(1, 1); // Same leader, same term - OK
        checker.record_leader(2, 2); // Different leader, different term - OK

        assert!(checker.check_single_leader().is_ok());
    }

    #[test]
    fn test_single_leader_violation() {
        let mut checker = InvariantChecker::new();
        checker.record_leader(1, 1);
        checker.record_leader(2, 1); // Different leader, same term - VIOLATION

        let result = checker.check_single_leader();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.invariant, "SingleLeaderPerTerm");
    }

    #[test]
    fn test_commit_safety_ok() {
        let mut checker = InvariantChecker::new();
        checker.record_committed(1, 1, 1, 12345);
        checker.record_committed(1, 2, 1, 67890);

        // Same entries
        let entries = vec![(1, 1, 12345), (2, 1, 67890)];
        assert!(checker.check_commit_safety(&entries).is_ok());
    }

    #[test]
    fn test_commit_safety_violation() {
        let mut checker = InvariantChecker::new();
        checker.record_committed(1, 1, 1, 12345);

        // Different data at same index
        let entries = vec![(1, 1, 99999)];
        let result = checker.check_commit_safety(&entries);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.invariant, "CommitSafety");
    }

    #[test]
    fn test_commit_monotonicity() {
        let mut checker = InvariantChecker::new();

        // Increasing - OK
        assert!(checker.check_commit_monotonicity(1, 5).is_ok());
        assert!(checker.check_commit_monotonicity(1, 10).is_ok());
        assert!(checker.check_commit_monotonicity(1, 10).is_ok()); // Same - OK

        // Decreasing - VIOLATION
        let result = checker.check_commit_monotonicity(1, 8);
        assert!(result.is_err());
    }

    #[test]
    fn test_state_machine_consistency() {
        let checker = InvariantChecker::new();

        let mut state1 = HashMap::new();
        state1.insert("key1".to_string(), "val1".to_string());
        state1.insert("key2".to_string(), "val2".to_string());

        let mut state2 = HashMap::new();
        state2.insert("key1".to_string(), "val1".to_string());
        state2.insert("key2".to_string(), "val2".to_string());

        let states = vec![(1, state1), (2, state2)];
        assert!(checker.check_state_machine_consistency(&states).is_ok());
    }

    #[test]
    fn test_state_machine_divergence() {
        let checker = InvariantChecker::new();

        let mut state1 = HashMap::new();
        state1.insert("key1".to_string(), "val1".to_string());

        let mut state2 = HashMap::new();
        state2.insert("key1".to_string(), "different".to_string());

        let states = vec![(1, state1), (2, state2)];
        let result = checker.check_state_machine_consistency(&states);
        assert!(result.is_err());
    }
}
