use std::collections::BTreeMap;

#[derive(Debug)]
pub struct LogDurabilityOracle {
    must_survive_entries: BTreeMap<u64, Vec<u8>>,
    may_be_lost_entries: Vec<(u64, Vec<u8>)>,
    must_survive_committed: Option<u64>,
    must_survive_vote: Option<(u64, u64)>,
    cycle: u64,
}

impl LogDurabilityOracle {
    pub fn new() -> Self {
        Self {
            must_survive_entries: BTreeMap::new(),
            may_be_lost_entries: Vec::new(),
            must_survive_committed: None,
            must_survive_vote: None,
            cycle: 0,
        }
    }

    pub fn record_entry(&mut self, index: u64, payload: Vec<u8>, must_survive: bool) {
        if must_survive {
            self.must_survive_entries.insert(index, payload);
        } else {
            self.may_be_lost_entries.push((index, payload));
        }
    }

    pub fn record_committed(&mut self, index: u64, must_survive: bool) {
        if must_survive {
            self.must_survive_committed = Some(index);
        }
    }

    pub fn record_vote(&mut self, term: u64, node_id: u64, must_survive: bool) {
        if must_survive {
            self.must_survive_vote = Some((term, node_id));
        }
    }

    pub fn verify_after_recovery(
        &self,
        recovered_entries: &BTreeMap<u64, Vec<u8>>,
        committed: Option<u64>,
        vote: Option<(u64, u64)>,
    ) -> Result<(), String> {
        for (idx, expected) in &self.must_survive_entries {
            match recovered_entries.get(idx) {
                Some(actual) if actual == expected => {}
                Some(_) => {
                    return Err(format!(
                        "log durability violation: entry {} data mismatch in cycle {}",
                        idx, self.cycle
                    ));
                }
                None => {
                    return Err(format!(
                        "log durability violation: entry {} missing in cycle {}",
                        idx, self.cycle
                    ));
                }
            }
        }

        if let Some(required) = self.must_survive_committed {
            match committed {
                Some(actual) if actual >= required => {}
                Some(actual) => {
                    return Err(format!(
                        "log durability violation: committed {} behind required {} in cycle {}",
                        actual, required, self.cycle
                    ));
                }
                None => {
                    return Err(format!(
                        "log durability violation: committed missing (required {}) in cycle {}",
                        required, self.cycle
                    ));
                }
            }
        }

        if let Some((term, node_id)) = self.must_survive_vote {
            match vote {
                Some((actual_term, actual_node)) if (actual_term, actual_node) == (term, node_id) => {}
                Some((actual_term, actual_node)) => {
                    return Err(format!(
                        "log durability violation: vote mismatch (expected {}:{}, got {}:{}) in cycle {}",
                        term, node_id, actual_term, actual_node, self.cycle
                    ));
                }
                None => {
                    return Err(format!(
                        "log durability violation: vote missing (expected {}:{}) in cycle {}",
                        term, node_id, self.cycle
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn after_recovery(&mut self, recovered_entries: &BTreeMap<u64, Vec<u8>>) {
        for (idx, payload) in std::mem::take(&mut self.may_be_lost_entries) {
            if recovered_entries.get(&idx) == Some(&payload) {
                self.must_survive_entries.insert(idx, payload);
            }
        }
        self.cycle += 1;
    }
}
