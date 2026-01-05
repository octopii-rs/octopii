use std::collections::{BTreeMap, HashMap};

#[derive(Debug)]
struct NodeDurability {
    must_survive_entries: BTreeMap<u64, Vec<u8>>,
    may_be_lost_entries: Vec<(u64, Vec<u8>)>,
    must_survive_committed: Option<u64>,
    must_survive_vote: Option<(u64, u64)>,
}

impl NodeDurability {
    fn new() -> Self {
        Self {
            must_survive_entries: BTreeMap::new(),
            may_be_lost_entries: Vec::new(),
            must_survive_committed: None,
            must_survive_vote: None,
        }
    }
}

#[derive(Debug)]
pub struct LogDurabilityOracle {
    per_node: HashMap<u64, NodeDurability>,
    cycle: u64,
}

impl LogDurabilityOracle {
    pub fn new() -> Self {
        Self {
            per_node: HashMap::new(),
            cycle: 0,
        }
    }

    pub fn record_entry(&mut self, node_id: u64, index: u64, payload: Vec<u8>, must_survive: bool) {
        let node = self.per_node.entry(node_id).or_insert_with(NodeDurability::new);
        if must_survive {
            node.must_survive_entries.insert(index, payload);
        } else {
            node.may_be_lost_entries.push((index, payload));
        }
    }

    pub fn record_committed(&mut self, node_id: u64, index: u64, must_survive: bool) {
        if must_survive {
            let node = self.per_node.entry(node_id).or_insert_with(NodeDurability::new);
            node.must_survive_committed = Some(index);
        }
    }

    pub fn record_vote(&mut self, node_id: u64, term: u64, leader_id: u64, must_survive: bool) {
        if must_survive {
            let node = self.per_node.entry(node_id).or_insert_with(NodeDurability::new);
            node.must_survive_vote = Some((term, leader_id));
        }
    }

    pub fn verify_after_recovery(
        &self,
        node_id: u64,
        recovered_entries: &BTreeMap<u64, Vec<u8>>,
        committed: Option<u64>,
        vote: Option<(u64, u64)>,
        last_purged: Option<u64>,
    ) -> Result<(), String> {
        let Some(node) = self.per_node.get(&node_id) else {
            return Ok(());
        };
        let purged = last_purged.unwrap_or(0);

        for (idx, expected) in &node.must_survive_entries {
            if *idx <= purged {
                continue;
            }
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

        if let Some(required) = node.must_survive_committed {
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

        if let Some((term, leader_id)) = node.must_survive_vote {
            match vote {
                Some((actual_term, actual_node)) if actual_term > term => {}
                Some((actual_term, actual_node)) if actual_term == term && actual_node == leader_id => {}
                Some((actual_term, actual_node)) => {
                    return Err(format!(
                        "log durability violation: vote mismatch (expected {}:{}, got {}:{}) in cycle {}",
                        term, leader_id, actual_term, actual_node, self.cycle
                    ));
                }
                None => {
                    return Err(format!(
                        "log durability violation: vote missing (expected {}:{}) in cycle {}",
                        term, leader_id, self.cycle
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn after_recovery(
        &mut self,
        node_id: u64,
        recovered_entries: &BTreeMap<u64, Vec<u8>>,
        last_purged: Option<u64>,
    ) {
        let node = self
            .per_node
            .entry(node_id)
            .or_insert_with(NodeDurability::new);
        for (idx, payload) in std::mem::take(&mut node.may_be_lost_entries) {
            if recovered_entries.get(&idx) == Some(&payload) {
                node.must_survive_entries.insert(idx, payload);
            }
        }
        if let Some(purged) = last_purged {
            node.must_survive_entries.retain(|idx, _| *idx > purged);
        }
        self.cycle += 1;
    }
}
