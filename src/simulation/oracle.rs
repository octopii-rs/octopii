use crate::wal::wal::Walrus;
use std::collections::HashMap;

pub struct Oracle {
    pub history: HashMap<String, Vec<Vec<u8>>>,
    read_cursors: HashMap<String, usize>,
}

impl Oracle {
    pub fn should_log() -> bool {
        matches!(std::env::var("SIM_VERBOSE").as_deref(), Ok("1"))
    }

    pub fn new() -> Self {
        Self {
            history: HashMap::new(),
            read_cursors: HashMap::new(),
        }
    }

    pub fn history_len(&self, topic: &str) -> usize {
        self.history.get(topic).map(|v| v.len()).unwrap_or(0)
    }

    pub fn record_write(&mut self, topic: &str, data: Vec<u8>) {
        let history = self.history.entry(topic.to_string()).or_default();
        let idx = history.len();
        if topic == "orders" && Self::should_log() {
            eprintln!("[ORACLE] Recording write for orders[{}] len={}", idx, data.len());
        }
        history.push(data);
    }

    pub fn verify_read(&mut self, topic: &str, actual_data: &[u8]) {
        let history = self.history.entry(topic.to_string()).or_default();
        let cursor = self.read_cursors.entry(topic.to_string()).or_default();

        if *cursor >= history.len() {
            panic!(
                "ORACLE FAILURE: Read data for topic '{}' but Oracle thinks stream is empty/finished.\nGot data len: {}",
                topic,
                actual_data.len()
            );
        }

        let expected = &history[*cursor];
        if expected != actual_data {
            let mut found_at = None;
            for (i, entry) in history.iter().enumerate() {
                if entry == actual_data {
                    found_at = Some(i);
                    break;
                }
            }

            eprintln!("ORACLE MISMATCH for topic '{}'", topic);
            eprintln!("Expected (cursor={}): {:?}", cursor, expected);
            eprintln!("Actual: {:?}", actual_data);
            eprintln!("Oracle history length: {}", history.len());
            if let Some(idx) = found_at {
                eprintln!("FOUND: Actual data matches Oracle history[{}]", idx);
            } else {
                eprintln!("NOT FOUND: Actual data doesn't match any Oracle history entry");
            }
            eprintln!("Nearby entries in Oracle:");
            let start = cursor.saturating_sub(2);
            let end = (*cursor + 3).min(history.len());
            for (i, entry) in history.iter().enumerate().take(end).skip(start) {
                eprintln!("  [{}] {:?}", i, entry);
            }
            panic!("Oracle validation failed for topic '{}'", topic);
        }

        *cursor += 1;
    }

    pub fn verify_batch_read(&mut self, topic: &str, actual_entries: &[Vec<u8>]) {
        if actual_entries.is_empty() {
            return;
        }

        let history = self.history.entry(topic.to_string()).or_default();
        let cursor = self.read_cursors.entry(topic.to_string()).or_default();

        if *cursor >= history.len() {
            panic!(
                "ORACLE FAILURE: Walrus returned entries for topic '{}' but Oracle thinks stream is empty.",
                topic
            );
        }

        for entry in actual_entries {
            let expected = &history[*cursor];
            if expected != entry {
                panic!(
                    "ORACLE FAILURE: batch read mismatch for topic '{}' at cursor {}.\nExpected: {:?}\nGot: {:?}",
                    topic,
                    cursor,
                    expected,
                    entry
                );
            }
            *cursor += 1;
        }
    }

    pub fn reset_read_cursors(&mut self) {
        for cursor in self.read_cursors.values_mut() {
            *cursor = 0;
        }
    }

    /// Verify a batch of entries (calls verify_read for each)
    pub fn verify_batch(&mut self, topic: &str, entries: &[Vec<u8>]) {
        for data in entries {
            self.verify_read(topic, data);
        }
    }

    /// Check that we've reached EOF for a topic
    pub fn check_eof(&self, topic: &str) {
        let history_len = self.history.get(topic).map(|v| v.len()).unwrap_or(0);
        let cursor = *self.read_cursors.get(topic).unwrap_or(&0);
        if cursor < history_len {
            panic!(
                "ORACLE FAILURE: Expected EOF for topic '{}', but Oracle has {} more entries.",
                topic,
                history_len - cursor
            );
        }
    }
}

// Tracks entries and durability across crash cycles
#[derive(Debug, Clone)]
struct TrackedEntry {
    cycle: usize,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DurabilityOracle {
    must_survive: HashMap<String, Vec<TrackedEntry>>,

    may_be_lost: HashMap<String, Vec<TrackedEntry>>,

    current_cycle: usize,

    verified_count: HashMap<String, usize>,
}

impl DurabilityOracle {
    pub fn new() -> Self {
        Self {
            must_survive: HashMap::new(),
            may_be_lost: HashMap::new(),
            current_cycle: 0,
            verified_count: HashMap::new(),
        }
    }

    pub fn record_write(
        &mut self,
        topic: &str,
        data: Vec<u8>,
        partial_before: u64,
        partial_after: u64,
    ) {
        let entry = TrackedEntry {
            cycle: self.current_cycle,
            data,
        };

        if partial_after == partial_before {
            self.must_survive
                .entry(topic.into())
                .or_default()
                .push(entry);
        } else {
            self.may_be_lost
                .entry(topic.into())
                .or_default()
                .push(entry);
        }
    }

    pub fn verify_after_recovery(&self, wal: &Walrus) -> Result<(), String> {
        for (topic, entries) in &self.must_survive {
            for (idx, tracked) in entries.iter().enumerate() {
                match wal.read_next(topic, true) {
                    Ok(Some(entry)) => {
                        if entry.data != tracked.data {
                            return Err(format!(
                                "DURABILITY VIOLATION: topic '{}' entry {} (from cycle {}) data mismatch.\n\
                                 Expected {} bytes: {:?}\n\
                                 Got {} bytes: {:?}",
                                topic,
                                idx,
                                tracked.cycle,
                                tracked.data.len(),
                                &tracked.data[..tracked.data.len().min(50)],
                                entry.data.len(),
                                &entry.data[..entry.data.len().min(50)]
                            ));
                        }
                    }
                    Ok(None) => {
                        return Err(format!(
                            "DURABILITY VIOLATION: topic '{}' entry {} (from cycle {}) MISSING.\n\
                             Expected {} bytes, got EOF.\n\
                             Total must_survive entries: {}, verified so far: {}",
                            topic,
                            idx,
                            tracked.cycle,
                            tracked.data.len(),
                            entries.len(),
                            idx
                        ));
                    }
                    Err(e) => {
                        return Err(format!(
                            "DURABILITY VIOLATION: topic '{}' entry {} (from cycle {}) read error.\n\
                             Expected {} bytes, got error: {:?}",
                            topic, idx, tracked.cycle, tracked.data.len(), e
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    pub fn after_recovery(&mut self, wal: &Walrus) {
        let may_be_lost = std::mem::take(&mut self.may_be_lost);

        for (topic, entries) in may_be_lost {
            let must_survive = self.must_survive.entry(topic.clone()).or_default();

            for tracked in entries {
                match wal.read_next(&topic, true) {
                    Ok(Some(entry)) if entry.data == tracked.data => {
                        must_survive.push(tracked);
                    }
                    _ => {
                        break;
                    }
                }
            }
        }

        self.current_cycle += 1;
        self.verified_count.clear();
    }

    /// Get counts for logging/debugging
    pub fn stats(&self) -> (usize, usize) {
        let must_count: usize = self.must_survive.values().map(|v| v.len()).sum();
        let may_count: usize = self.may_be_lost.values().map(|v| v.len()).sum();
        (must_count, may_count)
    }

    /// Get current cycle number
    pub fn cycle(&self) -> usize {
        self.current_cycle
    }

    /// Clear all state (for new test scenario)
    pub fn clear(&mut self) {
        self.must_survive.clear();
        self.may_be_lost.clear();
        self.current_cycle = 0;
        self.verified_count.clear();
    }
}
