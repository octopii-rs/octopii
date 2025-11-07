use bytes::Bytes;
use std::collections::HashMap;
use std::sync::RwLock;

/// Simple key-value state machine for demonstration
pub struct StateMachine {
    data: RwLock<HashMap<String, Bytes>>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    /// Apply a command to the state machine
    pub fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        // Simple command format: "SET key value" or "GET key"
        let cmd_str = String::from_utf8(command.to_vec())
            .map_err(|e| format!("Invalid UTF-8: {}", e))?;

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();

        match parts.as_slice() {
            ["SET", key, value] => {
                let mut data = self.data.write().unwrap();
                data.insert(key.to_string(), Bytes::from(value.to_string()));
                Ok(Bytes::from("OK"))
            }
            ["GET", key] => {
                let data = self.data.read().unwrap();
                match data.get(*key) {
                    Some(value) => Ok(value.clone()),
                    None => Ok(Bytes::from("NOT_FOUND")),
                }
            }
            ["DELETE", key] => {
                let mut data = self.data.write().unwrap();
                data.remove(*key);
                Ok(Bytes::from("OK"))
            }
            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
    }

    /// Get a snapshot of the current state
    pub fn snapshot(&self) -> HashMap<String, Bytes> {
        let data = self.data.read().unwrap();
        data.clone()
    }

    /// Restore from a snapshot
    pub fn restore(&self, snapshot: HashMap<String, Bytes>) {
        let mut data = self.data.write().unwrap();
        *data = snapshot;
    }
}

impl Default for StateMachine {
    fn default() -> Self {
        Self::new()
    }
}
