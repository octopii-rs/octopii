/// Example: Custom Counter State Machine
///
/// This example demonstrates how to implement a custom state machine
/// that will be replicated across a Raft cluster.
///
/// The CounterStateMachine maintains a simple distributed counter that supports:
/// - INCREMENT: Increments the counter
/// - DECREMENT: Decrements the counter
/// - GET: Returns the current counter value
/// - RESET: Resets the counter to zero
///
/// Run with: cargo run --example custom_state_machine

use bytes::Bytes;
use octopii::StateMachineTrait;
use std::sync::RwLock;

/// A simple counter state machine
struct CounterStateMachine {
    counter: RwLock<i64>,
}

impl CounterStateMachine {
    fn new() -> Self {
        Self {
            counter: RwLock::new(0),
        }
    }
}

impl StateMachineTrait for CounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let cmd_str = String::from_utf8(command.to_vec())
            .map_err(|e| format!("Invalid UTF-8: {}", e))?;

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();

        match parts.as_slice() {
            ["INCREMENT"] => {
                let mut counter = self.counter.write().unwrap();
                *counter += 1;
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["DECREMENT"] => {
                let mut counter = self.counter.write().unwrap();
                *counter -= 1;
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["GET"] => {
                let counter = self.counter.read().unwrap();
                Ok(Bytes::from(format!("{}", *counter)))
            }
            ["RESET"] => {
                let mut counter = self.counter.write().unwrap();
                *counter = 0;
                Ok(Bytes::from("0"))
            }
            _ => Err(format!("Unknown command: {}", cmd_str)),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        let counter = self.counter.read().unwrap();
        counter.to_le_bytes().to_vec()
    }

    fn restore(&self, snapshot: &[u8]) -> Result<(), String> {
        if snapshot.is_empty() {
            return Ok(());
        }

        if snapshot.len() != 8 {
            return Err(format!(
                "Invalid snapshot size: expected 8 bytes, got {}",
                snapshot.len()
            ));
        }

        let value = i64::from_le_bytes(
            snapshot
                .try_into()
                .map_err(|e| format!("Failed to convert snapshot: {:?}", e))?,
        );

        let mut counter = self.counter.write().unwrap();
        *counter = value;

        Ok(())
    }

    fn compact(&self) -> Result<(), String> {
        // Counter doesn't need compaction
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Custom State Machine Example ===\n");
    println!("This example shows how to implement a custom state machine");
    println!("that will be replicated across a Raft cluster.\n");

    println!("✓ Custom state machine implementation complete!");
    println!("\nTo use this custom state machine in production:");
    println!("1. The state machine implements StateMachineTrait");
    println!("2. It defines custom commands: INCREMENT, DECREMENT, GET, RESET");
    println!("3. It handles its own serialization for snapshots");
    println!("4. All operations will be replicated across the cluster\n");
    println!("5. See docs/custom_state_machines.md for integration guide\n");

    // Demonstrate the state machine API directly
    println!("Demonstrating the Counter state machine API:\n");

    let counter = CounterStateMachine::new();

    // Test INCREMENT
    let result = counter.apply(b"INCREMENT")?;
    println!("INCREMENT -> {}", String::from_utf8_lossy(&result));

    // Test INCREMENT again
    let result = counter.apply(b"INCREMENT")?;
    println!("INCREMENT -> {}", String::from_utf8_lossy(&result));

    // Test GET
    let result = counter.apply(b"GET")?;
    println!("GET -> {}", String::from_utf8_lossy(&result));

    // Test DECREMENT
    let result = counter.apply(b"DECREMENT")?;
    println!("DECREMENT -> {}", String::from_utf8_lossy(&result));

    // Test snapshot
    let snapshot = counter.snapshot();
    println!("\nSnapshot created: {:?}", snapshot);

    // Test restore
    counter.apply(b"RESET")?;
    println!("After RESET: {}", String::from_utf8_lossy(&counter.apply(b"GET")?));

    counter.restore(&snapshot)?;
    println!(
        "After restore: {}",
        String::from_utf8_lossy(&counter.apply(b"GET")?)
    );

    println!("\n✓ Example complete!");

    Ok(())
}
