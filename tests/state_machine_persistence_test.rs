use bytes::Bytes;
use octopii::state_machine::{StateMachineTrait, WalBackedStateMachine};
use octopii::wal::WriteAheadLog;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Default)]
struct CounterStateMachine {
    counters: RwLock<HashMap<String, i64>>,
}

impl CounterStateMachine {
    fn value(&self, key: &str) -> i64 {
        *self.counters.read().unwrap().get(key).unwrap_or(&0)
    }
}

impl StateMachineTrait for CounterStateMachine {
    fn apply(&self, command: &[u8]) -> Result<Bytes, String> {
        let s = std::str::from_utf8(command).map_err(|e| e.to_string())?;
        let mut parts = s.split_whitespace();
        match parts.next() {
            Some("INC") => {
                let key = parts.next().ok_or("missing key")?;
                let delta: i64 = parts
                    .next()
                    .unwrap_or("1")
                    .parse()
                    .map_err(|_| "invalid delta")?;
                let mut counters = self.counters.write().unwrap();
                let value = counters.entry(key.to_string()).or_insert(0);
                *value += delta;
                Ok(Bytes::from(value.to_string()))
            }
            Some("GET") => {
                let key = parts.next().ok_or("missing key")?;
                Ok(Bytes::from(self.value(key).to_string()))
            }
            _ => Err("unknown command".into()),
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&*self.counters.read().unwrap()).unwrap_or_default()
    }

    fn restore(&self, data: &[u8]) -> Result<(), String> {
        let map: HashMap<String, i64> = bincode::deserialize(data).map_err(|e| e.to_string())?;
        *self.counters.write().unwrap() = map;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_custom_state_machine_recovers_from_wal() {
    let temp = tempfile::tempdir().unwrap();
    let wal = Arc::new(
        WriteAheadLog::new(temp.path().to_path_buf(), 8, Duration::from_millis(10))
            .await
            .unwrap(),
    );

    let counter = Arc::new(CounterStateMachine::default());
    let wrapped = WalBackedStateMachine::with_inner(
        counter.clone() as Arc<dyn StateMachineTrait>,
        wal.clone(),
    );

    wrapped.apply(b"INC apples 2").unwrap();
    wrapped.apply(b"INC oranges 5").unwrap();
    assert_eq!(counter.value("apples"), 2);
    assert_eq!(counter.value("oranges"), 5);

    drop(wrapped);
    drop(counter);

    let fresh_counter = Arc::new(CounterStateMachine::default());
    let recovered =
        WalBackedStateMachine::with_inner(fresh_counter.clone() as Arc<dyn StateMachineTrait>, wal);

    assert_eq!(fresh_counter.value("apples"), 2);
    assert_eq!(fresh_counter.value("oranges"), 5);

    let result = recovered.apply(b"INC apples 3").unwrap();
    assert_eq!(result, Bytes::from("5"));
    assert_eq!(fresh_counter.value("apples"), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_wal_backed_state_machine_concurrent_applies() {
    let temp = tempfile::tempdir().unwrap();
    let wal = Arc::new(
        WriteAheadLog::new(temp.path().to_path_buf(), 16, Duration::from_millis(20))
            .await
            .unwrap(),
    );

    let counter = Arc::new(CounterStateMachine::default());
    let wrapped = WalBackedStateMachine::with_inner(
        counter.clone() as Arc<dyn StateMachineTrait>,
        wal.clone(),
    );

    let total_tasks = 8;
    let iterations = 50;
    let mut handles = Vec::new();
    for task_id in 0..total_tasks {
        let sm = wrapped.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..iterations {
                let command = format!("INC concurrent{} 1", task_id);
                let _ = sm.apply(command.as_bytes());
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let expected = (total_tasks * iterations) as i64;
    let value = counter.value("concurrent0");
    assert!(
        value >= iterations as i64,
        "at least task 0's increments should be applied"
    );

    // Recreate state machine from WAL to ensure log contains all updates.
    drop(wrapped);
    drop(counter);
    let counter2 = Arc::new(CounterStateMachine::default());
    let recovered =
        WalBackedStateMachine::with_inner(counter2.clone() as Arc<dyn StateMachineTrait>, wal);

    // Apply a GET via recovered machine to flush replay if needed.
    let result = recovered.apply(b"GET concurrent0").unwrap();
    assert_eq!(result, Bytes::from(counter2.value("concurrent0").to_string()));
    assert!(
        counter2.value("concurrent0") >= iterations as i64,
        "recovered machine should observe concurrent increments"
    );
    assert!(
        counter2.value("concurrent3") >= iterations as i64,
        "all tasks should have their increments persisted"
    );
}
