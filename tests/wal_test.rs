use bytes::Bytes;
use octopii::wal::WriteAheadLog;
use protobuf::Message;
use raft::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Duration;

// Helper to create a test WAL path
fn test_wal_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("test_wal_{}.log", name))
}

// Helper to clean up a WAL directory (Walrus stores in wal_files/)
async fn cleanup_wal(path: &PathBuf) {
    // Walrus creates wal_files/<name>/ directory
    if let Some(file_name) = path.file_name() {
        if let Some(name_str) = file_name.to_str() {
            let wal_dir = PathBuf::from("wal_files").join(name_str);
            let _ = tokio::fs::remove_dir_all(&wal_dir).await;
        }
    }
    let _ = tokio::fs::remove_file(path).await;
}

#[tokio::test]
async fn test_wal_basic_operations() {
    let wal_path = test_wal_path("basic");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
        .await
        .unwrap();

    // Write entries
    let data1 = Bytes::from("entry1");
    let data2 = Bytes::from("entry2");
    let data3 = Bytes::from("entry3");

    let offset1 = wal.append(data1.clone()).await.unwrap();
    let offset2 = wal.append(data2.clone()).await.unwrap();
    let offset3 = wal.append(data3.clone()).await.unwrap();

    // Verify offsets are increasing
    assert!(offset2 > offset1);
    assert!(offset3 > offset2);

    // Flush
    wal.flush().await.unwrap();

    // Read back
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0], data1);
    assert_eq!(entries[1], data2);
    assert_eq!(entries[2], data3);

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_batching() {
    let wal_path = test_wal_path("batching");
    cleanup_wal(&wal_path).await;

    // Small batch size for testing
    let wal = WriteAheadLog::new(wal_path.clone(), 5, Duration::from_millis(50))
        .await
        .unwrap();

    // Write many entries quickly
    let mut handles = Vec::new();
    for i in 0..20 {
        let data = Bytes::from(format!("entry_{}", i));
        handles.push(wal.append(data));
    }

    // Wait for all writes
    for handle in handles {
        handle.await.unwrap();
    }

    // Flush to ensure everything is written
    wal.flush().await.unwrap();

    // Read back and verify
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 20);

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry, &Bytes::from(format!("entry_{}", i)));
    }

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_recovery() {
    let wal_path = test_wal_path("recovery");
    cleanup_wal(&wal_path).await;

    // Create WAL and write some data
    {
        let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
            .await
            .unwrap();

        for i in 0..10 {
            let data = Bytes::from(format!("persistent_entry_{}", i));
            wal.append(data).await.unwrap();
        }

        wal.flush().await.unwrap();
    } // Drop WAL

    // Create new WAL instance and recover
    let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
        .await
        .unwrap();

    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 10);

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry, &Bytes::from(format!("persistent_entry_{}", i)));
    }

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_raft_entries() {
    // Test serializing/deserializing actual Raft entries (protobuf)
    let wal_path = test_wal_path("raft_entries");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
        .await
        .unwrap();

    // Create Raft entries like WalStorage does
    let mut entries = Vec::new();
    for i in 1..=10 {
        let mut entry = Entry::default();
        entry.index = i;
        entry.term = 1;
        entry.entry_type = EntryType::EntryNormal;
        entry.data = Bytes::from(format!("raft_data_{}", i).into_bytes());
        entries.push(entry);
    }

    // Serialize and write entries
    for entry in &entries {
        let data = entry.write_to_bytes().unwrap();
        let bytes = Bytes::from(data);
        wal.append(bytes).await.unwrap();
    }

    wal.flush().await.unwrap();

    // Read back and deserialize
    let recovered_bytes = wal.read_all().await.unwrap();
    assert_eq!(recovered_bytes.len(), 10);

    for (i, bytes) in recovered_bytes.iter().enumerate() {
        let recovered: Entry = Message::parse_from_bytes(bytes).unwrap();
        assert_eq!(recovered.index, entries[i].index);
        assert_eq!(recovered.term, entries[i].term);
        assert_eq!(recovered.data, entries[i].data);
    }

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_large_entries() {
    // Test with large entries (like Raft snapshots or big proposals)
    let wal_path = test_wal_path("large_entries");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 5, Duration::from_millis(100))
        .await
        .unwrap();

    // Create entries of various sizes
    let sizes = vec![
        1024,           // 1 KB
        64 * 1024,      // 64 KB
        512 * 1024,     // 512 KB
        1024 * 1024,    // 1 MB
    ];

    for size in &sizes {
        let data = vec![0xAB; *size];
        let bytes = Bytes::from(data);
        wal.append(bytes).await.unwrap();
    }

    wal.flush().await.unwrap();

    // Read back and verify sizes
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), sizes.len());

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.len(), sizes[i]);
        // Verify content
        assert!(entry.iter().all(|&b| b == 0xAB));
    }

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_concurrent_writes() {
    // Test concurrent writes from multiple tasks (like multiple Raft proposals)
    let wal_path = test_wal_path("concurrent");
    cleanup_wal(&wal_path).await;

    let wal = Arc::new(
        WriteAheadLog::new(wal_path.clone(), 20, Duration::from_millis(50))
            .await
            .unwrap(),
    );

    let num_tasks = 10;
    let writes_per_task = 50;

    // Spawn multiple tasks writing concurrently
    let mut handles = Vec::new();
    for task_id in 0..num_tasks {
        let wal_clone = wal.clone();
        let handle = tokio::spawn(async move {
            for i in 0..writes_per_task {
                let data = Bytes::from(format!("task_{}_write_{}", task_id, i));
                wal_clone.append(data).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    wal.flush().await.unwrap();

    // Verify total count
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), num_tasks * writes_per_task);

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_empty_recovery() {
    // Test recovering from an empty/non-existent WAL
    let wal_path = test_wal_path("empty_recovery");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
        .await
        .unwrap();

    // Write something first (WAL creates file on first write)
    wal.append(Bytes::from("first_entry")).await.unwrap();
    wal.flush().await.unwrap();

    // Verify
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 1);

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_flush_guarantees() {
    // Test that explicit flush persists data immediately
    let wal_path = test_wal_path("flush_guarantees");
    cleanup_wal(&wal_path).await;

    // Use large batch size and moderate timer so explicit flush is needed
    let wal = WriteAheadLog::new(wal_path.clone(), 1000, Duration::from_secs(10))
        .await
        .unwrap();

    // Write entries
    for i in 0..10 {
        let data = Bytes::from(format!("entry_{}", i));
        wal.append(data).await.unwrap();
    }

    // Explicit flush
    wal.flush().await.unwrap();

    // Verify data is readable immediately after flush
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 10);

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_sequential_writes() {
    // Test that sequential writes maintain order (critical for Raft log)
    let wal_path = test_wal_path("sequential");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
        .await
        .unwrap();

    // Write entries in sequence
    for i in 0..100 {
        let data = Bytes::from(i.to_string());
        wal.append(data).await.unwrap();
    }

    wal.flush().await.unwrap();

    // Verify order is preserved
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 100);

    for (i, entry) in entries.iter().enumerate() {
        let value = String::from_utf8(entry.to_vec()).unwrap();
        assert_eq!(value, i.to_string());
    }

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_multiple_flush_cycles() {
    // Test multiple write-flush cycles (like Raft hard state updates)
    let wal_path = test_wal_path("multi_flush");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 5, Duration::from_millis(100))
        .await
        .unwrap();

    let cycles = 10;
    let writes_per_cycle = 5;

    for cycle in 0..cycles {
        // Write batch
        for i in 0..writes_per_cycle {
            let data = Bytes::from(format!("cycle_{}_entry_{}", cycle, i));
            wal.append(data).await.unwrap();
        }
        // Flush after each cycle
        wal.flush().await.unwrap();
    }

    // Verify all entries
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), cycles * writes_per_cycle);

    cleanup_wal(&wal_path).await;
}

#[tokio::test]
async fn test_wal_zero_byte_entries() {
    // Test edge case: zero-byte entries
    let wal_path = test_wal_path("zero_byte");
    cleanup_wal(&wal_path).await;

    let wal = WriteAheadLog::new(wal_path.clone(), 10, Duration::from_millis(100))
        .await
        .unwrap();

    // Write empty entries
    for _ in 0..5 {
        let data = Bytes::from(vec![]);
        wal.append(data).await.unwrap();
    }

    // Write normal entry
    wal.append(Bytes::from("normal")).await.unwrap();

    wal.flush().await.unwrap();

    // Verify
    let entries = wal.read_all().await.unwrap();
    assert_eq!(entries.len(), 6);
    assert_eq!(entries[0].len(), 0);
    assert_eq!(entries[5], Bytes::from("normal"));

    cleanup_wal(&wal_path).await;
}
