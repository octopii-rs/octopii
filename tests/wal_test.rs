use bytes::Bytes;
use octopii::wal::WriteAheadLog;
use std::path::PathBuf;
use tokio::time::Duration;

#[tokio::test]
async fn test_wal_basic_operations() {
    let temp_dir = std::env::temp_dir();
    let wal_path = temp_dir.join("test_wal_basic.log");

    // Clean up old file
    let _ = tokio::fs::remove_file(&wal_path).await;

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

    // Clean up
    let _ = tokio::fs::remove_file(&wal_path).await;
}

#[tokio::test]
async fn test_wal_batching() {
    let temp_dir = std::env::temp_dir();
    let wal_path = temp_dir.join("test_wal_batching.log");

    // Clean up old file
    let _ = tokio::fs::remove_file(&wal_path).await;

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

    // Clean up
    let _ = tokio::fs::remove_file(&wal_path).await;
}

#[tokio::test]
async fn test_wal_recovery() {
    let temp_dir = std::env::temp_dir();
    let wal_path = temp_dir.join("test_wal_recovery.log");

    // Clean up old file
    let _ = tokio::fs::remove_file(&wal_path).await;

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

    // Clean up
    let _ = tokio::fs::remove_file(&wal_path).await;
}
