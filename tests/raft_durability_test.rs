//! Comprehensive tests for Raft durability with Walrus
//!
//! These tests verify that all Raft state components survive crashes
//! and are correctly recovered from Walrus.

use bytes::Bytes;
use octopii::raft::{KvStateMachine, WalStorage};
use octopii::StateMachineTrait;
use octopii::wal::WriteAheadLog;
use raft::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

/// Helper to create a temporary WAL for testing
/// Returns (WAL, actual_path_used, temp_dir)
async fn create_test_wal() -> (Arc<WriteAheadLog>, PathBuf, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    // Use thread ID + timestamp for unique key (thread IDs can be reused!)
    let thread_id = std::thread::current().id();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let unique_key = format!("test_wal_{:?}_{}", thread_id, timestamp);
    let wal_path = temp_dir.path().join(&unique_key);
    let wal = Arc::new(
        WriteAheadLog::new(wal_path.clone(), 100, Duration::from_millis(100))
            .await
            .unwrap(),
    );
    (wal, wal_path, temp_dir)
}

/// Helper to create a WAL at the same path (simulates restart)
async fn create_wal_at_path(path: PathBuf) -> Arc<WriteAheadLog> {
    Arc::new(
        WriteAheadLog::new(path, 100, Duration::from_millis(100))
            .await
            .unwrap(),
    )
}

/// Cleanup wal_files directory
fn cleanup_wal_files() {
    let _ = std::fs::remove_dir_all("wal_files");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hard_state_persistence_and_recovery() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Create storage and set hard state
    {
        let storage = WalStorage::new(Arc::clone(&wal));

        let mut hs = HardState::default();
        hs.term = 42;
        hs.vote = 7;
        hs.commit = 100;

        storage.set_hard_state(hs.clone());

        // Verify in-memory state
        let initial = storage.initial_state().unwrap();
        assert_eq!(initial.hard_state.term, 42);
        assert_eq!(initial.hard_state.vote, 7);
        assert_eq!(initial.hard_state.commit, 100);
    }

    // Drop storage and WAL (simulate crash)
    drop(wal);

    // Recreate WAL and storage (simulate restart)
    let wal2 = create_wal_at_path(wal_path).await;
    let storage2 = WalStorage::new(wal2);

    // Verify hard state was recovered
    let recovered = storage2.initial_state().unwrap();
    assert_eq!(recovered.hard_state.term, 42, "Term should survive restart");
    assert_eq!(recovered.hard_state.vote, 7, "Vote should survive restart");
    assert_eq!(
        recovered.hard_state.commit, 100,
        "Commit should survive restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hard_state_prevents_double_vote() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Node votes for candidate 5 in term 3
    {
        let storage = WalStorage::new(Arc::clone(&wal));

        let mut hs = HardState::default();
        hs.term = 3;
        hs.vote = 5;

        storage.set_hard_state(hs);
    }

    drop(wal);

    // After crash, verify node remembers it voted
    let wal2 = create_wal_at_path(wal_path).await;
    let storage2 = WalStorage::new(wal2);

    let recovered = storage2.initial_state().unwrap();
    assert_eq!(recovered.hard_state.term, 3);
    assert_eq!(
        recovered.hard_state.vote, 5,
        "Must remember who we voted for!"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_conf_state_persistence_and_recovery() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Create storage and set conf state
    {
        let storage = WalStorage::new(Arc::clone(&wal));

        let mut cs = ConfState::default();
        cs.voters = vec![1, 2, 3];
        cs.learners = vec![4, 5];
        cs.auto_leave = true;

        storage.set_conf_state(cs.clone());

        // Verify in-memory state
        let initial = storage.initial_state().unwrap();
        assert_eq!(initial.conf_state.voters, vec![1, 2, 3]);
        assert_eq!(initial.conf_state.learners, vec![4, 5]);
        assert_eq!(initial.conf_state.auto_leave, true);
    }

    drop(wal);

    // Recreate and verify recovery
    let wal2 = create_wal_at_path(wal_path).await;
    let storage2 = WalStorage::new(wal2);

    let recovered = storage2.initial_state().unwrap();
    assert_eq!(
        recovered.conf_state.voters,
        vec![1, 2, 3],
        "Voters should survive"
    );
    assert_eq!(
        recovered.conf_state.learners,
        vec![4, 5],
        "Learners should survive"
    );
    assert_eq!(
        recovered.conf_state.auto_leave, true,
        "auto_leave should survive"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_persistence_and_recovery() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Create storage and apply snapshot
    {
        let storage = WalStorage::new(Arc::clone(&wal));

        let mut snapshot = Snapshot::default();
        snapshot.data = Bytes::from("snapshot_data_here");

        let metadata = snapshot.mut_metadata();
        metadata.index = 1000;
        metadata.term = 5;

        let mut cs = ConfState::default();
        cs.voters = vec![1, 2, 3];
        *metadata.mut_conf_state() = cs;

        storage.apply_snapshot(snapshot.clone()).unwrap();

        // Verify snapshot is stored
        let stored = storage.snapshot(0, 0).unwrap();
        assert_eq!(stored.data, Bytes::from("snapshot_data_here"));
        assert_eq!(stored.get_metadata().index, 1000);
    }

    drop(wal);

    // Recreate and verify snapshot recovery
    let wal2 = create_wal_at_path(wal_path).await;
    let storage2 = WalStorage::new(wal2);

    let recovered = storage2.snapshot(0, 0).unwrap();
    assert_eq!(
        recovered.data,
        Bytes::from("snapshot_data_here"),
        "Snapshot data should survive"
    );
    assert_eq!(
        recovered.get_metadata().index,
        1000,
        "Snapshot index should survive"
    );
    assert_eq!(
        recovered.get_metadata().term,
        5,
        "Snapshot term should survive"
    );
    assert_eq!(
        recovered.get_metadata().get_conf_state().voters,
        vec![1, 2, 3]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_state_machine_persistence_and_recovery() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Create state machine and apply operations
    {
        let sm = KvStateMachine::with_wal(Arc::clone(&wal));

        // Apply some SET operations
        let result = sm.apply(b"SET foo bar").unwrap();
        assert_eq!(result, Bytes::from("OK"));

        let result = sm.apply(b"SET hello world").unwrap();
        assert_eq!(result, Bytes::from("OK"));

        let result = sm.apply(b"SET count 42").unwrap();
        assert_eq!(result, Bytes::from("OK"));

        // Verify data is there
        let result = sm.apply(b"GET foo").unwrap();
        assert_eq!(result, Bytes::from("bar"));
    }

    drop(wal);

    // Recreate state machine (simulate restart)
    let wal2 = create_wal_at_path(wal_path).await;
    let sm2 = KvStateMachine::with_wal(wal2);

    // Verify all data was recovered
    let result = sm2.apply(b"GET foo").unwrap();
    assert_eq!(result, Bytes::from("bar"), "foo should be recovered");

    let result = sm2.apply(b"GET hello").unwrap();
    assert_eq!(result, Bytes::from("world"), "hello should be recovered");

    let result = sm2.apply(b"GET count").unwrap();
    assert_eq!(result, Bytes::from("42"), "count should be recovered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_state_machine_delete_with_tombstone() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Create state machine, SET then DELETE
    {
        let sm = KvStateMachine::with_wal(Arc::clone(&wal));

        sm.apply(b"SET temp_key temp_value").unwrap();
        sm.apply(b"DELETE temp_key").unwrap();

        // Verify it's gone
        let result = sm.apply(b"GET temp_key").unwrap();
        assert_eq!(result, Bytes::from("NOT_FOUND"));
    }

    drop(wal);

    // After restart, verify deletion persisted
    let wal2 = create_wal_at_path(wal_path).await;
    let sm2 = KvStateMachine::with_wal(wal2);

    let result = sm2.apply(b"GET temp_key").unwrap();
    assert_eq!(
        result,
        Bytes::from("NOT_FOUND"),
        "DELETE should survive restart"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_state_machine_overwrite_survives() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    {
        let sm = KvStateMachine::with_wal(Arc::clone(&wal));

        sm.apply(b"SET key v1").unwrap();
        sm.apply(b"SET key v2").unwrap();
        sm.apply(b"SET key v3").unwrap();

        // Latest value should be v3
        let result = sm.apply(b"GET key").unwrap();
        assert_eq!(result, Bytes::from("v3"));
    }

    drop(wal);

    // After recovery, should have latest value
    let wal2 = create_wal_at_path(wal_path).await;
    let sm2 = KvStateMachine::with_wal(wal2);

    let result = sm2.apply(b"GET key").unwrap();
    assert_eq!(
        result,
        Bytes::from("v3"),
        "Should recover latest value after overwrites"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_log_entries_persistence() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // Create some log entries
    let entries = vec![
        {
            let mut e = Entry::default();
            e.index = 1;
            e.term = 1;
            e.data = Bytes::from(&b"entry1"[..]);
            e
        },
        {
            let mut e = Entry::default();
            e.index = 2;
            e.term = 1;
            e.data = Bytes::from(&b"entry2"[..]);
            e
        },
        {
            let mut e = Entry::default();
            e.index = 3;
            e.term = 2;
            e.data = Bytes::from(&b"entry3"[..]);
            e
        },
    ];

    {
        let storage = WalStorage::new(Arc::clone(&wal));
        storage.append_entries(&entries).await.unwrap();

        // Verify entries are stored
        assert_eq!(storage.first_index().unwrap(), 1);
        assert_eq!(storage.last_index().unwrap(), 3);
    }

    drop(wal);

    // After restart, verify entries recovered
    let wal2 = create_wal_at_path(wal_path).await;
    let storage2 = WalStorage::new(wal2);

    assert_eq!(
        storage2.first_index().unwrap(),
        1,
        "First index should survive"
    );
    assert_eq!(
        storage2.last_index().unwrap(),
        3,
        "Last index should survive"
    );

    // Verify entry contents
    let recovered = storage2
        .entries(1, 4, None, raft::GetEntriesContext::empty(false))
        .unwrap();
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[0].index, 1);
    assert_eq!(recovered[0].data, Bytes::from(&b"entry1"[..]));
    assert_eq!(recovered[2].index, 3);
    assert_eq!(recovered[2].term, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multiple_hard_state_updates_latest_wins() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    {
        let storage = WalStorage::new(Arc::clone(&wal));

        // Simulate term progression
        for term in 1..=5 {
            let mut hs = HardState::default();
            hs.term = term;
            hs.vote = term * 10;
            hs.commit = term * 100;
            storage.set_hard_state(hs);
        }
    }

    drop(wal);

    // Should recover latest state (term 5)
    let wal2 = create_wal_at_path(wal_path).await;
    let storage2 = WalStorage::new(wal2);

    let recovered = storage2.initial_state().unwrap();
    assert_eq!(recovered.hard_state.term, 5, "Should recover latest term");
    assert_eq!(recovered.hard_state.vote, 50, "Should recover latest vote");
    assert_eq!(
        recovered.hard_state.commit, 500,
        "Should recover latest commit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_crash_recovery_full_scenario() {
    // cleanup_wal_files removed - using unique keys per test instead
    // This test simulates a full crash recovery scenario:
    // 1. Node starts, becomes leader
    // 2. Receives some entries
    // 3. Updates hard state
    // 4. Applies to state machine
    // 5. CRASHES
    // 6. Restarts and verifies everything is recovered

    let (wal, wal_path, _temp_dir) = create_test_wal().await;

    // === Phase 1: Normal operation ===
    {
        let storage = WalStorage::new(Arc::clone(&wal));
        let sm = KvStateMachine::with_wal(Arc::clone(&wal));

        // Set initial conf state (cluster of 3 nodes)
        let mut cs = ConfState::default();
        cs.voters = vec![1, 2, 3];
        storage.set_conf_state(cs);

        // Become leader in term 2
        let mut hs = HardState::default();
        hs.term = 2;
        hs.vote = 1; // voted for ourselves
        hs.commit = 0;
        storage.set_hard_state(hs.clone());

        // Append some log entries
        let entries = vec![
            {
                let mut e = Entry::default();
                e.index = 1;
                e.term = 2;
                e.data = Bytes::from(&b"SET x 100"[..]);
                e
            },
            {
                let mut e = Entry::default();
                e.index = 2;
                e.term = 2;
                e.data = Bytes::from(&b"SET y 200"[..]);
                e
            },
        ];
        storage.append_entries(&entries).await.unwrap();

        // Commit and apply entries
        hs.commit = 2;
        storage.set_hard_state(hs);

        sm.apply(b"SET x 100").unwrap();
        sm.apply(b"SET y 200").unwrap();

        // Verify current state
        assert_eq!(sm.apply(b"GET x").unwrap(), Bytes::from("100"));
        assert_eq!(sm.apply(b"GET y").unwrap(), Bytes::from("200"));
    }

    // === CRASH === (drop everything)
    drop(wal);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // === Phase 2: Recovery after crash ===
    {
        let wal2 = create_wal_at_path(wal_path).await;
        let storage2 = WalStorage::new(Arc::clone(&wal2));
        let sm2 = KvStateMachine::with_wal(wal2);

        // Verify hard state recovered
        let initial = storage2.initial_state().unwrap();
        assert_eq!(initial.hard_state.term, 2, "Term should survive crash");
        assert_eq!(initial.hard_state.vote, 1, "Vote should survive crash");
        assert_eq!(initial.hard_state.commit, 2, "Commit should survive crash");

        // Verify conf state recovered
        assert_eq!(
            initial.conf_state.voters,
            vec![1, 2, 3],
            "Voters should survive crash"
        );

        // Verify log entries recovered
        assert_eq!(
            storage2.last_index().unwrap(),
            2,
            "Log entries should survive crash"
        );

        // Verify state machine data recovered
        assert_eq!(
            sm2.apply(b"GET x").unwrap(),
            Bytes::from("100"),
            "State machine data should survive crash"
        );
        assert_eq!(
            sm2.apply(b"GET y").unwrap(),
            Bytes::from("200"),
            "State machine data should survive crash"
        );

        // Node can continue operations after crash
        sm2.apply(b"SET z 300").unwrap();
        assert_eq!(sm2.apply(b"GET z").unwrap(), Bytes::from("300"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_empty_state_machine_no_crash() {
    // cleanup_wal_files removed - using unique keys per test instead
    // Verify that creating a state machine without prior data doesn't crash
    let (wal, _wal_path, _temp_dir) = create_test_wal().await;

    let sm = KvStateMachine::with_wal(wal);

    // Should handle GET on empty state
    let result = sm.apply(b"GET nonexistent").unwrap();
    assert_eq!(result, Bytes::from("NOT_FOUND"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_no_prior_hard_state_defaults_to_zero() {
    // cleanup_wal_files removed - using unique keys per test instead
    let (wal, _wal_path, _temp_dir) = create_test_wal().await;

    let storage = WalStorage::new(wal);

    let initial = storage.initial_state().unwrap();
    assert_eq!(initial.hard_state.term, 0);
    assert_eq!(initial.hard_state.vote, 0);
    assert_eq!(initial.hard_state.commit, 0);
}
