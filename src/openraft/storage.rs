#![cfg(feature = "openraft")]

use crate::openraft::types::{AppEntry, AppResponse, AppSnapshot};
use crate::wal::WriteAheadLog;
use std::sync::Arc;
use bytes::Bytes;

/// Minimal, in-memory storage to get OpenRaft running for tests.
///
/// TODO(wal): Replace with a WAL-backed adapter that persists logs, votes,
/// membership and snapshots via existing Walrus APIs.
#[derive(Clone, Default)]
pub struct MemStorage {
	entries: Arc<tokio::sync::RwLock<Vec<AppEntry>>>,
	snapshot: Arc<tokio::sync::RwLock<AppSnapshot>>,
}

impl MemStorage {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn append(&self, e: AppEntry) {
		let mut g = self.entries.blocking_write();
		g.push(e);
	}

	pub fn snapshot(&self) -> AppSnapshot {
		self.snapshot.blocking_read().clone()
	}

	pub fn install_snapshot(&self, snap: AppSnapshot) {
		*self.snapshot.blocking_write() = snap;
	}
}

/// WAL-backed storage adapter skeleton.
///
/// TODO(wal): Implement OpenRaft RaftStorage over Walrus. Keep this type here
/// to outline the integration path; not used until we flip from MemStorage.
#[allow(dead_code)]
pub struct WalStorageAdapter {
	_wal: Arc<WriteAheadLog>,
}

#[allow(dead_code)]
impl WalStorageAdapter {
	pub fn new(wal: Arc<WriteAheadLog>) -> Self {
		Self { _wal: wal }
	}

	/// Append a serialized log entry to WAL.
	pub async fn append(&self, data: Vec<u8>) -> crate::error::Result<()> {
		// Prefix with "log:" to distinguish from metadata records
		let mut rec = Vec::with_capacity(4 + data.len());
		rec.extend_from_slice(b"log:");
		rec.extend_from_slice(&data);
		self._wal.append(Bytes::from(rec)).await?;
		Ok(())
	}

	/// Flush pending WAL writes.
	pub async fn flush(&self) -> crate::error::Result<()> {
		self._wal.flush().await?;
		Ok(())
	}

	/// Read back all WAL entries (testing helper).
	pub async fn read_all(&self) -> crate::error::Result<Vec<bytes::Bytes>> {
		self._wal.read_all().await
	}

	/// Persist vote/membership metadata snapshots by writing a small record.
	pub async fn persist_metadata(&self, key: &str, data: Vec<u8>) -> crate::error::Result<()> {
		// Prefix the record to distinguish metadata logically.
		let mut rec = Vec::with_capacity(8 + data.len());
		rec.extend_from_slice(key.as_bytes());
		rec.extend_from_slice(&data);
		self._wal.append(Bytes::from(rec)).await?;
		self._wal.flush().await?;
		Ok(())
	}

	/// Persist a snapshot image into WAL snapshot topic through state machine serializer.
	pub async fn install_snapshot(&self, snapshot: AppSnapshot) -> crate::error::Result<()> {
		// Leverage existing WalStorage snapshot application path by writing a record;
		// actual OpenRaft path will stream chunks; this is a minimal placeholder.
		self._wal.append(Bytes::from(snapshot)).await?;
		self._wal.flush().await?;
		Ok(())
	}

	/// Read last persisted vote (term, voted_for) if any.
	pub async fn read_vote(&self) -> crate::error::Result<Option<(u64, u64)>> {
		let all = self._wal.read_all().await?;
		let mut last: Option<(u64, u64)> = None;
		for rec in all {
			if rec.len() >= 5 && &rec[..5] == b"vote:" {
				// decode two u64 via bincode
				if let Ok((t, v)) = bincode::deserialize::<(u64, u64)>(&rec[5..]) {
					last = Some((t, v));
				}
			}
		}
		Ok(last)
	}

	/// Save vote (term, voted_for).
	pub async fn save_vote(&self, term: u64, voted_for: u64) -> crate::error::Result<()> {
		let bytes = bincode::serialize(&(term, voted_for))
			.map_err(|e| crate::error::OctopiiError::Wal(format!("vote encode: {e}")))?;
		self.persist_metadata("vote:", bytes).await
	}

	/// Read last persisted membership (raw bytes) if any.
	pub async fn read_membership(&self) -> crate::error::Result<Option<Vec<u8>>> {
		let all = self._wal.read_all().await?;
		let mut last: Option<Vec<u8>> = None;
		for rec in all {
			if rec.len() >= 11 && &rec[..11] == b"membership:" {
				last = Some(rec[11..].to_vec());
			}
		}
		Ok(last)
	}

	/// Save membership (raw bytes).
	pub async fn save_membership(&self, membership_bytes: Vec<u8>) -> crate::error::Result<()> {
		self.persist_metadata("membership:", membership_bytes).await
	}

	/// Read back all log entries (raw payloads) in order.
	pub async fn read_log_entries(&self) -> crate::error::Result<Vec<Vec<u8>>> {
		let all = self._wal.read_all().await?;
		let mut entries = Vec::new();
		for rec in all {
			if rec.len() >= 4 && &rec[..4] == b"log:" {
				entries.push(rec[4..].to_vec());
			}
		}
		Ok(entries)
	}
}

