use crate::error::{OctopiiError, Result};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};

/// Content-addressed blob storage. Path: `root/ab/cd/abcd1234...`
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    pub fn new<P: AsRef<Path>>(root: P) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|e| {
            OctopiiError::Transport(format!("failed to create blob store root: {}", e))
        })?;
        Ok(Self { root })
    }

    fn path(&self, hash: &[u8; 32]) -> PathBuf {
        let hex = hash_to_hex(hash);
        self.root
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    pub fn put(&self, data: &[u8]) -> Result<[u8; 32]> {
        let hash: [u8; 32] = Sha256::digest(data).into();
        self.put_with_hash(&hash, data)?;
        Ok(hash)
    }

    pub fn put_with_hash(&self, hash: &[u8; 32], data: &[u8]) -> Result<()> {
        let path = self.path(hash);

        if path.exists() {
            return Ok(());
        }

        let computed: [u8; 32] = Sha256::digest(data).into();
        if &computed != hash {
            return Err(OctopiiError::Transport(
                "hash mismatch in put_with_hash".to_string(),
            ));
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                OctopiiError::Transport(format!("failed to create blob directory: {}", e))
            })?;
        }

        let temp_path = path.with_extension("tmp");
        fs::write(&temp_path, data)
            .map_err(|e| OctopiiError::Transport(format!("failed to write blob: {}", e)))?;
        fs::rename(&temp_path, &path)
            .map_err(|e| OctopiiError::Transport(format!("failed to rename blob: {}", e)))?;

        Ok(())
    }

    pub fn get(&self, hash: &[u8; 32]) -> Result<Vec<u8>> {
        let path = self.path(hash);
        fs::read(&path).map_err(|e| OctopiiError::Transport(format!("failed to read blob: {}", e)))
    }

    pub fn delete(&self, hash: &[u8; 32]) -> Result<bool> {
        let path = self.path(hash);
        if path.exists() {
            fs::remove_file(&path)
                .map_err(|e| OctopiiError::Transport(format!("failed to delete blob: {}", e)))?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn exists(&self, hash: &[u8; 32]) -> bool {
        self.path(hash).exists()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

fn hash_to_hex(hash: &[u8; 32]) -> String {
    const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(64);
    for byte in hash {
        hex.push(HEX_CHARS[(byte >> 4) as usize] as char);
        hex.push(HEX_CHARS[(byte & 0x0f) as usize] as char);
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_to_hex() {
        let hash = [
            0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x12, 0x34, 0x56, 0x78,
            0x9a, 0xbc, 0xde, 0xf0,
        ];
        assert_eq!(
            hash_to_hex(&hash),
            "abcdef012345678900112233445566778899aabbccddeeff123456789abcdef0"
        );
    }

    #[test]
    fn test_blob_store_put_get_delete() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::new(dir.path()).unwrap();

        let data = b"hello world";
        let hash = store.put(data).unwrap();

        assert!(store.exists(&hash));

        let retrieved = store.get(&hash).unwrap();
        assert_eq!(retrieved, data);

        assert!(store.delete(&hash).unwrap());
        assert!(!store.exists(&hash));
        assert!(!store.delete(&hash).unwrap()); // Already deleted
    }

    #[test]
    fn test_blob_store_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::new(dir.path()).unwrap();

        let data = b"duplicate content";
        let hash1 = store.put(data).unwrap();
        let hash2 = store.put(data).unwrap();

        assert_eq!(hash1, hash2);
        assert!(store.exists(&hash1));
    }

    #[test]
    fn test_blob_store_sharded_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::new(dir.path()).unwrap();

        let data = b"test data for sharding";
        let hash = store.put(data).unwrap();

        let path = store.path(&hash);
        let hex = hash_to_hex(&hash);

        // Check path structure: root/ab/cd/abcd...
        assert!(path.starts_with(dir.path()));
        assert!(path.to_string_lossy().contains(&hex[0..2]));
        assert!(path.to_string_lossy().contains(&hex[2..4]));
        assert!(path.ends_with(&hex));
    }

    #[test]
    fn test_blob_store_put_with_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::new(dir.path()).unwrap();

        let data = b"some data";
        let wrong_hash = [0u8; 32]; // All zeros - won't match

        let result = store.put_with_hash(&wrong_hash, data);
        assert!(result.is_err());
    }
}
