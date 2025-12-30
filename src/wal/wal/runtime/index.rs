use crate::wal::wal::paths::WalPathManager;
use crate::wal::wal::vfs as fs;
use crate::invariants::sim_assert;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
pub struct BlockPos {
    pub cur_block_idx: u64,
    pub cur_block_offset: u64,
}

pub struct WalIndex {
    store: HashMap<String, BlockPos>,
    path: String,
}

impl WalIndex {
    pub fn new(file_name: &str) -> std::io::Result<Self> {
        let paths = WalPathManager::default();
        Self::new_in(&paths, file_name)
    }

    pub(super) fn new_in(paths: &WalPathManager, file_name: &str) -> std::io::Result<Self> {
        paths.ensure_root()?;
        let path = paths.index_path(file_name);
        let store = fs::exists(&path)
            .then(|| fs::read(&path).ok())
            .flatten()
            .and_then(|bytes| {
                if bytes.is_empty() {
                    return None;
                }
                // SAFETY: `bytes` comes from our persisted index file which we control;
                // we only proceed when the file is non-empty and rkyv can interpret it.
                let archived = unsafe { rkyv::archived_root::<HashMap<String, BlockPos>>(&bytes) };
                archived.deserialize(&mut rkyv::Infallible).ok()
            })
            .unwrap_or_default();

        Ok(Self {
            store,
            path: path.to_string_lossy().into_owned(),
        })
    }

    pub fn set(&mut self, key: String, idx: u64, offset: u64) -> std::io::Result<()> {
        sim_assert(!key.is_empty(), "wal index set with empty key");
        self.store.insert(
            key,
            BlockPos {
                cur_block_idx: idx,
                cur_block_offset: offset,
            },
        );
        self.persist()
    }

    pub fn get(&self, key: &str) -> Option<&BlockPos> {
        self.store.get(key)
    }

    pub fn remove(&mut self, key: &str) -> std::io::Result<Option<BlockPos>> {
        let result = self.store.remove(key);
        if result.is_some() {
            self.persist()?;
        }
        Ok(result)
    }

    fn persist(&self) -> std::io::Result<()> {
        let tmp_path = format!("{}.tmp", self.path);
        let bytes = rkyv::to_bytes::<_, 256>(&self.store).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("index serialize failed: {:?}", e),
            )
        })?;

        fs::write(&tmp_path, &bytes)?;
        fs::File::open(&tmp_path)?.sync_all()?;
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }
}
