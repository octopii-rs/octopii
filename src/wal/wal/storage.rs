use crate::wal::wal::config::{is_fd_backend_enabled, FsyncSchedule};
use crate::wal::wal::vfs::{now, File, OpenOptions};
use memmap2::MmapMut;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(not(feature = "simulation"))]
use std::sync::RwLock;

#[cfg(not(feature = "simulation"))]
use std::sync::OnceLock;

#[cfg(feature = "simulation")]
use std::cell::{Cell, RefCell};

#[derive(Debug)]
pub(crate) struct FdBackend {
    file: File,
    len: usize,
}

impl FdBackend {
    fn new(path: &str, use_o_sync: bool) -> std::io::Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);

        #[cfg(unix)]
        if use_o_sync {
            // vfs::OpenOptions has custom_flags on unix
            opts.custom_flags(libc::O_SYNC);
        }

        let file = opts.open(path)?;
        let len = file.len()? as usize;

        Ok(Self { file, len })
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        // pwrite doesn't move the file cursor
        let written = self.file.write_at(data, offset as u64)?;
        if written != data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("partial write: {} of {} bytes", written, data.len()),
            ));
        }
        Ok(())
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) -> std::io::Result<()> {
        use std::os::unix::fs::FileExt;
        // pread doesn't move the file cursor
        let read_bytes = self.file.read_at(dest, offset as u64)?;
        if read_bytes != dest.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("partial read: {} of {} bytes", read_bytes, dest.len()),
            ));
        }
        Ok(())
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }
}

#[derive(Debug)]
pub(crate) enum StorageImpl {
    Mmap(MmapMut),
    Fd(FdBackend),
}

impl StorageImpl {
    pub(crate) fn write(&self, offset: usize, data: &[u8]) -> std::io::Result<()> {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset <= mmap.len());
                debug_assert!(mmap.len() - offset >= data.len());
                unsafe {
                    let ptr = mmap.as_ptr() as *mut u8;
                    std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(offset), data.len());
                }
                Ok(())
            }
            StorageImpl::Fd(fd) => fd.write(offset, data),
        }
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) -> std::io::Result<()> {
        match self {
            StorageImpl::Mmap(mmap) => {
                debug_assert!(offset + dest.len() <= mmap.len());
                let src = &mmap[offset..offset + dest.len()];
                dest.copy_from_slice(src);
                Ok(())
            }
            StorageImpl::Fd(fd) => fd.read(offset, dest),
        }
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        match self {
            StorageImpl::Mmap(mmap) => mmap.flush(),
            StorageImpl::Fd(fd) => fd.flush(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            StorageImpl::Mmap(mmap) => mmap.len(),
            StorageImpl::Fd(fd) => fd.len(),
        }
    }

    pub(crate) fn as_fd(&self) -> Option<&FdBackend> {
        if let StorageImpl::Fd(fd) = self {
            Some(fd)
        } else {
            None
        }
    }
}

#[cfg(not(feature = "simulation"))]
static GLOBAL_FSYNC_SCHEDULE: OnceLock<FsyncSchedule> = OnceLock::new();

#[cfg(feature = "simulation")]
thread_local! {
    static GLOBAL_FSYNC_SCHEDULE: Cell<Option<FsyncSchedule>> = Cell::new(None);
}

fn should_use_o_sync() -> bool {
    #[cfg(feature = "simulation")]
    {
        GLOBAL_FSYNC_SCHEDULE
            .with(|s| s.get())
            .map(|s| matches!(s, FsyncSchedule::SyncEach))
            .unwrap_or(false)
    }
    #[cfg(not(feature = "simulation"))]
    {
        GLOBAL_FSYNC_SCHEDULE
            .get()
            .map(|s| matches!(s, FsyncSchedule::SyncEach))
            .unwrap_or(false)
    }
}

fn create_storage_impl(path: &str) -> std::io::Result<StorageImpl> {
    // Use helper that enforces FD backend in simulation mode
    if is_fd_backend_enabled() {
        let use_o_sync = should_use_o_sync();
        Ok(StorageImpl::Fd(FdBackend::new(path, use_o_sync)?))
    } else {
        // This path is unreachable in simulation mode due to is_fd_backend_enabled check
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        // SAFETY: `file` is opened read/write and lives for the duration of this
        // mapping; `memmap2` upholds aliasing invariants for `MmapMut`.
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(StorageImpl::Mmap(mmap))
    }
}

#[derive(Debug)]
pub(crate) struct SharedMmap {
    storage: StorageImpl,
    last_touched_at: AtomicU64,
}

// SAFETY: `SharedMmap` provides interior mutability only via methods that
// enforce bounds and perform atomic timestamp updates; the underlying
// storage supports concurrent reads and explicit flushes.
unsafe impl Sync for SharedMmap {}
// SAFETY: The struct holds storage that is safe to move between threads;
// timestamps are atomics, so sending is sound.
unsafe impl Send for SharedMmap {}

impl SharedMmap {
    pub(crate) fn new(path: &str) -> std::io::Result<Arc<Self>> {
        let storage = create_storage_impl(path)?;

        // Use vfs::now() for deterministic time in simulation mode
        let now_ms = now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        Ok(Arc::new(Self {
            storage,
            last_touched_at: AtomicU64::new(now_ms),
        }))
    }

    pub(crate) fn write(&self, offset: usize, data: &[u8]) -> std::io::Result<()> {
        // Bounds check before raw copy to maintain memory safety
        debug_assert!(offset <= self.storage.len());
        debug_assert!(self.storage.len() - offset >= data.len());

        self.storage.write(offset, data)?;

        // Use vfs::now() for deterministic time in simulation mode
        let now_ms = now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64;
        self.last_touched_at.store(now_ms, Ordering::Relaxed);
        Ok(())
    }

    pub(crate) fn read(&self, offset: usize, dest: &mut [u8]) -> std::io::Result<()> {
        debug_assert!(offset + dest.len() <= self.storage.len());
        self.storage.read(offset, dest)
    }

    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.storage.flush()
    }

    pub(crate) fn storage(&self) -> &StorageImpl {
        &self.storage
    }
}

pub(crate) struct SharedMmapKeeper {
    data: HashMap<String, Arc<SharedMmap>>,
}

// Global keeper instance - process-wide in production, thread-local in simulation
#[cfg(not(feature = "simulation"))]
static MMAP_KEEPER: OnceLock<RwLock<SharedMmapKeeper>> = OnceLock::new();

#[cfg(feature = "simulation")]
thread_local! {
    static MMAP_KEEPER: RefCell<SharedMmapKeeper> = RefCell::new(SharedMmapKeeper::new());
}

impl SharedMmapKeeper {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    #[cfg(not(feature = "simulation"))]
    fn get_keeper() -> &'static RwLock<SharedMmapKeeper> {
        MMAP_KEEPER.get_or_init(|| RwLock::new(SharedMmapKeeper::new()))
    }

    // Clear all cached storage instances (used for simulation crash testing)
    #[cfg(feature = "simulation")]
    pub(crate) fn clear_all() {
        MMAP_KEEPER.with(|keeper| {
            let mut keeper = keeper.borrow_mut();
            for (_, arc) in keeper.data.iter() {
                let _ = arc.flush();
            }
            keeper.data.clear();
        });
    }

    // Fast path: many readers concurrently (production only)
    #[cfg(not(feature = "simulation"))]
    fn get_mmap_arc_read(path: &str) -> Option<Arc<SharedMmap>> {
        let keeper = Self::get_keeper().read().ok()?;
        keeper.data.get(path).cloned()
    }

    // Read-mostly accessor that escalates to write lock only on miss
    #[cfg(feature = "simulation")]
    pub(crate) fn get_mmap_arc(path: &str) -> std::io::Result<Arc<SharedMmap>> {
        MMAP_KEEPER.with(|keeper| {
            let mut keeper = keeper.borrow_mut();
            if let Some(existing) = keeper.data.get(path) {
                return Ok(existing.clone());
            }
            let arc = SharedMmap::new(path)?;
            keeper.data.insert(path.to_string(), arc.clone());
            Ok(arc)
        })
    }

    #[cfg(not(feature = "simulation"))]
    pub(crate) fn get_mmap_arc(path: &str) -> std::io::Result<Arc<SharedMmap>> {
        if let Some(existing) = Self::get_mmap_arc_read(path) {
            return Ok(existing);
        }

        let keeper_lock = Self::get_keeper();

        // Double-check with a fresh read lock to avoid unnecessary write lock
        {
            let keeper = keeper_lock.read().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper read lock poisoned")
            })?;
            if let Some(existing) = keeper.data.get(path) {
                return Ok(existing.clone());
            }
        }

        let mut keeper = keeper_lock.write().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::Other, "mmap keeper write lock poisoned")
        })?;
        if let Some(existing) = keeper.data.get(path) {
            return Ok(existing.clone());
        }

        let arc = SharedMmap::new(path)?;
        keeper.data.insert(path.to_string(), arc.clone());
        Ok(arc)
    }
}

pub(crate) fn set_fsync_schedule(schedule: FsyncSchedule) {
    #[cfg(feature = "simulation")]
    {
        GLOBAL_FSYNC_SCHEDULE.with(|s| s.set(Some(schedule)));
    }
    #[cfg(not(feature = "simulation"))]
    {
        let _ = GLOBAL_FSYNC_SCHEDULE.set(schedule);
    }
}

pub(crate) fn open_storage_for_path(path: &str) -> std::io::Result<StorageImpl> {
    create_storage_impl(path)
}
