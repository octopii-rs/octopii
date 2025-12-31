use super::allocator::BlockStateTracker;
use super::reader::ColReaderInfo;
use super::{ReadConsistency, Walrus};
use crate::wal::wal::block::{Block, Entry, Metadata};
use crate::wal::wal::config::{
    checksum64, debug_print, is_io_uring_enabled, ENTRY_TRAILER_MAGIC, ENTRY_TRAILER_SIZE,
    MAX_BATCH_ENTRIES, PREFIX_META_SIZE,
};
use crate::invariants::sim_assert;
use std::io;
use std::sync::{Arc, RwLock};

use rkyv::{AlignedVec, Deserialize};

#[cfg(target_os = "linux")]
use io_uring;

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

impl Walrus {
    pub fn read_next(&self, col_name: &str, checkpoint: bool) -> io::Result<Option<Entry>> {
        const TAIL_FLAG: u64 = 1u64 << 63;

        // Debug: unconditional logging for orders to trace the issue
        #[cfg(feature = "simulation")]
        if col_name == "orders" {
            if let Ok(map) = self.reader.data.read() {
                if let Some(info_arc) = map.get(col_name) {
                    if let Ok(info) = info_arc.read() {
                        debug_print!(
                            "[reader] READ_START orders: chain_len={} cur_idx={} cur_off={}",
                            info.chain.len(), info.cur_block_idx, info.cur_block_offset
                        );
                    }
                } else {
                    debug_print!("[reader] READ_START orders: NOT IN MAP!");
                }
            }
        }

        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            arc
        } else {
            let mut map = self.reader.data.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col_name.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };
        let mut info = info_arc
            .write()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "col info write lock poisoned"))?;
        debug_print!(
            "[reader] read_next start: col={}, chain_len={}, idx={}, offset={}",
            col_name,
            info.chain.len(),
            info.cur_block_idx,
            info.cur_block_offset
        );

        // Load persisted position (supports tail sentinel)
        // (block_id, within_block_offset, file_path, file_offset)
        let mut persisted_tail: Option<(u64, u64, Option<String>, Option<u64>)> = None;
        if !info.hydrated_from_index {
            if let Ok(idx_guard) = self.read_offset_index.read() {
                if let Some(pos) = idx_guard.get(col_name) {
                    if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                        let tail_block_id = pos.cur_block_idx & (!TAIL_FLAG);
                        persisted_tail = Some((
                            tail_block_id,
                            pos.cur_block_offset,
                            pos.file_path.clone(),
                            pos.file_offset,
                        ));
                        // sealed state is considered caught up
                        info.cur_block_idx = info.chain.len();
                        info.cur_block_offset = 0;
                    } else {
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                    }
                    info.hydrated_from_index = true;
                } else {
                    // No persisted state present; mark hydrated to avoid re-checking every call
                    info.hydrated_from_index = true;
                }
            }
        }

        // If we have a persisted tail and some sealed blocks were recovered, fold into the last block
        if let Some((tail_block_id, tail_off, file_path_opt, file_offset_opt)) = persisted_tail {
            if !info.chain.is_empty() {
                // Try to match by file position first (stable across recovery),
                // then fall back to block_id (may not match after recovery)
                let found = if let (Some(ref fp), Some(fo)) = (&file_path_opt, file_offset_opt) {
                    info.chain
                        .iter()
                        .enumerate()
                        .find(|(_, b)| &b.file_path == fp && b.offset == fo)
                } else {
                    info.chain
                        .iter()
                        .enumerate()
                        .find(|(_, b)| b.id == tail_block_id)
                };

                if let Some((idx, block)) = found {
                    let used = block.used;
                    info.cur_block_idx = idx;
                    info.cur_block_offset = tail_off.min(used);
                } else {
                    // Block not found - start from beginning
                    info.cur_block_idx = 0;
                    info.cur_block_offset = 0;
                }
            }
            persisted_tail = None;
        }

        // Important: release the per-column lock; we'll reacquire each iteration
        drop(info);

        loop {
            // Reacquire column lock at the start of each iteration
            let mut info = info_arc.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
            })?;

            // Debug: track read position for orders topic when chain_len >= 5
            #[cfg(feature = "simulation")]
            if col_name == "orders" && info.chain.len() >= 5 {
                debug_print!(
                    "[reader] ORDERS_READ: cur_block_idx={} chain_len={} cur_block_offset={}",
                    info.cur_block_idx, info.chain.len(), info.cur_block_offset
                );
                if info.cur_block_idx < info.chain.len() {
                    let blk = &info.chain[info.cur_block_idx];
                    debug_print!(
                        "[reader] ORDERS_READ: chain[{}] block_id={} used={}",
                        info.cur_block_idx, blk.id, blk.used
                    );
                } else {
                    debug_print!(
                        "[reader] ORDERS_READ: PAST CHAIN! cur_block_idx {} >= chain_len {}",
                        info.cur_block_idx, info.chain.len()
                    );
                }
            }

            // Sealed chain path
            if info.cur_block_idx < info.chain.len() {
                let idx = info.cur_block_idx;
                let off = info.cur_block_offset;
                let block = info.chain[idx].clone();
                sim_assert(off <= block.used, "sealed read offset beyond block.used");

                // Detailed debug for orders topic
                #[cfg(feature = "simulation")]
                if col_name == "orders" && idx >= 9 {
                    debug_print!(
                        "[reader] DETAIL: orders chain[{}] block_id={} off={} used={} chain_len={}",
                        idx, block.id, off, block.used, info.chain.len()
                    );
                }

                if off >= block.used {
                    debug_print!(
                        "[reader] read_next: advance block col={}, block_id={}, offset={}, used={}",
                        col_name,
                        block.id,
                        off,
                        block.used
                    );
                    BlockStateTracker::set_checkpointed_true(block.id as usize);
                    info.cur_block_idx += 1;
                    info.cur_block_offset = 0;
                    continue;
                }

                match block.read(off) {
                    Ok((entry, consumed)) => {
                        // Compute new offset and decide whether to commit progress
                        let new_off = off + consumed as u64;
                        sim_assert(consumed > 0, "sealed read consumed zero bytes");
                        sim_assert(new_off >= off, "sealed read cursor regressed");
                        sim_assert(new_off <= block.used, "sealed read advanced past block.used");
                        let mut maybe_persist = None;
                        if checkpoint {
                            info.cur_block_offset = new_off;
                            maybe_persist = if self.should_persist(&mut info, false) {
                                Some((info.cur_block_idx as u64, new_off))
                            } else {
                                None
                            };
                        }

                        // Drop the column lock before touching the index to avoid lock inversion
                        drop(info);
                        if checkpoint {
                            if let Some((idx_val, off_val)) = maybe_persist {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(col_name.to_string(), idx_val, off_val, None, None);
                                }
                            }
                        }

                        debug_print!(
                            "[reader] read_next: OK col={}, block_id={}, consumed={}, new_offset={}",
                            col_name,
                            block.id,
                            consumed,
                            new_off
                        );
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!(
                            "[reader] read_next: read error col={}, block_id={}, offset={}",
                            col_name,
                            block.id,
                            off
                        );
                        return Ok(None);
                    }
                }
            }

            // Tail path
            let tail_snapshot = (info.tail_block_id, info.tail_offset);
            drop(info);

            let writer_arc = {
                let map = self.writers.read().map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "writers read lock poisoned")
                })?;
                match map.get(col_name) {
                    Some(w) => w.clone(),
                    None => return Ok(None),
                }
            };
            let (active_block, written) = writer_arc.snapshot_block()?;

            // If persisted tail points to a different block and that block is now sealed in chain, fold it
            // Reacquire column lock for folding/rebasing decisions
            let mut info = info_arc.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
            })?;
            if let Some((tail_block_id, tail_off, ref file_path_opt, file_offset_opt)) = persisted_tail {
                if tail_block_id != active_block.id {
                    // Try to find the block by file position (stable) or block_id
                    let found = if let (Some(ref fp), Some(fo)) = (file_path_opt, file_offset_opt) {
                        info.chain
                            .iter()
                            .enumerate()
                            .find(|(_, b)| &b.file_path == fp && b.offset == fo)
                    } else {
                        info.chain
                            .iter()
                            .enumerate()
                            .find(|(_, b)| b.id == tail_block_id)
                    };

                    if let Some((idx, _)) = found {
                        info.cur_block_idx = idx;
                        info.cur_block_offset = tail_off.min(info.chain[idx].used);
                        if checkpoint {
                            if self.should_persist(&mut info, true) {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(
                                        col_name.to_string(),
                                        info.cur_block_idx as u64,
                                        info.cur_block_offset,
                                        None,
                                        None,
                                    );
                                }
                            }
                        }
                        persisted_tail = None; // sealed now
                        drop(info);
                        continue;
                    } else {
                        // rebase tail to current active block at 0
                        persisted_tail = Some((
                            active_block.id,
                            0,
                            Some(active_block.file_path.clone()),
                            Some(active_block.offset),
                        ));
                        if checkpoint {
                            if self.should_persist(&mut info, true) {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(
                                        col_name.to_string(),
                                        active_block.id | TAIL_FLAG,
                                        0,
                                        Some(active_block.file_path.clone()),
                                        Some(active_block.offset),
                                    );
                                }
                            }
                        }
                    }
                }
            } else {
                // No persisted tail; init at current active block start
                persisted_tail = Some((
                    active_block.id,
                    0,
                    Some(active_block.file_path.clone()),
                    Some(active_block.offset),
                ));
                if checkpoint {
                    if self.should_persist(&mut info, true) {
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(
                                col_name.to_string(),
                                active_block.id | TAIL_FLAG,
                                0,
                                Some(active_block.file_path.clone()),
                                Some(active_block.offset),
                            );
                        }
                    }
                }
            }
            drop(info);

            // Choose the best known tail offset: prefer in-memory snapshot for current active block
            let (tail_block_id, mut tail_off) = match &persisted_tail {
                Some((blk_id, off, _, _)) => (*blk_id, *off),
                None => return Ok(None),
            };
            if tail_block_id == active_block.id {
                let (snap_id, snap_off) = tail_snapshot;
                if snap_id == active_block.id {
                    tail_off = tail_off.max(snap_off);
                }
            } else {
                // If writer rotated and persisted tail points elsewhere, loop above will fold/rebase
            }
            // If writer rotated after we set persisted_tail, loop to fold/rebase
            if tail_block_id != active_block.id {
                // Loop to next iteration; `info` will be reacquired at loop top
                continue;
            }

            if tail_off < written {
                match active_block.read(tail_off) {
                    Ok((entry, consumed)) => {
                        let new_off = tail_off + consumed as u64;
                        sim_assert(consumed > 0, "tail read consumed zero bytes");
                        sim_assert(new_off >= tail_off, "tail read cursor regressed");
                        sim_assert(new_off <= written, "tail read advanced past written");
                        // Reacquire column lock to update in-memory progress, then decide persistence
                        let mut info = info_arc.write().map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
                        })?;
                        let mut maybe_persist = None;
                        if checkpoint {
                            info.tail_block_id = active_block.id;
                            info.tail_offset = new_off;
                            maybe_persist = if self.should_persist(&mut info, false) {
                                Some((
                                    tail_block_id | TAIL_FLAG,
                                    new_off,
                                    active_block.file_path.clone(),
                                    active_block.offset,
                                ))
                            } else {
                                None
                            };
                        }
                        drop(info);
                        if checkpoint {
                            if let Some((idx_val, off_val, file_path, file_offset)) = maybe_persist {
                                if let Ok(mut idx_guard) = self.read_offset_index.write() {
                                    let _ = idx_guard.set(
                                        col_name.to_string(),
                                        idx_val,
                                        off_val,
                                        Some(file_path),
                                        Some(file_offset),
                                    );
                                }
                            }
                        }

                        debug_print!(
                            "[reader] read_next: tail OK col={}, block_id={}, consumed={}, new_tail_off={}",
                            col_name,
                            active_block.id,
                            consumed,
                            new_off
                        );
                        return Ok(Some(entry));
                    }
                    Err(_) => {
                        debug_print!(
                            "[reader] read_next: tail read error col={}, block_id={}, offset={}",
                            col_name,
                            active_block.id,
                            tail_off
                        );
                        return Ok(None);
                    }
                }
            } else {
                sim_assert(
                    tail_off <= written,
                    "tail read offset beyond written but not caught by planner",
                );
                debug_print!(
                    "[reader] read_next: tail caught up col={}, block_id={}, off={}, written={}",
                    col_name,
                    active_block.id,
                    tail_off,
                    written
                );
                return Ok(None);
            }
        }
    }

    fn should_persist(&self, info: &mut ColReaderInfo, force: bool) -> bool {
        match self.read_consistency {
            ReadConsistency::StrictlyAtOnce => true,
            ReadConsistency::AtLeastOnce { persist_every } => {
                let every = persist_every.max(1);
                if force {
                    info.reads_since_persist = 0;
                    return true;
                }
                let next = info.reads_since_persist.saturating_add(1);
                if next >= every {
                    info.reads_since_persist = 0;
                    true
                } else {
                    info.reads_since_persist = next;
                    false
                }
            }
        }
    }

    pub fn batch_read_for_topic(
        &self,
        col_name: &str,
        max_bytes: usize,
        checkpoint: bool,
    ) -> io::Result<Vec<Entry>> {
        // Debug: unconditional logging for orders to trace the issue
        #[cfg(feature = "simulation")]
        if col_name == "orders" {
            if let Ok(map) = self.reader.data.read() {
                if let Some(info_arc) = map.get(col_name) {
                    if let Ok(info) = info_arc.read() {
                        debug_print!(
                            "[reader] BATCH_START orders: chain_len={} cur_idx={} cur_off={}",
                            info.chain.len(), info.cur_block_idx, info.cur_block_offset
                        );
                        // Log each block's info when chain_len >= 10
                        if info.chain.len() >= 10 {
                            for (i, blk) in info.chain.iter().enumerate() {
                                debug_print!(
                                    "[reader] BATCH_CHAIN[{}]: block_id={} used={}",
                                    i, blk.id, blk.used
                                );
                            }
                        }
                    }
                } else {
                    debug_print!("[reader] BATCH_START orders: NOT IN MAP!");
                }
            }
        }

        // Helper struct for read planning
        struct ReadPlan {
            blk: Block,
            start: u64,
            end: u64,
            is_tail: bool,
            chain_idx: Option<usize>,
        }

        const TAIL_FLAG: u64 = 1u64 << 63;

        // Pre-snapshot active writer state to avoid lock-order inversion later
        let writer_snapshot: Option<(Block, u64)> = {
            let map = self
                .writers
                .read()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "writers read lock poisoned"))?;
            match map.get(col_name).cloned() {
                Some(w) => match w.snapshot_block() {
                    Ok(snapshot) => Some(snapshot),
                    Err(_) => None,
                },
                None => None,
            }
        };

        // 1) Get or create reader info
        let info_arc = if let Some(arc) = {
            let map = self.reader.data.read().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map read lock poisoned")
            })?;
            map.get(col_name).cloned()
        } {
            arc
        } else {
            let mut map = self.reader.data.write().map_err(|_| {
                io::Error::new(io::ErrorKind::Other, "reader map write lock poisoned")
            })?;
            map.entry(col_name.to_string())
                .or_insert_with(|| {
                    Arc::new(RwLock::new(ColReaderInfo {
                        chain: Vec::new(),
                        cur_block_idx: 0,
                        cur_block_offset: 0,
                        reads_since_persist: 0,
                        tail_block_id: 0,
                        tail_offset: 0,
                        hydrated_from_index: false,
                    }))
                })
                .clone()
        };

        let mut info = info_arc
            .write()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "col info write lock poisoned"))?;

        // Hydrate from index if needed
        // (block_id, within_block_offset, file_path, file_offset)
        let mut persisted_tail_for_fold: Option<(u64, u64, Option<String>, Option<u64>)> = None;
        if !info.hydrated_from_index {
            if let Ok(idx_guard) = self.read_offset_index.read() {
                if let Some(pos) = idx_guard.get(col_name) {
                    if (pos.cur_block_idx & TAIL_FLAG) != 0 {
                        let tail_block_id = pos.cur_block_idx & (!TAIL_FLAG);
                        info.tail_block_id = tail_block_id;
                        info.tail_offset = pos.cur_block_offset;
                        info.cur_block_idx = info.chain.len();
                        info.cur_block_offset = 0;
                        persisted_tail_for_fold = Some((
                            tail_block_id,
                            pos.cur_block_offset,
                            pos.file_path.clone(),
                            pos.file_offset,
                        ));
                    } else {
                        let mut ib = pos.cur_block_idx as usize;
                        if ib > info.chain.len() {
                            ib = info.chain.len();
                        }
                        info.cur_block_idx = ib;
                        if ib < info.chain.len() {
                            let used = info.chain[ib].used;
                            info.cur_block_offset = pos.cur_block_offset.min(used);
                        } else {
                            info.cur_block_offset = 0;
                        }
                    }

                    info.hydrated_from_index = true;
                } else {
                    info.hydrated_from_index = true;
                }
            }
        }

        // Fold persisted tail into sealed blocks if possible
        if let Some((tail_block_id, tail_off, file_path_opt, file_offset_opt)) = persisted_tail_for_fold {
            // Try to match by file position first (stable across recovery),
            // then fall back to block_id
            let found = if let (Some(ref fp), Some(fo)) = (&file_path_opt, file_offset_opt) {
                info.chain
                    .iter()
                    .enumerate()
                    .find(|(_, b)| &b.file_path == fp && b.offset == fo)
                    .map(|(idx, _)| idx)
            } else {
                info.chain
                    .iter()
                    .enumerate()
                    .find(|(_, b)| b.id == tail_block_id)
                    .map(|(idx, _)| idx)
            };

            if let Some(idx) = found {
                let used = info.chain[idx].used;
                info.cur_block_idx = idx;
                info.cur_block_offset = tail_off.min(used);
            }
        }

        // 2) Build read plan up to byte and entry limits
        let mut plan: Vec<ReadPlan> = Vec::new();
        let mut planned_bytes: usize = 0;
        let chain_len_at_plan = info.chain.len();
        let mut cur_idx = info.cur_block_idx;
        let mut cur_off = info.cur_block_offset;

        while cur_idx < info.chain.len() && planned_bytes < max_bytes {
            let block = info.chain[cur_idx].clone();
            sim_assert(cur_off <= block.used, "batch plan offset beyond block.used");
            if cur_off >= block.used {
                BlockStateTracker::set_checkpointed_true(block.id as usize);
                cur_idx += 1;
                cur_off = 0;
                continue;
            }

            let end = block.used.min(cur_off + (max_bytes - planned_bytes) as u64);
            if end > cur_off {
                #[cfg(feature = "simulation")]
                if col_name == "orders" {
                    debug_print!(
                        "[reader] BATCH_PLAN_BUILD: chain[{}] block_id={} start={} end={} used={} planned_bytes={} max_bytes={}",
                        cur_idx, block.id, cur_off, end, block.used, planned_bytes, max_bytes
                    );
                }
                plan.push(ReadPlan {
                    blk: block.clone(),
                    start: cur_off,
                    end,
                    is_tail: false,
                    chain_idx: Some(cur_idx),
                });
                planned_bytes += (end - cur_off) as usize;
            }
            // Only advance to next block if we've fully read this one
            // Otherwise, stay at current block with updated offset
            if end >= block.used {
                cur_idx += 1;
                cur_off = 0;
            } else {
                // Partially read block - don't advance, just update offset
                cur_off = end;
                // Exit loop since we've hit the byte limit
                break;
            }
        }

        // Plan tail if we're at the end of sealed chain
        if cur_idx >= chain_len_at_plan {
            if let Some((active_block, written)) = writer_snapshot.clone() {
                // Use in-memory tail progress if available for this block
                let tail_start = if info.tail_block_id == active_block.id {
                    info.tail_offset
                } else {
                    0
                };
                if tail_start < written {
                    let end = written; // read up to current writer offset
                    plan.push(ReadPlan {
                        blk: active_block.clone(),
                        start: tail_start,
                        end,
                        is_tail: true,
                        chain_idx: None,
                    });
                }
            }
        }

        if plan.is_empty() {
            return Ok(Vec::new());
        }

        // Hold lock across IO/parse for StrictlyAtOnce to avoid duplicate consumption
        let hold_lock_during_io = matches!(self.read_consistency, ReadConsistency::StrictlyAtOnce);
        // Manage the guard explicitly to satisfy the borrow checker
        let mut info_opt = Some(info);
        if !hold_lock_during_io {
            // Release lock for AtLeastOnce before IO
            drop(info_opt.take().unwrap());
        }

        // 3) Read ranges via io_uring (FD backend) or mmap
        #[cfg(target_os = "linux")]
        // Use helper that enforces FD backend in simulation mode
        let buffers = if is_io_uring_enabled() {
            // io_uring path - try to initialize, fall back to mmap if not supported
            let ring_size = (plan.len() + 64).min(4096) as u32;
            let ring = match io_uring::IoUring::new(ring_size) {
                Ok(r) => Some(r),
                Err(_) => {
                    // io_uring not supported, will fall back to mmap path below
                    None
                }
            };

            if let Some(mut ring) = ring {
                // io_uring is available, use it
                let mut temp_buffers: Vec<Vec<u8>> = vec![Vec::new(); plan.len()];
                let mut expected_sizes: Vec<usize> = vec![0; plan.len()];

                for (plan_idx, read_plan) in plan.iter().enumerate() {
                    let size = (read_plan.end - read_plan.start) as usize;
                    expected_sizes[plan_idx] = size;
                    let mut buffer = vec![0u8; size];
                    let file_offset = read_plan.blk.offset + read_plan.start;

                    let fd = if let Some(fd_backend) = read_plan.blk.mmap.storage().as_fd() {
                        io_uring::types::Fd(fd_backend.file().as_raw_fd())
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "batch reads require FD backend when io_uring is enabled",
                        ));
                    };

                    let read_op = io_uring::opcode::Read::new(fd, buffer.as_mut_ptr(), size as u32)
                        .offset(file_offset)
                        .build()
                        .user_data(plan_idx as u64);

                    temp_buffers[plan_idx] = buffer;

                    unsafe {
                        ring.submission().push(&read_op).map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("io_uring push failed: {}", e),
                            )
                        })?;
                    }
                }

                // Submit and wait for all reads
                ring.submit_and_wait(plan.len())?;

                // Process completions and validate read lengths
                for _ in 0..plan.len() {
                    if let Some(cqe) = ring.completion().next() {
                        let plan_idx = cqe.user_data() as usize;
                        let got = cqe.result();
                        if got < 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("io_uring read failed: {}", got),
                            ));
                        }
                        if (got as usize) != expected_sizes[plan_idx] {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                format!(
                                    "short read: got {} bytes, expected {}",
                                    got, expected_sizes[plan_idx]
                                ),
                            ));
                        }
                    }
                }

                temp_buffers
            } else {
                // io_uring not available, fall back to mmap reads
                plan.iter()
                    .map(|read_plan| {
                        let size = (read_plan.end - read_plan.start) as usize;
                        let mut buffer = vec![0u8; size];
                        let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                        read_plan.blk.mmap.read(file_offset, &mut buffer);
                        buffer
                    })
                    .collect()
            }
        } else {
            plan.iter()
                .map(|read_plan| {
                    let size = (read_plan.end - read_plan.start) as usize;
                    let mut buffer = vec![0u8; size];
                    let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                    read_plan.blk.mmap.read(file_offset, &mut buffer);
                    buffer
                })
                .collect()
        };

        #[cfg(not(target_os = "linux"))]
        let buffers: Vec<Vec<u8>> = plan
            .iter()
            .map(|read_plan| {
                let size = (read_plan.end - read_plan.start) as usize;
                let mut buffer = vec![0u8; size];
                let file_offset = (read_plan.blk.offset + read_plan.start) as usize;
                read_plan.blk.mmap.read(file_offset, &mut buffer);
                buffer
            })
            .collect();

        // 4) Parse entries from buffers in plan order
        let mut entries = Vec::new();
        let mut total_data_bytes = 0usize;
        let mut final_block_idx = 0usize;
        let mut final_block_offset = 0u64;
        let mut final_tail_block_id = 0u64;
        let mut final_tail_offset = 0u64;
        let mut final_tail_file_path = String::new();
        let mut final_tail_file_offset = 0u64;
        let mut entries_parsed = 0u32;
        let mut saw_tail = false;

        for (plan_idx, read_plan) in plan.iter().enumerate() {
            if entries.len() >= MAX_BATCH_ENTRIES {
                break;
            }
            let buffer = &buffers[plan_idx];
            let mut buf_offset = 0usize;

            // Header layout constants (must match block.rs)
            const HEADER_CHECKSUM_SIZE: usize = 8;
            const META_LEN_OFFSET: usize = HEADER_CHECKSUM_SIZE;
            const META_DATA_OFFSET: usize = META_LEN_OFFSET + 2;
            const MAX_META_BYTES: usize = PREFIX_META_SIZE - HEADER_CHECKSUM_SIZE - 2;

            while buf_offset < buffer.len() {
                if entries.len() >= MAX_BATCH_ENTRIES {
                    break;
                }
                // Try to read metadata header
                if buf_offset + PREFIX_META_SIZE > buffer.len() {
                    break; // Not enough data for header
                }

                // Step 1: Verify header checksum (detects partial/torn writes)
                let header_start = buf_offset;
                let stored_checksum = u64::from_le_bytes(
                    buffer[header_start..header_start + HEADER_CHECKSUM_SIZE]
                        .try_into()
                        .expect("slice is exactly 8 bytes"),
                );
                let computed_checksum =
                    checksum64(&buffer[header_start + HEADER_CHECKSUM_SIZE..header_start + PREFIX_META_SIZE]);

                if stored_checksum != computed_checksum {
                    // Header corrupted or zeroed - stop parsing this block
                    #[cfg(feature = "simulation")]
                    if col_name == "orders" {
                        debug_print!(
                            "[reader] BATCH_CHECKSUM_FAIL orders: plan_idx={} is_tail={} buf_offset={} entries_so_far={}",
                            plan_idx, read_plan.is_tail, buf_offset, entries.len()
                        );
                    }
                    break;
                }

                // Step 2: Read meta_len from bytes 8-9
                let meta_len = (buffer[buf_offset + META_LEN_OFFSET] as usize)
                    | ((buffer[buf_offset + META_LEN_OFFSET + 1] as usize) << 8);

                if meta_len == 0 || meta_len > MAX_META_BYTES {
                    // Invalid metadata length - stop parsing this block
                    break;
                }

                // Step 3: Deserialize metadata (starts at byte 10)
                let mut aligned = AlignedVec::with_capacity(meta_len);
                aligned.extend_from_slice(
                    &buffer[buf_offset + META_DATA_OFFSET..buf_offset + META_DATA_OFFSET + meta_len],
                );

                // Use safe check_archived_root to validate potentially corrupted data
                let archived = match rkyv::validation::validators::check_archived_root::<Metadata>(
                    &aligned[..],
                ) {
                    Ok(a) => a,
                    Err(_) => break, // Corrupted metadata - stop parsing
                };
                let meta: Metadata = match archived.deserialize(&mut rkyv::Infallible) {
                    Ok(m) => m,
                    Err(_) => break, // Parse error - stop
                };

                let data_size = meta.read_size;
                let entry_consumed = PREFIX_META_SIZE + data_size + ENTRY_TRAILER_SIZE;
                let entry_end = read_plan.start + buf_offset as u64 + entry_consumed as u64;
                sim_assert(
                    entry_end <= read_plan.blk.limit,
                    "batch read entry exceeds block limit",
                );

                // Check if we have enough buffer space for the data
                if buf_offset + entry_consumed > buffer.len() {
                    break; // Incomplete entry
                }

                // Enforce byte budget on payload bytes, but always allow at least one entry.
                let next_total = total_data_bytes
                    .checked_add(data_size)
                    .unwrap_or(usize::MAX);
                if next_total > max_bytes && !entries.is_empty() {
                    break;
                }

                // Verify header checksum (detects partial/torn writes)
                let stored_header = u64::from_le_bytes(
                    buffer[buf_offset..buf_offset + 8]
                        .try_into()
                        .expect("slice is exactly 8 bytes"),
                );
                let computed_header = checksum64(
                    &buffer[buf_offset + 8..buf_offset + PREFIX_META_SIZE],
                );
                if stored_header != computed_header {
                    break;
                }

                // Extract and verify data
                let data_start = buf_offset + PREFIX_META_SIZE;
                let data_end = data_start + data_size;
                let data_slice = &buffer[data_start..data_end];
                sim_assert(data_end <= buffer.len(), "batch read returned truncated payload");

                // Verify checksum; treat mismatches as incomplete tail
                if checksum64(data_slice) != meta.checksum {
                    break;
                }

                // Verify trailer commit marker
                let trailer_start = data_end;
                let trailer_end = trailer_start + ENTRY_TRAILER_SIZE;
                let trailer_slice = &buffer[trailer_start..trailer_end];
                sim_assert(trailer_end <= buffer.len(), "batch read trailer out of bounds");
                let magic = u64::from_le_bytes(
                    trailer_slice[0..8]
                        .try_into()
                        .expect("slice is exactly 8 bytes"),
                );
                let checksum = u64::from_le_bytes(
                    trailer_slice[8..16]
                        .try_into()
                        .expect("slice is exactly 8 bytes"),
                );
                if magic != ENTRY_TRAILER_MAGIC || checksum != meta.checksum {
                    break;
                }

                // Add to results
                entries.push(Entry {
                    data: data_slice.to_vec(),
                });
                total_data_bytes = next_total;
                entries_parsed += 1;

                // Update position tracking
                let in_block_offset = read_plan.start + buf_offset as u64 + entry_consumed as u64;

                if read_plan.is_tail {
                    saw_tail = true;
                    final_tail_block_id = read_plan.blk.id;
                    final_tail_offset = in_block_offset;
                    final_tail_file_path = read_plan.blk.file_path.clone();
                    final_tail_file_offset = read_plan.blk.offset;
                } else if let Some(idx) = read_plan.chain_idx {
                    final_block_idx = idx;
                    final_block_offset = in_block_offset;
                }

                buf_offset += entry_consumed;
            }

            // Log entries parsed from this plan block
            #[cfg(feature = "simulation")]
            if col_name == "orders" && plan.len() >= 10 {
                debug_print!(
                    "[reader] BATCH_PLAN[{}]: is_tail={} block_id={} entries_total_now={}",
                    plan_idx, read_plan.is_tail, read_plan.blk.id, entries.len()
                );
            }
        }

        // 5) Commit progress (optional)
        if entries_parsed > 0 {
            enum PersistTarget {
                Tail {
                    blk_id: u64,
                    off: u64,
                    file_path: String,
                    file_offset: u64,
                },
                Sealed { idx: u64, off: u64 },
                None,
            }
            let mut target = PersistTarget::None;

            if hold_lock_during_io {
                // We still hold the original write guard here
                let mut info = info_opt.take().expect("column lock should be held");
                if checkpoint {
                    let prev_idx = info.cur_block_idx;
                    let prev_off = info.cur_block_offset;
                    if saw_tail {
                        info.cur_block_idx = chain_len_at_plan;
                        info.cur_block_offset = 0;
                        info.tail_block_id = final_tail_block_id;
                        info.tail_offset = final_tail_offset;
                        sim_assert(info.cur_block_idx >= prev_idx, "tail batch read regressed block index");
                        if let Some((_, written)) = writer_snapshot {
                            sim_assert(final_tail_offset <= written, "tail batch read advanced past written");
                        }
                        target = PersistTarget::Tail {
                            blk_id: final_tail_block_id,
                            off: final_tail_offset,
                            file_path: final_tail_file_path.clone(),
                            file_offset: final_tail_file_offset,
                        };
                    } else {
                        info.cur_block_idx = final_block_idx;
                        info.cur_block_offset = final_block_offset;
                        sim_assert(
                            info.cur_block_idx > prev_idx
                                || info.cur_block_offset >= prev_off,
                            "sealed batch read regressed cursor",
                        );
                        if final_block_idx < info.chain.len() {
                            sim_assert(
                                final_block_offset <= info.chain[final_block_idx].used,
                                "sealed batch read advanced past block.used",
                            );
                        }
                        target = PersistTarget::Sealed {
                            idx: final_block_idx as u64,
                            off: final_block_offset,
                        };
                    }
                }
                drop(info);
            } else {
                // Reacquire to update
                let mut info2 = info_arc.write().map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "col info write lock poisoned")
                })?;
                if checkpoint {
                    let prev_idx = info2.cur_block_idx;
                    let prev_off = info2.cur_block_offset;
                    if saw_tail {
                        info2.cur_block_idx = chain_len_at_plan;
                        info2.cur_block_offset = 0;
                        info2.tail_block_id = final_tail_block_id;
                        info2.tail_offset = final_tail_offset;
                        sim_assert(info2.cur_block_idx >= prev_idx, "tail batch read regressed block index");
                        if let Some((_, written)) = writer_snapshot {
                            sim_assert(final_tail_offset <= written, "tail batch read advanced past written");
                        }
                        if let ReadConsistency::AtLeastOnce { persist_every } =
                            self.read_consistency
                        {
                            // Clamp contribution so a single call can't reach the threshold
                            let room = persist_every
                                .saturating_sub(1)
                                .saturating_sub(info2.reads_since_persist);
                            let add = entries_parsed.min(room);
                            info2.reads_since_persist =
                                info2.reads_since_persist.saturating_add(add);
                            // target remains None here to avoid persisting to end in one batch
                        }
                    } else {
                        info2.cur_block_idx = final_block_idx;
                        info2.cur_block_offset = final_block_offset;
                        sim_assert(
                            info2.cur_block_idx > prev_idx
                                || info2.cur_block_offset >= prev_off,
                            "sealed batch read regressed cursor",
                        );
                        if final_block_idx < info2.chain.len() {
                            sim_assert(
                                final_block_offset <= info2.chain[final_block_idx].used,
                                "sealed batch read advanced past block.used",
                            );
                        }
                        if let ReadConsistency::AtLeastOnce { persist_every } =
                            self.read_consistency
                        {
                            let room = persist_every
                                .saturating_sub(1)
                                .saturating_sub(info2.reads_since_persist);
                            let add = entries_parsed.min(room);
                            info2.reads_since_persist =
                                info2.reads_since_persist.saturating_add(add);
                        }
                    }
                }
                drop(info2);
            }

            if checkpoint {
                match target {
                    PersistTarget::Tail {
                        blk_id,
                        off,
                        file_path,
                        file_offset,
                    } => {
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(
                                col_name.to_string(),
                                blk_id | TAIL_FLAG,
                                off,
                                Some(file_path),
                                Some(file_offset),
                            );
                        }
                    }
                    PersistTarget::Sealed { idx, off } => {
                        if let Ok(mut idx_guard) = self.read_offset_index.write() {
                            let _ = idx_guard.set(col_name.to_string(), idx, off, None, None);
                        }
                    }
                    PersistTarget::None => {}
                }
            }
        }

        Ok(entries)
    }
}
