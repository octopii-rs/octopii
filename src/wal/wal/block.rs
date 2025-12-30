use crate::wal::wal::config::{
    checksum64, debug_print, ENTRY_TRAILER_MAGIC, ENTRY_TRAILER_SIZE, PREFIX_META_SIZE,
};
use crate::invariants::sim_assert;
use crate::wal::wal::storage::SharedMmap;
use rkyv::{Archive, Deserialize, Serialize};
use std::sync::Arc;

/// Size of the header checksum field (8 bytes for u64)
const HEADER_CHECKSUM_SIZE: usize = 8;
/// Offset where metadata length is stored (after header checksum)
const META_LEN_OFFSET: usize = HEADER_CHECKSUM_SIZE;
/// Size of the metadata length field (2 bytes for u16)
const META_LEN_SIZE: usize = 2;
/// Offset where actual metadata bytes start
const META_DATA_OFFSET: usize = META_LEN_OFFSET + META_LEN_SIZE;
/// Maximum size for serialized metadata (PREFIX_META_SIZE - checksum - length)
const MAX_META_BYTES: usize = PREFIX_META_SIZE - HEADER_CHECKSUM_SIZE - META_LEN_SIZE;
#[derive(Clone, Debug)]
pub struct Entry {
    pub data: Vec<u8>,
}

#[derive(Archive, Deserialize, Serialize, Debug)]
#[archive_attr(derive(bytecheck::CheckBytes))]
pub(crate) struct Metadata {
    pub(crate) read_size: usize,
    pub(crate) owned_by: String,
    pub(crate) next_block_start: u64,
    pub(crate) checksum: u64,
}

#[derive(Clone, Debug)]
pub struct Block {
    pub(crate) id: u64,
    pub(crate) file_path: String,
    pub(crate) offset: u64,
    pub(crate) limit: u64,
    pub(crate) mmap: Arc<SharedMmap>,
    pub(crate) used: u64,
}

impl Block {
    pub(crate) fn write(
        &self,
        in_block_offset: u64,
        data: &[u8],
        owned_by: &str,
        next_block_start: u64,
    ) -> std::io::Result<()> {
        debug_assert!(
            in_block_offset
                + (data.len() as u64 + PREFIX_META_SIZE as u64 + ENTRY_TRAILER_SIZE as u64)
                <= self.limit
        );

        let checksum = checksum64(data);
        self.write_with_trailer(
            in_block_offset,
            data,
            owned_by,
            next_block_start,
            ENTRY_TRAILER_MAGIC,
            checksum,
            checksum,
        )
    }

    pub(crate) fn write_uncommitted(
        &self,
        in_block_offset: u64,
        data: &[u8],
        owned_by: &str,
        next_block_start: u64,
    ) -> std::io::Result<()> {
        let checksum = checksum64(data);
        self.write_with_trailer(
            in_block_offset,
            data,
            owned_by,
            next_block_start,
            0,
            0,
            checksum,
        )
    }

    fn write_with_trailer(
        &self,
        in_block_offset: u64,
        data: &[u8],
        owned_by: &str,
        next_block_start: u64,
        trailer_magic: u64,
        trailer_checksum: u64,
        payload_checksum: u64,
    ) -> std::io::Result<()> {
        debug_assert!(
            in_block_offset
                + (data.len() as u64 + PREFIX_META_SIZE as u64 + ENTRY_TRAILER_SIZE as u64)
                <= self.limit
        );

        let new_meta = Metadata {
            read_size: data.len(),
            owned_by: owned_by.to_string(),
            next_block_start,
            checksum: payload_checksum,
        };

        let meta_bytes = rkyv::to_bytes::<_, 256>(&new_meta).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("serialize metadata failed: {:?}", e),
            )
        })?;
        if meta_bytes.len() > MAX_META_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "metadata too large",
            ));
        }

        let mut meta_buffer = vec![0u8; PREFIX_META_SIZE];

        // Store metadata length at offset 8 (after header checksum slot)
        meta_buffer[META_LEN_OFFSET] = (meta_bytes.len() & 0xFF) as u8;
        meta_buffer[META_LEN_OFFSET + 1] = ((meta_bytes.len() >> 8) & 0xFF) as u8;

        // Copy actual metadata starting at offset 10
        meta_buffer[META_DATA_OFFSET..META_DATA_OFFSET + meta_bytes.len()]
            .copy_from_slice(&meta_bytes);

        // Compute header checksum over bytes [8..PREFIX_META_SIZE]
        // This covers: length field + metadata + padding
        let header_checksum = checksum64(&meta_buffer[HEADER_CHECKSUM_SIZE..]);

        // Store header checksum in first 8 bytes (little endian)
        meta_buffer[0..8].copy_from_slice(&header_checksum.to_le_bytes());

        // Combine and write (header + payload + trailer)
        let mut combined = Vec::with_capacity(PREFIX_META_SIZE + data.len() + ENTRY_TRAILER_SIZE);
        combined.extend_from_slice(&meta_buffer);
        combined.extend_from_slice(data);
        combined.extend_from_slice(&trailer_magic.to_le_bytes());
        combined.extend_from_slice(&trailer_checksum.to_le_bytes());

        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &combined)?;
        Ok(())
    }

    pub(crate) fn write_trailer(
        &self,
        in_block_offset: u64,
        data_len: usize,
        checksum: u64,
    ) -> std::io::Result<()> {
        let trailer_offset =
            self.offset + in_block_offset + PREFIX_META_SIZE as u64 + data_len as u64;
        let mut trailer_buf = [0u8; ENTRY_TRAILER_SIZE];
        trailer_buf[0..8].copy_from_slice(&ENTRY_TRAILER_MAGIC.to_le_bytes());
        trailer_buf[8..16].copy_from_slice(&checksum.to_le_bytes());
        self.mmap.write(trailer_offset as usize, &trailer_buf)?;
        Ok(())
    }

    pub(crate) fn invalidate_entry(
        &self,
        in_block_offset: u64,
        data_len: usize,
    ) -> std::io::Result<()> {
        sim_assert(
            in_block_offset
                + (PREFIX_META_SIZE + data_len + ENTRY_TRAILER_SIZE) as u64
                <= self.limit,
            "invalidate_entry out of bounds",
        );
        let header_offset = self.offset + in_block_offset;
        let mut zero_header = [0u8; HEADER_CHECKSUM_SIZE];
        self.mmap.write(header_offset as usize, &zero_header)?;

        let trailer_offset =
            self.offset + in_block_offset + PREFIX_META_SIZE as u64 + data_len as u64;
        let zero_trailer = [0u8; ENTRY_TRAILER_SIZE];
        self.mmap.write(trailer_offset as usize, &zero_trailer)?;
        Ok(())
    }

    pub(crate) fn read(&self, in_block_offset: u64) -> std::io::Result<(Entry, usize)> {
        let mut meta_buffer = vec![0; PREFIX_META_SIZE];
        let file_offset = self.offset + in_block_offset;
        self.mmap.read(file_offset as usize, &mut meta_buffer)?;

        // Step 1: Verify header checksum FIRST (detects partial/torn writes)
        let stored_checksum = u64::from_le_bytes(
            meta_buffer[0..HEADER_CHECKSUM_SIZE]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );
        let computed_checksum = checksum64(&meta_buffer[HEADER_CHECKSUM_SIZE..]);

        if stored_checksum != computed_checksum {
            // Header is corrupted - likely a partial write
            debug_print!(
                "[reader] header checksum mismatch at offset={} in file={}, block_id={} (stored={:#x}, computed={:#x})",
                in_block_offset,
                self.file_path,
                self.id,
                stored_checksum,
                computed_checksum
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "header checksum mismatch, partial write detected",
            ));
        }

        // Step 2: Read metadata length (now at offset 8)
        let meta_len = (meta_buffer[META_LEN_OFFSET] as usize)
            | ((meta_buffer[META_LEN_OFFSET + 1] as usize) << 8);

        if meta_len == 0 || meta_len > MAX_META_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid metadata length: {}", meta_len),
            ));
        }

        // Step 3: Deserialize metadata (starts at offset 10)
        let mut aligned = rkyv::AlignedVec::with_capacity(meta_len);
        aligned.extend_from_slice(&meta_buffer[META_DATA_OFFSET..META_DATA_OFFSET + meta_len]);

        // Use check_archived_root to safely validate potentially corrupted data
        let archived =
            rkyv::validation::validators::check_archived_root::<Metadata>(&aligned[..]).map_err(
                |e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("corrupted metadata archive: {}", e),
                    )
                },
            )?;
        let meta: Metadata = archived.deserialize(&mut rkyv::Infallible).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to deserialize metadata",
            )
        })?;
        let actual_entry_size = meta.read_size;
        let total_size = PREFIX_META_SIZE + actual_entry_size + ENTRY_TRAILER_SIZE;
        sim_assert(
            in_block_offset + total_size as u64 <= self.limit,
            "entry exceeds block limit",
        );

        // Step 4: Read the payload data
        let new_offset = file_offset + PREFIX_META_SIZE as u64;
        let mut ret_buffer = vec![0; actual_entry_size];
        self.mmap.read(new_offset as usize, &mut ret_buffer)?;

        // Step 5: Verify payload checksum
        let expected = meta.checksum;
        if checksum64(&ret_buffer) != expected {
            debug_print!(
                "[reader] payload checksum mismatch at offset={} in file={}, block_id={}",
                in_block_offset,
                self.file_path,
                self.id
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "payload checksum mismatch, data corruption detected",
            ));
        }

        // Step 6: Verify trailer commit marker
        let trailer_offset = new_offset + actual_entry_size as u64;
        let mut trailer_buf = [0u8; ENTRY_TRAILER_SIZE];
        self.mmap
            .read(trailer_offset as usize, &mut trailer_buf)?;
        let magic = u64::from_le_bytes(
            trailer_buf[0..8]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );
        let checksum = u64::from_le_bytes(
            trailer_buf[8..16]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );
        if magic != ENTRY_TRAILER_MAGIC || checksum != expected {
            debug_print!(
                "[reader] trailer mismatch at offset={} in file={}, block_id={}",
                in_block_offset,
                self.file_path,
                self.id
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "trailer mismatch, incomplete write detected",
            ));
        }

        let consumed = PREFIX_META_SIZE + actual_entry_size + ENTRY_TRAILER_SIZE;
        Ok((Entry { data: ret_buffer }, consumed))
    }

    pub(crate) fn zero_range(&self, in_block_offset: u64, size: u64) -> std::io::Result<()> {
        // Zero a small region within this block; used to invalidate headers on rollback
        // Caller ensures size is reasonable (typically PREFIX_META_SIZE)
        let len = size as usize;
        if len == 0 {
            return Ok(());
        }
        let zeros = vec![0u8; len];
        let file_offset = self.offset + in_block_offset;
        self.mmap.write(file_offset as usize, &zeros)?;
        Ok(())
    }
}
