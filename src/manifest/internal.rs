use crate::le_reader::LeReader;
use crate::manifest::entry::ManifestEntry;
use crate::manifest::errors::{ManifestOpenError, ManifestWriteError};
use crate::manifest::snapshot::ManifestSnapshot;
use crate::ss_table::metadata::{SsTableId, SsTableLevel, SsTableMetadata};
use glommio_ng::io::{BufferedFile, Directory, OpenOptions};
use std::collections::{BTreeSet, HashMap};
use std::io::ErrorKind;
use std::path::Path;
use std::rc::Rc;

pub(super) struct ManifestInternal {
    cpu_shard_id: u32,
    by_id: HashMap<SsTableId, Rc<SsTableMetadata>>,
    // Although it's not intuitive, due to use of snapshot the BTreeSet must be wrapped by Rc for best performance.
    // Let's say that N is amount of copies performed for snapshots (without Rc over BTreeSet, quite expensive, but mutating costs nothing).
    // Let's say X = number of copies (single, mutated level copy, NOT WHOLE BTreeSet!) performed for inserts and Y is analogical, but for removals.
    // Because these copies (for inserts/removals, mutating) are going to be done only when necessary (old snapshot is referenced somewhere)
    // We can conclude that X + Y <= N * (number of levels).
    // So wrapping it in Rc<> always procudes better or at least the same performance as not doing it.
    by_level: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
    level_size_bytes: HashMap<SsTableLevel, u64>,
    file: BufferedFile,
    write_pos: u64,
    entry_serialization_buffer: Vec<u8>,
}

impl ManifestInternal {
    const FILE_NAME: &'static str = "MANIFEST";

    pub(super) async fn open_or_create(
        dir: &Path,
        cpu_shard_id: u32,
    ) -> Result<Self, ManifestOpenError> {
        match Self::open(dir, cpu_shard_id).await {
            Ok(manifest) => Ok(manifest),
            Err(ManifestOpenError::Open(src)) if src.kind() == ErrorKind::NotFound => {
                Self::create(dir, cpu_shard_id).await
            }
            Err(err) => Err(err),
        }
    }

    pub(super) async fn add_ss_table(
        &mut self,
        metadata: SsTableMetadata,
    ) -> Result<(), ManifestWriteError> {
        let rc = Rc::new(metadata);

        self.append(ManifestEntry::AddSsTable(rc.clone())).await?;
        Self::memory_add_ss_table(
            &mut self.by_id,
            &mut self.by_level,
            &mut self.level_size_bytes,
            rc,
        );

        Ok(())
    }

    pub(super) async fn remove_ss_table(
        &mut self,
        id: SsTableId,
    ) -> Result<(), ManifestWriteError> {
        self.append(ManifestEntry::RemoveSsTable(id)).await?;
        Self::memory_remove_ss_table(
            &mut self.by_id,
            &mut self.by_level,
            &mut self.level_size_bytes,
            id,
        );

        Ok(())
    }

    // Compaction isn't just set of add and remove - they all need to be done in batch, all or none at once (fulfill ACID)
    // That is why this method exists
    pub(super) async fn apply_compaction(
        &mut self,
        new_ss_tables: Vec<SsTableMetadata>,
        old_ss_table_ids: Vec<SsTableId>,
    ) -> Result<(), ManifestWriteError> {
        let to_add: Vec<Rc<SsTableMetadata>> = new_ss_tables.into_iter().map(Rc::new).collect();

        let entries = old_ss_table_ids
            .iter()
            .map(|&id| ManifestEntry::RemoveSsTable(id))
            .chain(to_add.iter().cloned().map(ManifestEntry::AddSsTable))
            .collect();

        self.append_batch(entries).await?;

        // ATOMICITY: No await between - should not cause invalid state (and valid state is also enforced by using snapshot in manifest handler)
        // DURABILITY: Processed AFTER durable append batch
        for id in old_ss_table_ids {
            Self::memory_remove_ss_table(
                &mut self.by_id,
                &mut self.by_level,
                &mut self.level_size_bytes,
                id,
            );
        }

        for metadata in to_add {
            Self::memory_add_ss_table(
                &mut self.by_id,
                &mut self.by_level,
                &mut self.level_size_bytes,
                metadata,
            );
        }

        Ok(())
    }

    pub(super) fn snapshot(&self) -> ManifestSnapshot {
        ManifestSnapshot::new(self.by_level.clone(), self.level_size_bytes.clone())
    }

    async fn open(dir: &Path, cpu_shard_id: u32) -> Result<Self, ManifestOpenError> {
        let path = dir.join(format!("{}_{}", Self::FILE_NAME, cpu_shard_id));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .buffered_open(path)
            .await
            .map_err(|e| ManifestOpenError::Open(e.into()))?;
        let file_size = file
            .file_size()
            .await
            .map_err(|e| ManifestOpenError::Read(e.into()))? as usize;
        let result = file
            .read_at(0, file_size)
            .await
            .map_err(|e| ManifestOpenError::Read(e.into()))?;

        let mut by_id: HashMap<SsTableId, Rc<SsTableMetadata>> = HashMap::new();
        let mut by_level: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>> = HashMap::new();
        let mut level_size_bytes: HashMap<SsTableLevel, u64> = HashMap::new();
        let mut pos = 0usize;

        while let Some((buffer, consumed)) = Self::safe_read_at(&result[pos..])
            && let Some((entry, _)) = ManifestEntry::decode(&buffer)
        {
            match entry {
                ManifestEntry::AddSsTable(metadata) => Self::memory_add_ss_table(
                    &mut by_id,
                    &mut by_level,
                    &mut level_size_bytes,
                    metadata,
                ),
                ManifestEntry::RemoveSsTable(id) => Self::memory_remove_ss_table(
                    &mut by_id,
                    &mut by_level,
                    &mut level_size_bytes,
                    id,
                ),
            }

            pos += consumed;
        }

        // We don't know if manifest was corrupted so we stopped reading, or normal EOF happened.
        // However, we don't need to truncate that manifest. We just maintain write_pos before that corrupted entry, so that future appends overwrite corrupted data.
        // If that overwrite doesn't happen, we repeat the same processs (read till correct, and so on). There is no risk.

        Ok(Self {
            cpu_shard_id,
            by_id,
            by_level,
            level_size_bytes,
            file,
            write_pos: pos as u64,
            entry_serialization_buffer: Vec::new(),
        })
    }

    async fn create(dir: &Path, cpu_shard_id: u32) -> Result<Self, ManifestOpenError> {
        let path = dir.join(format!("{}_{}", Self::FILE_NAME, cpu_shard_id));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .buffered_open(path)
            .await
            .map_err(|e| ManifestOpenError::Create(e.into()))?;

        // DURABILITY: Flush to disk
        file.fdatasync()
            .await
            .map_err(|e| ManifestOpenError::Create(e.into()))?;

        // DURABILITY: Sync dir metadata
        let parent = Directory::open(&dir)
            .await
            .map_err(|e| ManifestOpenError::DirMetadataSync(e.into()))?;
        parent
            .sync()
            .await
            .map_err(|e| ManifestOpenError::DirMetadataSync(e.into()))?;

        let _ = parent.close().await;

        Ok(Self {
            cpu_shard_id,
            by_id: HashMap::new(),
            by_level: HashMap::new(),
            level_size_bytes: HashMap::new(),
            file,
            write_pos: 0,
            entry_serialization_buffer: Vec::new(),
        })
    }

    async fn append(&mut self, entry: ManifestEntry) -> Result<(), ManifestWriteError> {
        let mut buffer = std::mem::take(&mut self.entry_serialization_buffer);
        buffer.clear();
        entry.encode(&mut buffer);

        let next_capacity = buffer.capacity();
        self.entry_serialization_buffer = Vec::with_capacity(next_capacity);

        self.safe_write_at(buffer).await?;

        // DURABILITY: flush to disk
        self.file
            .fdatasync()
            .await
            .map_err(|e| ManifestWriteError::NotDurable(e.into()))?;

        Ok(())
    }

    async fn append_batch(
        &mut self,
        entries: Vec<ManifestEntry>,
    ) -> Result<(), ManifestWriteError> {
        let mut buffer =
            Vec::with_capacity(entries.len() * self.entry_serialization_buffer.capacity());

        for entry in &entries {
            entry.encode(&mut buffer);
        }

        self.safe_write_at(buffer).await?;

        // ATOMICITY, DURABILITY: flush batch at once
        self.file
            .fdatasync()
            .await
            .map_err(|e| ManifestWriteError::NotDurable(e.into()))?;

        Ok(())
    }

    async fn safe_write_at(&mut self, buffer: Vec<u8>) -> Result<(), ManifestWriteError> {
        let pos = self.write_pos;
        let buffer_len = buffer.len();
        let buffer_len_bytes = (buffer_len as u32).to_le_bytes();
        let pre_write_pos = self.write_pos;

        let crc = crc32c::crc32c_append(0u32, &buffer_len_bytes);
        let crc = crc32c::crc32c_append(crc, &buffer);

        // capacity = len (4bytes) + crc size (4 bytes) + buffer len
        let mut complete = Vec::with_capacity(size_of::<u32>() + size_of::<u32>() + buffer_len);
        complete.extend_from_slice(&buffer_len_bytes);
        complete.extend_from_slice(&crc.to_le_bytes());
        complete.extend_from_slice(&buffer);

        let complete_len = complete.len();

        match self.file.write_at(complete, pos).await {
            Ok(bytes_written) => {
                if bytes_written != complete_len {
                    // TODO: short write may be part of normal flow, reconsider this piece of code. For now, it doesn't cause any problems except unnecessary retry
                    // Don't advance write_pos, short write will be overwritten by future appends
                    Err(ManifestWriteError::IncompleteWrite)
                } else {
                    self.write_pos += bytes_written as u64;
                    Ok(())
                }
            }
            Err(e) => Err(ManifestWriteError::WriteFailed(e.into())),
        }
    }

    /// Returns None if it is not safe to read manifest anymore
    fn safe_read_at(buffer: &[u8]) -> Option<(Vec<u8>, usize)> {
        if buffer.len() < 8 {
            // If buffer size is lesser than len + crc size
            return None;
        }

        let mut offset = 0usize;

        let payload_len_bytes = &buffer[0..4];
        offset += size_of::<u32>();
        let payload_len = LeReader::read_u32_le(&payload_len_bytes, 0);

        let crc = LeReader::read_u32_le(buffer, offset);
        offset += size_of::<u32>();

        let payload = buffer.get(offset..offset + payload_len as usize)?;
        offset += payload_len as usize;

        let expected_crc = crc32c::crc32c_append(0u32, payload_len_bytes);
        let expected_crc = crc32c::crc32c_append(expected_crc, payload);

        if crc != expected_crc {
            return None;
        }

        Some((payload.to_vec(), offset))
    }

    fn memory_add_ss_table(
        by_id: &mut HashMap<SsTableId, Rc<SsTableMetadata>>,
        by_level: &mut HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
        level_size: &mut HashMap<SsTableLevel, u64>,
        metadata: Rc<SsTableMetadata>,
    ) {
        let level = metadata.level;

        by_id.insert(metadata.id, Rc::clone(&metadata));
        let set_rc = by_level
            .entry(level)
            .or_insert_with(|| Rc::new(BTreeSet::new()));

        *level_size.entry(level).or_insert(0) += metadata.size_bytes;
        Rc::make_mut(set_rc).insert(metadata);
    }

    fn memory_remove_ss_table(
        by_id: &mut HashMap<SsTableId, Rc<SsTableMetadata>>,
        by_level: &mut HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>>,
        level_size: &mut HashMap<SsTableLevel, u64>,
        id: SsTableId,
    ) {
        if let Some(table) = by_id.remove(&id) {
            if let Some(set_rc) = by_level.get_mut(&table.level) {
                let set = Rc::make_mut(set_rc);
                set.remove(&table);
                *level_size.get_mut(&table.level).unwrap() -= table.size_bytes;
            }
        }
    }
}
