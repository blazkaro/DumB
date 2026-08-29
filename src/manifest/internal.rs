use crate::manifest::entry::ManifestEntry;
use crate::manifest::snapshot::ManifestSnapshot;
use crate::ss_table::metadata::{SsTableId, SsTableLevel, SsTableMetadata};
use glommio_ng::GlommioError;
use glommio_ng::io::{BufferedFile, Directory};
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
    ) -> Result<Self, GlommioError<()>> {
        match Self::open(dir, cpu_shard_id).await {
            Ok(manifest) => Ok(manifest),
            Err(GlommioError::EnhancedIoError { source, .. })
                if source.kind() == ErrorKind::NotFound =>
            {
                Self::create(dir, cpu_shard_id).await
            }
            Err(e) => Err(e),
        }
    }

    pub(super) async fn add_ss_table(
        &mut self,
        metadata: SsTableMetadata,
    ) -> Result<(), GlommioError<()>> {
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

    pub(super) async fn remove_ss_table(&mut self, id: SsTableId) -> Result<(), GlommioError<()>> {
        self.append(ManifestEntry::RemoveSsTable(id)).await?;
        Self::memory_remove_ss_table(
            &mut self.by_id,
            &mut self.by_level,
            &mut self.level_size_bytes,
            id,
        );

        Ok(())
    }

    // Compaction isn't just set of add and remove - they all need to be done in batch, all or none at once (fulfill ACID
    // That is why this method exists
    pub(super) async fn apply_compaction(
        &mut self,
        new_ss_tables: Vec<SsTableMetadata>,
        old_ss_table_ids: Vec<SsTableId>,
    ) -> Result<(), GlommioError<()>> {
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

    async fn open(dir: &Path, cpu_shard_id: u32) -> Result<Self, GlommioError<()>> {
        let path = dir.join(format!("{}_{}", Self::FILE_NAME, cpu_shard_id));
        let file = BufferedFile::open(&path).await?;
        let file_size = file.file_size().await? as usize;
        let result = file.read_at(0, file_size).await?;

        let mut by_id: HashMap<SsTableId, Rc<SsTableMetadata>> = HashMap::new();
        let mut by_level: HashMap<SsTableLevel, Rc<BTreeSet<Rc<SsTableMetadata>>>> = HashMap::new();
        let mut level_size_bytes: HashMap<SsTableLevel, u64> = HashMap::new();
        let mut pos = 0usize;

        while let Some((entry, consumed)) = ManifestEntry::decode(&result[pos..]) {
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

            pos += consumed as usize;
        }

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

    async fn create(dir: &Path, cpu_shard_id: u32) -> Result<Self, GlommioError<()>> {
        let path = dir.join(format!("{}_{}", Self::FILE_NAME, cpu_shard_id));

        // DURABILITY: Flush to disk
        let file = BufferedFile::create(&path).await?;
        file.fdatasync().await?;

        // DURABILITY: Sync dir metadata
        let parent = Directory::open(&dir).await?;
        parent.sync().await?;

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

    async fn append(&mut self, entry: ManifestEntry) -> Result<(), GlommioError<()>> {
        let mut buffer = std::mem::take(&mut self.entry_serialization_buffer);
        buffer.clear();
        entry.encode(&mut buffer);

        let next_capacity = buffer.capacity();
        let len = buffer.len() as u64;
        let pos = self.write_pos;

        self.write_pos += len;
        self.entry_serialization_buffer = Vec::with_capacity(next_capacity);
        self.file.write_at(buffer, pos).await?; // buffer is gone

        // DURABILITY: flush to disk
        self.file.fdatasync().await?;

        Ok(())
    }

    async fn append_batch(&mut self, entries: Vec<ManifestEntry>) -> Result<(), GlommioError<()>> {
        let mut buffer =
            Vec::with_capacity(entries.len() * self.entry_serialization_buffer.capacity());

        for entry in &entries {
            entry.encode(&mut buffer);
        }

        let len = buffer.len() as u64;
        let pos = self.write_pos;

        self.write_pos += len;
        self.file.write_at(buffer, pos).await?;

        // ATOMICITY, DURABILITY: flush batch at once
        self.file.fdatasync().await?;

        Ok(())
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
