use crate::manifest::entry::ManifestEntry;
use crate::ss_table_metadata::{SsTableId, SsTableLevel, SsTableMetadata};
use glommio::GlommioError;
use glommio::io::{BufferedFile, Directory};
use std::collections::{BTreeSet, HashMap};
use std::io::ErrorKind;
use std::path::Path;
use std::rc::Rc;

pub(super) struct ManifestInternal {
    cpu_shard_id: u32,
    by_id: HashMap<SsTableId, Rc<SsTableMetadata>>,
    by_level: HashMap<SsTableLevel, BTreeSet<Rc<SsTableMetadata>>>,
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
            Err(GlommioError::IoError(e)) if e.kind() == ErrorKind::NotFound => {
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
        Self::memory_add_ss_table(&mut self.by_id, &mut self.by_level, rc);

        Ok(())
    }

    pub(super) async fn remove_ss_table(&mut self, id: SsTableId) -> Result<(), GlommioError<()>> {
        self.append(ManifestEntry::RemoveSsTable(id)).await?;
        Self::memory_remove_ss_table(&mut self.by_id, &mut self.by_level, id);

        Ok(())
    }

    async fn open(dir: &Path, cpu_shard_id: u32) -> Result<Self, GlommioError<()>> {
        let path = dir.join(format!("{}_{}", Self::FILE_NAME, cpu_shard_id));
        let file = BufferedFile::open(&path).await?;
        let file_size = file.file_size().await? as usize;
        let result = file.read_at(0, file_size).await?;

        let mut by_id: HashMap<SsTableId, Rc<SsTableMetadata>> = HashMap::new();
        let mut by_level: HashMap<SsTableLevel, BTreeSet<Rc<SsTableMetadata>>> = HashMap::new();
        let mut pos = 0usize;

        while let Some((entry, consumed)) = ManifestEntry::decode(&result[pos..]) {
            match entry {
                ManifestEntry::AddSsTable(metadata) => {
                    Self::memory_add_ss_table(&mut by_id, &mut by_level, metadata)
                }
                ManifestEntry::RemoveSsTable(id) => {
                    Self::memory_remove_ss_table(&mut by_id, &mut by_level, id)
                }
            }

            pos += consumed as usize;
        }

        Ok(Self {
            cpu_shard_id,
            by_id,
            by_level,
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

    fn memory_add_ss_table(
        by_id: &mut HashMap<SsTableId, Rc<SsTableMetadata>>,
        by_level: &mut HashMap<SsTableLevel, BTreeSet<Rc<SsTableMetadata>>>,
        metadata: Rc<SsTableMetadata>,
    ) {
        let level = metadata.level;

        by_id.insert(metadata.id, Rc::clone(&metadata));
        by_level
            .entry(level)
            .or_insert_with(BTreeSet::new)
            .insert(metadata);
    }

    fn memory_remove_ss_table(
        by_id: &mut HashMap<SsTableId, Rc<SsTableMetadata>>,
        by_level: &mut HashMap<SsTableLevel, BTreeSet<Rc<SsTableMetadata>>>,
        id: SsTableId,
    ) {
        if let Some(table) = by_id.remove(&id) {
            if let Some(set) = by_level.get_mut(&table.level) {
                set.remove(&table);
            }
        }
    }
}
