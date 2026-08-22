use crate::compaction::trigger::CompactionTrigger;
use crate::manifest::handler::ManifestHandler;
use crate::mem_table::mem_table::MemTable;
use crate::ss_table::id_generator::SsTableIdGenerator;
use crate::ss_table::metadata::SsTableMetadata;
use crate::ss_table::writer::SsTableWriter;
use crate::storage_config::StorageConfig;
use glommio::GlommioError;
use glommio::io::{Directory, DmaFile};
use std::path::PathBuf;
use std::rc::Rc;

pub struct MemTableFlusher {
    cpu_shard_id: u32,
    storage_config: Rc<StorageConfig>,
    dir: PathBuf,
    manifest: ManifestHandler,
    compaction_trigger: CompactionTrigger,
    parent: Directory,
    id_generator: Rc<SsTableIdGenerator>,
}

impl MemTableFlusher {
    pub async fn new(
        dir: PathBuf,
        cpu_shard_id: u32,
        storage_config: Rc<StorageConfig>,
        manifest: ManifestHandler,
        compaction_trigger: CompactionTrigger,
        id_generator: Rc<SsTableIdGenerator>,
    ) -> Result<MemTableFlusher, GlommioError<()>> {
        let parent = Directory::open(&dir).await?;
        Ok(Self {
            cpu_shard_id,
            storage_config,
            dir,
            manifest,
            compaction_trigger,
            parent,
            id_generator,
        })
    }

    pub async fn flush<MT: MemTable>(&mut self, mem_table: MT) -> Result<(), GlommioError<()>> {
        // ATOMICITY: we're not flushing to temp file, but we won't list this ss table in manifest unless completely saved
        let id = self.id_generator.next_id();
        let path = self
            .dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, id));
        let file = DmaFile::create(&path).await?;

        let mut writer = SsTableWriter::init(file, 0, Rc::clone(&self.storage_config)).await?;

        for (key, value) in mem_table.iter() {
            writer.write_entry(key, value).await?;
        }

        let bytes_written = writer.bytes_written();

        // DURABILITY: flush to disk then update dir metadata
        writer.finish().await?;
        self.parent.sync().await?;

        // AFTER durable save, make ss table reachable
        self.manifest
            .add_ss_table(SsTableMetadata {
                id,
                entry_count: mem_table.entry_count(),
                size_bytes: bytes_written,
                level: 0,
                min_key: mem_table.min_key().clone(),
                max_key: mem_table.max_key().clone(),
            })
            .await?;

        self.compaction_trigger.notify();

        Ok(())
    }
}
