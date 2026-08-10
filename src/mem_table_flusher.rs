use crate::entry::{DbEntry, DbValue};
use crate::mem_table::MemTable;
use crate::ss_table_metadata::{SsTableId, SsTableMetadata};
use crate::storage_config::StorageConfig;
use futures::AsyncWriteExt;
use glommio::GlommioError;
use glommio::io::{Directory, DmaFile, DmaStreamWriterBuilder};
use std::cell::Cell;
use std::path::PathBuf;
use std::sync::Arc;

pub struct MemTableFlusher {
    next_id: Cell<SsTableId>,
    cpu_shard_id: u32,
    storage_config: Arc<StorageConfig>,
    dir: PathBuf,
    parent: Directory,
}

impl MemTableFlusher {
    pub async fn new(
        dir: PathBuf,
        cpu_shard_id: u32,
        storage_config: Arc<StorageConfig>,
    ) -> Result<MemTableFlusher, GlommioError<()>> {
        let parent = Directory::open(&dir).await?;
        Ok(Self {
            next_id: Cell::new(0), // TODO: read from disk
            cpu_shard_id,
            storage_config,
            dir,
            parent,
        })
    }

    pub fn db_entry_bytes_size(entry: &DbEntry) -> u32 {
        // Key length in bytes + key bytes + value type in bytes (0 = value, or 1 = tombstone)
        size_of::<u32>() as u32
            + entry.key.len() as u32
            + size_of::<u8>() as u32
            + match &entry.value {
                DbValue::Value(bytes) => {
                    // + value length in bytes + value in bytes
                    size_of::<u32>() as u32 + bytes.len() as u32
                }
                DbValue::Tombstone => 0,
            }
    }

    pub async fn flush<MT: MemTable>(
        &self,
        mem_table: MT,
    ) -> Result<SsTableMetadata, GlommioError<()>> {
        // ATOMICITY: we're not flushing to temp file, but we won't list this ss table in manifest unless completely saved
        let id = self.next_id();
        let path = self
            .dir
            .join(format!("ss_table_{}_{}", self.cpu_shard_id, id));
        let file = DmaFile::create(&path).await?;

        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(self.storage_config.ss_table_flush_buffer_size_bytes)
            .with_write_behind(self.storage_config.ss_table_flush_buffer_writes_behind)
            .build();

        // entry count
        writer
            .write_all(&(mem_table.entry_count() as u32).to_le_bytes())
            .await?;

        for (key, value) in mem_table.iter() {
            writer.write_all(&(key.len() as u32).to_le_bytes()).await?; // Key length
            writer.write_all(key).await?; // Key

            match value {
                DbValue::Value(bytes) => {
                    writer.write_all(&[0u8]).await?; // Value type
                    writer
                        .write_all(&(bytes.len() as u32).to_le_bytes())
                        .await?; // Value length in bytes
                    writer.write_all(bytes).await?; // Value
                }
                DbValue::Tombstone => {
                    writer.write_all(&[1u8]).await?;
                }
            }
        }

        // Flush buffers to the OS/Storage Controller
        // DURABILITY: As well, it does sync, so forces the storage drive to flush its hardware cache.
        writer.close().await?;

        // DURABILITY: the DIRECTORY ENTRY for this new file is separate
        // metadata from the file's data — needs its own sync. Without this,
        // a crash could leave the manifest (appended to right after this
        // function returns) referencing a file that isn't actually durably
        // discoverable on disk.
        self.parent.sync().await?;

        Ok(SsTableMetadata {
            id,
            entry_count: mem_table.entry_count(),
            level: 0,
            min_key: mem_table.min_key().clone(),
            max_key: mem_table.max_key().clone(),
        })
    }

    fn next_id(&self) -> SsTableId {
        let next_free = self.next_id.get();
        self.next_id.set(next_free + 1);
        next_free
    }
}
