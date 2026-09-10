use crate::db_entry::{DbKey, DbValue};
use crate::ss_table::errors::SsTableWriteError;
use crate::ss_table::metadata::SsTableLevel;
use crate::storage_config::StorageConfig;
use futures::AsyncWriteExt;
use glommio_ng::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder};
use std::rc::Rc;

pub struct SsTableWriter {
    dma_writer: DmaStreamWriter,
    storage_config: Rc<StorageConfig>,
}

impl SsTableWriter {
    pub async fn init(
        file: DmaFile,
        level: SsTableLevel,
        storage_config: Rc<StorageConfig>,
    ) -> Result<Self, SsTableWriteError> {
        let mut dma_writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(storage_config.ss_table_flush_buffer_size_bytes)
            .with_write_behind(storage_config.ss_table_flush_buffer_writes_behind)
            .build();

        dma_writer
            .write_all(&level.to_le_bytes())
            .await
            .map_err(SsTableWriteError::InitFailed)?;
        Ok(Self {
            dma_writer,
            storage_config,
        })
    }

    pub async fn write_entry(
        &mut self,
        key: &DbKey,
        value: &DbValue,
    ) -> Result<(), SsTableWriteError> {
        self.dma_writer
            .write_all(&(key.len() as u32).to_le_bytes())
            .await
            .map_err(SsTableWriteError::WriteFailed)?; // Key length
        self.dma_writer
            .write_all(&key)
            .await
            .map_err(SsTableWriteError::WriteFailed)?; // Key

        match value {
            DbValue::Value(bytes) => {
                self.dma_writer
                    .write_all(&[0u8])
                    .await
                    .map_err(SsTableWriteError::WriteFailed)?; // Value type
                self.dma_writer
                    .write_all(&(bytes.len() as u32).to_le_bytes())
                    .await
                    .map_err(SsTableWriteError::WriteFailed)?; // Value length in bytes
                self.dma_writer
                    .write_all(bytes)
                    .await
                    .map_err(SsTableWriteError::WriteFailed)?; // Value
            }
            DbValue::Tombstone => {
                self.dma_writer
                    .write_all(&[1u8])
                    .await
                    .map_err(SsTableWriteError::WriteFailed)?;
            }
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<(), SsTableWriteError> {
        // Flush buffers to the OS/Storage Controller
        // DURABILITY: As well, it does sync, so forces the storage drive to flush its hardware cache.
        self.dma_writer
            .close()
            .await
            .map_err(SsTableWriteError::DurabilityError)?;
        Ok(())
    }

    pub fn db_entry_bytes_size(key: &DbKey, value: &DbValue) -> u32 {
        // Key length in bytes + key bytes + value type in bytes (0 = value, or 1 = tombstone)
        size_of::<u32>() as u32
            + key.len() as u32
            + size_of::<u8>() as u32
            + match value {
                DbValue::Value(bytes) => {
                    // + value length in bytes + value in bytes
                    size_of::<u32>() as u32 + bytes.len() as u32
                }
                DbValue::Tombstone => 0,
            }
    }

    pub fn bytes_written(&self) -> u64 {
        self.dma_writer.current_pos()
    }
}
