use crate::commands::request::CommandRequest;
use crate::db_entry::DbValue;
use crate::storage_config::StorageConfig;
use crate::wal::errors::WalWriterError;
use crate::wal::mode::WalMode;
use futures::AsyncWriteExt;
use glommio_ng::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use std::path::Path;
use std::rc::Rc;

pub struct WalWriter {
    dma_writer: DmaStreamWriter,
    storage_config: Rc<StorageConfig>,
}

impl WalWriter {
    pub async fn init(
        path: &Path,
        wal_mode: Rc<WalMode>,
        storage_config: Rc<StorageConfig>,
    ) -> Result<WalWriter, WalWriterError> {
        let max_batch_size = match wal_mode.as_ref() {
            WalMode::Strict { max_batch_size, .. } | WalMode::Relaxed { max_batch_size, .. } => {
                *max_batch_size as u32
            }
        };
        let avg_entry_size = storage_config.avg_command_size_hint;

        let buffer_size = max_batch_size * avg_entry_size;
        let writes_behind = max_batch_size.max(64); // at least one full batch

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .dma_open(path)
            .await
            .map_err(|e| WalWriterError::OpenFailed(e.into()))?;

        let dma_writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(buffer_size as usize)
            .with_write_behind(writes_behind as usize)
            .build();

        Ok(Self {
            dma_writer,
            storage_config,
        })
    }

    pub async fn append(&mut self, request: &CommandRequest) -> Result<(), WalWriterError> {
        match request {
            CommandRequest::Get(_) => panic!("WAL only stores mutating operations"),
            CommandRequest::Set(cmd) => {
                self.dma_writer
                    .write_all(&0u8.to_le_bytes())
                    .await
                    .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Command type - set - 0

                self.dma_writer
                    .write_all(&(cmd.key.len() as u32).to_le_bytes())
                    .await
                    .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Key length

                self.dma_writer
                    .write_all(&cmd.key)
                    .await
                    .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Key

                match &cmd.value {
                    DbValue::Value(bytes) => {
                        self.dma_writer
                            .write_all(&(bytes.len() as u32).to_le_bytes())
                            .await
                            .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Value len

                        self.dma_writer
                            .write_all(bytes)
                            .await
                            .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Value
                    }
                    DbValue::Tombstone => panic!(
                        "SET cannot be used by clients to write Tombstone. Server-side validation/processing failed somewhere"
                    ),
                }
            }
            CommandRequest::Remove(cmd) => {
                self.dma_writer
                    .write_all(&1u8.to_le_bytes())
                    .await
                    .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Command type - remove - 1

                self.dma_writer
                    .write_all(&(cmd.key.len() as u32).to_le_bytes())
                    .await
                    .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Key length

                self.dma_writer
                    .write_all(&cmd.key)
                    .await
                    .map_err(|e| WalWriterError::WriteFailed(e.into()))?; // Key
            }
        };

        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), WalWriterError> {
        self.dma_writer
            .sync()
            .await
            .map_err(|e| WalWriterError::DurabilityError(e.into()))?;

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<(), WalWriterError> {
        self.dma_writer
            .close()
            .await
            .map_err(WalWriterError::DurabilityError)?;

        Ok(())
    }
}
