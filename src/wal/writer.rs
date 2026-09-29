use crate::commands::request::CommandRequest;
use crate::db_entry::DbValue;
use crate::safe_io::errors::SafeIoInitError;
use crate::safe_io::writer::SafeWriter;
use crate::storage_config::StorageConfig;
use crate::wal::errors::WalWriterError;
use crate::wal::id_generator::OrderedWalId;
use crate::wal::mode::WalMode;
use std::path::Path;
use std::rc::Rc;

pub struct WalWriter {
    writer: SafeWriter,
    storage_config: Rc<StorageConfig>,
}

impl WalWriter {
    pub async fn open_and_truncate(
        path: &Path,
        id: Rc<OrderedWalId>,
        wal_mode: Rc<WalMode>,
        storage_config: Rc<StorageConfig>,
    ) -> Result<WalWriter, SafeIoInitError> {
        let max_batch_size = match wal_mode.as_ref() {
            WalMode::Strict { max_batch_size, .. } | WalMode::Relaxed { max_batch_size, .. } => {
                *max_batch_size as u32
            }
        };
        let avg_entry_size = storage_config.avg_command_size_hint;

        let buffer_size = max_batch_size * avg_entry_size;
        let writes_behind = max_batch_size.max(64); // at least one full batch

        let mut writer =
            SafeWriter::init(path, buffer_size as usize, writes_behind as usize, true).await?;

        writer
            .write_record(&[&id.order.to_le_bytes()])
            .await
            .expect("Could not preserve WAL ordering");

        Ok(Self {
            writer,
            storage_config,
        })
    }

    pub async fn append(&mut self, request: &CommandRequest) -> Result<(), WalWriterError> {
        match request {
            CommandRequest::Get(_) => panic!("WAL only stores mutating operations"),
            CommandRequest::Set(cmd) => {
                let command_type = [0u8];
                let key_len = (cmd.key.len() as u32).to_le_bytes();

                match &cmd.value {
                    DbValue::Value(value) => {
                        let value_len = (value.len() as u32).to_le_bytes();
                        self.writer
                            .write_record(&[&command_type, &key_len, &cmd.key, &value_len, value])
                            .await
                            .map_err(WalWriterError::WriteFailed)?;
                    }
                    DbValue::Tombstone => panic!("SET cannot be used write Tombstone"),
                }
            }
            CommandRequest::Remove(cmd) => {
                let command_type = [1u8];
                let key_len = (cmd.key.len() as u32).to_le_bytes();

                self.writer
                    .write_record(&[&command_type, &key_len, &cmd.key])
                    .await
                    .map_err(WalWriterError::WriteFailed)?;
            }
        };

        Ok(())
    }

    pub async fn fsync(&self) -> Result<(), WalWriterError> {
        self.writer
            .fsync()
            .await
            .map_err(|e| WalWriterError::DurabilityError(e.into()))?;

        Ok(())
    }

    pub async fn finish(self) -> Result<(), WalWriterError> {
        self.writer
            .close()
            .await
            .map_err(WalWriterError::DurabilityError)?;

        Ok(())
    }
}
