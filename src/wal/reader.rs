use crate::commands::remove::RemoveCommand;
use crate::commands::request::CommandRequest;
use crate::commands::set::SetCommand;
use crate::db_entry::DbValue;
use crate::le_reader::LeReader;
use crate::storage_config::StorageConfig;
use crate::wal::errors::WalReaderError;
use crate::wal::writer::WalWriter;
use futures::AsyncReadExt;
use glommio_ng::io::{DmaStreamReader, DmaStreamReaderBuilder, OpenOptions};
use std::path::Path;
use std::rc::Rc;

pub struct WalReader {
    dma_reader: DmaStreamReader,
    file_size: u64,
    storage_config: Rc<StorageConfig>,
    order: u64,
}

impl WalReader {
    pub async fn init(
        path: &Path,
        storage_config: Rc<StorageConfig>,
    ) -> Result<Self, WalReaderError> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .dma_open(path)
            .await
            .map_err(|e| WalReaderError::OpenFailed(e.into()))?;

        let file_size = file
            .file_size()
            .await
            .map_err(|e| WalReaderError::ReadFailed(e.into()))?;

        let mut dma_reader = DmaStreamReaderBuilder::new(file)
            .with_start_pos(0)
            .with_buffer_size(storage_config.wal_read_buffer_size as usize)
            .with_read_ahead(storage_config.wal_buffer_read_ahead as usize)
            .build();

        let mut order_bytes = [0u8; 8];
        dma_reader
            .read_exact(&mut order_bytes)
            .await
            .map_err(WalReaderError::ReadFailed)?;

        let order = LeReader::read_u64_le(&order_bytes, 0);

        Ok(Self {
            dma_reader,
            file_size,
            storage_config,
            order,
        })
    }

    pub fn get_order(&self) -> u64 {
        self.order
    }

    pub async fn next_command(&mut self) -> Result<Option<CommandRequest>, WalReaderError> {
        let expected = WalWriter::ENTRY_START_HEADER.to_le_bytes();

        let mut header = [0u8; 4];

        match self.dma_reader.read(&mut header).await {
            Ok(len) => {
                if len != expected.len() {
                    return Ok(None);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => {
                return Err(WalReaderError::ReadFailed(e));
            }
        }

        if header != expected {
            return Ok(None);
        }

        let mut cmd_type_bytes = [0u8; 1];
        self.dma_reader
            .read_exact(&mut cmd_type_bytes)
            .await
            .map_err(WalReaderError::ReadFailed)?;
        let cmd_type = cmd_type_bytes[0];

        let mut key_length_bytes = [0u8; 4];
        self.dma_reader
            .read_exact(&mut key_length_bytes)
            .await
            .map_err(WalReaderError::ReadFailed)?;
        let key_length = LeReader::read_u32_le(&key_length_bytes, 0);

        let mut key = vec![0u8; key_length as usize];
        self.dma_reader
            .read_exact(&mut key)
            .await
            .map_err(WalReaderError::ReadFailed)?;

        match cmd_type {
            0u8 => {
                // Set
                let mut value_len_bytes = [0u8; 4];
                self.dma_reader
                    .read_exact(&mut value_len_bytes)
                    .await
                    .map_err(WalReaderError::ReadFailed)?;

                let value_len = LeReader::read_u32_le(&value_len_bytes, 0);

                let mut value = vec![0u8; value_len as usize];
                self.dma_reader
                    .read_exact(&mut value)
                    .await
                    .map_err(WalReaderError::ReadFailed)?;

                Ok(Some(CommandRequest::Set(SetCommand {
                    key,
                    value: DbValue::Value(value),
                })))
            }
            1u8 => {
                // Remove
                Ok(Some(CommandRequest::Remove(RemoveCommand { key })))
            }
            _ => panic!("unsupported command type"),
        }
    }
}
