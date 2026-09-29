use crate::commands::remove::RemoveCommand;
use crate::commands::request::CommandRequest;
use crate::commands::set::SetCommand;
use crate::db_entry::DbValue;
use crate::le_reader::LeReader;
use crate::safe_io::reader::SafeReader;
use crate::storage_config::StorageConfig;
use crate::wal::errors::WalReaderError;
use std::path::PathBuf;
use std::rc::Rc;

pub struct WalReader {
    reader: SafeReader,
    storage_config: Rc<StorageConfig>,
    order: u64,
}

impl WalReader {
    pub async fn init(
        path: PathBuf,
        storage_config: Rc<StorageConfig>,
    ) -> Result<Self, WalReaderError> {
        let mut reader = SafeReader::init(
            path,
            storage_config.wal_read_buffer_size as usize,
            storage_config.wal_buffer_read_ahead as usize,
            0,
        )
        .await
        .map_err(WalReaderError::InitFailed)?;

        let order_bytes = reader
            .next_record()
            .await
            .map_err(WalReaderError::ReadFailed)?
            .expect("Cannot determine WAL order");

        let order = LeReader::read_u64_le(&order_bytes, 0);

        Ok(Self {
            reader,
            storage_config,
            order,
        })
    }

    pub fn get_order(&self) -> u64 {
        self.order
    }

    pub async fn next_command(&mut self) -> Result<Option<CommandRequest>, WalReaderError> {
        let record = self
            .reader
            .next_record()
            .await
            .map_err(WalReaderError::ReadFailed)?;

        if let Some(cmd_bytes) = record {
            let mut offset = 0;
            let cmd_type = cmd_bytes[0];
            offset += size_of::<u8>();

            let key_len = LeReader::read_u32_le(&cmd_bytes, offset);
            offset += size_of::<u32>();

            let key = cmd_bytes[offset..offset + key_len as usize].to_vec();
            offset += key_len as usize;

            match cmd_type {
                0u8 => {
                    // Set
                    let value_len = LeReader::read_u32_le(&cmd_bytes, offset);
                    offset += size_of::<u32>();

                    let value = cmd_bytes[offset..offset + value_len as usize].to_vec();
                    offset += value_len as usize;

                    return Ok(Some(CommandRequest::Set(SetCommand {
                        key: key.to_vec(),
                        value: DbValue::Value(value.to_vec()),
                    })));
                }
                1u8 => {
                    // Remove
                    return Ok(Some(CommandRequest::Remove(RemoveCommand { key })));
                }
                _ => panic!("unsupported command type"),
            }
        }

        Ok(None)
    }
}
