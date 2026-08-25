use crate::db_entry::{DbEntry, DbValue};
use crate::le_reader::LeReader;
use crate::ss_table::metadata::SsTableLevel;
use crate::storage_config::StorageConfig;
use futures::AsyncReadExt;
use glommio::GlommioError;
use glommio::io::{DmaFile, DmaStreamReader, DmaStreamReaderBuilder};
use std::rc::Rc;

pub struct SsTableReader {
    dma_reader: DmaStreamReader,
    storage_config: Rc<StorageConfig>,
    level: SsTableLevel,
    file_size: u64,
}

impl SsTableReader {
    pub async fn init(
        file: DmaFile,
        storage_config: Rc<StorageConfig>,
    ) -> Result<Self, GlommioError<()>> {
        let file_size = file.file_size().await?;
        let mut dma_reader = DmaStreamReaderBuilder::new(file)
            .with_start_pos(0)
            .with_buffer_size(storage_config.ss_table_read_buffer_size as usize)
            .with_read_ahead(storage_config.ss_table_buffer_read_ahead as usize)
            .build();

        let mut level_bytes = [0u8; 2]; // u16
        dma_reader.read_exact(&mut level_bytes).await?;

        let level = LeReader::read_u16_le(level_bytes.as_slice(), 0);

        Ok(Self {
            dma_reader,
            storage_config,
            level,
            file_size,
        })
    }

    pub fn is_eof(&self) -> bool {
        self.dma_reader.current_pos() >= self.file_size
    }

    pub async fn next_entry(&mut self) -> Result<DbEntry, GlommioError<()>> {
        let mut key_len_bytes = [0u8; 4];
        self.dma_reader.read_exact(&mut key_len_bytes).await?;

        let key_len = LeReader::read_u32_le(key_len_bytes.as_slice(), 0);
        let mut key = vec![0u8; key_len as usize];
        self.dma_reader.read_exact(key.as_mut_slice()).await?;

        let mut value_type_bytes = [0u8];
        self.dma_reader.read_exact(&mut value_type_bytes).await?;
        let value_type = value_type_bytes[0];
        let value = match value_type {
            0u8 => {
                let mut value_len_bytes = [0u8; 4];
                self.dma_reader.read_exact(&mut value_len_bytes).await?;

                let value_len = LeReader::read_u32_le(value_len_bytes.as_slice(), 0);

                let mut value = vec![0u8; value_len as usize];
                self.dma_reader.read_exact(value.as_mut_slice()).await?;
                DbValue::Value(value)
            }
            1u8 => DbValue::Tombstone,
            _ => panic!("Unsupported value type {}", value_type),
        };

        Ok(DbEntry { key, value })
    }

    pub fn get_level(&self) -> SsTableLevel {
        self.level
    }

    pub async fn close(self) -> Result<(), GlommioError<()>> {
        self.dma_reader.close().await
    }
}
