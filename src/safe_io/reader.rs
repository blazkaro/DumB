use crate::le_reader::LeReader;
use crate::safe_io::errors::{SafeIoInitError, SafeReadError};
use futures::AsyncReadExt;
use glommio_ng::io::{DmaStreamReader, DmaStreamReaderBuilder, OpenOptions};
use std::path::PathBuf;

pub struct SafeReader {
    path: PathBuf,
    dma_reader: DmaStreamReader,
    buffer_size: usize,
    read_ahead: usize,
    last_valid_offset: u64,
}

impl SafeReader {
    pub async fn init(
        path: PathBuf,
        buffer_size: usize,
        read_ahead: usize,
        start_pos: u64,
    ) -> Result<Self, SafeIoInitError> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .dma_open(&path)
            .await
            .map_err(|e| SafeIoInitError::OpenFailed(e.into()))?;

        let dma_reader = DmaStreamReaderBuilder::new(file)
            .with_start_pos(start_pos)
            .with_buffer_size(buffer_size)
            .with_read_ahead(read_ahead)
            .build();

        Ok(Self {
            path,
            dma_reader,
            buffer_size,
            read_ahead,
            last_valid_offset: start_pos,
        })
    }

    /// Reads next record. Not safe to retry - use recover(), and then try again
    pub async fn next_record(&mut self) -> Result<Option<Vec<u8>>, SafeReadError> {
        let header_pos = self.dma_reader.current_pos();

        let mut header = [0u8; 8];
        match Self::read_exact(&mut self.dma_reader, &mut header).await {
            Ok(_) => {}
            Err(SafeReadError::TruncatedEntry) if self.dma_reader.current_pos() == header_pos => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e);
            }
        };

        let buf_len_bytes = &header[0..4];
        let len = LeReader::read_u32_le(buf_len_bytes, 0);
        let expected_crc = LeReader::read_u32_le(&header[4..8], 0);

        // Padding written by DmaStreamWriter::sync()
        if len == 0 && expected_crc == 0 {
            return Ok(None);
            // If padding is written when sync(), it will be overwritten by future operations.
            // Therefore, padding can't exist between entries. It means eof.
            // However, it may break if someone uses OpenOptions::new().append(true).
            // That operation is not safe with DmaStreamWriter, so we don't consider this as a case.
        }

        let mut buf = vec![0u8; len as usize];
        match Self::read_exact(&mut self.dma_reader, &mut buf).await {
            Ok(_) => {}
            Err(e) => {
                return Err(e);
            }
        }

        let crc = crc32c::crc32c_append(0u32, buf_len_bytes);
        let crc = crc32c::crc32c_append(crc, &buf);

        if crc != expected_crc {
            return Err(SafeReadError::CorruptedEntry);
        }

        self.last_valid_offset = self.dma_reader.current_pos();
        Ok(Some(buf))
    }

    pub async fn close(self) -> Result<(), std::io::Error> {
        self.dma_reader.close().await.map_err(|e| e.into())
    }

    pub async fn recover(self) -> Result<Self, SafeIoInitError> {
        Self::init(
            self.path,
            self.buffer_size,
            self.read_ahead,
            self.last_valid_offset,
        )
        .await
    }

    pub fn current_pos(&self) -> u64 {
        self.dma_reader.current_pos()
    }

    async fn read_exact(reader: &mut DmaStreamReader, buf: &mut [u8]) -> Result<(), SafeReadError> {
        reader.read_exact(buf).await.map_err(|e| match e.kind() {
            std::io::ErrorKind::UnexpectedEof => SafeReadError::TruncatedEntry,
            _ => SafeReadError::IoError(e),
        })
    }
}
