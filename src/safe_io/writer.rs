use crate::safe_io::errors::SafeIoInitError;
use futures::AsyncWriteExt;
use glommio_ng::GlommioError;
use glommio_ng::io::{DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions};
use std::path::Path;

pub struct SafeWriter {
    dma_writer: DmaStreamWriter,
}

impl SafeWriter {
    pub async fn init(
        path: &Path,
        buffer_size: usize,
        write_behind: usize,
        truncate: bool,
    ) -> Result<Self, SafeIoInitError> {
        let file = OpenOptions::new()
            .read(false)
            .write(true)
            .truncate(truncate)
            .dma_open(path)
            .await
            .map_err(|e| SafeIoInitError::OpenFailed(e.into()))?;

        let writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(buffer_size)
            .with_write_behind(write_behind)
            .build();

        Ok(Self { dma_writer: writer })
    }

    /// Writes buffer and awaits for completion.
    /// File is not safe to write anymore if failure happens
    // Parts are used instead of something like buffer to avoid extra copies on the caller side
    pub async fn write_record(&mut self, parts: &[&[u8]]) -> Result<(), std::io::Error> {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        let len = u32::try_from(len).expect("record too large, validation failed somewhere");
        let len_bytes = len.to_le_bytes();

        let mut crc = crc32c::crc32c_append(0u32, &len_bytes);
        for part in parts {
            crc = crc32c::crc32c_append(crc, part);
        }

        let crc_bytes = crc.to_le_bytes();

        self.dma_writer.write_all(&len_bytes).await?;
        self.dma_writer.write_all(&crc_bytes).await?;

        for part in parts {
            self.dma_writer.write_all(part).await?;
        }

        Ok(())
    }

    pub async fn fsync(&self) -> Result<u64, GlommioError<()>> {
        self.dma_writer.sync().await
    }

    pub async fn close(mut self) -> Result<(), std::io::Error> {
        self.dma_writer.close().await
    }

    pub fn current_pos(&self) -> u64 {
        self.dma_writer.current_pos()
    }
}
