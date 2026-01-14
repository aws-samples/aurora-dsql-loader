use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};

use super::byte_reader::ByteReader;

/// ByteReader implementation for local files
#[derive(Clone)]
pub struct LocalFileByteReader {
    file_path: PathBuf,
}

impl LocalFileByteReader {
    pub fn new(file_path: impl AsRef<Path>) -> Self {
        Self {
            file_path: file_path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl ByteReader for LocalFileByteReader {
    async fn size(&self) -> Result<u64> {
        let file = File::open(&self.file_path)
            .await
            .context("Failed to open file")?;
        let size = file.metadata().await?.len();
        Ok(size)
    }

    async fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        let file = File::open(&self.file_path)
            .await
            .context("Failed to open file for reading")?;

        let mut file = BufReader::new(file);
        file.seek(std::io::SeekFrom::Start(start)).await?;

        let size = (end - start) as usize;
        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }
}
