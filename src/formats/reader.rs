use anyhow::Result;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;

use super::delimited::reader::GenericDelimitedReader;
use super::parquet::GenericParquetReader;
use crate::io::{LocalFileByteReader, S3ByteReader, SourceUri};

/// Metadata about a file to be loaded
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_size_bytes: u64,
    pub estimated_rows: Option<u64>,
}

/// A chunk of a file, defined by byte offsets
#[derive(Debug, Clone)]
pub struct Chunk {
    pub chunk_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub estimated_rows: Option<u64>,
}

/// A single record (row) from the file
#[derive(Debug, Clone)]
pub struct Record {
    pub fields: Vec<String>,
}

/// Data from a chunk read
#[derive(Debug)]
pub struct ChunkData {
    pub records: Vec<Record>,
    pub bytes_read: u64,
}

/// Trait for reading different file formats with chunking support
#[async_trait]
pub trait FileReader: Send + Sync {
    /// Get metadata about the file
    async fn metadata(&self) -> Result<FileMetadata>;

    /// Create chunks for the file, respecting record boundaries
    /// target_size is the approximate size in bytes for each chunk
    async fn create_chunks(&self, target_size: u64) -> Result<Vec<Chunk>>;

    /// Read a specific chunk from the file
    async fn read_chunk(&self, chunk: &Chunk) -> Result<ChunkData>;
}

/// Configuration for delimited file reading (CSV, TSV, etc.)
#[derive(Debug, Clone)]
pub struct DelimitedConfig {
    pub delimiter: u8,
    pub has_header: bool,
    pub quote: u8,
}

impl DelimitedConfig {
    /// Convert delimiter byte to string representation
    pub(crate) fn delimiter_as_string(&self) -> String {
        if self.delimiter == b'\t' {
            "\\t".to_string()
        } else {
            String::from_utf8(vec![self.delimiter])
                .unwrap_or_else(|_| format!("\\x{:02x}", self.delimiter))
        }
    }

    /// Convert quote byte to string representation
    pub(crate) fn quote_as_string(&self) -> String {
        String::from_utf8(vec![self.quote]).unwrap_or_else(|_| format!("\\x{:02x}", self.quote))
    }

    /// Create from string representations
    pub(crate) fn from_strings(delimiter: &str, has_header: bool, quote: &str) -> Result<Self> {
        let delimiter_byte = if delimiter == "\\t" {
            b'\t'
        } else if delimiter.len() == 1 {
            delimiter.as_bytes()[0]
        } else {
            anyhow::bail!("Delimiter must be a single character or \\t");
        };

        let quote_byte = if quote.len() == 1 {
            quote.as_bytes()[0]
        } else {
            anyhow::bail!("Quote must be a single character");
        };

        Ok(Self {
            delimiter: delimiter_byte,
            has_header,
            quote: quote_byte,
        })
    }
}

impl Default for DelimitedConfig {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_header: true,
            quote: b'"',
        }
    }
}

impl DelimitedConfig {
    pub fn csv() -> Self {
        Self::default()
    }

    pub fn tsv() -> Self {
        Self {
            delimiter: b'\t',
            has_header: true,
            quote: b'"',
        }
    }
}

/// Supported file formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Csv,
    Tsv,
    Parquet,
}

/// Factory for creating FileReader instances based on URI and format
pub struct ReaderFactory {
    s3_client: Arc<S3Client>,
}

impl ReaderFactory {
    /// Create a new ReaderFactory
    pub fn new(aws_config: &SdkConfig) -> Self {
        let s3_client = Arc::new(S3Client::new(aws_config));
        Self { s3_client }
    }

    /// Create a FileReader based on source URI and format
    pub async fn create_reader(
        &self,
        source_uri: &SourceUri,
        format: Format,
    ) -> Result<Arc<dyn FileReader>> {
        match (source_uri, format) {
            // Local CSV
            (SourceUri::Local(path), Format::Csv) => {
                let config = DelimitedConfig::csv();
                let byte_reader = LocalFileByteReader::new(path);
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // Local TSV
            (SourceUri::Local(path), Format::Tsv) => {
                let config = DelimitedConfig::tsv();
                let byte_reader = LocalFileByteReader::new(path);
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // S3 CSV
            (SourceUri::S3 { bucket, key }, Format::Csv) => {
                let config = DelimitedConfig::csv();
                let byte_reader =
                    S3ByteReader::new(Arc::clone(&self.s3_client), bucket.clone(), key.clone());
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // S3 TSV
            (SourceUri::S3 { bucket, key }, Format::Tsv) => {
                let config = DelimitedConfig::tsv();
                let byte_reader =
                    S3ByteReader::new(Arc::clone(&self.s3_client), bucket.clone(), key.clone());
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // Local Parquet
            (SourceUri::Local(path), Format::Parquet) => {
                let byte_reader = LocalFileByteReader::new(path);
                let reader = GenericParquetReader::new(byte_reader).await?;
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // S3 Parquet
            (SourceUri::S3 { bucket, key }, Format::Parquet) => {
                let byte_reader =
                    S3ByteReader::new(Arc::clone(&self.s3_client), bucket.clone(), key.clone());
                let reader = GenericParquetReader::new(byte_reader).await?;
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::delimited::reader::GenericDelimitedReader;
    use super::*;
    use crate::io::LocalFileByteReader;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_csv_metadata() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,email").unwrap();
        writeln!(temp_file, "1,Alice,alice@example.com").unwrap();
        writeln!(temp_file, "2,Bob,bob@example.com").unwrap();
        temp_file.flush().unwrap();

        let byte_reader = LocalFileByteReader::new(temp_file.path());
        let reader = GenericDelimitedReader::new(byte_reader, DelimitedConfig::csv());
        let metadata = reader.metadata().await.unwrap();

        assert!(metadata.file_size_bytes > 0);
    }

    #[tokio::test]
    async fn test_csv_chunking() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,email").unwrap();
        for i in 0..100 {
            writeln!(temp_file, "{},Name{},email{}@example.com", i, i, i).unwrap();
        }
        temp_file.flush().unwrap();

        let byte_reader = LocalFileByteReader::new(temp_file.path());
        let reader = GenericDelimitedReader::new(byte_reader, DelimitedConfig::csv());
        let chunks = reader.create_chunks(500).await.unwrap();

        // Should create multiple chunks
        assert!(chunks.len() > 1);

        // Chunks should be contiguous
        for i in 1..chunks.len() {
            assert_eq!(chunks[i - 1].end_offset, chunks[i].start_offset);
        }
    }

    #[tokio::test]
    async fn test_read_chunk() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,email").unwrap();
        writeln!(temp_file, "1,Alice,alice@example.com").unwrap();
        writeln!(temp_file, "2,Bob,bob@example.com").unwrap();
        writeln!(temp_file, "3,Charlie,charlie@example.com").unwrap();
        temp_file.flush().unwrap();

        let byte_reader = LocalFileByteReader::new(temp_file.path());
        let reader = GenericDelimitedReader::new(byte_reader, DelimitedConfig::csv());
        let metadata = reader.metadata().await.unwrap();

        // Create chunks and use the first one (which skips the header)
        let chunks = reader
            .create_chunks(metadata.file_size_bytes)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);

        let chunk = &chunks[0];
        let data = reader.read_chunk(chunk).await.unwrap();

        assert_eq!(data.records.len(), 3);
        assert_eq!(data.records[0].fields.len(), 3);
        assert_eq!(data.records[0].fields[0], "1");
        assert_eq!(data.records[0].fields[1], "Alice");
    }
}
