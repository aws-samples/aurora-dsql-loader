use anyhow::Result;
use async_trait::async_trait;
use aws_config::SdkConfig;
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;

use super::delimited::reader::GenericDelimitedReader;
use super::parquet::GenericParquetReader;
use crate::io::{
    ByteReader, LocalFileByteReader, S3ByteReader, SourceUri, estimate_rows_in_range,
    find_next_record_boundary,
};

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

/// A single record (row) from the file.
///
/// `nulls[i]` is the authoritative SQL-NULL marker for `fields[i]`. Each
/// reader populates it according to its format's NULL convention:
/// - **pg_dump:** `\N` → null, genuine empty string → not null. Preserves
///   the `\N` vs `""` distinction pg_dump emits at the byte level.
/// - **CSV / TSV / parquet:** any field whose trimmed text is empty → null.
///   Mirrors the legacy worker-side inference these formats relied on
///   before the mask existed.
///
/// The worker uses the mask verbatim — no further inference — so each
/// format owns its NULL semantics end-to-end.
#[derive(Debug, Clone)]
pub struct Record {
    pub fields: Vec<String>,
    pub nulls: Vec<bool>,
}

/// Data from a chunk read
#[derive(Debug)]
pub struct ChunkData {
    pub records: Vec<Record>,
    pub bytes_read: u64,
    /// Number of records that failed to parse
    pub parse_errors: u64,
}

/// Build newline-aligned `Chunk`s over the byte range `[start, end)` from
/// `reader`, each at most `target_size` bytes. Each chunk's `end_offset` is
/// snapped forward to the next `\n` so a row never straddles two chunks.
/// Estimated rows are sampled per chunk via `estimate_rows_in_range`.
///
/// Used by every newline-delimited reader (CSV/TSV, pg_dump). Parquet has
/// its own row-group-aware chunking and does not call this.
pub async fn build_chunks_over_range(
    reader: &dyn ByteReader,
    start: u64,
    end: u64,
    target_size: u64,
) -> Result<Vec<Chunk>> {
    if start >= end {
        return Ok(vec![]);
    }

    let mut chunks = Vec::new();
    let mut current = start;
    let mut chunk_id = 0u32;

    while current < end {
        let target_end = std::cmp::min(current + target_size, end);
        let actual_end = if target_end >= end {
            end
        } else {
            // find_next_record_boundary returns end-of-file when no \n exists
            // in [target_end, file_size); clamp to our range so a pgdump caller
            // doesn't push past data_end.
            std::cmp::min(find_next_record_boundary(reader, target_end).await?, end)
        };
        let estimated_rows = estimate_rows_in_range(reader, current, actual_end).await?;
        chunks.push(Chunk {
            chunk_id,
            start_offset: current,
            end_offset: actual_end,
            estimated_rows,
        });
        current = actual_end;
        chunk_id += 1;
    }
    Ok(chunks)
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DelimitedConfig {
    pub delimiter: String,
    pub has_header: bool,
    pub quote: String,
    pub escape: Option<String>,
}

impl Default for DelimitedConfig {
    fn default() -> Self {
        Self {
            delimiter: ",".to_string(),
            has_header: false,
            quote: "\"".to_string(),
            escape: None,
        }
    }
}

impl DelimitedConfig {
    pub fn csv() -> Self {
        Self::default()
    }

    pub fn tsv() -> Self {
        Self {
            delimiter: "\\t".to_string(),
            ..Self::default()
        }
    }
}

/// Supported file formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Csv,
    Tsv,
    Parquet,
    PgDump,
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
    ///
    /// If delimited_config is provided for CSV/TSV formats, it will be used instead of defaults
    pub async fn create_reader(
        &self,
        source_uri: &SourceUri,
        format: Format,
        delimited_config: Option<DelimitedConfig>,
    ) -> Result<Arc<dyn FileReader>> {
        match (source_uri, format) {
            // Local CSV
            (SourceUri::Local(path), Format::Csv) => {
                let config = delimited_config.unwrap_or_else(DelimitedConfig::csv);
                let byte_reader = LocalFileByteReader::new(path);
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // Local TSV
            (SourceUri::Local(path), Format::Tsv) => {
                let config = delimited_config.unwrap_or_else(DelimitedConfig::tsv);
                let byte_reader = LocalFileByteReader::new(path);
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // S3 CSV
            (SourceUri::S3 { bucket, key }, Format::Csv) => {
                let config = delimited_config.unwrap_or_else(DelimitedConfig::csv);
                let byte_reader =
                    S3ByteReader::new(Arc::clone(&self.s3_client), bucket.clone(), key.clone());
                let reader = GenericDelimitedReader::new(byte_reader, config);
                Ok(Arc::new(reader) as Arc<dyn FileReader>)
            }

            // S3 TSV
            (SourceUri::S3 { bucket, key }, Format::Tsv) => {
                let config = delimited_config.unwrap_or_else(DelimitedConfig::tsv);
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

            // pg_dump needs out-of-band column metadata (the COPY clause), so it
            // is built via `create_pgdump_reader` instead. The variant lives on
            // the same enum for ergonomic dispatch in `runner.rs`; reaching this
            // arm means a caller bypassed that dispatch.
            (_, Format::PgDump) => {
                anyhow::bail!("internal: pg_dump format must be created via create_pgdump_reader()")
            }
        }
    }

    /// Create a FileReader for a pg_dump source URI, scoped to a single COPY block.
    /// Returns the reader plus the column names declared in the matching
    /// `COPY ... (cols)` clause, in declaration order.
    pub async fn create_pgdump_reader(
        &self,
        source_uri: &SourceUri,
        schema: &str,
        table: &str,
    ) -> Result<(Arc<dyn FileReader>, Vec<String>)> {
        use crate::formats::pgdump::PgDumpReader;
        match source_uri {
            SourceUri::Local(path) => {
                let byte_reader = LocalFileByteReader::new(path);
                let reader = PgDumpReader::new(byte_reader, schema, table).await?;
                let columns = reader.columns().to_vec();
                Ok((Arc::new(reader) as Arc<dyn FileReader>, columns))
            }
            SourceUri::S3 { bucket, key } => {
                let byte_reader =
                    S3ByteReader::new(Arc::clone(&self.s3_client), bucket.clone(), key.clone());
                let reader = PgDumpReader::new(byte_reader, schema, table).await?;
                let columns = reader.columns().to_vec();
                Ok((Arc::new(reader) as Arc<dyn FileReader>, columns))
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
        // Fixture has a header row, so override the has_header default.
        let config = DelimitedConfig {
            has_header: true,
            ..DelimitedConfig::csv()
        };
        let reader = GenericDelimitedReader::new(byte_reader, config);
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
