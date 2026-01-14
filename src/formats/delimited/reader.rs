use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::formats::reader::{Chunk, ChunkData, DelimitedConfig, FileMetadata, FileReader, Record};
use crate::io::{ByteReader, estimate_rows_in_range, find_next_record_boundary};

/// Generic delimited file reader that works with any ByteReader implementation
/// This provides the common chunking and parsing logic for CSV, TSV, etc.
pub struct GenericDelimitedReader<R: ByteReader> {
    reader: R,
    config: DelimitedConfig,
}

impl<R: ByteReader> GenericDelimitedReader<R> {
    pub fn new(reader: R, config: DelimitedConfig) -> Self {
        Self { reader, config }
    }
}

#[async_trait]
impl<R: ByteReader + 'static> FileReader for GenericDelimitedReader<R> {
    async fn metadata(&self) -> Result<FileMetadata> {
        let file_size_bytes = self
            .reader
            .size()
            .await
            .context("Failed to get file size")?;

        // Estimate rows by sampling the file
        let estimated_rows = estimate_rows_in_range(&self.reader, 0, file_size_bytes).await?;

        Ok(FileMetadata {
            file_size_bytes,
            estimated_rows,
        })
    }

    async fn create_chunks(&self, target_size: u64) -> Result<Vec<Chunk>> {
        let metadata = self.metadata().await?;
        let file_size = metadata.file_size_bytes;

        if file_size == 0 {
            return Ok(vec![]);
        }

        let mut chunks = Vec::new();
        let mut current_offset = 0u64;
        let mut chunk_id = 0u32;

        // Skip header if present
        if self.config.has_header {
            current_offset = find_next_record_boundary(&self.reader, 0).await?;
        }

        while current_offset < file_size {
            let target_end = std::cmp::min(current_offset + target_size, file_size);

            // Find the actual end at a record boundary
            let actual_end = if target_end >= file_size {
                file_size
            } else {
                find_next_record_boundary(&self.reader, target_end).await?
            };

            // Estimate rows for this chunk
            let estimated_rows =
                estimate_rows_in_range(&self.reader, current_offset, actual_end).await?;

            chunks.push(Chunk {
                chunk_id,
                start_offset: current_offset,
                end_offset: actual_end,
                estimated_rows,
            });

            current_offset = actual_end;
            chunk_id += 1;
        }

        Ok(chunks)
    }

    async fn read_chunk(&self, chunk: &Chunk) -> Result<ChunkData> {
        // Read the chunk data
        let buffer = self
            .reader
            .read_range(chunk.start_offset, chunk.end_offset)
            .await
            .context("Failed to read chunk data")?;

        // Parse the CSV data
        // Convert string delimiter to byte (handle \t specially)
        let delimiter_byte = if self.config.delimiter == "\\t" {
            b'\t'
        } else {
            self.config.delimiter.as_bytes()[0]
        };
        let quote_byte = self.config.quote.as_bytes()[0];

        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(delimiter_byte)
            .quote(quote_byte)
            .has_headers(false) // We handle headers at the file level
            .from_reader(buffer.as_slice());

        let mut records = Vec::new();

        for result in csv_reader.records() {
            let record = result.context("Failed to parse CSV record")?;

            records.push(Record {
                fields: record.iter().map(|s| s.to_string()).collect(),
            });
        }

        Ok(ChunkData {
            records,
            bytes_read: chunk.end_offset - chunk.start_offset,
        })
    }
}
