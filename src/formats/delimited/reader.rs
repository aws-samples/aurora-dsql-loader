use anyhow::{Context, Result};
use async_trait::async_trait;

use crate::formats::reader::{
    Chunk, ChunkData, DelimitedConfig, FileMetadata, FileReader, Record, build_chunks_over_range,
};
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
        let file_size = self.metadata().await?.file_size_bytes;
        let start = if self.config.has_header {
            find_next_record_boundary(&self.reader, 0).await?
        } else {
            0
        };
        build_chunks_over_range(&self.reader, start, file_size, target_size).await
    }

    async fn read_chunk(&self, chunk: &Chunk) -> Result<ChunkData> {
        // Read the chunk data
        let buffer = self
            .reader
            .read_range(chunk.start_offset, chunk.end_offset)
            .await
            .context("Failed to read chunk data")?;

        // Parse the CSV data
        // Convert string delimiter to byte (handle escape sequences)
        let delimiter_byte =
            parse_escape_sequence(&self.config.delimiter).context("Invalid delimiter")?;

        let quote_byte =
            parse_escape_sequence(&self.config.quote).context("Invalid quote character")?;

        // Convert escape string to optional byte
        let escape_byte = if let Some(ref escape_str) = self.config.escape {
            Some(parse_escape_sequence(escape_str).context("Invalid escape character")?)
        } else {
            None
        };

        let mut csv_reader = csv::ReaderBuilder::new()
            .delimiter(delimiter_byte)
            .quote(quote_byte)
            .escape(escape_byte)
            .has_headers(false) // We handle headers at the file level
            .from_reader(buffer.as_slice());

        let mut records = Vec::new();
        let mut parse_errors = 0u64;
        let mut expected_columns: Option<usize> = None;

        for result in csv_reader.records() {
            match result {
                Ok(record) => {
                    let field_count = record.len();
                    // Establish expected column count from first record
                    if expected_columns.is_none() {
                        expected_columns = Some(field_count);
                    }
                    // Reject records with mismatched column count (catches unclosed quotes)
                    if Some(field_count) != expected_columns {
                        parse_errors += 1;
                        continue;
                    }
                    let fields: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                    records.push(Record::from_text_fields(fields));
                }
                Err(e) => {
                    if e.is_io_error() {
                        break;
                    }
                    parse_errors += 1;
                }
            }
        }

        Ok(ChunkData {
            records,
            bytes_read: chunk.end_offset - chunk.start_offset,
            parse_errors,
        })
    }
}

/// Parse a string that may contain escape sequences into a single byte
/// Supports: \\, \t, \n, \r, \", \'
fn parse_escape_sequence(s: &str) -> Result<u8> {
    if s.len() == 1 {
        // Single character, no escape sequence
        return Ok(s.as_bytes()[0]);
    }

    if s.len() == 2 && s.starts_with('\\') {
        // Two-character escape sequence
        match s.chars().nth(1) {
            Some('\\') => Ok(b'\\'),
            Some('t') => Ok(b'\t'),
            Some('n') => Ok(b'\n'),
            Some('r') => Ok(b'\r'),
            Some('"') => Ok(b'"'),
            Some('\'') => Ok(b'\''),
            Some(c) => anyhow::bail!("Unknown escape sequence: \\{}", c),
            None => anyhow::bail!("Invalid escape sequence"),
        }
    } else {
        anyhow::bail!(
            "Character must be exactly one character (got {} bytes). Use escape sequences like \\t, \\n, or \\\\",
            s.len()
        )
    }
}
