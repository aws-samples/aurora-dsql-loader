use anyhow::{Context, Result};
use async_trait::async_trait;

use super::escape::decode_field;
use super::scan::{CopyBlock, find_copy_block};
use crate::formats::reader::{
    Chunk, ChunkData, FileMetadata, FileReader, Record, build_chunks_over_range,
};
use crate::io::{ByteReader, estimate_rows_in_range};

/// Reader for pg_dump plain-text output, scoped to a single table's
/// `COPY ... FROM stdin` block. DDL/SET/comment statements outside the
/// targeted COPY block are ignored (running pg_dump with `--data-only` is
/// recommended, not required).
///
/// **NULL handling:** the literal `\N` is the only NULL marker pg_dump emits
/// in COPY text format. `escape::decode_field` returns `Option<String>` —
/// `None` for `\N`, `Some("")` for a genuine empty field — which flows
/// directly into `Record.fields`, so the worker emits SQL NULL only for
/// `\N` and never collapses an empty string into NULL.
pub struct PgDumpReader<R: ByteReader> {
    reader: R,
    block: CopyBlock,
}

impl<R: ByteReader> PgDumpReader<R> {
    pub async fn new(reader: R, schema: &str, table: &str) -> Result<Self> {
        let block = find_copy_block(&reader, schema, table)
            .await
            .with_context(|| format!("locating COPY block for {schema}.{table}"))?;
        Ok(Self { reader, block })
    }

    /// Build from a pre-resolved [`CopyBlock`]. Lets the migrate path reuse a
    /// single dump scan across N tables instead of re-walking the file
    /// once per table inside `find_copy_block`.
    pub fn from_block(reader: R, block: CopyBlock) -> Self {
        Self { reader, block }
    }

    /// Column names declared in the COPY statement, in order.
    pub fn columns(&self) -> &[String] {
        &self.block.columns
    }
}

#[async_trait]
impl<R: ByteReader + 'static> FileReader for PgDumpReader<R> {
    async fn metadata(&self) -> Result<FileMetadata> {
        let block_size = self.block.data_end - self.block.data_start;
        let estimated_rows =
            estimate_rows_in_range(&self.reader, self.block.data_start, self.block.data_end)
                .await?;
        Ok(FileMetadata {
            file_size_bytes: block_size,
            estimated_rows,
        })
    }

    async fn create_chunks(&self, target_size: u64) -> Result<Vec<Chunk>> {
        build_chunks_over_range(
            &self.reader,
            self.block.data_start,
            self.block.data_end,
            target_size,
        )
        .await
    }

    async fn read_chunk(&self, chunk: &Chunk) -> Result<ChunkData> {
        let buffer = self
            .reader
            .read_range(chunk.start_offset, chunk.end_offset)
            .await
            .context("Failed to read chunk data")?;

        let expected_columns = self.block.columns.len();
        let mut records = Vec::new();
        let mut parse_errors = 0u64;

        for line in split_lines(&buffer) {
            // pg_dump emits exactly one record per non-terminator line; an empty
            // line in COPY data is structural corruption, not a row to skip.
            // Surface it as a parse error rather than silently dropping it.
            if line.is_empty() {
                parse_errors += 1;
                continue;
            }

            // `\N` decodes to `None`; genuine empty string decodes to `Some("")`.
            let fields: Vec<Option<String>> =
                line.split(|&b| b == b'\t').map(decode_field).collect();

            if fields.len() != expected_columns {
                parse_errors += 1;
                continue;
            }
            records.push(Record { fields });
        }

        Ok(ChunkData {
            records,
            bytes_read: chunk.end_offset - chunk.start_offset,
            parse_errors,
        })
    }
}

/// Split a byte buffer on `\n`. Strips trailing `\r` (CRLF tolerant). The
/// trailing empty element produced by a final `\n` is dropped; intermediate
/// empty lines are preserved so callers can flag them as corruption.
fn split_lines(buf: &[u8]) -> impl Iterator<Item = &[u8]> {
    let buf = buf.strip_suffix(b"\n").unwrap_or(buf);
    buf.split(|&b| b == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
}
