use anyhow::{Context, Result};
use async_trait::async_trait;

use super::escape::{DecodedField, decode_field};
use super::scan::{CopyBlock, find_copy_block};
use crate::formats::reader::{Chunk, ChunkData, FileMetadata, FileReader, Record};
use crate::io::{ByteReader, estimate_rows_in_range, find_next_record_boundary};

/// Reader for plain pg_dump --data-only output, scoped to a single table's
/// COPY FROM stdin block.
///
/// **NULL handling:** PG's `\N` is decoded by `escape::decode_field` as a
/// distinct `DecodedField::Null` variant, but the loader's `Record { fields:
/// Vec<String> }` shape carries strings only, so `\N` is collapsed to an
/// empty string here. The downstream worker (`coordination/worker.rs`)
/// already maps empty strings to SQL NULL during binding, so functionally
/// `\N → NULL` survives end-to-end. The trade-off is that real empty strings
/// AND whitespace-only strings (the worker `trim()`s before the empty check)
/// become indistinguishable from NULL; if your dataset depends on either
/// distinction, do not use `--format pgdump` until proper `Option<String>`
/// fidelity lands.
pub struct PgDumpReader<R: ByteReader> {
    reader: R,
    block: CopyBlock,
}

impl<R: ByteReader> std::fmt::Debug for PgDumpReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgDumpReader")
            .field("block", &self.block)
            .finish_non_exhaustive()
    }
}

impl<R: ByteReader> PgDumpReader<R> {
    pub async fn new(reader: R, schema: &str, table: &str) -> Result<Self> {
        let block = find_copy_block(&reader, schema, table)
            .await
            .with_context(|| format!("locating COPY block for {schema}.{table}"))?;
        Ok(Self { reader, block })
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
        let block_size = self.block.data_end - self.block.data_start;
        if block_size == 0 {
            return Ok(vec![]);
        }

        let mut chunks = Vec::new();
        let mut current = self.block.data_start;
        let mut chunk_id = 0u32;
        let end = self.block.data_end;

        while current < end {
            let target_end = std::cmp::min(current + target_size, end);
            let actual_end = if target_end >= end {
                end
            } else {
                let boundary = find_next_record_boundary(&self.reader, target_end).await?;
                std::cmp::min(boundary, end)
            };
            let estimated_rows = estimate_rows_in_range(&self.reader, current, actual_end).await?;
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

            let fields: Vec<String> = line
                .split(|&b| b == b'\t')
                .map(|raw| match decode_field(raw) {
                    DecodedField::Value(s) => s,
                    // See PgDumpReader rustdoc for the \N → "" trade-off.
                    DecodedField::Null => String::new(),
                })
                .collect();

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
