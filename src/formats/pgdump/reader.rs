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

        // Exact L1 count: pg_dump escapes embedded `\n` as `\` + `n`,
        // so every byte-newline in `[data_start, data_end)` is one
        // record. `\.` is excluded by chunk construction.
        let source_rows = buffer.iter().filter(|&&b| b == b'\n').count() as u64;

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
            source_rows_in_chunk: Some(source_rows),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::pgdump::scan::find_copy_block;
    use crate::io::LocalFileByteReader;
    use std::io::Write;

    /// L1 invariant: source_rows_in_chunk == record count for a clean
    /// single-chunk dump. `\.` is excluded by chunk construction.
    #[tokio::test]
    async fn pgdump_read_chunk_source_rows_matches_record_count() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "-- preamble").unwrap();
        writeln!(f, "COPY public.t (id, name) FROM stdin;").unwrap();
        for i in 0..10 {
            writeln!(f, "{i}\tname{i}").unwrap();
        }
        writeln!(f, "\\.").unwrap();
        f.flush().unwrap();

        let reader = LocalFileByteReader::new(f.path());
        let block = find_copy_block(&reader, "public", "t").await.unwrap();
        let pgdump = PgDumpReader::from_block(reader, block);
        let chunks = pgdump.create_chunks(1024 * 1024).await.unwrap();
        let chunk_data = pgdump.read_chunk(&chunks[0]).await.unwrap();
        assert_eq!(
            chunk_data.source_rows_in_chunk,
            Some(10),
            "source_rows must equal the 10 data rows in the COPY block"
        );
        assert_eq!(chunk_data.records.len(), 10);
        assert_eq!(chunk_data.parse_errors, 0);
    }

    /// Per-chunk source_rows must sum to the total source-row count.
    /// Pins the reader-side invariant that `aggregate_source_rows` folds.
    #[tokio::test]
    async fn pgdump_source_rows_sum_across_chunks_equals_total() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "COPY public.t (id, name) FROM stdin;").unwrap();
        for i in 0..100 {
            writeln!(f, "{i}\tname{i}").unwrap();
        }
        writeln!(f, "\\.").unwrap();
        f.flush().unwrap();

        let reader = LocalFileByteReader::new(f.path());
        let block = find_copy_block(&reader, "public", "t").await.unwrap();
        let pgdump = PgDumpReader::from_block(reader, block);
        // Force multiple chunks: ~10 rows per chunk over 100 rows.
        let chunks = pgdump.create_chunks(80).await.unwrap();
        assert!(
            chunks.len() > 1,
            "test setup: expected multiple chunks, got {}",
            chunks.len()
        );

        let mut total_source_rows = 0u64;
        let mut total_records = 0usize;
        for chunk in &chunks {
            let cd = pgdump.read_chunk(chunk).await.unwrap();
            total_source_rows += cd.source_rows_in_chunk.unwrap();
            total_records += cd.records.len();
            assert_eq!(cd.parse_errors, 0);
        }
        assert_eq!(total_source_rows, 100);
        assert_eq!(total_records, 100);
    }

    /// Empty-line invariant: every `\n` in a COPY block becomes a
    /// record OR a parse_error. Silent drops would break L1.
    #[tokio::test]
    async fn pgdump_empty_line_counts_as_parse_error_so_l1_holds() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "COPY public.t (id, name) FROM stdin;").unwrap();
        writeln!(f, "1\talpha").unwrap();
        // Hand-edited / corrupted dump: empty line mid-block. Must
        // surface as parse_error, not a silent drop.
        writeln!(f).unwrap();
        writeln!(f, "2\tbeta").unwrap();
        writeln!(f, "\\.").unwrap();
        f.flush().unwrap();

        let reader = LocalFileByteReader::new(f.path());
        let block = find_copy_block(&reader, "public", "t").await.unwrap();
        let pgdump = PgDumpReader::from_block(reader, block);
        let chunks = pgdump.create_chunks(1024 * 1024).await.unwrap();
        let chunk_data = pgdump.read_chunk(&chunks[0]).await.unwrap();
        assert_eq!(
            chunk_data.source_rows_in_chunk,
            Some(3),
            "source_rows counts every \\n: 2 valid rows + 1 empty line"
        );
        assert_eq!(chunk_data.records.len(), 2);
        assert_eq!(
            chunk_data.parse_errors, 1,
            "the empty line must surface as a parse_error so L1 cross-check stays in sync"
        );
    }
}
