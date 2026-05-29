use anyhow::{Context, Result};
use async_trait::async_trait;

use super::scan::{CopyBlock, find_copy_block};
use crate::formats::reader::{Chunk, ChunkData, FileMetadata, FileReader};
use crate::io::{ByteReader, estimate_rows_in_range, find_next_record_boundary};

/// Reader for plain pg_dump --data-only output, scoped to a single table's
/// COPY FROM stdin block.
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

    async fn read_chunk(&self, _chunk: &Chunk) -> Result<ChunkData> {
        // Implemented in Task 5.
        anyhow::bail!("PgDumpReader::read_chunk not yet implemented")
    }
}
