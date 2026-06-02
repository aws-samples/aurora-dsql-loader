use anyhow::Result;
use async_trait::async_trait;

use crate::config::{CHUNK_SIZE, SAMPLE_SIZE};

/// Abstraction for byte-level I/O operations
/// This trait enables reading files from different sources (local, S3, etc.)
/// with a unified interface for chunking and parsing
#[async_trait]
pub trait ByteReader: Send + Sync {
    /// Get the total size of the file/object in bytes
    async fn size(&self) -> Result<u64>;

    /// Read a range of bytes from the file/object
    /// Returns the bytes read (may be less than requested if EOF is reached)
    async fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>>;
}

/// Helper functions for working with ByteReaders
/// Find the start of the next complete record after the given offset
/// Returns the byte offset of the start of the next line
pub async fn find_next_record_boundary(reader: &dyn ByteReader, offset: u64) -> Result<u64> {
    let file_size = reader.size().await?;
    let mut current_offset = offset;

    loop {
        if current_offset >= file_size {
            return Ok(file_size);
        }

        // Read a chunk
        let end_offset = std::cmp::min(current_offset + CHUNK_SIZE as u64, file_size);
        let buffer = reader.read_range(current_offset, end_offset).await?;

        if buffer.is_empty() {
            // Reached end of file
            return Ok(current_offset);
        }

        // Search for newline in the chunk we just read
        if let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
            // Found newline, next record starts after it
            return Ok(current_offset + pos as u64 + 1);
        }

        current_offset += buffer.len() as u64;
    }
}

/// Estimate the number of rows in a byte range by sampling
pub async fn estimate_rows_in_range(
    reader: &dyn ByteReader,
    start: u64,
    end: u64,
) -> Result<Option<u64>> {
    if start >= end {
        return Ok(Some(0));
    }

    // Sample the first few KB to estimate average row size
    let sample_size = std::cmp::min(SAMPLE_SIZE as u64, end - start);
    let sample_end = start + sample_size;

    let buffer = reader.read_range(start, sample_end).await?;

    if buffer.is_empty() {
        return Ok(Some(0));
    }

    // Count newlines in the sample
    let newline_count = buffer.iter().filter(|&&b| b == b'\n').count();

    if newline_count == 0 {
        // Can't estimate if no newlines in sample
        return Ok(None);
    }

    // Estimate average bytes per row
    let avg_bytes_per_row = buffer.len() / newline_count;
    let total_bytes = end - start;
    let estimated_rows = total_bytes / avg_bytes_per_row as u64;

    Ok(Some(estimated_rows))
}

/// In-memory `ByteReader` implementation for tests across the crate. Wraps a
/// `Vec<u8>` and serves `read_range` against the slice. Reachable from other
/// modules' `#[cfg(test)]` code as `crate::io::byte_reader::MockByteReader`.
#[cfg(test)]
pub(crate) struct MockByteReader {
    pub data: Vec<u8>,
}

#[cfg(test)]
impl MockByteReader {
    pub fn new(data: impl Into<Vec<u8>>) -> Self {
        Self { data: data.into() }
    }
}

#[cfg(test)]
#[async_trait]
impl ByteReader for MockByteReader {
    async fn size(&self) -> Result<u64> {
        Ok(self.data.len() as u64)
    }

    async fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        let start = start as usize;
        let end = std::cmp::min(end as usize, self.data.len());
        Ok(self.data[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_find_next_record_boundary() {
        let reader = MockByteReader::new(b"line1\nline2\nline3\n".to_vec());

        let boundary = find_next_record_boundary(&reader, 0).await.unwrap();
        assert_eq!(boundary, 6);

        let boundary = find_next_record_boundary(&reader, 3).await.unwrap();
        assert_eq!(boundary, 6);

        let boundary = find_next_record_boundary(&reader, 6).await.unwrap();
        assert_eq!(boundary, 12);
    }

    #[tokio::test]
    async fn test_estimate_rows() {
        let reader = MockByteReader::new(b"line1\nline2\nline3\nline4\nline5\n".to_vec());
        let size = reader.size().await.unwrap();
        let estimate = estimate_rows_in_range(&reader, 0, size).await.unwrap();

        let count = estimate.expect("non-empty range estimates rows");
        assert!((4..=6).contains(&count)); // Allow some variance in estimation
    }
}
