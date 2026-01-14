//! Adapter to bridge ByteReader to AsyncRead + AsyncSeek for Arrow.
//!
//! Arrow's ParquetRecordBatchStream requires an AsyncRead + AsyncSeek implementation.
//! This adapter wraps our ByteReader trait to provide that interface, with buffering
//! to minimize byte-range requests (especially important for S3).

use anyhow::Result;
use bytes::Bytes;
use futures::future::BoxFuture;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

use crate::config::PARQUET_BUFFER_SIZE;
use crate::io::ByteReader;

/// State for tracking in-progress buffer loads
enum ReadState {
    Idle,
    Loading {
        future: BoxFuture<'static, Result<Vec<u8>>>,
        target_position: u64,
    },
}

/// Adapter that implements AsyncRead + AsyncSeek over a ByteReader
///
/// This allows Arrow's ParquetRecordBatchStream to work with our ByteReader abstraction.
/// The adapter maintains an internal buffer to minimize byte-range requests.
pub struct ByteReaderAdapter<R: ByteReader> {
    reader: Arc<R>,
    file_size: u64,
    position: u64,
    buffer: Option<Bytes>,
    buffer_start: u64,
    read_state: ReadState,
}

impl<R: ByteReader> ByteReaderAdapter<R> {
    /// Create a new ByteReaderAdapter
    pub async fn new(reader: R) -> Result<Self> {
        let file_size = reader.size().await?;
        Ok(Self {
            reader: Arc::new(reader),
            file_size,
            position: 0,
            buffer: None,
            buffer_start: 0,
            read_state: ReadState::Idle,
        })
    }

    /// Check if current position is within the buffered range
    fn is_position_in_buffer(&self) -> bool {
        if let Some(ref buffer) = self.buffer {
            let buffer_end = self.buffer_start + buffer.len() as u64;
            self.position >= self.buffer_start && self.position < buffer_end
        } else {
            false
        }
    }

    /// Read data from buffer at current position
    fn read_from_buffer(&mut self, buf: &mut [u8]) -> usize {
        if let Some(ref buffer) = self.buffer {
            let offset = (self.position - self.buffer_start) as usize;
            let available = buffer.len() - offset;
            let to_read = std::cmp::min(buf.len(), available);

            buf[..to_read].copy_from_slice(&buffer[offset..offset + to_read]);
            self.position += to_read as u64;
            to_read
        } else {
            0
        }
    }
}

impl<R: ByteReader + 'static> AsyncRead for ByteReaderAdapter<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // If at EOF, return immediately
        if self.position >= self.file_size {
            return Poll::Ready(Ok(()));
        }

        // If position is in buffer, read from it
        if self.is_position_in_buffer() {
            let bytes_read = self.read_from_buffer(buf.initialize_unfilled());
            buf.advance(bytes_read);
            return Poll::Ready(Ok(()));
        }

        // Need to load buffer - check if we have a pending read
        loop {
            match &mut self.read_state {
                ReadState::Idle => {
                    // Start a new read
                    let reader = Arc::clone(&self.reader);
                    let position = self.position;
                    let file_size = self.file_size;

                    let read_size = std::cmp::min(PARQUET_BUFFER_SIZE as u64, file_size - position);
                    let end_pos = position + read_size;

                    // Create future and store it
                    let future: BoxFuture<'static, Result<Vec<u8>>> =
                        Box::pin(async move { reader.read_range(position, end_pos).await });

                    self.read_state = ReadState::Loading {
                        future,
                        target_position: position,
                    };
                    // Loop to immediately poll the future
                }
                ReadState::Loading {
                    future,
                    target_position,
                } => {
                    // Poll the stored future
                    match future.as_mut().poll(cx) {
                        Poll::Ready(Ok(data)) => {
                            // Store the loaded data in buffer
                            let pos = *target_position;
                            self.buffer_start = pos;
                            self.buffer = Some(Bytes::from(data));
                            self.read_state = ReadState::Idle;

                            // Now read from buffer
                            let bytes_read = self.read_from_buffer(buf.initialize_unfilled());
                            buf.advance(bytes_read);
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Ready(Err(e)) => {
                            self.read_state = ReadState::Idle;
                            return Poll::Ready(Err(io::Error::other(e)));
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }
}

impl<R: ByteReader + 'static> AsyncSeek for ByteReaderAdapter<R> {
    fn start_seek(mut self: Pin<&mut Self>, seek_pos: io::SeekFrom) -> io::Result<()> {
        let new_position = match seek_pos {
            io::SeekFrom::Start(pos) => pos as i64,
            io::SeekFrom::End(offset) => self.file_size as i64 + offset,
            io::SeekFrom::Current(offset) => self.position as i64 + offset,
        };

        if new_position < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot seek to negative position",
            ));
        }

        self.position = new_position as u64;

        // Invalidate buffer if position moved outside buffer range
        if !self.is_position_in_buffer() {
            self.buffer = None;
        }

        // Cancel any pending read since we've moved position
        self.read_state = ReadState::Idle;

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    /// Mock ByteReader for testing
    struct MockByteReader {
        data: Vec<u8>,
    }

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

    #[tokio::test]
    async fn test_adapter_sequential_read() {
        let data = b"Hello, World! This is a test.";
        let reader = MockByteReader {
            data: data.to_vec(),
        };

        let mut adapter = ByteReaderAdapter::new(reader).await.unwrap();

        let mut buf = vec![0u8; 13];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"Hello, World!");

        let mut buf = vec![0u8; 16];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b" This is a test.");
    }

    #[tokio::test]
    async fn test_adapter_seek_and_read() {
        let data = b"0123456789ABCDEFGHIJ";
        let reader = MockByteReader {
            data: data.to_vec(),
        };

        let mut adapter = ByteReaderAdapter::new(reader).await.unwrap();

        // Seek to position 10
        adapter.seek(io::SeekFrom::Start(10)).await.unwrap();

        let mut buf = vec![0u8; 5];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ABCDE");

        // Seek from end
        adapter.seek(io::SeekFrom::End(-5)).await.unwrap();

        let mut buf = vec![0u8; 5];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"FGHIJ");
    }

    #[tokio::test]
    async fn test_adapter_seek_current() {
        let data = b"0123456789";
        let reader = MockByteReader {
            data: data.to_vec(),
        };

        let mut adapter = ByteReaderAdapter::new(reader).await.unwrap();

        // Read 5 bytes
        let mut buf = vec![0u8; 5];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"01234");

        // Seek forward 2 from current
        adapter.seek(io::SeekFrom::Current(2)).await.unwrap();

        let mut buf = vec![0u8; 2];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"78");

        // Seek backward 5 from current (from position 9 to position 4)
        adapter.seek(io::SeekFrom::Current(-5)).await.unwrap();

        let mut buf = vec![0u8; 2];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"45"); // Position 4-5
    }

    #[tokio::test]
    async fn test_adapter_read_at_eof() {
        let data = b"short";
        let reader = MockByteReader {
            data: data.to_vec(),
        };

        let mut adapter = ByteReaderAdapter::new(reader).await.unwrap();

        // Read all data
        let mut buf = vec![0u8; 5];
        adapter.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"short");

        // Try to read more - should return 0 bytes
        let mut buf = vec![0u8; 10];
        let n = adapter.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_adapter_seek_beyond_file() {
        let data = b"test";
        let reader = MockByteReader {
            data: data.to_vec(),
        };

        let mut adapter = ByteReaderAdapter::new(reader).await.unwrap();

        // Seek beyond file
        adapter.seek(io::SeekFrom::Start(100)).await.unwrap();

        // Read should return 0 bytes
        let mut buf = vec![0u8; 10];
        let n = adapter.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_adapter_buffering() {
        // Create data larger than buffer size
        let data = vec![0xABu8; 512 * 1024]; // 512 KB
        let reader = MockByteReader { data };

        let mut adapter = ByteReaderAdapter::new(reader).await.unwrap();

        // Read in chunks
        let mut total_read = 0;
        let mut buf = vec![0u8; 1024];

        while total_read < 512 * 1024 {
            let n = adapter.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            assert_eq!(&buf[..n], &vec![0xABu8; n]);
            total_read += n;
        }

        assert_eq!(total_read, 512 * 1024);
    }
}
