use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use std::sync::Arc;

use super::byte_reader::ByteReader;

/// ByteReader implementation for S3 objects
#[derive(Clone)]
pub struct S3ByteReader {
    s3_client: Arc<S3Client>,
    bucket: String,
    key: String,
}

impl S3ByteReader {
    /// Create a new S3ByteReader
    pub fn new(s3_client: Arc<S3Client>, bucket: String, key: String) -> Self {
        Self {
            s3_client,
            bucket,
            key,
        }
    }
}

#[async_trait]
impl ByteReader for S3ByteReader {
    async fn size(&self) -> Result<u64> {
        // Use HeadObject to get file size
        let head_response = self
            .s3_client
            .head_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .send()
            .await
            .context("Failed to get S3 object metadata")?;

        let size = head_response
            .content_length()
            .ok_or_else(|| anyhow::anyhow!("S3 object missing content-length"))?
            as u64;

        Ok(size)
    }

    async fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        // Use S3 range request to read the data
        let range = format!("bytes={}-{}", start, end - 1);

        let response = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(range)
            .send()
            .await
            .context("Failed to read range from S3")?;

        let bytes = response
            .body
            .collect()
            .await
            .context("Failed to collect S3 response body")?
            .into_bytes()
            .to_vec();

        Ok(bytes)
    }
}
