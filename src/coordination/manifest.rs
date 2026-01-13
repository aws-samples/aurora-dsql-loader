use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::formats::DelimitedConfig;

/// DSQL connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DsqlConfig {
    pub endpoint: String,
    pub region: String,
    pub username: String,
}

/// Schema column definition (JSON serialization format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnJson {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
}

/// Table schema definition (JSON serialization format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaJson {
    pub columns: Vec<ColumnJson>,
}

/// Configuration for Parquet files
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParquetConfig {
    // Future: row group batch size, compression settings, etc.
}

/// File format and its associated configuration
///
/// This enum encapsulates both the file format type and its specific configuration.
/// Uses serde's adjacently tagged enum serialization for a clean nested JSON structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format", content = "config")]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Csv(DelimitedConfig),
    Tsv(DelimitedConfig),
    Parquet(ParquetConfig),
}

/// Information about a chunk to be processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub chunk_id: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
}

/// Default schema name for backward compatibility
fn default_schema() -> String {
    "public".to_string()
}

/// Table metadata and configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    /// Database schema name (e.g., "public", "sales")
    #[serde(default = "default_schema")]
    pub schema_name: String,
    /// Table structure (column definitions)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaJson>,
    pub was_created: bool,
    #[serde(default)]
    pub has_unique_constraints: bool,
}

/// The manifest file structure written by the coordinator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    pub job_id: String,
    pub created_at: String, // ISO 8601
    pub source_uri: String,
    pub table: TableInfo,
    #[serde(flatten)]
    pub file_format: FileFormat,
    pub dsql_config: DsqlConfig,
    pub total_size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    pub batch_size: usize,
    pub chunks: Vec<ChunkInfo>,
}

/// Claim file structure created when a worker claims a chunk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimFile {
    pub chunk_id: u32,
    pub worker_id: String,
    pub claimed_at: String, // ISO 8601
}

/// Status of a chunk processing operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChunkStatus {
    Success,
    Failed,
}

/// The result file structure written by workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkResultFile {
    pub chunk_id: u32,
    pub worker_id: String,
    pub status: ChunkStatus,
    pub records_loaded: u64,
    pub records_failed: u64,
    pub bytes_processed: u64,
    pub started_at: String,   // ISO 8601
    pub completed_at: String, // ISO 8601
    pub duration_secs: u64,
    pub errors: Vec<ErrorRecord>,
}

/// Record of an error that occurred during processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorRecord {
    pub line_number: u64,
    pub error_type: String,
    pub error_message: String,
}

/// Trait for manifest storage operations
///
/// This trait abstracts the coordination mechanism used by workers and the coordinator.
/// Implementations can use local filesystem, S3, databases, or other storage backends.
#[async_trait::async_trait]
pub trait ManifestStorage: Send + Sync {
    /// Write the manifest file for a job
    async fn write_manifest(&self, job_id: &str, manifest: &ManifestFile) -> Result<()>;

    /// Read the manifest file for a job
    async fn read_manifest(&self, job_id: &str) -> Result<ManifestFile>;

    /// Attempt to claim a chunk atomically
    /// Returns true if the claim was successful, false if already claimed
    async fn try_claim_chunk(&self, job_id: &str, chunk_id: u32, claim: &ClaimFile)
    -> Result<bool>;

    /// Write a result file for a completed chunk
    async fn write_result(
        &self,
        job_id: &str,
        chunk_id: u32,
        result: &ChunkResultFile,
    ) -> Result<()>;

    /// Read a result file for a completed chunk
    async fn read_result(&self, job_id: &str, chunk_id: u32) -> Result<ChunkResultFile>;

    /// List all chunk IDs that have not been claimed yet
    async fn list_unclaimed_chunks(&self, job_id: &str) -> Result<Vec<u32>>;
}

/// Local filesystem implementation of ManifestStorage
///
/// Uses atomic file operations to ensure correct coordination between workers.
/// Directory structure:
///   {base_dir}/jobs/{job_id}/manifest.json
///   {base_dir}/jobs/{job_id}/chunks/{chunk_id:04}.claim
///   {base_dir}/jobs/{job_id}/chunks/{chunk_id:04}.result
pub struct LocalManifestStorage {
    base_dir: PathBuf,
}

impl LocalManifestStorage {
    /// Create a new LocalManifestStorage with the given base directory
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Get the job directory path
    fn job_dir(&self, job_id: &str) -> PathBuf {
        self.base_dir.join("jobs").join(job_id)
    }

    /// Get the chunks directory path
    fn chunks_dir(&self, job_id: &str) -> PathBuf {
        self.job_dir(job_id).join("chunks")
    }

    /// Get the manifest file path
    fn manifest_path(&self, job_id: &str) -> PathBuf {
        self.job_dir(job_id).join("manifest.json")
    }

    /// Get the claim file path for a chunk
    fn claim_path(&self, job_id: &str, chunk_id: u32) -> PathBuf {
        self.chunks_dir(job_id)
            .join(format!("{:04}.claim", chunk_id))
    }

    /// Get the result file path for a chunk
    fn result_path(&self, job_id: &str, chunk_id: u32) -> PathBuf {
        self.chunks_dir(job_id)
            .join(format!("{:04}.result", chunk_id))
    }
}

#[async_trait::async_trait]
impl ManifestStorage for LocalManifestStorage {
    async fn write_manifest(&self, job_id: &str, manifest: &ManifestFile) -> Result<()> {
        let manifest_path = self.manifest_path(job_id);

        // Create parent directories
        if let Some(parent) = manifest_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create manifest directory")?;
        }

        // Serialize and write manifest
        let json =
            serde_json::to_string_pretty(manifest).context("Failed to serialize manifest")?;

        fs::write(&manifest_path, json)
            .await
            .context("Failed to write manifest file")?;

        // Create chunks directory
        let chunks_dir = self.chunks_dir(job_id);
        fs::create_dir_all(&chunks_dir)
            .await
            .context("Failed to create chunks directory")?;

        Ok(())
    }

    async fn read_manifest(&self, job_id: &str) -> Result<ManifestFile> {
        let manifest_path = self.manifest_path(job_id);

        let contents = fs::read_to_string(&manifest_path)
            .await
            .context("Failed to read manifest file")?;

        let manifest: ManifestFile =
            serde_json::from_str(&contents).context("Failed to parse manifest file")?;

        Ok(manifest)
    }

    async fn try_claim_chunk(
        &self,
        job_id: &str,
        chunk_id: u32,
        claim: &ClaimFile,
    ) -> Result<bool> {
        let claim_path = self.claim_path(job_id, chunk_id);

        // Serialize claim data
        let json = serde_json::to_string_pretty(claim).context("Failed to serialize claim")?;

        // Try to create the file atomically using create_new
        // This will fail if the file already exists (already claimed)
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&claim_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(json.as_bytes())
                    .await
                    .context("Failed to write claim file")?;
                file.flush().await.context("Failed to flush claim file")?;
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Chunk already claimed
                Ok(false)
            }
            Err(e) => Err(e).context("Failed to create claim file"),
        }
    }

    async fn write_result(
        &self,
        job_id: &str,
        chunk_id: u32,
        result: &ChunkResultFile,
    ) -> Result<()> {
        let result_path = self.result_path(job_id, chunk_id);

        // Serialize result data
        let json = serde_json::to_string_pretty(result).context("Failed to serialize result")?;

        fs::write(&result_path, json)
            .await
            .context("Failed to write result file")?;

        Ok(())
    }

    async fn read_result(&self, job_id: &str, chunk_id: u32) -> Result<ChunkResultFile> {
        let result_path = self.result_path(job_id, chunk_id);

        let contents = fs::read_to_string(&result_path)
            .await
            .context("Failed to read result file")?;

        let result: ChunkResultFile =
            serde_json::from_str(&contents).context("Failed to parse result file")?;

        Ok(result)
    }

    async fn list_unclaimed_chunks(&self, job_id: &str) -> Result<Vec<u32>> {
        // First read the manifest to get the total number of chunks
        let manifest = self.read_manifest(job_id).await?;

        // Check each chunk to see if it has a claim file
        let mut unclaimed = Vec::new();

        for chunk in &manifest.chunks {
            let claim_path = self.claim_path(job_id, chunk.chunk_id);

            // If claim file doesn't exist, chunk is unclaimed
            if !fs::try_exists(&claim_path)
                .await
                .context("Failed to check claim file existence")?
            {
                unclaimed.push(chunk.chunk_id);
            }
        }

        Ok(unclaimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_format_serialization() {
        // Test CSV format serialization
        let csv_format = FileFormat::Csv(DelimitedConfig::csv());

        let json = serde_json::to_string_pretty(&csv_format).unwrap();
        println!("CSV format:\n{}", json);

        // Test TSV format serialization
        let tsv_format = FileFormat::Tsv(DelimitedConfig::tsv());

        let json = serde_json::to_string_pretty(&tsv_format).unwrap();
        println!("TSV format:\n{}", json);

        // Test Parquet format serialization
        let parquet_format = FileFormat::Parquet(ParquetConfig::default());
        let json = serde_json::to_string_pretty(&parquet_format).unwrap();
        println!("Parquet format:\n{}", json);
    }

    #[test]
    fn test_file_format_deserialization() {
        // Test CSV deserialization with new nested structure
        let csv_json = r#"{
            "format": "csv",
            "config": {
                "delimiter": ",",
                "has_header": true,
                "quote": "\""
            }
        }"#;

        let format: FileFormat = serde_json::from_str(csv_json).unwrap();
        match format {
            FileFormat::Csv(config) => {
                assert_eq!(config.delimiter, ",");
                assert!(config.has_header);
                assert_eq!(config.quote, "\"");
            }
            _ => panic!("Expected CSV format"),
        }

        // Test Parquet deserialization with new nested structure
        let parquet_json = r#"{
            "format": "parquet",
            "config": {}
        }"#;

        let format: FileFormat = serde_json::from_str(parquet_json).unwrap();
        assert!(matches!(format, FileFormat::Parquet(_)));
    }

    #[test]
    fn test_manifest_with_file_format() {
        let manifest = ManifestFile {
            job_id: "test-job".to_string(),
            created_at: "2025-12-03T10:00:00Z".to_string(),
            source_uri: "/data/test.csv".to_string(),
            table: TableInfo {
                name: "test_table".to_string(),
                schema_name: "public".to_string(),
                schema: None,
                was_created: false,
                has_unique_constraints: false,
            },
            file_format: FileFormat::Csv(DelimitedConfig::csv()),
            dsql_config: DsqlConfig {
                endpoint: "test.dsql.us-west-2.on.aws".to_string(),
                region: "us-west-2".to_string(),
                username: "admin".to_string(),
            },
            total_size_bytes: 1024,
            estimated_rows: Some(100),
            batch_size: 1000,
            chunks: vec![],
        };

        // Serialize and print
        let json = serde_json::to_string_pretty(&manifest).unwrap();
        println!("Manifest with CSV format:\n{}", json);

        // Deserialize back
        let deserialized: ManifestFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.job_id, "test-job");
        assert!(matches!(deserialized.file_format, FileFormat::Csv(_)));
    }
}
