//! High-level runner API for the DSQL data loader.
//!
//! This module provides a simplified public interface that encapsulates all the
//! internal complexity of setting up connections, readers, coordinators, etc.
//!
//! This is the primary API for external users and for the CLI.

use anyhow::Result;
use aws_config::{BehaviorVersion, Region};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::coordination::manifest::{LocalManifestStorage, ParquetConfig};
use crate::coordination::{Coordinator, DsqlConfig, FileFormat, LoadConfig};
use crate::db::pool::PoolArgsBuilder;
use crate::db::{self as db_pool, SchemaInferrer};
use crate::formats::{DelimitedConfig, ReaderFactory};
use crate::io::SourceUri;

/// File format for the source data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Csv,
    Tsv,
    Parquet,
}

impl Format {
    /// Parse format from string (case-insensitive)
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(Format::Csv),
            "tsv" => Ok(Format::Tsv),
            "parquet" => Ok(Format::Parquet),
            _ => Err(anyhow::anyhow!(
                "Unsupported format: {}. Supported formats: csv, tsv, parquet",
                s
            )),
        }
    }

    /// Convert to internal Format type
    fn to_internal(self) -> crate::formats::Format {
        match self {
            Format::Csv => crate::formats::Format::Csv,
            Format::Tsv => crate::formats::Format::Tsv,
            Format::Parquet => crate::formats::Format::Parquet,
        }
    }
}

/// Arguments for running a data load operation
#[derive(Debug, Clone)]
pub struct LoadArgs {
    // Connection configuration
    pub endpoint: String,
    pub region: String,
    pub username: String,

    // Source configuration
    pub source_uri: String,
    pub target_table: String,
    pub schema: String,
    pub format: Format,

    // Performance tuning
    pub worker_count: usize,
    pub partition_size_bytes: u64,
    pub batch_size: usize,
    pub batch_concurrency: usize,

    // Options
    pub create_table_if_missing: bool,
    pub manifest_dir: Option<PathBuf>,
    pub quiet: bool,

    // Column mapping: source column name -> destination column name
    pub column_mappings: HashMap<String, String>,

    // Test-only: inject a pre-created pool (for SQLite testing)
    #[cfg(test)]
    pub test_pool: Option<crate::db::Pool>,
}

/// Result of a completed data load operation
#[derive(Debug)]
pub struct LoadResult {
    pub job_id: String,
    pub partitions_processed: usize,
    pub records_loaded: u64,
    pub records_failed: u64,
    pub duration: Duration,
    /// Path to persisted manifest directory (if errors occurred and temp dir was used)
    pub persisted_manifest_dir: Option<PathBuf>,
}

/// Run a data load operation with the specified arguments
///
/// This is the main entry point for loading data into DSQL. It handles all the
/// internal setup including:
/// - Creating connection pools with IAM authentication
/// - Setting up file readers (local or S3)
/// - Creating manifest storage for coordination
/// - Spawning workers and tracking progress
///
/// # Example
///
/// ```no_run
/// use aurora_dsql_loader::runner::{LoadArgs, Format, run_load};
/// use std::collections::HashMap;
///
/// # async fn example() -> anyhow::Result<()> {
/// let args = LoadArgs {
///     endpoint: "xxx.dsql.us-west-2.on.aws".to_string(),
///     region: "us-west-2".to_string(),
///     username: "admin".to_string(),
///     source_uri: "s3://my-bucket/data.csv".to_string(),
///     target_table: "my_table".to_string(),
///     schema: "public".to_string(),
///     format: Format::Csv,
///     worker_count: 8,
///     partition_size_bytes: 10 * 1024 * 1024, // 10MB
///     batch_size: 3000,
///     batch_concurrency: 20,
///     create_table_if_missing: true,
///     manifest_dir: None,
///     column_mappings: HashMap::new(),
///     quiet: true,
/// };
///
/// let result = run_load(args).await?;
/// println!("Loaded {} records in {:?}", result.records_loaded, result.duration);
/// # Ok(())
/// # }
/// ```
pub async fn run_load(args: LoadArgs) -> Result<LoadResult> {
    // Set up manifest directory (use temp dir if not provided)
    let (mut temp_dir, manifest_dir_path) = if let Some(dir) = args.manifest_dir {
        (None, dir)
    } else {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();
        (Some(temp_dir), path)
    };

    // Create connection pool (or use test pool if provided)
    #[cfg(test)]
    let pool = if let Some(test_pool) = args.test_pool {
        test_pool
    } else {
        let pool_args = PoolArgsBuilder::default()
            .region(Region::new(args.region.clone()))
            .endpoint(&args.endpoint)
            .username(&args.username)
            .build()?;
        db_pool::pool::pool(pool_args).await?
    };

    #[cfg(not(test))]
    let pool = {
        let pool_args = PoolArgsBuilder::default()
            .region(Region::new(args.region.clone()))
            .endpoint(&args.endpoint)
            .username(&args.username)
            .build()?;
        db_pool::pool::pool(pool_args).await?
    };

    // Parse source URI
    let parsed_uri = SourceUri::parse(&args.source_uri)?;

    // Load AWS config (needed for S3 access)
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(args.region.clone()))
        .load()
        .await;

    // Create reader factory and file reader
    let reader_factory = ReaderFactory::new(&aws_config);
    let file_reader = reader_factory
        .create_reader(&parsed_uri, args.format.to_internal())
        .await?;

    // Determine file format config based on format
    let (has_header, file_format) = match args.format {
        Format::Csv => (true, FileFormat::Csv(DelimitedConfig::csv())),
        Format::Tsv => (true, FileFormat::Tsv(DelimitedConfig::tsv())),
        Format::Parquet => (false, FileFormat::Parquet(ParquetConfig::default())),
    };

    // Create manifest storage
    let manifest_storage = Arc::new(LocalManifestStorage::new(manifest_dir_path));

    // Create schema inferrer
    let schema_inferrer = SchemaInferrer { has_header };

    // Create coordinator
    let coordinator = Coordinator::new(manifest_storage, file_reader, schema_inferrer, pool);

    // Build load config
    let load_config = LoadConfig {
        source_uri: args.source_uri,
        target_table: args.target_table,
        schema: args.schema,
        dsql_config: DsqlConfig {
            endpoint: args.endpoint,
            region: args.region,
            username: args.username,
        },
        worker_count: args.worker_count,
        partition_size_bytes: args.partition_size_bytes,
        batch_size: args.batch_size,
        batch_concurrency: args.batch_concurrency,
        create_table_if_missing: args.create_table_if_missing,
        file_format,
        column_mappings: args.column_mappings,
        quiet: args.quiet,
    };

    // Run the load
    let result = coordinator.run_load(load_config).await?;

    // If there were errors and we used a temp directory, persist it for debugging
    let persisted_manifest_dir = if result.records_failed > 0 && temp_dir.is_some() {
        let temp = temp_dir.take().unwrap();
        let persisted_path = temp.keep();
        Some(persisted_path)
    } else {
        None
    };

    // Convert to public LoadResult type
    Ok(LoadResult {
        job_id: result.job_id,
        partitions_processed: result.partitions_processed,
        records_loaded: result.records_loaded,
        records_failed: result.records_failed,
        duration: result.duration,
        persisted_manifest_dir,
    })
}
