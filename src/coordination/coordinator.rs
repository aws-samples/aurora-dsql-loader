use anyhow::{Context, Result};
use chrono::Utc;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use super::manifest::{
    DsqlConfig, FileFormat, ManifestFile, ManifestStorage, PartitionInfo, PartitionResultFile,
};
use super::worker::Worker;
use crate::db::{Pool, SchemaInferrer};
use crate::formats::FileReader;
use crate::telemetry::{ProgressStats, TelemetryEvent};

/// Maximum bytes to read for schema inference
///
/// Caps the amount of data read to prevent loading entire large files just for
/// schema detection. 10MB is sufficient to capture thousands of rows for accurate
/// type inference while maintaining reasonable memory usage.
const MAX_SCHEMA_INFERENCE_BYTES: u64 = 10 * 1024 * 1024; // 10 MB

/// Configuration for a data load operation
#[derive(Debug, Clone)]
pub struct LoadConfig {
    pub source_uri: String,
    pub target_table: String,
    pub dsql_config: DsqlConfig,
    pub worker_count: usize,
    pub partition_size_bytes: u64,
    pub batch_size: usize,
    pub batch_concurrency: usize,
    pub create_table_if_missing: bool,
    pub file_format: FileFormat,
    pub column_mappings: std::collections::HashMap<String, String>,
    pub quiet: bool,
}

/// Result of a completed data load operation
#[derive(Debug)]
pub struct LoadResult {
    pub job_id: String,
    pub partitions_processed: usize,
    pub records_loaded: u64,
    pub records_failed: u64,
    pub duration: Duration,
    /// Detailed results for each partition (accessed in integration tests)
    #[cfg_attr(not(test), allow(dead_code))]
    pub partition_results: Vec<PartitionResultFile>,
}

/// The Coordinator orchestrates the data load process.
pub struct Coordinator {
    manifest_storage: Arc<dyn ManifestStorage>,
    file_reader: Arc<dyn FileReader>,
    schema_inferrer: SchemaInferrer,
    pool: Pool,
}

impl Coordinator {
    /// Create a new Coordinator instance
    pub fn new(
        manifest_storage: Arc<dyn ManifestStorage>,
        file_reader: Arc<dyn FileReader>,
        schema_inferrer: SchemaInferrer,
        pool: Pool,
    ) -> Self {
        Self {
            manifest_storage,
            file_reader,
            schema_inferrer,
            pool,
        }
    }

    /// Run the complete data load operation
    ///
    /// This method orchestrates the entire load process:
    /// 1. Generate unique job ID
    /// 2. Create partitions from the source file
    /// 3. Optionally infer schema from sample data
    /// 4. Optionally create the target table
    /// 5. Write manifest file
    /// 6. Spawn worker tasks to process partitions
    /// 7. Wait for completion and aggregate results
    pub async fn run_load(&self, config: LoadConfig) -> Result<LoadResult> {
        let start_time = Instant::now();

        // 1. Generate job ID
        let job_id = Uuid::new_v4().to_string();
        info!("Starting load job: {}", job_id);

        // 2. Create partitions
        let (file_metadata, partitions) = self
            .create_file_partitions(config.partition_size_bytes)
            .await?;

        // 3. Resolve schema (infer or query)
        let mut schema = self.resolve_table_schema(&config, &partitions).await?;

        // Apply column mappings to schema if provided
        Self::apply_column_mappings(&mut schema, &config.column_mappings);

        // 4. Create table if needed
        let table_created = self.ensure_table_exists(&config, &schema).await?;

        // 5. Write manifest
        self.create_and_write_manifest(
            &job_id,
            &config,
            &file_metadata,
            &partitions,
            schema,
            table_created,
        )
        .await?;

        // 6. Create telemetry channel and spawn workers
        let (telemetry_tx, telemetry_rx) = mpsc::unbounded_channel::<TelemetryEvent>();
        let worker_handles = self.spawn_worker_pool(&job_id, &config, telemetry_tx.clone());

        // Drop the coordinator's copy of the sender so the channel closes when workers finish
        drop(telemetry_tx);

        // 7. Setup progress tracking
        let prog_jh =
            Self::setup_progress_tracking(&config, &file_metadata, &partitions, telemetry_rx);

        // 8. Wait for all workers to complete
        let worker_results = futures::future::join_all(worker_handles).await;

        // Wait for the progress bar to finish so we don't collide output
        if let Some(jh) = prog_jh {
            let _ = jh.await;
        }

        // Check for any worker errors
        for (i, result) in worker_results.iter().enumerate() {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!("Worker {} failed: {:#}", i, e);
                }
                Err(e) => {
                    warn!("Worker {} panicked: {:#}", i, e);
                }
            }
        }

        // 9. Aggregate results
        self.aggregate_final_results(&job_id, &partitions, start_time)
            .await
    }

    /// Collect all partition results from manifest storage
    async fn collect_results(
        &self,
        job_id: &str,
        partition_count: usize,
    ) -> Result<Vec<PartitionResultFile>> {
        let mut results = Vec::new();

        for partition_id in 0..partition_count as u32 {
            match self
                .manifest_storage
                .read_result(job_id, partition_id)
                .await
            {
                Ok(result) => results.push(result),
                Err(_) => {
                    // Partition result may be missing if worker crashed before writing result file
                    // This is tracked in failure metrics and doesn't require hard failure
                    tracing::warn!("Partition {partition_id} result missing");
                }
            }
        }

        Ok(results)
    }

    /// Create partitions from the source file and return metadata
    async fn create_file_partitions(
        &self,
        partition_size_bytes: u64,
    ) -> Result<(
        crate::formats::reader::FileMetadata,
        Vec<crate::formats::reader::Partition>,
    )> {
        info!(
            "Creating partitions with target size: {} bytes",
            partition_size_bytes
        );

        let file_metadata = self
            .file_reader
            .metadata()
            .await
            .context("Failed to get file metadata")?;

        let partitions = self
            .file_reader
            .create_partitions(partition_size_bytes)
            .await
            .context("Failed to create partitions")?;

        info!(
            "Created {} partitions for file of {} bytes (estimated {} rows)",
            partitions.len(),
            file_metadata.file_size_bytes,
            file_metadata.estimated_rows.unwrap_or(0)
        );

        Ok((file_metadata, partitions))
    }

    /// Resolve the table schema: either infer from data or query existing table
    async fn resolve_table_schema(
        &self,
        config: &LoadConfig,
        partitions: &[crate::formats::reader::Partition],
    ) -> Result<Option<crate::db::schema::Schema>> {
        if config.create_table_if_missing {
            info!("Inferring schema from sample data...");

            // For schema inference, we need to read from the beginning of the file
            // including the header, not from partition 0 (which starts after the header)
            let first_partition = partitions
                .first()
                .context("No partitions available for schema inference")?;

            // Cap the sample size to avoid reading massive files
            let end_offset = first_partition.end_offset.min(MAX_SCHEMA_INFERENCE_BYTES);

            let sample_partition = crate::formats::reader::Partition {
                partition_id: 0,
                start_offset: 0,
                end_offset,
                estimated_rows: first_partition.estimated_rows,
            };

            let sample_data = self
                .file_reader
                .read_partition(&sample_partition)
                .await
                .context("Failed to read sample data for schema inference")?;

            // Convert Records to FieldValues (Vec<Vec<String>>)
            let field_values: Vec<Vec<String>> = sample_data
                .records
                .iter()
                .map(|record| record.fields.clone())
                .collect();

            let inferred_schema = self
                .schema_inferrer
                .infer_from_data(&field_values)
                .context("Failed to infer schema")?;

            info!(
                "Inferred schema with {} columns",
                inferred_schema.columns.len()
            );

            Ok(Some(inferred_schema))
        } else {
            // Query existing table schema for proper type handling
            info!("Querying schema from existing table...");
            let queried_schema = crate::db::schema::query_table_schema(
                &self.pool,
                &config.target_table,
            )
            .await
            .with_context(|| {
                format!(
                    "Table '{}' does not exist. Use --if-not-exists to create it automatically.",
                    config.target_table
                )
            })?;

            info!(
                "Retrieved schema with {} columns",
                queried_schema.columns.len()
            );
            Ok(Some(queried_schema))
        }
    }

    /// Apply column mappings to schema if provided
    fn apply_column_mappings(
        schema: &mut Option<crate::db::schema::Schema>,
        column_mappings: &std::collections::HashMap<String, String>,
    ) {
        if !column_mappings.is_empty()
            && let Some(s) = schema
        {
            for col in &mut s.columns {
                if let Some(new_name) = column_mappings.get(&col.name) {
                    info!("Mapping column: {} -> {}", col.name, new_name);
                    col.name = new_name.clone();
                }
            }
        }
    }

    /// Create the table if it doesn't exist (when create_table_if_missing is true)
    async fn ensure_table_exists(
        &self,
        config: &LoadConfig,
        schema: &Option<crate::db::schema::Schema>,
    ) -> Result<bool> {
        if config.create_table_if_missing {
            if let Some(schema) = schema {
                info!("Creating table: {}", config.target_table);
                let ddl = self
                    .schema_inferrer
                    .generate_ddl(&config.target_table, schema);

                match self.pool.execute_query(&ddl).await {
                    Ok(_) => {
                        info!("Table created successfully");
                        Ok(true)
                    }
                    Err(e) => {
                        // Check if error is "table already exists" - if so, that's ok
                        let error_msg = e.to_string();
                        if error_msg.contains("already exists") {
                            info!("Table already exists, continuing...");
                            Ok(false)
                        } else {
                            Err(e).context("Failed to create table")
                        }
                    }
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Create and write the manifest file
    async fn create_and_write_manifest(
        &self,
        job_id: &str,
        config: &LoadConfig,
        file_metadata: &crate::formats::reader::FileMetadata,
        partitions: &[crate::formats::reader::Partition],
        schema: Option<crate::db::schema::Schema>,
        table_created: bool,
    ) -> Result<()> {
        // Detect unique constraints
        info!("Checking for unique constraints on table...");
        let has_unique_constraints = self
            .pool
            .has_unique_constraints(&config.target_table)
            .await
            .unwrap_or(false);

        if has_unique_constraints {
            info!("Table has unique constraints - will use ON CONFLICT DO NOTHING");
        }

        // Build manifest
        let partition_infos: Vec<PartitionInfo> = partitions
            .iter()
            .enumerate()
            .map(|(idx, p)| PartitionInfo {
                partition_id: idx as u32,
                start_offset: p.start_offset,
                end_offset: p.end_offset,
                estimated_rows: p.estimated_rows,
            })
            .collect();

        let table = super::manifest::TableInfo {
            name: config.target_table.clone(),
            schema: schema.map(|s| s.into()),
            was_created: table_created,
            has_unique_constraints,
        };

        let manifest = ManifestFile {
            job_id: job_id.to_string(),
            created_at: Utc::now().to_rfc3339(),
            source_uri: config.source_uri.clone(),
            table,
            file_format: config.file_format.clone(),
            dsql_config: config.dsql_config.clone(),
            total_size_bytes: file_metadata.file_size_bytes,
            estimated_rows: file_metadata.estimated_rows,
            batch_size: config.batch_size,
            partitions: partition_infos,
        };

        self.manifest_storage
            .write_manifest(job_id, &manifest)
            .await
            .context("Failed to write manifest")?;

        info!("Manifest written for job: {}", job_id);
        Ok(())
    }

    /// Spawn worker tasks to process partitions
    fn spawn_worker_pool(
        &self,
        job_id: &str,
        config: &LoadConfig,
        telemetry_tx: mpsc::UnboundedSender<TelemetryEvent>,
    ) -> Vec<tokio::task::JoinHandle<Result<()>>> {
        info!("Spawning {} workers...", config.worker_count);
        let mut worker_handles = Vec::new();

        for _i in 0..config.worker_count {
            let worker = Worker::new(
                Arc::clone(&self.manifest_storage),
                self.pool.clone(),
                config.batch_size,
                config.batch_concurrency,
                telemetry_tx.clone(),
            );

            let job_id_clone = job_id.to_string();
            let file_reader = Arc::clone(&self.file_reader);

            let handle = tokio::spawn(async move { worker.run(&job_id_clone, file_reader).await });

            worker_handles.push(handle);
        }

        worker_handles
    }

    /// Setup progress tracking with progress bars
    fn setup_progress_tracking(
        config: &LoadConfig,
        file_metadata: &crate::formats::reader::FileMetadata,
        partitions: &[crate::formats::reader::Partition],
        mut telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if config.quiet {
            return None;
        }

        // Create progress bars with MultiProgress
        let multi_progress = MultiProgress::new();
        let total_partitions = partitions.len() as u64;
        let estimated_total_rows = file_metadata.estimated_rows.unwrap_or(0);

        let partition_bar = multi_progress.add(ProgressBar::new(total_partitions));
        partition_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] Partitions: [{bar:30.cyan/blue}] {pos}/{len} ({percent}%)",
                )
                .unwrap()
                .progress_chars("=>-"),
        );

        let rows_bar = if estimated_total_rows > 0 {
            let bar = multi_progress.add(ProgressBar::new(estimated_total_rows));
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] Rows:       [{bar:30.green/blue}] {human_pos}/{human_len} ({percent}%) | {per_sec}")
                    .unwrap()
                    .progress_chars("=>-")
            );
            Some(bar)
        } else {
            None
        };

        let bytes_bar = multi_progress.add(ProgressBar::new(file_metadata.file_size_bytes));
        bytes_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] Bytes:      [{bar:30.yellow/blue}] {bytes}/{total_bytes} ({percent}%) | {bytes_per_sec}")
                .unwrap()
                .progress_chars("=>-")
        );

        let stats_bar = multi_progress.add(ProgressBar::new(0));
        stats_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] Batch Time: {msg}")
                .unwrap(),
        );

        // Spawn telemetry processing task
        Some(tokio::spawn(async move {
            let mut stats = ProgressStats::new();

            while let Some(event) = telemetry_rx.recv().await {
                stats.update(&event);

                // Update progress bars
                partition_bar.set_position(stats.partitions_completed as u64);
                if let Some(ref bar) = rows_bar {
                    bar.set_position(stats.records_loaded);
                }
                bytes_bar.set_position(stats.bytes_processed);

                // Update batch timing percentiles
                let (p50, p90, p99) = stats.get_percentiles();
                if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                    stats_bar
                        .set_message(format!("p50: {}ms, p90: {}ms, p99: {}ms", p50, p90, p99));
                }
            }

            // Finish progress bars
            partition_bar.finish_with_message("All partitions completed");
            if let Some(bar) = rows_bar {
                bar.finish();
            }
            bytes_bar.finish();

            // Final percentile stats
            let (p50, p90, p99) = stats.get_percentiles();
            if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                stats_bar
                    .finish_with_message(format!("p50: {}ms, p90: {}ms, p99: {}ms", p50, p90, p99));
            } else {
                stats_bar.finish();
            }
        }))
    }

    /// Aggregate the final results from all partition result files
    async fn aggregate_final_results(
        &self,
        job_id: &str,
        partitions: &[crate::formats::reader::Partition],
        start_time: Instant,
    ) -> Result<LoadResult> {
        info!("Aggregating results...");
        let partition_results = self
            .collect_results(job_id, partitions.len())
            .await
            .context("Failed to collect results")?;

        let total_records_loaded: u64 = partition_results.iter().map(|r| r.records_loaded).sum();
        let total_records_failed: u64 = partition_results.iter().map(|r| r.records_failed).sum();
        let duration = start_time.elapsed();

        info!(
            "Load complete: {} partitions, {} records loaded, {} records failed in {:.2}s",
            partition_results.len(),
            total_records_loaded,
            total_records_failed,
            duration.as_secs_f64()
        );

        Ok(LoadResult {
            job_id: job_id.to_string(),
            partitions_processed: partition_results.len(),
            records_loaded: total_records_loaded,
            records_failed: total_records_failed,
            duration,
            partition_results,
        })
    }
}
