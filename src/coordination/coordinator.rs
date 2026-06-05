use anyhow::{Context, Result};
use chrono::Utc;
use derive_builder::Builder;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

use super::manifest::{
    ChunkInfo, ChunkResultFile, DsqlConfig, FileFormat, ManifestFile, ManifestStorage, OnConflict,
};
use super::worker::Worker;
use crate::db::schema::{Schema, query_table_schema, validate_schema_exists};
use crate::db::{Pool, SchemaInferrer};
use crate::formats::FileReader;
use crate::formats::reader::{Chunk, FileMetadata};
use crate::telemetry::{ProgressStats, TelemetryEvent};

/// Validate that every excluded name exists in the schema and that at least
/// one column would remain after exclusion.
fn validate_excluded_columns(schema: &Schema, exclude: &[String]) -> Result<()> {
    for name in exclude {
        if !schema.columns.iter().any(|c| &c.name == name) {
            let available: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
            return Err(anyhow::anyhow!(
                "Column '{}' not found in table schema. Available columns: {:?}",
                name,
                available
            ));
        }
    }
    if exclude.len() >= schema.columns.len() {
        return Err(anyhow::anyhow!(
            "Cannot exclude all columns - at least one column must remain for insertion"
        ));
    }
    Ok(())
}

/// Reject any column name present in both --exclude-columns and --column-map.
fn validate_no_exclude_rename_conflict(
    exclude: &[String],
    mappings: &HashMap<String, String>,
) -> Result<()> {
    for name in exclude {
        if mappings.contains_key(name) {
            return Err(anyhow::anyhow!(
                "Column '{}' cannot be both excluded and renamed",
                name
            ));
        }
    }
    Ok(())
}

/// Return the indices of excluded columns, walking the schema in its original
/// order. Output is naturally sorted because `enumerate` yields strictly
/// increasing indices.
fn compute_excluded_positions(schema: &Schema, exclude: &[String]) -> Vec<usize> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(i, c)| exclude.contains(&c.name).then_some(i))
        .collect()
}

/// Remove excluded columns from the schema in place, preserving original column order.
fn filter_schema(schema: &mut Schema, exclude: &[String]) {
    schema.columns.retain(|c| !exclude.contains(&c.name));
}

/// Error if ALL conflict columns are excluded under do-update (ON CONFLICT can never match),
/// warn if SOME are excluded.
fn validate_conflict_columns_not_all_excluded(
    exclude: &[String],
    conflict_columns: &[String],
    on_conflict: OnConflict,
) -> Result<()> {
    if on_conflict != OnConflict::DoUpdate || conflict_columns.is_empty() {
        return Ok(());
    }

    let excluded_conflict: Vec<&String> = conflict_columns
        .iter()
        .filter(|c| exclude.contains(c))
        .collect();

    if excluded_conflict.len() == conflict_columns.len() {
        return Err(anyhow::anyhow!(
            "All conflict columns are excluded. ON CONFLICT can never match — \
             use --on-conflict do-nothing or remove conflicting columns from --exclude-columns"
        ));
    }

    if !excluded_conflict.is_empty() {
        warn!(
            "Some conflict columns are excluded: {:?}. ON CONFLICT may not behave as expected.",
            excluded_conflict
        );
    }

    Ok(())
}

/// Maximum bytes to read for schema inference
///
/// Caps the amount of data read to prevent loading entire large files just for
/// schema detection. 10MB is sufficient to capture thousands of rows for accurate
/// type inference while maintaining reasonable memory usage.
const MAX_SCHEMA_INFERENCE_BYTES: u64 = 10 * 1024 * 1024; // 10 MB

/// pg_dump positional-binding alignment: the COPY statement's columns and the
/// target table's columns must form the same SET. When the names match as sets
/// but appear in different orders, we reorder the resolved target schema in
/// place so that field N (from the COPY data, in COPY-clause order) binds to
/// column N (from the schema worker.rs uses to build the INSERT). This lets a
/// hand-rolled DSQL DDL succeed even if the operator listed columns in a
/// different order than the source PG table.
///
/// Errors when the sets diverge (a column is in the dump but missing from the
/// target, or vice versa). Returning `Ok(())` guarantees the schema's column
/// order matches `copy_columns`.
///
/// No-op for non-pg_dump formats.
fn align_pgdump_schema_to_copy_columns(
    config: &LoadConfig,
    schema: &mut Option<Schema>,
) -> Result<()> {
    let FileFormat::PgDump(pg) = &config.file_format else {
        return Ok(());
    };
    let copy_cols = &pg.copy_columns;
    let resolved = schema.as_mut().ok_or_else(|| {
        anyhow::anyhow!(
            "Internal error: pg_dump load resolved no schema for {}.{}",
            config.schema,
            config.target_table
        )
    })?;

    if resolved
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .eq(copy_cols.iter().map(String::as_str))
    {
        return Ok(());
    }

    let copy_set: HashSet<&str> = copy_cols.iter().map(String::as_str).collect();
    let target_set: HashSet<&str> = resolved.columns.iter().map(|c| c.name.as_str()).collect();

    if copy_set != target_set {
        let target_names: Vec<&str> = resolved.columns.iter().map(|c| c.name.as_str()).collect();
        // Sort the diff lists so the bail message is deterministic across runs
        // (HashSet::difference iteration order is not stable).
        let mut missing_in_target: Vec<&str> = copy_set.difference(&target_set).copied().collect();
        let mut missing_in_dump: Vec<&str> = target_set.difference(&copy_set).copied().collect();
        missing_in_target.sort_unstable();
        missing_in_dump.sort_unstable();
        anyhow::bail!(
            "pg_dump column-set mismatch for {}.{}:\n  \
             dump COPY columns:    {:?}\n  \
             target table columns: {:?}\n  \
             in dump but missing from target table: {:?}\n  \
             in target table but missing from dump:  {:?}\n\
             Recreate the target table to contain exactly the COPY columns. \
             Order does not matter — the loader will reorder by name.",
            config.schema,
            config.target_table,
            copy_cols,
            target_names,
            missing_in_target,
            missing_in_dump,
        );
    }

    // Sets match, order differs — reorder by name so field N → column N.
    let original = std::mem::take(&mut resolved.columns);
    let mut by_name: HashMap<String, crate::db::schema::Column> =
        original.into_iter().map(|c| (c.name.clone(), c)).collect();
    resolved.columns = copy_cols
        .iter()
        .map(|name| {
            by_name.remove(name.as_str()).ok_or_else(|| {
                anyhow::anyhow!(
                    "Internal: target column {name:?} disappeared between set check and reorder"
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    info!(
        "pg_dump: reordered target schema columns to match COPY clause ({} columns)",
        resolved.columns.len()
    );
    Ok(())
}

/// Resume-time variant: the manifest's persisted schema is what the worker
/// actually uses, so we don't reorder anything — we only verify that the live
/// target table's column SET still matches the dump's COPY columns. This
/// catches an `ALTER TABLE ... DROP/ADD COLUMN` that happened between runs.
fn validate_pgdump_column_set(config: &LoadConfig, live_schema: &Schema) -> Result<()> {
    let FileFormat::PgDump(pg) = &config.file_format else {
        return Ok(());
    };
    let copy_set: HashSet<&str> = pg.copy_columns.iter().map(String::as_str).collect();
    let target_set: HashSet<&str> = live_schema
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    if copy_set == target_set {
        return Ok(());
    }
    let target_names: Vec<&str> = live_schema
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    anyhow::bail!(
        "Cannot resume: pg_dump COPY column set no longer matches target {}.{}.\n  \
         dump COPY columns:    {:?}\n  \
         live target columns:  {:?}\n\
         The target table was altered between runs.",
        config.schema,
        config.target_table,
        pg.copy_columns,
        target_names,
    );
}

/// Configuration for a data load operation
#[derive(Debug, Clone, Builder)]
pub struct LoadConfig {
    #[builder(setter(into))]
    source_uri: String,
    #[builder(setter(into))]
    target_table: String,
    #[builder(setter(into))]
    schema: String,
    dsql_config: DsqlConfig,
    worker_count: usize,
    chunk_size_bytes: u64,
    batch_size: usize,
    batch_concurrency: usize,
    create_table_if_missing: bool,
    file_format: FileFormat,
    #[builder(default)]
    column_mappings: HashMap<String, String>,
    quiet: bool,
    #[builder(default)]
    debug: bool,
    #[builder(default)]
    resume_job_id: Option<String>,
    #[builder(default)]
    on_conflict: OnConflict,
    #[builder(default)]
    exclude_columns: Vec<String>,
}

/// Result of a completed data load operation
#[derive(Debug)]
pub struct LoadResult {
    pub job_id: String,
    pub chunks_processed: usize,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// Sum of per-chunk row estimates obtained by sampling the source bytes;
    /// used to flag a large delta vs records actually loaded.
    pub estimated_rows: Option<u64>,
    pub duration: Duration,
    /// Detailed results for each chunk (accessed in integration tests)
    #[cfg_attr(not(test), allow(dead_code))]
    pub chunk_results: Vec<ChunkResultFile>,
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
    /// This method orchestrates the entire load process locally
    pub async fn run_load(&self, config: &LoadConfig) -> Result<LoadResult> {
        let start_time = Instant::now();

        // Setup job (either resume existing or start new)
        let (job_id, file_metadata, chunks) = match &config.resume_job_id {
            Some(resume_id) => self.setup_resume_job(resume_id, config).await?,
            None => self.setup_new_job(config).await?,
        };

        // Create telemetry channel and spawn workers
        let (telemetry_tx, telemetry_rx) = mpsc::unbounded_channel::<TelemetryEvent>();
        let worker_handles = self.spawn_worker_pool(&job_id, config, telemetry_tx.clone());

        // Drop the coordinator's copy of the sender so the channel closes when workers finish
        drop(telemetry_tx);

        // Setup progress tracking
        let prog_jh = Self::setup_progress_tracking(config, &file_metadata, &chunks, telemetry_rx);

        // Wait for all workers to complete
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

        self.aggregate_final_results(&job_id, &chunks, start_time)
            .await
    }

    /// Collect all chunk results from manifest storage
    async fn collect_results(
        &self,
        job_id: &str,
        chunk_count: usize,
    ) -> Result<Vec<ChunkResultFile>> {
        let mut results = Vec::with_capacity(chunk_count);

        for chunk_id in 0..chunk_count as u32 {
            match self.manifest_storage.read_result(job_id, chunk_id).await {
                Ok(result) => results.push(result),
                Err(_) => {
                    // Chunk result may be missing if worker crashed before writing result file
                    // This is tracked in failure metrics and doesn't require hard failure
                    warn!("Chunk {chunk_id} result missing");
                }
            }
        }

        Ok(results)
    }

    /// Create chunks from the source file and return metadata
    async fn create_file_chunks(
        &self,
        chunk_size_bytes: u64,
    ) -> Result<(FileMetadata, Vec<Chunk>)> {
        info!(
            "Creating chunks with target size: {} bytes",
            chunk_size_bytes
        );

        let file_metadata = self
            .file_reader
            .metadata()
            .await
            .context("Failed to get file metadata")?;

        let chunks = self
            .file_reader
            .create_chunks(chunk_size_bytes)
            .await
            .context("Failed to create chunks")?;

        info!(
            "Created {} chunks for file of {} bytes (estimated {} rows)",
            chunks.len(),
            file_metadata.file_size_bytes,
            file_metadata.estimated_rows.unwrap_or(0)
        );

        Ok((file_metadata, chunks))
    }

    /// Resolve the table schema: either infer from data or query existing table
    async fn resolve_table_schema(
        &self,
        config: &LoadConfig,
        chunks: &[Chunk],
    ) -> Result<Option<Schema>> {
        if config.create_table_if_missing {
            info!("Inferring schema from sample data...");

            // For schema inference, we need to read from the beginning of the file
            // including the header, not from chunk 0 (which starts after the header)
            let first_chunk = chunks
                .first()
                .context("No chunks available for schema inference")?;

            // Cap the sample size to avoid reading massive files
            let end_offset = first_chunk.end_offset.min(MAX_SCHEMA_INFERENCE_BYTES);

            let sample_chunk = Chunk {
                chunk_id: 0,
                start_offset: 0,
                end_offset,
                estimated_rows: first_chunk.estimated_rows,
            };

            let sample_data = self
                .file_reader
                .read_chunk(&sample_chunk)
                .await
                .context("Failed to read sample data for schema inference")?;

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
            let queried_schema = query_table_schema(
                &self.pool,
                &config.schema,
                &config.target_table,
            )
            .await
            .with_context(|| {
                format!(
                    "Table '{}.{}' does not exist. Use --if-not-exists to create it automatically.",
                    config.schema, config.target_table
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
        schema: &mut Option<Schema>,
        column_mappings: &HashMap<String, String>,
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

    /// Setup a new load job by creating chunks and manifest
    async fn setup_new_job(
        &self,
        config: &LoadConfig,
    ) -> Result<(String, FileMetadata, Vec<Chunk>)> {
        let job_id = Uuid::new_v4().to_string();
        info!("Starting load job: {job_id}");

        // Reject --exclude-columns combined with --if-not-exists before any I/O: the loader
        // would otherwise generate a CREATE TABLE that silently omits the excluded columns
        // entirely, producing a broken table with no DEFAULT to fall back to.
        if !config.exclude_columns.is_empty() && config.create_table_if_missing {
            return Err(anyhow::anyhow!(
                "--exclude-columns is not supported with --if-not-exists. \
                 The table must already exist with DEFAULT expressions on the excluded columns \
                 (e.g. `pk_id UUID DEFAULT gen_random_uuid()`)."
            ));
        }

        validate_schema_exists(&self.pool, &config.schema).await?;

        let (file_metadata, chunks) = self.create_file_chunks(config.chunk_size_bytes).await?;

        let mut schema = self.resolve_table_schema(config, &chunks).await?;

        align_pgdump_schema_to_copy_columns(config, &mut schema)?;

        // Apply --exclude-columns before renames. Produces the effective schema
        // that downstream code uses for INSERT generation.
        let excluded_positions = if !config.exclude_columns.is_empty() {
            let s = schema.as_mut().ok_or_else(|| {
                anyhow::anyhow!(
                    "Internal error: table schema is missing while --exclude-columns is active"
                )
            })?;
            validate_excluded_columns(s, &config.exclude_columns)?;
            validate_no_exclude_rename_conflict(&config.exclude_columns, &config.column_mappings)?;
            let positions = compute_excluded_positions(s, &config.exclude_columns);
            filter_schema(s, &config.exclude_columns);
            positions
        } else {
            Vec::new()
        };

        Self::apply_column_mappings(&mut schema, &config.column_mappings);

        let table_created = self.ensure_table_exists(config, &schema).await?;

        self.create_and_write_manifest(
            &job_id,
            config,
            &file_metadata,
            &chunks,
            schema,
            table_created,
            excluded_positions,
        )
        .await?;

        Ok((job_id, file_metadata, chunks))
    }

    /// Setup a resumed job by reading the manifest and cleaning up old claims
    async fn setup_resume_job(
        &self,
        resume_id: &str,
        config: &LoadConfig,
    ) -> Result<(String, FileMetadata, Vec<Chunk>)> {
        info!("Resuming load job: {}", resume_id);

        // Read existing manifest
        let manifest = self
            .manifest_storage
            .read_manifest(resume_id)
            .await
            .context("Failed to read manifest for resume")?;

        self.verify_resume_compatibility(&manifest, config).await?;

        if !manifest.table.excluded_columns.is_empty() {
            info!(
                "Resuming with excluded columns from manifest: {:?}",
                manifest.table.excluded_columns
            );
        }

        // On resume, the worker uses the manifest's persisted schema (already
        // aligned at original setup_new_job time), so we do not reorder again.
        // We do verify the live target table's column SET still matches the
        // dump — catches ALTER TABLE ADD/DROP COLUMN between runs, which
        // would silently misalign every chunk loaded after this point.
        if matches!(config.file_format, FileFormat::PgDump(_)) {
            let live_schema = query_table_schema(&self.pool, &config.schema, &config.target_table)
                .await
                .with_context(|| {
                    format!(
                        "Cannot resume: failed to query target schema for {}.{}",
                        config.schema, config.target_table
                    )
                })?;
            validate_pgdump_column_set(config, &live_schema)?;
        }

        info!("Cleaning up incomplete claims...");
        self.manifest_storage.cleanup_old_claims(resume_id).await?;

        let file_metadata = FileMetadata {
            file_size_bytes: manifest.total_size_bytes,
            estimated_rows: manifest.estimated_rows,
        };

        let chunks: Vec<Chunk> = manifest
            .chunks
            .iter()
            .map(|c| Chunk {
                chunk_id: c.chunk_id,
                start_offset: c.start_offset,
                end_offset: c.end_offset,
                estimated_rows: c.estimated_rows,
            })
            .collect();

        Ok((resume_id.to_string(), file_metadata, chunks))
    }

    /// Verify that the manifest is compatible with the resume request
    async fn verify_resume_compatibility(
        &self,
        manifest: &ManifestFile,
        config: &LoadConfig,
    ) -> Result<()> {
        if manifest.source_uri != config.source_uri {
            return Err(anyhow::anyhow!(
                "Cannot resume: source URI mismatch\n\
                 Manifest source: {}\n\
                 Requested source: {}",
                manifest.source_uri,
                config.source_uri
            ));
        }

        if manifest.table.name != config.target_table {
            return Err(anyhow::anyhow!(
                "Cannot resume: target table mismatch\n\
                 Manifest table: {}\n\
                 Requested table: {}",
                manifest.table.name,
                config.target_table
            ));
        }

        if manifest.table.schema_name != config.schema {
            return Err(anyhow::anyhow!(
                "Cannot resume: schema mismatch\n\
                 Manifest schema: {}\n\
                 Requested schema: {}",
                manifest.table.schema_name,
                config.schema
            ));
        }

        let current_metadata = self
            .file_reader
            .metadata()
            .await
            .context("Failed to get current file metadata")?;

        if current_metadata.file_size_bytes != manifest.total_size_bytes {
            return Err(anyhow::anyhow!(
                "Cannot resume: source file size changed\n\
                 Manifest size: {} bytes\n\
                 Current size: {} bytes",
                manifest.total_size_bytes,
                current_metadata.file_size_bytes
            ));
        }

        // Manifest chunks are aligned by the original format's chunker
        // (CSV/TSV split on newlines, parquet on row groups, pg_dump on row
        // boundaries inside a specific COPY block). Resuming under a
        // different format would reuse those byte ranges with a parser that
        // does not understand them and silently mis-parse every chunk.
        if std::mem::discriminant(&manifest.file_format)
            != std::mem::discriminant(&config.file_format)
        {
            return Err(anyhow::anyhow!(
                "Cannot resume: file format mismatch\n\
                 Manifest format: {:?}\n\
                 Requested format: {:?}\n\
                 The manifest's chunks were aligned by the original format's \
                 chunker; resuming with a different format would parse the \
                 wrong byte ranges. Start a fresh load.",
                manifest.file_format,
                config.file_format,
            ));
        }

        // pg_dump: catch a source edit that changed the COPY column list while
        // leaving the file size unchanged (e.g. swapping two columns in the
        // header without altering data). Chunks in the manifest were created
        // against the original byte ranges; a different positional column
        // mapping would silently corrupt every loaded row.
        if let (FileFormat::PgDump(manifest_pg), FileFormat::PgDump(config_pg)) =
            (&manifest.file_format, &config.file_format)
            && manifest_pg.copy_columns != config_pg.copy_columns
        {
            return Err(anyhow::anyhow!(
                "Cannot resume: pg_dump COPY column list changed between runs.\n\
                 Manifest columns: {:?}\n\
                 Current columns:  {:?}\n\
                 The chunks in the manifest were positioned against the \
                 original column list; resuming with a different list would \
                 silently misalign data. Start a fresh load.",
                manifest_pg.copy_columns,
                config_pg.copy_columns,
            ));
        }

        // Manifest integrity: the two exclusion fields are written together and must
        // stay in lock-step (length parity), and every position must be strictly
        // increasing (implying unique + sorted) and satisfy
        // `p < schema.columns.len() + excluded_positions.len()` (the reconstructed
        // original arity). A mismatch indicates a corrupted or hand-edited manifest
        // and would cause silent data misalignment at worker field-mapping time.
        if manifest.table.excluded_columns.len() != manifest.table.excluded_positions.len() {
            return Err(anyhow::anyhow!(
                "Cannot resume: manifest exclusion fields disagree \
                 (excluded_columns.len() = {}, excluded_positions.len() = {})",
                manifest.table.excluded_columns.len(),
                manifest.table.excluded_positions.len()
            ));
        }
        if !manifest.table.excluded_positions.is_empty() {
            let schema = manifest.table.schema.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot resume: manifest has excluded_positions but no stored schema"
                )
            })?;
            let full_len = schema.columns.len() + manifest.table.excluded_positions.len();
            let mut prev: Option<usize> = None;
            for &p in &manifest.table.excluded_positions {
                if p >= full_len {
                    return Err(anyhow::anyhow!(
                        "Cannot resume: manifest excluded_position {} is out of range \
                         for original schema size {}",
                        p,
                        full_len
                    ));
                }
                if let Some(prev) = prev
                    && p <= prev
                {
                    return Err(anyhow::anyhow!(
                        "Cannot resume: manifest excluded_positions are not strictly \
                         increasing (saw {} after {})",
                        p,
                        prev
                    ));
                }
                prev = Some(p);
            }
        }

        // --exclude-columns on resume: the flag may be omitted (the manifest's list
        // is authoritative), but if supplied it must match the manifest as a set.
        // Avoids forcing operators to retype the list while still catching accidental
        // drift. Compare as sets (sorted) because the manifest stores the list in
        // schema order while the user-passed list is in CLI order — semantically
        // equivalent lists should not trip the check.
        if !config.exclude_columns.is_empty() {
            let mut requested = config.exclude_columns.clone();
            let mut stored = manifest.table.excluded_columns.clone();
            requested.sort();
            stored.sort();
            if requested != stored {
                return Err(anyhow::anyhow!(
                    "Cannot resume: --exclude-columns mismatch\n\
                     Manifest excluded_columns: {:?}\n\
                     Requested exclude_columns: {:?}\n\
                     To resume an existing job, either omit --exclude-columns (the manifest value is used) \
                     or pass the same list as the original job.",
                    manifest.table.excluded_columns,
                    config.exclude_columns
                ));
            }
        }

        Ok(())
    }

    /// Create the table if it doesn't exist (when create_table_if_missing is true)
    async fn ensure_table_exists(
        &self,
        config: &LoadConfig,
        schema: &Option<Schema>,
    ) -> Result<bool> {
        if config.create_table_if_missing {
            if let Some(schema) = schema {
                info!("Creating table: {}.{}", config.schema, config.target_table);

                // Use Pool helper to generate the properly formatted table name
                let table_spec = self
                    .pool
                    .qualified_table_name(&config.schema, &config.target_table);

                let ddl = self.schema_inferrer.generate_ddl(&table_spec, schema);

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
    #[allow(clippy::too_many_arguments)]
    async fn create_and_write_manifest(
        &self,
        job_id: &str,
        config: &LoadConfig,
        file_metadata: &FileMetadata,
        chunks: &[Chunk],
        schema: Option<Schema>,
        table_created: bool,
        excluded_positions: Vec<usize>,
    ) -> Result<()> {
        // Detect unique constraints and get conflict columns if needed
        info!("Checking for unique constraints on table...");
        let has_unique_constraints = self
            .pool
            .has_unique_constraints(&config.schema, &config.target_table)
            .await
            .context("Failed to check for unique constraints")?;

        // Fetch constraint columns if they exist, or validate DoUpdate requirements
        let conflict_columns = if has_unique_constraints {
            self.pool
                .get_unique_constraint_columns(&config.schema, &config.target_table)
                .await
                .context("Failed to query constraint columns")?
        } else {
            if config.on_conflict == OnConflict::DoUpdate {
                return Err(anyhow::anyhow!(
                    "do-update mode requires table '{}' to have a primary key or unique constraint",
                    config.target_table
                ));
            }
            Vec::new()
        };

        if has_unique_constraints {
            info!(
                "Table has unique constraints on columns: {:?}",
                conflict_columns
            );
        }

        // Conflict-column validation must run after `conflict_columns` is fetched
        // from the database (it requires a DB query).
        validate_conflict_columns_not_all_excluded(
            &config.exclude_columns,
            &conflict_columns,
            config.on_conflict,
        )?;

        // Build manifest
        let chunk_infos: Vec<ChunkInfo> = chunks
            .iter()
            .enumerate()
            .map(|(idx, p)| ChunkInfo {
                chunk_id: idx as u32,
                start_offset: p.start_offset,
                end_offset: p.end_offset,
                estimated_rows: p.estimated_rows,
            })
            .collect();

        let table = super::manifest::TableInfo {
            name: config.target_table.clone(),
            schema_name: config.schema.clone(),
            schema: schema.map(|s| s.into()),
            was_created: table_created,
            has_unique_constraints,
            on_conflict: config.on_conflict,
            conflict_columns,
            excluded_columns: config.exclude_columns.clone(),
            excluded_positions,
        };
        // TableInfo invariant: the two exclusion vectors are written together and
        // must stay in lock-step. Promoted from debug_assert to a real guard so a
        // release-mode regression can't ship a corrupted manifest that the resume
        // path would then reject only on the next run.
        if table.excluded_columns.len() != table.excluded_positions.len() {
            return Err(anyhow::anyhow!(
                "Internal error: TableInfo exclusion fields disagree \
                 (excluded_columns.len() = {}, excluded_positions.len() = {})",
                table.excluded_columns.len(),
                table.excluded_positions.len()
            ));
        }

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
            chunks: chunk_infos,
        };

        self.manifest_storage
            .write_manifest(job_id, &manifest)
            .await
            .context("Failed to write manifest")?;

        info!("Manifest written for job: {}", job_id);
        Ok(())
    }

    /// Spawn worker tasks to process chunks
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
                config.debug,
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
        file_metadata: &FileMetadata,
        chunks: &[Chunk],
        mut telemetry_rx: mpsc::UnboundedReceiver<TelemetryEvent>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if config.quiet {
            return None;
        }

        // Create progress bars with MultiProgress
        let multi_progress = MultiProgress::new();
        let total_chunks = chunks.len() as u64;
        let estimated_total_rows = file_metadata.estimated_rows.unwrap_or(0);

        let chunk_bar = multi_progress.add(ProgressBar::new(total_chunks));
        chunk_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{elapsed_precise}] Chunks: [{bar:30.cyan/blue}] {pos}/{len} ({percent}%)",
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

        Some(tokio::spawn(async move {
            let mut stats = ProgressStats::new();

            while let Some(event) = telemetry_rx.recv().await {
                stats.update(&event);

                chunk_bar.set_position(stats.chunks_completed as u64);
                if let Some(ref bar) = rows_bar {
                    bar.set_position(stats.records_loaded);
                }
                bytes_bar.set_position(stats.bytes_processed);

                let (p50, p90, p99) = stats.get_percentiles();
                if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                    stats_bar
                        .set_message(format!("p50: {}ms, p90: {}ms, p99: {}ms", p50, p90, p99));
                }
            }

            chunk_bar.finish_with_message("All chunks completed");
            if let Some(bar) = rows_bar {
                bar.finish();
            }
            bytes_bar.finish();

            let (p50, p90, p99) = stats.get_percentiles();
            if let (Some(p50), Some(p90), Some(p99)) = (p50, p90, p99) {
                stats_bar
                    .finish_with_message(format!("p50: {}ms, p90: {}ms, p99: {}ms", p50, p90, p99));
            } else {
                stats_bar.finish();
            }
        }))
    }

    /// Aggregate the final results from all chunk result files
    async fn aggregate_final_results(
        &self,
        job_id: &str,
        chunks: &[Chunk],
        start_time: Instant,
    ) -> Result<LoadResult> {
        info!("Aggregating results...");
        let chunk_results = self
            .collect_results(job_id, chunks.len())
            .await
            .context("Failed to collect results")?;

        let total_records_loaded: u64 = chunk_results.iter().map(|r| r.records_loaded).sum();
        let total_records_failed: u64 = chunk_results.iter().map(|r| r.records_failed).sum();
        let duration = start_time.elapsed();

        // Warn if significantly fewer records were processed than estimated
        let total_estimated: u64 = chunks.iter().filter_map(|c| c.estimated_rows).sum();
        let total_processed = total_records_loaded + total_records_failed;
        if total_estimated > 0 && total_processed < total_estimated / 2 {
            warn!(
                "Only {} records processed out of ~{} estimated. \
                 This may indicate parse errors in the source file. \
                 Check delimiter, quote, and escape settings.",
                total_processed, total_estimated
            );
        }

        info!(
            "Load complete: {} chunks, {} records loaded, {} records failed in {:.2}s",
            chunk_results.len(),
            total_records_loaded,
            total_records_failed,
            duration.as_secs_f64()
        );

        Ok(LoadResult {
            job_id: job_id.to_string(),
            chunks_processed: chunk_results.len(),
            records_loaded: total_records_loaded,
            records_failed: total_records_failed,
            estimated_rows: Some(total_estimated).filter(|&e| e > 0),
            duration,
            chunk_results,
        })
    }
}

#[cfg(test)]
mod exclusion_tests {
    use super::*;
    use crate::coordination::manifest::PgDumpConfig;
    use crate::db::schema::{Column, Schema, SqlType};

    fn mk_schema(names: &[&str]) -> Schema {
        Schema {
            columns: names
                .iter()
                .map(|n| Column {
                    name: n.to_string(),
                    sql_type: SqlType::Text,
                    nullable: true,
                })
                .collect(),
        }
    }

    #[test]
    fn validate_excluded_columns_ok() {
        let s = mk_schema(&["id", "name", "email"]);
        validate_excluded_columns(&s, &["id".to_string()]).unwrap();
    }

    #[test]
    fn validate_excluded_columns_unknown_name() {
        let s = mk_schema(&["id", "name"]);
        let err = validate_excluded_columns(&s, &["nope".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Column 'nope' not found"));
        assert!(err.contains("\"id\""));
    }

    #[test]
    fn validate_excluded_columns_all_excluded_errors() {
        let s = mk_schema(&["id", "name"]);
        let err = validate_excluded_columns(&s, &["id".to_string(), "name".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("Cannot exclude all columns"));
    }

    #[test]
    fn validate_no_exclude_rename_conflict_detects_overlap() {
        let mut map = HashMap::new();
        map.insert("id".to_string(), "new_id".to_string());
        let err = validate_no_exclude_rename_conflict(&["id".to_string()], &map)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Column 'id' cannot be both excluded and renamed"));
    }

    #[test]
    fn validate_no_exclude_rename_conflict_disjoint_ok() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), "full_name".to_string());
        validate_no_exclude_rename_conflict(&["id".to_string()], &map).unwrap();
    }

    #[test]
    fn exclude_then_rename_ordering_applies_rename_to_remaining_columns() {
        // Design invariant: exclusion uses ORIGINAL schema names; rename is applied
        // to the effective schema (post-exclusion). This verifies the compose:
        //   filter(pk_id) then rename(name -> full_name) == [full_name, email]
        let mut schema = Some(mk_schema(&["pk_id", "name", "email"]));

        // Step 1: exclusion (matches setup_new_job order)
        let exclude = vec!["pk_id".to_string()];
        let s = schema.as_mut().unwrap();
        validate_excluded_columns(s, &exclude).unwrap();
        let positions = compute_excluded_positions(s, &exclude);
        filter_schema(s, &exclude);
        assert_eq!(positions, vec![0]);

        // Step 2: rename
        let mut mappings = HashMap::new();
        mappings.insert("name".to_string(), "full_name".to_string());
        Coordinator::apply_column_mappings(&mut schema, &mappings);

        let names: Vec<&str> = schema
            .as_ref()
            .unwrap()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["full_name", "email"]);
    }

    #[test]
    fn compute_excluded_positions_returns_sorted_indices_in_schema_order() {
        let s = mk_schema(&["id", "name", "email", "created_at"]);
        let positions =
            compute_excluded_positions(&s, &["created_at".to_string(), "id".to_string()]);
        assert_eq!(positions, vec![0, 3]);
    }

    #[test]
    fn filter_schema_preserves_order_and_types() {
        let mut s = mk_schema(&["id", "name", "email", "created_at"]);
        filter_schema(&mut s, &["id".to_string(), "created_at".to_string()]);
        let names: Vec<&str> = s.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["name", "email"]);
    }

    fn mk_pgdump_load_config(copy_columns: Vec<String>) -> LoadConfig {
        LoadConfigBuilder::default()
            .source_uri("dummy".to_string())
            .target_table("things".to_string())
            .schema("public".to_string())
            .dsql_config(DsqlConfig {
                endpoint: "ignored".into(),
                region: "us-east-1".into(),
                username: "admin".into(),
            })
            .worker_count(1)
            .chunk_size_bytes(1024)
            .batch_size(10)
            .batch_concurrency(1)
            .create_table_if_missing(false)
            .file_format(FileFormat::PgDump(PgDumpConfig { copy_columns }))
            .quiet(true)
            .build()
            .unwrap()
    }

    #[test]
    fn align_pgdump_schema_accepts_matching_order() {
        let config = mk_pgdump_load_config(vec!["id".into(), "name".into(), "note".into()]);
        let mut schema = Some(mk_schema(&["id", "name", "note"]));
        align_pgdump_schema_to_copy_columns(&config, &mut schema).unwrap();
        let names: Vec<&str> = schema
            .as_ref()
            .unwrap()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, vec!["id", "name", "note"]);
    }

    #[test]
    fn align_pgdump_schema_reorders_when_set_matches_but_order_differs() {
        let config = mk_pgdump_load_config(vec!["id".into(), "name".into(), "note".into()]);
        // Target table has same columns but in a different order than COPY.
        let mut schema = Some(mk_schema(&["name", "id", "note"]));
        align_pgdump_schema_to_copy_columns(&config, &mut schema).unwrap();
        let names: Vec<&str> = schema
            .as_ref()
            .unwrap()
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["id", "name", "note"],
            "schema must be reordered to COPY-clause order so positional binding aligns"
        );
    }

    #[test]
    fn align_pgdump_schema_rejects_when_dump_has_extra_column() {
        let config = mk_pgdump_load_config(vec!["id".into(), "name".into(), "note".into()]);
        // Target table is missing `note`.
        let mut schema = Some(mk_schema(&["id", "name"]));
        let err = align_pgdump_schema_to_copy_columns(&config, &mut schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains("column-set mismatch"), "{err}");
        assert!(
            err.contains("note"),
            "error must name the missing column: {err}"
        );
    }

    #[test]
    fn align_pgdump_schema_rejects_when_target_has_extra_column() {
        let config = mk_pgdump_load_config(vec!["id".into(), "name".into()]);
        // Target table has an extra column not present in the dump.
        let mut schema = Some(mk_schema(&["id", "name", "extra"]));
        let err = align_pgdump_schema_to_copy_columns(&config, &mut schema)
            .unwrap_err()
            .to_string();
        assert!(err.contains("column-set mismatch"), "{err}");
        assert!(
            err.contains("extra"),
            "error must name the extra column: {err}"
        );
    }

    #[test]
    fn validate_pgdump_column_set_accepts_same_set_any_order() {
        let config = mk_pgdump_load_config(vec!["id".into(), "name".into(), "note".into()]);
        let live = mk_schema(&["note", "id", "name"]);
        validate_pgdump_column_set(&config, &live).unwrap();
    }

    #[test]
    fn validate_pgdump_column_set_rejects_diverging_sets() {
        let config = mk_pgdump_load_config(vec!["id".into(), "name".into(), "note".into()]);
        let live = mk_schema(&["id", "name"]);
        let err = validate_pgdump_column_set(&config, &live)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no longer matches"), "{err}");
    }

    #[test]
    fn validate_conflict_columns_all_excluded_do_update_errors() {
        let err = validate_conflict_columns_not_all_excluded(
            &["pk_id".to_string()],
            &["pk_id".to_string()],
            OnConflict::DoUpdate,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("All conflict columns are excluded"));
        assert!(err.contains("--on-conflict do-nothing"));
    }

    #[test]
    fn validate_conflict_columns_some_excluded_do_update_warns_but_ok() {
        // Warn path: should succeed
        validate_conflict_columns_not_all_excluded(
            &["a".to_string()],
            &["a".to_string(), "b".to_string()],
            OnConflict::DoUpdate,
        )
        .unwrap();
    }

    #[test]
    fn validate_conflict_columns_none_excluded_do_update_ok() {
        validate_conflict_columns_not_all_excluded(
            &["x".to_string()],
            &["a".to_string(), "b".to_string()],
            OnConflict::DoUpdate,
        )
        .unwrap();
    }

    #[test]
    fn validate_conflict_columns_all_excluded_do_nothing_skipped() {
        // do-nothing: validation should be a no-op even when all conflict cols excluded
        validate_conflict_columns_not_all_excluded(
            &["pk_id".to_string()],
            &["pk_id".to_string()],
            OnConflict::DoNothing,
        )
        .unwrap();
    }

    // Feature: loader-column-exclusion, Property 2: Schema filtering preserves order
    proptest::proptest! {
        #[test]
        fn prop_filter_schema_preserves_non_excluded_in_order(
            names in proptest::collection::vec("[a-z][a-z0-9]{0,7}", 2..10),
            // which indices to exclude (by position) — always leave at least one
            exclude_mask in proptest::collection::vec(proptest::bool::ANY, 2..10),
        ) {
            // Dedup names (schema columns are unique)
            let mut seen = HashSet::new();
            let unique_names: Vec<String> = names.into_iter().filter(|n| seen.insert(n.clone())).collect();
            proptest::prop_assume!(unique_names.len() >= 2);

            // Build exclusion list; ensure we don't exclude everything
            let n = unique_names.len();
            let mask: Vec<bool> = exclude_mask.into_iter().take(n).chain(std::iter::repeat(false)).take(n).collect();
            let exclude: Vec<String> = unique_names.iter().zip(&mask).filter_map(|(name, &ex)| if ex { Some(name.clone()) } else { None }).collect();
            proptest::prop_assume!(!exclude.is_empty() && exclude.len() < n);

            let mut schema = Schema {
                columns: unique_names.iter().map(|n| Column {
                    name: n.clone(),
                    sql_type: SqlType::Text,
                    nullable: true,
                }).collect(),
            };
            filter_schema(&mut schema, &exclude);

            // Expected: non-excluded names in original order
            let expected: Vec<String> = unique_names.iter().filter(|n| !exclude.contains(n)).cloned().collect();
            let actual: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
            proptest::prop_assert_eq!(actual, expected);
        }

        #[test]
        fn prop_validate_excluded_columns_rejects_unknown(
            names in proptest::collection::vec("[a-z][a-z0-9]{0,7}", 2..8),
            bogus in "[A-Z][A-Z0-9]{0,7}",
        ) {
            // `bogus` starts uppercase so it can't collide with lowercase `names`
            let mut seen = HashSet::new();
            let unique_names: Vec<String> = names.into_iter().filter(|n| seen.insert(n.clone())).collect();
            proptest::prop_assume!(unique_names.len() >= 2);

            let schema = Schema {
                columns: unique_names.iter().map(|n| Column {
                    name: n.clone(),
                    sql_type: SqlType::Text,
                    nullable: true,
                }).collect(),
            };

            let result = validate_excluded_columns(&schema, &[bogus]);
            proptest::prop_assert!(result.is_err());
        }
    }
}
