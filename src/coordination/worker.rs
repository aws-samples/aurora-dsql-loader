use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::manifest::{ChunkResultFile, ChunkStatus, ClaimFile, ErrorRecord, ManifestStorage};
use crate::config::{BASE_DELAY, MAX_DELAY, MAX_RETRIES, QUERY_TIMEOUT};
use crate::db::Pool;
use crate::formats::{FileReader, Record};
use crate::telemetry::TelemetryEvent;

/// Apply `--exclude-columns` field filtering to a batch of records.
///
/// Skip-mode only: each record must have `full_len = effective_len + excluded_positions.len()`
/// fields. Matching records drop fields at `excluded_positions`; mismatched records are
/// excluded from the returned batch and counted.
///
/// Returns `(filtered_records, mismatch_count, first_mismatch)` where
/// `first_mismatch` is `(record_index_within_chunk, observed_field_count)` for the
/// first mismatched record, useful for locating the offending row in the source file.
fn apply_field_exclusion(
    records: Vec<Record>,
    excluded_positions: &[usize],
    full_len: usize,
) -> (Vec<Record>, u64, Option<(usize, usize)>) {
    let excluded_set: HashSet<usize> = excluded_positions.iter().copied().collect();
    let mut filtered = Vec::with_capacity(records.len());
    let mut mismatch = 0u64;
    let mut first_mismatch: Option<(usize, usize)> = None;
    for (idx, record) in records.into_iter().enumerate() {
        if record.fields.len() == full_len {
            let kept_fields: Vec<String> = record
                .fields
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !excluded_set.contains(i))
                .map(|(_, f)| f)
                .collect();
            let kept_nulls: Vec<bool> = record
                .nulls
                .into_iter()
                .enumerate()
                .filter(|(i, _)| !excluded_set.contains(i))
                .map(|(_, n)| n)
                .collect();
            filtered.push(Record {
                fields: kept_fields,
                nulls: kept_nulls,
            });
        } else {
            first_mismatch.get_or_insert((idx, record.fields.len()));
            mismatch += 1;
        }
    }
    (filtered, mismatch, first_mismatch)
}

/// Type category for SQL type conversion strategy
#[derive(Debug, Clone, Copy, PartialEq)]
enum TypeCategory {
    /// All types except text - bind as string and use CAST() in SQL for type conversion
    /// This ensures PostgreSQL is the single source of truth for type validation
    StringCast,
    /// Text types with direct string binding (TEXT, VARCHAR, CHAR)
    DirectString,
}

impl TypeCategory {
    /// Classify a SQL type into its conversion category
    fn from_sql_type(col_type: &str) -> Self {
        match col_type {
            "TEXT" | "VARCHAR" | "CHAR" => TypeCategory::DirectString,
            _ => TypeCategory::StringCast,
        }
    }
}

/// Result of executing a batch of records
#[derive(Debug)]
struct BatchResult {
    records_loaded: u64,
    records_failed: u64,
    errors: Vec<ErrorRecord>,
    duration_ms: u64,
}

/// Worker that processes chunks of data
pub struct Worker {
    pub worker_id: String,
    pub manifest_storage: Arc<dyn ManifestStorage>,
    pub pool: Pool,
    pub batch_size: usize,
    pub batch_concurrency: usize,
    pub debug: bool,
    pub telemetry_tx: mpsc::UnboundedSender<TelemetryEvent>,
}

impl Worker {
    /// Create a new worker with a random UUID
    pub fn new(
        manifest_storage: Arc<dyn ManifestStorage>,
        pool: Pool,
        batch_size: usize,
        batch_concurrency: usize,
        debug: bool,
        telemetry_tx: mpsc::UnboundedSender<TelemetryEvent>,
    ) -> Self {
        Self {
            worker_id: Uuid::new_v4().to_string(),
            manifest_storage,
            pool,
            batch_size,
            batch_concurrency,
            debug,
            telemetry_tx,
        }
    }

    /// Send batch telemetry event
    fn send_batch_telemetry(&self, batch_result: &BatchResult, bytes_per_record: u64) {
        let _ = self.telemetry_tx.send(TelemetryEvent::BatchLoaded {
            records_loaded: batch_result.records_loaded,
            bytes_processed: batch_result.records_loaded * bytes_per_record,
            duration_ms: batch_result.duration_ms,
        });
    }

    /// Run the worker loop until no more work is available
    pub async fn run(&self, job_id: &str, file_reader: Arc<dyn FileReader>) -> Result<()> {
        use rand::seq::SliceRandom;

        loop {
            let mut unclaimed = self
                .manifest_storage
                .list_unclaimed_chunks(job_id)
                .await
                .context("Failed to list unclaimed chunks")?;

            if unclaimed.is_empty() {
                break;
            }

            // Shuffle chunks to randomize processing order and avoid contention
            unclaimed.shuffle(&mut rand::thread_rng());

            let mut claimed_any = false;
            for chunk_id in unclaimed {
                let claimed = self
                    .try_claim_and_process(job_id, chunk_id, &file_reader)
                    .await?;
                if claimed {
                    claimed_any = true;
                    break;
                }
            }

            if !claimed_any {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }

        Ok(())
    }

    /// Try to claim and process a single chunk
    async fn try_claim_and_process(
        &self,
        job_id: &str,
        chunk_id: u32,
        file_reader: &Arc<dyn FileReader>,
    ) -> Result<bool> {
        let claim = ClaimFile {
            chunk_id,
            worker_id: self.worker_id.clone(),
            claimed_at: Utc::now().to_rfc3339(),
        };

        let claimed = self
            .manifest_storage
            .try_claim_chunk(job_id, chunk_id, &claim)
            .await
            .context("Failed to claim chunk")?;

        if !claimed {
            return Ok(false);
        }

        // Process chunk (writes its own result)
        self.process_chunk(job_id, chunk_id, file_reader).await?;
        Ok(true)
    }

    /// Process a single chunk by reading data and loading it into the database
    /// Writes the result to manifest storage before returning
    ///
    /// # Cancellation Safety
    ///
    /// This function is **not cancellation-safe** in the strict sense: if the future is dropped
    /// mid-execution, some database records may be committed while others are not. However, the
    /// manifest tracking system provides recovery guarantees:
    ///
    /// ## What happens on cancellation/crash:
    /// - **Partial batch commits**: Individual batches are committed independently via separate
    ///   INSERT statements. If cancellation occurs after some batches complete, those records
    ///   remain committed in the database.
    /// - **No result file**: The chunk result file is only written at the end (after all batches
    ///   complete). If cancelled before completion, no result file exists.
    /// - **Chunk remains claimed**: The claim file was created before processing started and
    ///   persists after cancellation.
    ///
    /// ## Recovery on retry:
    /// - **Stuck chunks detection**: When resuming with `--resume-job-id`, the coordinator detects:
    ///   - Chunks with claim files but no result files (worker crash/cancellation)
    ///   - Chunks with result files where `records_failed > 0` (partial failures)
    ///
    ///   Both types are automatically retried by removing their claim/result files.
    /// - **Re-processing with idempotency**: If the table has unique constraints
    ///   (`has_unique_constraints = true`), re-processing the chunk will use `ON CONFLICT DO NOTHING`,
    ///   making duplicate inserts safe. Already-committed records are silently skipped.
    /// - **Without unique constraints**: Re-processing without unique constraints may insert
    ///   duplicate records. Use unique constraints for safe retry, or manually deduplicate afterward.
    ///
    /// ## Design rationale:
    /// - **Performance over strict ACID**: Batches are committed independently (not in a single
    ///   transaction) to maximize throughput and avoid long-running transactions.
    /// - **Resumability**: The chunk-level manifest tracking allows detecting and re-processing
    ///   incomplete work, trading strict once-semantics for better fault tolerance and performance.
    async fn process_chunk(
        &self,
        job_id: &str,
        chunk_id: u32,
        file_reader: &Arc<dyn FileReader>,
    ) -> Result<()> {
        use tokio::task::JoinSet;

        // Send chunk started event
        let _ = self.telemetry_tx.send(TelemetryEvent::ChunkStarted);

        let start_time = Utc::now();
        let start_instant = std::time::Instant::now();

        let manifest = self
            .manifest_storage
            .read_manifest(job_id)
            .await
            .context("Failed to read manifest")?;

        let chunk_info = manifest
            .chunks
            .iter()
            .find(|p| p.chunk_id == chunk_id)
            .context("Chunk not found in manifest")?;

        let chunk = crate::formats::Chunk {
            chunk_id,
            start_offset: chunk_info.start_offset,
            end_offset: chunk_info.end_offset,
            estimated_rows: chunk_info.estimated_rows,
        };

        let mut chunk_data = file_reader
            .read_chunk(&chunk)
            .await
            .context("Failed to read chunk data")?;

        // Apply --exclude-columns field filtering before batching. Skip-mode:
        // each source record must contain all table columns; we drop fields at
        // excluded positions so downstream batch INSERTs target only the effective
        // schema. Records with the wrong field count are dropped and counted as
        // failures, mirroring the existing `parse_errors` aggregation pattern.
        let excluded_positions: &[usize] = &manifest.table.excluded_positions;

        // Snapshot the record count before exclusion so bytes_per_record divides by
        // the true number of records the chunk's bytes came from. Dividing by the
        // post-filter count would overstate per-record size (numerator unchanged,
        // denominator smaller) and, when multiplied by records_loaded downstream,
        // would cause reported bytes_processed to exceed bytes actually read.
        let pre_filter_record_count = chunk_data.records.len();

        let (field_mapping_errors, first_mismatch, full_len) = if !excluded_positions.is_empty() {
            // Invariant: setup_new_job refuses to activate exclusion without a
            // resolved schema. Fail loudly rather than silently mis-batch every record.
            let effective_len = manifest
                .table
                .schema
                .as_ref()
                .map(|s| s.columns.len())
                .ok_or_else(|| {
                    anyhow!(
                        "Internal error: manifest has excluded_positions but no schema; \
                         cannot compute effective field count"
                    )
                })?;
            let full_len = effective_len + excluded_positions.len();
            let original_records = std::mem::take(&mut chunk_data.records);
            let (filtered, mismatch, first_sample) =
                apply_field_exclusion(original_records, excluded_positions, full_len);
            chunk_data.records = filtered;
            (mismatch, first_sample, Some(full_len))
        } else {
            (0u64, None, None)
        };

        // Split chunk into batches with line offset tracking
        let batches: Vec<Vec<Record>> = chunk_data
            .records
            .chunks(self.batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Calculate average bytes per record for telemetry
        let bytes_per_record = if pre_filter_record_count == 0 {
            0
        } else {
            chunk_data.bytes_read / pre_filter_record_count as u64
        };

        // Process batches in parallel with JoinSet for concurrency control
        let mut join_set: JoinSet<BatchResult> = JoinSet::new();
        let mut results = Vec::new();

        // Track line numbers: chunk records start at line 1, +1 if file has header (to skip header line).
        // Note: `current_line` under-counts by the number of records dropped before
        // batching (parse_errors or field_count_mismatch paths), so per-batch line
        // numbers are unreliable as absolute source-file positions whenever drops
        // occurred upstream. Chunk-level errors from those drop paths use
        // line_number: 0 as a sentinel (see ErrorRecord doc comment).
        let has_header = match &manifest.file_format {
            crate::coordination::manifest::FileFormat::Csv(c)
            | crate::coordination::manifest::FileFormat::Tsv(c) => c.has_header,
            crate::coordination::manifest::FileFormat::Parquet(_) => false,
            crate::coordination::manifest::FileFormat::PgDump(_) => false,
        };
        let mut current_line = if has_header { 2u64 } else { 1u64 };

        for batch in batches {
            // Wait if we've reached concurrency limit
            while join_set.len() >= self.batch_concurrency {
                if let Some(result) = join_set.join_next().await {
                    match result {
                        Ok(batch_result) => {
                            self.send_batch_telemetry(&batch_result, bytes_per_record);
                            results.push(batch_result);
                        }
                        Err(e) => return Err(anyhow!("Task panicked: {}", e)),
                    }
                }
            }

            // Spawn new batch processing task
            let pool = self.pool.clone();
            let schema_name = manifest.table.schema_name.clone();
            let table_name = manifest.table.name.clone();
            let schema = manifest.table.schema.clone();
            let on_conflict = manifest.table.on_conflict;
            let conflict_columns = manifest.table.conflict_columns.clone();
            let line_offset = current_line;
            let batch_len = batch.len() as u64;
            let debug = self.debug;

            join_set.spawn(async move {
                Self::load_batch(
                    &pool,
                    &schema_name,
                    &table_name,
                    &batch,
                    &schema,
                    on_conflict,
                    &conflict_columns,
                    line_offset,
                    debug,
                )
                .await
            });

            current_line += batch_len;
        }

        // Wait for remaining tasks
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(batch_result) => {
                    self.send_batch_telemetry(&batch_result, bytes_per_record);
                    results.push(batch_result);
                }
                Err(e) => return Err(anyhow!("Task panicked: {}", e)),
            }
        }

        // Aggregate results from all batches
        let mut records_loaded = 0u64;
        let mut records_failed = 0u64;
        let mut errors = Vec::new();

        // Include CSV/TSV parse errors as failed records
        if chunk_data.parse_errors > 0 {
            records_failed += chunk_data.parse_errors;
            errors.push(ErrorRecord {
                line_number: 0,
                error_type: "parse_error".to_string(),
                error_message: format!(
                    "{} record(s) failed to parse from source file. Check delimiter, quote, and escape settings.",
                    chunk_data.parse_errors
                ),
            });
        }

        // Include --exclude-columns field-count mismatches as failed records.
        // `field_mapping_errors > 0` implies exclusion was active, so `full_len` is Some.
        if let Some(full_len) = full_len
            && field_mapping_errors > 0
        {
            records_failed += field_mapping_errors;
            let first_sample = first_mismatch
                .map(|(idx, n)| {
                    format!(
                        " (first mismatch at chunk record index {} had {} fields)",
                        idx, n
                    )
                })
                .unwrap_or_default();
            errors.push(ErrorRecord {
                line_number: 0,
                error_type: "field_count_mismatch".to_string(),
                error_message: format!(
                    "{} record(s) in chunk {} had wrong field count; expected {}{} \
                     (source file must include all table columns including excluded \
                     ones, which will be skipped)",
                    field_mapping_errors, chunk_id, full_len, first_sample
                ),
            });
        }

        for result in results {
            records_loaded += result.records_loaded;
            records_failed += result.records_failed;
            errors.extend(result.errors);
        }

        let end_time = Utc::now();
        let duration_secs = start_instant.elapsed().as_secs();

        // Send chunk completed event
        let _ = self
            .telemetry_tx
            .send(TelemetryEvent::ChunkCompleted { records_failed });

        // Determine status based on whether any records failed
        let status = if records_failed > 0 {
            ChunkStatus::Failed
        } else {
            ChunkStatus::Success
        };

        let result = ChunkResultFile {
            chunk_id,
            worker_id: self.worker_id.clone(),
            status,
            records_loaded,
            records_failed,
            bytes_processed: chunk_data.bytes_read,
            started_at: start_time.to_rfc3339(),
            completed_at: end_time.to_rfc3339(),
            duration_secs,
            errors,
        };

        // Write result to manifest
        self.manifest_storage
            .write_result(job_id, chunk_id, &result)
            .await
            .context("Failed to write chunk result")?;

        // Return error if any records failed
        if records_failed > 0 {
            Err(anyhow!(
                "Chunk had {} failed records out of {} total",
                records_failed,
                records_loaded + records_failed
            ))
        } else {
            Ok(())
        }
    }

    /// Load a batch of records into the database using batch inserts
    /// Returns BatchResult with accurate counts of loaded/failed records
    /// Errors are captured in the result, not returned as Err
    #[allow(clippy::too_many_arguments)]
    async fn load_batch(
        pool: &Pool,
        schema_name: &str,
        table_name: &str,
        records: &[Record],
        schema: &Option<super::manifest::SchemaJson>,
        on_conflict: super::manifest::OnConflict,
        conflict_columns: &[String],
        line_offset: u64,
        debug: bool,
    ) -> BatchResult {
        // Check if we're using PostgreSQL (CAST needed) or SQLite (CAST causes issues)
        let use_pg_cast = pool.is_postgres();
        let start = std::time::Instant::now();

        if records.is_empty() {
            return BatchResult {
                records_loaded: 0,
                records_failed: 0,
                errors: Vec::new(),
                duration_ms: 0,
            };
        }

        let num_columns = if let Some(s) = schema {
            s.columns.len()
        } else {
            records.first().map(|r| r.fields.len()).unwrap_or(0)
        };

        // Build column list for INSERT statement
        let column_list = if let Some(s) = schema {
            let col_names: Vec<String> = s
                .columns
                .iter()
                .map(|c| format!("\"{}\"", c.name))
                .collect();
            format!("({})", col_names.join(", "))
        } else {
            String::new()
        };

        // Build batch INSERT statement: INSERT INTO table (col1, col2) VALUES ('val1', 'val2'), ('val3', 'val4'), ...
        // Values are embedded directly in the SQL string with proper escaping
        let mut value_groups = Vec::new();

        for record in records {
            let values: Vec<String> = record
                .fields
                .iter()
                .enumerate()
                .map(|(col_idx, field)| {
                    // The reader sets `nulls[i]` per its format's convention:
                    // CSV/TSV/parquet flag trimmed-empty fields (preserving the
                    // legacy empty→NULL inference that used to live here);
                    // pg_dump flags only `\N` so genuine empty strings round-trip.
                    // Worker just consults the mask.
                    if record.nulls.get(col_idx).copied().unwrap_or(false) {
                        return "NULL".to_string();
                    }

                    // Escape the field value for SQL
                    let escaped = Self::escape_sql_string(field);

                    // Add CAST() for types that need it
                    // Only for PostgreSQL - SQLite doesn't handle CAST() well for DATE/TIME types
                    let value_expr = format!("'{}'", escaped);
                    schema
                        .as_ref()
                        .and_then(|s| s.columns.get(col_idx))
                        .filter(|col| {
                            use_pg_cast
                                && TypeCategory::from_sql_type(&col.col_type)
                                    == TypeCategory::StringCast
                        })
                        .map(|col| format!("CAST({} AS {})", value_expr, col.col_type))
                        .unwrap_or(value_expr)
                })
                .collect();
            value_groups.push(format!("({})", values.join(", ")));
        }

        let values_clause = format!("VALUES {}", value_groups.join(", "));
        let conflict_clause =
            match Self::build_conflict_clause(on_conflict, conflict_columns, schema) {
                Ok(clause) => clause,
                Err(err) => {
                    let duration_ms = start.elapsed().as_millis() as u64;
                    return BatchResult {
                        records_loaded: 0,
                        records_failed: records.len() as u64,
                        errors: vec![ErrorRecord {
                            line_number: line_offset,
                            error_type: "conflict_clause_error".to_string(),
                            error_message: err,
                        }],
                        duration_ms,
                    };
                }
            };

        // Use Pool helper to generate the properly formatted table name
        let table_spec = pool.qualified_table_name(schema_name, table_name);
        let insert_sql = format!(
            "INSERT INTO {} {} {}{}",
            table_spec, column_list, values_clause, conflict_clause
        );

        // Execute with retry on error code 42001
        match Self::execute_with_retry(pool, &insert_sql).await {
            Ok(_) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                BatchResult {
                    records_loaded: records.len() as u64,
                    records_failed: 0,
                    errors: Vec::new(),
                    duration_ms,
                }
            }
            Err(e) => {
                // Batch failed - create error with context about the failing records
                let first_record_sample = records
                    .first()
                    .map(|r| {
                        let preview: Vec<_> = r
                            .fields
                            .iter()
                            .take(3)
                            .map(|f| {
                                if f.len() > 20 {
                                    format!("{}...", &f[..20])
                                } else {
                                    f.clone()
                                }
                            })
                            .collect();
                        format!(
                            "[{}{}]",
                            preview.join(", "),
                            if r.fields.len() > 3 { ", ..." } else { "" }
                        )
                    })
                    .unwrap_or_else(|| "<empty>".to_string());

                let chain: Vec<_> = e.chain().collect();
                // Show both first and last errors for complete context
                let db_error = if chain.len() > 1
                    && chain[0].to_string() != chain[chain.len() - 1].to_string()
                {
                    format!("{}\n(Root cause: {})", chain[0], chain[chain.len() - 1])
                } else {
                    chain[0].to_string()
                };
                // Build detailed error message with full chain for debugging
                let full_error_chain = if chain.len() > 1 {
                    let chain_str = chain
                        .iter()
                        .enumerate()
                        .map(|(i, err)| format!("  {}. {}", i + 1, err))
                        .collect::<Vec<_>>()
                        .join("\n");
                    format!("\n\nFull error chain:\n{}", chain_str)
                } else {
                    String::new()
                };

                // Check if this is a parameter limit error and add helpful hint
                let parameter_limit_hint =
                    Self::get_parameter_limit_hint(&db_error, records.len(), num_columns);

                // Gate verbose output behind debug flag
                let verbose_details = if debug {
                    format!(
                        "\n\
                         \n\
                         Batch context:\n\
                         - Batch size: {} records\n\
                         - First record sample: {}\n\
                         - SQL statement: INSERT INTO {} {} VALUES ...{}",
                        records.len(),
                        first_record_sample,
                        pool.qualified_table_name(schema_name, table_name),
                        column_list,
                        full_error_chain
                    )
                } else {
                    String::new()
                };

                let error_message = format!(
                    "Database error: {}{}{}",
                    db_error, verbose_details, parameter_limit_hint
                );

                let duration_ms = start.elapsed().as_millis() as u64;
                BatchResult {
                    records_loaded: 0,
                    records_failed: records.len() as u64,
                    errors: vec![ErrorRecord {
                        line_number: line_offset,
                        error_type: "batch_error".to_string(),
                        error_message,
                    }],
                    duration_ms,
                }
            }
        }
    }

    /// Escape a string value for use in SQL
    /// Handles single quotes by doubling them (SQL standard escaping)
    fn escape_sql_string(value: &str) -> String {
        value.replace('\'', "''")
    }

    /// Build the ON CONFLICT clause based on the conflict resolution mode
    ///
    /// Note: For DoUpdate mode, this updates ALL columns (including non-key columns).
    /// The conflict key columns are included in the SET clause, though PostgreSQL
    /// will effectively ignore updates to the constrained columns.
    fn build_conflict_clause(
        on_conflict: super::manifest::OnConflict,
        conflict_columns: &[String],
        schema: &Option<super::manifest::SchemaJson>,
    ) -> Result<String, String> {
        use super::manifest::OnConflict;
        match on_conflict {
            OnConflict::DoNothing => {
                // Always use DO NOTHING
                Ok(" ON CONFLICT DO NOTHING".to_string())
            }
            OnConflict::DoUpdate => {
                // Build ON CONFLICT ... DO UPDATE SET ...
                if conflict_columns.is_empty() {
                    return Err(
                        "do-update mode requires table with unique constraints or primary key"
                            .to_string(),
                    );
                }

                // Require schema information to build the SET clause
                let Some(s) = schema else {
                    return Err(
                        "do-update mode requires schema information to build UPDATE clause"
                            .to_string(),
                    );
                };

                // Build conflict target: ON CONFLICT (col1, col2)
                let conflict_target = conflict_columns
                    .iter()
                    .map(|c| format!("\"{}\"", c))
                    .collect::<Vec<_>>()
                    .join(", ");

                // Build SET clause updating all columns
                let set_clause = s
                    .columns
                    .iter()
                    .map(|c| format!("\"{}\" = EXCLUDED.\"{}\"", c.name, c.name))
                    .collect::<Vec<_>>()
                    .join(", ");

                Ok(format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    conflict_target, set_clause
                ))
            }
            OnConflict::Error => {
                // No conflict clause - let database error on conflicts
                Ok(String::new())
            }
        }
    }

    /// Execute a batch insert with retry logic for transient errors
    async fn execute_with_retry(pool: &Pool, insert_sql: &str) -> Result<()> {
        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            let result = Self::try_execute(pool, insert_sql).await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) if Self::is_retryable_error(&e) && attempt < MAX_RETRIES - 1 => {
                    let delay = Self::backoff_delay(attempt);
                    tokio::time::sleep(delay).await;
                    last_error = Some(e);
                }
                Err(e) => {
                    return Err(e).context(format!(
                        "Failed to execute batch insert after {} attempts",
                        attempt + 1
                    ));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Unknown error")))
            .context(format!("Exhausted {MAX_RETRIES} retry attempts"))
    }

    /// Calculate exponential backoff delay with jitter
    fn backoff_delay(attempt: u32) -> Duration {
        let exp_delay = BASE_DELAY.as_millis().saturating_mul(2u128.pow(attempt));
        let jitter = rand::thread_rng().gen_range(0.75..=1.25);
        let delay_ms = (exp_delay as f64 * jitter) as u64;
        Duration::from_millis(delay_ms).min(MAX_DELAY)
    }

    /// Attempt to execute a batch insert (single try, no retry logic)
    async fn try_execute(pool: &Pool, insert_sql: &str) -> Result<()> {
        let mut conn = pool
            .acquire()
            .await
            .context("Failed to acquire connection from pool")?;

        // We use raw_sql to batch insert here because prepared statements
        // don't improve performance (it's a very simple query plan) and
        // increase memory usage of QPs. The indirection of the helpers is
        // to help convince the compiler that our lifetimes are right.
        let execute = async {
            match &mut conn {
                crate::db::pool::PoolConnection::Postgres(pg_conn) => {
                    run_raw_sql_pg(pg_conn, insert_sql).await.map(|_| ())
                }
                #[cfg(test)]
                crate::db::pool::PoolConnection::Sqlite(sqlite_conn) => {
                    run_raw_sql_sqlite(sqlite_conn, insert_sql)
                        .await
                        .map(|_| ())
                }
            }
        };

        tokio::time::timeout(QUERY_TIMEOUT, execute)
            .await
            .map_err(|_| anyhow!("Query timed out after {}s", QUERY_TIMEOUT.as_secs()))?
            .map_err(Into::into)
    }

    /// Generate a helpful hint if the error is related to parameter limits
    fn get_parameter_limit_hint(error_msg: &str, batch_size: usize, num_columns: usize) -> String {
        let error_lower = error_msg.to_lowercase();

        // Check for parameter limit related errors
        // PostgreSQL/DSQL: "too many arguments for query"
        // SQLite: "too many SQL variables"
        let is_param_limit_error = error_lower.contains("too many arguments")
            || error_lower.contains("too many sql variables");

        if !is_param_limit_error {
            return String::new();
        }

        let total_params = batch_size * num_columns;

        // PostgreSQL/DSQL limit: 65,535, SQLite limit: 32,766
        // Suggest a batch size that keeps us well under both limits
        let suggested_batch_size = match 20000usize.checked_div(num_columns) {
            // Target ~20,000 parameters to stay safely under both limits
            Some(safe_batch) => safe_batch.max(1).min(batch_size - 1),
            None => batch_size / 2,
        };

        format!(
            "\n\n\
            Hint: This error is caused by exceeding the database parameter limit.\n\
            - Current batch: {} records × {} columns = {} parameters\n\
            - Try reducing --batch-size to {} or lower to stay under the limit.",
            batch_size, num_columns, total_params, suggested_batch_size
        )
    }

    /// Check if error is retriable (transient errors that may resolve with retry)
    fn is_retryable_error(error: &anyhow::Error) -> bool {
        // Check for timeout errors (from tokio::time::timeout)
        let error_msg = error.to_string();
        if error_msg.contains("timed out") {
            return true;
        }

        // Try to downcast to sqlx::Error for proper variant matching
        if let Some(sqlx_error) = error.downcast_ref::<sqlx::Error>() {
            match sqlx_error {
                // Connection pool errors - always retryable
                sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed => return true,

                // IO errors (network issues, broken pipes, etc.) - retryable
                sqlx::Error::Io(_) => return true,

                // Database-specific errors - check error codes
                sqlx::Error::Database(db_err) => {
                    if let Some(code) = db_err.code() {
                        // PostgreSQL/DSQL error codes:
                        // 40001 - serialization_failure (transaction conflicts)
                        // 40P01 - deadlock_detected
                        // 42001 - DSQL-specific transient error
                        // 08xxx - Connection errors (08000, 08003, 08006, 08P01)
                        // 53xxx - Insufficient resources (53000, 53100, 53200, 53300, 53400)
                        // 57xxx - Operator intervention (57000, 57014, 57P01, 57P02, 57P03)
                        if code.starts_with("08")
                            || code.starts_with("53")
                            || code.starts_with("57")
                            || code == "40001"
                            || code == "40P01"
                            || code == "42001"
                        {
                            return true;
                        }
                    }

                    // Fall back to message checking for specific patterns
                    let db_msg = db_err.message();
                    if db_msg.contains("server unavailable")
                        || db_msg.contains("transaction age limit")
                        || db_msg.contains("Connection reset")
                    {
                        return true;
                    }
                }

                // All other errors are not retryable
                _ => return false,
            }
        }

        false
    }
}

async fn run_raw_sql_pg(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    sql: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    use sqlx::Executor;
    (&mut **conn).execute(sqlx::raw_sql(sql)).await
}

#[cfg(test)]
async fn run_raw_sql_sqlite(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    sql: &str,
) -> Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> {
    use sqlx::Executor;
    (&mut **conn).execute(sqlx::raw_sql(sql)).await
}

#[cfg(test)]
mod field_exclusion_tests {
    use super::*;

    fn mk_record(fields: &[&str]) -> Record {
        let fields: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
        let nulls = fields.iter().map(|f| f.trim().is_empty()).collect();
        Record { fields, nulls }
    }

    #[test]
    fn skip_mode_drops_excluded_positions() {
        let records = vec![mk_record(&[
            "uuid-1",
            "Alice",
            "alice@ex.com",
            "2025-01-01",
        ])];
        let (filtered, mismatch, _first) = apply_field_exclusion(records, &[0, 3], 4);
        assert_eq!(mismatch, 0);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].fields, vec!["Alice", "alice@ex.com"]);
    }

    #[test]
    fn mismatched_field_count_is_counted_and_dropped() {
        let records = vec![
            mk_record(&["uuid-1", "Alice", "alice@ex.com", "2025-01-01"]),
            mk_record(&["Alice", "alice@ex.com"]),
            mk_record(&["uuid-2", "Bob", "bob@ex.com", "2025-01-02", "extra"]),
        ];
        let (filtered, mismatch, first) = apply_field_exclusion(records, &[0, 3], 4);
        assert_eq!(mismatch, 2);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].fields, vec!["Alice", "alice@ex.com"]);
        // First mismatch is the 2-field record at chunk index 1.
        assert_eq!(first, Some((1, 2)));
    }

    #[test]
    fn empty_excluded_positions_is_identity_over_fields() {
        let records = vec![mk_record(&["a", "b"])];
        let (filtered, mismatch, _first) = apply_field_exclusion(records, &[], 2);
        assert_eq!(mismatch, 0);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].fields, vec!["a", "b"]);
    }

    // Feature: loader-column-exclusion, Property 3: field mapping correctly skips excluded positions
    proptest::proptest! {
        #[test]
        fn prop_skip_mode_preserves_non_excluded_fields_in_order(
            full_len in 2usize..10,
            exclude_mask in proptest::collection::vec(proptest::bool::ANY, 2..10),
            record_count in 1usize..5,
        ) {
            let mask: Vec<bool> = exclude_mask
                .into_iter()
                .take(full_len)
                .chain(std::iter::repeat(false))
                .take(full_len)
                .collect();
            let excluded_positions: Vec<usize> = mask
                .iter()
                .enumerate()
                .filter_map(|(i, &b)| b.then_some(i))
                .collect();
            proptest::prop_assume!(
                !excluded_positions.is_empty() && excluded_positions.len() < full_len
            );

            let records: Vec<Record> = (0..record_count)
                .map(|r| Record {
                    fields: (0..full_len).map(|i| format!("r{}_c{}", r, i)).collect(),
                    nulls: vec![false; full_len],
                })
                .collect();

            let (filtered, mismatch, _first) =
                apply_field_exclusion(records.clone(), &excluded_positions, full_len);
            proptest::prop_assert_eq!(mismatch, 0);
            proptest::prop_assert_eq!(filtered.len(), record_count);

            let expected_len = full_len - excluded_positions.len();
            for (r_idx, record) in filtered.iter().enumerate() {
                proptest::prop_assert_eq!(record.fields.len(), expected_len);
                let expected: Vec<String> = (0..full_len)
                    .filter(|i| !excluded_positions.contains(i))
                    .map(|i| format!("r{}_c{}", r_idx, i))
                    .collect();
                proptest::prop_assert_eq!(&record.fields, &expected);
            }
        }

        #[test]
        fn prop_wrong_field_count_always_counts_as_mismatch(
            full_len in 2usize..8,
            offset in proptest::sample::select(vec![-2i32, -1, 1, 2, 3]),
        ) {
            let actual_len = (full_len as i32 + offset).max(0) as usize;
            proptest::prop_assume!(actual_len != full_len);

            let excluded_positions: Vec<usize> = vec![0];

            let records = vec![Record {
                fields: (0..actual_len).map(|i| format!("c{}", i)).collect(),
                nulls: vec![false; actual_len],
            }];
            let (filtered, mismatch, first) =
                apply_field_exclusion(records, &excluded_positions, full_len);
            proptest::prop_assert_eq!(mismatch, 1);
            proptest::prop_assert!(filtered.is_empty());
            proptest::prop_assert_eq!(first, Some((0, actual_len)));
        }
    }
}
