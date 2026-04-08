use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::manifest::{ChunkResultFile, ChunkStatus, ClaimFile, ErrorRecord, ManifestStorage};
use crate::config::{BASE_DELAY, MAX_DELAY, MAX_RETRIES, QUERY_TIMEOUT};
use crate::db::Pool;
use crate::formats::{FileReader, Record};
use crate::telemetry::TelemetryEvent;

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

        let chunk_data = file_reader
            .read_chunk(&chunk)
            .await
            .context("Failed to read chunk data")?;

        // Split chunk into batches with line offset tracking
        let batches: Vec<Vec<Record>> = chunk_data
            .records
            .chunks(self.batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Calculate average bytes per record for telemetry
        let bytes_per_record = if chunk_data.records.is_empty() {
            0
        } else {
            chunk_data.bytes_read / chunk_data.records.len() as u64
        };

        // Process batches in parallel with JoinSet for concurrency control
        let mut join_set: JoinSet<BatchResult> = JoinSet::new();
        let mut results = Vec::new();

        // Track line numbers: chunk records start at line 1, +1 if file has header (to skip header line)
        let has_header = matches!(
            manifest.file_format,
            crate::coordination::manifest::FileFormat::Csv(_)
                | crate::coordination::manifest::FileFormat::Tsv(_)
        );
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

        // Build batch INSERT statement: INSERT INTO table (col1, col2) VALUES ($1, $2), ($3, $4), ...
        // For types that PostgreSQL won't auto-cast from text, add explicit CAST
        let mut value_groups = Vec::new();
        let mut param_idx = 1;

        for _ in 0..records.len() {
            let placeholders: Vec<String> = (0..num_columns)
                .map(|col_idx| {
                    let placeholder = format!("${}", param_idx);
                    param_idx += 1;

                    // Add CAST() for types we bind as strings (not parsed to native Rust types)
                    // Only for PostgreSQL - SQLite doesn't handle CAST() well for DATE/TIME types
                    schema
                        .as_ref()
                        .and_then(|s| s.columns.get(col_idx))
                        .filter(|col| {
                            use_pg_cast
                                && TypeCategory::from_sql_type(&col.col_type)
                                    == TypeCategory::StringCast
                        })
                        .map(|col| format!("CAST({} AS {})", placeholder, col.col_type))
                        .unwrap_or(placeholder)
                })
                .collect();
            value_groups.push(format!("({})", placeholders.join(", ")));
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
        match Self::execute_with_retry(pool, &insert_sql, records, schema).await {
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
    async fn execute_with_retry(
        pool: &Pool,
        insert_sql: &str,
        records: &[Record],
        schema: &Option<super::manifest::SchemaJson>,
    ) -> Result<()> {
        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            let result = Self::try_execute(pool, insert_sql, records, schema).await;

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
    async fn try_execute(
        pool: &Pool,
        insert_sql: &str,
        records: &[Record],
        schema: &Option<super::manifest::SchemaJson>,
    ) -> Result<()> {
        let mut conn = pool
            .acquire()
            .await
            .context("Failed to acquire connection from pool")?;

        let query_future = match &mut conn {
            crate::db::pool::PoolConnection::Postgres(_) => {
                let mut query = sqlx::query(insert_sql);
                for record in records {
                    query = Self::bind_record_fields(query, &record.fields, schema)?;
                }
                query.execute(&mut *conn)
            }
            #[cfg(test)]
            crate::db::pool::PoolConnection::Sqlite(sqlite_conn) => {
                return Self::execute_sqlite_batch(sqlite_conn, insert_sql, records).await;
            }
        };

        tokio::time::timeout(QUERY_TIMEOUT, query_future)
            .await
            .map_err(|_| anyhow!("Query timed out after {}s", QUERY_TIMEOUT.as_secs()))?
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Execute a batch insert for SQLite (simple string binding for testing)
    #[cfg(test)]
    async fn execute_sqlite_batch(
        conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
        insert_sql: &str,
        records: &[Record],
    ) -> Result<()> {
        // Convert Postgres placeholders ($1, $2) to SQLite placeholders (?, ?)
        let sqlite_sql = Self::convert_to_sqlite_placeholders(insert_sql);

        let mut query = sqlx::query(&sqlite_sql);

        // Bind all fields as strings (simple approach for testing)
        for record in records {
            for field in &record.fields {
                query = query.bind(field);
            }
        }

        tokio::time::timeout(QUERY_TIMEOUT, query.execute(&mut **conn))
            .await
            .map_err(|_| anyhow!("Query timed out after {}s", QUERY_TIMEOUT.as_secs()))?
            .map(|_| ())
            .map_err(Into::into)
    }

    /// Convert Postgres-style placeholders ($1, $2, ...) to SQLite-style (?, ?, ...)
    #[cfg(test)]
    fn convert_to_sqlite_placeholders(sql: &str) -> String {
        let mut result = String::new();
        let mut chars = sql.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '$' {
                // Skip the dollar sign and any following digits
                while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    chars.next();
                }
                result.push('?');
            } else {
                result.push(ch);
            }
        }

        result
    }

    /// Bind record fields to query with proper types based on schema
    fn bind_record_fields<'q>(
        mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        fields: &'q [String],
        _schema: &Option<super::manifest::SchemaJson>,
    ) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
        for field in fields.iter() {
            let trimmed = field.trim();

            // Handle NULL values (empty strings become NULL)
            if trimmed.is_empty() {
                query = query.bind(None::<String>);
            } else {
                // All types bound as strings - PostgreSQL CAST() validates and converts
                query = query.bind(field.as_str());
            }
        }

        Ok(query)
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
        let suggested_batch_size = if num_columns > 0 {
            // Target ~20,000 parameters to stay safely under both limits
            let safe_batch = 20000 / num_columns;
            safe_batch.max(1).min(batch_size - 1)
        } else {
            batch_size / 2
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
