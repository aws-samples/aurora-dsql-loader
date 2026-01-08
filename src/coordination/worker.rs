use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::manifest::{ClaimFile, ErrorRecord, ManifestStorage, PartitionResultFile};
use crate::config::{MAX_RETRIES, QUERY_TIMEOUT};
use crate::db::Pool;
use crate::formats::{FileReader, Record};
use crate::telemetry::TelemetryEvent;

/// Type category for SQL type conversion strategy
#[derive(Debug, Clone, Copy, PartialEq)]
enum TypeCategory {
    /// Native numeric types parsed to Rust types (i16, i32, i64, f32, f64, bool)
    NativeNumeric,
    /// Complex types parsed to native Rust types (UUID, NaiveDateTime)
    NativeParsed,
    /// Complex types requiring CAST in PostgreSQL (DATE, TIME, INTERVAL, BYTEA, TIMESTAMP WITH TIME ZONE)
    StringCast,
    /// Text types with direct string binding (TEXT, VARCHAR, CHAR)
    DirectString,
}

impl TypeCategory {
    /// Classify a SQL type into its conversion category
    fn from_sql_type(col_type: &str) -> Self {
        match col_type {
            "BOOLEAN" | "SMALLINT" | "INTEGER" | "BIGINT" | "REAL" | "DOUBLE PRECISION"
            | "NUMERIC" => TypeCategory::NativeNumeric,
            "UUID" | "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => TypeCategory::NativeParsed,
            "TIMESTAMP WITH TIME ZONE"
            | "DATE"
            | "TIME"
            | "TIME WITHOUT TIME ZONE"
            | "INTERVAL"
            | "BYTEA" => TypeCategory::StringCast,
            _ => TypeCategory::DirectString,
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

/// Worker that processes partitions of data
pub struct Worker {
    pub worker_id: String,
    pub manifest_storage: Arc<dyn ManifestStorage>,
    pub pool: Pool,
    pub batch_size: usize,
    pub batch_concurrency: usize,
    pub telemetry_tx: mpsc::UnboundedSender<TelemetryEvent>,
}

impl Worker {
    /// Create a new worker with a random UUID
    pub fn new(
        manifest_storage: Arc<dyn ManifestStorage>,
        pool: Pool,
        batch_size: usize,
        batch_concurrency: usize,
        telemetry_tx: mpsc::UnboundedSender<TelemetryEvent>,
    ) -> Self {
        Self {
            worker_id: Uuid::new_v4().to_string(),
            manifest_storage,
            pool,
            batch_size,
            batch_concurrency,
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
        loop {
            let unclaimed = self
                .manifest_storage
                .list_unclaimed_partitions(job_id)
                .await
                .context("Failed to list unclaimed partitions")?;

            if unclaimed.is_empty() {
                break;
            }

            let mut claimed_any = false;
            for partition_id in unclaimed {
                let claimed = self
                    .try_claim_and_process(job_id, partition_id, &file_reader)
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

    /// Try to claim and process a single partition
    async fn try_claim_and_process(
        &self,
        job_id: &str,
        partition_id: u32,
        file_reader: &Arc<dyn FileReader>,
    ) -> Result<bool> {
        let claim = ClaimFile {
            partition_id,
            worker_id: self.worker_id.clone(),
            claimed_at: Utc::now().to_rfc3339(),
        };

        let claimed = self
            .manifest_storage
            .try_claim_partition(job_id, partition_id, &claim)
            .await
            .context("Failed to claim partition")?;

        if !claimed {
            return Ok(false);
        }

        // Process partition (writes its own result)
        self.process_partition(job_id, partition_id, file_reader)
            .await?;
        Ok(true)
    }

    /// Process a single partition by reading data and loading it into the database
    /// Writes the result to manifest storage before returning
    async fn process_partition(
        &self,
        job_id: &str,
        partition_id: u32,
        file_reader: &Arc<dyn FileReader>,
    ) -> Result<()> {
        use tokio::task::JoinSet;

        // Send partition started event
        let _ = self.telemetry_tx.send(TelemetryEvent::PartitionStarted);

        let start_time = Utc::now();
        let start_instant = std::time::Instant::now();

        let manifest = self
            .manifest_storage
            .read_manifest(job_id)
            .await
            .context("Failed to read manifest")?;

        let partition_info = manifest
            .partitions
            .iter()
            .find(|p| p.partition_id == partition_id)
            .context("Partition not found in manifest")?;

        let partition = crate::formats::Partition {
            partition_id,
            start_offset: partition_info.start_offset,
            end_offset: partition_info.end_offset,
            estimated_rows: partition_info.estimated_rows,
        };

        let partition_data = file_reader
            .read_partition(&partition)
            .await
            .context("Failed to read partition data")?;

        // Split partition into batches with line offset tracking
        let batches: Vec<Vec<Record>> = partition_data
            .records
            .chunks(self.batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Calculate average bytes per record for telemetry
        let bytes_per_record = if partition_data.records.is_empty() {
            0
        } else {
            partition_data.bytes_read / partition_data.records.len() as u64
        };

        // Process batches in parallel with JoinSet for concurrency control
        let mut join_set: JoinSet<BatchResult> = JoinSet::new();
        let mut results = Vec::new();

        // Track line numbers: partition records start at line 1, +1 if file has header (to skip header line)
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
            let has_unique_constraints = manifest.table.has_unique_constraints;
            let line_offset = current_line;
            let batch_len = batch.len() as u64;

            join_set.spawn(async move {
                Self::load_batch(
                    &pool,
                    &schema_name,
                    &table_name,
                    &batch,
                    &schema,
                    has_unique_constraints,
                    line_offset,
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

        for result in results {
            records_loaded += result.records_loaded;
            records_failed += result.records_failed;
            errors.extend(result.errors);
        }

        let end_time = Utc::now();
        let duration_secs = start_instant.elapsed().as_secs();

        // Send partition completed event
        let _ = self
            .telemetry_tx
            .send(TelemetryEvent::PartitionCompleted { records_failed });

        // Determine status based on whether any records failed
        let status = if records_failed > 0 {
            "failed"
        } else {
            "success"
        };

        let result = PartitionResultFile {
            partition_id,
            worker_id: self.worker_id.clone(),
            status: status.to_string(),
            records_loaded,
            records_failed,
            bytes_processed: partition_data.bytes_read,
            started_at: start_time.to_rfc3339(),
            completed_at: end_time.to_rfc3339(),
            duration_secs,
            errors,
        };

        // Write result to manifest
        self.manifest_storage
            .write_result(job_id, partition_id, &result)
            .await
            .context("Failed to write partition result")?;

        // Return error if any records failed
        if records_failed > 0 {
            Err(anyhow!(
                "Partition had {} failed records out of {} total",
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
    async fn load_batch(
        pool: &Pool,
        schema_name: &str,
        table_name: &str,
        records: &[Record],
        schema: &Option<super::manifest::SchemaJson>,
        has_unique_constraints: bool,
        line_offset: u64,
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
                    if use_pg_cast {
                        if let Some(s) = schema {
                            if let Some(col) = s.columns.get(col_idx) {
                                if TypeCategory::from_sql_type(&col.col_type)
                                    == TypeCategory::StringCast
                                {
                                    format!("CAST({} AS {})", placeholder, col.col_type)
                                } else {
                                    placeholder
                                }
                            } else {
                                placeholder
                            }
                        } else {
                            placeholder
                        }
                    } else {
                        placeholder
                    }
                })
                .collect();
            value_groups.push(format!("({})", placeholders.join(", ")));
        }

        let values_clause = format!("VALUES {}", value_groups.join(", "));
        let conflict_clause = if has_unique_constraints {
            " ON CONFLICT DO NOTHING"
        } else {
            ""
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

                // Extract just the database error message (root cause)
                let db_error = e
                    .source()
                    .and_then(|s| s.source())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| e.to_string());

                let error_message = format!(
                    "Database error: {}\n\
                     \n\
                     Batch context:\n\
                     - Batch size: {} records\n\
                     - First record sample: {}",
                    db_error,
                    records.len(),
                    first_record_sample
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

    /// Execute a batch insert with retry logic for transient errors
    async fn execute_with_retry(
        pool: &Pool,
        insert_sql: &str,
        records: &[Record],
        schema: &Option<super::manifest::SchemaJson>,
    ) -> Result<()> {
        for attempt in 0..MAX_RETRIES {
            // Acquire a connection from the pool
            let mut conn = match pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    if attempt < MAX_RETRIES - 1 {
                        let delay_ms = 100 * 2u64.pow(attempt);
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    return Err(e).context("Failed to acquire connection from pool");
                }
            };

            // Branch based on connection type
            let execute_result = match &mut conn {
                crate::db::pool::PoolConnection::Postgres(_) => {
                    // Postgres: Use existing complex type binding
                    let mut query = sqlx::query(insert_sql);
                    for record in records {
                        query = Self::bind_record_fields(query, &record.fields, schema)?;
                    }
                    tokio::time::timeout(QUERY_TIMEOUT, query.execute(&mut *conn)).await
                }
                #[cfg(test)]
                crate::db::pool::PoolConnection::Sqlite(sqlite_conn) => {
                    // SQLite: Simple string binding for testing
                    Self::execute_sqlite_batch(sqlite_conn, insert_sql, records).await
                }
            };

            match execute_result {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(e)) => {
                    let is_retryable = Self::is_retryable_error(&e);

                    if is_retryable {
                        if attempt < MAX_RETRIES - 1 {
                            let delay_ms = 1000 * 2u64.pow(attempt);
                            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                            continue;
                        }
                        return Err(e)
                            .context("Failed to execute batch insert after {attempt} attempts");
                    }
                    return Err(e).context("Failed to execute batch insert");
                }
                Err(_) => {
                    // Timeout occurred
                    if attempt < MAX_RETRIES - 1 {
                        let delay_ms = 100 * 2u64.pow(attempt);
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        continue;
                    }
                    return Err(anyhow!(
                        "Query execution timed out after {} seconds",
                        QUERY_TIMEOUT.as_secs()
                    ));
                }
            }
        }

        unreachable!("Retry loop should always return");
    }

    /// Execute a batch insert for SQLite (simple string binding for testing)
    #[cfg(test)]
    async fn execute_sqlite_batch(
        conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
        insert_sql: &str,
        records: &[Record],
    ) -> Result<Result<sqlx::postgres::PgQueryResult, sqlx::Error>, tokio::time::error::Elapsed>
    {
        // Convert Postgres placeholders ($1, $2) to SQLite placeholders (?, ?)
        let sqlite_sql = Self::convert_to_sqlite_placeholders(insert_sql);

        let mut query = sqlx::query(&sqlite_sql);

        // Bind all fields as strings (simple approach for testing)
        for record in records {
            for field in &record.fields {
                query = query.bind(field);
            }
        }

        // Execute and convert result to match Postgres return type
        let result = tokio::time::timeout(QUERY_TIMEOUT, query.execute(&mut **conn)).await;
        match result {
            Ok(Ok(_)) => Ok(Ok(sqlx::postgres::PgQueryResult::default())),
            Ok(Err(e)) => Ok(Err(e)),
            Err(e) => Err(e),
        }
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
        schema: &Option<super::manifest::SchemaJson>,
    ) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
        // If no schema, bind as TEXT (old behavior)
        if schema.is_none() {
            for field in fields {
                query = query.bind(field);
            }
            return Ok(query);
        }

        let schema = schema.as_ref().unwrap();

        for (idx, field) in fields.iter().enumerate() {
            let col_type = schema
                .columns
                .get(idx)
                .map(|c| c.col_type.as_str())
                .unwrap_or("TEXT");

            query = Self::bind_typed_value(query, field, col_type)?;
        }

        Ok(query)
    }

    /// Bind a single value with proper type conversion
    fn bind_typed_value<'q>(
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        value: &'q str,
        col_type: &str,
    ) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>> {
        let trimmed = value.trim();

        // Handle NULL values
        if trimmed.is_empty() {
            return Ok(Self::bind_null(query, col_type));
        }

        // Parse and bind based on type
        Ok(match col_type {
            // Numeric types - parse for type safety and validation
            "BOOLEAN" => query.bind(Self::parse_bool(trimmed)),
            "SMALLINT" => query.bind(Self::parse::<i16>(trimmed, col_type)?),
            "INTEGER" => query.bind(Self::parse::<i32>(trimmed, col_type)?),
            "BIGINT" => query.bind(Self::parse::<i64>(trimmed, col_type)?),
            "REAL" => query.bind(Self::parse::<f32>(trimmed, col_type)?),
            "DOUBLE PRECISION" => query.bind(Self::parse::<f64>(trimmed, col_type)?),
            "NUMERIC" => query.bind(Self::parse::<f64>(trimmed, col_type)?),

            // UUID type with native sqlx support
            "UUID" => {
                let uuid = uuid::Uuid::parse_str(trimmed).context(format!(
                    "Type mismatch: Cannot convert value to UUID.\n\
                     - Expected: UUID format (e.g., '550e8400-e29b-41d4-a716-446655440000')\n\
                     - Got: '{}'\n\
                     \n\
                     Suggestion: Check your source data for invalid UUID values.",
                    trimmed
                ))?;
                query.bind(uuid)
            }

            // TIMESTAMP - parse common formats
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
                let timestamp = chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S")
                    })
                    .context(format!(
                        "Type mismatch: Cannot convert value to TIMESTAMP.\n\
                         - Expected: TIMESTAMP format (e.g., '2024-01-15 14:30:00' or '2024-01-15T14:30:00')\n\
                         - Got: '{}'\n\
                         \n\
                         Suggestion: Check your source data for invalid timestamp values.",
                        trimmed
                    ))?;
                query.bind(timestamp)
            }

            // All other types - bind as string, use CAST() in SQL
            // Includes: TIMESTAMP WITH TIME ZONE, DATE, TIME, INTERVAL, BYTEA, TEXT, VARCHAR, CHAR
            _ => query.bind(value),
        })
    }

    /// Bind NULL value for the appropriate type
    fn bind_null<'q>(
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
        col_type: &str,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        match col_type {
            // Numeric types - use typed NULL for proper parameter binding
            "BOOLEAN" => query.bind(None::<bool>),
            "SMALLINT" => query.bind(None::<i16>),
            "INTEGER" => query.bind(None::<i32>),
            "BIGINT" => query.bind(None::<i64>),
            "REAL" => query.bind(None::<f32>),
            "DOUBLE PRECISION" => query.bind(None::<f64>),
            "NUMERIC" => query.bind(None::<f64>),

            // UUID and TIMESTAMP - typed NULL for native types we parse
            "UUID" => query.bind(None::<uuid::Uuid>),
            "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
                query.bind(None::<chrono::NaiveDateTime>)
            }

            // All other types - bind as NULL string, SQL CAST will handle type conversion
            // This includes: TIMESTAMP WITH TIME ZONE, DATE, TIME, INTERVAL, BYTEA, TEXT, VARCHAR, CHAR
            _ => query.bind(None::<String>),
        }
    }

    /// Parse a value from string
    fn parse<T: std::str::FromStr>(value: &str, type_name: &str) -> Result<T>
    where
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        value.parse().map_err(|e| {
            anyhow!(
                "Type mismatch: Cannot convert value to {}.\n\
                 - Expected: Valid {} value\n\
                 - Got: '{}'\n\
                 - Error: {}\n\
                 \n\
                 Suggestion: Check your source data for non-numeric values in numeric columns.",
                type_name,
                type_name,
                value,
                e
            )
        })
    }

    /// Parse boolean value
    fn parse_bool(value: &str) -> bool {
        value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("t") || value == "1"
    }

    /// Check if error is retriable (transient errors that may resolve with retry)
    fn is_retryable_error(error: &sqlx::Error) -> bool {
        let error_msg = error.to_string();

        // Error code 42001 (specific DSQL error)
        if error_msg.contains("42001") {
            return true;
        }

        // Server unavailable or connection errors
        if error_msg.contains("server unavailable")
            || error_msg.contains("OC")
            || error_msg.contains("40001")
            || error_msg.contains("connection")
            || error_msg.contains("timeout")
            || error_msg.contains("transaction age limit")
            || error_msg.contains("Connection reset")
            || error_msg.contains("broken pipe")
        {
            return true;
        }

        false
    }
}
