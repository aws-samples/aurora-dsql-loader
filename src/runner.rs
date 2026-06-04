//! High-level runner API for the DSQL data loader.
//!
//! This module provides a simplified public interface that encapsulates all the
//! internal complexity of setting up connections, readers, coordinators, etc.
//!
//! This is the primary API for external users and for the CLI.

// Re-export OnConflict for external use
pub use crate::coordination::manifest::OnConflict;

use anyhow::Result;
use aws_config::{BehaviorVersion, Region};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::coordination::manifest::{LocalManifestStorage, ParquetConfig};
use crate::coordination::{Coordinator, DsqlConfig, FileFormat, LoadConfigBuilder};
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
    PgDump,
}

impl Format {
    /// Parse format from string (case-insensitive)
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(Format::Csv),
            "tsv" => Ok(Format::Tsv),
            "parquet" => Ok(Format::Parquet),
            "pgdump" => Ok(Format::PgDump),
            _ => Err(anyhow::anyhow!(
                "Unsupported format: {}. Supported formats: csv, tsv, parquet, pgdump",
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
            Format::PgDump => crate::formats::Format::PgDump,
        }
    }

    /// Whether the format is delimited text (CSV/TSV) — i.e. whether the CLI's
    /// `--delimiter`/`--quote`/`--escape`/`--header` knobs apply. pg_dump uses
    /// delimited-ish parsing internally but exposes none of those knobs to the
    /// user (separator is fixed, header is the COPY statement), so it returns
    /// `false`.
    pub fn is_delimited(self) -> bool {
        matches!(self, Format::Csv | Format::Tsv)
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
    pub chunk_size_bytes: u64,
    pub batch_size: usize,
    pub batch_concurrency: usize,

    // Options
    pub create_table_if_missing: bool,
    pub manifest_dir: Option<PathBuf>,
    pub quiet: bool,
    pub debug: bool,

    // Column mapping: source column name -> destination column name
    pub column_mappings: HashMap<String, String>,

    // Resume options
    pub resume_job_id: Option<String>,

    // Conflict resolution strategy
    pub on_conflict: OnConflict,

    // Columns to exclude from INSERT statements (DB applies DEFAULT values)
    pub exclude_columns: Vec<String>,

    // Delimited file options (CSV/TSV)
    /// Field delimiter (default: "," for CSV, "\t" for TSV)
    pub delimiter: Option<String>,
    /// Quote character (default: "\"")
    pub quote: Option<String>,
    /// Escape character for escaping quotes (default: None)
    pub escape: Option<String>,
    /// Override for header-row handling. `None` uses the format default
    /// (CSV/TSV: no header — matches PostgreSQL `COPY FROM`).
    pub has_header: Option<bool>,

    // Test-only: inject a pre-created pool (for SQLite testing)
    #[cfg(test)]
    pub test_pool: Option<crate::db::Pool>,
}
/// Result of a completed data load operation
#[derive(Debug)]
pub struct LoadResult {
    pub job_id: String,
    pub chunks_processed: usize,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// Estimated row count from file size (for mismatch detection)
    pub estimated_rows: Option<u64>,
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
/// use aurora_dsql_loader::runner::{LoadArgs, Format, OnConflict, run_load};
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
///     chunk_size_bytes: 10 * 1024 * 1024, // 10MB
///     batch_size: 3000,
///     batch_concurrency: 20,
///     create_table_if_missing: true,
///     manifest_dir: None,
///     column_mappings: HashMap::new(),
///     quiet: true,
///     debug: false,
///     resume_job_id: None,
///     on_conflict: OnConflict::DoNothing,
///     delimiter: None,
///     quote: None,
///     escape: None,
///     // The CSV in this example has a header row that names the destination
///     // columns. With `create_table_if_missing: true`, leaving this as `None`
///     // would name columns from row 1's data values (the new 3.0 default
///     // treats every row as data). Set explicitly when loading a header-bearing file.
///     has_header: Some(true),
///     exclude_columns: Vec::new(),
/// };
///
/// let result = run_load(args).await?;
/// println!("Loaded {} records in {:?}", result.records_loaded, result.duration);
/// # Ok(())
/// # }
/// ```
pub async fn run_load(args: LoadArgs) -> Result<LoadResult> {
    let pool = build_pool(&args).await?;
    run_load_with_pool(pool, args).await
}

/// Build the connection pool from `args`, falling back to `args.test_pool`
/// in `#[cfg(test)]` builds. Factored out of [`run_load`] so the migrate
/// orchestrator can build a pool once and reuse it across the apply stage
/// and every per-table load (the production path can't take the
/// `#[cfg(test)] test_pool` shortcut, so a single helper keeps the two
/// entry points using the same pool-acquisition logic).
async fn build_pool(args: &LoadArgs) -> Result<crate::db::Pool> {
    #[cfg(test)]
    if let Some(test_pool) = args.test_pool.clone() {
        return Ok(test_pool);
    }

    let pool_args = PoolArgsBuilder::default()
        .region(&args.region)
        .endpoint(&args.endpoint)
        .username(&args.username)
        .build()?;
    db_pool::pool::pool(pool_args).await
}

/// Run a load against an externally-supplied pool.
///
/// Identical to [`run_load`] except the caller owns pool construction —
/// the migrate orchestrator uses this so it can apply DDL and run the
/// per-table load against the same physical pool, avoiding repeated IAM
/// auth and connection-establishment overhead.
pub(crate) async fn run_load_with_pool(
    pool: crate::db::Pool,
    args: LoadArgs,
) -> Result<LoadResult> {
    validate_load_args(&args)?;

    let delimited_config = maybe_delimited_config(&args);

    // Set up manifest directory (use temp dir if not provided)
    let (mut temp_dir, manifest_dir_path) = if let Some(dir) = args.manifest_dir {
        (None, dir)
    } else {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();
        (Some(temp_dir), path)
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
    let (file_reader, pgdump_columns) = match args.format {
        Format::PgDump => {
            let (reader, cols) = reader_factory
                .create_pgdump_reader(&parsed_uri, &args.schema, &args.target_table)
                .await?;
            (reader, cols)
        }
        _ => {
            let reader = reader_factory
                .create_reader(
                    &parsed_uri,
                    args.format.to_internal(),
                    delimited_config.clone(),
                )
                .await?;
            (reader, Vec::new())
        }
    };

    // Determine file format config based on format
    let (has_header, file_format) = match args.format {
        Format::Csv => {
            let config = delimited_config.unwrap_or_else(DelimitedConfig::csv);
            (config.has_header, FileFormat::Csv(config))
        }
        Format::Tsv => {
            let config = delimited_config.unwrap_or_else(DelimitedConfig::tsv);
            (config.has_header, FileFormat::Tsv(config))
        }
        Format::Parquet => (false, FileFormat::Parquet(ParquetConfig::default())),
        Format::PgDump => {
            use crate::coordination::manifest::PgDumpConfig;
            (
                false,
                FileFormat::PgDump(PgDumpConfig {
                    copy_columns: pgdump_columns,
                }),
            )
        }
    };

    // Create manifest storage
    let manifest_storage = Arc::new(LocalManifestStorage::new(manifest_dir_path));

    // Create schema inferrer
    let schema_inferrer = SchemaInferrer { has_header };

    // Create coordinator
    let coordinator = Coordinator::new(manifest_storage, file_reader, schema_inferrer, pool);

    // Build load config
    let load_config = LoadConfigBuilder::default()
        .source_uri(args.source_uri)
        .target_table(args.target_table)
        .schema(args.schema)
        .dsql_config(DsqlConfig {
            endpoint: args.endpoint,
            region: args.region,
            username: args.username,
        })
        .worker_count(args.worker_count)
        .chunk_size_bytes(args.chunk_size_bytes)
        .batch_size(args.batch_size)
        .batch_concurrency(args.batch_concurrency)
        .create_table_if_missing(args.create_table_if_missing)
        .file_format(file_format)
        .column_mappings(args.column_mappings)
        .quiet(args.quiet)
        .debug(args.debug)
        .resume_job_id(args.resume_job_id)
        .on_conflict(args.on_conflict)
        .exclude_columns(args.exclude_columns)
        .build()?;

    // Run the load
    let result = coordinator.run_load(&load_config).await?;

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
        chunks_processed: result.chunks_processed,
        records_loaded: result.records_loaded,
        records_failed: result.records_failed,
        estimated_rows: result.estimated_rows,
        duration: result.duration,
        persisted_manifest_dir,
    })
}

/// Reject combinations that would otherwise cause silent drops downstream.
///
/// Mirrors the CLI's `validate_delimited_options` so library consumers calling
/// `run_load` directly get the same feedback as CLI users.
fn validate_load_args(args: &LoadArgs) -> Result<()> {
    validate_identifier("schema", &args.schema)?;
    validate_identifier("table", &args.target_table)?;

    let has_delimited_options = args.delimiter.is_some()
        || args.quote.is_some()
        || args.escape.is_some()
        || args.has_header.is_some();

    if has_delimited_options && !args.format.is_delimited() {
        return Err(anyhow::anyhow!(
            "Delimited file options (delimiter, quote, escape, has_header) \
             can only be used with CSV or TSV formats, not {:?}",
            args.format
        ));
    }

    if args.format == Format::PgDump {
        if !args.column_mappings.is_empty() {
            anyhow::bail!(
                "column_mappings (--column-map) is not supported with pg_dump: \
                 column names come from the COPY statement and cannot be remapped"
            );
        }
        if !args.exclude_columns.is_empty() {
            anyhow::bail!(
                "exclude_columns (--exclude-columns) is not supported with pg_dump: \
                 the column set is fixed by the COPY statement"
            );
        }
        if args.create_table_if_missing {
            anyhow::bail!(
                "create_table_if_missing (--if-not-exists) is not supported with \
                 pg_dump: schema inference from a COPY-format byte stream is not \
                 currently supported; pre-create the target table"
            );
        }
    }
    Ok(())
}

/// Reject SQL-identifier inputs (`--schema`, `--table`) that would break
/// downstream identifier quoting. `Pool::qualified_table_name` interpolates
/// the value into `format!("\"{}\"", …)` without escape-doubling embedded
/// quotes, and the worker's INSERT generation does the same with column
/// names. Rejecting embedded `"` and control bytes here closes the otherwise
/// open path from CLI args into raw SQL.
fn validate_identifier(field: &'static str, value: &str) -> Result<()> {
    if value.is_empty() {
        anyhow::bail!("--{field} must not be empty");
    }
    if value.chars().any(is_unsafe_identifier_char) {
        anyhow::bail!(
            "--{field} {value:?} contains an unsafe character (control byte, \
             backslash, double-quote, or Unicode bidi/format codepoint) that would \
             corrupt SQL identifier quoting or visually deceive an operator reading \
             logs. Rename the table or use a quoted identifier in your DB instead."
        );
    }
    Ok(())
}

/// Unicode bidi-control and zero-width / format codepoints that visually
/// reorder or hide surrounding text in a terminal — RLM, LRM, RTL/LTR
/// overrides, ZWSP/ZWNJ/ZWJ, BOM/ZWNBSP, line/paragraph separators, bidi
/// isolates. A deceptive identifier carrying any of these would confuse an
/// operator reading load logs even though it cannot escape SQL quoting.
///
/// Shared between `validate_identifier` (CLI `--schema` / `--table` inputs)
/// and `main::is_unsafe_for_listing` (the `list-tables` TSV output guard).
pub fn is_bidi_or_format_char(c: char) -> bool {
    matches!(
        c,
        '\u{200B}'..='\u{200F}'
            | '\u{2028}'..='\u{202E}'
            | '\u{2066}'..='\u{2069}'
            | '\u{FEFF}'
    )
}

fn is_unsafe_identifier_char(c: char) -> bool {
    c.is_control() || c == '"' || c == '\\' || is_bidi_or_format_char(c)
}

/// One entry per `COPY ... FROM stdin;` block in a pg_dump file.
#[derive(Debug, Clone)]
pub struct PgDumpTable {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
}

/// Enumerate every COPY block in a pg_dump file, in source order.
///
/// Pre-flight discovery for multi-table workflows. Customers script with this
/// today; future versions may add a built-in `--all-tables` mode that uses the
/// same primitive internally.
pub async fn list_pgdump_tables(source_uri: &str) -> Result<Vec<PgDumpTable>> {
    use crate::formats::pgdump::list_copy_blocks;
    use crate::io::{LocalFileByteReader, S3ByteReader};

    let parsed = SourceUri::parse(source_uri)?;
    let blocks = match parsed {
        SourceUri::Local(path) => {
            let reader = LocalFileByteReader::new(&path);
            list_copy_blocks(&reader).await?
        }
        SourceUri::S3 { bucket, key } => {
            let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
            let s3 = std::sync::Arc::new(aws_sdk_s3::Client::new(&aws_config));
            let reader = S3ByteReader::new(s3, bucket, key);
            list_copy_blocks(&reader).await?
        }
    };

    Ok(blocks
        .into_iter()
        .map(|b| PgDumpTable {
            schema: b.schema,
            table: b.table,
            columns: b.columns,
        })
        .collect())
}

// Build custom delimited config if provided
fn maybe_delimited_config(args: &LoadArgs) -> Option<DelimitedConfig> {
    if args.format.is_delimited() {
        let mut config = if args.format == Format::Csv {
            DelimitedConfig::csv()
        } else {
            DelimitedConfig::tsv()
        };

        if let Some(delimiter) = args.delimiter.clone() {
            config.delimiter = delimiter;
        }
        if let Some(quote) = args.quote.clone() {
            config.quote = quote;
        }
        if let Some(escape) = args.escape.clone() {
            config.escape = Some(escape);
        }
        if let Some(has_header) = args.has_header {
            config.has_header = has_header;
        }

        Some(config)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_parse_accepts_pgdump() {
        assert_eq!(Format::parse("pgdump").unwrap(), Format::PgDump);
        assert_eq!(Format::parse("PGDUMP").unwrap(), Format::PgDump);
    }

    #[test]
    fn format_pgdump_is_not_delimited() {
        assert!(!Format::PgDump.is_delimited());
    }

    #[test]
    fn validate_identifier_rejects_embedded_quote() {
        let err = validate_identifier("table", "x\";DROP TABLE y;--").unwrap_err();
        assert!(err.to_string().contains("unsafe character"));
    }

    #[test]
    fn validate_identifier_rejects_backslash() {
        let err = validate_identifier("table", "a\\b").unwrap_err();
        assert!(err.to_string().contains("unsafe character"));
    }

    #[test]
    fn validate_identifier_rejects_control_byte() {
        let err = validate_identifier("schema", "a\x1bb").unwrap_err();
        assert!(err.to_string().contains("unsafe character"));
    }

    #[test]
    fn validate_identifier_rejects_empty() {
        assert!(validate_identifier("table", "").is_err());
    }

    #[test]
    fn validate_identifier_accepts_normal_names() {
        validate_identifier("table", "users").unwrap();
        validate_identifier("schema", "public").unwrap();
        // Non-ASCII letters are allowed (PG quoted identifiers may contain
        // them); we only reject control bytes / quote / backslash / NUL /
        // bidi+format codepoints.
        validate_identifier("table", "naïve").unwrap();
    }

    #[test]
    fn validate_identifier_rejects_bidi_and_format_codepoints() {
        // RTL override (would visually reverse trailing characters in logs)
        assert!(validate_identifier("table", "ev\u{202E}lit").is_err());
        // Zero-width space (invisible character splitting an identifier)
        assert!(validate_identifier("table", "us\u{200B}ers").is_err());
        // BOM / ZWNBSP
        assert!(validate_identifier("schema", "\u{FEFF}public").is_err());
        // Bidi isolate
        assert!(validate_identifier("table", "x\u{2066}y").is_err());
    }

    #[test]
    fn pgdump_rejects_column_map_at_validation() {
        let mut args = sample_pgdump_args();
        args.column_mappings.insert("a".into(), "b".into());
        let err = validate_load_args(&args).unwrap_err().to_string();
        assert!(err.contains("column_mappings"), "{err}");
    }

    #[test]
    fn pgdump_rejects_exclude_columns_at_validation() {
        let mut args = sample_pgdump_args();
        args.exclude_columns = vec!["x".into()];
        let err = validate_load_args(&args).unwrap_err().to_string();
        assert!(err.contains("exclude_columns"), "{err}");
    }

    #[test]
    fn pgdump_rejects_if_not_exists_at_validation() {
        let mut args = sample_pgdump_args();
        args.create_table_if_missing = true;
        let err = validate_load_args(&args).unwrap_err().to_string();
        assert!(err.contains("create_table_if_missing"), "{err}");
    }

    #[tokio::test]
    async fn list_pgdump_tables_reports_each_block() -> Result<()> {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "-- preamble")?;
        writeln!(f, "COPY public.users (id, name) FROM stdin;")?;
        writeln!(f, "1\tAlice")?;
        writeln!(f, "\\.")?;
        writeln!(f, "COPY sales.orders (id, total) FROM stdin;")?;
        writeln!(f, "1\t99")?;
        writeln!(f, "\\.")?;
        f.flush()?;

        let tables = list_pgdump_tables(&f.path().to_string_lossy()).await?;

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].schema, "public");
        assert_eq!(tables[0].table, "users");
        assert_eq!(tables[0].columns, vec!["id", "name"]);
        assert_eq!(tables[1].schema, "sales");
        assert_eq!(tables[1].table, "orders");
        assert_eq!(tables[1].columns, vec!["id", "total"]);
        Ok(())
    }

    #[tokio::test]
    async fn list_pgdump_tables_empty_for_no_copy_blocks() -> Result<()> {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new()?;
        writeln!(f, "-- nothing here")?;
        f.flush()?;
        let tables = list_pgdump_tables(&f.path().to_string_lossy()).await?;
        assert!(tables.is_empty());
        Ok(())
    }

    fn sample_pgdump_args() -> LoadArgs {
        LoadArgs {
            endpoint: "x.dsql.us-east-1.on.aws".into(),
            region: "us-east-1".into(),
            username: "admin".into(),
            source_uri: "/tmp/x.sql".into(),
            target_table: "t".into(),
            schema: "public".into(),
            format: Format::PgDump,
            worker_count: 1,
            chunk_size_bytes: 1024,
            batch_size: 1,
            batch_concurrency: 1,
            create_table_if_missing: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: Default::default(),
            resume_job_id: None,
            on_conflict: OnConflict::DoNothing,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            test_pool: None,
        }
    }
}
