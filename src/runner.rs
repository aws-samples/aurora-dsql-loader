//! High-level runner API for the DSQL data loader.
//!
//! This module provides a simplified public interface that encapsulates all the
//! internal complexity of setting up connections, readers, coordinators, etc.
//!
//! This is the primary API for external users and for the CLI.

// Re-export OnConflict for external use
pub use crate::coordination::manifest::OnConflict;

// Re-export the migrate API so external consumers (and the CLI binary)
// reach it through the same `runner::` namespace as `run_load`. The
// types embedded in MigrateReport ride along.
pub use crate::migrate::{
    AppliedStatement, ApplyOutcome, Diagnostic as DdlDiagnostic, MigrateArgs, MigrateReport,
    TableLoadSummary, run_migrate,
};

// Re-export the export API so the CLI binary and library consumers reach it
// through the same `runner::` namespace as `run_load` / `run_migrate`.
pub use crate::export::{ExportArgs, ExportReport, run_export};

// Re-export the verification surface — `VerifyMode` is set by the CLI on
// `LoadArgs`, and `VerifyOutcome` / `VerifyVerdict` ship as fields of
// `LoadResult` and `TableLoadSummary`.
pub use crate::verify::{
    L2Counts, L3Details, SchemaCheck, VerifyMode, VerifyOutcome, VerifyVerdict,
};

use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use crate::coordination::manifest::{LocalManifestStorage, ParquetConfig, PgDumpConfig};
use crate::coordination::{Coordinator, DsqlConfig, FileFormat, LoadConfigBuilder};
use crate::db::pool::PoolArgsBuilder;
use crate::db::schema::query_table_schema;
use crate::db::{self as db_pool, SchemaInferrer};
use crate::formats::pgdump::{CopyBlock, PgDumpReader, list_copy_blocks};
use crate::formats::{DelimitedConfig, ReaderFactory};
use crate::io::{LocalFileByteReader, S3ByteReader, SourceUri};
use crate::verify::{self, VerifyInputs};

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
    /// Opt-in all-or-nothing. DSQL can't run a multi-worker bulk load in one
    /// transaction (3,000-row/txn cap, no SAVEPOINT), so the only clean
    /// rollback is to DROP a table the loader created this run. Requires
    /// `create_table_if_missing`; refuses if the table already exists.
    pub atomic: bool,
    pub manifest_dir: Option<PathBuf>,
    pub quiet: bool,
    pub debug: bool,

    // Column mapping: source column name -> destination column name
    pub column_mappings: HashMap<String, String>,

    // Resume options
    pub resume_job_id: Option<String>,

    // Conflict resolution strategy
    pub on_conflict: OnConflict,

    /// Verification toggle. Source-row counting runs parser-side
    /// regardless; `Count` adds pre/post `count(*)` + the affirmative schema
    /// check; `Full` additionally runs the per-row L3 value check. CLI
    /// default: `load`=Off, `migrate`=Count.
    pub verify: VerifyMode,

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
    /// Exact source-row count summed across chunks. `Some` for pgdump
    /// and parquet; `None` for csv/tsv.
    pub source_rows: Option<u64>,
    /// `Some` under `--verify=count` on the `run_load` path. Migrate
    /// builds its own outcome on `TableLoadSummary.verify` and leaves
    /// this `None`.
    pub verify: Option<VerifyOutcome>,
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
/// use aurora_dsql_loader::runner::{LoadArgs, Format, OnConflict, VerifyMode, run_load};
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
///     atomic: false,
///     manifest_dir: None,
///     column_mappings: HashMap::new(),
///     quiet: true,
///     debug: false,
///     resume_job_id: None,
///     on_conflict: OnConflict::DoNothing,
///     verify: VerifyMode::Off,
///     delimiter: None,
///     quote: None,
///     escape: None,
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
    // Fail fast on bad args before paying for IAM + pool open. The
    // downstream entry point re-validates (cheap) so library callers
    // who skip `run_load` still get the same checks.
    validate_load_args(&args)?;
    let pool = build_pool(&args).await?;

    // Only a table the loader creates this run is safe to DROP on failure;
    // gate out the cases where rollback would destroy pre-existing data.
    if args.atomic {
        if !args.create_table_if_missing {
            anyhow::bail!(
                "--atomic requires --if-not-exists: rollback drops the table the loader \
                 creates this run, so the table must not already exist (DSQL has no \
                 SAVEPOINT and caps transactions at 3,000 rows, so a bulk load can't be \
                 one atomic transaction)."
            );
        }
        if query_table_schema(&pool, &args.schema, &args.target_table)
            .await
            .is_ok()
        {
            anyhow::bail!(
                "--atomic refuses to load into existing table {}.{}: rollback would DROP it, \
                 destroying pre-existing data. Drop it yourself first, or run without --atomic.",
                args.schema,
                args.target_table
            );
        }
    }

    let atomic = args.atomic;
    let atomic_schema = args.schema.clone();
    let atomic_table = args.target_table.clone();

    // L2 needs a stable pre/post pair AND an exact source count to be
    // meaningful. Skip on `--if-not-exists` (table may not exist yet, and a
    // re-run would mis-classify as ExtraTarget) and on csv/tsv (classify()
    // short-circuits to SkippedNoExactSourceCount, wasting two count(*)).
    // L2 runs under Count and Full alike (Full implies Count). Off skips it.
    let verify_on = matches!(args.verify, VerifyMode::Count | VerifyMode::Full);
    let l2_runs = verify_on
        && !args.create_table_if_missing
        && matches!(args.format, Format::PgDump | Format::Parquet);
    let target_pre = if l2_runs {
        Some(
            verify::count_table_rows(&pool, &args.schema, &args.target_table)
                .await
                .context("verify: failed to read target pre-count")?,
        )
    } else {
        None
    };

    if verify_on && !l2_runs {
        // Operator opted into verification but the gating conditions skipped
        // L2; surface that instead of silently returning Match.
        tracing::info!(
            schema = %args.schema,
            table = %args.target_table,
            "verify: L2 skipped (--if-not-exists or csv/tsv); L1 only"
        );
    }

    let schema = args.schema.clone();
    let target_table = args.target_table.clone();
    let on_conflict = args.on_conflict;
    let verify_mode = args.verify;
    // Captured for the L3 re-read (args is moved into run_load_with_pool).
    let source_uri = args.source_uri.clone();
    let region = args.region.clone();
    let format = args.format;
    let chunk_size_bytes = args.chunk_size_bytes;

    let mut result = match run_load_with_pool(pool.clone(), args).await {
        Ok(r) if atomic && r.records_failed > 0 => {
            drop_atomic_table(&pool, &atomic_schema, &atomic_table).await;
            anyhow::bail!(
                "--atomic: rolled back — {} record(s) failed; dropped table {}.{}.",
                r.records_failed,
                atomic_schema,
                atomic_table
            );
        }
        Ok(r) => r,
        Err(e) if atomic => {
            drop_atomic_table(&pool, &atomic_schema, &atomic_table).await;
            return Err(e.context(format!(
                "--atomic: rolled back — load failed; dropped table {atomic_schema}.{atomic_table}"
            )));
        }
        Err(e) => return Err(e),
    };

    if verify_on {
        let target_post = if l2_runs {
            Some(
                verify::count_table_rows(&pool, &schema, &target_table)
                    .await
                    .context("verify: failed to read target post-count")?,
            )
        } else {
            None
        };
        let target_counts = target_pre
            .zip(target_post)
            .map(|(pre, post)| L2Counts { pre, post });

        // Affirmative schema check runs under both Count and Full (the
        // MUST #3 floor, matching migrate); the per-row L3 value check runs
        // under Full only. The load path resolves the COPY block itself
        // (unlike migrate, which pre-resolved it). `l3`/`l3_details` ride
        // only under Full (and details only when L3 actually Ran).
        let run_value_check = verify_mode == VerifyMode::Full;
        let (l3, schema_check, l3_details) = {
            let (sc, outcome, details) = verify::run_load_value_check(
                &pool,
                verify::LoadVerifyTarget {
                    source_uri: &source_uri,
                    region: &region,
                    schema: &schema,
                    table: &target_table,
                    is_pgdump: format == Format::PgDump,
                    run_value_check,
                    chunk_size_bytes,
                },
            )
            .await?;
            if run_value_check {
                let (l3, details) = outcome.with_details(details);
                (l3, Some(sc), details)
            } else {
                (None, Some(sc), None)
            }
        };

        result.verify = Some(VerifyOutcome::from_inputs(
            schema,
            target_table,
            VerifyInputs {
                mode: verify_mode,
                on_conflict,
                source_rows: result.source_rows,
                records_loaded: result.records_loaded,
                records_failed: result.records_failed,
                target_counts,
                l3,
            },
            schema_check,
            l3_details,
        ));
    }

    Ok(result)
}

/// Drop the table created by an `--atomic` load that's about to fail. The
/// DROP error is logged, not propagated, so it can't mask the load error the
/// caller is already returning (the original cause is what the operator needs).
async fn drop_atomic_table(pool: &crate::db::Pool, schema: &str, table: &str) {
    let ddl = format!("DROP TABLE {}", pool.qualified_table_name(schema, table));
    if let Err(e) = pool.execute_query(&ddl).await {
        tracing::error!(
            "--atomic: rollback DROP of {schema}.{table} failed: {e:#}. \
             Drop it manually to restore the prior state."
        );
    }
}

/// Build the connection pool. Honors `args.test_pool` in `#[cfg(test)]`.
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

/// Run a load against an externally-supplied pool. Identical to
/// [`run_load`] except the caller owns pool construction. The migrate
/// orchestrator uses [`run_load_with_pool_for_pgdump_block`] instead —
/// it pre-resolves the COPY block and reuses one `aws_config` across
/// every table.
pub(crate) async fn run_load_with_pool(
    pool: crate::db::Pool,
    args: LoadArgs,
) -> Result<LoadResult> {
    validate_load_args(&args)?;

    let delimited_config = maybe_delimited_config(&args);
    let parsed_uri = SourceUri::parse(&args.source_uri)?;

    // Build the file reader. The pg_dump path here scans the dump to find
    // the matching COPY block; migrate has already done that and uses
    // `run_load_with_pool_for_pgdump_block` below to skip the rescan.
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(args.region.clone()))
        .load()
        .await;
    let reader_factory = ReaderFactory::new(&aws_config);
    let (file_reader, pgdump_columns) = match args.format {
        Format::PgDump => {
            let (reader, columns) = reader_factory
                .create_pgdump_reader(&parsed_uri, &args.schema, &args.target_table)
                .await?;
            // Symmetric to migrate's per-block validation — guards INSERT SQL.
            for col in &columns {
                validate_pgdump_identifier("column", col)?;
            }
            (reader, columns)
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

    run_load_with_pool_and_reader(
        pool,
        args,
        file_reader,
        pgdump_columns,
        delimited_config,
        None,
    )
    .await
}

/// Migrate-only entry point: load one pg_dump COPY block using a
/// pre-resolved [`CopyBlock`] (no dump rescan) and a caller-supplied
/// `aws_config` (no second credential-chain walk). The orchestrator
/// scans the dump exactly once at startup and threads the result
/// through here per table. `aws_config` is required for `s3://`
/// sources and ignored for `file://`.
///
/// Caller invariant: `block.schema` and `block.table` must match the
/// equivalent fields on `args` — this entry point trusts the block
/// instead of running `find_copy_block`.
pub(crate) async fn run_load_with_pool_for_pgdump_block(
    pool: crate::db::Pool,
    args: LoadArgs,
    block: CopyBlock,
    aws_config: Option<&aws_config::SdkConfig>,
    parent_multi: Option<indicatif::MultiProgress>,
) -> Result<LoadResult> {
    validate_load_args(&args)?;
    debug_assert_eq!(args.target_table, block.table);
    debug_assert_eq!(args.schema, block.schema);

    let delimited_config = maybe_delimited_config(&args);
    let parsed_uri = SourceUri::parse(&args.source_uri)?;

    let pgdump_columns = block.columns.clone();
    let file_reader: Arc<dyn crate::formats::reader::FileReader> = match &parsed_uri {
        SourceUri::Local(path) => {
            let byte_reader = LocalFileByteReader::new(path);
            Arc::new(PgDumpReader::from_block(byte_reader, block))
        }
        SourceUri::S3 { bucket, key } => {
            let cfg = aws_config.context(
                "internal: run_load_with_pool_for_pgdump_block needs aws_config for s3:// sources",
            )?;
            let s3 = Arc::new(aws_sdk_s3::Client::new(cfg));
            let byte_reader = S3ByteReader::new(s3, bucket.clone(), key.clone());
            Arc::new(PgDumpReader::from_block(byte_reader, block))
        }
    };

    run_load_with_pool_and_reader(
        pool,
        args,
        file_reader,
        pgdump_columns,
        delimited_config,
        parent_multi,
    )
    .await
}

async fn run_load_with_pool_and_reader(
    pool: crate::db::Pool,
    args: LoadArgs,
    file_reader: Arc<dyn crate::formats::reader::FileReader>,
    pgdump_columns: Vec<String>,
    delimited_config: Option<DelimitedConfig>,
    parent_multi: Option<indicatif::MultiProgress>,
) -> Result<LoadResult> {
    // Set up manifest directory (use temp dir if not provided)
    let (mut temp_dir, manifest_dir_path) = if let Some(dir) = args.manifest_dir {
        (None, dir)
    } else {
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_path_buf();
        (Some(temp_dir), path)
    };

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
        Format::PgDump => (
            false,
            FileFormat::PgDump(PgDumpConfig {
                copy_columns: pgdump_columns,
            }),
        ),
    };

    let manifest_storage = Arc::new(LocalManifestStorage::new(manifest_dir_path));
    let schema_inferrer = SchemaInferrer { has_header };
    let coordinator = Coordinator::new(manifest_storage, file_reader, schema_inferrer, pool);

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
        .parent_multi(parent_multi)
        .build()?;

    let result = coordinator.run_load(&load_config).await?;

    let persisted_manifest_dir = if result.records_failed > 0 && temp_dir.is_some() {
        let temp = temp_dir.take().unwrap();
        Some(temp.keep())
    } else {
        None
    };

    // L1 inputs only — callers (run_load / migrate orchestrator) own
    // L2 and the final VerifyOutcome.
    Ok(LoadResult {
        job_id: result.job_id,
        chunks_processed: result.chunks_processed,
        records_loaded: result.records_loaded,
        records_failed: result.records_failed,
        estimated_rows: result.estimated_rows,
        source_rows: result.source_rows,
        verify: None,
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

    // L2 (run under both Count and Full) needs target_pre sampled at the
    // start of the load; on resume the manifest already holds rows from the
    // original run, so post-pre would be < records_loaded and classify
    // produces a false MissingTarget/RowsConflictedAtTarget. Full also
    // re-reads only the resumed portion for L3, compounding the problem.
    if args.resume_job_id.is_some() && matches!(args.verify, VerifyMode::Count | VerifyMode::Full) {
        anyhow::bail!(
            "--verify (count/full) cannot be combined with --resume-job-id: pre-count \
             would not include rows from the original run. Re-run with --verify=off."
        );
    }
    Ok(())
}

/// Reject CLI identifiers that would break SQL identifier quoting or
/// deceive an operator reading logs. `Pool::qualified_table_name`
/// escape-doubles `"` defensively, but control bytes and bidi/format
/// codepoints are blocked here so they never reach SQL or terminals.
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

/// Same allowlist as `validate_identifier` but applied to identifiers
/// parsed out of the dump (block.schema/table/columns). Distinct error
/// wording so the operator knows the dump is at fault, not their CLI.
pub(crate) fn validate_pgdump_identifier(field: &'static str, value: &str) -> Result<()> {
    if value.is_empty() {
        anyhow::bail!("pg_dump {field} identifier must not be empty");
    }
    if value.chars().any(is_unsafe_identifier_char) {
        anyhow::bail!(
            "pg_dump {field} identifier {value:?} contains an unsafe character \
             (control byte, backslash, double-quote, or Unicode bidi/format \
             codepoint) that would corrupt SQL identifier quoting or visually \
             deceive an operator reading logs. Edit the dump to rename the \
             offending identifier."
        );
    }
    Ok(())
}

/// Unicode bidi/zero-width/format codepoints that visually reorder or
/// hide surrounding text in a terminal. Shared between identifier
/// validation and the `list-tables` TSV output guard.
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
    use std::io::Write;

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

    /// Resume + verify (count OR full) would mis-classify: pre-count is
    /// sampled at resume start but records_loaded sums the entire manifest,
    /// so post − pre < records_loaded → false MissingTarget. Both L2-running
    /// modes must be rejected.
    #[test]
    fn resume_with_verify_count_or_full_is_rejected_at_validation() {
        for mode in [VerifyMode::Count, VerifyMode::Full] {
            let mut args = sample_pgdump_args();
            args.resume_job_id = Some("abc-123".into());
            args.verify = mode;
            let err = validate_load_args(&args).unwrap_err().to_string();
            assert!(err.contains("--resume-job-id"), "mode {mode:?}: {err}");
        }
    }

    /// Resume + verify=off is allowed (no L2 to mis-classify).
    #[test]
    fn resume_with_verify_off_is_allowed() {
        let mut args = sample_pgdump_args();
        args.resume_job_id = Some("abc-123".into());
        args.verify = VerifyMode::Off;
        validate_load_args(&args).unwrap();
    }

    #[tokio::test]
    async fn list_pgdump_tables_reports_each_block() -> Result<()> {
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
            atomic: false,
            manifest_dir: None,
            quiet: true,
            debug: false,
            column_mappings: Default::default(),
            resume_job_id: None,
            on_conflict: OnConflict::DoNothing,
            verify: VerifyMode::Off,
            exclude_columns: Vec::new(),
            delimiter: None,
            quote: None,
            escape: None,
            has_header: None,
            test_pool: None,
        }
    }

    /// The `debug_assert_eq!` routing-identity guard must fire when
    /// `args.target_table` and `block.table` disagree — the trap that
    /// would otherwise allow silent cross-table writes.
    #[tokio::test]
    #[should_panic(expected = "assertion `left == right` failed")]
    async fn pgdump_block_routing_mismatch_panics_in_debug() {
        let pool = crate::db::Pool::sqlite_in_memory().await.unwrap();
        let mut args = sample_pgdump_args();
        args.target_table = "t".into();
        args.test_pool = Some(pool.clone());
        let block = CopyBlock {
            schema: "public".into(),
            table: "OTHER".into(),
            header_start: 0,
            data_start: 1,
            data_end: 2,
            block_end: 4,
            columns: vec!["id".into()],
        };
        let _ = run_load_with_pool_for_pgdump_block(pool, args, block, None, None).await;
    }
}
