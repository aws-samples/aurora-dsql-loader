use aurora_dsql_loader::runner::{
    ApplyOutcome, ExportArgs, Format, LoadArgs, MigrateArgs, OnConflict, VerifyMode, VerifyOutcome,
    VerifyVerdict, run_export, run_load, run_migrate,
};
use clap::{ArgGroup, Args as ClapArgs, Parser, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser, Clone)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, ClapArgs)]
struct ConnectionArgs {
    /// DSQL cluster endpoint (region is auto-detected from endpoint format)
    #[arg(short, long)]
    endpoint: String,

    /// AWS region (optional, inferred from endpoint if not specified)
    #[arg(short, long)]
    region: Option<String>,

    /// Database username
    #[arg(short, long, default_value = "admin")]
    username: String,
}

#[derive(Clone, ClapArgs)]
#[command(group = ArgGroup::new("delimited").multiple(true))]
struct SourceArgs {
    /// Path to source data file or S3 URI (local path, s3://bucket/key)
    #[arg(short, long)]
    source_uri: String,

    /// File format (csv, tsv, parquet, pgdump). Auto-detected from extension
    /// for csv/tsv/parquet; pgdump must be specified explicitly. The `load`
    /// subcommand's pgdump format requires plain (`-Fp`) `pg_dump
    /// --data-only` output and is not compatible with --column-map,
    /// --exclude-columns, or --if-not-exists. (The `migrate` subcommand
    /// reads a full `pg_dump -Fp` instead — see `migrate --help`.)
    #[arg(short, long)]
    format: Option<String>,

    // Delimited file options (CSV/TSV only)
    /// Field delimiter (Default "," for CSV, and "\t" for TSV)
    #[arg(long, group = "delimited")]
    delimiter: Option<String>,

    /// Quote character (default: "\"")
    #[arg(long, group = "delimited")]
    quote: Option<String>,

    /// Escape character to use for escaping content
    #[arg(long, group = "delimited")]
    escape: Option<String>,

    /// CSV/TSV file has a header row that should be skipped
    #[arg(long, group = "delimited", conflicts_with = "no_header")]
    header: bool,

    /// Deprecated. Explicitly sets header behavior to false, which matches the
    /// new default. Prefer omitting the flag; will be removed in a future release.
    #[arg(long, group = "delimited")]
    no_header: bool,
}

#[derive(Clone, ClapArgs)]
struct TargetArgs {
    /// Target table name
    #[arg(short, long)]
    table: String,

    /// Database schema (default: "public")
    #[arg(long, default_value = "public")]
    schema: String,
}

#[derive(Clone, ClapArgs)]
struct LoadParams {
    /// Number of worker threads
    #[arg(short, long, default_value = "8")]
    workers: usize,

    /// Chunk size (e.g., 10MB, 1GB)
    #[arg(short, long, default_value = "10MB")]
    chunk_size: String,

    /// Batch size for inserts
    #[arg(short, long, default_value = "2000")]
    batch_size: usize,

    /// Number of concurrent batches per worker
    #[arg(long, default_value = "32")]
    batch_concurrency: usize,

    /// Create table if it doesn't exist
    #[arg(long)]
    if_not_exists: bool,

    /// All-or-nothing: on any load failure, DROP the table the loader created
    /// and exit non-zero. Requires --if-not-exists and refuses to run if the
    /// table already exists or its absence can't be verified (it will not drop
    /// pre-existing data); cannot be combined with --resume-job-id. DSQL can't
    /// do a single-transaction bulk load (3,000-row/txn cap, no SAVEPOINT), so
    /// this is the only clean rollback.
    #[arg(long, requires = "if_not_exists", conflicts_with = "resume_job_id")]
    atomic: bool,

    /// Column mapping from source to destination (format: src:dest,src2:dest2)
    #[arg(long)]
    column_map: Option<String>,

    /// Conflict resolution strategy (do-nothing, do-update, error)
    #[arg(long, default_value = "do-nothing")]
    on_conflict: String,

    /// Post-load verification (`off` | `count` | `full`). `count` adds
    /// pre/post `count(*)` and reports an L1+L2 verdict per table. `full`
    /// adds per-row value verification (L3) — opt-in, ~one extra full table
    /// read. Assumes the loader is the sole writer during the run. L2 is
    /// skipped under `--if-not-exists` (no stable pre-count) and on csv/tsv
    /// (no exact source count); the per-table verdict still surfaces.
    #[arg(long, default_value = "off")]
    verify: String,

    /// Columns to exclude from INSERT statements so the DB applies DEFAULT values
    /// (format: col1,col2). Source records must still contain these columns - the
    /// loader skips them by position. Requires the table to already exist; cannot
    /// be combined with --if-not-exists.
    #[arg(long, conflicts_with = "if_not_exists")]
    exclude_columns: Option<String>,
}

#[derive(Clone, ClapArgs)]
struct OutputArgs {
    /// Quiet mode - minimal output, only show summary
    #[arg(short, long)]
    quiet: bool,

    /// Debug mode - show verbose error traces and diagnostic information
    #[arg(long)]
    debug: bool,

    /// Validate schema and show plan without loading data
    #[arg(long)]
    dry_run: bool,

    /// Keep manifest directory after load (for debugging)
    #[arg(long)]
    keep_manifest: bool,

    /// Directory for manifest files (default: system temp directory)
    #[arg(long)]
    manifest_dir: Option<String>,

    /// Resume an incomplete job by job ID (requires --manifest-dir)
    #[arg(long, requires = "manifest_dir")]
    resume_job_id: Option<String>,
}

#[derive(Clone, Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    Load {
        #[command(flatten)]
        connection: ConnectionArgs,

        #[command(flatten)]
        source: SourceArgs,

        #[command(flatten)]
        target: TargetArgs,

        #[command(flatten)]
        load: LoadParams,

        #[command(flatten)]
        output: OutputArgs,
    },
    /// List every `COPY ... FROM stdin;` block in a pg_dump file.
    ///
    /// Use this to discover tables in a dump before running `load --table=...`
    /// per table. Output is one line per table, suitable for shell scripting.
    ListTables {
        /// Path to a pg_dump file (local path or s3:// URI).
        #[arg(short, long)]
        source_uri: String,
    },
    /// Migrate a `pg_dump` file (full dump, including DDL) into Aurora DSQL.
    ///
    /// Steps the loader takes for you:
    /// 1. Reads the embedded DDL out of the dump.
    /// 2. Transforms it via `dsql-lint` to be DSQL-compatible (notably,
    ///    collapses pg_dump's 4-statement `SERIAL` expansion into a single
    ///    inline `BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1)` column).
    ///    Foreign keys, partitioning, and other unsupported constructs are
    ///    dropped or rejected.
    /// 3. Applies the transformed DDL to the cluster, one statement per
    ///    transaction (DSQL accepts only one DDL per txn).
    /// 4. Loads each `COPY` block's data using the same connection pool.
    ///
    /// Use `--dry-run` to see the diagnostics + proposed DDL without
    /// touching the cluster. The dump must be plain-format (`pg_dump
    /// -Fp`); `-Fc`/`-Fd` archives are rejected up front.
    Migrate {
        #[command(flatten)]
        connection: ConnectionArgs,

        /// Path to a pg_dump file (local path or s3:// URI). Plain-format
        /// only (`pg_dump -Fp`). Schema-only dumps (`-s`) work and apply
        /// just the DDL; data-only dumps (`--data-only`) have no DDL to
        /// transform.
        #[arg(short, long)]
        source_uri: String,

        /// Schema the dump targets in DSQL. Informational only —
        /// per-table routing reads each `COPY` block's schema-qualified
        /// name in the dump itself; this flag does not override it.
        #[arg(long, default_value = "public")]
        schema: String,

        /// Print the diagnostics + proposed DDL and exit. No statements
        /// are sent to the cluster, no data is loaded. Use this to
        /// review what `dsql-lint` would change before committing.
        #[arg(long)]
        dry_run: bool,

        /// Number of worker threads for each table load.
        #[arg(short, long, default_value = "8")]
        workers: usize,

        /// Chunk size for the per-table load (e.g., `10MB`, `1GB`).
        #[arg(short, long, default_value = "10MB")]
        chunk_size: String,

        /// Batch size for inserts.
        #[arg(short, long, default_value = "2000")]
        batch_size: usize,

        /// Number of concurrent batches per worker.
        #[arg(long, default_value = "32")]
        batch_concurrency: usize,

        /// Conflict resolution strategy (do-nothing, do-update, error).
        /// Migration runs typically want `error` to surface duplicates.
        #[arg(long, default_value = "error")]
        on_conflict: String,

        /// Post-load verification (`off` | `count` | `full`). Default
        /// `count`: pre/post `count(*)` per loaded table → L1+L2 verdict
        /// plus the affirmative schema check. `full` adds per-row value
        /// verification (L3) — ~one extra full table read. Fresh-cluster
        /// migrate makes the sole-writer assumption hold trivially.
        #[arg(long, default_value = "count")]
        verify: String,

        /// Quiet mode - minimal output, only show summary.
        #[arg(short, long)]
        quiet: bool,

        /// Debug mode - show verbose error traces.
        #[arg(long)]
        debug: bool,
    },
    /// Export a source DSQL cluster to a plain `pg_dump`-shaped `.sql`.
    ///
    /// Connects to the source cluster directly (no `pg_dump`, no proxy),
    /// reads its catalog, and emits DSQL-clean DDL + `COPY ... FROM stdin`
    /// data blocks. The output is what `migrate --source-uri` consumes, so
    /// a DSQL → DSQL move is:
    ///
    ///   aurora-dsql-loader export --endpoint SRC --output dump.sql
    ///   aurora-dsql-loader migrate --endpoint DST --source-uri dump.sql
    ///
    /// Identity columns are emitted as `GENERATED BY DEFAULT ... (CACHE n)`
    /// and the identity's sequence position carries over on reload.
    Export {
        #[command(flatten)]
        connection: ConnectionArgs,

        /// Restrict the export to a single schema (default: all user schemas).
        #[arg(long)]
        schema: Option<String>,

        /// Restrict the export to a single table (default: all tables).
        #[arg(long)]
        table: Option<String>,

        /// Destination file. Omit to write the dump to stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Load {
            connection,
            source,
            target,
            load,
            output,
        } => {
            run_loader(connection, source, target, load, output).await?;
        }
        Command::ListTables { source_uri } => {
            let tables = aurora_dsql_loader::runner::list_pgdump_tables(&source_uri).await?;
            // Validate ALL identifiers before printing ANY line: a malicious
            // dump that places a title-injection payload in the Nth block
            // must not be able to leak the first N-1 lines onto the operator's
            // TTY before we bail. Tab-separated, one block per line, for
            // downstream `awk`/`cut` consumption.
            for t in &tables {
                if let Some(name) = check_listable(t) {
                    anyhow::bail!(
                        "list-tables: identifier {name:?} contains a tab, newline, \
                         control byte, or Unicode bidi/format character that would \
                         corrupt the TSV output or visually deceive the operator."
                    );
                }
            }
            for t in &tables {
                println!("{}\t{}\t{}", t.schema, t.table, t.columns.join(","));
            }
        }
        Command::Migrate {
            connection,
            source_uri,
            schema,
            dry_run,
            workers,
            chunk_size,
            batch_size,
            batch_concurrency,
            on_conflict,
            verify,
            quiet,
            debug,
        } => {
            run_migrate_cli(MigrateCliArgs {
                connection,
                source_uri,
                schema,
                dry_run,
                workers,
                chunk_size,
                batch_size,
                batch_concurrency,
                on_conflict,
                verify,
                quiet,
                debug,
            })
            .await?;
        }
        Command::Export {
            connection,
            schema,
            table,
            output,
        } => {
            run_export_cli(connection, schema, table, output).await?;
        }
    }
    Ok(())
}

/// Wire up the `export` subcommand: resolve the region, run the export, and
/// print a one-line summary. The summary goes to stderr so a stdout dump
/// (no `--output`) stays uncorrupted for piping into `migrate`.
async fn run_export_cli(
    connection: ConnectionArgs,
    schema: Option<String>,
    table: Option<String>,
    output: Option<PathBuf>,
) -> anyhow::Result<()> {
    init_tracing(true);
    let region = resolve_region(connection.region.as_deref(), &connection.endpoint)?;

    let report = run_export(ExportArgs {
        endpoint: connection.endpoint,
        region,
        username: connection.username,
        schema,
        table,
        output: output.clone(),
    })
    .await?;

    let dest = match &output {
        Some(path) => path.display().to_string(),
        None => "stdout".to_string(),
    };
    eprintln!(
        "Exported {} table(s) ({} bytes) to {dest}.",
        report.tables.len(),
        report.bytes_written
    );
    Ok(())
}

/// Internal carrier for the migrate flag set; flattens the clap-derived
/// fields out of the `Command::Migrate` variant so [`run_migrate_cli`]
/// has a single argument and the variant can stay readable. Lives in
/// `main.rs` rather than `runner.rs` because it's a CLI-shaped struct
/// (string knobs, not parsed types) — `MigrateArgs` is the parsed
/// equivalent that callers of the library use directly.
struct MigrateCliArgs {
    connection: ConnectionArgs,
    source_uri: String,
    schema: String,
    dry_run: bool,
    workers: usize,
    chunk_size: String,
    batch_size: usize,
    batch_concurrency: usize,
    on_conflict: String,
    verify: String,
    quiet: bool,
    debug: bool,
}

/// Wire up the global tracing subscriber. `quiet` keeps loader logs at
/// `warn`; otherwise they are at `info`. `RUST_LOG` always overrides.
/// Called by both `run_loader` and `run_migrate_cli`.
fn init_tracing(quiet: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if quiet {
            EnvFilter::new("aurora_dsql_loader=warn,sqlx=off")
        } else {
            EnvFilter::new("aurora_dsql_loader=info,sqlx=off")
        }
    });
    // Write logs to stderr so they never corrupt a stdout payload — `export`
    // without `--output` streams the `.sql` dump to stdout, and an interleaved
    // `tracing::warn!` (e.g. the identity-sequence warnings) would mangle the
    // dump that pipes into `migrate`. stdout is reserved for the payload.
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

/// Pick the AWS region: explicit `--region` wins, otherwise extract from
/// the DSQL endpoint hostname. Errors with a message pointing the operator
/// at the expected endpoint shape.
fn resolve_region(explicit: Option<&str>, endpoint: &str) -> anyhow::Result<String> {
    if let Some(r) = explicit {
        return Ok(r.to_owned());
    }
    cli::extract_region_from_endpoint(endpoint).ok_or_else(|| {
        anyhow::anyhow!(
            "Could not extract region from endpoint '{}'.\n\
             Expected format: xxx.dsql.REGION.on.aws (e.g., xxx.dsql.us-east-1.on.aws)\n\
             Please specify --region explicitly.",
            endpoint
        )
    })
}

async fn run_migrate_cli(args: MigrateCliArgs) -> anyhow::Result<()> {
    init_tracing(args.quiet);

    let region = resolve_region(args.connection.region.as_deref(), &args.connection.endpoint)?;

    let chunk_size_bytes = cli::parse_size_string(&args.chunk_size)
        .map_err(|e| anyhow::anyhow!("Invalid chunk size '{}': {}", args.chunk_size, e))?;
    let on_conflict = args
        .on_conflict
        .parse::<OnConflict>()
        .map_err(|e| anyhow::anyhow!("Invalid --on-conflict value: {}", e))?;
    let verify = args
        .verify
        .parse::<VerifyMode>()
        .map_err(|e| anyhow::anyhow!("Invalid --verify value: {}", e))?;

    let migrate_args = MigrateArgs {
        endpoint: args.connection.endpoint,
        region,
        username: args.connection.username,
        source_uri: args.source_uri,
        schema: args.schema,
        dry_run: args.dry_run,
        worker_count: args.workers,
        batch_size: args.batch_size,
        batch_concurrency: args.batch_concurrency,
        chunk_size_bytes,
        on_conflict,
        verify,
        quiet: args.quiet,
        debug: args.debug,
    };

    let report = run_migrate(migrate_args).await?;

    // Print summary. `--dry-run` and the unfixable-short-circuit branches
    // produce a different shape (no apply / no tables), so handle them
    // up front before the per-stage output.
    if !report.ddl_unfixable.is_empty() {
        println!(
            "Migration cannot proceed: {} unfixable diagnostic(s) found.",
            report.ddl_unfixable.len()
        );
        println!("Edit the dump to address each, then re-run.");
        println!();
        for d in &report.ddl_unfixable {
            println!(
                "  [{rule}] line {line}: {msg}",
                rule = d.rule,
                line = d.line,
                msg = d.message
            );
            println!("    suggestion: {}", d.suggestion);
        }
        anyhow::bail!("migrate aborted: unfixable diagnostics");
    }

    if report.dry_run {
        println!("DRY RUN — no changes were applied.");
    } else {
        let applied = report
            .ddl_applied
            .iter()
            .filter(|s| matches!(s.outcome, ApplyOutcome::Applied))
            .count();
        let skipped = report
            .ddl_applied
            .iter()
            .filter(|s| matches!(s.outcome, ApplyOutcome::SkippedAlreadyExists))
            .count();
        println!(
            "Applied {applied} DDL statement(s){suffix}.",
            suffix = if skipped > 0 {
                format!(" (skipped {skipped} already-existing object(s))")
            } else {
                String::new()
            }
        );
    }

    if !report.ddl_changes.is_empty() {
        println!();
        println!("DDL changes ({}):", report.ddl_changes.len());
        for d in &report.ddl_changes {
            println!(
                "  [{rule}] line {line}: {msg}",
                rule = d.rule,
                line = d.line,
                msg = d.message
            );
        }
    }

    if !report.warnings.is_empty() {
        println!();
        println!("Warnings:");
        for w in &report.warnings {
            println!("  {w}");
        }
    }

    if !report.tables.is_empty() {
        println!();
        println!("Loaded tables:");
        for t in &report.tables {
            println!(
                "  {schema}.{table}: {loaded} record(s){failed}",
                schema = t.schema,
                table = t.table,
                loaded = t.records_loaded,
                failed = if t.records_failed > 0 {
                    format!(", {} FAILED", t.records_failed)
                } else {
                    String::new()
                }
            );
            // Skip when a verify outcome follows — it reprints the same
            // count in its `counts:` line on non-Match verdicts.
            if let Some(n) = t.source_rows
                && t.verify.is_none()
            {
                println!("    Source rows: {n}");
            }
            if let Some(path) = &t.persisted_manifest_dir {
                println!("    Failed-row manifest: {}", path.display());
            }
            if let Some(v) = &t.verify {
                print_verify_detail(v, true);
            }
            if let Some(err) = &t.verify_error {
                println!("    Verify: ERROR — {err}");
            }
        }
        // The orchestrator halts on the first table whose load failed
        // OR whose post-count failed; in either case the report is
        // truncated and re-running blind risks silent FK-less drift.
        let last_halted = report
            .tables
            .last()
            .is_some_and(|t| t.records_failed > 0 || t.verify_error.is_some());
        if last_halted {
            let n = report.tables.len();
            let suffix = if n == 1 { "" } else { "s" };
            println!();
            println!(
                "Halted after {n} table{suffix} due to load or verify failures. \
                 Inspect the output above before re-running."
            );
        }

        // Discoverability nudge: only when count+schema actually ran (mode
        // Count) — point the operator at the deeper per-row check they
        // didn't opt into. Not under `off` (nothing verified) or `full`
        // (already the deepest).
        if verify == VerifyMode::Count {
            println!();
            println!(
                "Row counts and schema verified. For per-row value verification, \
                 re-run with --verify=full."
            );
        }
    }

    // Mirror `run_loader`: per-table failures, non-Match verdicts, and
    // post-count errors all exit non-zero so a wrapping
    // `migrate && deploy.sh` can't ship against a partially-loaded cluster.
    let any_failed_records = report.tables.iter().any(|t| t.records_failed > 0);
    let any_bad_verdict = report.tables.iter().any(|t| {
        t.verify
            .as_ref()
            .is_some_and(|v| is_bad_verdict(&v.verdict))
    });
    let any_verify_error = report.tables.iter().any(|t| t.verify_error.is_some());
    if any_failed_records || any_bad_verdict || any_verify_error {
        std::process::exit(1);
    }

    Ok(())
}

/// True for verdicts that should make the CLI exit non-zero. The `_`
/// arm is mandatory (cross-crate `#[non_exhaustive]`) and fails closed:
/// an unknown future variant exits non-zero, never green.
fn is_bad_verdict(v: &VerifyVerdict) -> bool {
    // Only a clean or explicitly-not-checked verdict is non-failing; every
    // other variant (incl. unknown future ones, via the `_`) fails closed.
    !matches!(
        v,
        VerifyVerdict::Match
            | VerifyVerdict::SkippedNoExactSourceCount
            | VerifyVerdict::ValueCheckSkipped
    )
}

/// Print `Verify: <verdict>`, the affirmative schema-conformance line (when
/// present), and — on a non-clean verdict — the raw counts plus the first
/// offending primary keys so the operator can act without re-running.
/// `nested` indents under a parent bullet (per-table `migrate` rows).
fn print_verify_detail(v: &VerifyOutcome, nested: bool) {
    let indent = if nested { "    " } else { "" };
    println!("{indent}Verify: {}", format_verdict(&v.verdict));

    // Affirmative schema line: shown regardless of verdict (it's the
    // default-on MUST #3 floor). Scoped to column-set + PK presence.
    if let Some(s) = &v.schema_check {
        let pk = if s.pk_present {
            "primary/unique key present"
        } else {
            "no primary/unique key"
        };
        let cols = match s.columns_matched {
            Some(n) => format!("{n} column(s) match"),
            None => "column set verified via load".to_string(),
        };
        println!("{indent}  Schema: {cols}, {pk}");
    }

    if matches!(
        v.verdict,
        VerifyVerdict::Match
            | VerifyVerdict::SkippedNoExactSourceCount
            | VerifyVerdict::ValueCheckSkipped
    ) {
        return;
    }

    let fmt = |n: Option<u64>| n.map_or("n/a".to_string(), |n| n.to_string());
    let (pre, post) = match v.target_counts {
        Some(c) => (fmt(Some(c.pre)), fmt(Some(c.post))),
        None => (fmt(None), fmt(None)),
    };
    println!(
        "{indent}  counts: source={src}, loaded={loaded}, failed={failed}, target_pre={pre}, target_post={post}",
        src = fmt(v.source_rows),
        loaded = v.records_loaded,
        failed = v.records_failed,
    );

    // First offending PKs (capped) so the operator can inspect them. PK
    // values are dump ROW data (never run through identifier validation), so
    // defang control/bidi bytes before they hit the terminal — a crafted dump
    // could otherwise smuggle an ANSI escape or RTL-override to stdout.
    if let Some(d) = &v.l3_details {
        if !d.mismatch_pks.is_empty() {
            println!(
                "{indent}  mismatched PKs: {}",
                defang_pk_list(&d.mismatch_pks)
            );
        }
        if !d.missing_pks.is_empty() {
            println!("{indent}  missing PKs: {}", defang_pk_list(&d.missing_pks));
        }
    }
}

/// Render offending PK values for the terminal with control and bidi/format
/// bytes escaped (reusing the `list-tables` defang set), so untrusted dump row
/// data can't reorder or hide output. Comma-joined for the report line.
fn defang_pk_list(pks: &[String]) -> String {
    pks.iter()
        .map(|pk| {
            pk.chars()
                .map(|c| {
                    if c.is_control() || aurora_dsql_loader::runner::is_bidi_or_format_char(c) {
                        c.escape_default().to_string()
                    } else {
                        c.to_string()
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// One-line CLI summary per VerifyVerdict.
fn format_verdict(v: &VerifyVerdict) -> String {
    match v {
        VerifyVerdict::Match => "OK (counts match)".to_string(),
        VerifyVerdict::LoaderDropped(n) => {
            format!(
                "LOADER DROPPED {n} row(s) — source count exceeds loaded+failed; investigate parser/chunker"
            )
        }
        VerifyVerdict::MissingTarget(n) => {
            format!(
                "MISSING TARGET {n} row(s) — target grew less than loader submitted (concurrent DELETE? verify=count assumes sole writer)"
            )
        }
        VerifyVerdict::ExtraTarget(n) => {
            format!(
                "EXTRA TARGET {n} row(s) — target grew more than loader submitted (concurrent INSERT? verify=count assumes sole writer)"
            )
        }
        VerifyVerdict::RowsConflictedAtTarget(n) => {
            format!(
                "CONFLICT: {n} row(s) conflict-resolved at target under do-nothing/do-update; CLI exits 1 — re-run with verify=off if this is the intended idempotent re-apply"
            )
        }
        VerifyVerdict::SkippedNoExactSourceCount => {
            "SKIPPED — no exact source-row count for this format (csv/tsv)".to_string()
        }
        VerifyVerdict::ValueMismatch(n) => {
            format!("VALUE MISMATCH {n} row(s) — target value differs from source by primary key")
        }
        VerifyVerdict::ValueRowMissingAtTarget(n) => {
            format!("MISSING AT TARGET {n} row(s) — source primary key not found in target")
        }
        VerifyVerdict::ValueCheckSkipped => {
            // Covers every reason L3 couldn't align rows: no single-column
            // primary/unique key, the key absent from the COPY set, or a
            // non-pgdump format. Stay general — a specific cause here can
            // contradict the schema line (which may show a key IS present).
            "SKIPPED value check — could not align source rows to the target by primary key"
                .to_string()
        }
        _ => format!("UNKNOWN VERDICT: {v:?}"),
    }
}

async fn run_loader(
    connection: ConnectionArgs,
    source: SourceArgs,
    target: TargetArgs,
    load: LoadParams,
    output: OutputArgs,
) -> anyhow::Result<()> {
    init_tracing(output.quiet);

    // Setup output based on quiet flag
    if !output.quiet {
        println!("DSQL Data Loader");
        println!("================");
        println!("Endpoint: {}", connection.endpoint);
        println!("Source: {}", source.source_uri);
        println!("Table: {}.{}", target.schema, target.table);
        println!("Workers: {}", load.workers);
        println!();
    }

    let region = resolve_region(connection.region.as_deref(), &connection.endpoint)?;

    // Auto-detect format from file extension if not provided
    let format = if let Some(f) = &source.format {
        f.as_str()
    } else {
        cli::detect_format_from_path(&source.source_uri).ok_or_else(|| {
            anyhow::anyhow!(
                "Could not detect format from file '{}'.\n\
                 Supported extensions: .csv, .tsv, .parquet\n\
                 Please specify --format explicitly.",
                source.source_uri
            )
        })?
    };

    // Parse column mapping if provided
    let column_mappings = if let Some(ref mapping_str) = load.column_map {
        cli::parse_column_mapping(mapping_str).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse column mapping: {}\n\
                 Example: --column-map \"old_col:new_col,timestamp:created_at\"",
                e
            )
        })?
    } else {
        HashMap::new()
    };

    let exclude_columns = if let Some(ref excl_str) = load.exclude_columns {
        cli::parse_exclude_columns(excl_str)?
    } else {
        Vec::new()
    };

    // Parse on_conflict mode
    let on_conflict = load
        .on_conflict
        .parse::<OnConflict>()
        .map_err(|e| anyhow::anyhow!("Invalid --on-conflict value: {}", e))?;

    let verify = load
        .verify
        .parse::<VerifyMode>()
        .map_err(|e| anyhow::anyhow!("Invalid --verify value: {}", e))?;

    // Parse chunk size
    let chunk_size_bytes = cli::parse_size_string(&load.chunk_size)
        .map_err(|e| anyhow::anyhow!("Invalid chunk size '{}': {}", load.chunk_size, e))?;

    // Parse format
    let format_enum = Format::parse(format)?;
    validate_delimited_options(format, format_enum, &source)?;

    // Handle dry-run mode
    if output.dry_run {
        println!("DRY RUN MODE - No data will be loaded");
        println!();
        println!("Configuration:");
        println!("  Table: {}", target.table);
        println!("  Workers: {}", load.workers);
        println!("  Chunk size: {} bytes", chunk_size_bytes);
        println!("  Batch size: {}", load.batch_size);
        println!("  Batch concurrency: {}", load.batch_concurrency);
        println!("  Create table if missing: {}", load.if_not_exists);
        if !column_mappings.is_empty() {
            println!("  Column mappings:");
            for (src, dest) in &column_mappings {
                println!("    {} -> {}", src, dest);
            }
        }
        if !exclude_columns.is_empty() {
            println!("  Excluded columns: {}", exclude_columns.join(", "));
        }
        println!();
        println!("To execute, run without --dry-run");
        return Ok(());
    }

    // Build load arguments
    let load_args = LoadArgs {
        endpoint: connection.endpoint,
        region,
        username: connection.username,
        source_uri: source.source_uri.clone(),
        target_table: target.table.clone(),
        schema: target.schema,
        format: format_enum,
        worker_count: load.workers,
        chunk_size_bytes,
        batch_size: load.batch_size,
        batch_concurrency: load.batch_concurrency,
        create_table_if_missing: load.if_not_exists,
        atomic: load.atomic,
        manifest_dir: output.manifest_dir.clone().map(PathBuf::from),
        quiet: output.quiet,
        debug: output.debug,
        column_mappings,
        resume_job_id: output.resume_job_id.clone(),
        on_conflict,
        verify,
        exclude_columns,
        delimiter: source.delimiter.clone(),
        quote: source.quote.clone(),
        escape: source.escape.clone(),
        // --header / --no-header are mutually exclusive (clap `conflicts_with`).
        has_header: if source.header {
            Some(true)
        } else if source.no_header {
            Some(false)
        } else {
            None
        },
    };

    // Run the load
    let result = run_load(load_args).await?;

    println!();
    println!("Load Summary");
    println!("============");
    println!("Job ID: {}", result.job_id);
    println!("Chunks processed: {}", result.chunks_processed);
    println!("Records loaded: {}", result.records_loaded);
    println!("Records failed: {}", result.records_failed);
    println!("Duration: {:.2}s", result.duration.as_secs_f64());
    println!(
        "Throughput: {:.2} records/sec",
        result.records_loaded as f64 / result.duration.as_secs_f64()
    );
    if let Some(src) = result.source_rows {
        println!("Source rows: {src}");
    }
    if let Some(v) = &result.verify {
        print_verify_detail(v, false);
    }

    // Warn if significantly fewer records were loaded than estimated
    let mut row_count_mismatch = false;
    if let Some(estimated) = result.estimated_rows {
        let total_processed = result.records_loaded + result.records_failed;
        if estimated > 0 && total_processed < estimated * 3 / 4 {
            row_count_mismatch = true;
            println!();
            println!(
                "WARNING: Only {} records processed out of ~{} estimated from file size.",
                total_processed, estimated
            );
            println!(
                "This may indicate silent parse errors. Check --header, --delimiter, --quote, and --escape settings."
            );
        }
    }

    // If errors occurred and manifest was persisted, tell the user where to find them
    if let Some(ref persisted_path) = result.persisted_manifest_dir {
        println!();
        println!("Errors detected! Manifest directory has been preserved for debugging:");
        println!("  {}", persisted_path.display());
        println!();
        println!("To inspect errors:");
        println!("  # View all chunk results");
        println!(
            "  ls {}/jobs/{}/chunks/",
            persisted_path.display(),
            result.job_id
        );
        println!("  # View errors from a specific chunk");
        println!(
            "  cat {}/jobs/{}/chunks/0000.result",
            persisted_path.display(),
            result.job_id
        );
    }

    // Note: To keep manifest directory, use --manifest-dir to specify a persistent location
    if output.keep_manifest && output.manifest_dir.is_none() {
        println!();
        println!("Warning: --keep-manifest is only effective with --manifest-dir.");
        println!("Manifest was stored in a temporary directory and has been cleaned up.");
    }

    let bad_verdict = result
        .verify
        .as_ref()
        .is_some_and(|v| is_bad_verdict(&v.verdict));
    if result.records_failed > 0 || row_count_mismatch || bad_verdict {
        std::process::exit(1);
    }

    Ok(())
}

/// Reject identifiers that would corrupt the `list-tables` TSV output (which
/// downstream `awk`/`cut` pipelines depend on) or visually deceive an operator
/// reading the output on a TTY. On a hit, returns the offending identifier.
///
/// Reject classes:
/// - C0/C1 control bytes (covers `\t`, `\n`, `\r`, NUL, DEL, ESC) — corrupt
///   TSV framing or smuggle ANSI terminal escapes.
/// - Unicode bidi/format codepoints (RLM, LRM, RTL/LTR overrides, ZWSP, BOM,
///   line/paragraph separators) — visually reorder or hide characters in a
///   terminal without using any C0/C1 byte.
/// - Comma — would mis-parse the comma-joined third TSV field on column
///   names. The schema and table fields occupy their own tab-separated slots
///   so a comma there does not ambiguate framing; checked only on columns.
fn check_listable(t: &aurora_dsql_loader::runner::PgDumpTable) -> Option<&str> {
    if t.schema.contains(is_unsafe_for_listing) {
        return Some(&t.schema);
    }
    if t.table.contains(is_unsafe_for_listing) {
        return Some(&t.table);
    }
    t.columns
        .iter()
        .find(|col| col.contains(is_unsafe_for_listing) || col.contains(','))
        .map(String::as_str)
}

fn is_unsafe_for_listing(c: char) -> bool {
    c.is_control() || aurora_dsql_loader::runner::is_bidi_or_format_char(c)
}

fn validate_delimited_options(
    format: &str,
    format_enum: Format,
    source: &SourceArgs,
) -> anyhow::Result<()> {
    let has_delimited_options = source.delimiter.is_some()
        || source.quote.is_some()
        || source.escape.is_some()
        || source.header
        || source.no_header;

    if has_delimited_options && !format_enum.is_delimited() {
        return Err(anyhow::anyhow!(
            "Delimited file options (--delimiter, --quote, --escape, --header, --no-header) \
             can only be used with CSV or TSV formats, not {}",
            format
        ));
    }
    Ok(())
}

/// CLI utility functions for parsing command-line arguments
mod cli {
    use std::collections::HashMap;

    /// Parse human-readable size strings like "10MB", "1GB", "512KB"
    pub fn parse_size_string(s: &str) -> anyhow::Result<u64> {
        let s = s.trim().to_uppercase();

        let (number_part, unit_part) = if let Some(pos) = s.find(|c: char| !c.is_ascii_digit()) {
            s.split_at(pos)
        } else {
            // No unit, assume bytes
            return s
                .parse::<u64>()
                .map_err(|e| anyhow::anyhow!("Invalid size: {}", e));
        };

        let number: u64 = number_part
            .parse()
            .map_err(|e| anyhow::anyhow!("Invalid size number '{}': {}", number_part, e))?;

        let multiplier = match unit_part.trim() {
            "B" => 1,
            "KB" => 1024,
            "MB" => 1024 * 1024,
            "GB" => 1024 * 1024 * 1024,
            "TB" => 1024 * 1024 * 1024 * 1024,
            _ => {
                return Err(anyhow::anyhow!(
                    "Invalid size unit '{}'. Valid units: B, KB, MB, GB, TB",
                    unit_part
                ));
            }
        };

        Ok(number * multiplier)
    }

    /// Extract region from DSQL endpoint format: xxx.dsql.{region}.on.aws
    pub fn extract_region_from_endpoint(endpoint: &str) -> Option<String> {
        // Format: xxx.dsql.{region}.on.aws
        let parts: Vec<&str> = endpoint.split('.').collect();
        if parts.len() >= 5
            && parts[1].contains("dsql")
            && parts[parts.len() - 2] == "on"
            && parts[parts.len() - 1] == "aws"
        {
            Some(parts[2].to_string())
        } else {
            None
        }
    }

    /// Auto-detect file format from path/URI
    pub fn detect_format_from_path(path: &str) -> Option<&'static str> {
        let lower = path.to_lowercase();

        if lower.ends_with(".csv") {
            Some("csv")
        } else if lower.ends_with(".tsv") {
            Some("tsv")
        } else if lower.ends_with(".parquet") {
            Some("parquet")
        } else {
            None
        }
    }

    /// Parse "col1,col2,col3" into Vec<String>.
    /// Errors on empty input, empty column names, or duplicates.
    pub fn parse_exclude_columns(input: &str) -> anyhow::Result<Vec<String>> {
        if input.trim().is_empty() {
            return Err(anyhow::anyhow!(
                "At least one column name is required for --exclude-columns"
            ));
        }

        let mut cols = Vec::new();
        for raw in input.split(',') {
            let name = raw.trim();
            if name.is_empty() {
                return Err(anyhow::anyhow!(
                    "Empty column name in --exclude-columns value '{}'",
                    input
                ));
            }
            if cols.iter().any(|c| c == name) {
                return Err(anyhow::anyhow!(
                    "Duplicate column name '{}' in --exclude-columns",
                    name
                ));
            }
            cols.push(name.to_string());
        }
        Ok(cols)
    }

    /// Parse column mapping string "src:dest,src2:dest2" into HashMap
    pub fn parse_column_mapping(mapping_str: &str) -> anyhow::Result<HashMap<String, String>> {
        let mut mappings = HashMap::new();

        if mapping_str.trim().is_empty() {
            return Ok(mappings);
        }

        for pair in mapping_str.split(',') {
            let parts: Vec<&str> = pair.trim().split(':').collect();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!(
                    "Invalid column mapping format '{}'. Expected format: 'source:dest,source2:dest2'",
                    pair
                ));
            }

            let source = parts[0].trim();
            let dest = parts[1].trim();

            if source.is_empty() || dest.is_empty() {
                return Err(anyhow::anyhow!(
                    "Column names cannot be empty in mapping '{}'",
                    pair
                ));
            }

            if mappings
                .insert(source.to_string(), dest.to_string())
                .is_some()
            {
                return Err(anyhow::anyhow!(
                    "Duplicate source column '{}' in column mapping",
                    source
                ));
            }
        }

        Ok(mappings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::collections::HashSet;

    /// Behaviour table for the exit-code policy (not a compile-time
    /// exhaustiveness pin — `VerifyVerdict` is cross-crate
    /// `#[non_exhaustive]`, so `is_bad_verdict` keeps a fail-closed `_`
    /// arm). Add new variants here when they land.
    #[test]
    fn is_bad_verdict_classifies_every_variant() {
        assert!(!is_bad_verdict(&VerifyVerdict::Match));
        assert!(!is_bad_verdict(&VerifyVerdict::SkippedNoExactSourceCount));
        assert!(!is_bad_verdict(&VerifyVerdict::ValueCheckSkipped));
        assert!(is_bad_verdict(&VerifyVerdict::LoaderDropped(1)));
        assert!(is_bad_verdict(&VerifyVerdict::MissingTarget(1)));
        assert!(is_bad_verdict(&VerifyVerdict::ExtraTarget(1)));
        assert!(is_bad_verdict(&VerifyVerdict::RowsConflictedAtTarget(1)));
        assert!(is_bad_verdict(&VerifyVerdict::ValueMismatch(1)));
        assert!(is_bad_verdict(&VerifyVerdict::ValueRowMissingAtTarget(1)));
    }

    /// The L3 verdict strings are the operator's only window into a failed
    /// value check; pin that each renders its key token and interpolates the
    /// magnitude (a swapped arm or dropped count would ship silently).
    #[test]
    fn format_verdict_renders_l3_verdicts() {
        assert!(format_verdict(&VerifyVerdict::ValueMismatch(7)).contains("VALUE MISMATCH 7"));
        assert!(
            format_verdict(&VerifyVerdict::ValueRowMissingAtTarget(3))
                .contains("MISSING AT TARGET 3")
        );
        assert!(format_verdict(&VerifyVerdict::ValueCheckSkipped).contains("SKIPPED value check"));
    }

    #[test]
    fn parse_exclude_columns_basic() {
        assert_eq!(
            cli::parse_exclude_columns("id").unwrap(),
            vec!["id".to_string()]
        );
        assert_eq!(
            cli::parse_exclude_columns("id,created_at,updated_at").unwrap(),
            vec![
                "id".to_string(),
                "created_at".to_string(),
                "updated_at".to_string()
            ]
        );
        assert_eq!(
            cli::parse_exclude_columns("id, created_at , updated_at ").unwrap(),
            vec![
                "id".to_string(),
                "created_at".to_string(),
                "updated_at".to_string()
            ]
        );
    }

    #[test]
    fn parse_exclude_columns_empty_errors() {
        let err = cli::parse_exclude_columns("").unwrap_err().to_string();
        assert!(err.contains("At least one column name is required"));
        let err = cli::parse_exclude_columns("   ").unwrap_err().to_string();
        assert!(err.contains("At least one column name is required"));
    }

    #[test]
    fn parse_exclude_columns_empty_segment_errors() {
        let err = cli::parse_exclude_columns("id,,name")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Empty column name"));
    }

    #[test]
    fn parse_exclude_columns_duplicate_errors() {
        let err = cli::parse_exclude_columns("id,name,id")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Duplicate column name 'id'"));
    }

    // Feature: loader-column-exclusion, Property 1: Parsing round-trip and duplicate detection
    // NOTE: generator restricts to ASCII identifiers so no commas/whitespace leak into inputs
    // and break the round-trip.
    proptest::proptest! {
        #[test]
        fn prop_parse_exclude_columns_roundtrip(
            names in proptest::collection::vec("[a-zA-Z_][a-zA-Z0-9_]{0,15}", 1..8)
        ) {
            let unique: Vec<String> = {
                let mut seen = HashSet::new();
                names.into_iter().filter(|n| seen.insert(n.clone())).collect()
            };
            proptest::prop_assume!(!unique.is_empty());
            let joined = unique.join(",");
            let parsed = cli::parse_exclude_columns(&joined).unwrap();
            proptest::prop_assert_eq!(parsed, unique);
        }

        #[test]
        fn prop_parse_exclude_columns_duplicate_detected(
            base in "[a-zA-Z_][a-zA-Z0-9_]{0,15}",
            others in proptest::collection::vec("[a-zA-Z_][a-zA-Z0-9_]{0,15}", 0..4),
        ) {
            // Inject base twice somewhere in the list to force a duplicate
            let mut names = others;
            names.push(base.clone());
            names.push(base.clone());
            let joined = names.join(",");
            let err = cli::parse_exclude_columns(&joined).unwrap_err().to_string();
            // Specifically the duplicate-detection path (not some other error)
            proptest::prop_assert!(err.contains("Duplicate column name"));
        }
    }

    #[test]
    fn exclude_columns_conflicts_with_if_not_exists_at_parse_time() {
        let result = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "test.csv",
            "--table",
            "t",
            "--exclude-columns",
            "pk_id",
            "--if-not-exists",
            "--dry-run",
        ]);
        let err = match result {
            Ok(_) => panic!("clap should reject --exclude-columns + --if-not-exists"),
            Err(e) => e,
        };
        // Assert on ErrorKind (the stable contract) rather than message wording,
        // which clap is free to re-phrase between releases.
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn atomic_requires_if_not_exists_at_parse_time() {
        let result = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "test.csv",
            "--table",
            "t",
            "--atomic",
            "--dry-run",
        ]);
        let err = match result {
            Ok(_) => panic!("clap should reject --atomic without --if-not-exists"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn atomic_conflicts_with_resume_at_parse_time() {
        let result = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "test.csv",
            "--table",
            "t",
            "--if-not-exists",
            "--atomic",
            "--manifest-dir",
            "/tmp/m",
            "--resume-job-id",
            "job-123",
            "--dry-run",
        ]);
        let err = match result {
            Ok(_) => panic!("clap should reject --atomic + --resume-job-id"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn header_and_no_header_are_mutually_exclusive_at_parse_time() {
        let result = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "test.csv",
            "--table",
            "t",
            "--header",
            "--no-header",
            "--dry-run",
        ]);
        let err = match result {
            Ok(_) => panic!("clap should reject --header + --no-header"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn header_flag_rejected_with_parquet_format() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "test_file.parquet",
            "--table",
            "my_table",
            "--header",
            "--dry-run",
        ])
        .expect("args should parse (clap groups don't restrict by format)");

        let Command::Load { source, .. } = args.command else {
            panic!("expected Load")
        };
        let err = validate_delimited_options("parquet", Format::Parquet, &source)
            .expect_err("--header on parquet must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("--header"),
            "error should name the offending --header flag: {msg}"
        );
    }

    #[test]
    fn pgdump_format_parses_through_cli() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "/tmp/foo.sql",
            "--format",
            "pgdump",
            "--table",
            "users",
            "--dry-run",
        ])
        .expect("pgdump format should parse");
        let Command::Load { source, .. } = args.command else {
            panic!("expected Load")
        };
        validate_delimited_options("pgdump", Format::PgDump, &source)
            .expect("no delimited options were passed");
    }

    #[test]
    fn pgdump_format_rejects_delimited_options() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "/tmp/foo.sql",
            "--format",
            "pgdump",
            "--table",
            "users",
            "--header",
            "--dry-run",
        ])
        .expect("clap accepts --header (group is per-format)");
        let Command::Load { source, .. } = args.command else {
            panic!("expected Load")
        };
        let err = validate_delimited_options("pgdump", Format::PgDump, &source)
            .expect_err("--header on pgdump must be rejected");
        assert!(err.to_string().contains("--header"));
    }

    #[test]
    fn list_tables_subcommand_parses() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "list-tables",
            "--source-uri",
            "/tmp/x.sql",
        ])
        .expect("list-tables should parse");
        match args.command {
            Command::ListTables { source_uri } => {
                assert_eq!(source_uri, "/tmp/x.sql");
            }
            _ => panic!("expected ListTables"),
        }
    }

    /// `migrate` accepts the minimum required flags (endpoint + source-uri)
    /// and applies sensible defaults for everything else. Pinning those
    /// defaults guards against an accidental change to the on_conflict /
    /// schema / chunk-size defaults silently shifting migration behavior.
    #[test]
    fn migrate_subcommand_parses_with_defaults() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "migrate",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "/tmp/dump.sql",
        ])
        .expect("migrate should parse");
        match args.command {
            Command::Migrate {
                connection,
                source_uri,
                schema,
                dry_run,
                workers,
                chunk_size,
                batch_size,
                batch_concurrency,
                on_conflict,
                verify,
                quiet,
                debug,
            } => {
                assert_eq!(connection.endpoint, "xxxx.dsql.us-east-1.on.aws");
                assert_eq!(connection.username, "admin");
                assert!(connection.region.is_none());
                assert_eq!(source_uri, "/tmp/dump.sql");
                assert_eq!(schema, "public");
                assert!(!dry_run);
                assert_eq!(workers, 8);
                assert_eq!(chunk_size, "10MB");
                assert_eq!(batch_size, 2000);
                assert_eq!(batch_concurrency, 32);
                assert_eq!(on_conflict, "error");
                assert_eq!(verify, "count");
                assert!(!quiet);
                assert!(!debug);
                // The CLI default round-trips through `OnConflict::FromStr`
                // to `OnConflict::Error`. A future enum-variant rename
                // that breaks the round-trip would surface here even
                // though the raw string default ("error") stays the same.
                assert_eq!(
                    on_conflict.parse::<OnConflict>().unwrap(),
                    OnConflict::Error,
                    "migrate --on-conflict default must parse to OnConflict::Error",
                );
                // CLI default "count" must round-trip to VerifyMode::Count.
                assert_eq!(
                    verify.parse::<VerifyMode>().unwrap(),
                    VerifyMode::Count,
                    "migrate --verify default must parse to VerifyMode::Count",
                );
            }
            _ => panic!("expected Migrate"),
        }
    }

    /// `--dry-run` is the safe-review flag; assert it parses cleanly. Used
    /// in real workflows to preview the proposed DDL before committing.
    #[test]
    fn migrate_subcommand_parses_dry_run() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "migrate",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "s3://bucket/dump.sql",
            "--dry-run",
        ])
        .expect("migrate --dry-run should parse");
        match args.command {
            Command::Migrate {
                source_uri,
                dry_run,
                ..
            } => {
                assert_eq!(source_uri, "s3://bucket/dump.sql");
                assert!(dry_run);
            }
            _ => panic!("expected Migrate"),
        }
    }

    /// `migrate` requires `--source-uri`; omitting it must surface a clap
    /// error, not panic at runtime. Guards the contract that operators
    /// can't accidentally launch a migration with no input.
    #[test]
    fn migrate_subcommand_rejects_missing_source_uri() {
        let result = Args::try_parse_from([
            "aurora-dsql-loader",
            "migrate",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
        ]);
        // Args doesn't derive Debug, so we can't call .expect_err — pattern
        // match instead.
        match result {
            Ok(_) => panic!("missing --source-uri must be a parse error"),
            Err(e) => assert_eq!(e.kind(), clap::error::ErrorKind::MissingRequiredArgument),
        }
    }

    #[test]
    fn check_listable_accepts_safe_identifiers() {
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "users".into(),
            columns: vec!["id".into(), "name".into()],
        };
        assert!(check_listable(&t).is_none());
    }

    #[test]
    fn check_listable_rejects_tab_in_identifier() {
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "bad\tname".into(),
            columns: vec!["id".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn check_listable_rejects_newline_in_column_name() {
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "users".into(),
            columns: vec!["id".into(), "broken\nname".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn check_listable_rejects_ansi_escape_in_table() {
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "x\x1b]0;OWNED\x07".into(),
            columns: vec!["id".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn check_listable_rejects_comma_in_column_name() {
        // Comma in a column name would mis-parse the comma-joined third TSV field.
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "users".into(),
            columns: vec!["a,b".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn check_listable_rejects_unicode_rtl_override() {
        // U+202E "RIGHT-TO-LEFT OVERRIDE" reorders subsequent characters in
        // a terminal — classic homograph spoof. Must be rejected even though
        // it's not a C0/C1 control byte.
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "evil\u{202E}name".into(),
            columns: vec!["id".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn check_listable_rejects_zero_width_space() {
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "public".into(),
            table: "users".into(),
            columns: vec!["id\u{200B}hidden".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn check_listable_rejects_bom() {
        let t = aurora_dsql_loader::runner::PgDumpTable {
            schema: "\u{FEFF}public".into(),
            table: "users".into(),
            columns: vec!["id".into()],
        };
        assert!(check_listable(&t).is_some());
    }

    #[test]
    fn parquet_without_delimited_options_parses_successfully() {
        let args = Args::try_parse_from([
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "test_file.parquet",
            "--table",
            "my_table",
            "--dry-run",
        ])
        .expect("args should parse");

        let Command::Load { source, .. } = args.command else {
            panic!("expected Load")
        };
        validate_delimited_options("parquet", Format::Parquet, &source)
            .expect("parquet load should not trigger the delimited-options error");
    }
}
