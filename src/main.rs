use aurora_dsql_loader::runner::{Format, LoadArgs, OnConflict, run_load};
use clap::{ArgGroup, Args as ClapArgs, Parser, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;

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
    /// for csv/tsv/parquet; pgdump must be specified explicitly.
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

    /// Column mapping from source to destination (format: src:dest,src2:dest2)
    #[arg(long)]
    column_map: Option<String>,

    /// Conflict resolution strategy (do-nothing, do-update, error)
    #[arg(long, default_value = "do-nothing")]
    on_conflict: String,

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
            for t in tables {
                // Tab-separated for easy `awk`/`cut` consumption. Columns are
                // comma-joined on the third column so each block is one line.
                println!("{}\t{}\t{}", t.schema, t.table, t.columns.join(","));
            }
        }
    }
    Ok(())
}

async fn run_loader(
    connection: ConnectionArgs,
    source: SourceArgs,
    target: TargetArgs,
    load: LoadParams,
    output: OutputArgs,
) -> anyhow::Result<()> {
    // Initialize tracing based on quiet mode
    // RUST_LOG environment variable takes precedence if set
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        if output.quiet {
            EnvFilter::new("aurora_dsql_loader=warn,sqlx=off")
        } else {
            EnvFilter::new("aurora_dsql_loader=info,sqlx=off")
        }
    });
    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

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

    // Extract region from endpoint if not provided
    let region = if let Some(r) = connection.region {
        r
    } else {
        cli::extract_region_from_endpoint(&connection.endpoint).ok_or_else(|| {
            anyhow::anyhow!(
                "Could not extract region from endpoint '{}'.\n\
                 Expected format: xxx.dsql.REGION.on.aws (e.g., xxx.dsql.us-east-1.on.aws)\n\
                 Please specify --region explicitly.",
                connection.endpoint
            )
        })?
    };

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
        manifest_dir: output.manifest_dir.clone().map(PathBuf::from),
        quiet: output.quiet,
        debug: output.debug,
        column_mappings,
        resume_job_id: output.resume_job_id.clone(),
        on_conflict,
        exclude_columns,
        delimiter: source.delimiter.clone(),
        quote: source.quote.clone(),
        escape: source.escape.clone(),
        // `header_mode` ArgGroup makes --header/--no-header mutually exclusive at parse time.
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

    if result.records_failed > 0 || row_count_mismatch {
        std::process::exit(1);
    }

    Ok(())
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
