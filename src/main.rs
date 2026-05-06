use aurora_dsql_loader::runner::{Format, LoadArgs, OnConflict, run_load};
use clap::parser::ValueSource;
use clap::{ArgGroup, ArgMatches, Args as ClapArgs, CommandFactory, FromArgMatches, Subcommand};
use std::path::PathBuf;

const DELIMITED_OPTION_FLAGS: &[&str] = &["delimiter", "quote", "escape", "no_header"];

#[derive(clap::Parser, Clone)]
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

    /// File format (csv, tsv, parquet) - auto-detected from extension if not specified
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

    /// File has no header row
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = Args::command().get_matches();
    let args = Args::from_arg_matches(&matches)
        .map_err(|e| anyhow::anyhow!("Failed to parse arguments: {}", e))?;

    match args.command {
        Command::Load {
            connection,
            source,
            target,
            load,
            output,
        } => {
            let load_matches = matches
                .subcommand_matches("load")
                .expect("parsed Load command must have matching subcommand matches");
            run_loader(connection, source, target, load, output, load_matches).await?;
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
    load_matches: &ArgMatches,
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
        std::collections::HashMap::new()
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
    validate_delimited_options(format, format_enum, load_matches)?;

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
        delimiter: source.delimiter.clone(),
        quote: source.quote.clone(),
        escape: source.escape.clone(),
        has_header: if source.no_header { Some(false) } else { None },
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
                "This may indicate silent parse errors. Check --delimiter, --quote, and --escape settings."
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
    matches: &ArgMatches,
) -> anyhow::Result<()> {
    let user_provided_delimited_option = DELIMITED_OPTION_FLAGS
        .iter()
        .any(|id| matches.value_source(id) == Some(ValueSource::CommandLine));

    if user_provided_delimited_option && !format_enum.is_delimited() {
        return Err(anyhow::anyhow!(
            "Delimited file options (--delimiter, --quote, --escape, --no-header) \
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

    fn load_matches(extra: &[&str]) -> ArgMatches {
        let mut argv: Vec<&str> = vec![
            "aurora-dsql-loader",
            "load",
            "--endpoint",
            "xxxx.dsql.us-east-1.on.aws",
            "--source-uri",
            "file.dat",
            "--table",
            "my_table",
            "--dry-run",
        ];
        argv.extend_from_slice(extra);
        let matches = Args::command()
            .try_get_matches_from(argv)
            .expect("args should parse");
        matches
            .subcommand_matches("load")
            .expect("load subcommand")
            .clone()
    }

    #[test]
    fn parquet_without_delimited_options_parses_successfully() {
        let m = load_matches(&[]);
        validate_delimited_options("parquet", Format::Parquet, &m)
            .expect("parquet load with no user-provided delimited flags must pass");
    }

    // Covers {csv, tsv, parquet} x each delimited flag. Catches future
    // regressions from a new delimited option not being registered in
    // DELIMITED_OPTION_FLAGS, or a new format not being added to
    // Format::is_delimited.
    #[test]
    fn delimited_options_matrix() {
        struct Case {
            flags: &'static [&'static str],
            format_name: &'static str,
            format: Format,
            should_pass: bool,
        }

        let cases = [
            Case {
                flags: &[],
                format_name: "csv",
                format: Format::Csv,
                should_pass: true,
            },
            Case {
                flags: &[],
                format_name: "tsv",
                format: Format::Tsv,
                should_pass: true,
            },
            Case {
                flags: &[],
                format_name: "parquet",
                format: Format::Parquet,
                should_pass: true,
            },
            Case {
                flags: &["--delimiter", ","],
                format_name: "csv",
                format: Format::Csv,
                should_pass: true,
            },
            Case {
                flags: &["--quote", "\""],
                format_name: "csv",
                format: Format::Csv,
                should_pass: true,
            },
            Case {
                flags: &["--escape", "\\"],
                format_name: "csv",
                format: Format::Csv,
                should_pass: true,
            },
            Case {
                flags: &["--no-header"],
                format_name: "tsv",
                format: Format::Tsv,
                should_pass: true,
            },
            Case {
                flags: &["--delimiter", ","],
                format_name: "parquet",
                format: Format::Parquet,
                should_pass: false,
            },
            Case {
                flags: &["--quote", "\""],
                format_name: "parquet",
                format: Format::Parquet,
                should_pass: false,
            },
            Case {
                flags: &["--escape", "\\"],
                format_name: "parquet",
                format: Format::Parquet,
                should_pass: false,
            },
            Case {
                flags: &["--no-header"],
                format_name: "parquet",
                format: Format::Parquet,
                should_pass: false,
            },
        ];

        for case in &cases {
            let m = load_matches(case.flags);
            let result = validate_delimited_options(case.format_name, case.format, &m);
            assert_eq!(
                result.is_ok(),
                case.should_pass,
                "flags={:?} format={} expected_pass={} got={:?}",
                case.flags,
                case.format_name,
                case.should_pass,
                result,
            );
        }
    }

    // A flag with a default must not count as user-provided for validation.
    #[test]
    fn defaults_do_not_count_as_user_provided() {
        // Sanity: the real CLI (no defaults on these fields today) accepts parquet + no flags.
        let m = load_matches(&[]);
        for flag in DELIMITED_OPTION_FLAGS {
            assert_ne!(
                m.value_source(flag),
                Some(ValueSource::CommandLine),
                "{flag} must not be reported as user-provided when absent from argv",
            );
        }
        validate_delimited_options("parquet", Format::Parquet, &m)
            .expect("parquet load must pass when no delimited flag is on the command line");
    }
}
