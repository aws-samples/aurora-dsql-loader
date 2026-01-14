use aurora_dsql_loader::runner::{Format, LoadArgs, run_load};
use clap::{Args as ClapArgs, Parser, Subcommand};
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
struct SourceArgs {
    /// Path to source data file or S3 URI (local path, s3://bucket/key)
    #[arg(short, long)]
    source_uri: String,

    /// File format (csv, tsv, parquet) - auto-detected from extension if not specified
    #[arg(short, long)]
    format: Option<String>,
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
}

#[derive(Clone, ClapArgs)]
struct OutputArgs {
    /// Quiet mode - minimal output, only show summary
    #[arg(short, long)]
    quiet: bool,

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
    let format = if let Some(f) = source.format {
        f
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

    // Parse chunk size
    let chunk_size_bytes = cli::parse_size_string(&load.chunk_size)
        .map_err(|e| anyhow::anyhow!("Invalid chunk size '{}': {}", load.chunk_size, e))?;

    // Parse format
    let format_enum = Format::parse(&format)?;

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
        source_uri: source.source_uri,
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
        column_mappings,
        resume_job_id: output.resume_job_id.clone(),
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
    pub fn detect_format_from_path(path: &str) -> Option<String> {
        let lower = path.to_lowercase();

        if lower.ends_with(".csv") {
            Some("csv".to_string())
        } else if lower.ends_with(".tsv") {
            Some("tsv".to_string())
        } else if lower.ends_with(".parquet") {
            Some("parquet".to_string())
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
