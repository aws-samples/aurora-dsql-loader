use aurora_dsql_loader::runner::{Format, LoadArgs, run_load};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Clone)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Subcommand)]
enum Command {
    Load {
        /// DSQL cluster endpoint (region is auto-detected from endpoint format)
        #[arg(short, long)]
        endpoint: String,

        /// Path to source data file or S3 URI (local path, s3://bucket/key)
        #[arg(short, long)]
        source_uri: String,

        /// Target table name
        #[arg(short, long)]
        table: String,

        /// AWS region (optional, inferred from endpoint if not specified)
        #[arg(short, long)]
        region: Option<String>,

        /// Database username
        #[arg(short, long, default_value = "admin")]
        username: String,

        /// File format (csv, tsv, parquet) - auto-detected from extension if not specified
        #[arg(short, long)]
        format: Option<String>,

        /// Number of worker threads
        #[arg(short, long, default_value = "8")]
        workers: usize,

        /// Partition size (e.g., 10MB, 1GB)
        #[arg(short, long, default_value = "10MB")]
        partition_size: String,

        /// Batch size for inserts
        #[arg(short, long, default_value = "2000")]
        batch_size: usize,

        /// Number of concurrent batches per worker
        #[arg(long, default_value = "32")]
        batch_concurrency: usize,

        /// Create table if it doesn't exist
        #[arg(long)]
        if_not_exists: bool,

        /// Validate schema and show plan without loading data
        #[arg(long)]
        dry_run: bool,

        /// Keep manifest directory after load (for debugging)
        #[arg(long)]
        keep_manifest: bool,

        /// Column mapping from source to destination (format: src:dest,src2:dest2)
        #[arg(long)]
        column_map: Option<String>,

        /// Quiet mode - minimal output, only show summary
        #[arg(short, long)]
        quiet: bool,

        /// Directory for manifest files (default: system temp directory)
        #[arg(long)]
        manifest_dir: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match args.command {
        Command::Load {
            endpoint,
            source_uri,
            table,
            region,
            username,
            format,
            workers,
            partition_size,
            batch_size,
            batch_concurrency,
            if_not_exists,
            dry_run,
            keep_manifest,
            column_map,
            quiet,
            manifest_dir,
        } => {
            run_loader(
                endpoint,
                source_uri,
                table,
                region,
                username,
                format,
                workers,
                partition_size,
                batch_size,
                batch_concurrency,
                if_not_exists,
                dry_run,
                keep_manifest,
                column_map,
                quiet,
                manifest_dir,
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_loader(
    endpoint: String,
    source_uri: String,
    table: String,
    region: Option<String>,
    username: String,
    format: Option<String>,
    workers: usize,
    partition_size: String,
    batch_size: usize,
    batch_concurrency: usize,
    if_not_exists: bool,
    dry_run: bool,
    keep_manifest: bool,
    column_map: Option<String>,
    quiet: bool,
    manifest_dir: Option<String>,
) -> anyhow::Result<()> {
    // Initialize tracing based on quiet mode
    use tracing_subscriber::{EnvFilter, FmtSubscriber};
    let filter = if quiet {
        EnvFilter::new("aurora_dsql_loader=warn,sqlx=off")
    } else {
        EnvFilter::new("aurora_dsql_loader=info,sqlx=off")
    };
    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    // Setup output based on quiet flag
    if !quiet {
        println!("DSQL Data Loader");
        println!("================");
        println!("Endpoint: {}", endpoint);
        println!("Source: {}", source_uri);
        println!("Table: {}", table);
        println!("Workers: {}", workers);
        println!();
    }

    // Extract region from endpoint if not provided
    let region = if let Some(r) = region {
        r
    } else {
        cli::extract_region_from_endpoint(&endpoint).ok_or_else(|| {
            anyhow::anyhow!(
                "Could not extract region from endpoint '{}'.\n\
                 Expected format: xxx.dsql.REGION.on.aws (e.g., xxx.dsql.us-east-1.on.aws)\n\
                 Please specify --region explicitly.",
                endpoint
            )
        })?
    };

    // Auto-detect format from file extension if not provided
    let format = if let Some(f) = format {
        f
    } else {
        cli::detect_format_from_path(&source_uri).ok_or_else(|| {
            anyhow::anyhow!(
                "Could not detect format from file '{}'.\n\
                 Supported extensions: .csv, .tsv, .parquet\n\
                 Please specify --format explicitly.",
                source_uri
            )
        })?
    };

    // Parse column mapping if provided
    let column_mappings = if let Some(ref mapping_str) = column_map {
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

    // Parse partition size
    let partition_size_bytes = cli::parse_size_string(&partition_size)
        .map_err(|e| anyhow::anyhow!("Invalid partition size '{}': {}", partition_size, e))?;

    // Parse format
    let format_enum = Format::parse(&format)?;

    // Handle dry-run mode
    if dry_run {
        println!("DRY RUN MODE - No data will be loaded");
        println!();
        println!("Configuration:");
        println!("  Table: {}", table);
        println!("  Workers: {}", workers);
        println!("  Partition size: {} bytes", partition_size_bytes);
        println!("  Batch size: {}", batch_size);
        println!("  Batch concurrency: {}", batch_concurrency);
        println!("  Create table if missing: {}", if_not_exists);
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
        endpoint,
        region,
        username,
        source_uri,
        target_table: table.clone(),
        format: format_enum,
        worker_count: workers,
        partition_size_bytes,
        batch_size,
        batch_concurrency,
        create_table_if_missing: if_not_exists,
        manifest_dir: manifest_dir.clone().map(PathBuf::from),
        quiet,
        column_mappings,
    };

    // Run the load
    let result = run_load(load_args).await?;

    println!();
    println!("Load Summary");
    println!("============");
    println!("Job ID: {}", result.job_id);
    println!("Partitions processed: {}", result.partitions_processed);
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
        println!("  # View all partition results");
        println!("  ls {}/jobs/{}/partitions/", persisted_path.display(), result.job_id);
        println!("  # View errors from a specific partition");
        println!("  cat {}/jobs/{}/partitions/0000.result", persisted_path.display(), result.job_id);
    }

    // Note: To keep manifest directory, use --manifest-dir to specify a persistent location
    if keep_manifest && manifest_dir.is_none() {
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
