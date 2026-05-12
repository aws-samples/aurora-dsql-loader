# Aurora DSQL Data Loader

Fast, parallel data loader for Aurora DSQL. Load CSV, TSV, and Parquet files into DSQL with automatic schema detection and progress tracking.

> Migrating from Python v1? See [CHANGELOG.md](CHANGELOG.md).

## Quick Start

### 1. Install

**Download pre-built binary:** [Latest releases](https://github.com/aws-samples/aurora-dsql-loader/releases/latest)

**Or build from source:**

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone and install
git clone https://github.com/aws-samples/aurora-dsql-loader.git
cd aurora-dsql-loader
cargo install --path .
```

### 2. Configure AWS credentials

```bash
aws configure
# or
aws sso login
```

### 3. Load data

```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri data.csv \
  --table my_table
```

That's it! The loader will:
- Auto-detect the file format (CSV, TSV, or Parquet)
- Infer the schema from your data
- Load data in parallel with progress tracking
- Handle retries and errors automatically

## Common Examples

**Load from S3:**
```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri s3://my-bucket/data.parquet \
  --table analytics_data
```

**Create table automatically:**
```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri data.csv \
  --table new_table \
  --if-not-exists
```

**Validate without loading:**
```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri data.csv \
  --table my_table \
  --dry-run
```

**Use DB-side defaults (e.g. server-generated UUIDs):**
```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri data.csv \
  --table my_table \
  --exclude-columns pk_id,created_at
```
Listed columns are dropped from the INSERT so DSQL applies the column's `DEFAULT` expression (e.g. `gen_random_uuid()`, `CURRENT_TIMESTAMP`). Source records must still contain these columns in their original positions.

## Key Features

- **Fast**: Parallel loading with configurable workers
- **Smart**: Auto-detects file format and region
- **Reliable**: Automatic retries and fault-tolerant loading
- **Flexible**: Works with local files or S3 URIs
- **Formats**: CSV, TSV, and Parquet support

## Resuming Failed Loads

If a load fails mid-execution, resume from where it left off using `--resume-job-id`:

```bash
# Initial load (note the Job ID in output)
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri large-file.csv \
  --table my_table \
  --manifest-dir ./my-load-manifest

# Resume after failure
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri large-file.csv \
  --table my_table \
  --manifest-dir ./my-load-manifest \
  --resume-job-id abc-123-def-456
```

Resume automatically retries failed chunks and skips completed ones. For safety against duplicates on retry, use unique constraints on your table—the loader will use `ON CONFLICT DO NOTHING` to skip duplicates.

## Performance Tuning

The loader parallelizes work on two axes. Total in-flight INSERTs ≈ `workers × batch-concurrency`.

| Flag | Default | Effect |
|------|---------|--------|
| `--workers` | `8` | Worker threads competing for file chunks |
| `--batch-concurrency` | `32` | Concurrent INSERT batches per worker |
| `--batch-size` | `2000` | Records per INSERT statement |
| `--chunk-size` | `10MB` | File is split into chunks of this size; workers claim one chunk at a time |

**To load faster**, raise `--workers` and/or `--batch-concurrency`. For very large files, a smaller `--chunk-size` (e.g. `5MB`) produces more chunks and lets workers balance load better.

```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri big.csv \
  --table my_table \
  --workers 16 \
  --batch-concurrency 64 \
  --chunk-size 5MB
```

**Watch for:**
- **Parameter limit**: DSQL caps a statement at 65,535 parameters, so `batch-size × column count` must stay below that. If you see `too many arguments for query`, the loader will print a suggested smaller `--batch-size`.
- **Rate limits**: pushing concurrency very high can get requests throttled by DSQL. Back off `--workers` or `--batch-concurrency` if you see retry-heavy behavior.

## Requirements

- **Rust**: 1.85 or later (for building)
- **AWS**: Aurora DSQL cluster with `dsql:DbConnectAdmin` or `dsql:DbConnect` permissions
- **Credentials**: Configured via AWS CLI, SSO, or IAM role

## Options

See all options with:
```bash
aurora-dsql-loader load --help
```

## Troubleshooting

**Authentication error?**
```bash
aws sts get-caller-identity  # Verify credentials
```

**Build error?**
```bash
rustup update stable  # Update Rust
```

**Connection error?**
See [Aurora DSQL Troubleshooting](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/troubleshooting.html)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## License

MIT-0 License. See [LICENSE](LICENSE) file.
