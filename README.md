# Aurora DSQL Data Loader

Fast, parallel data loader for Aurora DSQL. Load CSV, TSV, and Parquet files into DSQL with automatic schema detection and progress tracking.

> Migrating from Python v1? See [CHANGELOG.md](CHANGELOG.md).

## Quick Start

### 1. Install

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

## Key Features

- **Fast**: Parallel loading with configurable workers
- **Smart**: Auto-detects file format and region
- **Reliable**: Automatic retries and resumable loads
- **Flexible**: Works with local files or S3 URIs
- **Formats**: CSV, TSV, and Parquet support

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
