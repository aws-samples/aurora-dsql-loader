# Changelog

## [2.0.0] - 2026-01-06

Rewrite in Rust with 3-5x throughput improvement and new features.

### Added
- Parquet and TSV file support
- S3 URI support (load directly from S3)
- Auto-detection of file format and AWS region
- Real-time progress bars with throughput metrics
- Resumable loads with manifest tracking
- Automatic retries and error handling
- Dry run mode and column mapping
- Quiet mode for scripting

### Changed
- **Breaking**: New CLI (see migration below)
- **Breaking**: Uses AWS SDK for authentication (no manual token generation)
- Requires Rust 1.85+ to build

### Removed
- Python v1 implementation
- `PGHOST`, `PGUSER`, `PGPASSWORD` environment variables

### Migrating from v1.x

**Old (Python):**
```bash
PGUSER=admin \
PGHOST=xxxx.dsql.us-east-1.on.aws \
PGPASSWORD="$(aws dsql generate-db-connect-admin-auth-token --hostname $PGHOST --region us-east-1)" \
./aurora-dsql-loader.py \
  --filename data.csv \
  --tablename my_table \
  --threads 10
```

**New (Rust):**
```bash
aurora-dsql-loader load \
  --endpoint xxxx.dsql.us-east-1.on.aws \
  --source-uri data.csv \
  --table my_table
```

**Key CLI changes:**
- `--filename` → `--source-uri` (now supports S3)
- `--tablename` → `--table`
- `--threads` → `--workers`
- `--host`, `--user`, `--password` → `--endpoint` (AWS SDK handles auth)
- `--delim` → `--format csv|tsv|parquet` (or auto-detect)

## [1.0.0] - 2024

Initial Python implementation.
