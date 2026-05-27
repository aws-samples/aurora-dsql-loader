# Changelog

## [3.0.0] - 2026-05-26

### Changed
- **Breaking**: CSV/TSV loads now default to `--no-header` (treat first row as
  data). Previously, the loader assumed a header row existed and silently
  dropped the first data row when one did not. This caused customers loading
  headerless files to lose exactly one row per file with no diagnostic
  ([#28](https://github.com/aws-samples/aurora-dsql-loader/issues/28)).
  The new default aligns with PostgreSQL `COPY FROM` (HEADER false), Amazon
  Redshift (`IGNOREHEADER 0`), Snowflake (`SKIP_HEADER 0`), and BigQuery
  (`--skip_leading_rows 0`).

### Added
- `--header` flag for CSV/TSV loads to opt into header-row skipping.
- Mutual-exclusion validation: `--header` and `--no-header` cannot both be set.

### Migration from 2.x
- If your CSV/TSV has a header row, **add `--header`** to your invocation:
  ```
  aurora-dsql-loader load ... --source-uri data.csv --header
  ```
- If your file has no header, no change is needed — the new default already
  matches the actual file shape.
- The `--no-header` flag continues to work; it explicitly sets header behavior
  to `false`, which matches the new default. Emits no diagnostic and will be
  removed in a future release.

## [2.1.0] - 2026-05-07

### Added
- `--exclude-columns col1,col2` flag to skip columns from generated INSERTs so
  the DB applies the column's `DEFAULT` expression (e.g. `gen_random_uuid()`,
  `CURRENT_TIMESTAMP`). Source records must still contain these columns in
  their original positions; the loader drops them by index before batching.
  Cannot be combined with `--if-not-exists`.

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
