# Aurora DSQL Data Loader

Fast, parallel data loader for Aurora DSQL. Load CSV, TSV, and Parquet files into DSQL with automatic schema detection and progress tracking.

> Migrating from Python v1? See [CHANGELOG.md](CHANGELOG.md).

## Contents

- [Quick Start](#quick-start)
- [Commands](#commands)
- [Examples](#examples)
  - [Load CSV, TSV, or Parquet](#load-csv-tsv-or-parquet)
  - [Load from a pg_dump file](#load-from-a-pg_dump-file)
  - [Migrate a full pg_dump into DSQL](#migrate-a-full-pg_dump-into-dsql)
  - [Migrate from another DSQL cluster](#migrate-from-another-dsql-cluster)
  - [CSV/TSV header behavior](#csvtsv-header-behavior)
- [Resuming Failed Loads](#resuming-failed-loads)
- [Verification](#verification)
- [Performance Tuning](#performance-tuning)
- [Requirements](#requirements)
- [Options](#options)
- [Troubleshooting](#troubleshooting)

## Commands

| Command | What it does |
|---------|--------------|
| `load` | Load a CSV, TSV, Parquet, or `pg_dump` data file into an existing DSQL table. |
| `migrate` | Apply a full `pg_dump` (DDL + data) into DSQL: create tables, then load. |
| `export` | Read a DSQL cluster into a plain `.sql` file (schema + data) for `migrate`. |
| `list-tables` | List the tables in a `pg_dump` file (for scripting multi-table loads). |

Run `aurora-dsql-loader <command> --help` for the full flag set.

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

## Examples

### Load CSV, TSV, or Parquet

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

### Load from a pg_dump file

```bash
# 1. Generate the dump on the source PG side:
pg_dump --data-only --table=users mydb > users.sql

# 2. Load. The target table must already exist with a compatible schema.
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri users.sql \
  --format pgdump \
  --table users
```

Notes:
- Only plain (`-Fp`, the default) `pg_dump` output with `COPY FROM stdin` blocks
  is supported. Custom (`-Fc`) and directory (`-Fd`) formats are not.
- The dump may contain DDL and other tables; we locate the `COPY` block matching
  `--schema.--table` and ignore the rest.
- Run with `--data-only` to skip DDL the loader would otherwise discard. The
  target table must exist; `--if-not-exists` is rejected for `pgdump`.
- `--column-map` and `--exclude-columns` are not supported (the column set is
  embedded in the dump's `COPY` statement).

**Discover tables in a dump (multi-table workflows):**

```bash
$ aurora-dsql-loader list-tables --source-uri backup.sql
public	users	id,email,created_at
public	orders	id,user_id,total_cents,status
sales	commissions	rep_id,amount,paid_at

# Driving multi-table loads with shell:
aurora-dsql-loader list-tables --source-uri backup.sql | while IFS=$'\t' read schema table cols; do
  aurora-dsql-loader load \
    --endpoint $DSQL_ENDPOINT \
    --source-uri backup.sql \
    --format pgdump \
    --schema "$schema" \
    --table "$table"
done
```

Pre-create each target table in DSQL with the same column set as the source
COPY clause. Order does not matter — the loader reorders columns by name to
match the dump. A column-set mismatch (extra or missing columns) is rejected
with a clear error before any rows are loaded.

### Migrate a full pg_dump into DSQL

```bash
# 1. Generate a FULL dump (no --data-only) so the DDL is included.
pg_dump -Fp mydb > mydb.sql

# 2. Migrate. Schema and data land in DSQL automatically.
aurora-dsql-loader migrate \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri mydb.sql
```

`migrate` is a four-stage pipeline that runs end-to-end on a single
connection pool:

1. **Extract** the embedded DDL out of the dump (everything between the
   `COPY ... FROM stdin;` blocks).
2. **Transform** via [`dsql-lint`](https://crates.io/crates/dsql-lint),
   which rewrites pg_dump output into DSQL-compatible DDL (e.g.
   `CREATE INDEX` → `CREATE INDEX ASYNC`). See the dsql-lint docs for
   the full rule set.
3. **Apply** the transformed DDL to the cluster, one statement per
   transaction (DSQL accepts only one DDL per txn). On re-run,
   already-created tables / indexes / constraints / columns are skipped
   by name so you can resume past a partial failure — schema-drift
   detection is on you, the skip matches identity, not definition.
4. **Load** each `COPY` block's data using the same connection pool —
   no IAM round-trips between stages.

Use `--dry-run` to preview the diagnostics + proposed DDL before
committing:

```bash
aurora-dsql-loader migrate \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri mydb.sql \
  --dry-run
```

The dry run is offline for `file://` sources; for `s3://`, AWS config
still loads to fetch the dump but DSQL itself is not contacted. Either
way you can review a dump from a workstation that doesn't have DSQL
access yet.

**Known limits:**

- Plain (`-Fp`) format only; `-Fc` / `-Fd` archives are rejected up
  front. `--inserts` (INSERT-format dumps) is not supported either —
  use the default `COPY FROM stdin` shape.
- After loading explicit PK values into an `IDENTITY` column, the
  identity counter is NOT auto-advanced. The migrate report includes a
  warning per converted column; reset it after the load if the table is
  pre-populated, e.g.:
  ```sql
  -- Look up the next value:
  SELECT max(id) + 1 FROM t;
  -- Then run, with <next> set to the value above:
  ALTER TABLE t ALTER COLUMN id RESTART WITH <next>;
  ```
- Statements `dsql-lint` cannot auto-fix surface as `Unfixable`
  diagnostics. The migrate flow refuses to apply DDL while any
  unfixable diagnostic exists (the operator has to edit the dump and
  re-run). The CLI prints each finding with a suggestion.
- A `SET DEFAULT nextval(...)` whose matching `CREATE SEQUENCE` lives
  in a different file (cross-file sequence reference) cannot be
  collapsed and will surface as `Unfixable` — drop it from the dump or
  inline the sequence.

**Halt-on-failure:** if any per-table load reports failed records (or
verify=count's post-count fails mid-run), `migrate` halts at that table
and exits non-zero. Without DSQL FK enforcement, continuing past a
partially-loaded parent would silently load child rows pointing at
missing parents. The persisted manifest path is printed for each failed
table so the operator can inspect the dropped chunks before re-running.

**Destructive-statement caveat:** `migrate` is intended for fresh /
empty DSQL clusters. `dsql-lint` does not flag `DROP TABLE`,
`DROP SCHEMA`, `DROP INDEX`, `DELETE`, or `UPDATE` statements; if your
dump contains them (typical with `pg_dump --clean`, which prepends
`DROP TABLE IF EXISTS …` to every CREATE), the migrate flow will
execute them against the target without confirmation. The
already-exists skip path makes a re-run safe for `CREATE` collisions
only — it does not prevent a `DROP` from destroying data on the
target. If you need to refresh into a populated cluster, drop the
`--clean` flag from `pg_dump` and either truncate the target tables
manually or migrate into a fresh cluster.

### Migrate from another DSQL cluster

`export` reads a DSQL cluster directly and writes a plain `.sql` file
(schema + data). Feed that file to `migrate` to load it into another
cluster:

```bash
# 1. Export the source cluster.
aurora-dsql-loader export \
  --endpoint source-cluster.dsql.us-east-1.on.aws \
  --output dsql.sql

# 2. (Optional) edit dsql.sql — change a column type, add a constraint, etc.

# 3. Migrate it into the destination cluster.
aurora-dsql-loader migrate \
  --endpoint dest-cluster.dsql.us-east-1.on.aws \
  --source-uri dsql.sql
```

Step 2 is optional but useful: since the export is plain SQL, you can
edit it before loading. This is the way to change something DSQL won't
let you alter in place — e.g. a column type or a constraint — by
exporting, editing, and migrating into a fresh cluster.

`export` covers tables, columns, `NOT NULL`, defaults, primary keys,
identity columns (the counter carries over), secondary indexes, and row
data. It does not export views, foreign keys, grants, or triggers. Tables
with generated columns or `CHECK` constraints are **rejected** rather than
silently exported without them — rewrite or drop those in the source (or
edit the dump by hand) before migrating.

Run `export` against a **quiesced source** (no concurrent writes): it reads
each table in a separate query rather than one cluster-wide snapshot, so a
write mid-export can produce an internally inconsistent dump. It also buffers
the whole dump in memory before writing, so it suits the one-time-migration
use case rather than continuously replicating a very large, actively-written
cluster.

**Options:** `--schema NAME` / `--table NAME` narrow the export to one
schema or table; omit `--output` to write to stdout.

### CSV/TSV header behavior

By default, the loader treats every row of a CSV or TSV file as data — it does
**not** skip the first row. This matches PostgreSQL `COPY FROM` (default
`HEADER false`), Redshift, Snowflake, and BigQuery defaults.

If your file has a header row, pass `--header` so it gets skipped:

```bash
aurora-dsql-loader load \
  --endpoint $DSQL_ENDPOINT \
  --source-uri sales_with_header.csv \
  --table sales \
  --header
```

If your file has no header row, no flag is needed:

```bash
aurora-dsql-loader load \
  --endpoint $DSQL_ENDPOINT \
  --source-uri sales_data_only.csv \
  --table sales
```

> **Migrating from 2.x:** the previous default was the opposite — the loader
> assumed a header row and silently dropped the first data row when one was
> missing. Add `--header` to any 2.x invocation that loaded a header-bearing
> file. See the [CHANGELOG](CHANGELOG.md) for full migration notes.

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

## Atomic Loads (all-or-nothing)

DSQL caps every transaction at 3,000 rows / 10 MiB / 5 min and has no `SAVEPOINT`, so a parallel bulk load can't be one atomic transaction. The one clean rollback is to drop a table the loader created this run, so `--atomic` covers exactly that case:

```bash
aurora-dsql-loader load \
  --endpoint your-cluster.dsql.us-east-1.on.aws \
  --source-uri data.csv \
  --table new_table \
  --if-not-exists \
  --atomic
```

On any load failure (an error, or one or more failed records) the loader drops the table it created and exits non-zero. If that cleanup DROP itself fails, the loader says so and names the table for you to drop manually. `--atomic` requires `--if-not-exists` and refuses to run if the table already exists or can't be verified absent (it will not drop pre-existing data—drop it yourself first). It can't be combined with `--resume-job-id`. Loading into an existing table atomically is not yet supported.

Rollback covers load failures only. A non-fatal `--verify` mismatch or row-count warning still exits non-zero but keeps the table so you can inspect it.

## Verification

After a load, `--verify` cross-checks what landed and prints one verdict per table. Pick the level by how much assurance you need vs. how much extra time you'll spend.

| `--verify` | What it confirms | Cost |
|------------|------------------|------|
| `off` | Nothing (load only). | None. |
| `count` | **No rows were lost.** Source row count = rows loaded + rows that failed, and the target table grew by exactly the number loaded. Also confirms the target's columns match the file and it has a primary/unique key. (csv/tsv have no exact source count, so the row-count check reports `SkippedNoExactSourceCount` — use pgdump or parquet for it.) | Two `COUNT(*)` per table. Negligible. |
| `full` | Everything `count` does, **plus every value landed correctly.** Each row is re-read from the source and compared to the target by primary key, letting the database decide equality (so `1.50` vs `1.5`, JSON key order, timestamp precision, etc. are not false alarms). | One extra full read of each table — roughly doubles load time. Opt-in. |

Default: `off` for `load`, `count` for `migrate`. Both `count` and `full` assume the loader is the sole writer to the target during the run.

**Verdicts** (anything but `Match`, `SkippedNoExactSourceCount`, or `ValueCheckSkipped` exits non-zero, so it fails CI/scripts):

- `Match` — clean.
- `LoaderDropped(N)` — rows lost before the database (parser/chunker issue).
- `MissingTarget(N)` / `ExtraTarget(N)` — target grew by less / more than loaded (concurrent writer?).
- `RowsConflictedAtTarget(N)` — N rows hit `ON CONFLICT` under `do-nothing`/`do-update` (re-run with `--verify=off` if that's an intended idempotent re-apply).
- `ValueMismatch(N)` / `ValueRowMissingAtTarget(N)` — *(`full` only)* N rows differ in value / are absent at the target; the report lists the offending primary keys.
- `SkippedNoExactSourceCount` — csv/tsv have no exact source count, so row-count checks can't run.
- `ValueCheckSkipped` — *(`full` only)* no single-column primary/unique key to align rows by, so the value check couldn't run (explicit, never a silent pass).

`full` verifies that the target faithfully reproduces what the loader read from your file — it catches load, transport, and type-coercion errors, but trusts the reader to have decoded the file correctly (it does not re-validate against the original live source).

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

See all options for each subcommand with:
```bash
aurora-dsql-loader load --help
aurora-dsql-loader migrate --help
aurora-dsql-loader export --help
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
