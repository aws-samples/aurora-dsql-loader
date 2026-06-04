//! Glue the migrate stages together: open dump → list COPY blocks →
//! extract DDL → transform via `dsql-lint` → apply DDL → load each
//! table — all against a single shared `Pool` so the production path
//! pays the IAM/connection-establishment cost only once.

use crate::db::Pool;
use crate::formats::pgdump::{extract_ddl, list_copy_blocks};
use crate::io::{ByteReader, LocalFileByteReader, S3ByteReader, SourceUri};
use crate::migrate::apply::{AppliedStatement, apply_ddl};
use crate::migrate::transform::{Diagnostic, TransformResult, transform_ddl};
use crate::runner::{Format, LoadArgs, OnConflict, run_load_with_pool};
use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use std::collections::HashMap;
use std::sync::Arc;

/// Caller-supplied configuration for a migrate run. Holds the connection
/// info, the source URI of the pg_dump file, and the per-load knobs that
/// the orchestrator threads into each per-table [`LoadArgs`].
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MigrateArgs {
    /// DSQL endpoint (`<cluster>.dsql.<region>.on.aws`).
    pub endpoint: String,
    /// AWS region the cluster lives in (also used to fetch credentials).
    pub region: String,
    /// IAM principal name (`admin`, or a custom DB user pre-created by the
    /// operator).
    pub username: String,
    /// `s3://bucket/key` or `file:///path` of the pg_dump file. Plain-format
    /// only (`pg_dump -Fp`); `-Fc`/`-Fd` archives are rejected at scan time.
    pub source_uri: String,
    /// PostgreSQL schema the dump targets in DSQL; almost always `"public"`
    /// for migrations from a single-schema source.
    pub schema: String,
    /// Stop after `transform_ddl` and report what would have happened, do
    /// not call `apply_ddl` and do not load any data. Use for review of the
    /// diagnostics + the proposed DDL before committing.
    pub dry_run: bool,
    /// Number of worker tasks per table load (forwarded to `LoadArgs`).
    pub worker_count: usize,
    /// Forwarded to `LoadArgs.batch_size`.
    pub batch_size: usize,
    /// Forwarded to `LoadArgs.batch_concurrency`.
    pub batch_concurrency: usize,
    /// Forwarded to `LoadArgs.chunk_size_bytes`.
    pub chunk_size_bytes: u64,
    /// Forwarded to `LoadArgs.on_conflict`. Most migrations want
    /// `OnConflict::Error` — duplicate rows usually indicate a bug. The
    /// default mirrors what `run_load` accepts so this knob is opt-in.
    pub on_conflict: OnConflict,
    /// Forwarded to `LoadArgs.quiet` / `debug` for log-level control.
    pub quiet: bool,
    pub debug: bool,
    /// Test-only: caller-supplied `Pool` to short-circuit the DSQL IAM
    /// path. Mirrors `LoadArgs.test_pool` so SQLite-backed tests can
    /// drive the orchestrator end-to-end without a real cluster.
    #[cfg(test)]
    pub test_pool: Option<Pool>,
}

/// Final report from a migrate run. The CLI / library consumer prints
/// this as a summary table; tests assert on the structured fields.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MigrateReport {
    /// Statements that successfully ran against the cluster (or were
    /// skipped because the target already existed). Empty in `--dry-run`.
    pub ddl_applied: Vec<AppliedStatement>,
    /// Diagnostics where `dsql-lint` auto-applied a rewrite. Surfaced for
    /// transparency (the operator should know what was changed in their
    /// dump before it hit the cluster).
    pub ddl_changes: Vec<Diagnostic>,
    /// Diagnostics flagged but NOT auto-fixed. If non-empty the
    /// orchestrator never reaches the apply or load stages — the run
    /// short-circuits with this populated and `tables` empty.
    pub ddl_unfixable: Vec<Diagnostic>,
    /// Per-COPY-block load summary, in source order. Empty in
    /// `--dry-run` and when `ddl_unfixable` is non-empty.
    pub tables: Vec<TableLoadSummary>,
    /// Free-form warnings the operator should see (e.g. "identity counter
    /// not advanced — reset after load"). Populated independently of
    /// `ddl_changes` so callers don't have to re-walk the diagnostic list.
    pub warnings: Vec<String>,
    /// True when `MigrateArgs.dry_run` was set; lets callers print a
    /// "no changes applied" banner without inspecting `ddl_applied.len()`.
    pub dry_run: bool,
}

/// Outcome of loading one COPY block's data. Shape mirrors
/// [`LoadResult`](crate::runner::LoadResult) but flattens the fields the
/// migrate report cares about so callers don't carry the full type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct TableLoadSummary {
    pub schema: String,
    pub table: String,
    pub records_loaded: u64,
    pub records_failed: u64,
}

/// Drive the end-to-end migrate flow against a real (or test) cluster.
///
/// Stages, in order:
/// 1. open the dump as a `ByteReader` (local file or S3),
/// 2. list every `COPY ... FROM stdin;` block in source order,
/// 3. extract the non-data DDL between blocks,
/// 4. transform the DDL via `dsql_lint::fix_sql`,
/// 5. if any `unfixable` diagnostic surfaced — short-circuit and return
///    the report so the operator can edit the dump and re-run,
/// 6. otherwise apply the fixed DDL one statement at a time, and
/// 7. load each table's data via `run_load_with_pool` reusing the same
///    `Pool` the apply stage used.
///
/// When `args.dry_run` is set the function stops after step 4 (transform)
/// — the operator gets the proposed DDL and the diagnostic split, no
/// statements are sent to the cluster.
#[allow(dead_code)]
pub async fn run_migrate(args: MigrateArgs) -> Result<MigrateReport> {
    // 1. Open the dump.
    let reader = open_source(&args).await?;

    // 2. List COPY blocks.
    let blocks = list_copy_blocks(&*reader)
        .await
        .context("Failed to scan pg_dump for COPY blocks")?;

    // 3. Extract DDL.
    let ddl = extract_ddl(&*reader, &blocks)
        .await
        .context("Failed to extract DDL from pg_dump")?;

    // 4. Transform via dsql-lint.
    let TransformResult {
        fixed_sql,
        changes,
        unfixable,
    } = transform_ddl(&ddl);

    let warnings = collect_warnings(&changes);

    // Dry-run / unfixable short-circuit. We DO NOT apply DDL when there
    // are unfixable diagnostics — the operator must edit the dump first.
    // Crucially, both branches return BEFORE building a pool, so a
    // --dry-run review of an offline fixture works without IAM
    // credentials or cluster access.
    if args.dry_run || !unfixable.is_empty() {
        return Ok(MigrateReport {
            ddl_applied: Vec::new(),
            ddl_changes: changes,
            ddl_unfixable: unfixable,
            tables: Vec::new(),
            warnings,
            dry_run: args.dry_run,
        });
    }

    // 5/6. Apply DDL — pool is built lazily here so the steps above
    // can run offline.
    let pool = build_pool(&args).await?;
    let ddl_applied = apply_ddl(&pool, &fixed_sql)
        .await
        .context("Failed to apply DDL to cluster")?;

    // 7. Per-table load.
    let mut tables = Vec::with_capacity(blocks.len());
    for block in &blocks {
        let load_args = build_load_args(&args, &block.table, &block.schema);
        let r = run_load_with_pool(pool.clone(), load_args)
            .await
            .with_context(|| {
                format!(
                    "Failed to load table {schema}.{table}",
                    schema = block.schema,
                    table = block.table,
                )
            })?;
        tables.push(TableLoadSummary {
            schema: block.schema.clone(),
            table: block.table.clone(),
            records_loaded: r.records_loaded,
            records_failed: r.records_failed,
        });
    }

    Ok(MigrateReport {
        ddl_applied,
        ddl_changes: changes,
        ddl_unfixable: unfixable,
        tables,
        warnings,
        dry_run: args.dry_run,
    })
}

/// Build a `Pool` for migrate. In `#[cfg(test)]` builds, prefer
/// `args.test_pool` so SQLite-backed tests skip the IAM path; otherwise
/// fall through to the real DSQL connector. Mirrors `runner::build_pool`
/// shape so future pool-acquisition changes can be made in one place.
async fn build_pool(args: &MigrateArgs) -> Result<Pool> {
    #[cfg(test)]
    if let Some(p) = args.test_pool.clone() {
        return Ok(p);
    }
    use crate::db::pool::{PoolArgsBuilder, pool};
    let pool_args = PoolArgsBuilder::default()
        .region(&args.region)
        .endpoint(&args.endpoint)
        .username(&args.username)
        .build()?;
    pool(pool_args).await
}

/// Open the source dump as a `ByteReader`. Mirrors what
/// `runner::list_pgdump_tables` does so the two entry points see the same
/// inputs (and reject the same things — e.g. `s3://` without an AWS
/// config).
async fn open_source(args: &MigrateArgs) -> Result<Arc<dyn ByteReader>> {
    let parsed = SourceUri::parse(&args.source_uri)?;
    Ok(match parsed {
        SourceUri::Local(path) => Arc::new(LocalFileByteReader::new(&path)),
        SourceUri::S3 { bucket, key } => {
            let aws_config = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(args.region.clone()))
                .load()
                .await;
            let s3 = Arc::new(aws_sdk_s3::Client::new(&aws_config));
            Arc::new(S3ByteReader::new(s3, bucket, key))
        }
    })
}

/// Build the `LoadArgs` for a single COPY block. Forwards the migrate
/// knobs and fills in the pg_dump-specific defaults (no header, no
/// delimiter overrides, no column mappings — pg_dump COPY is positional
/// and the loader's pg_dump reader handles column reordering internally).
fn build_load_args(args: &MigrateArgs, table: &str, schema: &str) -> LoadArgs {
    LoadArgs {
        endpoint: args.endpoint.clone(),
        region: args.region.clone(),
        username: args.username.clone(),
        source_uri: args.source_uri.clone(),
        target_table: table.to_string(),
        schema: schema.to_string(),
        format: Format::PgDump,
        worker_count: args.worker_count,
        batch_size: args.batch_size,
        batch_concurrency: args.batch_concurrency,
        chunk_size_bytes: args.chunk_size_bytes,
        // The migrate flow runs apply_ddl right before this, so the table
        // already exists; the IF-NOT-EXISTS path is gated against pg_dump
        // anyway by validate_load_args.
        create_table_if_missing: false,
        column_mappings: HashMap::new(),
        manifest_dir: None,
        quiet: args.quiet,
        debug: args.debug,
        resume_job_id: None,
        on_conflict: args.on_conflict,
        exclude_columns: Vec::new(),
        delimiter: None,
        quote: None,
        escape: None,
        has_header: None,
        #[cfg(test)]
        test_pool: args.test_pool.clone(),
    }
}

/// Walk `changes` and pull out the fix-result detail strings as
/// human-readable warnings. The migrate report's `warnings` field is
/// shorter than `ddl_changes` because callers usually want a one-line
/// summary per warning, not the full structured diagnostic.
fn collect_warnings(changes: &[Diagnostic]) -> Vec<String> {
    changes
        .iter()
        .filter_map(|d| {
            d.fix_detail.as_ref().map(|detail| {
                format!(
                    "[{rule}] line {line}: {detail}",
                    rule = d.rule,
                    line = d.line
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::apply::ApplyOutcome;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Build a minimal `MigrateArgs` for tests: SQLite test pool, local
    /// `file://` source URI, conservative load knobs.
    fn args_with_pool(source_uri: &str, pool: Pool, dry_run: bool) -> MigrateArgs {
        MigrateArgs {
            endpoint: "test.dsql.us-west-2.on.aws".to_string(),
            region: "us-west-2".to_string(),
            username: "admin".to_string(),
            source_uri: source_uri.to_string(),
            schema: "public".to_string(),
            dry_run,
            worker_count: 1,
            batch_size: 100,
            batch_concurrency: 1,
            chunk_size_bytes: 1024 * 1024,
            on_conflict: OnConflict::Error,
            quiet: true,
            debug: false,
            test_pool: Some(pool),
        }
    }

    fn write_dump(contents: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn file_uri(path: &std::path::Path) -> String {
        format!("file://{}", path.display())
    }

    /// Dry run: transform happens, NO DDL is applied, NO tables are
    /// loaded. The report carries the proposed DDL changes and any
    /// warnings so the operator can review before committing.
    #[tokio::test]
    async fn dry_run_reports_changes_without_applying() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let dump = write_dump(
            "\
CREATE TABLE public.t (id integer NOT NULL, x text);
CREATE SEQUENCE public.t_id_seq AS integer START WITH 1 INCREMENT BY 1 CACHE 1;
ALTER SEQUENCE public.t_id_seq OWNED BY public.t.id;
ALTER TABLE ONLY public.t ALTER COLUMN id SET DEFAULT nextval('public.t_id_seq'::regclass);
COPY public.t (id, x) FROM stdin;
1\ta
\\.
",
        );
        let report = run_migrate(args_with_pool(&file_uri(dump.path()), pool, true))
            .await
            .unwrap();

        assert!(report.dry_run);
        assert!(report.ddl_applied.is_empty());
        assert!(report.tables.is_empty());
        assert_eq!(report.ddl_changes.len(), 1);
        assert_eq!(report.ddl_changes[0].rule, "serial_sequence_idiom");
        assert!(report.ddl_unfixable.is_empty());
    }

    /// Unfixable diagnostic short-circuits the run: NO DDL is applied,
    /// NO tables are loaded, the report flags the problem so the
    /// operator can edit the dump and re-run. Critical safety
    /// invariant — silently applying DDL with an unfixable diagnostic
    /// would let a partial migration land.
    #[tokio::test]
    async fn unfixable_diagnostic_short_circuits_apply_and_load() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let dump = write_dump(
            "\
CREATE TABLE public.t (id integer NOT NULL);
ALTER TABLE ONLY public.t ALTER COLUMN id SET DEFAULT nextval('other_db.s'::regclass);
COPY public.t (id) FROM stdin;
1
\\.
",
        );
        let report = run_migrate(args_with_pool(&file_uri(dump.path()), pool.clone(), false))
            .await
            .unwrap();

        assert!(
            !report.ddl_unfixable.is_empty(),
            "unfixable diagnostic must be reported"
        );
        assert!(
            report.ddl_applied.is_empty(),
            "no DDL should be applied when unfixable diagnostics exist"
        );
        assert!(
            report.tables.is_empty(),
            "no tables should be loaded when unfixable diagnostics exist"
        );

        // Confirm side-effect free: the table was NOT created.
        #[derive(sqlx::FromRow)]
        struct Row(i64);
        let rows = pool
            .fetch_all_with_binds::<Row>(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].0, 0);
    }

    /// Schema-only dump (no COPY blocks): DDL applies, no tables get
    /// loaded. Validates that the orchestrator handles the
    /// "blocks.is_empty()" edge case without panicking. The CREATE TABLE
    /// is unqualified so SQLite (used as the test pool) accepts it
    /// without a `public` database; pg_dump always emits the schema
    /// prefix in real output, but this test exercises the orchestrator
    /// glue, not the schema-rewriting behavior.
    #[tokio::test]
    async fn schema_only_dump_applies_ddl_loads_no_tables() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let dump = write_dump(
            "-- preamble\n\
             CREATE TABLE t (id INTEGER, x TEXT);\n",
        );
        let report = run_migrate(args_with_pool(&file_uri(dump.path()), pool.clone(), false))
            .await
            .unwrap();

        assert!(!report.dry_run);
        assert!(report.ddl_unfixable.is_empty());
        assert!(report.tables.is_empty(), "no COPY blocks → no loads");
        assert!(
            report
                .ddl_applied
                .iter()
                .any(|s| s.sql.to_uppercase().contains("CREATE TABLE")),
            "schema-only DDL should be applied: {:?}",
            report.ddl_applied
        );
        // Table actually exists in the pool.
        #[derive(sqlx::FromRow)]
        struct Row(i64);
        let rows = pool
            .fetch_all_with_binds::<Row>(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].0, 1);
    }

    /// Re-running migrate against a cluster that already has the target
    /// objects must NOT fail — the apply stage's already-exists skip
    /// keeps the flow idempotent. Critical for operators who hit a
    /// transient failure during the data load and re-run.
    #[tokio::test]
    async fn rerun_after_partial_apply_is_idempotent() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        // Pre-create the table the dump would CREATE.
        pool.execute_query("CREATE TABLE t (id INTEGER, x TEXT)")
            .await
            .unwrap();

        let dump = write_dump(
            "-- preamble\n\
             CREATE TABLE t (id INTEGER, x TEXT);\n",
        );
        let report = run_migrate(args_with_pool(&file_uri(dump.path()), pool.clone(), false))
            .await
            .unwrap();

        let create_outcomes: Vec<_> = report
            .ddl_applied
            .iter()
            .filter(|s| s.sql.to_uppercase().contains("CREATE TABLE"))
            .map(|s| s.outcome.clone())
            .collect();
        assert_eq!(
            create_outcomes,
            vec![ApplyOutcome::SkippedAlreadyExists],
            "pre-existing CREATE TABLE must be skipped, got: {:?}",
            report.ddl_applied
        );
    }

    /// `--dry-run` MUST NOT build a connection pool — the operator runs
    /// dry-run from a workstation that may not have IAM credentials or
    /// network access to the cluster yet. Pinning this so the orchestrator
    /// stays offline-friendly: setting `test_pool: None` and `dry_run:
    /// true` and pointing at a real fixture must succeed regardless of
    /// any DSQL endpoint validity.
    ///
    /// This is the regression test for the bug where `run_migrate`
    /// unconditionally called `build_pool` before the dry-run branch.
    #[tokio::test]
    async fn dry_run_does_not_build_pool() {
        let dump = write_dump(
            "-- preamble\nCREATE TABLE t (id integer NOT NULL);\nCOPY public.t (id) FROM stdin;\n1\n\\.\n",
        );
        let args = MigrateArgs {
            endpoint: "doesnotexist.invalid".to_string(),
            region: "us-east-1".to_string(),
            username: "ignored".to_string(),
            source_uri: file_uri(dump.path()),
            schema: "public".to_string(),
            dry_run: true,
            worker_count: 1,
            batch_size: 1,
            batch_concurrency: 1,
            chunk_size_bytes: 1024,
            on_conflict: OnConflict::Error,
            quiet: true,
            debug: false,
            // Critically: NO test_pool. If the orchestrator tried to
            // build a real pool against the bogus endpoint, this would
            // fail. It must stay offline.
            test_pool: None,
        };
        let report = run_migrate(args).await.unwrap();
        assert!(report.dry_run);
        assert!(report.ddl_applied.is_empty());
        assert!(report.tables.is_empty());
    }

    /// Offline smoke test: feed a hand-authored full-dump fixture
    /// through `run_migrate(..., dry_run=true)` and assert the
    /// orchestrator produces the diagnostic shape we promise in the CLI
    /// help / README. Covers the realistic shape pg_dump emits for a
    /// schema with SERIAL PK + FK + sync index + NOT NULL DEFAULT — the
    /// proof that all of dsql-lint's transforms compose end-to-end via
    /// the migrate flow without touching a cluster.
    #[tokio::test]
    async fn dry_run_full_dump_fixture_collapses_idioms_and_strips_fk() {
        let fixture = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/pgdump_full.sql");
        let args = MigrateArgs {
            endpoint: "doesnotexist.invalid".to_string(),
            region: "us-east-1".to_string(),
            username: "ignored".to_string(),
            source_uri: file_uri(&fixture),
            schema: "public".to_string(),
            dry_run: true,
            worker_count: 1,
            batch_size: 1,
            batch_concurrency: 1,
            chunk_size_bytes: 1024,
            on_conflict: OnConflict::Error,
            quiet: true,
            debug: false,
            test_pool: None,
        };
        let report = run_migrate(args).await.unwrap();

        // Both SERIAL columns collapse to inline identity. The fixture
        // has two SERIAL PKs (events.id and users.id), so we expect
        // exactly two `serial_sequence_idiom` change diagnostics.
        let serial = report
            .ddl_changes
            .iter()
            .filter(|d| d.rule == "serial_sequence_idiom")
            .count();
        assert_eq!(
            serial, 2,
            "expected 2 serial_sequence_idiom changes (events + users), got: {:?}",
            report.ddl_changes
        );

        // Foreign key auto-removed (DSQL has no FK enforcement).
        assert!(
            report.ddl_changes.iter().any(|d| d.rule == "foreign_key"),
            "FK should be reported as auto-removed, got: {:?}",
            report.ddl_changes
        );

        // Sync CREATE INDEX rewritten to ASYNC, USING clause stripped.
        assert!(
            report.ddl_changes.iter().any(|d| d.rule == "index_async"),
            "CREATE INDEX should be rewritten to ASYNC, got: {:?}",
            report.ddl_changes
        );

        // Critical: nothing unfixable. If a future dsql-lint upgrade
        // starts flagging something in this fixture, that's a real
        // signal — the migrate happy-path no longer covers all the
        // shapes we promise.
        assert!(
            report.ddl_unfixable.is_empty(),
            "fixture must dry-run cleanly with zero unfixable, got: {:?}",
            report.ddl_unfixable
        );

        // Warnings were collected from each change's fix_detail (one per
        // change). Assert there's at least one identity-counter warning
        // since that's the operator-action-required nudge for SERIAL.
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.to_lowercase().contains("identity counter")),
            "expected an 'identity counter' warning, got: {:?}",
            report.warnings
        );
    }
}
