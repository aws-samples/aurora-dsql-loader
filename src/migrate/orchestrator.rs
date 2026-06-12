//! Glue the migrate stages together: open dump → list COPY blocks →
//! extract DDL → transform via `dsql-lint` → apply DDL → load each
//! table — all against a single shared `Pool` so the production path
//! pays the IAM/connection-establishment cost only once.

use crate::coordination::MigrateProgress;
use crate::db::Pool;
use crate::db::pool::{PoolArgsBuilder, pool as build_dsql_pool};
use crate::formats::pgdump::{extract_ddl, list_copy_blocks};
use crate::io::{ByteReader, LocalFileByteReader, S3ByteReader, SourceUri};
use crate::migrate::apply::{AppliedStatement, apply_ddl};
use crate::migrate::transform::{Diagnostic, TransformResult, transform_ddl};
use crate::runner::{Format, LoadArgs, OnConflict, run_load_with_pool_for_pgdump_block};
use anyhow::{Context, Result};
use aws_config::{BehaviorVersion, Region};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Caller-supplied configuration for a migrate run. Holds the connection
/// info, the source URI of the pg_dump file, and the per-load knobs that
/// the orchestrator threads into each per-table [`LoadArgs`].
#[derive(Debug, Clone)]
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
    /// Forwarded to `LoadArgs.quiet` for log-level control.
    pub quiet: bool,
    /// Forwarded to `LoadArgs.debug` for log-level control.
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
pub struct TableLoadSummary {
    pub schema: String,
    pub table: String,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// Path to the persisted manifest dir when this table had failures.
    /// Mirrors `LoadResult.persisted_manifest_dir` — the operator needs
    /// it to inspect the failed-row chunks. `None` when the table loaded
    /// cleanly or when `--manifest-dir` was supplied (caller-owned).
    pub persisted_manifest_dir: Option<PathBuf>,
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
pub async fn run_migrate(args: MigrateArgs) -> Result<MigrateReport> {
    let (reader, aws_config) = open_source(&args).await?;

    let blocks = list_copy_blocks(&*reader)
        .await
        .context("Failed to scan pg_dump for COPY blocks")?;
    tracing::info!(
        copy_blocks = blocks.len(),
        "migrate: scanned pg_dump for COPY blocks"
    );

    let ddl = extract_ddl(&*reader, &blocks)
        .await
        .context("Failed to extract DDL from pg_dump")?;
    tracing::info!(ddl_bytes = ddl.len(), "migrate: extracted DDL");

    let TransformResult {
        fixed_sql,
        changes,
        unfixable,
    } = transform_ddl(&ddl);
    tracing::info!(
        changes = changes.len(),
        unfixable = unfixable.len(),
        "migrate: dsql-lint transform complete"
    );

    let warnings = collect_warnings(&changes);

    // Dry-run / unfixable short-circuit. We DO NOT apply DDL when there
    // are unfixable diagnostics — the operator must edit the dump first.
    // Crucially, both branches return BEFORE building a pool, so a
    // --dry-run review of an offline fixture works without IAM
    // credentials or cluster access. Pinned by tests::dry_run_does_not
    // _build_pool and tests::unfixable_does_not_build_pool.
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

    // Pool is built lazily here so dry-run / unfixable short-circuits
    // skip the DSQL IAM round-trip; for an offline (`file://`) fixture
    // the entire run stays cluster-free up to this point.
    let pool = build_pool(&args).await?;
    let ddl_applied = apply_ddl(&pool, &fixed_sql)
        .await
        .context("Failed to apply DDL to cluster")?;

    // None under `--quiet` skips the dump-wide bars; per-table silence
    // rides independently on `LoadArgs.quiet` set in `build_load_args`.
    let migrate_progress = if args.quiet {
        None
    } else {
        Some(MigrateProgress::new(&blocks))
    };

    // Halt on the first table with `records_failed > 0`. DSQL doesn't
    // enforce FKs, so continuing would silently load child rows against
    // a partially-loaded parent and the final report would look healthy.
    // The pre-resolved `blocks` and `aws_config` are threaded through so
    // each load skips the dump rescan + credential-chain walk.
    let mut tables = Vec::with_capacity(blocks.len());
    let mut halted = false;
    for block in &blocks {
        let load_args = build_load_args(&args, &block.table, &block.schema);
        let r = run_load_with_pool_for_pgdump_block(
            pool.clone(),
            load_args,
            block.clone(),
            aws_config.as_ref(),
            migrate_progress.as_ref().map(MigrateProgress::multi),
        )
        .await
        .with_context(|| {
            format!(
                "Failed to load table {schema}.{table}",
                schema = block.schema,
                table = block.table,
            )
        })?;
        let records_failed = r.records_failed;
        let block_bytes = block.data_end - block.data_start;
        tables.push(TableLoadSummary {
            schema: block.schema.clone(),
            table: block.table.clone(),
            records_loaded: r.records_loaded,
            records_failed,
            persisted_manifest_dir: r.persisted_manifest_dir,
        });
        if records_failed > 0 {
            if let Some(mp) = &migrate_progress {
                mp.finish_halted(&format!(
                    "{schema}.{table}: {n} rows failed",
                    schema = block.schema,
                    table = block.table,
                    n = records_failed,
                ));
            }
            halted = true;
            break;
        }
        if let Some(mp) = &migrate_progress {
            mp.record_table_loaded(block_bytes);
        }
    }

    if !halted && let Some(mp) = &migrate_progress {
        mp.finish_clean();
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
    let pool_args = PoolArgsBuilder::default()
        .region(&args.region)
        .endpoint(&args.endpoint)
        .username(&args.username)
        .build()?;
    build_dsql_pool(pool_args).await
}

/// Open the source dump as a `ByteReader` and return the
/// `aws_config::SdkConfig` (or `None` for `file://`) alongside it so the
/// per-table load loop can reuse the same config rather than rebuilding
/// it once per table. Migrate uses an explicit region (the cluster's),
/// unlike `list_pgdump_tables` which has no `--region` flag and falls
/// back to the AWS env default.
async fn open_source(
    args: &MigrateArgs,
) -> Result<(Arc<dyn ByteReader>, Option<aws_config::SdkConfig>)> {
    let parsed = SourceUri::parse(&args.source_uri)?;
    Ok(match parsed {
        SourceUri::Local(path) => (Arc::new(LocalFileByteReader::new(&path)), None),
        SourceUri::S3 { bucket, key } => {
            let aws_config = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(args.region.clone()))
                .load()
                .await;
            let s3 = Arc::new(aws_sdk_s3::Client::new(&aws_config));
            (
                Arc::new(S3ByteReader::new(s3, bucket, key)),
                Some(aws_config),
            )
        }
    })
}

/// Build the `LoadArgs` for a single COPY block. Forwards the migrate
/// knobs and fills in the pg_dump-specific defaults (no header, no
/// delimiter overrides, no column mappings — pg_dump COPY is positional
/// and the coordinator reorders the target schema by column name to
/// match the COPY clause; see `align_pgdump_schema_to_copy_columns` in
/// `coordination::coordinator`).
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

        // Standalone `ALTER TABLE ... ADD CONSTRAINT users_email_key
        // UNIQUE (email)` (the form pg_dump always emits even when the
        // source declared UNIQUE inline) folds back onto the CREATE
        // TABLE.
        assert!(
            report
                .ddl_changes
                .iter()
                .any(|d| d.rule == "alter_add_unique_collapse"),
            "ALTER ADD UNIQUE should fold into CREATE TABLE, got: {:?}",
            report.ddl_changes
        );

        // Standalone `ALTER TABLE ... ADD CONSTRAINT events_pkey PRIMARY
        // KEY (id)` (the form pg_dump always emits — PG stores PK
        // separately) folds back onto the CREATE TABLE. The fixture has
        // two such ALTERs (events_pkey, users_pkey), so we expect
        // exactly two collapse diagnostics.
        let pk_collapse = report
            .ddl_changes
            .iter()
            .filter(|d| d.rule == "alter_add_primary_key_collapse")
            .count();
        assert_eq!(
            pk_collapse, 2,
            "expected 2 alter_add_primary_key_collapse changes (events + users), got: {:?}",
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

    /// Symmetric guarantee to `dry_run_does_not_build_pool`: when
    /// `transform_ddl` surfaces an `unfixable` diagnostic, the orchestrator
    /// must short-circuit BEFORE building a pool. A future refactor that
    /// re-orders `build_pool` above the unfixable branch (the same class of
    /// bug already fixed for `dry_run`) would otherwise pass every other
    /// test silently.
    #[tokio::test]
    async fn unfixable_does_not_build_pool() {
        // Cross-file SET DEFAULT with no preceding sequence DECLARE in the
        // same input is the canonical unfixable diagnostic from dsql-lint.
        let dump = "\
ALTER TABLE public.events ALTER COLUMN id SET DEFAULT nextval('public.events_id_seq'::regclass);
COPY public.events (id) FROM stdin;
1
\\.
";
        let f = write_dump(dump);
        let args = MigrateArgs {
            endpoint: "doesnotexist.invalid".to_string(),
            region: "us-east-1".to_string(),
            username: "ignored".to_string(),
            source_uri: file_uri(f.path()),
            schema: "public".to_string(),
            dry_run: false,
            worker_count: 1,
            batch_size: 1,
            batch_concurrency: 1,
            chunk_size_bytes: 1024,
            on_conflict: OnConflict::Error,
            quiet: true,
            debug: false,
            // Critically: NO test_pool. If the orchestrator tried to build
            // a real pool against the bogus endpoint, this would fail.
            test_pool: None,
        };
        let report = run_migrate(args).await.unwrap();
        // Pin the specific rule, not just "non-empty unfixable" — if a
        // future dsql-lint reclassifies cross-file SET DEFAULT under a
        // different rule name (or worse, makes it fixable), the test
        // surfaces here rather than silently passing for an unrelated
        // reason.
        assert!(
            report
                .ddl_unfixable
                .iter()
                .any(|d| d.rule == "at_unsupported_alter_column_set_default"),
            "must surface at_unsupported_alter_column_set_default; got: {:?}",
            report.ddl_unfixable
        );
        assert!(report.ddl_applied.is_empty());
        assert!(report.tables.is_empty());
        // dry_run echoes the input flag (false here); the offline-ness is
        // independent of dry_run.
        assert!(!report.dry_run);
    }

    /// Pin `run_migrate`'s error propagation when `apply_ddl` fails on a
    /// statement after earlier statements have already landed. Surfaces:
    /// (1) the error bubbles out as `Err` with context tagging the failing
    /// SQL; (2) earlier statements remain applied (DSQL has no
    /// multi-statement DDL transaction); (3) no per-table load runs after a
    /// mid-apply failure. A future refactor that swallows the apply error
    /// or proceeds to the load stage would silently regress.
    #[tokio::test]
    async fn run_migrate_propagates_mid_apply_error() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        // The second statement parses cleanly (so dsql-lint does not flag
        // it as unfixable) but fails at execute time against SQLite —
        // INSERT into a non-existent table. The third statement must not
        // run because apply stops at the first non-recoverable error.
        let dump = "\
CREATE TABLE valid_first (id INTEGER);
INSERT INTO does_not_exist_at_apply VALUES (1);
CREATE TABLE never_runs (id INTEGER);
COPY public.valid_first (id) FROM stdin;
1
\\.
";
        let f = write_dump(dump);
        let args = args_with_pool(&file_uri(f.path()), pool.clone(), false);
        let err = run_migrate(args).await.unwrap_err();
        let chain = format!("{err:#}");
        assert!(
            chain.contains("does_not_exist_at_apply"),
            "error chain should identify the failing statement; got: {chain}"
        );

        let first_exists: i64 = pool
            .fetch_all_with_binds::<(i64,)>(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='valid_first'",
                &[],
            )
            .await
            .unwrap()[0]
            .0;
        assert_eq!(first_exists, 1, "first CREATE TABLE should have applied");
        let third_exists: i64 = pool
            .fetch_all_with_binds::<(i64,)>(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='never_runs'",
                &[],
            )
            .await
            .unwrap()[0]
            .0;
        assert_eq!(third_exists, 0, "third CREATE TABLE must not have applied");
        // No per-table load should have run after the apply failure.
        let row_count: i64 = pool
            .fetch_all_with_binds::<(i64,)>("SELECT COUNT(*) FROM valid_first", &[])
            .await
            .unwrap()[0]
            .0;
        assert_eq!(row_count, 0);
    }

    /// `quiet=false` smoke: captured stderr hides the render, so this
    /// only catches panic-class bar misuse (finish-before-pump-join,
    /// double-finish) plus pins the report shape.
    #[tokio::test]
    async fn run_migrate_quiet_false_drives_progress_lifecycle_without_panicking() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let dump = write_dump(
            "\
CREATE TABLE a (id INTEGER, x TEXT);
CREATE TABLE b (id INTEGER, x TEXT);
COPY public.a (id, x) FROM stdin;
1\tone
2\ttwo
\\.
COPY public.b (id, x) FROM stdin;
1\talpha
\\.
",
        );
        let mut args = args_with_pool(&file_uri(dump.path()), pool, false);
        args.quiet = false;

        let report = run_migrate(args).await.unwrap();
        assert_eq!(report.tables.len(), 2);
        assert_eq!(report.tables[0].records_loaded, 2);
        assert_eq!(report.tables[1].records_loaded, 1);
        for t in &report.tables {
            assert_eq!(t.records_failed, 0);
        }
    }

    /// Pins ordering: the per-table bar is finalized inside `run_load`
    /// before `records_failed` is observed by the orchestrator.
    #[tokio::test]
    async fn run_migrate_quiet_false_halts_on_records_failed_without_panicking() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        // Pre-create with PK, seed a row that the dump will conflict with.
        pool.execute_query("CREATE TABLE t (id INTEGER PRIMARY KEY, x TEXT)")
            .await
            .unwrap();
        pool.execute_query("INSERT INTO t VALUES (1, 'pre1')")
            .await
            .unwrap();

        let dump = write_dump(
            "\
COPY public.t (id, x) FROM stdin;
1\ta
\\.
COPY public.t (id, x) FROM stdin;
2\tb
\\.
",
        );
        let mut args = args_with_pool(&file_uri(dump.path()), pool, false);
        args.quiet = false;
        args.on_conflict = OnConflict::Error;

        let report = run_migrate(args).await.unwrap();
        assert_eq!(report.tables.len(), 1, "halt must skip the second block");
        assert!(report.tables[0].records_failed > 0);
    }
}
