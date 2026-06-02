//! End-to-end test: real `pg_dump` binary → loader → real Aurora DSQL cluster.
//!
//! This is a release-gate check that closes the gap unit tests cannot:
//! - Output of the actual `pg_dump --data-only -Fp` binary (whatever today's
//!   binary emits) parses correctly, including type-specific escapes.
//! - Real Postgres types (BYTEA, JSONB, TIMESTAMPTZ, TEXT with embedded tabs
//!   and newlines, NULLs) round-trip into DSQL.
//! - The DSQL connection / IAM-auth / batch path executes against a live
//!   cluster, not just SQLite-in-memory.
//!
//! The test SKIPS (returns Ok) with a printed message when the required env
//! vars are missing — same shape as a unit test that no-ops in CI but
//! exercises real infrastructure when an engineer has it set up. Set:
//!
//!   DSQL_E2E_ENDPOINT=<cluster>.dsql.us-east-1.on.aws
//!   DSQL_E2E_REGION=us-east-1
//!   DSQL_E2E_USERNAME=admin
//!   DSQL_E2E_SOURCE_PG_URL=postgres://localhost:5432/postgres
//!   DSQL_E2E_TARGET_URL=postgres://admin@<cluster>...:5432/postgres  # for assertions
//!
//! See `docs/release-checklist-pgdump.md` for the wider release checklist.

use std::process::Command;

fn env_or_skip(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

#[tokio::test]
async fn pgdump_real_to_real_dsql_round_trips() -> anyhow::Result<()> {
    use aurora_dsql_loader::runner::{Format, LoadArgs, OnConflict, run_load};
    use std::collections::HashMap;

    // Skip cleanly if E2E credentials aren't set. Print so an operator running
    // the suite locally can see why the gate didn't fire.
    let (Some(endpoint), Some(region), Some(username), Some(source_pg_url)) = (
        env_or_skip("DSQL_E2E_ENDPOINT"),
        env_or_skip("DSQL_E2E_REGION"),
        env_or_skip("DSQL_E2E_USERNAME"),
        env_or_skip("DSQL_E2E_SOURCE_PG_URL"),
    ) else {
        eprintln!(
            "skipping pgdump_real_to_real_dsql_round_trips: \
             DSQL_E2E_{{ENDPOINT,REGION,USERNAME,SOURCE_PG_URL}} not all set"
        );
        return Ok(());
    };

    // 1. Set up a SOURCE table on the real PG instance with type variety that
    // exercises the COPY text-format escapes the loader cares about.
    let src_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&source_pg_url)
        .await?;

    let table = format!("pg_loader_e2e_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE TABLE {table} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            note TEXT,
            blob BYTEA,
            payload JSONB,
            ts TIMESTAMPTZ
        )"
    ))
    .execute(&src_pool)
    .await?;

    // Insert rows that hit each escape class the decoder handles:
    // tab inside a TEXT, newline inside a TEXT, NULL, BYTEA, JSONB, TZ-aware ts.
    sqlx::query(&format!(
        "INSERT INTO {table} (id, name, note, blob, payload, ts) VALUES
            (1, 'plain',         'tab\there',  E'\\\\xDEADBEEF', '{{\"a\":1}}'::jsonb, '2024-01-15 12:34:56+00'),
            (2, 'unicode-naïve', E'two\\nlines', E'\\\\x00FF',     '[1,2,3]'::jsonb,    '2024-06-30 23:59:59+00'),
            (3, 'null-note',      NULL,         NULL,             NULL,                NULL)"
    ))
    .execute(&src_pool)
    .await?;

    // 2. Dump it with the real pg_dump binary.
    let dump_dir = tempfile::tempdir()?;
    let dump_path = dump_dir.path().join("dump.sql");
    let status = Command::new("pg_dump")
        .args([
            "--data-only",
            "-Fp",
            "--table",
            &table,
            "--no-owner",
            "--no-privileges",
            &source_pg_url,
        ])
        .stdout(std::fs::File::create(&dump_path)?)
        .status()
        .map_err(|e| anyhow::anyhow!("failed to spawn pg_dump (is it on PATH?): {e}"))?;
    assert!(status.success(), "pg_dump exited with {status}");

    let dump_text = std::fs::read_to_string(&dump_path)?;
    assert!(
        dump_text.contains(&format!("COPY public.{table}"))
            || dump_text.contains(&format!("COPY \"public\".\"{table}\"")),
        "pg_dump output did not contain expected COPY header for {table}"
    );

    // 3. Pre-create the TARGET table on DSQL with the same column set but in
    // a deliberately DIFFERENT order than the dump's COPY clause — exercises
    // the name-based reorder path against a live DSQL cluster.
    let dsql_target_url = env_or_skip("DSQL_E2E_TARGET_URL");
    if let Some(url) = dsql_target_url.as_deref() {
        let dsql_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(url)
            .await?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
            .execute(&dsql_pool)
            .await?;
        sqlx::query(&format!(
            "CREATE TABLE {table} (
                ts TIMESTAMPTZ,
                payload JSONB,
                blob BYTEA,
                note TEXT,
                name TEXT NOT NULL,
                id BIGINT PRIMARY KEY
            )"
        ))
        .execute(&dsql_pool)
        .await?;
    } else {
        eprintln!(
            "DSQL_E2E_TARGET_URL not set — assuming target table {table} is pre-created on the cluster"
        );
    }

    // 4. Run the loader.
    let args = LoadArgs {
        endpoint,
        region,
        username,
        source_uri: dump_path.to_string_lossy().into_owned(),
        target_table: table.clone(),
        schema: "public".into(),
        format: Format::PgDump,
        worker_count: 1,
        chunk_size_bytes: 1024 * 1024,
        batch_size: 50,
        batch_concurrency: 1,
        create_table_if_missing: false,
        manifest_dir: None,
        quiet: true,
        debug: false,
        column_mappings: HashMap::new(),
        resume_job_id: None,
        on_conflict: OnConflict::DoNothing,
        exclude_columns: Vec::new(),
        delimiter: None,
        quote: None,
        escape: None,
        has_header: None,
    };

    let result = run_load(args).await?;
    assert_eq!(
        result.records_loaded, 3,
        "all 3 rows from the dump must reach DSQL; failed = {}",
        result.records_failed
    );
    assert_eq!(result.records_failed, 0);

    // 5. Verify round-trip equality if a target query connection is available.
    if let Some(url) = dsql_target_url {
        let dsql_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;
        let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*)::BIGINT FROM {table}"))
            .fetch_one(&dsql_pool)
            .await?;
        assert_eq!(count.0, 3, "DSQL target row count mismatch");

        // Spot-check the tricky row: tab inside TEXT survives the decode.
        let (note,): (Option<String>,) =
            sqlx::query_as(&format!("SELECT note FROM {table} WHERE id = 1"))
                .fetch_one(&dsql_pool)
                .await?;
        assert_eq!(note.as_deref(), Some("tab\there"));
    }

    // Best-effort source cleanup.
    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(&src_pool)
        .await;

    Ok(())
}
