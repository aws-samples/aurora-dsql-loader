//! L3 value verification: build the server-side recast-compare query and
//! diff the source (re-read, decoded TEXT) against the target by primary
//! key. The DB is the type authority — a source field is recast through the
//! column's *parameterized* type and compared null-safe (`IS NOT DISTINCT
//! FROM` on Postgres/DSQL, `IS` on SQLite), so the comparison reproduces the
//! column's assignment-time coercion: canonical forms (`1.5` vs `1.50`, `\N`
//! vs NULL) and typmod rounding (`numeric(10,2)` storing `1.005`→`1.01`)
//! match without a client normalizer. See [`column_types`] for why the type
//! name must carry its precision/scale.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{L3Details, L3Outcome, SchemaCheck};
use crate::db::Pool;
use crate::db::schema::{escape_sql_literal, query_table_schema, recast_literal};
use crate::formats::pgdump::{CopyBlock, PgDumpReader, find_copy_block};
use crate::formats::reader::{FileReader, Record};
use crate::io::{ByteReader, LocalFileByteReader, S3ByteReader, SourceUri};

/// Where + how the load-path verification locates and reads the source.
/// Bundled so `run_load_value_check` stays under the argument-count bar and
/// the `run_load` call site reads clearly.
pub(crate) struct LoadVerifyTarget<'a> {
    pub source_uri: &'a str,
    pub region: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
    pub is_pgdump: bool,
    /// Run the per-row L3 value check (mode=Full); schema check runs regardless.
    pub run_value_check: bool,
    pub chunk_size_bytes: u64,
}

/// Load-path verification entry: resolve the COPY block for
/// `(schema, table)` from the source URI, run the affirmative schema check
/// (always — the MUST #3 floor, same as migrate), and — when
/// `run_value_check` (mode=Full) — the per-row L3 value check. Used by
/// `run_load` where, unlike migrate, the block isn't pre-resolved.
///
/// Non-pgdump formats can't anchor a per-row compare or count COPY columns
/// here, so they report PK presence only and `L3Outcome::Skipped`.
pub(crate) async fn run_load_value_check(
    pool: &Pool,
    target: LoadVerifyTarget<'_>,
) -> Result<(SchemaCheck, L3Outcome, L3Details)> {
    let LoadVerifyTarget {
        source_uri,
        region,
        schema,
        table,
        is_pgdump,
        run_value_check,
        chunk_size_bytes,
    } = target;

    if !is_pgdump {
        // No COPY block to anchor a per-row compare or count columns; report
        // PK presence only and skip the value check.
        let pk_present = !pool
            .get_unique_constraint_columns(schema, table)
            .await
            .with_context(|| format!("verify: PK lookup for {schema}.{table}"))?
            .is_empty();
        let sc = SchemaCheck {
            columns_matched: None,
            pk_present,
        };
        return Ok((sc, L3Outcome::Skipped, L3Details::default()));
    }

    let parsed = SourceUri::parse(source_uri)?;
    let reader: Arc<dyn ByteReader> = match &parsed {
        SourceUri::Local(path) => Arc::new(LocalFileByteReader::new(path)),
        SourceUri::S3 { bucket, key } => {
            let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_config::Region::new(region.to_string()))
                .load()
                .await;
            let s3 = Arc::new(aws_sdk_s3::Client::new(&cfg));
            Arc::new(S3ByteReader::new(s3, bucket.clone(), key.clone()))
        }
    };
    let block = find_copy_block(&*reader, schema, table)
        .await
        .with_context(|| format!("verify: locating COPY block for {schema}.{table}"))?;

    let sc = schema_check(pool, &block).await?;
    let (outcome, details) = if run_value_check {
        verify_table_values(pool, reader, &block, chunk_size_bytes).await?
    } else {
        (L3Outcome::Skipped, L3Details::default())
    };
    Ok((sc, outcome, details))
}

/// Affirmative schema-conformance line (MUST #3): a successful load already
/// proves the COPY column set matched the target (otherwise
/// `align_pgdump_schema_to_copy_columns` would have bailed), so
/// `columns_matched` is just the COPY column count; `pk_present` reflects
/// whether the target has a primary/unique key. Catalog-only, runs under
/// both `Count` and `Full`.
pub(crate) async fn schema_check(pool: &Pool, block: &CopyBlock) -> Result<SchemaCheck> {
    let pk_present = !pool
        .get_unique_constraint_columns(&block.schema, &block.table)
        .await
        .with_context(|| format!("verify: PK lookup for {}.{}", block.schema, block.table))?
        .is_empty();
    Ok(SchemaCheck {
        columns_matched: Some(block.columns.len() as u64),
        pk_present,
    })
}

/// Target-fetch batch size: how many source PKs are checked per `IN (...)`
/// query. Bounds query size + memory to one batch.
const VALUE_CHECK_BATCH: usize = 1000;

/// Max offending PKs recorded per category in [`L3Details`] (the verdict
/// magnitude still carries the true total — this only caps the report).
const DETAIL_CAP: usize = 100;

/// Re-read the COPY block's rows and verify each landed at the target by
/// primary key, comparing every loaded column through the same server-side
/// `CAST` the worker used at INSERT. Returns the aggregate [`L3Outcome`]
/// plus the first-K offending PKs in [`L3Details`].
///
/// `L3Outcome::Skipped` (never a silent pass) when the table has no usable
/// single-column key (primary or unique) — the only key shape v1 verifies.
///
/// Reuses the live source `reader` (no re-open) and the existing pgdump
/// decode path; assumes the pgdump/migrate field mapping where
/// `record.fields[i]` corresponds to `block.columns[i]` and no columns are
/// excluded (the migrate path never excludes).
///
/// Assumes source PK values are unique (guaranteed for any real
/// single-column-PK dump). A hand-corrupted dump with duplicate PKs would
/// mis-count by the duplicate multiplicity but cannot panic.
///
/// Recast comparison assumes pg_dump's canonical COPY output: `timestamptz`
/// values carry an explicit UTC offset (so the recast is session-`timezone`
/// independent). A hand-edited dump with an *unqualified* timestamp for a
/// `timestamptz` column could false-mismatch if the load and verify
/// connections run under different session `timezone` — offset-bearing
/// literals (what pg_dump emits) are immune.
pub(crate) async fn verify_table_values(
    pool: &Pool,
    reader: Arc<dyn ByteReader>,
    block: &CopyBlock,
    chunk_size_bytes: u64,
) -> Result<(L3Outcome, L3Details)> {
    let pk_cols = pool
        .get_unique_constraint_columns(&block.schema, &block.table)
        .await
        .with_context(|| {
            format!(
                "verify=full: PK lookup for {}.{}",
                block.schema, block.table
            )
        })?;
    // v1 verifies a single-column key (primary or unique) only; composite /
    // no-key → explicit skip.
    let [pk_col] = pk_cols.as_slice() else {
        return Ok((L3Outcome::Skipped, L3Details::default()));
    };
    let Some(pk_idx) = block.columns.iter().position(|c| c == pk_col) else {
        // PK column isn't in the COPY set (can't align positionally) → skip.
        return Ok((L3Outcome::Skipped, L3Details::default()));
    };

    let col_types = column_types(pool, &block.schema, &block.table).await?;
    // Every COPY column must resolve to a type name for the recast compare.
    let types: Vec<String> = block
        .columns
        .iter()
        .map(|c| {
            col_types
                .get(c)
                .cloned()
                .with_context(|| format!("verify=full: no type for column {c}"))
        })
        .collect::<Result<_>>()?;

    let records = read_source_records(reader, block, chunk_size_bytes).await?;

    // Fail-closed floor: a non-empty COPY payload that re-reads to zero rows
    // is a decode/short-read failure, not a clean table. Without this guard it
    // would fall through to `Ran{0,0}` → `Match` (L1's counts are frozen at
    // load time and can't see the re-read), silently passing the exact
    // fidelity gap L3 exists to catch.
    if records.is_empty() && block.data_end > block.data_start {
        anyhow::bail!(
            "verify=full: re-read of {}.{} decoded 0 rows from a non-empty COPY block \
             ({} bytes) — possible short read or decode failure; not reporting Match",
            block.schema,
            block.table,
            block.data_end - block.data_start,
        );
    }

    let qualified = pool.qualified_table_name(&block.schema, &block.table);
    let is_postgres = pool.is_postgres();

    let mut value_mismatches = 0u64;
    let mut rows_missing_at_target = 0u64;
    let mut details = L3Details::default();

    for batch in records.chunks(VALUE_CHECK_BATCH) {
        let rows = batch
            .iter()
            .map(|rec| source_row(rec, block, pk_idx, &types))
            .collect::<Result<Vec<_>>>()?;
        let requested: Vec<&str> = rows.iter().map(|r| r.pk_value).collect();

        let sql = build_batch_query(&qualified, pk_col, &types[pk_idx], &rows, is_postgres);
        let returned: Vec<(String, String)> = pool
            .fetch_all_with_binds(&sql, &[])
            .await
            .with_context(|| format!("verify=full: compare query for {qualified}"))?;

        let returned_map: HashMap<&str, &str> = returned
            .iter()
            .map(|(pk, m)| (pk.as_str(), m.as_str()))
            .collect();
        for pk in requested {
            match returned_map.get(pk) {
                None => {
                    rows_missing_at_target += 1;
                    push_capped(&mut details.missing_pks, pk);
                }
                Some(m) if !is_match(m) => {
                    value_mismatches += 1;
                    push_capped(&mut details.mismatch_pks, pk);
                }
                Some(_) => {}
            }
        }
    }

    Ok((
        L3Outcome::Ran {
            value_mismatches,
            rows_missing_at_target,
        },
        details,
    ))
}

/// Append `pk` to a detail list unless it's already at [`DETAIL_CAP`].
fn push_capped(list: &mut Vec<String>, pk: &str) {
    if list.len() < DETAIL_CAP {
        list.push(pk.to_string());
    }
}

/// `true` only when the projected match-bool text is the backend's true
/// form: `CAST(bool AS TEXT)` yields `true` on Postgres/DSQL, `1` on SQLite.
/// Fail-closed: anything else — the false form, or any unexpected token —
/// counts as a mismatch, so a verification tool never silently passes a row
/// it couldn't positively confirm matched.
fn is_match(matches_text: &str) -> bool {
    matches!(matches_text, "true" | "1")
}

/// name → the column's fully-parameterized type name for every column of the
/// target table, e.g. `numeric(10,2)`, `timestamp(0) without time zone`.
/// Verify recasts each source value through this *exact* type so the compare
/// reproduces the column's assignment-time coercion: a `numeric(10,2)` column
/// rounds `'1.005'`→`1.01` on load, and recasting through the bare `NUMERIC`
/// (no scale) keeps `1.005`, false-mismatching a faithful load. The
/// parameterized recast rounds identically. (Verified on a live DSQL cluster.)
///
/// `pg_catalog.format_type(atttypid, atttypmod)` yields the parameterized
/// spelling in one castable string. SQLite (tests) has no such catalog, so it
/// falls back to the inferred [`SqlType::to_postgres`] name — SQLite's type
/// affinity makes the recast forgiving and tests don't exercise typmod.
async fn column_types(pool: &Pool, schema: &str, table: &str) -> Result<HashMap<String, String>> {
    if pool.is_postgres() {
        let sql = "SELECT a.attname, format_type(a.atttypid, a.atttypmod) \
                   FROM pg_attribute a \
                   JOIN pg_class c ON c.oid = a.attrelid \
                   JOIN pg_namespace n ON n.oid = c.relnamespace \
                   WHERE n.nspname = $1 AND c.relname = $2 \
                     AND a.attnum > 0 AND NOT a.attisdropped";
        let rows: Vec<(String, String)> = pool
            .fetch_all_with_binds(sql, &[schema, table])
            .await
            .with_context(|| format!("verify=full: type lookup for {schema}.{table}"))?;
        return Ok(rows.into_iter().collect());
    }
    // SQLite test path: no pg_catalog; use the inferred type names.
    let resolved = query_table_schema(pool, schema, table).await?;
    Ok(resolved
        .columns
        .into_iter()
        .map(|c| (c.name, c.sql_type.to_postgres().to_string()))
        .collect())
}

/// Re-read every record from the COPY block using the existing pgdump
/// decode path (no re-open: `reader` is the already-open source).
async fn read_source_records(
    reader: Arc<dyn ByteReader>,
    block: &CopyBlock,
    chunk_size_bytes: u64,
) -> Result<Vec<Record>> {
    let pgdump = PgDumpReader::from_block(reader, block.clone());
    let chunks = pgdump.create_chunks(chunk_size_bytes).await?;
    let mut records = Vec::new();
    for chunk in &chunks {
        records.extend(pgdump.read_chunk(chunk).await?.records);
    }
    Ok(records)
}

/// Borrow a decoded `Record` into a `SourceRow`: the PK field value plus
/// every non-PK column's (name, type, value). Errors if the PK field is
/// SQL NULL (a NULL PK can't anchor the row-alignment).
fn source_row<'a>(
    rec: &'a Record,
    block: &'a CopyBlock,
    pk_idx: usize,
    types: &'a [String],
) -> Result<SourceRow<'a>> {
    let pk_value = rec.fields[pk_idx].as_deref().with_context(|| {
        format!(
            "verify=full: NULL primary key in source row of {}",
            block.table
        )
    })?;
    let columns = block
        .columns
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != pk_idx)
        .map(|(i, name)| (name.as_str(), types[i].as_str(), rec.fields[i].as_deref()))
        .collect();
    Ok(SourceRow { pk_value, columns })
}

/// Null-safe-equality operator for the backend: `IS NOT DISTINCT FROM` on
/// Postgres/DSQL, `IS` on SQLite (which lacks the former but gives `IS` the
/// same null-safe semantics).
fn null_safe_eq(is_postgres: bool) -> &'static str {
    if is_postgres {
        "IS NOT DISTINCT FROM"
    } else {
        "IS"
    }
}

/// `true` for a type name that must be compared as text rather than recast
/// natively: `json` has no `=` operator (a direct `IS NOT DISTINCT FROM`
/// against a `CAST(.. AS json)` errors), but it stores the verbatim source
/// COPY text, so `CAST("col" AS TEXT)` vs the bare source text is exact and
/// equality-capable. `jsonb` is deliberately excluded: it *has* `=` and
/// canonicalizes input (key order / whitespace), so it recasts natively (a
/// text compare would false-mismatch on the canonicalization).
///
/// Case-insensitive: the name may be the worker's `to_postgres()` form
/// (`JSON`) or the catalog's `format_type` form (`json`).
fn compares_as_text(pg_type: &str) -> bool {
    pg_type.eq_ignore_ascii_case("json")
}

/// Per-column match predicate: `None` (source `\N`) → `"col" IS NULL`;
/// `Some(v)` → `"col" <nseq> <recast literal>`. `col` is double-quoted.
/// `ty` is the column's parameterized type name — `format_type(atttypid,
/// atttypmod)` on Postgres/DSQL, `to_postgres()` on the SQLite test path;
/// `json` takes the text-compare path (see [`compares_as_text`]), everything
/// else recasts.
fn column_predicate(col: &str, ty: &str, value: Option<&str>, is_postgres: bool) -> String {
    let quoted = format!("\"{col}\"");
    let nseq = null_safe_eq(is_postgres);
    match value {
        None => format!("{quoted} IS NULL"),
        Some(v) if compares_as_text(ty) => {
            let lit = format!("'{}'", escape_sql_literal(v));
            format!("CAST({quoted} AS TEXT) {nseq} {lit}")
        }
        Some(v) => format!("{quoted} {nseq} {}", recast_literal(ty, v)),
    }
}

/// One source row to verify: its primary-key text value plus the
/// (column, type, value) tuples for every non-PK loaded column.
pub(crate) struct SourceRow<'a> {
    pub pk_value: &'a str,
    pub columns: Vec<(&'a str, &'a str, Option<&'a str>)>,
}

/// Build the projected-bool batch query: for each `SourceRow`, return
/// `(source_pk_text, matches)` where `matches` ANDs every column predicate.
/// A requested PK absent from the result set is a missing row (the caller
/// diffs requested vs returned PKs).
///
/// The first column echoes back the row's **verbatim source PK text**, not
/// `CAST(pk AS TEXT)`. The target canonicalizes on cast (`numeric` `1.5`→
/// `1.50`, `uuid` upper→lower), so projecting the target's text and matching
/// it against the raw source text would false-miss every canonicalized PK and
/// report `ValueRowMissingAtTarget` on a faithful load. Echoing the source
/// text via a CASE that shares the recast `IS NOT DISTINCT FROM` arms keeps
/// the row-match server-side (canonicalization-correct) while the caller's
/// diff compares like-for-like.
///
/// The `WHERE pk IN (...)` list and the per-row `CASE WHEN pk <nseq> ...`
/// arms are built from the SAME recast PK literals, so the batch is exact
/// regardless of PK type. `IN` of the exact PKs (not `pk BETWEEN lo AND hi`)
/// avoids needing the caller to sort PKs in the DB's type order — a lexical
/// sort of integer/uuid text would mis-window a range.
fn build_batch_query(
    qualified_table: &str,
    pk_col: &str,
    pk_type: &str,
    rows: &[SourceRow<'_>],
    is_postgres: bool,
) -> String {
    let pk_lit = |v: &str| recast_literal(pk_type, v);
    let quoted_pk = format!("\"{pk_col}\"");
    let nseq = null_safe_eq(is_postgres);

    // Two CASE expressions over the same `WHEN pk <nseq> <recast literal>`
    // arms: one echoes the verbatim source PK text (the map key), the other
    // ANDs the row's column predicates (the match bool).
    let key_arms: String = rows
        .iter()
        .map(|row| {
            format!(
                "      WHEN {quoted_pk} {nseq} {} THEN '{}'",
                pk_lit(row.pk_value),
                escape_sql_literal(row.pk_value),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let match_arms: String = rows
        .iter()
        .map(|row| {
            let row_match = if row.columns.is_empty() {
                "TRUE".to_string()
            } else {
                row.columns
                    .iter()
                    .map(|(col, ty, val)| column_predicate(col, ty, *val, is_postgres))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            };
            format!(
                "      WHEN {quoted_pk} {nseq} {} THEN ({row_match})",
                pk_lit(row.pk_value)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let in_list = rows
        .iter()
        .map(|row| pk_lit(row.pk_value))
        .collect::<Vec<_>>()
        .join(", ");

    // The match bool is wrapped in CAST(... AS TEXT) so it decodes uniformly —
    // Postgres yields `true`/`false`, SQLite yields `1`/`0` (see `is_match`).
    format!(
        "SELECT CASE\n{key_arms}\n    END, CAST(CASE\n{match_arms}\n    END AS TEXT)\n  \
         FROM {qualified_table}\n  WHERE {quoted_pk} IN ({in_list})"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::pgdump::list_copy_blocks;
    use crate::io::LocalFileByteReader;
    use std::io::Write;

    /// Write a one-table pgdump COPY block to a temp file and resolve its
    /// `CopyBlock` — the migrate orchestrator's per-table input.
    async fn fixture_block(
        copy_header: &str,
        rows: &[&str],
    ) -> (tempfile::NamedTempFile, CopyBlock) {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "{copy_header}").unwrap();
        for r in rows {
            writeln!(f, "{r}").unwrap();
        }
        writeln!(f, "\\.").unwrap();
        f.flush().unwrap();
        let reader = LocalFileByteReader::new(f.path());
        let block = list_copy_blocks(&reader).await.unwrap().pop().unwrap();
        (f, block)
    }

    fn reader_for(f: &tempfile::NamedTempFile) -> Arc<dyn ByteReader> {
        Arc::new(LocalFileByteReader::new(f.path()))
    }

    #[tokio::test]
    async fn l3_clean_load_is_ran_zero() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, amt NUMERIC, note TEXT)")
            .await
            .unwrap();
        // Stored exactly what the source says (1.50 vs source 1.5 must still match).
        pool.execute_query("INSERT INTO things VALUES (1, 1.50, 'a'), (2, NULL, 'b')")
            .await
            .unwrap();

        let (f, block) = fixture_block(
            "COPY public.things (id, amt, note) FROM stdin;",
            &["1\t1.5\ta", "2\t\\N\tb"],
        )
        .await;

        let (out, _details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 0,
                rows_missing_at_target: 0
            }
        );
    }

    #[tokio::test]
    async fn l3_json_column_compares_as_text_no_equality_error() {
        // `json` has no `=` operator, so the recast compare must route json
        // through CAST(col AS TEXT): a clean load must Match (not error), and
        // a corrupted json value must surface as a mismatch.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE evts (id INTEGER PRIMARY KEY, payload JSON)")
            .await
            .unwrap();
        pool.execute_query("INSERT INTO evts VALUES (1, '{\"k\":\"v\"}'), (2, '[1,2,3]')")
            .await
            .unwrap();

        let (f, block) = fixture_block(
            "COPY public.evts (id, payload) FROM stdin;",
            &["1\t{\"k\":\"v\"}", "2\t[1,2,3]"],
        )
        .await;
        let (out, _d) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 0,
                rows_missing_at_target: 0
            },
            "clean json load must Match without an equality-operator error"
        );

        // Corrupt row 2's json at the target → mismatch on that PK.
        pool.execute_query("UPDATE evts SET payload = '[9,9,9]' WHERE id = 2")
            .await
            .unwrap();
        let (out, details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 1,
                rows_missing_at_target: 0
            }
        );
        assert_eq!(details.mismatch_pks, vec!["2".to_string()]);
    }

    #[tokio::test]
    async fn l3_value_mismatch_counted() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        // Target row 2 holds the wrong note vs the source.
        pool.execute_query("INSERT INTO things VALUES (1, 'a'), (2, 'WRONG')")
            .await
            .unwrap();

        let (f, block) = fixture_block(
            "COPY public.things (id, note) FROM stdin;",
            &["1\ta", "2\tb"],
        )
        .await;

        let (out, details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 1,
                rows_missing_at_target: 0
            }
        );
        // The offending PK is localized for the operator.
        assert_eq!(details.mismatch_pks, vec!["2".to_string()]);
        assert!(details.missing_pks.is_empty());
    }

    #[tokio::test]
    async fn l3_missing_row_counted() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        // Row 2 never landed at the target.
        pool.execute_query("INSERT INTO things VALUES (1, 'a')")
            .await
            .unwrap();

        let (f, block) = fixture_block(
            "COPY public.things (id, note) FROM stdin;",
            &["1\ta", "2\tb"],
        )
        .await;

        let (out, details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 0,
                rows_missing_at_target: 1
            }
        );
        assert_eq!(details.missing_pks, vec!["2".to_string()]);
        assert!(details.mismatch_pks.is_empty());
    }

    #[tokio::test]
    async fn l3_no_primary_key_is_skipped() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER, note TEXT)")
            .await
            .unwrap();
        pool.execute_query("INSERT INTO things VALUES (1, 'a')")
            .await
            .unwrap();

        let (f, block) =
            fixture_block("COPY public.things (id, note) FROM stdin;", &["1\ta"]).await;

        let (out, _details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(out, L3Outcome::Skipped);
    }

    #[tokio::test]
    async fn l3_pk_not_in_copy_set_is_skipped() {
        // Target has a PK (`id`), but the COPY block doesn't include it — the
        // row can't be aligned positionally, so L3 must Skip (never a silent
        // pass), distinct from the no-PK case.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        pool.execute_query("INSERT INTO things VALUES (1, 'a')")
            .await
            .unwrap();

        // COPY clause omits the PK column `id`.
        let (f, block) = fixture_block("COPY public.things (note) FROM stdin;", &["a"]).await;

        let (out, details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(out, L3Outcome::Skipped);
        assert_eq!(details, L3Details::default());
    }

    #[tokio::test]
    async fn l3_empty_block_does_not_trip_zero_row_floor() {
        // A genuinely empty COPY block (header then `\.`, no data) has
        // data_end == data_start, so the fail-closed floor must NOT fire — it
        // reports a clean Ran{0,0}, not an error.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        let (f, block) = fixture_block("COPY public.things (id, note) FROM stdin;", &[]).await;
        assert_eq!(
            block.data_end, block.data_start,
            "empty block must have a zero-length payload"
        );

        let (out, _details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 0,
                rows_missing_at_target: 0
            }
        );
    }

    #[tokio::test]
    async fn run_load_value_check_non_pgdump_skips_with_uncounted_columns() {
        // csv/parquet under --verify=full: no COPY block to anchor a per-row
        // compare or count columns → report PK presence only, columns
        // uncounted (None, NOT Some(0)), and L3 Skipped (never a silent
        // Match). The early return must not touch the source URI.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();

        let (sc, outcome, details) = run_load_value_check(
            &pool,
            LoadVerifyTarget {
                source_uri: "s3://unused/for-non-pgdump",
                region: "us-west-2",
                schema: "public",
                table: "things",
                is_pgdump: false,
                run_value_check: true,
                chunk_size_bytes: 1 << 20,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            sc,
            SchemaCheck {
                columns_matched: None,
                pk_present: true,
            }
        );
        assert_eq!(outcome, L3Outcome::Skipped);
        assert_eq!(details, L3Details::default());
    }

    #[tokio::test]
    async fn l3_composite_primary_key_is_skipped() {
        // v1 aligns rows on a single-column key only; a composite PK must
        // Skip (explicit "not checked"), never silently verify on the first
        // column. A regression to `pk_cols.first()` would mis-align rows.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE parts (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b))")
            .await
            .unwrap();
        pool.execute_query("INSERT INTO parts VALUES (1, 2, 'x')")
            .await
            .unwrap();

        let (f, block) =
            fixture_block("COPY public.parts (a, b, v) FROM stdin;", &["1\t2\tx"]).await;

        let (out, _details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(out, L3Outcome::Skipped);
    }

    #[tokio::test]
    async fn l3_boolean_column_mismatch_counted() {
        // BOOLEAN is where is_match's backend divergence bites: the match
        // bool serializes as 0/1 (SQLite) vs false/true (PG). Pin that a
        // real boolean-column mismatch is caught, not silently passed.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE flags (id INTEGER PRIMARY KEY, ok BOOLEAN)")
            .await
            .unwrap();
        // Row 1 stored true (matches source 't'); row 2 stored false but
        // source says true → mismatch.
        pool.execute_query("INSERT INTO flags VALUES (1, 1), (2, 0)")
            .await
            .unwrap();

        let (f, block) =
            fixture_block("COPY public.flags (id, ok) FROM stdin;", &["1\tt", "2\tt"]).await;

        let (out, _details) = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 1,
                rows_missing_at_target: 0
            }
        );
    }

    #[tokio::test]
    async fn l3_null_primary_key_in_source_errors() {
        // A NULL single-column PK can't anchor row-alignment — it signals a
        // corrupt dump and must surface as an error, not a silent skip.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        pool.execute_query("INSERT INTO things VALUES (1, 'a')")
            .await
            .unwrap();

        let (f, block) =
            fixture_block("COPY public.things (id, note) FROM stdin;", &["\\N\ta"]).await;

        let err = verify_table_values(&pool, reader_for(&f), &block, 1 << 20)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("NULL primary key"),
            "expected NULL-PK error, got: {err:#}"
        );
    }

    #[tokio::test]
    async fn l3_batches_across_multiple_queries() {
        // More rows than VALUE_CHECK_BATCH would be ideal, but that's slow;
        // instead force a tiny chunk size so the re-read spans many chunks
        // and assert the per-chunk records all aggregate into one verdict.
        let pool = Pool::sqlite_in_memory().await.unwrap();
        pool.execute_query("CREATE TABLE things (id INTEGER PRIMARY KEY, note TEXT)")
            .await
            .unwrap();
        let mut insert = String::from("INSERT INTO things VALUES ");
        let mut rows = Vec::new();
        for i in 0..50 {
            if i > 0 {
                insert.push(',');
            }
            // Row 7 is wrong at the target; the rest match.
            let stored = if i == 7 { "X" } else { "ok" };
            insert.push_str(&format!("({i}, '{stored}')"));
            rows.push(format!("{i}\tok"));
        }
        pool.execute_query(&insert).await.unwrap();
        let row_refs: Vec<&str> = rows.iter().map(String::as_str).collect();

        let (f, block) =
            fixture_block("COPY public.things (id, note) FROM stdin;", &row_refs).await;

        // Tiny chunk → many chunks during re-read (exercises the loop).
        let (out, _details) = verify_table_values(&pool, reader_for(&f), &block, 16)
            .await
            .unwrap();
        assert_eq!(
            out,
            L3Outcome::Ran {
                value_mismatches: 1,
                rows_missing_at_target: 0
            }
        );
    }

    #[test]
    fn is_match_is_fail_closed() {
        // True forms: `true` (Postgres/DSQL `CAST(bool AS TEXT)`), `1` (SQLite).
        assert!(is_match("true"));
        assert!(is_match("1"));
        // False forms and any unexpected token are NOT a match — a value the
        // tool can't positively confirm must count as a mismatch, never a
        // silent pass.
        assert!(!is_match("false"));
        assert!(!is_match("0"));
        assert!(!is_match(""));
        assert!(!is_match("t"));
        assert!(!is_match("unexpected"));
    }

    #[test]
    fn predicate_null_source_is_is_null() {
        let p = column_predicate("note", "TEXT", None, true);
        assert_eq!(p, "\"note\" IS NULL");
    }

    #[test]
    fn predicate_text_column_no_cast() {
        // TEXT family inserts bare, so verify compares bare.
        let p = column_predicate("name", "TEXT", Some("alice"), true);
        assert_eq!(p, "\"name\" IS NOT DISTINCT FROM 'alice'");
    }

    #[test]
    fn predicate_numeric_column_casts() {
        let p = column_predicate("amount", "NUMERIC", Some("1.5"), true);
        assert_eq!(p, "\"amount\" IS NOT DISTINCT FROM CAST('1.5' AS NUMERIC)");
    }

    #[test]
    fn predicate_sqlite_uses_is() {
        let p = column_predicate("amount", "NUMERIC", Some("1.5"), false);
        assert_eq!(p, "\"amount\" IS CAST('1.5' AS NUMERIC)");
    }

    #[test]
    fn predicate_escapes_single_quotes() {
        let p = column_predicate("note", "TEXT", Some("o'brien"), true);
        assert_eq!(p, "\"note\" IS NOT DISTINCT FROM 'o''brien'");
    }

    #[test]
    fn predicate_timestamptz_uses_full_type_name() {
        let p = column_predicate("ts", "TIMESTAMP WITH TIME ZONE", Some("2024-01-01Z"), true);
        assert_eq!(
            p,
            "\"ts\" IS NOT DISTINCT FROM CAST('2024-01-01Z' AS TIMESTAMP WITH TIME ZONE)"
        );
    }

    #[test]
    fn predicate_json_compares_column_as_text() {
        // json has no `=`; compare CAST(col AS TEXT) against the bare source
        // text instead of CAST('..' AS JSON) IS NOT DISTINCT FROM (which errors).
        let p = column_predicate("payload", "JSON", Some("{\"k\":\"v\"}"), true);
        assert_eq!(
            p,
            "CAST(\"payload\" AS TEXT) IS NOT DISTINCT FROM '{\"k\":\"v\"}'"
        );
    }

    #[test]
    fn predicate_jsonb_compares_natively_not_as_text() {
        // jsonb HAS `=` and canonicalizes input, so it must recast to jsonb
        // (a text compare would false-mismatch on key/whitespace reordering).
        let p = column_predicate("payload", "JSONB", Some("{\"k\":\"v\"}"), true);
        assert_eq!(
            p,
            "\"payload\" IS NOT DISTINCT FROM CAST('{\"k\":\"v\"}' AS JSONB)"
        );
    }

    #[test]
    fn predicate_parameterized_type_recasts_with_precision() {
        // The catalog (format_type) hands verify the *parameterized* type, so
        // the recast reproduces the column's assignment-time rounding —
        // `numeric(10,2)` stores '1.005' as 1.01 and the recast must round the
        // same way (validated against live DSQL). The bare `NUMERIC` would
        // keep 1.005 and false-mismatch a faithful load.
        let p = column_predicate("amt", "numeric(10,2)", Some("1.005"), true);
        assert_eq!(
            p,
            "\"amt\" IS NOT DISTINCT FROM CAST('1.005' AS numeric(10,2))"
        );
        let p = column_predicate(
            "ts",
            "timestamp(0) without time zone",
            Some("2024-01-01 10:23:54.6"),
            true,
        );
        assert_eq!(
            p,
            "\"ts\" IS NOT DISTINCT FROM CAST('2024-01-01 10:23:54.6' AS timestamp(0) without time zone)"
        );
    }

    #[test]
    fn predicate_lowercase_json_takes_text_path() {
        // format_type yields lowercase `json`; compares_as_text must still
        // route it to the text-compare (json has no `=` operator).
        let p = column_predicate("payload", "json", Some("{\"k\":\"v\"}"), true);
        assert_eq!(
            p,
            "CAST(\"payload\" AS TEXT) IS NOT DISTINCT FROM '{\"k\":\"v\"}'"
        );
    }

    #[test]
    fn predicate_parameterized_varchar_casts_with_length() {
        // Verify casts the TEXT family through the parameterized name (it
        // carries the length, unlike the worker's bare `VARCHAR`), so
        // character varying(5) blank-/length-coerces faithfully.
        let p = column_predicate("vc", "character varying(5)", Some("ab"), true);
        assert_eq!(
            p,
            "\"vc\" IS NOT DISTINCT FROM CAST('ab' AS character varying(5))"
        );
    }

    #[test]
    fn batch_query_projects_pk_and_match_bool_over_range() {
        let rows = vec![
            SourceRow {
                pk_value: "1",
                columns: vec![
                    ("name", "TEXT", Some("alice")),
                    ("amt", "NUMERIC", Some("1.5")),
                ],
            },
            SourceRow {
                pk_value: "2",
                columns: vec![("name", "TEXT", None), ("amt", "NUMERIC", Some("2"))],
            },
        ];
        let sql = build_batch_query("\"t\"", "id", "INTEGER", &rows, true);

        // First column is a CASE that echoes the verbatim source PK text (not
        // CAST(pk AS TEXT)); the match bool is the second CAST(... AS TEXT)
        // CASE. Per-row arms keyed by recast PK; IN list holds the same
        // recast literals; null source field → IS NULL.
        assert!(sql.contains("SELECT CASE"), "{sql}");
        assert!(sql.contains("END, CAST(CASE"), "{sql}");
        assert!(sql.contains("END AS TEXT)"), "{sql}");
        // Source PK text echoed as the key for row 1.
        assert!(
            sql.contains("WHEN \"id\" IS NOT DISTINCT FROM CAST('1' AS INTEGER) THEN '1'"),
            "{sql}"
        );
        assert!(
            sql.contains("WHEN \"id\" IS NOT DISTINCT FROM CAST('1' AS INTEGER) THEN ("),
            "{sql}"
        );
        assert!(
            sql.contains("\"name\" IS NOT DISTINCT FROM 'alice' AND \"amt\" IS NOT DISTINCT FROM CAST('1.5' AS NUMERIC)"),
            "{sql}"
        );
        assert!(sql.contains("\"name\" IS NULL AND"), "{sql}");
        assert!(
            sql.contains("WHERE \"id\" IN (CAST('1' AS INTEGER), CAST('2' AS INTEGER))"),
            "{sql}"
        );
        assert!(sql.contains("FROM \"t\""), "{sql}");
    }

    #[test]
    fn batch_query_echoes_source_pk_text_not_target_canonical() {
        // Regression: a numeric(10,2) PK with source text `1.5` is stored as
        // `1.50`. The key column must echo the SOURCE text `1.5` so the
        // caller's diff (keyed on raw source text) matches — projecting
        // CAST(pk AS TEXT) would yield `1.50` and false-report missing.
        let rows = vec![SourceRow {
            pk_value: "1.5",
            columns: vec![("v", "TEXT", Some("x"))],
        }];
        let sql = build_batch_query("\"t\"", "id", "numeric(10,2)", &rows, true);
        assert!(
            sql.contains(
                "WHEN \"id\" IS NOT DISTINCT FROM CAST('1.5' AS numeric(10,2)) THEN '1.5'"
            ),
            "key column must echo verbatim source text '1.5': {sql}"
        );
        // The target's canonical form must NOT be what we key on.
        assert!(
            !sql.contains("CAST(\"id\" AS TEXT)"),
            "must not project target-canonical PK text: {sql}"
        );
    }

    #[test]
    fn batch_query_empty_columns_matches_true() {
        // A row with only a PK (no other loaded columns) trivially matches.
        let rows = vec![SourceRow {
            pk_value: "5",
            columns: vec![],
        }];
        let sql = build_batch_query("\"t\"", "id", "INTEGER", &rows, true);
        assert!(sql.contains("THEN (TRUE)"), "{sql}");
        assert!(
            sql.contains("WHERE \"id\" IN (CAST('5' AS INTEGER))"),
            "{sql}"
        );
    }

    #[test]
    fn batch_query_uuid_pk_casts_in_arm_and_in_list() {
        let rows = vec![SourceRow {
            pk_value: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            columns: vec![("v", "TEXT", Some("x"))],
        }];
        let sql = build_batch_query("\"t\"", "id", "UUID", &rows, true);
        // PK literal recast in: key-CASE arm, match-CASE arm, and IN list.
        assert_eq!(
            sql.matches("CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)")
                .count(),
            3,
            "{sql}"
        );
    }
}
