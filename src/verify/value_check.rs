//! L3 value verification: build the server-side recast-compare query and
//! diff the source (re-read, decoded TEXT) against the target by primary
//! key. The DB is the type authority — a source field is pushed back
//! through the same `CAST` the loader used at INSERT, compared null-safe
//! (`IS NOT DISTINCT FROM` on Postgres/DSQL, `IS` on SQLite), so canonical
//! forms (`1.5` vs `1.50`, `\N` vs NULL) match without a client normalizer.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{L3Details, L3Outcome, SchemaCheck};
use crate::db::Pool;
use crate::db::schema::{SqlType, escape_sql_literal, inserts_as_bare_literal, query_table_schema};
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
    // Every COPY column must resolve to a type for the recast compare.
    let types: Vec<SqlType> = block
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

/// name → SqlType for every column of the target table.
async fn column_types(pool: &Pool, schema: &str, table: &str) -> Result<HashMap<String, SqlType>> {
    let resolved = query_table_schema(pool, schema, table).await?;
    Ok(resolved
        .columns
        .into_iter()
        .map(|c| (c.name, c.sql_type))
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
    types: &'a [SqlType],
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
        .map(|(i, name)| (name.as_str(), &types[i], rec.fields[i].as_deref()))
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

/// Render a value as the SQL literal the worker would have inserted: bare
/// `'v'` for the TEXT family, `CAST('v' AS TYPE)` otherwise. Shares
/// `inserts_as_bare_literal` with the worker so the bare-vs-CAST decision
/// matches the write path on Postgres/DSQL (SQLite tests accept the extra
/// CAST harmlessly — the worker inserts bare there).
fn recast_literal(ty: &SqlType, value: &str) -> String {
    let lit = format!("'{}'", escape_sql_literal(value));
    if inserts_as_bare_literal(ty.to_postgres()) {
        lit
    } else {
        format!("CAST({lit} AS {})", ty.to_postgres())
    }
}

/// Per-column match predicate: `None` (source `\N`) → `"col" IS NULL`;
/// `Some(v)` → `"col" <nseq> <recast literal>`. `col` is double-quoted.
///
/// `json` is special-cased: it has no `=` operator (a direct
/// `IS NOT DISTINCT FROM` errors), but it stores the verbatim source COPY
/// text, so we compare `CAST("col" AS text)` against the bare source text —
/// exact and equality-capable. This is textual, not semantic-JSON. `jsonb`
/// is NOT special-cased: it *has* `=` and canonicalizes input (key order /
/// whitespace), so it must compare via native `CAST('v' AS jsonb)` (a text
/// compare would false-mismatch on the canonicalization) — that's the
/// default `recast_literal` arm below.
fn column_predicate(col: &str, ty: &SqlType, value: Option<&str>, is_postgres: bool) -> String {
    let quoted = format!("\"{col}\"");
    let nseq = null_safe_eq(is_postgres);
    match (value, ty) {
        (None, _) => format!("{quoted} IS NULL"),
        (Some(v), SqlType::Json) => {
            let lit = format!("'{}'", escape_sql_literal(v));
            format!("CAST({quoted} AS TEXT) {nseq} {lit}")
        }
        (Some(v), _) => format!("{quoted} {nseq} {}", recast_literal(ty, v)),
    }
}

/// One source row to verify: its primary-key text value plus the
/// (column, type, value) tuples for every non-PK loaded column.
pub(crate) struct SourceRow<'a> {
    pub pk_value: &'a str,
    pub columns: Vec<(&'a str, &'a SqlType, Option<&'a str>)>,
}

/// Build the projected-bool batch query: for each `SourceRow`, return
/// `(pk::text, matches)` where `matches` ANDs every column predicate. A
/// requested PK absent from the result set is a missing row (the caller
/// diffs requested vs returned PKs).
///
/// The `WHERE pk IN (...)` list and the per-row `CASE WHEN pk <nseq> ...`
/// arms are built from the SAME recast PK literals, so the batch is exact
/// regardless of PK type. `IN` of the exact PKs (not `pk BETWEEN lo AND hi`)
/// avoids needing the caller to sort PKs in the DB's type order — a lexical
/// sort of integer/uuid text would mis-window a range.
fn build_batch_query(
    qualified_table: &str,
    pk_col: &str,
    pk_type: &SqlType,
    rows: &[SourceRow<'_>],
    is_postgres: bool,
) -> String {
    let pk_lit = |v: &str| recast_literal(pk_type, v);
    let quoted_pk = format!("\"{pk_col}\"");
    let nseq = null_safe_eq(is_postgres);

    let arms: String = rows
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

    // Both columns are fetched as text: the match bool is wrapped in
    // CAST(... AS TEXT) so it decodes uniformly — Postgres yields
    // `true`/`false`, SQLite yields `1`/`0` (see `is_match`).
    format!(
        "SELECT CAST({quoted_pk} AS TEXT), CAST(CASE\n{arms}\n    END AS TEXT)\n  \
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
        let p = column_predicate("note", &SqlType::Text, None, true);
        assert_eq!(p, "\"note\" IS NULL");
    }

    #[test]
    fn predicate_text_column_no_cast() {
        // TEXT family inserts bare, so verify compares bare.
        let p = column_predicate("name", &SqlType::Text, Some("alice"), true);
        assert_eq!(p, "\"name\" IS NOT DISTINCT FROM 'alice'");
    }

    #[test]
    fn predicate_numeric_column_casts() {
        let p = column_predicate("amount", &SqlType::Numeric, Some("1.5"), true);
        assert_eq!(p, "\"amount\" IS NOT DISTINCT FROM CAST('1.5' AS NUMERIC)");
    }

    #[test]
    fn predicate_sqlite_uses_is() {
        let p = column_predicate("amount", &SqlType::Numeric, Some("1.5"), false);
        assert_eq!(p, "\"amount\" IS CAST('1.5' AS NUMERIC)");
    }

    #[test]
    fn predicate_escapes_single_quotes() {
        let p = column_predicate("note", &SqlType::Text, Some("o'brien"), true);
        assert_eq!(p, "\"note\" IS NOT DISTINCT FROM 'o''brien'");
    }

    #[test]
    fn predicate_timestamptz_uses_full_type_name() {
        let p = column_predicate("ts", &SqlType::TimestampTz, Some("2024-01-01Z"), true);
        assert_eq!(
            p,
            "\"ts\" IS NOT DISTINCT FROM CAST('2024-01-01Z' AS TIMESTAMP WITH TIME ZONE)"
        );
    }

    #[test]
    fn predicate_json_compares_column_as_text() {
        // json has no `=`; compare CAST(col AS TEXT) against the bare source
        // text instead of CAST('..' AS JSON) IS NOT DISTINCT FROM (which errors).
        let p = column_predicate("payload", &SqlType::Json, Some("{\"k\":\"v\"}"), true);
        assert_eq!(
            p,
            "CAST(\"payload\" AS TEXT) IS NOT DISTINCT FROM '{\"k\":\"v\"}'"
        );
    }

    #[test]
    fn predicate_jsonb_compares_natively_not_as_text() {
        // jsonb HAS `=` and canonicalizes input, so it must recast to jsonb
        // (a text compare would false-mismatch on key/whitespace reordering).
        let p = column_predicate("payload", &SqlType::Jsonb, Some("{\"k\":\"v\"}"), true);
        assert_eq!(
            p,
            "\"payload\" IS NOT DISTINCT FROM CAST('{\"k\":\"v\"}' AS JSONB)"
        );
    }

    #[test]
    fn batch_query_projects_pk_and_match_bool_over_range() {
        let int = SqlType::Integer;
        let text = SqlType::Text;
        let num = SqlType::Numeric;
        let rows = vec![
            SourceRow {
                pk_value: "1",
                columns: vec![("name", &text, Some("alice")), ("amt", &num, Some("1.5"))],
            },
            SourceRow {
                pk_value: "2",
                columns: vec![("name", &text, None), ("amt", &num, Some("2"))],
            },
        ];
        let sql = build_batch_query("\"t\"", "id", &int, &rows, true);

        // PK projected as text; per-row CASE arm keyed by recast PK; the IN
        // list holds the same recast literals; null source field → IS NULL.
        assert!(
            sql.contains("SELECT CAST(\"id\" AS TEXT), CAST(CASE"),
            "{sql}"
        );
        assert!(sql.contains("END AS TEXT)"), "{sql}");
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
    fn batch_query_empty_columns_matches_true() {
        // A row with only a PK (no other loaded columns) trivially matches.
        let int = SqlType::Integer;
        let rows = vec![SourceRow {
            pk_value: "5",
            columns: vec![],
        }];
        let sql = build_batch_query("\"t\"", "id", &int, &rows, true);
        assert!(sql.contains("THEN (TRUE)"), "{sql}");
        assert!(
            sql.contains("WHERE \"id\" IN (CAST('5' AS INTEGER))"),
            "{sql}"
        );
    }

    #[test]
    fn batch_query_uuid_pk_casts_in_arm_and_in_list() {
        let uuid = SqlType::Uuid;
        let text = SqlType::Text;
        let rows = vec![SourceRow {
            pk_value: "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            columns: vec![("v", &text, Some("x"))],
        }];
        let sql = build_batch_query("\"t\"", "id", &uuid, &rows, true);
        // PK literal recast in both the CASE arm and the IN list.
        assert_eq!(
            sql.matches("CAST('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' AS UUID)")
                .count(),
            2,
            "{sql}"
        );
    }
}
