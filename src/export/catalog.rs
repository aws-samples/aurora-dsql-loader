//! Read a source DSQL cluster's catalog into the structs the document
//! assembler consumes. The raw queries are Postgres-only and exercised by
//! the gated round-trip integration test; the index-selection decision is
//! pure and unit-tested here.

use anyhow::{Context, Result};
use sqlx::Row;

use super::ddl::{ColumnDef, TableDef};
use super::document::TableExport;
use super::quote_ident;
use crate::db::Pool;
use crate::runner::validate_pgdump_identifier;

/// Build the `"schema"."table"` text literal that the per-table `::regclass`
/// casts and `pg_get_serial_sequence`'s first argument resolve against.
///
/// MUST be quoted: a bare `schema.table` text fed to `::regclass` is parsed
/// with identifier rules — unquoted parts are case-folded to lowercase — so a
/// mixed-case or reserved-word table either fails to resolve or silently
/// resolves a *different* lowercased relation (verified on a live cluster:
/// `to_regclass('s.PG_CLASS')` folds and resolves, `to_regclass('"s"."PG_CLASS"')`
/// returns NULL). Quoting preserves the catalog's exact name; lowercase names
/// still resolve when quoted, so there is no regression. This matches the data
/// path, which already quotes via `qualified_table_name`. Note the *column*
/// argument of `pg_get_serial_sequence` is intentionally left unquoted — that
/// arg is matched literally against `pg_attribute.attname`, not re-parsed.
fn regclass_literal(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

/// List user tables `(schema, table)` in catalog order, skipping the system
/// schemas. An optional `schema`/`table` filter narrows the export.
pub async fn list_tables(
    pool: &Pool,
    schema: Option<&str>,
    table: Option<&str>,
) -> Result<Vec<(String, String)>> {
    let mut sql = String::from(
        "SELECT n.nspname, c.relname \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relkind = 'r' \
           AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'sys') \
           AND n.nspname NOT LIKE 'pg\\_%'",
    );
    let mut binds: Vec<&str> = Vec::new();
    if let Some(s) = schema {
        binds.push(s);
        sql.push_str(&format!(" AND n.nspname = ${}", binds.len()));
    }
    if let Some(t) = table {
        binds.push(t);
        sql.push_str(&format!(" AND c.relname = ${}", binds.len()));
    }
    sql.push_str(" ORDER BY n.nspname, c.relname");

    let rows: Vec<(String, String)> = pool
        .fetch_all_with_binds(&sql, &binds)
        .await
        .context("Failed to list source tables")?;

    for (schema, table) in &rows {
        validate_pgdump_identifier("schema", schema)?;
        validate_pgdump_identifier("table", table)?;
    }
    Ok(rows)
}

/// Read one table's full export payload: columns, primary key, kept indexes,
/// and all row data. Builds the quoted `"schema"."table"` regclass literal
/// (see [`regclass_literal`]) the per-column `::regclass` casts resolve against.
pub async fn read_table_export(pool: &Pool, schema: &str, table: &str) -> Result<TableExport> {
    let oid_param = regclass_literal(schema, table);

    let columns = read_columns(pool, &oid_param).await?;
    reject_check_constraints(pool, &oid_param).await?;
    let (pk_index_name, pk_columns) = read_primary_key(pool, &oid_param).await?;
    let index_defs = {
        let all = read_indexes(pool, schema, table).await?;
        select_index_defs(&all, pk_index_name.as_deref())
    };

    let table_def = TableDef {
        schema: schema.to_string(),
        name: table.to_string(),
        columns: columns.clone(),
        pk_columns: pk_columns.clone(),
    };
    let copy_columns: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let rows = read_rows(pool, schema, table, &copy_columns, &pk_columns).await?;

    let identity_col = columns.iter().find(|c| c.identity_cache.is_some());
    let identity_setval = match identity_col {
        Some(col) if !rows.is_empty() => {
            read_identity_setval(pool, schema, table, &col.name).await?
        }
        _ => None,
    };

    Ok(TableExport {
        table: table_def,
        index_defs,
        copy_columns,
        rows,
        identity_setval,
    })
}

/// Build the lint-safe `setval` that advances the identity column's sequence
/// past the loaded rows, so a reloaded cluster's next implicit insert
/// continues past them. Returns `None` when the column is empty (max is NULL).
/// We use `setval(seq, max, true)` rather than `ALTER … RESTART`, which DSQL's
/// parser rejects.
///
/// The caller only invokes this for a confirmed identity column on a non-empty
/// table, so a *missing* sequence here is anomalous (not the benign empty
/// case): emitting no `setval` would let the reloaded cluster's next implicit
/// insert collide with a loaded id. We warn loudly instead of skipping
/// silently, since that failure would otherwise surface only as a duplicate-key
/// error on the destination, far from the export.
async fn read_identity_setval(
    pool: &Pool,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<Option<String>> {
    let oid_param = regclass_literal(schema, table);
    let qualified = pool.qualified_table_name(schema, table);
    // (sequence_name, max_value-as-text): both from one row, so an empty
    // table yields a NULL max and we skip.
    let rows: Vec<(Option<String>, Option<String>)> = pool
        .fetch_all_with_binds(
            &format!(
                "SELECT pg_get_serial_sequence($1, $2), max({col})::text FROM {qualified}",
                col = quote_ident(column),
            ),
            &[&oid_param, column],
        )
        .await
        .with_context(|| format!("Failed to read identity max for {oid_param}.{column}"))?;

    let (seq, max) = match rows.into_iter().next() {
        // Empty table: max is NULL — genuinely no continuation needed.
        Some((_, None)) | None => return Ok(None),
        // Rows exist but the backing sequence didn't resolve: identity
        // continuation would be silently dropped. Surface it.
        Some((None, Some(_))) => {
            tracing::warn!(
                table = %oid_param,
                column,
                "identity column has rows but its backing sequence could not be \
                 resolved; the reloaded cluster's identity counter will NOT be \
                 advanced and its next implicit insert may collide with a loaded id"
            );
            return Ok(None);
        }
        Some((Some(seq), Some(max))) => (seq, max),
    };
    // setval's first arg is a regclass text literal; escape embedded quotes.
    let seq_lit = seq.replace('\'', "''");
    Ok(Some(format!(
        "SELECT pg_catalog.setval('{seq_lit}', {max}, true)"
    )))
}

/// Read column definitions in attnum order. Identity columns carry their
/// sequence `CACHE`; non-identity columns carry their default expression.
///
/// Generated columns (`GENERATED ALWAYS AS (expr) STORED`) are rejected, not
/// exported: their `pg_get_expr` reads back as a plain default, so emitting it
/// would silently downgrade the column to a writable default AND dump its
/// computed values into the COPY block — a fidelity loss the operator wouldn't
/// see. Failing fast names the column so they can edit the dump deliberately.
async fn read_columns(pool: &Pool, oid_param: &str) -> Result<Vec<ColumnDef>> {
    // (attname, format_type, attnotnull, attidentity, attgenerated, default_expr)
    let rows: Vec<(String, String, bool, String, String, Option<String>)> = pool
        .fetch_all_with_binds(
            "SELECT a.attname, \
                    format_type(a.atttypid, a.atttypmod), \
                    a.attnotnull, \
                    a.attidentity::text, \
                    a.attgenerated::text, \
                    pg_get_expr(ad.adbin, ad.adrelid) \
             FROM pg_attribute a \
             LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum \
             WHERE a.attrelid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[oid_param],
        )
        .await
        .with_context(|| format!("Failed to read columns for {oid_param}"))?;

    let mut columns = Vec::with_capacity(rows.len());
    for (name, type_name, not_null, identity, generated, default_expr) in rows {
        validate_pgdump_identifier("column", &name)?;
        // attgenerated is '' for a normal column, 's' for STORED generated.
        // Exporting a generated column correctly needs a GENERATED clause plus
        // COPY-exclusion (you can't INSERT into it); rather than silently emit
        // the wrong thing, refuse and tell the operator which column.
        if !generated.is_empty() {
            anyhow::bail!(
                "{oid_param}.{name:?} is a generated column, which export does not \
                 support — its values are computed, not stored as a writable \
                 default. Drop or rewrite the column in the source, or edit the \
                 dump by hand, before migrating."
            );
        }
        let identity_cache = if identity.is_empty() {
            None
        } else {
            Some(read_identity_cache(pool, oid_param, &name).await?)
        };
        columns.push(ColumnDef {
            name,
            type_name,
            not_null,
            identity_cache,
            // An identity column's default is the implicit nextval; never
            // emit it as an explicit DEFAULT (the IDENTITY clause owns it).
            default_expr: if identity_cache.is_some() {
                None
            } else {
                default_expr
            },
        });
    }
    Ok(columns)
}

/// Look up the sequence `CACHE` backing an identity column. DSQL requires
/// an explicit CACHE on the regenerated identity, so we carry the source's.
async fn read_identity_cache(pool: &Pool, oid_param: &str, column: &str) -> Result<i64> {
    let rows: Vec<(i64,)> = pool
        .fetch_all_with_binds(
            "SELECT s.seqcache FROM pg_sequence s \
             WHERE s.seqrelid = pg_get_serial_sequence($1, $2)::regclass",
            &[oid_param, column],
        )
        .await
        .with_context(|| format!("Failed to read identity cache for {oid_param}.{column}"))?;
    match rows.first() {
        Some((c,)) => Ok(*c),
        // The column is a confirmed identity (caller only calls us then), so an
        // unreadable backing sequence is anomalous. Default to 1 (DSQL's
        // minimum) to keep the export usable, but warn — silently substituting
        // 1 for a source `CACHE 128` would change allocation semantics invisibly.
        None => {
            tracing::warn!(
                table = %oid_param,
                column,
                "identity column's backing sequence could not be read; \
                 emitting CACHE 1 (the source's actual cache is unknown)"
            );
            Ok(1)
        }
    }
}

/// Refuse to export a table that has `CHECK` constraints. The DDL generator
/// emits only columns + PK + identity, so a `CHECK` would be silently dropped —
/// the destination would lose the constraint with no signal, exactly the
/// protection a "recreate into a fresh cluster" migration must preserve. Fail
/// fast naming the constraints so the operator can re-add them deliberately.
async fn reject_check_constraints(pool: &Pool, oid_param: &str) -> Result<()> {
    let rows: Vec<(String,)> = pool
        .fetch_all_with_binds(
            "SELECT conname FROM pg_constraint \
             WHERE conrelid = $1::regclass AND contype = 'c' ORDER BY conname",
            &[oid_param],
        )
        .await
        .with_context(|| format!("Failed to read CHECK constraints for {oid_param}"))?;
    if !rows.is_empty() {
        let names = rows
            .iter()
            .map(|(n,)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        anyhow::bail!(
            "{oid_param} has CHECK constraint(s) [{names}], which export does not \
             emit — they would be silently dropped on the destination. Remove them \
             from the source or add them back by hand after migrating."
        );
    }
    Ok(())
}

/// Read the primary key's backing index name and ordered columns. Returns
/// `(None, [])` when the table has no primary key.
async fn read_primary_key(pool: &Pool, oid_param: &str) -> Result<(Option<String>, Vec<String>)> {
    // One row per PK column, in key order, carrying the constraint name
    // (which equals the backing index name we later exclude).
    let rows: Vec<(String, String)> = pool
        .fetch_all_with_binds(
            "SELECT con.conname, att.attname \
             FROM pg_constraint con \
             JOIN unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true \
             JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum \
             WHERE con.conrelid = $1::regclass AND con.contype = 'p' \
             ORDER BY k.ord",
            &[oid_param],
        )
        .await
        .with_context(|| format!("Failed to read primary key for {oid_param}"))?;

    if rows.is_empty() {
        return Ok((None, Vec::new()));
    }
    let index_name = rows[0].0.clone();
    let mut pk_columns = Vec::with_capacity(rows.len());
    for (_, col) in rows {
        validate_pgdump_identifier("column", &col)?;
        pk_columns.push(col);
    }
    Ok((Some(index_name), pk_columns))
}

/// Read `(index_name, indexdef)` for every index on the table. `pg_get_indexdef`
/// output is emitted verbatim; dsql-lint normalizes it (ASYNC, USING).
async fn read_indexes(pool: &Pool, schema: &str, table: &str) -> Result<Vec<(String, String)>> {
    pool.fetch_all_with_binds(
        "SELECT indexname, indexdef FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 ORDER BY indexname",
        &[schema, table],
    )
    .await
    .with_context(|| format!("Failed to read indexes for {schema}.{table}"))
}

/// Read all rows, every column cast to text (so bytea→`\x…`, jsonb→canonical
/// json, timestamps→ISO all round-trip through the loader's recast on
/// reload). Ordered by primary key when present for deterministic output.
async fn read_rows(
    pool: &Pool,
    schema: &str,
    table: &str,
    columns: &[String],
    pk_columns: &[String],
) -> Result<Vec<Vec<Option<String>>>> {
    let qualified = pool.qualified_table_name(schema, table);
    let col_list = columns
        .iter()
        .map(|c| format!("{}::text", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let order = if pk_columns.is_empty() {
        String::new()
    } else {
        let cols = pk_columns
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        format!(" ORDER BY {cols}")
    };
    let sql = format!("SELECT {col_list} FROM {qualified}{order}");

    let pg_rows = sqlx::query(&sql)
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to read rows from {schema}.{table}"))?;

    let mut rows = Vec::with_capacity(pg_rows.len());
    for row in &pg_rows {
        let mut fields = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            fields.push(
                row.try_get::<Option<String>, _>(i)
                    .with_context(|| format!("Failed to read column {i} from {schema}.{table}"))?,
            );
        }
        rows.push(fields);
    }
    Ok(rows)
}

/// Pick the index statements to emit from `(index_name, indexdef)` pairs,
/// dropping the primary key's backing index (we emit the PK inline on the
/// `CREATE TABLE`). `pk_index_name` is the PK constraint's index name, or
/// `None` when the table has no primary key.
fn select_index_defs(indexes: &[(String, String)], pk_index_name: Option<&str>) -> Vec<String> {
    indexes
        .iter()
        .filter(|(name, _)| Some(name.as_str()) != pk_index_name)
        .map(|(_, def)| def.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn excludes_primary_key_backing_index_keeps_others() {
        // pg_indexes lists the PK's implicit index alongside secondary ones.
        // We emit the PK inline on CREATE TABLE, so its backing index (named
        // after the PK constraint) must be dropped; a secondary index and a
        // UNIQUE-constraint index are kept (dsql-lint normalizes them).
        let indexes = vec![
            (
                "t_pkey".to_string(),
                "CREATE UNIQUE INDEX t_pkey ON public.t USING btree_index (id) INCLUDE (name)"
                    .to_string(),
            ),
            (
                "idx_t_name".to_string(),
                "CREATE INDEX idx_t_name ON public.t USING btree_index (name)".to_string(),
            ),
            (
                "t_email_key".to_string(),
                "CREATE UNIQUE INDEX t_email_key ON public.t USING btree_index (email)".to_string(),
            ),
        ];
        let kept = select_index_defs(&indexes, Some("t_pkey"));
        assert_eq!(
            kept,
            vec![
                "CREATE INDEX idx_t_name ON public.t USING btree_index (name)".to_string(),
                "CREATE UNIQUE INDEX t_email_key ON public.t USING btree_index (email)".to_string(),
            ]
        );
    }

    #[test]
    fn keeps_all_indexes_when_table_has_no_primary_key() {
        let indexes = vec![(
            "idx_t_name".to_string(),
            "CREATE INDEX idx_t_name ON public.t (name)".to_string(),
        )];
        let kept = select_index_defs(&indexes, None);
        assert_eq!(
            kept,
            vec!["CREATE INDEX idx_t_name ON public.t (name)".to_string()]
        );
    }
}
