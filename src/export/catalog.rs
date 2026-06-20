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
/// and all row data. `qualified`/`oid_param` is the schema-qualified name the
/// `::regclass` casts resolve against.
pub async fn read_table_export(pool: &Pool, schema: &str, table: &str) -> Result<TableExport> {
    let oid_param = format!("{schema}.{table}");

    let columns = read_columns(pool, &oid_param).await?;
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

    Ok(TableExport {
        table: table_def,
        index_defs,
        copy_columns,
        rows,
    })
}

/// Read column definitions in attnum order. Identity columns carry their
/// sequence `CACHE`; non-identity columns carry their default expression.
async fn read_columns(pool: &Pool, oid_param: &str) -> Result<Vec<ColumnDef>> {
    // (attname, format_type, attnotnull, attidentity, default_expr)
    let rows: Vec<(String, String, bool, String, Option<String>)> = pool
        .fetch_all_with_binds(
            "SELECT a.attname, \
                    format_type(a.atttypid, a.atttypmod), \
                    a.attnotnull, \
                    a.attidentity::text, \
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
    for (name, type_name, not_null, identity, default_expr) in rows {
        validate_pgdump_identifier("column", &name)?;
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
    // Default to 1 (DSQL's minimum) if the sequence is somehow unreadable.
    Ok(rows.first().map(|(c,)| *c).unwrap_or(1))
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
        assert_eq!(kept, vec!["CREATE INDEX idx_t_name ON public.t (name)".to_string()]);
    }
}
