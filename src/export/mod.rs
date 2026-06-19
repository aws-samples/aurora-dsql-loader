//! Native `export`: read a source DSQL cluster's catalog and emit a plain
//! pg_dump-shaped `.sql` (DDL + `COPY ... FROM stdin` blocks) that the
//! `migrate` flow consumes unchanged.

mod data;
mod ddl;

/// Quote a SQL identifier, doubling any embedded `"`. Defense in depth —
/// callers validate identifiers up front (see `validate_pgdump_identifier`),
/// but every emitted identifier passes through here so a stray quote can
/// never break out of its quoting.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
