//! Drive the export flow: open a pool to the source cluster, read each
//! table's catalog + data, assemble a pg_dump-shaped `.sql`, and write it to
//! a file or stdout. The output is consumed unchanged by `migrate`.

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};

use super::catalog::{list_tables, read_table_export};
use super::document::assemble_dump;
use crate::db::Pool;
use crate::db::pool::{PoolArgsBuilder, pool as build_dsql_pool};

/// Caller-supplied configuration for an export run.
#[derive(Debug, Clone)]
pub struct ExportArgs {
    /// Source DSQL endpoint (`<cluster>.dsql.<region>.on.aws`).
    pub endpoint: String,
    /// AWS region the source cluster lives in.
    pub region: String,
    /// IAM principal name (`admin` or a custom DB user).
    pub username: String,
    /// Optional schema filter; `None` exports all user schemas.
    pub schema: Option<String>,
    /// Optional single-table filter; `None` exports all tables.
    pub table: Option<String>,
    /// Destination file; `None` writes the dump to stdout.
    pub output: Option<PathBuf>,
    /// Test-only: caller-supplied `Pool` to bypass the DSQL IAM path so a
    /// real-Postgres round-trip test can drive the reader end-to-end.
    #[cfg(test)]
    pub test_pool: Option<Pool>,
}

/// Summary of a completed export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportReport {
    /// Schema-qualified names of the exported tables, in output order.
    pub tables: Vec<String>,
    /// Total bytes written.
    pub bytes_written: usize,
}

/// Read the source cluster's catalog and write a pg_dump-shaped `.sql`.
pub async fn run_export(args: ExportArgs) -> Result<ExportReport> {
    let pool = build_pool(&args).await?;

    let tables = list_tables(&pool, args.schema.as_deref(), args.table.as_deref()).await?;

    // An empty match almost always means a fat-fingered --schema/--table (or a
    // truly empty cluster). Writing a near-empty dump that exits 0 would let a
    // typo's output pipe straight into `migrate` and look like a clean run, so
    // fail loudly with the filters that produced nothing.
    if tables.is_empty() {
        anyhow::bail!(empty_match_message(
            args.schema.as_deref(),
            args.table.as_deref()
        ));
    }

    let mut exports = Vec::with_capacity(tables.len());
    for (schema, table) in &tables {
        exports.push(
            read_table_export(&pool, schema, table)
                .await
                .with_context(|| format!("Failed to export {schema}.{table}"))?,
        );
    }

    let dump = assemble_dump(&exports);
    let table_names = tables
        .iter()
        .map(|(s, t)| format!("{s}.{t}"))
        .collect::<Vec<_>>();

    write_output(args.output.as_deref(), dump.as_bytes())?;

    Ok(ExportReport {
        tables: table_names,
        bytes_written: dump.len(),
    })
}

/// Build the "matched no tables" error message for the active filters. Pure so
/// the four filter combinations can be unit-tested without a live catalog. Uses
/// `{:?}` on the filter values, which escapes control/bidi bytes (so a crafted
/// `--table` can't reflow the operator's terminal through the error).
fn empty_match_message(schema: Option<&str>, table: Option<&str>) -> String {
    match (schema, table) {
        (Some(s), Some(t)) => {
            format!("export matched no tables for --schema {s:?} --table {t:?}; check the names")
        }
        (Some(s), None) => format!("export matched no tables in --schema {s:?}; check the name"),
        (None, Some(t)) => format!("export matched no table named --table {t:?}; check the name"),
        (None, None) => {
            "export found no user tables in the source cluster; nothing to export".to_string()
        }
    }
}

/// Write the assembled dump to the destination file, or stdout when `None`.
fn write_output(output: Option<&std::path::Path>, bytes: &[u8]) -> Result<()> {
    match output {
        Some(path) => {
            std::fs::write(path, bytes)
                .with_context(|| format!("Failed to write dump to {}", path.display()))?;
        }
        None => {
            std::io::stdout()
                .write_all(bytes)
                .context("Failed to write dump to stdout")?;
        }
    }
    Ok(())
}

/// Build a `Pool` for export. Prefers `args.test_pool` in test builds (so a
/// real-Postgres round-trip test skips IAM); otherwise opens the DSQL
/// connector. Mirrors `migrate::orchestrator::build_pool`.
async fn build_pool(args: &ExportArgs) -> Result<Pool> {
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

#[cfg(test)]
mod tests {
    use super::empty_match_message;

    #[test]
    fn empty_match_message_names_the_active_filters() {
        // Each filter combination gets a distinct message naming what matched
        // nothing, so a typo'd --schema/--table is actionable rather than a
        // silent 0-byte dump. Guards the four arms of the empty-match bail.
        let both = empty_match_message(Some("sales"), Some("orders"));
        assert!(both.contains("sales") && both.contains("orders"), "{both}");
        assert!(
            both.contains("--schema") && both.contains("--table"),
            "{both}"
        );

        let schema_only = empty_match_message(Some("sales"), None);
        assert!(schema_only.contains("sales") && schema_only.contains("--schema"));
        assert!(!schema_only.contains("--table"), "{schema_only}");

        let table_only = empty_match_message(None, Some("orders"));
        assert!(table_only.contains("orders") && table_only.contains("--table"));
        assert!(!table_only.contains("--schema"), "{table_only}");

        let neither = empty_match_message(None, None);
        assert!(neither.contains("no user tables"), "{neither}");
        assert!(!neither.contains("--schema") && !neither.contains("--table"));
    }

    #[test]
    fn empty_match_message_escapes_control_bytes_in_filters() {
        // `{:?}` (Debug) escapes control/bidi bytes so a crafted --table can't
        // reflow the operator's terminal through the error line.
        let msg = empty_match_message(None, Some("evil\u{202e}\nname"));
        assert!(!msg.contains('\n'), "raw newline leaked: {msg:?}");
        assert!(!msg.contains('\u{202e}'), "RTL override leaked: {msg:?}");
    }
}
