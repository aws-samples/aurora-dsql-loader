//! Apply DSQL-compatible DDL to a `Pool`, one statement per call.
//!
//! Splits the input on `;` outside string and dollar-quoted literals and
//! comments, then drives each statement through `Pool::execute_query`.
//! DSQL allows only one DDL statement per transaction, so each statement
//! runs in its own implicit transaction (sqlx auto-commits per `execute`).
//!
//! "Already exists" errors are non-fatal so the migrate flow is idempotent
//! — a re-run after a partial failure picks up where the previous one left
//! off without the operator having to manually drop existing objects.

use crate::db::Pool;
use anyhow::Result;

/// Outcome of a single applied DDL statement, captured so the migrate
/// orchestrator can build a useful summary report and so callers (e.g. the
/// CLI's `--dry-run`) can surface what would have been applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedStatement {
    /// The SQL statement text that was sent to the cluster, trimmed of
    /// surrounding whitespace and the trailing `;`.
    pub sql: String,
    /// Outcome — see [`ApplyOutcome`].
    pub outcome: ApplyOutcome,
}

/// Whether a statement succeeded, was skipped because the target already
/// exists, or failed in a way the operator must address. Failures bubble
/// out as `Err` from [`apply_ddl`] rather than appearing here, so the
/// vector holds only successes/skips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// Statement executed successfully.
    Applied,
    /// Statement targeted an object that already exists (CREATE TABLE on
    /// an existing name, CREATE INDEX with an existing index name, etc.)
    /// and was skipped to keep the migrate flow re-runnable.
    SkippedAlreadyExists,
}

/// Apply each DDL statement in `ddl` to `pool`, in source order, one at a
/// time (DSQL does not allow multiple DDL statements per transaction).
///
/// On success returns a `Vec` of [`AppliedStatement`] in apply order. On
/// the first non-recoverable error returns `Err` with which statement
/// failed and the underlying database error — earlier statements remain
/// applied because DSQL has no all-or-nothing wrapper for multi-DDL
/// migrations. Operators are expected to investigate, fix, and re-run;
/// the "already exists" skip path makes that re-run safe.
#[allow(dead_code)]
pub async fn apply_ddl(pool: &Pool, ddl: &str) -> Result<Vec<AppliedStatement>> {
    let statements = split_sql_statements(ddl);
    let mut applied = Vec::with_capacity(statements.len());
    for stmt in statements {
        match pool.execute_query(&stmt).await {
            Ok(()) => applied.push(AppliedStatement {
                sql: stmt,
                outcome: ApplyOutcome::Applied,
            }),
            Err(e) if is_already_exists(&e) => applied.push(AppliedStatement {
                sql: stmt,
                outcome: ApplyOutcome::SkippedAlreadyExists,
            }),
            Err(e) => {
                return Err(
                    anyhow::Error::new(e).context(format!("Failed to apply DDL statement: {stmt}"))
                );
            }
        }
    }
    Ok(applied)
}

/// Whether a sqlx error represents a target object that already exists
/// (so the migrate flow can skip it for idempotency on re-run).
///
/// Postgres surfaces these as SQLSTATE 42P07 (`duplicate_table`),
/// 42P06 (`duplicate_schema`), 42710 (`duplicate_object` — index, sequence,
/// constraint, type), and 42701 (`duplicate_column`). DSQL inherits the
/// codes from its Postgres surface. SQLite uses message-string matching
/// since its `extended_code` mapping is much smaller, but the shape
/// `"already exists"` is stable across versions.
fn is_already_exists(err: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db_err) = err {
        if let Some(code) = db_err.code() {
            // Postgres / DSQL: `duplicate_*` family.
            if matches!(code.as_ref(), "42P07" | "42P06" | "42710" | "42701") {
                return true;
            }
        }
        // SQLite (and a generic safety net for any backend whose driver
        // doesn't surface a structured code): match on the standard
        // message string.
        if db_err
            .message()
            .to_ascii_lowercase()
            .contains("already exists")
        {
            return true;
        }
    }
    false
}

/// Split `sql` on `;` boundaries, skipping `;` that appears inside
/// single-quoted strings (with `''` escape), double-quoted identifiers
/// (with `""` escape), dollar-quoted strings (`$tag$...$tag$`, `$$...$$`),
/// line comments (`-- ...` to EOL) and block comments (`/* ... */`,
/// nested per Postgres semantics). Empty statements are dropped.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;

    while i < bytes.len() {
        let c = bytes[i];
        match c {
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => i = skip_line_comment(bytes, i),
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            b'$' => match try_skip_dollar_quoted(bytes, i) {
                Some(end) => i = end,
                // A `$` not introducing a dollar quote (e.g. `$1` placeholder)
                // is just a regular character.
                None => i += 1,
            },
            b';' => {
                push_if_nonempty(&mut out, &sql[start..i]);
                start = i + 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    push_if_nonempty(&mut out, &sql[start..]);
    out
}

/// Trim whitespace and skip statements that contain only whitespace or
/// comments.
fn push_if_nonempty(out: &mut Vec<String>, slice: &str) {
    let trimmed = slice.trim();
    if !trimmed.is_empty() && !is_comments_only(trimmed) {
        out.push(trimmed.to_owned());
    }
}

/// True when the slice is whitespace + line/block comments only — emitted
/// at end-of-input by pg_dump (the trailing `-- PostgreSQL database dump
/// complete` line). We don't want to send those to the database.
fn is_comments_only(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'-' && bytes.get(i + 1) == Some(&b'-') {
            i = skip_line_comment(bytes, i);
        } else if c == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i = skip_block_comment(bytes, i);
        } else {
            return false;
        }
    }
    true
}

/// Advance past `'...'` starting at `i` (caller has confirmed `bytes[i]
/// == '`). `''` is the embedded-quote escape. Returns the index of the
/// byte after the closing quote, or `bytes.len()` on unterminated input.
fn skip_single_quoted(bytes: &[u8], i: usize) -> usize {
    debug_assert_eq!(bytes[i], b'\'');
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == b'\'' {
            // `''` is an escaped single quote, not the end of the string.
            if bytes.get(j + 1) == Some(&b'\'') {
                j += 2;
            } else {
                return j + 1;
            }
        } else {
            j += 1;
        }
    }
    bytes.len()
}

/// Advance past `"..."` starting at `i`. `""` is the embedded-quote escape.
fn skip_double_quoted(bytes: &[u8], i: usize) -> usize {
    debug_assert_eq!(bytes[i], b'"');
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == b'"' {
            if bytes.get(j + 1) == Some(&b'"') {
                j += 2;
            } else {
                return j + 1;
            }
        } else {
            j += 1;
        }
    }
    bytes.len()
}

/// Advance past a `-- ...` line comment to the next `\n` (consumed) or EOF.
fn skip_line_comment(bytes: &[u8], i: usize) -> usize {
    debug_assert_eq!(&bytes[i..i + 2], b"--");
    let mut j = i + 2;
    while j < bytes.len() && bytes[j] != b'\n' {
        j += 1;
    }
    if j < bytes.len() { j + 1 } else { j }
}

/// Advance past a `/* ... */` block comment. Postgres allows nested
/// block comments, so we track depth.
fn skip_block_comment(bytes: &[u8], i: usize) -> usize {
    debug_assert_eq!(&bytes[i..i + 2], b"/*");
    let mut j = i + 2;
    let mut depth = 1u32;
    while j + 1 < bytes.len() && depth > 0 {
        match (bytes[j], bytes[j + 1]) {
            (b'/', b'*') => {
                depth += 1;
                j += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                j += 2;
            }
            _ => j += 1,
        }
    }
    j
}

/// If `bytes[i..]` opens a dollar-quoted string (`$tag$...$tag$` or
/// `$$...$$`), advance past the closing tag and return the index after it.
/// Returns `None` if the `$` does not introduce a valid dollar quote
/// (e.g. `$1` parameter placeholders, an arithmetic `$` etc.).
fn try_skip_dollar_quoted(bytes: &[u8], i: usize) -> Option<usize> {
    debug_assert_eq!(bytes[i], b'$');
    // Read the optional tag: identifier-friendly chars between two `$`.
    let mut tag_end = i + 1;
    while tag_end < bytes.len() {
        let c = bytes[tag_end];
        if c == b'$' {
            break;
        }
        // Dollar tags must look like identifiers — letters, digits, `_`.
        // Anything else (including a newline) means this `$` was not the
        // opener of a dollar quote.
        if !(c.is_ascii_alphanumeric() || c == b'_') {
            return None;
        }
        tag_end += 1;
    }
    if tag_end >= bytes.len() {
        return None;
    }
    let tag = &bytes[i..=tag_end]; // includes both `$` boundaries
    let body_start = tag_end + 1;
    let mut j = body_start;
    while j + tag.len() <= bytes.len() {
        if &bytes[j..j + tag.len()] == tag {
            return Some(j + tag.len());
        }
        j += 1;
    }
    // Unterminated dollar-quoted string: consume to EOF rather than treat
    // every subsequent `;` as a statement boundary.
    Some(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(sql: &str) -> Vec<String> {
        split_sql_statements(sql)
    }

    #[test]
    fn splits_simple_statements() {
        let r = split("SELECT 1; SELECT 2;");
        assert_eq!(r, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn handles_trailing_semicolon_and_whitespace() {
        let r = split(" CREATE TABLE t (id INT) ; \n  CREATE INDEX i ON t (id);  \n");
        assert_eq!(
            r,
            vec!["CREATE TABLE t (id INT)", "CREATE INDEX i ON t (id)"]
        );
    }

    #[test]
    fn drops_empty_and_whitespace_only_statements() {
        let r = split(";;\n;\nSELECT 1;\n;");
        assert_eq!(r, vec!["SELECT 1"]);
    }

    #[test]
    fn drops_comment_only_segments() {
        // pg_dump tail: "-- PostgreSQL database dump complete\n--\n"
        let r = split("SELECT 1;\n-- PostgreSQL database dump complete\n--\n");
        assert_eq!(r, vec!["SELECT 1"]);
    }

    #[test]
    fn semicolon_inside_single_quoted_string_does_not_split() {
        let r = split("INSERT INTO t VALUES ('a;b'); SELECT 2;");
        assert_eq!(r, vec!["INSERT INTO t VALUES ('a;b')", "SELECT 2"]);
    }

    #[test]
    fn doubled_single_quote_is_escape_not_terminator() {
        let r = split("INSERT INTO t VALUES ('it''s;ok'); SELECT 2;");
        assert_eq!(r, vec!["INSERT INTO t VALUES ('it''s;ok')", "SELECT 2"]);
    }

    #[test]
    fn semicolon_inside_double_quoted_identifier_does_not_split() {
        let r = split(r#"CREATE TABLE "weird;name" (id INT); SELECT 1;"#);
        assert_eq!(r, vec![r#"CREATE TABLE "weird;name" (id INT)"#, "SELECT 1"]);
    }

    #[test]
    fn semicolon_inside_dollar_quoted_string_does_not_split() {
        // Empty-tag dollar quoting (`$$...$$`).
        let r = split("CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql; SELECT 2;");
        assert_eq!(r.len(), 2);
        assert!(r[0].contains("$$ SELECT 1; $$"));
        assert_eq!(r[1], "SELECT 2");
    }

    #[test]
    fn semicolon_inside_tagged_dollar_quote_does_not_split() {
        let r = split("DO $body$ BEGIN PERFORM 1; END $body$; SELECT 2;");
        assert_eq!(r.len(), 2);
        assert!(r[0].contains("$body$"));
        assert_eq!(r[1], "SELECT 2");
    }

    #[test]
    fn line_comment_does_not_split_on_internal_semicolon() {
        // The splitter preserves comment text verbatim (Postgres parses
        // and ignores comments at execute time); it only ensures the
        // `;` inside the comment isn't treated as a statement boundary.
        let r = split("SELECT 1; -- comment with ; semicolon\nSELECT 2;");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], "SELECT 1");
        assert!(r[1].ends_with("SELECT 2"));
    }

    #[test]
    fn block_comment_does_not_split_on_internal_semicolon() {
        let r = split("SELECT 1; /* hidden ; here */ SELECT 2;");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], "SELECT 1");
        assert!(r[1].contains("SELECT 2"));
    }

    #[test]
    fn block_comments_can_nest() {
        let r = split("SELECT 1; /* outer /* inner ; */ still in outer */ SELECT 2;");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], "SELECT 1");
        assert!(r[1].contains("SELECT 2"));
    }

    #[test]
    fn input_without_trailing_semicolon_is_preserved() {
        let r = split("CREATE TABLE t (id INT)");
        assert_eq!(r, vec!["CREATE TABLE t (id INT)"]);
    }

    #[test]
    fn dollar_sign_not_introducing_quote_is_passthrough() {
        // `$1` is a parameter placeholder, not a dollar quote opener.
        let r = split("SELECT $1; SELECT 2;");
        assert_eq!(r, vec!["SELECT $1", "SELECT 2"]);
    }

    // ---- apply_ddl integration ----

    #[tokio::test]
    async fn apply_ddl_runs_each_statement_in_order() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let ddl = "\
CREATE TABLE t (id INTEGER, val TEXT);
CREATE INDEX t_id_idx ON t(id);
INSERT INTO t (id, val) VALUES (1, 'a');
INSERT INTO t (id, val) VALUES (2, 'b;not-a-split');
";
        let applied = apply_ddl(&pool, ddl).await.unwrap();
        assert_eq!(applied.len(), 4);
        assert!(
            applied
                .iter()
                .all(|a| matches!(a.outcome, ApplyOutcome::Applied))
        );
        // Verify the rows really landed (esp. the row with `;` inside the literal).
        #[derive(sqlx::FromRow)]
        struct Row {
            #[allow(dead_code)]
            id: i64,
            val: String,
        }
        let rows = pool
            .fetch_all_with_binds::<Row>("SELECT id, val FROM t ORDER BY id", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].val, "b;not-a-split");
    }

    #[tokio::test]
    async fn apply_ddl_skips_already_exists_for_idempotent_rerun() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        // Pre-create the target so the second statement collides.
        pool.execute_query("CREATE TABLE t (id INTEGER)")
            .await
            .unwrap();

        let applied = apply_ddl(
            &pool,
            "CREATE TABLE t (id INTEGER); CREATE TABLE u (id INTEGER);",
        )
        .await
        .unwrap();
        assert_eq!(applied.len(), 2);
        assert_eq!(applied[0].outcome, ApplyOutcome::SkippedAlreadyExists);
        assert_eq!(applied[1].outcome, ApplyOutcome::Applied);
    }

    #[tokio::test]
    async fn apply_ddl_returns_error_on_real_failure_with_offending_sql() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        // Not an "already exists" error — a syntax error must bubble out.
        let err = apply_ddl(&pool, "CREATE TABLE t (id INTEGER); NOT VALID SQL;")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Failed to apply DDL statement"),
            "error should name what failed, got: {msg}"
        );
        assert!(
            msg.contains("NOT VALID SQL"),
            "error should include the offending statement, got: {msg}"
        );
        // The statement before the failure stayed applied (DSQL/sqlite have
        // no implicit transaction wrapping multiple DDLs).
        let rows = pool
            .fetch_all_with_binds::<(i64,)>(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='t'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows[0].0, 1);
    }

    #[tokio::test]
    async fn apply_ddl_empty_input_is_noop() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        let applied = apply_ddl(&pool, "").await.unwrap();
        assert!(applied.is_empty());

        let applied = apply_ddl(&pool, "-- only a comment\n/* and a block */\n;\n;")
            .await
            .unwrap();
        assert!(applied.is_empty());
    }
}
