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
use anyhow::{Context, Result};
use aurora_dsql_sqlx_connector::{DsqlError, OCCRetryConfig, retry_on_occ};

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
    /// Statement targeted an object that already exists (see
    /// [`is_already_exists`] for the SQLSTATE allowlist) and was skipped
    /// to keep the migrate flow re-runnable.
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
pub async fn apply_ddl(pool: &Pool, ddl: &str) -> Result<Vec<AppliedStatement>> {
    let statements = split_sql_statements(ddl);
    let total = statements.len();
    let occ = OCCRetryConfig::default();
    tracing::info!(statements = total, "applying DDL");
    let mut applied = Vec::with_capacity(total);
    for (i, stmt) in statements.into_iter().enumerate() {
        tracing::debug!(
            statement = i + 1,
            total,
            preview = %safe_for_terminal(&stmt),
            "applying DDL statement"
        );
        // Retry on DSQL's optimistic-concurrency conflict (OC000/OC001/40001):
        // a CREATE/ALTER whose snapshot predates a just-committed catalog change
        // (e.g. a DROP of the same object, or concurrent DDL on a busy cluster)
        // is rejected and is safe to re-run — each statement is its own txn and
        // the already-exists skip below keeps re-application idempotent.
        match apply_one(pool, &occ, &stmt).await {
            Ok(()) => applied.push(AppliedStatement {
                sql: stmt,
                outcome: ApplyOutcome::Applied,
            }),
            Err(e) if is_already_exists(&e) => applied.push(AppliedStatement {
                sql: stmt,
                outcome: ApplyOutcome::SkippedAlreadyExists,
            }),
            Err(e) => {
                return Err(anyhow::Error::new(e)).with_context(|| {
                    format!(
                        "Failed to apply DDL statement: {summary}",
                        summary = safe_for_terminal(&stmt)
                    )
                });
            }
        }
    }
    Ok(applied)
}

/// Run one DDL statement with OCC retry, normalizing the connector's
/// [`DsqlError`] back to the underlying [`sqlx::Error`] so the caller's
/// already-exists classification (which inspects SQLSTATE on `sqlx::Error`)
/// is unchanged. OCC-exhausted and non-database errors surface as a synthetic
/// protocol-level `sqlx::Error` carrying the message.
async fn apply_one(pool: &Pool, occ: &OCCRetryConfig, stmt: &str) -> Result<(), sqlx::Error> {
    retry_on_occ(occ, || pool.execute_query(stmt))
        .await
        .map_err(|e| match e {
            DsqlError::DatabaseError(inner) | DsqlError::ConnectionError(inner) => inner,
            other => sqlx::Error::Protocol(other.to_string()),
        })
}

/// Whether a sqlx error represents a target object that already exists
/// (so the migrate flow can skip it for idempotency on re-run).
///
/// On DSQL, the reachable shapes are:
/// - `42P07` — `duplicate_table` (also emitted for duplicate index;
///   ALTER ADD CONSTRAINT and CREATE TYPE ENUM are `0A000` unsupported,
///   so the index/constraint/type codes don't surface from pg_dump).
/// - `42P06` — `duplicate_schema`.
/// - `42701` — `duplicate_column`.
/// - `42723` — `duplicate_function` (pg_dump emits `CREATE FUNCTION
///   ... LANGUAGE sql` and DSQL surfaces this on re-apply).
/// - `42710` — `duplicate_object` on Postgres; kept defensively for
///   parity though not currently reachable through DSQL.
///
/// SQLite uses numeric error codes (`1` for any logic error, etc.) that
/// don't carry the same level of detail, so we fall back to a
/// `"already exists"` message-string match for it.
fn is_already_exists(err: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db_err) = err else {
        return false;
    };
    let code = db_err.code();
    // Postgres-shaped SQLSTATEs are exactly 5 characters and (for the
    // duplicate-object family) start with `42`. When we see one, the
    // allowlist is authoritative — we MUST NOT also message-match, or a
    // future class-42 SQLSTATE we have not allowlisted (e.g. role-related
    // duplicates) whose text happens to contain "already exists" would be
    // silently treated as a successful skip.
    if let Some(c) = code.as_deref()
        && c.len() == 5
        && c.starts_with("42")
    {
        return matches!(c, "42P07" | "42P06" | "42710" | "42701" | "42723");
    }
    // Non-Postgres backend (e.g. SQLite, used by in-process tests, returns
    // numeric codes like "1") or no code at all: fall back to the stable
    // message-string shape.
    db_err
        .message()
        .to_ascii_lowercase()
        .contains("already exists")
}

/// Make a SQL fragment safe to embed in a terminal-bound error message
/// or log line: keep chars whose start byte is below 200 (so the head
/// can extend up to 3 bytes past 200 when a multi-byte char starts just
/// before the cap — real `pg_dump` statements can be MB-sized via
/// embedded function bodies or large enum lists) and replace control
/// bytes / Unicode bidi-format codepoints with `?` so a multi-line SQL
/// excerpt in an anyhow chain doesn't re-flow the operator's terminal.
/// Reuses the same character classes that `is_unsafe_for_listing` flags
/// in the `list-tables` path.
fn safe_for_terminal(stmt: &str) -> String {
    const MAX: usize = 200;
    let truncated = if stmt.len() > MAX {
        format!(
            "{head}... ({total} bytes total)",
            head = stmt
                .char_indices()
                .take_while(|(idx, _)| *idx < MAX)
                .map(|(_, c)| c)
                .collect::<String>(),
            total = stmt.len()
        )
    } else {
        stmt.to_owned()
    };
    truncated
        .chars()
        .map(|c| {
            if c.is_control() || crate::runner::is_bidi_or_format_char(c) {
                '?'
            } else {
                c
            }
        })
        .collect()
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
            b'\'' => i = skip_quoted(bytes, i, b'\''),
            b'"' => i = skip_quoted(bytes, i, b'"'),
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

/// Advance past `q...q` starting at `i` (caller has confirmed `bytes[i] ==
/// q`). The doubled-character form (`''` inside `'...'` or `""` inside
/// `"..."`) is the embedded-quote escape, matching Postgres syntax for both
/// single-quoted strings and double-quoted identifiers. Returns the index
/// of the byte after the closing quote, or `bytes.len()` on unterminated
/// input.
fn skip_quoted(bytes: &[u8], i: usize, q: u8) -> usize {
    debug_assert_eq!(bytes[i], q);
    let mut j = i + 1;
    while j < bytes.len() {
        if bytes[j] == q {
            if bytes.get(j + 1) == Some(&q) {
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
/// block comments, so we track depth. Returns the index of the byte
/// after `*/`, or `bytes.len()` on unterminated input — same shape as
/// [`skip_quoted`] and [`skip_line_comment`] so the splitter never
/// re-enters the loop on byte content the comment was supposed to
/// consume (a stray `;` inside an unterminated `/* ... */` would
/// otherwise be misread as a statement boundary).
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
    if depth == 0 { j } else { bytes.len() }
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

    // ---- malformed-input shapes: splitter must terminate, not panic ----

    #[test]
    fn unterminated_single_quote_consumes_to_eof() {
        let r = split("INSERT INTO t VALUES ('unterminated;");
        // Whatever the splitter decides about this malformed input,
        // it must terminate and not panic.
        assert!(r.len() <= 1, "unterminated quote: {r:?}");
    }

    #[test]
    fn unterminated_dollar_quote_consumes_to_eof() {
        // Documented behavior: an unterminated `$tag$` rolls forward to
        // EOF rather than splitting on every `;` after it.
        let r = split("CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; SELECT 2;");
        assert_eq!(
            r.len(),
            1,
            "unterminated dollar quote produced split: {r:?}"
        );
    }

    #[test]
    fn unterminated_block_comment_terminates_without_panic() {
        // depth never returns to 0; the loop must still exit at EOF.
        let r = split("SELECT 1; /* unterminated comment");
        assert!(!r.is_empty());
        assert_eq!(r[0], "SELECT 1");
    }

    #[test]
    fn unterminated_block_comment_does_not_split_on_inner_semicolon() {
        // Regression: `skip_block_comment` previously returned the
        // pre-EOF index when depth never hit 0, so the splitter
        // re-entered the loop on bytes inside the unterminated
        // comment. A `;` there was misread as a statement boundary.
        let r = split("SELECT 1; /* unterminated; SELECT 2");
        assert_eq!(r, vec!["SELECT 1"]);
    }

    #[test]
    fn line_comment_without_trailing_newline_terminates() {
        // EOF inside a line comment must not loop or OOB.
        let r = split("SELECT 1; -- comment without newline");
        assert_eq!(r, vec!["SELECT 1"]);
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

    /// Regression guard for the substring-fallback shape in
    /// `is_already_exists`: the SQLite / no-code path must not accept
    /// arbitrary errors whose message happens to contain "already
    /// exists". A SQLite "table … already exists" is a true skip; a
    /// SQLite "no such table" is not. Pinning the negative shape
    /// prevents a regression that broadens the substring match.
    #[tokio::test]
    async fn apply_ddl_substring_fallback_is_anchored_to_already_exists() {
        let pool = Pool::sqlite_in_memory().await.unwrap();
        // Drive an error whose message does NOT contain "already exists".
        // SQLite returns "no such table" for `INSERT INTO missing` — the
        // substring fallback must NOT classify this as a skip.
        let err = apply_ddl(&pool, "INSERT INTO missing_table VALUES (1);")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Failed to apply DDL statement"),
            "unrelated SQLite error should bubble out as Err, got: {msg}"
        );
        // The positive "table X already exists" case is covered by
        // `apply_ddl_skips_already_exists_for_idempotent_rerun`. This test
        // pins the negative shape — what the class-42 SQLSTATE allowlist
        // gate was added to protect against.
    }

    /// `safe_for_terminal` truncates large SQL excerpts so a multi-MB
    /// statement (e.g. a large enum list or function body) doesn't flood
    /// an anyhow chain. Pin both the ASCII-tail and the multi-byte-tail
    /// boundary semantics so a future tweak doesn't silently change error
    /// message shape.
    #[test]
    fn safe_for_terminal_truncates_large_excerpts_with_byte_count() {
        // ASCII tail: head is exactly 200 bytes, suffix names original size.
        let stmt = "x".repeat(250);
        let out = safe_for_terminal(&stmt);
        let head = out.split_once("...").unwrap().0;
        assert_eq!(head.len(), 200);
        assert_eq!(head, "x".repeat(200));
        assert!(out.contains("(250 bytes total)"));

        // Multi-byte tail: a 2-byte char starting at byte 199 is kept whole
        // (predicate is on start index, so head extends to 201 bytes).
        // Pinning this so a future "true floor_char_boundary" refactor is
        // a deliberate spec change, not silent drift.
        let mut multi = "a".repeat(199);
        multi.push('é');
        multi.push_str(&"b".repeat(50));
        let out = safe_for_terminal(&multi);
        let head = out.split_once("...").unwrap().0;
        assert_eq!(head.len(), 201);
        assert!(head.ends_with('é'));

        // Short inputs round-trip unchanged.
        assert_eq!(
            safe_for_terminal("CREATE TABLE t (id INT)"),
            "CREATE TABLE t (id INT)"
        );
    }

    /// The other half of `safe_for_terminal`: control bytes (ANSI escape)
    /// and Unicode bidi/format codepoints get replaced with `?`. A refactor
    /// that drops the replace pass would not fail the truncation test
    /// alone, so pin the defang behavior independently.
    #[test]
    fn safe_for_terminal_replaces_control_and_bidi_chars() {
        let input = "ok\x1b[31mred\x1b[0m \u{202e}drow \u{200b}zwsp";
        let out = safe_for_terminal(input);
        assert!(!out.contains('\x1b'), "control byte leaked: {out:?}");
        assert!(!out.contains('\u{202e}'), "RTL override leaked: {out:?}");
        assert!(!out.contains('\u{200b}'), "ZWSP leaked: {out:?}");
        assert!(out.contains("ok"), "plain text dropped: {out:?}");
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
