//! Locate the `COPY <schema>.<table> (cols...) FROM stdin;` block in a pg_dump file.
//!
//! Streams the file in a single forward pass reading line-by-line via the
//! ByteReader. Memory is bounded by the longest line (in practice, a few KB
//! for header, MB for huge data lines, but we never buffer the whole file).

use crate::config::CHUNK_SIZE;
use crate::io::ByteReader;
use anyhow::Result;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ScanError {
    #[error("no `COPY {schema}.{table} ... FROM stdin;` block found in pg_dump file")]
    NotFound { schema: String, table: String },

    #[error(
        "`COPY {schema}.{table}` block has no column list - the loader binds fields positionally and requires explicit column names. Ensure the source pg_dump emits `COPY t (cols) FROM stdin;` rather than `COPY t FROM stdin;`"
    )]
    MissingColumnList { schema: String, table: String },

    #[error("found multiple `COPY {schema}.{table} FROM stdin;` blocks; expected exactly one")]
    Duplicate { schema: String, table: String },

    #[error(
        "`COPY {schema}.{table}` block is missing the `\\.` terminator - file may be truncated"
    )]
    MissingTerminator { schema: String, table: String },

    #[error(
        "`COPY {schema}.{table}` block has a malformed column list (unterminated quoted identifier, missing close paren, or empty list)"
    )]
    MalformedColumnList { schema: String, table: String },

    #[error(
        "source file appears to be a {variant} pg_dump archive, which is not supported. \
         Re-run pg_dump with the plain text format flag (`-Fp`, the default) and \
         `--data-only`, e.g. `pg_dump -Fp --data-only --table=<t> <db> > dump.sql`."
    )]
    NonPlainPgDump { variant: &'static str },
}

/// Fast pre-flight: read a few bytes from the start of the source and reject
/// non-plain pg_dump archives with a precise diagnostic instead of letting the
/// caller hit a confusing "no COPY block found" error after streaming the
/// whole file.
///
/// Detects:
/// - **`-Fc` custom format** — magic `PGDMP` at offset 0.
/// - **`-Fd` directory format** — typically pointed at a directory, but a
///   user pointing at the contained `toc.dat` file would also see `PGDMP`
///   magic. The diagnostic names "custom or directory" together because
///   they share the same on-disk magic.
///
/// `-Ft` (tar) is harder to identify cheaply (the `ustar` magic sits at
/// offset 257); we deliberately do not check for it here. A tar dump that
/// reaches the COPY scanner will fail with a generic parse error, which is
/// acceptable since `-Ft` is rare in practice.
async fn detect_non_plain_format(reader: &dyn ByteReader) -> Result<()> {
    const MAGIC: &[u8] = b"PGDMP";
    let size = reader.size().await?;
    if size < MAGIC.len() as u64 {
        return Ok(());
    }
    let head = reader.read_range(0, MAGIC.len() as u64).await?;
    if head.starts_with(MAGIC) {
        return Err(ScanError::NonPlainPgDump {
            variant: "custom or directory (-Fc/-Fd)",
        }
        .into());
    }
    Ok(())
}

/// The byte range and metadata for a located COPY block.
///
/// The block spans `[header_start, block_end)` in the source file:
/// `header_start` ... `data_start` is the `COPY ... FROM stdin;` line;
/// `data_start` ... `data_end` is the data payload (no terminator);
/// `data_end` ... `block_end` is the `\.` terminator line.
/// The DDL between blocks lives in `[prev_block.block_end, next_block.header_start)`.
#[derive(Debug, Clone)]
pub struct CopyBlock {
    /// Schema name from the COPY statement (defaults to "public" if unqualified).
    pub schema: String,
    /// Table name from the COPY statement.
    pub table: String,
    /// First byte of the `COPY ... FROM stdin;` header line. Used by
    /// `extract_ddl` to slice out the DDL prefix that precedes this block.
    pub header_start: u64,
    /// First byte of the first data line (one past the header line's newline).
    pub data_start: u64,
    /// One past the last byte of the last data line (i.e. start of the `\.` terminator line).
    pub data_end: u64,
    /// One past the trailing newline of the `\.` terminator line. Used by
    /// `extract_ddl` to begin the next DDL slice immediately after the
    /// terminator without re-scanning.
    pub block_end: u64,
    /// Column names in the order declared by the COPY statement.
    pub columns: Vec<String>,
}

/// Outcome of trying to parse a single line as a COPY header.
enum HeaderMatch {
    /// Line is not a `COPY <schema>.<table> ... FROM stdin;` line at all.
    NoMatch,
    /// Header parsed but has no column list — caller raises `MissingColumnList`.
    NoColumnList { schema: String, table: String },
    /// Header started parsing as `COPY <schema>.<table> (` but the column list
    /// is malformed — caller raises `MalformedColumnList`. Distinct from
    /// `NoMatch` so the operator gets a specific diagnostic instead of a
    /// downstream "table not found" or "missing terminator".
    MalformedColumnList { schema: String, table: String },
    /// Header parsed with a column list.
    Matched {
        schema: String,
        table: String,
        columns: Vec<String>,
    },
}

/// List **all** `COPY ... FROM stdin;` blocks in the file in source order.
///
/// Used by `find_copy_block` (single-table load) and `list_pgdump_tables`
/// (multi-table discovery). Errors on a header that has no column list, or
/// on a block missing its `\.` terminator. Does **not** error on duplicates
/// — that check belongs to the caller, since multi-table workflows may
/// legitimately want to load each occurrence (though pg_dump never produces
/// duplicates in practice).
pub async fn list_copy_blocks(reader: &dyn ByteReader) -> Result<Vec<CopyBlock>> {
    detect_non_plain_format(reader).await?;
    let size = reader.size().await?;
    let mut pos = 0u64;
    let mut blocks = Vec::new();

    while pos < size {
        let line = read_line(reader, pos, size, MAX_HEADER_LINE_BYTES).await?;
        if line.is_empty() {
            break;
        }
        let line_end = pos + line.len() as u64;

        match parse_copy_header(&line) {
            HeaderMatch::NoMatch => {
                pos = line_end;
            }
            HeaderMatch::NoColumnList { schema, table } => {
                return Err(ScanError::MissingColumnList { schema, table }.into());
            }
            HeaderMatch::MalformedColumnList { schema, table } => {
                return Err(ScanError::MalformedColumnList { schema, table }.into());
            }
            HeaderMatch::Matched {
                schema,
                table,
                columns,
            } => {
                let header_start = pos;
                let data_start = line_end;
                let data_end = find_terminator(reader, data_start, size)
                    .await?
                    .ok_or_else(|| ScanError::MissingTerminator {
                        schema: schema.clone(),
                        table: table.clone(),
                    })?;
                let block_end = skip_line(reader, data_end, size).await?;
                blocks.push(CopyBlock {
                    schema,
                    table,
                    header_start,
                    data_start,
                    data_end,
                    block_end,
                    columns,
                });
                pos = block_end;
            }
        }
    }

    Ok(blocks)
}

/// Locate the unique COPY block matching the given schema and table.
/// Thin wrapper over `list_copy_blocks` that filters and enforces uniqueness.
pub async fn find_copy_block(
    reader: &dyn ByteReader,
    schema: &str,
    table: &str,
) -> Result<CopyBlock> {
    let mut matches = list_copy_blocks(reader)
        .await?
        .into_iter()
        .filter(|b| b.schema == schema && b.table == table);

    let first = matches.next().ok_or_else(|| ScanError::NotFound {
        schema: schema.into(),
        table: table.into(),
    })?;
    if matches.next().is_some() {
        return Err(ScanError::Duplicate {
            schema: schema.into(),
            table: table.into(),
        }
        .into());
    }
    Ok(first)
}

/// Extract the non-data (DDL) bytes from a pg_dump file: the prefix before
/// the first COPY block, the DDL between blocks, and the suffix after the
/// last block. Designed so the result can be fed directly to `dsql_lint::fix_sql`
/// to produce DSQL-compatible DDL for the `migrate` subcommand.
///
/// Returns the bytes as a UTF-8 string. pg_dump writes the textual section of
/// its plain-format output as UTF-8 (unrelated to the COPY data encoding);
/// any non-UTF-8 byte in the DDL slices indicates either format corruption or
/// a custom-format archive that the scanner should already have rejected.
///
/// Strips two classes of pg_dump preamble that DSQL refuses:
/// `\restrict` / `\unrestrict` psql meta-commands (PG17+ psql client
/// directives that lock object names within a script), and the
/// `SET <param> = ...;` session GUCs pg_dump emits at the top of
/// every dump (`statement_timeout`, `lock_timeout`, `client_encoding`,
/// etc.) which DSQL rejects with
/// "setting configuration parameter ... not supported".
///
/// `blocks` is the slice returned by [`list_copy_blocks`]; pass `&[]` for a
/// schema-only dump (no data) and the entire file is returned as DDL.
pub async fn extract_ddl(reader: &dyn ByteReader, blocks: &[CopyBlock]) -> Result<String> {
    let size = reader.size().await?;
    let mut out: Vec<u8> = Vec::new();

    let mut cursor = 0u64;
    for block in blocks {
        if block.header_start > cursor {
            out.extend_from_slice(&reader.read_range(cursor, block.header_start).await?);
        }
        cursor = block.block_end;
    }
    if cursor < size {
        out.extend_from_slice(&reader.read_range(cursor, size).await?);
    }

    let raw = String::from_utf8(out).map_err(|e| {
        anyhow::anyhow!(
            "pg_dump DDL section is not valid UTF-8 at byte {}: {e}",
            e.utf8_error().valid_up_to()
        )
    })?;
    Ok(strip_pgdump_preamble(&raw))
}

/// pg_dump session GUCs DSQL rejects. Sourced from `pg_dump.c`'s
/// `setupDumpWorker` / preamble emission — these are the always-on
/// parameters every plain-format dump prints. Anything outside this
/// allowlist (e.g. a customer-authored `SET search_path = ...;`) is
/// preserved so a real apply error surfaces if DSQL doesn't accept it.
const PGDUMP_REJECTED_SET_PARAMS: &[&str] = &[
    "statement_timeout",
    "lock_timeout",
    "idle_in_transaction_session_timeout",
    "transaction_timeout",
    "client_encoding",
    "standard_conforming_strings",
    "check_function_bodies",
    "xmloption",
    "client_min_messages",
    "row_security",
    "default_tablespace",
    "default_table_access_method",
    "default_with_oids",
];

/// Drop pg_dump preamble lines DSQL refuses. Tightly anchored to
/// start-of-line; the same caveat as the prior `\restrict` strip
/// applies — a `$body$...$body$` whose interior begins with one of
/// these tokens at column 0 would be (incorrectly) stripped, but
/// pg_dump does not produce such bodies.
fn strip_pgdump_preamble(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for line in input.split_inclusive('\n') {
        if is_strippable_preamble_line(line) {
            continue;
        }
        out.push_str(line);
    }
    out
}

/// Whether `line` is one of the pg_dump preamble shapes we drop:
/// `\restrict ...` / `\unrestrict ...` psql meta-commands, or
/// `SET <param> = ...;` for a `<param>` in [`PGDUMP_REJECTED_SET_PARAMS`].
fn is_strippable_preamble_line(line: &str) -> bool {
    for cmd in ["\\restrict", "\\unrestrict"] {
        if let Some(rest) = line.strip_prefix(cmd) {
            let next = rest.chars().next();
            if matches!(next, None | Some(' ' | '\t' | '\n' | '\r')) {
                return true;
            }
        }
    }
    is_pgdump_session_set_line(line)
}

/// Match `SET <param>` (case-insensitive on `SET`, exact on the param
/// name) at the very start of the line, with `<param>` in the
/// allowlist. The remainder of the line is not parsed — the line is
/// dropped wholesale because pg_dump always emits these as a single
/// `SET ident = value;` per line.
fn is_pgdump_session_set_line(line: &str) -> bool {
    let trimmed_eol = line.trim_end_matches(['\n', '\r']);
    let after_set = match trimmed_eol.as_bytes().get(..3) {
        Some(prefix) if prefix.eq_ignore_ascii_case(b"SET") => &trimmed_eol[3..],
        _ => return false,
    };
    let rest = after_set.trim_start_matches([' ', '\t']);
    if rest.len() == after_set.len() {
        // No whitespace after `SET` → not a SET statement.
        return false;
    }
    // Read the next identifier (alpha + alpha/digit/underscore).
    let mut end = 0;
    for (i, c) in rest.char_indices() {
        let ok = if i == 0 {
            c.is_ascii_alphabetic() || c == '_'
        } else {
            c.is_ascii_alphanumeric() || c == '_'
        };
        if !ok {
            end = i;
            break;
        }
        end = i + c.len_utf8();
    }
    let ident = &rest[..end];
    if ident.is_empty() {
        return false;
    }
    PGDUMP_REJECTED_SET_PARAMS
        .iter()
        .any(|p| ident.eq_ignore_ascii_case(p))
}

/// Parse a single line as `COPY <schema>.<table> (col1, col2) FROM stdin;`
/// (case-insensitive). Identifies the target schema/table without filtering;
/// callers that want a specific block compare names themselves.
fn parse_copy_header(line: &[u8]) -> HeaderMatch {
    let Ok(s) = std::str::from_utf8(line) else {
        return HeaderMatch::NoMatch;
    };
    let s = s.trim_end_matches(['\n', '\r']);
    let trimmed = s.trim_start();
    if !starts_with_ci(trimmed, "copy ") {
        return HeaderMatch::NoMatch;
    }
    let rest = &trimmed["copy ".len()..];

    // Parse `<schema>.<table>` (each part optionally double-quoted) or bare `<table>`.
    let (first_id, after_first) = read_identifier(rest);
    if first_id.is_empty() {
        return HeaderMatch::NoMatch;
    }
    let (schema, table, after_table) = if let Some(rest) = after_first.strip_prefix('.') {
        let (second_id, after_second) = read_identifier(rest);
        if second_id.is_empty() {
            return HeaderMatch::NoMatch;
        }
        (first_id, second_id, after_second)
    } else {
        ("public".to_string(), first_id, after_first)
    };

    let after_table = after_table.trim_start();
    if !after_table.starts_with('(') {
        // pg_dump always emits the column list. A bare `COPY t FROM stdin;` is
        // a parsed block but we cannot bind fields without column names —
        // surface that distinctly so the caller can raise MissingColumnList.
        return if check_from_stdin(after_table) {
            HeaderMatch::NoColumnList { schema, table }
        } else {
            HeaderMatch::NoMatch
        };
    }

    let Some((columns, after_paren)) = read_column_list(&after_table[1..]) else {
        // Reaching here means the line started with `COPY <schema>.<table> (`
        // — i.e. it IS a COPY header — but the column list itself is broken.
        // Surface that distinctly so the operator gets a precise diagnostic
        // instead of a downstream "table not found" / "missing terminator".
        return HeaderMatch::MalformedColumnList { schema, table };
    };
    if !check_from_stdin(after_paren.trim_start()) {
        return HeaderMatch::MalformedColumnList { schema, table };
    }

    HeaderMatch::Matched {
        schema,
        table,
        columns,
    }
}

/// Parse a quote-aware comma-separated column list ending in `)`. Returns
/// `(columns, rest_after_close_paren)` or `None` if the closing paren is
/// missing. Quoted identifiers may legally contain `,`, `)`, or `"` (as
/// `""`), so we cannot rely on `find(')')`/`split(',')`.
fn read_column_list(s: &str) -> Option<(Vec<String>, &str)> {
    let mut columns = Vec::new();
    let mut rest = s;
    loop {
        rest = rest.trim_start();
        let (col, after) = read_identifier(rest);
        if col.is_empty() {
            return None;
        }
        columns.push(col);
        let after = after.trim_start();
        if let Some(after) = after.strip_prefix(',') {
            rest = after;
            continue;
        }
        if let Some(after) = after.strip_prefix(')') {
            return Some((columns, after));
        }
        return None;
    }
}

fn check_from_stdin(s: &str) -> bool {
    let s = s.trim_start();
    if !starts_with_ci(s, "from stdin") {
        return false;
    }
    let after = s["from stdin".len()..].trim_start();
    after.is_empty() || after.starts_with(';')
}

fn starts_with_ci(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix)
}

/// Read either a quoted identifier `"..."` (with `""` as escaped quote) or a
/// bare identifier matching `[A-Za-z0-9_]+`. Returns `(identifier, rest)`.
///
/// PG quoted identifiers may contain any non-`"` character including
/// non-ASCII UTF-8 (e.g. `"naïve"`), so the quoted branch slices the source
/// `&str` directly — casting individual bytes to `char` would mangle multi-byte
/// sequences into Latin-1 codepoints.
fn read_identifier(s: &str) -> (String, &str) {
    let bytes = s.as_bytes();
    if bytes.first() == Some(&b'"') {
        // Walk byte-by-byte to find the closing quote (which is always ASCII
        // 0x22, never the second byte of a UTF-8 sequence). Doubled `""`
        // escapes a literal quote inside the identifier.
        let mut i = 1;
        let mut has_escaped = false;
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if bytes.get(i + 1) == Some(&b'"') {
                    has_escaped = true;
                    i += 2;
                } else {
                    let inner = &s[1..i];
                    let decoded = if has_escaped {
                        inner.replace("\"\"", "\"")
                    } else {
                        inner.to_string()
                    };
                    return (decoded, &s[i + 1..]);
                }
            } else {
                i += 1;
            }
        }
        // Unterminated quoted identifier — treat as no match.
        (String::new(), s)
    } else {
        let end = bytes
            .iter()
            .position(|&b| !(b.is_ascii_alphanumeric() || b == b'_'))
            .unwrap_or(bytes.len());
        (s[..end].to_string(), &s[end..])
    }
}

/// Cap on per-line buffer growth when scanning the SQL preamble for `COPY`
/// headers. Real pg_dump headers are kilobytes; 1 MiB is comfortable headroom
/// while keeping a no-newline pathological input from OOMing the loader.
const MAX_HEADER_LINE_BYTES: usize = 1024 * 1024;

/// Cap on per-line buffer growth when scanning COPY data lines for the `\.`
/// terminator. PostgreSQL TOAST allows attribute values up to ~1 GB; a JSONB
/// or BYTEA column on a single COPY line can legitimately be very large, so
/// this cap is 1024× the header cap. The bound exists only to backstop a
/// truncated/malformed file with no newline at all — if you genuinely have a
/// single row larger than 1 GB, this cap will need raising. Do not
/// "harmonize" the two: header lines are kilobytes, data lines can carry
/// whole TOASTed values.
const MAX_DATA_LINE_BYTES: usize = 1024 * 1024 * 1024;

fn line_too_long(pos: u64, cap: usize) -> anyhow::Error {
    anyhow::anyhow!(
        "pg_dump line at offset {pos} exceeds {cap} bytes \
         (likely a malformed or non-text file)"
    )
}

/// Read one line starting at `pos`. Returns bytes including the trailing `\n`
/// (if any). Reads in CHUNK_SIZE blocks. The returned Vec may be empty if
/// `pos >= size`. Errors if a single line exceeds `cap` bytes.
async fn read_line(reader: &dyn ByteReader, pos: u64, size: u64, cap: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut p = pos;
    while p < size {
        let end = std::cmp::min(p + CHUNK_SIZE as u64, size);
        let buf = reader.read_range(p, end).await?;
        if buf.is_empty() {
            break;
        }
        if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            if out.len().saturating_add(nl + 1) > cap {
                return Err(line_too_long(pos, cap));
            }
            out.extend_from_slice(&buf[..=nl]);
            return Ok(out);
        }
        if out.len().saturating_add(buf.len()) > cap {
            return Err(line_too_long(pos, cap));
        }
        out.extend_from_slice(&buf);
        p = end;
    }
    Ok(out)
}

/// Skip past one line; return the byte offset of the start of the next line.
/// Used only for stepping past `\.` terminators between COPY blocks, so the
/// generous data-line cap is appropriate.
async fn skip_line(reader: &dyn ByteReader, pos: u64, size: u64) -> Result<u64> {
    Ok(pos
        + read_line(reader, pos, size, MAX_DATA_LINE_BYTES)
            .await?
            .len() as u64)
}

/// Find the `\.` line that terminates a COPY data section. Returns the byte
/// offset of the start of that line (i.e. the end of the data range).
///
/// Streams the data range chunk-by-chunk and tracks the first two bytes,
/// last byte, and span of each line — enough to recognize a `\.` terminator
/// (with optional `\r` before the `\n`) without re-reading or buffering the
/// line body. A TOASTed value can legitimately be hundreds of MB and we do not
/// need its content, only its newline boundaries. Bounded by
/// `MAX_DATA_LINE_BYTES` only as a malformed-file backstop on the per-line
/// span between newlines.
async fn find_terminator(reader: &dyn ByteReader, start: u64, size: u64) -> Result<Option<u64>> {
    let mut line_start = start;
    let mut p = start;
    let mut span: usize = 0;
    let mut first_byte: u8 = 0;
    let mut second_byte: u8 = 0;
    let mut last_byte: u8 = 0;
    while p < size {
        let end = std::cmp::min(p + CHUNK_SIZE as u64, size);
        let buf = reader.read_range(p, end).await?;
        if buf.is_empty() {
            break;
        }
        for (i, &b) in buf.iter().enumerate() {
            if b == b'\n' {
                if is_terminator_pattern(span, first_byte, second_byte, last_byte) {
                    return Ok(Some(line_start));
                }
                line_start = p + i as u64 + 1;
                span = 0;
            } else {
                if span == 0 {
                    first_byte = b;
                } else if span == 1 {
                    second_byte = b;
                }
                last_byte = b;
                span += 1;
                if span > MAX_DATA_LINE_BYTES {
                    return Err(line_too_long(line_start, MAX_DATA_LINE_BYTES));
                }
            }
        }
        p = end;
    }
    // EOF without trailing newline: pg_dump always emits `\.\n`, but a
    // hand-truncated or interrupted file may end exactly at `\.` with no
    // newline. Accept only the exact `\.` shape — a `\.\r` at EOF would
    // require the file to end mid-CRLF, which indicates corruption, not a
    // valid terminator.
    if span == 2 && first_byte == b'\\' && second_byte == b'.' {
        return Ok(Some(line_start));
    }
    Ok(None)
}

fn is_terminator_pattern(span: usize, first: u8, second: u8, last: u8) -> bool {
    (span == 2 && first == b'\\' && second == b'.')
        || (span == 3 && first == b'\\' && second == b'.' && last == b'\r')
}
