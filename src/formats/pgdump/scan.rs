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
        "`COPY {schema}.{table}` block has no column list - re-run pg_dump without `--column-inserts` and ensure the source PG version emits column lists"
    )]
    MissingColumnList { schema: String, table: String },

    #[error("found multiple `COPY {schema}.{table} FROM stdin;` blocks; expected exactly one")]
    Duplicate { schema: String, table: String },

    #[error(
        "`COPY {schema}.{table}` block is missing the `\\.` terminator - file may be truncated"
    )]
    MissingTerminator { schema: String, table: String },
}

/// The byte range and metadata for a located COPY block.
#[derive(Debug, Clone)]
pub struct CopyBlock {
    /// Schema name from the COPY statement (defaults to "public" if unqualified).
    pub schema: String,
    /// Table name from the COPY statement.
    pub table: String,
    /// First byte of the first data line.
    pub data_start: u64,
    /// One past the last byte of the last data line (i.e. start of the `\.` terminator line).
    pub data_end: u64,
    /// Column names in the order declared by the COPY statement.
    pub columns: Vec<String>,
}

/// Outcome of trying to parse a single line as a COPY header.
enum HeaderMatch {
    /// Line is not a `COPY <schema>.<table> ... FROM stdin;` line at all.
    NoMatch,
    /// Header parsed but has no column list — caller raises `MissingColumnList`.
    NoColumnList { schema: String, table: String },
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
    let size = reader.size().await?;
    let mut pos = 0u64;
    let mut blocks = Vec::new();

    while pos < size {
        let line = read_line(reader, pos, size).await?;
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
            HeaderMatch::Matched {
                schema,
                table,
                columns,
            } => {
                let data_start = line_end;
                let data_end = find_terminator(reader, data_start, size)
                    .await?
                    .ok_or_else(|| ScanError::MissingTerminator {
                        schema: schema.clone(),
                        table: table.clone(),
                    })?;
                blocks.push(CopyBlock {
                    schema,
                    table,
                    data_start,
                    data_end,
                    columns,
                });
                pos = skip_line(reader, data_end, size).await?;
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
    let mut matching = list_copy_blocks(reader)
        .await?
        .into_iter()
        .filter(|b| b.schema == schema && b.table == table);

    let first = matching.next().ok_or_else(|| ScanError::NotFound {
        schema: schema.into(),
        table: table.into(),
    })?;
    if matching.next().is_some() {
        return Err(ScanError::Duplicate {
            schema: schema.into(),
            table: table.into(),
        }
        .into());
    }
    Ok(first)
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
        return HeaderMatch::NoMatch;
    };
    if !check_from_stdin(after_paren.trim_start()) {
        return HeaderMatch::NoMatch;
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

/// Cap the buffer growth in `read_line` so a pathological input (a giant
/// file with no `\n` byte at all) cannot OOM the loader. 64 MiB is far above
/// any realistic pg_dump line — even a multi-MB JSONB or BYTEA value fits.
const MAX_LINE_BYTES: usize = 64 * 1024 * 1024;

/// Read one line starting at `pos`. Returns bytes including the trailing `\n`
/// (if any). Reads in CHUNK_SIZE blocks. The returned Vec may be empty if
/// `pos >= size`. Errors if a single line exceeds `MAX_LINE_BYTES`.
async fn read_line(reader: &dyn ByteReader, pos: u64, size: u64) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut p = pos;
    while p < size {
        let end = std::cmp::min(p + CHUNK_SIZE as u64, size);
        let buf = reader.read_range(p, end).await?;
        if buf.is_empty() {
            break;
        }
        if let Some(nl) = buf.iter().position(|&b| b == b'\n') {
            if out.len().saturating_add(nl + 1) > MAX_LINE_BYTES {
                anyhow::bail!(
                    "pg_dump line at offset {pos} exceeds {MAX_LINE_BYTES} bytes \
                     (likely a malformed or non-text file)"
                );
            }
            out.extend_from_slice(&buf[..=nl]);
            return Ok(out);
        }
        if out.len().saturating_add(buf.len()) > MAX_LINE_BYTES {
            anyhow::bail!(
                "pg_dump line at offset {pos} exceeds {MAX_LINE_BYTES} bytes \
                 (likely a malformed or non-text file)"
            );
        }
        out.extend_from_slice(&buf);
        p = end;
    }
    Ok(out)
}

/// Skip past one line; return the byte offset of the start of the next line.
async fn skip_line(reader: &dyn ByteReader, pos: u64, size: u64) -> Result<u64> {
    Ok(pos + read_line(reader, pos, size).await?.len() as u64)
}

/// Find the `\.` line that terminates a COPY data section. Returns the byte
/// offset of the start of that line (i.e. the end of the data range).
async fn find_terminator(reader: &dyn ByteReader, start: u64, size: u64) -> Result<Option<u64>> {
    let mut p = start;
    while p < size {
        let line = read_line(reader, p, size).await?;
        if line.is_empty() {
            return Ok(None);
        }
        let content = strip_eol(&line);
        if content == b"\\." {
            return Ok(Some(p));
        }
        p += line.len() as u64;
    }
    Ok(None)
}

fn strip_eol(mut s: &[u8]) -> &[u8] {
    while let Some(&b'\n' | &b'\r') = s.last() {
        s = &s[..s.len() - 1];
    }
    s
}
