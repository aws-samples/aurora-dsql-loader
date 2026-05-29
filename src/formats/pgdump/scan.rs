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
/// This is the primitive for both single-table loading (Task 7.5 picks the
/// matching block) and listing/multi-table workflows (Task 12 enumerates).
/// Errors on a header that has no column list, or on a block missing its
/// `\.` terminator. Does **not** error on duplicates — that check belongs
/// to the caller, since multi-table workflows may legitimately want to load
/// each occurrence (though pg_dump never produces duplicates).
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
    let mut matching: Vec<CopyBlock> = list_copy_blocks(reader)
        .await?
        .into_iter()
        .filter(|b| b.schema == schema && b.table == table)
        .collect();

    match matching.len() {
        0 => Err(ScanError::NotFound {
            schema: schema.into(),
            table: table.into(),
        }
        .into()),
        1 => Ok(matching.pop().unwrap()),
        _ => Err(ScanError::Duplicate {
            schema: schema.into(),
            table: table.into(),
        }
        .into()),
    }
}

/// Parse a single line as `COPY <schema>.<table> (col1, col2) FROM stdin;`
/// (case-insensitive). Identifies the target schema/table without filtering;
/// callers that want a specific block compare names themselves.
fn parse_copy_header(line: &[u8]) -> HeaderMatch {
    let mut s = line;
    while let Some(&last) = s.last() {
        if last == b'\n' || last == b'\r' {
            s = &s[..s.len() - 1];
        } else {
            break;
        }
    }
    let Ok(s) = std::str::from_utf8(s) else {
        return HeaderMatch::NoMatch;
    };
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

    let Some(close) = after_table.find(')') else {
        return HeaderMatch::NoMatch;
    };
    let inner = &after_table[1..close];
    let after_paren = after_table[close + 1..].trim_start();
    if !check_from_stdin(after_paren) {
        return HeaderMatch::NoMatch;
    }

    let columns = inner
        .split(',')
        .map(|c| {
            let c = c.trim();
            if c.starts_with('"') && c.ends_with('"') && c.len() >= 2 {
                c[1..c.len() - 1].replace("\"\"", "\"")
            } else {
                c.to_string()
            }
        })
        .collect();
    HeaderMatch::Matched {
        schema,
        table,
        columns,
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

/// Read either a quoted identifier "..." (with "" as escaped quote) or a bare
/// identifier matching [A-Za-z0-9_]+. Returns (identifier, rest).
fn read_identifier(s: &str) -> (String, &str) {
    let bytes = s.as_bytes();
    if bytes.first() == Some(&b'"') {
        let mut out = String::new();
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == b'"' {
                if bytes.get(i + 1) == Some(&b'"') {
                    out.push('"');
                    i += 2;
                } else {
                    return (out, &s[i + 1..]);
                }
            } else {
                out.push(bytes[i] as char);
                i += 1;
            }
        }
        (out, "")
    } else {
        let end = bytes
            .iter()
            .position(|&b| !is_ident_byte(b))
            .unwrap_or(bytes.len());
        (s[..end].to_string(), &s[end..])
    }
}

fn is_ident_byte(b: u8) -> bool {
    matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_')
}

/// Read one line starting at `pos`. Returns bytes including the trailing `\n`
/// (if any). Reads in CHUNK_SIZE blocks. The returned Vec may be empty if pos >= size.
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
            out.extend_from_slice(&buf[..=nl]);
            return Ok(out);
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
        let mut content = line.as_slice();
        while let Some(&last) = content.last() {
            if last == b'\n' || last == b'\r' {
                content = &content[..content.len() - 1];
            } else {
                break;
            }
        }
        if content == b"\\." {
            return Ok(Some(p));
        }
        p += line.len() as u64;
    }
    Ok(None)
}
