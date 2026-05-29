//! Decoder for PostgreSQL COPY text format escape sequences.
//!
//! Reference: https://www.postgresql.org/docs/current/sql-copy.html
//! "Text Format" section: a backslash introduces an escape sequence.

/// Sentinel value returned for the `\N` escape (SQL NULL).
///
/// We use a distinct return type rather than `Option<String>` because:
/// 1. The downstream `Record { fields: Vec<String> }` shape stores strings,
/// 2. The DSQL writer treats empty strings as empty strings, not NULLs,
/// 3. Existing CSV/TSV behavior is to pass empty strings through; we match it.
///
/// On encountering `\N` we return an empty string. This matches how the
/// existing CSV/TSV path handles missing fields and avoids a wider refactor.
/// If true NULL handling is needed later, this is the central place to revisit.
#[derive(Debug, PartialEq, Eq)]
pub enum DecodedField {
    /// A normal value (possibly empty).
    Value(String),
    /// PG's `\N` literal — the field was SQL NULL in the source database.
    Null,
}

/// Decode one COPY-text-format field. Input is the raw bytes between
/// tabs (or between start-of-line and tab, etc.). The whole-field literal
/// `\N` is returned as `DecodedField::Null`; everything else is `Value`.
pub fn decode_field(input: &[u8]) -> DecodedField {
    if input == b"\\N" {
        return DecodedField::Null;
    }
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let next = match input.get(i + 1) {
            Some(&n) => n,
            None => {
                out.push(b'\\');
                i += 1;
                continue;
            }
        };
        match next {
            b'b' => {
                out.push(0x08);
                i += 2;
            }
            b'f' => {
                out.push(0x0c);
                i += 2;
            }
            b'n' => {
                out.push(b'\n');
                i += 2;
            }
            b'r' => {
                out.push(b'\r');
                i += 2;
            }
            b't' => {
                out.push(b'\t');
                i += 2;
            }
            b'v' => {
                out.push(0x0b);
                i += 2;
            }
            b'\\' => {
                out.push(b'\\');
                i += 2;
            }
            b'x' => {
                let mut consumed = 0u8;
                let mut value = 0u8;
                for _ in 0..2 {
                    match input.get(i + 2 + consumed as usize).copied() {
                        Some(c) if c.is_ascii_hexdigit() => {
                            value = value.wrapping_shl(4) | hex_digit(c);
                            consumed += 1;
                        }
                        _ => break,
                    }
                }
                if consumed == 0 {
                    out.push(b'\\');
                    out.push(b'x');
                    i += 2;
                } else {
                    out.push(value);
                    i += 2 + consumed as usize;
                }
            }
            b'0'..=b'7' => {
                let mut consumed = 0u8;
                let mut value = 0u32;
                for _ in 0..3 {
                    match input.get(i + 1 + consumed as usize).copied() {
                        Some(c @ b'0'..=b'7') => {
                            value = value * 8 + (c - b'0') as u32;
                            consumed += 1;
                        }
                        _ => break,
                    }
                }
                out.push((value & 0xff) as u8);
                i += 1 + consumed as usize;
            }
            other => {
                // Unknown escape — PG passes the char through (the backslash is dropped).
                out.push(other);
                i += 2;
            }
        }
    }
    // Lossy is OK: postgres COPY text emits UTF-8 by default. Bad bytes are
    // far better surfaced as replacement chars than as a hard parse error here.
    DecodedField::Value(String::from_utf8_lossy(&out).into_owned())
}

fn hex_digit(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}
