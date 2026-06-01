//! Decoder for PostgreSQL COPY text format escape sequences.
//!
//! Reference: https://www.postgresql.org/docs/current/sql-copy.html
//! "Text Format" section: a backslash introduces an escape sequence.

/// Outcome of decoding one COPY-text-format field.
///
/// `Null` is preserved here as a distinct variant; the call site in
/// `reader.rs` decides how to flatten it into the wider loader pipeline
/// (which carries `Vec<String>`, not `Vec<Option<String>>`). Keeping the
/// distinction at the decode layer means a future refactor that wants real
/// NULL fidelity has one site to change.
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
        let Some(&next) = input.get(i + 1) else {
            out.push(b'\\');
            i += 1;
            continue;
        };
        match next {
            b'b' | b'f' | b'n' | b'r' | b't' | b'v' | b'\\' => {
                out.push(match next {
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'v' => 0x0b,
                    b'\\' => b'\\',
                    _ => unreachable!(),
                });
                i += 2;
            }
            b'x' => {
                let mut consumed = 0usize;
                let mut value: u8 = 0;
                for _ in 0..2 {
                    match input
                        .get(i + 2 + consumed)
                        .and_then(|&c| (c as char).to_digit(16))
                    {
                        Some(d) => {
                            value = (value << 4) | d as u8;
                            consumed += 1;
                        }
                        None => break,
                    }
                }
                if consumed == 0 {
                    out.push(b'\\');
                    out.push(b'x');
                    i += 2;
                } else {
                    out.push(value);
                    i += 2 + consumed;
                }
            }
            b'0'..=b'7' => {
                let mut consumed = 0usize;
                let mut value: u8 = 0;
                for _ in 0..3 {
                    match input.get(i + 1 + consumed) {
                        Some(&c @ b'0'..=b'7') => {
                            value = value.wrapping_mul(8).wrapping_add(c - b'0');
                            consumed += 1;
                        }
                        _ => break,
                    }
                }
                out.push(value);
                i += 1 + consumed;
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
    // Non-UTF-8 fields show up as U+FFFD; if that proves problematic in
    // practice, switch to `std::str::from_utf8` and surface as a parse error.
    DecodedField::Value(String::from_utf8_lossy(&out).into_owned())
}
