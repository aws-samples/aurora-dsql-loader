//! Decoder for PostgreSQL COPY text format escape sequences.
//!
//! Reference: https://www.postgresql.org/docs/current/sql-copy.html
//! "Text Format" section: a backslash introduces an escape sequence.

/// Decode one COPY-text-format field. Input is the raw bytes between tabs
/// (or between start-of-line and tab). Returns `None` for SQL NULL —
/// the whole-field literal `\N` is the only encoding pg_dump uses for
/// it, distinct from a genuine empty string which decodes to `Some("")`.
pub fn decode_field(input: &[u8]) -> Option<String> {
    if input == b"\\N" {
        return None;
    }
    Some(decode_non_null(input))
}

/// Encode one field into COPY text format — the inverse of [`decode_field`].
/// `None` is SQL NULL and renders as the `\N` sentinel; `Some(s)` escapes
/// exactly the characters `decode_field`'s named escapes cover, so any value
/// round-trips through `decode_field(encode_field(v)) == v`. Used by the
/// `export` path to emit `COPY ... FROM stdin` rows.
pub fn encode_field(value: Option<&str>) -> String {
    let Some(s) = value else {
        return "\\N".to_string();
    };
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0b}' => out.push_str("\\v"),
            _ => out.push(c),
        }
    }
    out
}

fn decode_non_null(input: &[u8]) -> String {
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
    String::from_utf8_lossy(&out).into_owned()
}
