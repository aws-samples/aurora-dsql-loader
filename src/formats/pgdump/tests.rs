use super::escape::{DecodedField, decode_field};

#[test]
fn decode_plain_text() {
    assert_eq!(decode_field(b"hello"), DecodedField::Value("hello".into()));
}

#[test]
fn decode_empty() {
    assert_eq!(decode_field(b""), DecodedField::Value(String::new()));
}

#[test]
fn decode_null_sentinel() {
    assert_eq!(decode_field(b"\\N"), DecodedField::Null);
}

#[test]
fn decode_escape_t_n_r_etc() {
    assert_eq!(decode_field(b"a\\tb"), DecodedField::Value("a\tb".into()));
    assert_eq!(decode_field(b"a\\nb"), DecodedField::Value("a\nb".into()));
    assert_eq!(decode_field(b"a\\rb"), DecodedField::Value("a\rb".into()));
    assert_eq!(decode_field(b"a\\\\b"), DecodedField::Value("a\\b".into()));
}

#[test]
fn decode_hex_escape() {
    // \x41 → 'A'
    assert_eq!(decode_field(b"\\x41"), DecodedField::Value("A".into()));
    // \x4 (single hex digit) → byte 0x04
    assert_eq!(decode_field(b"\\x4"), DecodedField::Value("\u{4}".into()));
    // \x with no digits → literal \x
    assert_eq!(decode_field(b"\\xZ"), DecodedField::Value("\\xZ".into()));
}

#[test]
fn decode_octal_escape() {
    // \101 → 'A'
    assert_eq!(decode_field(b"\\101"), DecodedField::Value("A".into()));
    // \1 → byte 0x01
    assert_eq!(decode_field(b"\\1"), DecodedField::Value("\u{1}".into()));
}

#[test]
fn decode_unknown_escape_drops_backslash() {
    // PG behavior: \q → q
    assert_eq!(decode_field(b"\\q"), DecodedField::Value("q".into()));
}

#[test]
fn decode_backslash_n_inside_value_is_not_null() {
    // \N is only NULL when it is the entire field.
    assert_eq!(
        decode_field(b"x\\N"),
        DecodedField::Value("xN".into()) // \N → N (unknown escape rule)
    );
}

#[test]
fn decode_trailing_backslash_passes_through() {
    assert_eq!(decode_field(b"abc\\"), DecodedField::Value("abc\\".into()));
}

#[test]
fn decode_multi_byte_utf8() {
    let bytes = "café".as_bytes();
    assert_eq!(decode_field(bytes), DecodedField::Value("café".into()));
}
