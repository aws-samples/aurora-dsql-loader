use super::escape::{DecodedField, decode_field};
use super::reader::PgDumpReader;
use super::scan::{ScanError, find_copy_block, list_copy_blocks};
use crate::formats::FileReader;
use crate::io::{ByteReader, LocalFileByteReader};
use anyhow::Result;
use async_trait::async_trait;
use std::io::Write;
use tempfile::NamedTempFile;

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

struct MockReader(Vec<u8>);

#[async_trait]
impl ByteReader for MockReader {
    async fn size(&self) -> Result<u64> {
        Ok(self.0.len() as u64)
    }
    async fn read_range(&self, start: u64, end: u64) -> Result<Vec<u8>> {
        let s = start as usize;
        let e = std::cmp::min(end as usize, self.0.len());
        Ok(self.0[s..e].to_vec())
    }
}

const SAMPLE: &[u8] = b"\
--\n\
-- PostgreSQL database dump\n\
--\n\
\n\
SET statement_timeout = 0;\n\
\n\
COPY public.users (id, name, email) FROM stdin;\n\
1\tAlice\talice@example.com\n\
2\tBob\tbob@example.com\n\
\\.\n\
\n\
COPY public.orders (id, user_id) FROM stdin;\n\
1\t1\n\
\\.\n\
\n";

#[tokio::test]
async fn finds_users_block() {
    let reader = MockReader(SAMPLE.to_vec());
    let block = find_copy_block(&reader, "public", "users").await.unwrap();
    assert_eq!(block.columns, vec!["id", "name", "email"]);
    let data = &SAMPLE[block.data_start as usize..block.data_end as usize];
    let s = std::str::from_utf8(data).unwrap();
    assert!(s.starts_with("1\tAlice"));
    assert!(s.ends_with("@example.com\n"));
    assert_eq!(
        &SAMPLE[block.data_end as usize..block.data_end as usize + 2],
        b"\\."
    );
}

#[tokio::test]
async fn finds_orders_block() {
    let reader = MockReader(SAMPLE.to_vec());
    let block = find_copy_block(&reader, "public", "orders").await.unwrap();
    assert_eq!(block.columns, vec!["id", "user_id"]);
}

#[tokio::test]
async fn missing_table_errors() {
    let reader = MockReader(SAMPLE.to_vec());
    let err = find_copy_block(&reader, "public", "nonexistent")
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::NotFound { .. })
    ));
}

#[tokio::test]
async fn schema_must_match() {
    let reader = MockReader(SAMPLE.to_vec());
    let err = find_copy_block(&reader, "sales", "users").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::NotFound { .. })
    ));
}

#[tokio::test]
async fn case_insensitive_copy_keyword() {
    let data = b"copy public.t (a) from stdin;\n1\n\\.\n";
    let reader = MockReader(data.to_vec());
    let block = find_copy_block(&reader, "public", "t").await.unwrap();
    assert_eq!(block.columns, vec!["a"]);
}

#[tokio::test]
async fn no_column_list_is_supported() {
    let data = b"COPY public.t FROM stdin;\n1\ta\n\\.\n";
    let reader = MockReader(data.to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::MissingColumnList { .. })
    ));
}

#[tokio::test]
async fn quoted_identifiers_are_unquoted() {
    let data = b"COPY \"public\".\"My Table\" (\"col one\", \"col-two\") FROM stdin;\n1\t2\n\\.\n";
    let reader = MockReader(data.to_vec());
    let block = find_copy_block(&reader, "public", "My Table")
        .await
        .unwrap();
    assert_eq!(block.columns, vec!["col one", "col-two"]);
}

#[tokio::test]
async fn duplicate_block_errors() {
    let data = b"\
COPY public.t (a) FROM stdin;\n\
1\n\
\\.\n\
COPY public.t (a) FROM stdin;\n\
2\n\
\\.\n";
    let reader = MockReader(data.to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::Duplicate { .. })
    ));
}

#[tokio::test]
async fn missing_terminator_errors() {
    let data = b"COPY public.t (a) FROM stdin;\n1\n";
    let reader = MockReader(data.to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::MissingTerminator { .. })
    ));
}

#[tokio::test]
async fn list_copy_blocks_returns_all_blocks_in_order() {
    let reader = MockReader(SAMPLE.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].schema, "public");
    assert_eq!(blocks[0].table, "users");
    assert_eq!(blocks[0].columns, vec!["id", "name", "email"]);
    assert_eq!(blocks[1].schema, "public");
    assert_eq!(blocks[1].table, "orders");
    assert_eq!(blocks[1].columns, vec!["id", "user_id"]);
    assert!(blocks[0].data_start < blocks[1].data_start);
}

#[tokio::test]
async fn list_copy_blocks_empty_when_no_copy_lines() {
    let reader = MockReader(b"-- preamble\nSET timezone='UTC';\n".to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert!(blocks.is_empty());
}

#[tokio::test]
async fn pgdump_reader_metadata_returns_block_size() {
    let mut f = NamedTempFile::new().unwrap();
    write!(
        f,
        "{}",
        "-- preamble that should not count toward file_size_bytes\n"
    )
    .unwrap();
    write!(f, "COPY public.t (a, b) FROM stdin;\n").unwrap();
    let data_start_marker = "1\tx\n2\ty\n";
    write!(f, "{}", data_start_marker).unwrap();
    write!(f, "\\.\n").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t")
        .await
        .unwrap();
    let meta = reader.metadata().await.unwrap();
    assert_eq!(meta.file_size_bytes, data_start_marker.len() as u64);
}

#[tokio::test]
async fn pgdump_reader_chunks_cover_block_only() {
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "COPY public.t (a) FROM stdin;\n").unwrap();
    for i in 0..50 {
        writeln!(f, "{}", i).unwrap();
    }
    write!(f, "\\.\n").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t")
        .await
        .unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let c = &chunks[0];
    let peek = LocalFileByteReader::new(f.path());
    let buf = peek
        .read_range(c.start_offset, c.end_offset)
        .await
        .unwrap();
    assert!(!buf.windows(2).any(|w| w == b"\\."));
}

#[tokio::test]
async fn pgdump_reader_columns_are_exposed() {
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "COPY public.t (id, name) FROM stdin;\n1\tx\n\\.\n").unwrap();
    f.flush().unwrap();
    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t")
        .await
        .unwrap();
    assert_eq!(reader.columns(), &["id", "name"]);
}

#[tokio::test]
async fn read_chunk_decodes_rows_and_escapes() {
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "COPY public.t (a, b, c) FROM stdin;\n").unwrap();
    write!(f, "1\thello\tworld\n").unwrap();
    write!(f, "2\t\\N\tline\\nbreak\n").unwrap();
    write!(f, "3\ta\\tb\t\\\\esc\n").unwrap();
    write!(f, "\\.\n").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t")
        .await
        .unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    let data = reader.read_chunk(&chunks[0]).await.unwrap();

    assert_eq!(data.records.len(), 3);
    assert_eq!(data.parse_errors, 0);

    assert_eq!(data.records[0].fields, vec!["1", "hello", "world"]);
    assert_eq!(data.records[1].fields, vec!["2", "", "line\nbreak"]);
    assert_eq!(data.records[2].fields, vec!["3", "a\tb", "\\esc"]);
}

#[tokio::test]
async fn read_chunk_rejects_field_count_mismatch() {
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "COPY public.t (a, b) FROM stdin;\n").unwrap();
    write!(f, "1\tx\n").unwrap();
    write!(f, "2\ty\tEXTRA\n").unwrap();
    write!(f, "3\tz\n").unwrap();
    write!(f, "\\.\n").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t")
        .await
        .unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    let data = reader.read_chunk(&chunks[0]).await.unwrap();

    assert_eq!(data.records.len(), 2);
    assert_eq!(data.parse_errors, 1);
}

#[tokio::test]
async fn read_chunk_handles_multi_chunk_split() {
    const ROWS: usize = 200;
    let mut f = NamedTempFile::new().unwrap();
    write!(f, "COPY public.t (a, b) FROM stdin;\n").unwrap();
    for i in 0..ROWS {
        writeln!(f, "{}\tname_{:04}", i, i).unwrap();
    }
    write!(f, "\\.\n").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t")
        .await
        .unwrap();
    let meta = reader.metadata().await.unwrap();

    let target = std::cmp::max(meta.file_size_bytes / 4, 64);
    let chunks = reader.create_chunks(target).await.unwrap();
    assert!(chunks.len() >= 3, "expected ≥3 chunks, got {}", chunks.len());

    for w in chunks.windows(2) {
        assert_eq!(w[0].end_offset, w[1].start_offset, "non-contiguous chunks");
    }

    let peek = LocalFileByteReader::new(f.path());

    let mut total_records = 0usize;
    for chunk in &chunks {
        let data = reader.read_chunk(chunk).await.unwrap();
        let raw = peek
            .read_range(chunk.start_offset, chunk.end_offset)
            .await
            .unwrap();

        assert!(
            !raw.windows(2).any(|w| w == b"\\."),
            "chunk {} leaked the \\. terminator",
            chunk.chunk_id
        );

        let nl_count = raw.iter().filter(|&&b| b == b'\n').count();
        assert_eq!(
            nl_count,
            data.records.len(),
            "chunk {} record/line count mismatch: {} newlines vs {} records",
            chunk.chunk_id,
            nl_count,
            data.records.len(),
        );
        assert_eq!(
            data.parse_errors, 0,
            "chunk {} had parse errors",
            chunk.chunk_id
        );

        total_records += data.records.len();
    }

    assert_eq!(total_records, ROWS);
}
