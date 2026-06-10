use super::escape::decode_field;
use super::reader::PgDumpReader;
use super::scan::{ScanError, extract_ddl, find_copy_block, list_copy_blocks};
use crate::formats::FileReader;
use crate::io::byte_reader::MockByteReader;
use crate::io::{ByteReader, LocalFileByteReader};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn decode_plain_text() {
    assert_eq!(decode_field(b"hello"), Some("hello".into()));
}

#[test]
fn decode_empty_is_not_null() {
    // Genuine empty string is distinct from `\N`; only the latter is NULL.
    assert_eq!(decode_field(b""), Some(String::new()));
}

#[test]
fn decode_null_sentinel() {
    // `\N` is the COPY-format NULL sentinel; decoder returns None.
    assert_eq!(decode_field(b"\\N"), None);
}

#[test]
fn decode_escape_t_n_r_etc() {
    assert_eq!(decode_field(b"a\\tb"), Some("a\tb".into()));
    assert_eq!(decode_field(b"a\\nb"), Some("a\nb".into()));
    assert_eq!(decode_field(b"a\\rb"), Some("a\rb".into()));
    assert_eq!(decode_field(b"a\\\\b"), Some("a\\b".into()));
}

#[test]
fn decode_hex_escape() {
    // \x41 → 'A'
    assert_eq!(decode_field(b"\\x41"), Some("A".into()));
    // \x4 (single hex digit) → byte 0x04
    assert_eq!(decode_field(b"\\x4"), Some("\u{4}".into()));
    // \x with no digits → literal \x
    assert_eq!(decode_field(b"\\xZ"), Some("\\xZ".into()));
}

#[test]
fn decode_octal_escape() {
    // \101 → 'A'
    assert_eq!(decode_field(b"\\101"), Some("A".into()));
    // \1 → byte 0x01
    assert_eq!(decode_field(b"\\1"), Some("\u{1}".into()));
}

#[test]
fn decode_unknown_escape_drops_backslash() {
    // PG behavior: \q → q
    assert_eq!(decode_field(b"\\q"), Some("q".into()));
}

#[test]
fn decode_backslash_n_inside_value_is_not_null() {
    // \N is only NULL when it is the entire field. As a per-character escape
    // it falls into the unknown-escape arm, where the backslash is dropped
    // and the `N` passes through, so b"x\\N" decodes to "xN".
    assert_eq!(decode_field(b"x\\N"), Some("xN".into()));
}

#[test]
fn decode_trailing_backslash_passes_through() {
    assert_eq!(decode_field(b"abc\\"), Some("abc\\".into()));
}

#[test]
fn decode_multi_byte_utf8() {
    let bytes = "café".as_bytes();
    assert_eq!(decode_field(bytes), Some("café".into()));
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
    let reader = MockByteReader::new(SAMPLE.to_vec());
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
    let reader = MockByteReader::new(SAMPLE.to_vec());
    let block = find_copy_block(&reader, "public", "orders").await.unwrap();
    assert_eq!(block.columns, vec!["id", "user_id"]);
}

#[tokio::test]
async fn missing_table_errors() {
    let reader = MockByteReader::new(SAMPLE.to_vec());
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
    let reader = MockByteReader::new(SAMPLE.to_vec());
    let err = find_copy_block(&reader, "sales", "users")
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::NotFound { .. })
    ));
}

#[tokio::test]
async fn case_insensitive_copy_keyword() {
    let data = b"copy public.t (a) from stdin;\n1\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
    let block = find_copy_block(&reader, "public", "t").await.unwrap();
    assert_eq!(block.columns, vec!["a"]);
}

#[tokio::test]
async fn copy_without_column_list_is_rejected() {
    let data = b"COPY public.t FROM stdin;\n1\ta\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::MissingColumnList { .. })
    ));
}

#[tokio::test]
async fn quoted_identifiers_are_unquoted() {
    let data = b"COPY \"public\".\"My Table\" (\"col one\", \"col-two\") FROM stdin;\n1\t2\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
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
    let reader = MockByteReader::new(data.to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::Duplicate { .. })
    ));
}

#[tokio::test]
async fn missing_terminator_errors() {
    let data = b"COPY public.t (a) FROM stdin;\n1\n";
    let reader = MockByteReader::new(data.to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::MissingTerminator { .. })
    ));
}

#[tokio::test]
async fn list_copy_blocks_returns_all_blocks_in_order() {
    let reader = MockByteReader::new(SAMPLE.to_vec());
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
    let reader = MockByteReader::new(b"-- preamble\nSET timezone='UTC';\n".to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert!(blocks.is_empty());
}

#[tokio::test]
async fn pgdump_reader_metadata_returns_block_size() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(
        f,
        "-- preamble that should not count toward file_size_bytes"
    )
    .unwrap();
    writeln!(f, "COPY public.t (a, b) FROM stdin;").unwrap();
    let data_bytes = b"1\tx\n2\ty\n";
    f.write_all(data_bytes).unwrap();
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    let meta = reader.metadata().await.unwrap();
    assert_eq!(meta.file_size_bytes, data_bytes.len() as u64);

    // Verify the block bounds map to exactly the data lines, with no
    // preamble bleed-in at the start nor `\.` bleed-in at the end.
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let peek = LocalFileByteReader::new(f.path());
    let raw = peek
        .read_range(chunks[0].start_offset, chunks[0].end_offset)
        .await
        .unwrap();
    assert_eq!(raw, data_bytes);
}

#[tokio::test]
async fn pgdump_reader_chunks_cover_block_only() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (a) FROM stdin;").unwrap();
    for i in 0..50 {
        writeln!(f, "{i}").unwrap();
    }
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    assert_eq!(chunks.len(), 1);
    let c = &chunks[0];
    let peek = LocalFileByteReader::new(f.path());
    let buf = peek.read_range(c.start_offset, c.end_offset).await.unwrap();
    assert!(!buf.windows(2).any(|w| w == b"\\."));
}

#[tokio::test]
async fn pgdump_reader_columns_are_exposed() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (id, name) FROM stdin;\n1\tx\n\\.").unwrap();
    f.flush().unwrap();
    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    assert_eq!(reader.columns(), &["id", "name"]);
}

#[tokio::test]
async fn read_chunk_distinguishes_null_from_empty_string() {
    // Pins the fix for the empty-string-collapses-to-NULL bug: a row
    // with `\N` decodes to `None` and a row with a genuine empty string
    // between two tabs decodes to `Some("")`, so a column with
    // `NOT NULL DEFAULT ''` round-trips through migrate.
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (a, b) FROM stdin;").unwrap();
    // empty string in column b
    writeln!(f, "1\t").unwrap();
    // \N (real NULL) in column b
    writeln!(f, "2\t\\N").unwrap();
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    let data = reader.read_chunk(&chunks[0]).await.unwrap();

    assert_eq!(data.records.len(), 2);
    assert_eq!(
        data.records[0].fields,
        vec![Some("1".into()), Some("".into())]
    );
    assert_eq!(data.records[1].fields, vec![Some("2".into()), None]);
}

#[tokio::test]
async fn read_chunk_decodes_rows_and_escapes() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (a, b, c) FROM stdin;").unwrap();
    writeln!(f, "1\thello\tworld").unwrap();
    writeln!(f, "2\t\\N\tline\\nbreak").unwrap();
    writeln!(f, "3\ta\\tb\t\\\\esc").unwrap();
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    let data = reader.read_chunk(&chunks[0]).await.unwrap();

    assert_eq!(data.records.len(), 3);
    assert_eq!(data.parse_errors, 0);

    assert_eq!(
        data.records[0].fields,
        vec![Some("1".into()), Some("hello".into()), Some("world".into())]
    );
    // Row 2: column b was `\N` → None; column c is "line\nbreak".
    assert_eq!(
        data.records[1].fields,
        vec![Some("2".into()), None, Some("line\nbreak".into())]
    );
    assert_eq!(
        data.records[2].fields,
        vec![Some("3".into()), Some("a\tb".into()), Some("\\esc".into())]
    );
}

#[tokio::test]
async fn read_chunk_rejects_field_count_mismatch() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (a, b) FROM stdin;").unwrap();
    writeln!(f, "1\tx").unwrap();
    writeln!(f, "2\ty\tEXTRA").unwrap();
    writeln!(f, "3\tz").unwrap();
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    let meta = reader.metadata().await.unwrap();
    let chunks = reader.create_chunks(meta.file_size_bytes).await.unwrap();
    let data = reader.read_chunk(&chunks[0]).await.unwrap();

    assert_eq!(data.records.len(), 2);
    assert_eq!(data.parse_errors, 1);
}

#[tokio::test]
async fn read_chunk_counts_blank_lines_as_parse_errors() {
    // pg_dump never emits blank lines inside a COPY block; if we see one it's
    // structural corruption. Surface as parse_error rather than silently skip.
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (a, b) FROM stdin;").unwrap();
    writeln!(f, "1\tx").unwrap();
    writeln!(f).unwrap();
    writeln!(f, "2\ty").unwrap();
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
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
    writeln!(f, "COPY public.t (a, b) FROM stdin;").unwrap();
    for i in 0..ROWS {
        writeln!(f, "{i}\tname_{i:04}").unwrap();
    }
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();
    let meta = reader.metadata().await.unwrap();

    let target = std::cmp::max(meta.file_size_bytes / 4, 64);
    let chunks = reader.create_chunks(target).await.unwrap();
    assert!(
        chunks.len() >= 3,
        "expected ≥3 chunks, got {}",
        chunks.len()
    );

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

#[tokio::test]
async fn quoted_identifier_preserves_utf8_multibyte() {
    // PG quoted identifiers may contain non-ASCII (UTF-8) characters; pg_dump
    // emits the bytes verbatim. The scanner must round-trip them, not Latin-1
    // mangle them through `byte as char`.
    let data = "COPY \"public\".\"naïve\" (\"café\") FROM stdin;\n1\n\\.\n".as_bytes();
    let reader = MockByteReader::new(data.to_vec());
    let block = find_copy_block(&reader, "public", "naïve").await.unwrap();
    assert_eq!(block.table, "naïve");
    assert_eq!(block.columns, vec!["café"]);
}

#[tokio::test]
async fn quoted_column_list_handles_paren_and_comma_inside_quotes() {
    // A column named `c)d` must not truncate the column list; a column named
    // `a,b` must not be split. Both are legal PG quoted identifiers.
    let data = b"COPY public.weird (\"a,b\", normal, \"c)d\") FROM stdin;\n1\t2\t3\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
    let block = find_copy_block(&reader, "public", "weird").await.unwrap();
    assert_eq!(block.columns, vec!["a,b", "normal", "c)d"]);
}

#[tokio::test]
async fn malformed_column_list_unterminated_quote_errors() {
    // `COPY t ("a, b)` — unterminated quoted identifier inside the list.
    // Must surface as MalformedColumnList, not silently downgrade to
    // NoMatch (which would later look like NotFound or MissingTerminator).
    let data = b"COPY public.t (\"a, b) FROM stdin;\n1\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
    let err = list_copy_blocks(&reader).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::MalformedColumnList { .. })
    ));
}

#[tokio::test]
async fn malformed_column_list_empty_parens_errors() {
    let data = b"COPY public.t () FROM stdin;\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
    let err = list_copy_blocks(&reader).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::MalformedColumnList { .. })
    ));
}

#[tokio::test]
async fn quoted_identifier_with_escaped_quote_round_trips() {
    // A literal `"` inside a quoted identifier is escaped as `""`.
    let data = b"COPY public.\"weird\"\"name\" (a) FROM stdin;\n1\n\\.\n";
    let reader = MockByteReader::new(data.to_vec());
    let block = find_copy_block(&reader, "public", "weird\"name")
        .await
        .unwrap();
    assert_eq!(block.columns, vec!["a"]);
}

#[tokio::test]
async fn read_line_rejects_pathological_unbounded_header() {
    // A "line" in the SQL preamble with no newline byte must not buffer
    // unboundedly. The header path uses MAX_HEADER_LINE_BYTES (1 MiB), so
    // a 2 MiB no-newline input exercises the cap without taking forever.
    struct GiantNoNewline(u64);
    #[async_trait::async_trait]
    impl ByteReader for GiantNoNewline {
        async fn size(&self) -> anyhow::Result<u64> {
            Ok(self.0)
        }
        async fn read_range(&self, start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
            let len = (end - start) as usize;
            Ok(vec![b'x'; len])
        }
    }
    let reader = GiantNoNewline(2 * 1024 * 1024);
    let err = list_copy_blocks(&reader).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("exceeds") && msg.contains("bytes"),
        "expected size-cap error, got: {msg}"
    );
}

#[tokio::test]
async fn find_terminator_errors_on_truncated_body_without_newlines() {
    // A COPY block whose data section never contains `\n` between data_start
    // and EOF simulates a truncated dump body. find_terminator must surface
    // a MissingTerminator error rather than spin or grow memory unbounded.
    // 2 MiB body keeps the test fast; the production 1 GiB cap is the same
    // code path with a larger fixture.
    struct HeaderThenJunk;
    #[async_trait::async_trait]
    impl ByteReader for HeaderThenJunk {
        async fn size(&self) -> anyhow::Result<u64> {
            Ok(30 + 2 * 1024 * 1024)
        }
        async fn read_range(&self, start: u64, end: u64) -> anyhow::Result<Vec<u8>> {
            let header = b"COPY public.t (a) FROM stdin;\n";
            let total = 30 + 2 * 1024 * 1024;
            let mut out = Vec::with_capacity((end - start) as usize);
            for off in start..end.min(total) {
                if off < 30 {
                    out.push(header[off as usize]);
                } else {
                    out.push(b'x');
                }
            }
            Ok(out)
        }
    }
    let reader = HeaderThenJunk;
    let blocks = list_copy_blocks(&reader).await;
    assert!(
        blocks.is_err(),
        "expected MissingTerminator error on truncated body"
    );
    let msg = format!("{:#}", blocks.unwrap_err());
    assert!(
        msg.contains("missing") || msg.contains("terminator") || msg.contains("exceeds"),
        "expected truncation diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn read_chunk_handles_chunk_boundary_at_record_edge() {
    // Force a chunk boundary to land exactly on a record-ending newline. The
    // boundary search returns "offset just past \n", so the next chunk starts
    // at the next record — verify no record is dropped or duplicated.
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "COPY public.t (a) FROM stdin;").unwrap();
    for i in 0..50 {
        writeln!(f, "{i}").unwrap();
    }
    writeln!(f, "\\.").unwrap();
    f.flush().unwrap();

    let byte_reader = LocalFileByteReader::new(f.path());
    let reader = PgDumpReader::new(byte_reader, "public", "t").await.unwrap();

    // Tiny target_size forces many chunks, exercising boundary alignment.
    let chunks = reader.create_chunks(8).await.unwrap();
    let mut total_records = 0usize;
    for chunk in &chunks {
        let data = reader.read_chunk(chunk).await.unwrap();
        assert_eq!(
            data.parse_errors, 0,
            "chunk {} parse_errors should be 0",
            chunk.chunk_id
        );
        total_records += data.records.len();
    }
    assert_eq!(total_records, 50, "all records covered exactly once");
}

#[tokio::test]
async fn find_terminator_handles_eof_without_trailing_newline() {
    // pg_dump always emits `\.\n`, but a hand-truncated or interrupted file
    // may end exactly at `\.` with no final `\n`. The bytewise scanner must
    // still recognize the terminator at EOF. Pinning this so a future
    // refactor of find_terminator doesn't accidentally regress it.
    let data = b"COPY public.t (a) FROM stdin;\n1\n2\n\\.";
    let reader = MockByteReader::new(data.to_vec());
    let block = find_copy_block(&reader, "public", "t").await.unwrap();
    assert_eq!(block.columns, vec!["a"]);
    // data_end points to the start of the `\.` line (offset of the `\\` byte).
    let expected_data_end = data.len() as u64 - 2; // \. is the last 2 bytes
    assert_eq!(block.data_end, expected_data_end);
}

#[tokio::test]
async fn find_terminator_rejects_backslash_dot_cr_at_eof() {
    // `\.\r` at EOF (no trailing `\n`) is NOT a valid terminator: the file
    // ends mid-CRLF, which indicates corruption rather than a clean truncate.
    // Pinning the deliberate strictness so a future "preserve more inputs"
    // refactor doesn't silently accept a likely-malformed dump.
    let data = b"COPY public.t (a) FROM stdin;\n1\n\\.\r";
    let reader = MockByteReader::new(data.to_vec());
    let result = find_copy_block(&reader, "public", "t").await;
    assert!(
        result.is_err(),
        "\\.\\r at EOF must be rejected as MissingTerminator"
    );
}

#[tokio::test]
async fn rejects_pgc_custom_format_archive() {
    // pg_dump -Fc archives start with the magic bytes "PGDMP". A user pointing
    // the loader at a custom-format dump must get a clear "use -Fp" diagnostic
    // instead of "no COPY block found", which would only surface after
    // streaming the entire (binary) file.
    let mut data = b"PGDMP".to_vec();
    data.extend_from_slice(&[0x01, 0x0e, 0x00, 0x00]); // bogus version trailer
    let reader = MockByteReader::new(data);
    let err = list_copy_blocks(&reader).await.unwrap_err();
    let msg = format!("{:#}", err);
    assert!(
        matches!(
            err.downcast_ref::<ScanError>(),
            Some(ScanError::NonPlainPgDump { .. })
        ),
        "expected NonPlainPgDump, got: {msg}"
    );
    assert!(msg.contains("-Fp"), "diagnostic must point to -Fp: {msg}");
}

#[tokio::test]
async fn rejects_pgc_archive_via_find_copy_block() {
    // Same magic-byte rejection from the single-table entry point.
    let data = b"PGDMP\x01\x0e\x00\x00".to_vec();
    let reader = MockByteReader::new(data);
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::NonPlainPgDump { .. })
    ));
}

#[tokio::test]
async fn plain_dump_starting_with_dashes_is_not_misdetected() {
    // A plain dump's first bytes are usually `--` (SQL comment) or `SET`. The
    // magic-byte check must not false-positive on those.
    let data = b"-- PostgreSQL database dump\nCOPY public.t (a) FROM stdin;\n1\n\\.\n".to_vec();
    let reader = MockByteReader::new(data);
    let block = find_copy_block(&reader, "public", "t").await.unwrap();
    assert_eq!(block.columns, vec!["a"]);
}

#[tokio::test]
async fn empty_or_tiny_files_do_not_trigger_format_detection() {
    // Files smaller than the magic prefix must not panic or false-positive.
    let reader = MockByteReader::new(b"".to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::NotFound { .. })
    ));

    let reader = MockByteReader::new(b"PG".to_vec());
    let err = find_copy_block(&reader, "public", "t").await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<ScanError>(),
        Some(ScanError::NotFound { .. })
    ));
}

#[tokio::test]
async fn copy_block_records_header_start_and_block_end() {
    // header_start points at the leading byte of the `COPY ...` line.
    // block_end points one past the closing `\.` terminator line, so
    // [block_end, next_block.header_start) is exactly the bytes between
    // two data blocks (where pg_dump would interleave more DDL).
    let reader = MockByteReader::new(SAMPLE.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert_eq!(blocks.len(), 2);

    // First block: header starts where `COPY public.users` does.
    let users = &blocks[0];
    assert_eq!(
        &SAMPLE[users.header_start as usize..users.header_start as usize + 4],
        b"COPY"
    );
    // block_end is past the `\.\n`, so it must be > data_end and the byte
    // before block_end is the trailing newline of the terminator line.
    assert!(users.block_end > users.data_end);
    assert_eq!(SAMPLE[(users.block_end - 1) as usize], b'\n');

    // Sandwich invariant: between users.block_end and orders.header_start
    // there is only DDL/whitespace (no `COPY` keyword).
    let between = &SAMPLE[users.block_end as usize..blocks[1].header_start as usize];
    let between_str = std::str::from_utf8(between).unwrap();
    assert!(
        !between_str.to_ascii_uppercase().contains("COPY"),
        "between-blocks slice must not contain another COPY: {between_str:?}"
    );
}

#[tokio::test]
async fn extract_ddl_pulls_preamble_and_drops_data_blocks() {
    // pg_dump output has DDL before the COPY blocks. extract_ddl returns
    // exactly that DDL (and any trailing post-COPY DDL), with the data
    // payload sliced out so the result can be fed straight into fix_sql.
    let reader = MockByteReader::new(SAMPLE.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    let ddl = extract_ddl(&reader, &blocks).await.unwrap();

    // Comment preamble survives (SET preambles get stripped — covered by
    // extract_ddl_strips_pg_dump_session_set_preamble).
    assert!(ddl.contains("PostgreSQL database dump"));
    // ...but the data rows do NOT (no `1\tAlice` / `1\t1`).
    assert!(!ddl.contains("Alice"));
    assert!(!ddl.contains("Bob"));
    // ...and neither do the COPY headers themselves.
    assert!(
        !ddl.to_ascii_uppercase().contains("COPY "),
        "DDL must not contain COPY headers, got: {ddl:?}"
    );
    // ...and the `\.` terminators are gone.
    assert!(!ddl.contains("\\."));
}

#[tokio::test]
async fn extract_ddl_with_no_copy_blocks_returns_full_input() {
    // Schema-only dump (no data) -> all bytes are DDL.
    let raw = b"-- preamble\nCREATE TABLE t (id integer);\n";
    let reader = MockByteReader::new(raw.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert!(blocks.is_empty());
    let ddl = extract_ddl(&reader, &blocks).await.unwrap();
    assert_eq!(ddl, std::str::from_utf8(raw).unwrap());
}

#[tokio::test]
async fn extract_ddl_preserves_inter_block_ddl() {
    // pg_dump emits ALTER TABLE / SET DEFAULT statements BETWEEN data blocks
    // when SERIAL columns are involved. extract_ddl must preserve those —
    // they are exactly the statements fix_sql collapses.
    let raw = b"\
-- pre\n\
CREATE TABLE public.t (id integer);\n\
COPY public.t (id) FROM stdin;\n\
1\n\
\\.\n\
ALTER TABLE ONLY public.t ALTER COLUMN id SET DEFAULT nextval('public.t_seq'::regclass);\n\
COPY public.u (id) FROM stdin;\n\
2\n\
\\.\n\
-- post\n";
    let reader = MockByteReader::new(raw.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert_eq!(blocks.len(), 2);
    let ddl = extract_ddl(&reader, &blocks).await.unwrap();

    assert!(ddl.contains("CREATE TABLE public.t"));
    assert!(
        ddl.contains("ALTER TABLE ONLY public.t"),
        "inter-block DDL must be preserved, got: {ddl:?}"
    );
    assert!(ddl.contains("-- post"));
    // Data rows and COPY lines are gone.
    assert!(!ddl.to_ascii_uppercase().contains("COPY "));
    assert!(!ddl.contains("\\."));
}

#[tokio::test]
async fn extract_ddl_strips_psql_restrict_meta_commands() {
    // PostgreSQL 16+ pg_dump emits `\restrict <token>` at the top and
    // `\unrestrict <token>` at the bottom of every dump. They are psql
    // meta-commands, not SQL — any downstream SQL tool (a parser, our
    // dsql-lint) chokes on the leading backslash. extract_ddl strips
    // them so the migrate flow's transform stage sees clean SQL.
    let raw = b"\
\\restrict aB1xYz\n\
\n\
SET statement_timeout = 0;\n\
CREATE TABLE public.t (id integer NOT NULL);\n\
COPY public.t (id) FROM stdin;\n\
1\n\
\\.\n\
\n\
\\unrestrict aB1xYz\n\
\n\
-- complete\n";
    let reader = MockByteReader::new(raw.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    assert_eq!(blocks.len(), 1);
    let ddl = extract_ddl(&reader, &blocks).await.unwrap();

    assert!(
        !ddl.contains("\\restrict"),
        "\\restrict meta-command should be stripped, got: {ddl:?}"
    );
    assert!(
        !ddl.contains("\\unrestrict"),
        "\\unrestrict meta-command should be stripped, got: {ddl:?}"
    );
    // Real DDL survives.
    assert!(ddl.contains("CREATE TABLE"));
    assert!(ddl.contains("-- complete"));
}

#[tokio::test]
async fn extract_ddl_strips_pg_dump_session_set_preamble() {
    // pg_dump emits a block of `SET <param> = ...;` lines at the top of
    // every plain-format dump (statement_timeout, lock_timeout, etc.).
    // DSQL rejects these as "setting configuration parameter ... not
    // supported", so extract_ddl drops them. Customer-authored SETs
    // outside the allowlist (e.g. `SET search_path`) are preserved.
    let raw = b"\
SET statement_timeout = 0;\n\
SET lock_timeout = 0;\n\
SET client_encoding = 'UTF8';\n\
SET standard_conforming_strings = on;\n\
SET row_security = off;\n\
SET search_path = public;\n\
SET default_tablespace = '';\n\
SET default_table_access_method = heap;\n\
\n\
CREATE TABLE public.t (id integer NOT NULL);\n";
    let reader = MockByteReader::new(raw.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    let ddl = extract_ddl(&reader, &blocks).await.unwrap();

    for stripped in [
        "SET statement_timeout",
        "SET lock_timeout",
        "SET client_encoding",
        "SET standard_conforming_strings",
        "SET row_security",
        "SET default_tablespace",
        "SET default_table_access_method",
    ] {
        assert!(
            !ddl.contains(stripped),
            "{stripped} should be stripped, got: {ddl:?}"
        );
    }
    assert!(
        ddl.contains("SET search_path"),
        "non-allowlisted SET must survive (apply layer surfaces real errors), got: {ddl:?}"
    );
    assert!(ddl.contains("CREATE TABLE"));
}

#[tokio::test]
async fn extract_ddl_does_not_strip_lines_inside_unrelated_dollar_quotes() {
    // A function body that happens to contain a `\restrict`-looking
    // line as a string MUST NOT be touched. The filter is anchored on
    // the line start and the exact known commands, not on a generic
    // backslash regex; this test pins that.
    let raw = b"\
SET search_path = public;\n\
CREATE FUNCTION f() RETURNS text AS $$\n\
SELECT '\\restrict not a meta command'::text\n\
$$ LANGUAGE sql;\n\
COPY public.t (a) FROM stdin;\n\
1\n\
\\.\n";
    let reader = MockByteReader::new(raw.to_vec());
    let blocks = list_copy_blocks(&reader).await.unwrap();
    let ddl = extract_ddl(&reader, &blocks).await.unwrap();

    // The string literal stays put because the filter only matches
    // a meta-command at the VERY START of a line, with the known
    // command word, surrounded by whitespace.
    assert!(
        ddl.contains("'\\restrict not a meta command'"),
        "string-literal `\\restrict` must NOT be stripped, got: {ddl:?}"
    );
}

#[tokio::test]
async fn read_chunk_handles_crlf_line_endings() {
    // pg_dump on Windows-flavored paths can produce CRLF; split_lines strips
    // the trailing \r per line. Assert decoded fields don't carry stray \r.
    let data = b"COPY public.t (a, b) FROM stdin;\r\n1\twidget\r\n2\tgizmo\r\n\\.\r\n";
    let reader = MockByteReader::new(data.to_vec());
    let pg_reader = PgDumpReader::new(reader, "public", "t").await.unwrap();
    let meta = pg_reader.metadata().await.unwrap();
    let chunks = pg_reader.create_chunks(meta.file_size_bytes).await.unwrap();
    let chunk_data = pg_reader.read_chunk(&chunks[0]).await.unwrap();
    assert_eq!(chunk_data.records.len(), 2);
    assert_eq!(chunk_data.parse_errors, 0);
    assert_eq!(
        chunk_data.records[0].fields,
        vec![Some("1".into()), Some("widget".into())]
    );
    assert_eq!(
        chunk_data.records[1].fields,
        vec![Some("2".into()), Some("gizmo".into())]
    );
}
