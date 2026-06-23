//! Pure COPY-block formatting for the export data path. The orchestrator
//! reads each row as text (`SELECT col::text, …`) and streams it through
//! here; the database is never touched in this module.

use super::quote_ident;
use crate::formats::pgdump::encode_field;

/// The `COPY "schema"."table" (cols…) FROM stdin;` header line the migrate
/// scanner re-parses.
pub fn copy_header(schema: &str, table: &str, columns: &[String]) -> String {
    let cols = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "COPY {}.{} ({cols}) FROM stdin;",
        quote_ident(schema),
        quote_ident(table)
    )
}

/// One tab-separated COPY-text data line, each field encoded via
/// [`encode_field`] so embedded tabs/newlines/backslashes and NULLs survive
/// the round-trip back through the migrate reader.
pub fn copy_row_line(fields: &[Option<String>]) -> String {
    fields
        .iter()
        .map(|f| encode_field(f.as_deref()))
        .collect::<Vec<_>>()
        .join("\t")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_quotes_identifiers_and_lists_columns() {
        assert_eq!(
            copy_header("public", "t", &["id".into(), "name".into()]),
            "COPY \"public\".\"t\" (\"id\", \"name\") FROM stdin;"
        );
    }

    #[test]
    fn row_line_tab_joins_encoded_fields() {
        let row = vec![Some("1".to_string()), Some("a\tb".to_string()), None];
        // Embedded tab is escaped (so it can't be read as a field separator);
        // None renders as the \N NULL sentinel.
        assert_eq!(copy_row_line(&row), "1\ta\\tb\t\\N");
    }
}
