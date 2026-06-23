//! Assemble per-table pieces into one pg_dump-shaped `.sql` document. Pure
//! string assembly — the orchestrator supplies already-read structs and
//! row data. The output is what the `migrate` flow re-parses: each table's
//! DDL (CREATE TABLE + verbatim index defs) followed by its COPY block.

use super::data::{copy_header, copy_row_line};
use super::ddl::{TableDef, table_ddl};

/// One table's complete export payload: its definition, the verbatim index
/// statements from `pg_get_indexdef` (dsql-lint normalizes them downstream),
/// the COPY column order, and the row data (each field `None` = SQL NULL).
pub struct TableExport {
    pub table: TableDef,
    pub index_defs: Vec<String>,
    pub copy_columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    /// `setval(...)` statement to advance the identity column's sequence past
    /// the loaded rows, or `None` when the table has no identity / no rows.
    /// Emitted as the lint-safe `SELECT pg_catalog.setval(...)` form because
    /// DSQL rejects `ALTER … RESTART`.
    pub identity_setval: Option<String>,
}

/// Assemble all tables into one pg_dump-shaped `.sql` document. Each table
/// emits its `CREATE TABLE`, then any index statements, then its COPY block,
/// then an identity-sequence `setval` so a reload's next insert continues
/// past the loaded rows. Blank-line separated so the migrate scanner cleanly
/// delimits DDL from data blocks.
pub fn assemble_dump(exports: &[TableExport]) -> String {
    let mut out = String::new();
    for export in exports {
        out.push_str(&table_ddl(&export.table));
        out.push_str("\n\n");

        for index in &export.index_defs {
            out.push_str(index);
            out.push_str(";\n\n");
        }

        out.push_str(&copy_header(
            &export.table.schema,
            &export.table.name,
            &export.copy_columns,
        ));
        out.push('\n');
        for row in &export.rows {
            out.push_str(&copy_row_line(row));
            out.push('\n');
        }
        out.push_str("\\.\n\n");

        if let Some(setval) = &export.identity_setval {
            out.push_str(setval);
            out.push_str(";\n\n");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::ddl::{ColumnDef, TableDef};

    fn simple_table() -> TableDef {
        TableDef {
            schema: "public".into(),
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    type_name: "bigint".into(),
                    not_null: true,
                    identity_cache: None,
                    default_expr: None,
                },
                ColumnDef {
                    name: "name".into(),
                    type_name: "text".into(),
                    not_null: false,
                    identity_cache: None,
                    default_expr: None,
                },
            ],
            pk_columns: vec!["id".into()],
        }
    }

    /// A `TableExport` for `simple_table()` with no indexes / no identity —
    /// keeps the per-test literals focused on what each test exercises.
    fn export_of(rows: Vec<Vec<Option<String>>>) -> TableExport {
        TableExport {
            table: simple_table(),
            index_defs: vec![],
            copy_columns: vec!["id".into(), "name".into()],
            rows,
            identity_setval: None,
        }
    }

    #[test]
    fn one_table_ddl_then_indexes_then_copy_block() {
        let export = TableExport {
            index_defs: vec!["CREATE INDEX idx_t_name ON public.t USING btree_index (name)".into()],
            ..export_of(vec![
                vec![Some("1".into()), Some("alice".into())],
                vec![Some("2".into()), None],
            ])
        };

        let dump = assemble_dump(&[export]);

        let expected = "\
CREATE TABLE \"public\".\"t\" (
    \"id\" bigint NOT NULL,
    \"name\" text,
    PRIMARY KEY (\"id\")
);

CREATE INDEX idx_t_name ON public.t USING btree_index (name);

COPY \"public\".\"t\" (\"id\", \"name\") FROM stdin;
1\talice
2\t\\N
\\.

";
        assert_eq!(dump, expected);
    }

    #[test]
    fn empty_table_still_emits_copy_block_with_terminator() {
        // A table with no rows must still emit its COPY header + `\.` so the
        // migrate scanner sees a well-formed (empty) block.
        let dump = assemble_dump(&[export_of(vec![])]);
        assert!(dump.contains("COPY \"public\".\"t\" (\"id\", \"name\") FROM stdin;\n\\.\n"));
    }

    #[test]
    fn identity_setval_emitted_after_copy_block() {
        // The identity-continuation statement lands after the `\.` so the
        // sequence is advanced once the data is in place.
        let export = TableExport {
            identity_setval: Some("SELECT pg_catalog.setval('public.t_id_seq', 2, true)".into()),
            ..export_of(vec![
                vec![Some("1".into()), Some("alice".into())],
                vec![Some("2".into()), Some("bob".into())],
            ])
        };
        let dump = assemble_dump(&[export]);
        let term = dump.find("\\.\n").expect("terminator present");
        let setval = dump
            .find("SELECT pg_catalog.setval('public.t_id_seq', 2, true);")
            .expect("setval present");
        assert!(setval > term, "setval must follow the COPY terminator");
    }

    #[test]
    fn multiple_tables_are_separated() {
        let dump = assemble_dump(&[export_of(vec![]), export_of(vec![])]);
        // Two CREATE TABLE statements present.
        assert_eq!(dump.matches("CREATE TABLE").count(), 2);
    }
}
