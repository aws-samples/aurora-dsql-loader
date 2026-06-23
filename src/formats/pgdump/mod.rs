//! PostgreSQL pg_dump (plain `-Fp`) format support.
//!
//! Two related primitives live here:
//! - [`PgDumpReader`] — feeds the rows of one `COPY <schema>.<table>
//!   (cols...) FROM stdin;` ... `\.` block through the FileReader
//!   pipeline. Used by the `load --format pgdump` path.
//! - [`extract_ddl`] / [`list_copy_blocks`] — scan a full dump (DDL +
//!   data, no `--data-only`) and slice the non-data DDL between COPY
//!   blocks. Used by the migrate orchestrator.

mod escape;
mod reader;
mod scan;

#[cfg(test)]
mod tests;

pub use escape::encode_field;
pub use reader::PgDumpReader;
pub(crate) use scan::find_copy_block;
pub use scan::{CopyBlock, extract_ddl, list_copy_blocks};
