//! PostgreSQL pg_dump (plain --data-only) input format.
//!
//! Parses the `COPY <schema>.<table> (cols...) FROM stdin;` ... `\.` block
//! emitted by `pg_dump` and feeds rows through the existing FileReader pipeline.

mod escape;
mod reader;

#[cfg(test)]
mod tests;

pub use reader::PgDumpReader;
