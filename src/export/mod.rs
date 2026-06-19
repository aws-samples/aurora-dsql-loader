//! Native `export`: read a source DSQL cluster's catalog and emit a plain
//! pg_dump-shaped `.sql` (DDL + `COPY ... FROM stdin` blocks) that the
//! `migrate` flow consumes unchanged.

mod ddl;
