//! Runs the `pg_dump → DSQL` migrate flow: extract embedded DDL, transform
//! it for DSQL via `dsql-lint`, apply it to the cluster, then load the data.
//!
//! Composed of small, testable pieces (extract / transform / apply / load),
//! glued together by [`run_migrate`] — see the per-task spec in
//! `docs/plans/2026-06-02-pgdump-migrate-three-layer.md`.

mod transform;

// Task 3.4 (`run_migrate`) re-exports the pieces it needs once it
// composes them. Keeping the items reachable via `crate::migrate::transform`
// for now and silent on the parent module avoids a `dead_code` warning
// while leaving the public shape obvious.
#[allow(unused_imports)]
pub(crate) use transform::{Diagnostic, TransformResult, transform_ddl};
