//! Runs the `pg_dump → DSQL` migrate flow: extract embedded DDL, transform
//! it for DSQL via `dsql-lint`, apply it to the cluster, then load the data.
//!
//! Composed of small, testable pieces (extract / transform / apply / load),
//! glued together by [`run_migrate`] — see the per-task spec in
//! `docs/plans/2026-06-02-pgdump-migrate-three-layer.md`.

mod apply;
mod orchestrator;
mod transform;

// Task 3.5 (CLI subcommand) re-exports `run_migrate` and `MigrateArgs`
// once main.rs dispatches to it. Until then keep the items reachable
// via their submodule paths and silence the warnings centrally.
#[allow(unused_imports)]
pub(crate) use apply::{AppliedStatement, ApplyOutcome, apply_ddl};
#[allow(unused_imports)]
pub(crate) use orchestrator::{MigrateArgs, MigrateReport, TableLoadSummary, run_migrate};
#[allow(unused_imports)]
pub(crate) use transform::{Diagnostic, TransformResult, transform_ddl};
