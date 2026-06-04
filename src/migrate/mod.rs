//! Runs the `pg_dump → DSQL` migrate flow: extract embedded DDL, transform
//! it for DSQL via `dsql-lint`, apply it to the cluster, then load the data.
//!
//! Composed of small, testable pieces (extract / transform / apply / load),
//! glued together by [`run_migrate`] — see the per-task spec in
//! `docs/plans/2026-06-02-pgdump-migrate-three-layer.md`.

mod apply;
mod orchestrator;
mod transform;

// Public API: the orchestrator + its result/argument types are re-
// exported through `crate::runner::` (the documented public surface)
// so external consumers reach migrate through the same namespace as
// `run_load`. Keep the structs `pub` here so `pub use` at the runner
// layer can hoist them across the crate boundary.
pub use orchestrator::{MigrateArgs, MigrateReport, TableLoadSummary, run_migrate};

// Public surface of the report: AppliedStatement / ApplyOutcome /
// Diagnostic are reachable through MigrateReport, so they ship in the
// public API alongside it.
pub use apply::{AppliedStatement, ApplyOutcome};
pub use transform::Diagnostic;
