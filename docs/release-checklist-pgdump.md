# pg_dump → DSQL release-gate checklist

Run before tagging a release that touches the `--format pgdump` code path
(anything under `src/formats/pgdump/`, the coordinator's pgdump column-set
guard, the `list-tables` subcommand, or the pgdump arms in `runner.rs` /
`main.rs`).

The automated E2E test [tests/pgdump_e2e_dsql.rs](../tests/pgdump_e2e_dsql.rs)
covers the round-trip when these env vars are set:

```bash
export DSQL_E2E_ENDPOINT=<cluster>.dsql.us-east-1.on.aws
export DSQL_E2E_REGION=us-east-1
export DSQL_E2E_USERNAME=admin
export DSQL_E2E_SOURCE_PG_URL='postgres://localhost:5432/postgres'
export DSQL_E2E_TARGET_URL='postgres://admin@<cluster>.dsql.us-east-1.on.aws:5432/postgres'
cargo test --test pgdump_e2e_dsql -- --nocapture
```

If the env vars are unset the test prints `skipping ...` and returns Ok, so it
is safe in CI but only actually exercises the path when an engineer has the
credentials and binaries set up.

## Manual verifications (in addition to the automated test)

Run these once per pgdump-touching release. Tick each in the PR description.

- [ ] **Plain dump, single table.** `pg_dump --data-only -Fp --table=t` →
  `aurora-dsql-loader load --format pgdump --table t`. Row count and a
  spot-checked row match the source.
- [ ] **Multi-table loop with `list-tables`.** Dump a DB containing ≥ 3 tables.
  `aurora-dsql-loader list-tables --source-uri dump.sql` enumerates each
  block with the correct schema/table/column tuple. Pipe through
  `while read schema table cols; do load ...; done` and confirm every table
  loads.
- [ ] **Custom format rejection (`-Fc`).** `pg_dump -Fc --data-only` produces
  a binary archive. Pointing the loader at it must produce the
  `NonPlainPgDump` diagnostic that names `-Fp`, NOT a generic "no COPY block
  found".
- [ ] **Column reorder.** Pre-create the DSQL target with columns in a
  different order than the source. Load must succeed and rows land in the
  correct columns (see `pgdump_reorders_columns_when_target_order_differs`
  for the SQLite-side analogue).
- [ ] **Column-set mismatch.** Pre-create the DSQL target with an extra
  column not in the dump (or missing one that is). Load must reject
  before any rows are inserted, and the error must name both the column
  and which side it is on.
- [ ] **Resume after kill.** Start a multi-chunk pgdump load. Kill it
  mid-flight (`SIGINT`). Re-run with `--resume-job-id <id>`. Final row
  count must equal the dump's row count, with no duplicates and no gaps.
- [ ] **NULL/empty trade-off is acceptable for the release.** Confirm the
  documented v1 behavior — `\N` and whitespace-only TEXT both load as SQL
  NULL — has not regressed and is still acceptable for the customer use
  cases this release targets. If a customer needs `''` vs NULL fidelity,
  this gates a follow-up issue rather than blocking the release.

## Sign-off

Release gate signed off by: _________________  Date: _________________

PR: _________________  Cluster used: _________________
