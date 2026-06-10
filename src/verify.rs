//! Loader verification: count-level cross-checks on completed loads.
//!
//! Two layers, both in [`VerifyOutcome`]:
//!
//! - **L1 — source vs loader.** Compares the parser-independent
//!   `source_rows` count (pgdump `\n` byte tally; parquet footer total) to
//!   `records_loaded + records_failed`. Catches drops between the source
//!   bytes and the DB-input layer (parser regressions, chunk-data
//!   mis-handling, lost record vector pushes). Naturally `ON CONFLICT`-aware
//!   — neither side moves when conflict resolution skips a row.
//!
//! - **L2 — loader vs target.** Compares `target_delta = post_count -
//!   pre_count` against `records_loaded`. Catches drops between INSERT
//!   submission and the table contents. Verdict shape depends on
//!   [`OnConflict`]: under `Error` a shortfall is `MissingTarget` (real bug);
//!   under `Skip`/`Update` it's `RowsConflictedAtTarget` (informational —
//!   conflict resolution discarded N rows).
//!
//! Out of scope for v1: value-level fidelity (column-swap bugs,
//! `\N`-vs-`""` mistranslation, encoding mishandling, type coercion). A
//! count match proves completeness, not fidelity.

use crate::coordination::manifest::OnConflict;
use crate::db::Pool;
use anyhow::{Context, Result};

/// Whether to run L2 (target pre/post `count(*)`). L1 always runs when
/// `LoadResult.source_rows == Some` regardless of mode — its cost is
/// negligible (the source-row tally is computed during parsing) and its
/// signal is high.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerifyMode {
    /// L2 is off. L1 still runs when source rows are exact.
    #[default]
    Off,
    /// L2 is on: pre/post `count(*)` per loaded table.
    ///
    /// **Sole-writer assumption.** L2 compares `target_post -
    /// target_pre` against `records_loaded`. If anyone else writes to
    /// the same target table between pre-count and post-count the
    /// verdict reflects the combined delta — concurrent inserts surface
    /// as `ExtraTarget`, concurrent deletes as `MissingTarget`. The
    /// operator owns ensuring no other writer touches the target during
    /// the load window. `migrate` against a fresh cluster satisfies
    /// this by construction.
    Count,
}

impl std::str::FromStr for VerifyMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "off" => Ok(VerifyMode::Off),
            "count" => Ok(VerifyMode::Count),
            _ => Err(anyhow::anyhow!(
                "Invalid --verify value '{}'. Must be one of: off, count",
                s
            )),
        }
    }
}

impl std::fmt::Display for VerifyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VerifyMode::Off => write!(f, "off"),
            VerifyMode::Count => write!(f, "count"),
        }
    }
}

/// One verification outcome per loaded table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyOutcome {
    pub schema: String,
    pub table: String,
    /// Mirrored from `LoadResult.source_rows`. `None` for csv/tsv (no exact
    /// source-row count in v1) — short-circuits L1 to
    /// [`VerifyVerdict::SkippedNoExactSourceCount`].
    pub source_rows: Option<u64>,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// Target row count immediately before the load. `None` when L2 was not
    /// run (verify mode `Off`).
    pub target_pre_count: Option<u64>,
    /// Target row count immediately after the load. `None` when L2 was not
    /// run.
    pub target_post_count: Option<u64>,
    pub verdict: VerifyVerdict,
}

/// Single verdict per table, summarising both L1 and L2 with the most
/// actionable signal first. The verdict variant captures the magnitude
/// (`u64` payload) so callers don't need to recompute deltas from the raw
/// counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyVerdict {
    /// Both layers cleared. L1: `source_rows == loaded + failed`. When L2
    /// ran: `target_delta == records_loaded` (or `≤` under non-Error
    /// `OnConflict` modes — see [`RowsConflictedAtTarget`]).
    Match,
    /// L1: `source_rows > loaded + failed`. Rows entered the chunk buffer
    /// and disappeared before reaching the DB layer — parser regression or
    /// chunk-data mis-handling. Payload is `source_rows - (loaded + failed)`.
    LoaderDropped(u64),
    /// L2 under `OnConflict::Error`: `target_delta < records_loaded`.
    /// INSERTs reported success but the target row count grew by less than
    /// the loader claims — a real bug.
    MissingTarget(u64),
    /// L2: `target_delta > records_loaded`. The target gained rows the
    /// loader did not submit — concurrent writer or duplicate accounting
    /// bug. Surfaced under all `OnConflict` modes.
    ExtraTarget(u64),
    /// L2 informational under `OnConflict::Skip`/`DoUpdate`:
    /// `target_delta < records_loaded`. The shortfall is exactly the
    /// number of submitted rows that conflict resolution discarded — not a
    /// bug per se, but the operator should know.
    RowsConflictedAtTarget(u64),
    /// `LoadResult.source_rows == None` (csv/tsv in v1). L1 cannot run; L2
    /// may still have run, but its standalone interpretation is weak
    /// without L1, so we surface this single verdict and leave fine-grained
    /// L2 verdicts to the count-bearing formats. Quote-aware exact counting
    /// for csv/tsv is the v2 follow-up.
    SkippedNoExactSourceCount,
}

/// Inputs to [`classify`]. Grouped to keep the call site readable when
/// orchestrator-level code wires the values up.
#[derive(Debug, Clone, Copy)]
pub struct VerifyInputs {
    pub mode: VerifyMode,
    pub on_conflict: OnConflict,
    pub source_rows: Option<u64>,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// `Some` only when L2 ran. Required to pair with `target_post_count`.
    pub target_pre_count: Option<u64>,
    pub target_post_count: Option<u64>,
}

/// Pure verdict classifier: takes the count layer's inputs and returns the
/// most actionable verdict. No I/O, no allocation — the entire verification
/// surface lives here so it can be exhaustively unit-tested without a DB.
///
/// Decision order:
/// 1. If `source_rows == None` → [`VerifyVerdict::SkippedNoExactSourceCount`].
/// 2. L1 cross-check (`source` vs `loaded + failed`): a shortfall is
///    [`VerifyVerdict::LoaderDropped`] regardless of `OnConflict` (both sides
///    are insensitive to conflict resolution).
/// 3. L2 cross-check (only when `mode == Count` AND both pre/post counts
///    present): see [`VerifyVerdict`] variants for shape per `OnConflict`.
/// 4. Both clean → [`VerifyVerdict::Match`].
pub fn classify(inputs: VerifyInputs) -> VerifyVerdict {
    let Some(source_rows) = inputs.source_rows else {
        return VerifyVerdict::SkippedNoExactSourceCount;
    };

    let processed = inputs.records_loaded + inputs.records_failed;
    if source_rows > processed {
        return VerifyVerdict::LoaderDropped(source_rows - processed);
    }

    // L1 over-count (`processed > source_rows`) is treated as a Match: it
    // cannot happen for the formats covered today (pgdump `\n` count is
    // exact; parquet footer total is exact), and turning it into a verdict
    // would require a new variant the operator can't act on. If a future
    // format introduces uncertainty, add a verdict then.

    if inputs.mode == VerifyMode::Count
        && let (Some(pre), Some(post)) = (inputs.target_pre_count, inputs.target_post_count)
    {
        // saturating_sub rather than `if post >= pre` so a concurrent
        // delete on the table during the load doesn't yield a negative
        // delta and crash the verdict path. The post < pre case still
        // surfaces — it just gets normalised to a delta of 0 (i.e. nothing
        // landed), which then maps to a `MissingTarget` / conflict verdict
        // depending on `on_conflict`.
        let target_delta = post.saturating_sub(pre);

        if target_delta > inputs.records_loaded {
            return VerifyVerdict::ExtraTarget(target_delta - inputs.records_loaded);
        }

        if target_delta < inputs.records_loaded {
            let shortfall = inputs.records_loaded - target_delta;
            return match inputs.on_conflict {
                OnConflict::Error => VerifyVerdict::MissingTarget(shortfall),
                OnConflict::DoNothing | OnConflict::DoUpdate => {
                    VerifyVerdict::RowsConflictedAtTarget(shortfall)
                }
            };
        }
    }

    VerifyVerdict::Match
}

/// Run `SELECT COUNT(*) FROM <schema>.<table>` and return the result.
///
/// The orchestrator and `run_load` use this both for L2's pre-count (before
/// the load) and post-count (after the load). Reuses `Pool::qualified_table_name`
/// so identifier quoting matches what the rest of the loader emits — keeps
/// SQLite (test) and Postgres (prod) on a single code path.
pub async fn count_table_rows(pool: &Pool, schema: &str, table: &str) -> Result<u64> {
    let qualified = pool.qualified_table_name(schema, table);
    let sql = format!("SELECT COUNT(*) FROM {qualified}");
    let rows: Vec<(i64,)> = pool
        .fetch_all_with_binds::<(i64,)>(&sql, &[])
        .await
        .with_context(|| format!("Failed to count rows in {schema}.{table}"))?;
    let count = rows
        .first()
        .map(|(c,)| *c)
        .context("count(*) returned no rows")?;
    // `COUNT(*)` is non-negative on every backend we ship; a negative value
    // would imply a backend bug worth surfacing rather than silently
    // wrapping into a u64.
    if count < 0 {
        anyhow::bail!("count(*) on {schema}.{table} returned negative {count}");
    }
    Ok(count as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(
        mode: VerifyMode,
        on_conflict: OnConflict,
        source_rows: Option<u64>,
        loaded: u64,
        failed: u64,
        pre: Option<u64>,
        post: Option<u64>,
    ) -> VerifyInputs {
        VerifyInputs {
            mode,
            on_conflict,
            source_rows,
            records_loaded: loaded,
            records_failed: failed,
            target_pre_count: pre,
            target_post_count: post,
        }
    }

    #[test]
    fn none_source_short_circuits_to_skipped() {
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            None,
            10,
            0,
            Some(0),
            Some(10),
        ));
        assert_eq!(v, VerifyVerdict::SkippedNoExactSourceCount);
    }

    #[test]
    fn l1_drop_under_error_is_loader_dropped() {
        // source=100, loaded=90, failed=0 → 10 dropped before DB layer.
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            Some(100),
            90,
            0,
            Some(0),
            Some(90),
        ));
        assert_eq!(v, VerifyVerdict::LoaderDropped(10));
    }

    #[test]
    fn l1_drop_takes_precedence_over_l2_missing_target() {
        // Both layers see a problem: L1 sees 10 rows dropped before the
        // DB layer (source=100 vs loaded=90), L2 sees 10 rows missing
        // at the target (delta=80 vs loaded=90 under Error). L1 is the
        // root cause — without it the L2 shortfall has no explanation —
        // so its verdict wins. A regression that returns `MissingTarget`
        // here would hide the L1 signal behind a downstream symptom.
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            Some(100),
            90,
            0,
            Some(0),
            Some(80),
        ));
        assert_eq!(v, VerifyVerdict::LoaderDropped(10));
    }

    #[test]
    fn parse_failures_are_not_drops() {
        // source=100, loaded=70, failed=30 → 70+30 == 100, no drop.
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            Some(100),
            70,
            30,
            Some(0),
            Some(70),
        ));
        assert_eq!(v, VerifyVerdict::Match);
    }

    #[test]
    fn l2_shortfall_under_error_is_missing_target() {
        // L1 clean (source == loaded), L2 short (target_delta < loaded).
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            Some(100),
            100,
            0,
            Some(0),
            Some(95),
        ));
        assert_eq!(v, VerifyVerdict::MissingTarget(5));
    }

    #[test]
    fn l2_shortfall_under_skip_is_rows_conflicted() {
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::DoNothing,
            Some(100),
            100,
            0,
            Some(0),
            Some(95),
        ));
        assert_eq!(v, VerifyVerdict::RowsConflictedAtTarget(5));
    }

    #[test]
    fn l2_shortfall_under_update_is_rows_conflicted() {
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::DoUpdate,
            Some(100),
            100,
            0,
            Some(0),
            Some(95),
        ));
        assert_eq!(v, VerifyVerdict::RowsConflictedAtTarget(5));
    }

    #[test]
    fn l2_excess_is_extra_target_under_all_modes() {
        // target_delta > records_loaded — concurrent writer or duplicate
        // accounting. Same verdict regardless of OnConflict.
        for mode in [
            OnConflict::Error,
            OnConflict::DoNothing,
            OnConflict::DoUpdate,
        ] {
            let v = classify(inputs(
                VerifyMode::Count,
                mode,
                Some(100),
                100,
                0,
                Some(0),
                Some(105),
            ));
            assert_eq!(v, VerifyVerdict::ExtraTarget(5), "mode {mode:?}");
        }
    }

    #[test]
    fn l2_skipped_when_mode_off() {
        // mode=Off → L2 not consulted even with pre/post present (defensive
        // — the orchestrator wouldn't supply them in that mode, but the
        // classifier must not silently use them either).
        let v = classify(inputs(
            VerifyMode::Off,
            OnConflict::Error,
            Some(100),
            100,
            0,
            Some(0),
            Some(95),
        ));
        assert_eq!(v, VerifyVerdict::Match);
    }

    #[test]
    fn l2_skipped_when_pre_post_missing() {
        // mode=Count but counts unavailable (e.g. count query failed and
        // we synthesised None) — fall back to L1-only Match.
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            Some(100),
            100,
            0,
            None,
            None,
        ));
        assert_eq!(v, VerifyVerdict::Match);
    }

    #[test]
    fn pre_greater_than_post_does_not_panic() {
        // Concurrent delete during the load would invert pre/post —
        // saturating_sub keeps the verdict path safe; it surfaces as
        // MissingTarget under Error (or RowsConflicted under non-Error)
        // because target_delta resolves to 0.
        let v = classify(inputs(
            VerifyMode::Count,
            OnConflict::Error,
            Some(10),
            10,
            0,
            Some(20),
            Some(15),
        ));
        assert_eq!(v, VerifyVerdict::MissingTarget(10));
    }

    #[test]
    fn verify_mode_round_trips_through_str() {
        for m in [VerifyMode::Off, VerifyMode::Count] {
            let s = m.to_string();
            assert_eq!(s.parse::<VerifyMode>().unwrap(), m);
        }
    }

    #[test]
    fn verify_mode_rejects_unknown() {
        assert!("strict".parse::<VerifyMode>().is_err());
    }
}
