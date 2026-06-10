//! Count-level load verification.
//!
//! - **L1**: source `source_rows` vs `loaded + failed`. Catches drops
//!   between source bytes and the DB layer (parser/chunker bugs).
//! - **L2**: target `post - pre` vs `loaded`. Catches drops between
//!   INSERT and table contents. Shortfall is `MissingTarget` under
//!   `Error`, `RowsConflictedAtTarget` under `Skip`/`Update`.
//!
//! Counts only — no value-level fidelity check.

use crate::coordination::manifest::OnConflict;
use crate::db::Pool;
use anyhow::{Context, Result};

/// L2 toggle. L1 always runs when `source_rows` is exact (cost: free,
/// computed during parsing).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VerifyMode {
    #[default]
    Off,
    /// Pre/post `count(*)` per loaded table. Assumes the load is the
    /// sole writer to the target during the load window — concurrent
    /// writes surface as `ExtraTarget` (insert) or `MissingTarget`
    /// (delete). `migrate` against a fresh cluster satisfies this.
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
    /// `None` for csv/tsv — L1 short-circuits to `SkippedNoExactSourceCount`.
    pub source_rows: Option<u64>,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// Both `Some` when L2 ran, both `None` otherwise.
    pub target_pre_count: Option<u64>,
    pub target_post_count: Option<u64>,
    pub verdict: VerifyVerdict,
}

/// Most actionable verdict per table; payload is the magnitude.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyVerdict {
    /// L1 clean, and L2 (if run) clean.
    Match,
    /// L1 shortfall: rows lost between source bytes and the DB layer.
    LoaderDropped(u64),
    /// L2 shortfall under `OnConflict::Error` — INSERT reported success
    /// but target grew by less than `records_loaded`. Real bug.
    MissingTarget(u64),
    /// L2 excess: target grew more than `records_loaded`. Concurrent
    /// writer or duplicate accounting.
    ExtraTarget(u64),
    /// L2 shortfall under `Skip`/`DoUpdate` — exactly N rows lost to
    /// conflict resolution. Informational, not a bug.
    RowsConflictedAtTarget(u64),
    /// csv/tsv: no exact source count, so L1 can't run.
    SkippedNoExactSourceCount,
}

#[derive(Debug, Clone, Copy)]
pub struct VerifyInputs {
    pub mode: VerifyMode,
    pub on_conflict: OnConflict,
    pub source_rows: Option<u64>,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// Both `Some` when L2 ran, both `None` otherwise.
    pub target_pre_count: Option<u64>,
    pub target_post_count: Option<u64>,
}

/// Pure verdict classifier — no I/O, exhaustively unit-tested.
///
/// Order: source `None` → `SkippedNoExactSourceCount`; L1 shortfall →
/// `LoaderDropped`; L2 (if mode=Count and pre/post present) → see
/// variants; otherwise `Match`.
pub fn classify(inputs: VerifyInputs) -> VerifyVerdict {
    let Some(source_rows) = inputs.source_rows else {
        return VerifyVerdict::SkippedNoExactSourceCount;
    };

    let processed = inputs.records_loaded + inputs.records_failed;
    if source_rows > processed {
        return VerifyVerdict::LoaderDropped(source_rows - processed);
    }
    // Over-count (processed > source) cannot happen for pgdump or
    // parquet (counts are exact); collapse to Match. Add a variant when
    // a future format introduces uncertainty.

    if inputs.mode == VerifyMode::Count
        && let (Some(pre), Some(post)) = (inputs.target_pre_count, inputs.target_post_count)
    {
        // saturating_sub: a concurrent DELETE (post < pre) normalises to
        // delta=0 → MissingTarget/RowsConflictedAtTarget, instead of
        // panicking on underflow.
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

/// `SELECT COUNT(*)` for L2 pre/post. Reuses
/// `Pool::qualified_table_name` so identifier quoting matches the rest
/// of the loader.
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
    // Negative count = backend bug; surface it rather than wrap into u64.
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
        // source > loaded+failed → 10 dropped before DB layer.
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
        // L1 short by 10 AND L2 short by 10 — L1 is the root cause and
        // wins. A regression returning MissingTarget would hide the L1
        // signal behind a downstream symptom.
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
        // loaded+failed == source → no L1 drop.
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
        // L1 clean, L2 short → MissingTarget.
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
        // target_delta > loaded → ExtraTarget regardless of OnConflict.
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
        // mode=Off must not consult L2 even if pre/post leak in.
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
        // mode=Count but pre/post are None → fall back to L1-only Match.
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
        // Concurrent delete (post < pre): saturating_sub → delta=0 →
        // MissingTarget under Error. No panic.
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
