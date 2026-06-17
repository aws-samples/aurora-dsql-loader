//! Load verification.
//!
//! - **L1**: source `source_rows` vs `loaded + failed`. Catches drops
//!   between source bytes and the DB layer (parser/chunker bugs).
//! - **L2**: target `post - pre` vs `loaded`. Catches drops between
//!   INSERT and table contents. Shortfall is `MissingTarget` under
//!   `Error`, `RowsConflictedAtTarget` under `DoNothing`/`DoUpdate`.
//! - **L3** (`mode=Full`): per-row value comparison by primary key — see
//!   [`value_check`]. Verifies the target reproduces the loaded value;
//!   trusts the reader to decode (the self-as-oracle ceiling).

mod value_check;

pub(crate) use value_check::{
    LoadVerifyTarget, run_load_value_check, schema_check, verify_table_values,
};

use crate::coordination::manifest::OnConflict;
use crate::db::Pool;
use anyhow::{Context, Result};

/// Verification depth toggle. Source-row counting happens parser-side
/// regardless; higher modes add more checks and produce a `VerifyOutcome`
/// per loaded table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum VerifyMode {
    #[default]
    Off,
    /// Pre/post `count(*)` per loaded table (L2). Assumes the load is the
    /// sole writer to the target during the load window — concurrent
    /// writes surface as `ExtraTarget` (insert) or `MissingTarget`
    /// (delete). `migrate` against a fresh cluster satisfies this.
    Count,
    /// Everything `Count` does, plus per-row value verification (L3):
    /// re-read the source, fetch each target row by primary key, and
    /// confirm the target reproduces the loaded value. Cost ≈ one extra
    /// full read of the table — opt-in. Carries an inherent decode-class
    /// blind spot (it trusts the reader); see the value-fidelity design.
    Full,
}

impl std::str::FromStr for VerifyMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "off" => Ok(VerifyMode::Off),
            "count" => Ok(VerifyMode::Count),
            "full" => Ok(VerifyMode::Full),
            _ => Err(anyhow::anyhow!(
                "Invalid --verify value '{}'. Must be one of: off, count, full",
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
            VerifyMode::Full => write!(f, "full"),
        }
    }
}

/// L2 pre/post pair. `Some` exactly when L2 ran; the `(pre, post)`
/// invariant — both present together — is enforced by construction
/// rather than by convention on two parallel `Option` fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Counts {
    pub pre: u64,
    pub post: u64,
}

/// Result of the L3 value-verification pass (`mode=Full`). `Ran` carries
/// the aggregate counts across all PK batches; `Skipped` means L3 was
/// requested but could not run (no usable PK / non-exact-count format).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum L3Outcome {
    Ran {
        /// True total of rows whose target value diverged, summed across
        /// every batch (not capped — the detail list is capped separately).
        value_mismatches: u64,
        /// True total of source PKs not found at the target.
        rows_missing_at_target: u64,
    },
    Skipped,
}

impl L3Outcome {
    /// Pair this outcome with its per-PK detail, keeping `details` only when
    /// L3 actually `Ran` (a `Skipped` run has no PKs to localize). Centralizes
    /// the "details only when Ran" invariant the migrate and load paths share.
    pub(crate) fn with_details(self, details: L3Details) -> (Option<L3Outcome>, Option<L3Details>) {
        let details = matches!(self, L3Outcome::Ran { .. }).then_some(details);
        (Some(self), details)
    }
}

/// Affirmative schema-conformance line (MUST #3): the column set the load
/// enforced matched the dump's COPY columns, and whether the table has a
/// primary or unique key. Scoped to **column-set + key presence only** —
/// NOT column types/nullability/defaults (a successful load already proves
/// the column set matches via `align_pgdump_schema_to_copy_columns`; deeper
/// checks are out of scope, see the requirements doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SchemaCheck {
    /// `Some(n)` = the load enforced an n-column COPY set; `None` = not
    /// counted (non-pgdump format has no COPY block to measure), distinct
    /// from `Some(0)`.
    pub columns_matched: Option<u64>,
    /// True when the target has a primary key OR a unique constraint
    /// (`get_unique_constraint_columns` falls back to the first unique key).
    pub pk_present: bool,
}

/// Per-PK localization for L3 (`mode=Full`): the first K offending primary
/// keys, so the operator sees *which* rows diverged, not just how many.
/// PK-level only (the DMS `KEY` analog); column-level detail (DMS
/// `DETAILS`) is a follow-up. Lists are capped; the verdict magnitude
/// carries the true total.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct L3Details {
    pub mismatch_pks: Vec<String>,
    pub missing_pks: Vec<String>,
}

/// One verification outcome per loaded table.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyOutcome {
    pub schema: String,
    pub table: String,
    /// `None` for csv/tsv — L1 short-circuits to `SkippedNoExactSourceCount`.
    pub source_rows: Option<u64>,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// `Some` when L2 ran, `None` otherwise.
    pub target_counts: Option<L2Counts>,
    pub verdict: VerifyVerdict,
    /// Affirmative schema-conformance line; `None` when verify is `Off`.
    pub schema_check: Option<SchemaCheck>,
    /// First-K offending PKs when L3 found divergence; `None` when L3 didn't
    /// run or found nothing to localize.
    pub l3_details: Option<L3Details>,
}

/// Most actionable verdict per table; payload is the magnitude.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum VerifyVerdict {
    /// L1 clean, and L2 (if run) clean.
    Match,
    /// L1 shortfall: rows lost between source bytes and the DB layer.
    LoaderDropped(u64),
    /// L2 shortfall under `OnConflict::Error` — INSERT reported success
    /// but target grew by less than `records_loaded`. Concurrent DELETE
    /// or rolled-back INSERT (verify=count assumes sole writer).
    MissingTarget(u64),
    /// L2 excess: target grew more than `records_loaded`. Concurrent
    /// INSERT (verify=count assumes sole writer) or duplicate accounting.
    ExtraTarget(u64),
    /// L2 shortfall under `DoNothing`/`DoUpdate` — N rows resolved by
    /// ON CONFLICT. CLI exits 1 so chained `migrate && deploy.sh` does
    /// not ship a partially-loaded cluster without operator review.
    RowsConflictedAtTarget(u64),
    /// csv/tsv: no exact source count, so L1 can't run.
    SkippedNoExactSourceCount,
    /// L3 (`mode=Full`): N rows where the target value differs from the
    /// source after server-side re-cast. The **true total** across all
    /// PK batches; per-PK detail (capped) rides `VerifyOutcome`.
    ValueMismatch(u64),
    /// L3: N source PKs absent from the target. More fundamental than a
    /// value diff (the row isn't there at all), so it outranks
    /// `ValueMismatch` for the single verdict slot.
    ValueRowMissingAtTarget(u64),
    /// L3 requested (`mode=Full`) but could not run — no usable
    /// single-column primary/unique key, or a format without an exact
    /// source-row count. Explicit "not checked", never a silent pass.
    /// (A NULL value in the source key errors, it does not skip.)
    ValueCheckSkipped,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct VerifyInputs {
    pub mode: VerifyMode,
    pub on_conflict: OnConflict,
    pub source_rows: Option<u64>,
    pub records_loaded: u64,
    pub records_failed: u64,
    /// `Some` when L2 ran, `None` otherwise.
    pub target_counts: Option<L2Counts>,
    /// `Some` when L3 (`mode=Full`) ran or was attempted, `None` otherwise.
    pub l3: Option<L3Outcome>,
}

impl VerifyOutcome {
    /// Classify `inputs` and bind the verdict to `(schema, table)`. The
    /// affirmative outputs (`schema_check`, `l3_details`) are report-only —
    /// they don't affect the verdict, so they ride alongside `inputs`
    /// (which stays `Copy`) rather than feeding `classify`.
    pub(crate) fn from_inputs(
        schema: String,
        table: String,
        inputs: VerifyInputs,
        schema_check: Option<SchemaCheck>,
        l3_details: Option<L3Details>,
    ) -> Self {
        let verdict = classify(inputs);
        VerifyOutcome {
            schema,
            table,
            source_rows: inputs.source_rows,
            records_loaded: inputs.records_loaded,
            records_failed: inputs.records_failed,
            target_counts: inputs.target_counts,
            verdict,
            schema_check,
            l3_details,
        }
    }
}

/// Pure verdict classifier — no I/O, exhaustively unit-tested.
///
/// Order (precedence source > L1 > L2 > L3): source `None` →
/// `SkippedNoExactSourceCount`; L1 shortfall → `LoaderDropped`; L2 (if
/// mode is Count/Full and counts present) → `Missing`/`Extra`/`Conflicted`;
/// L3 (if mode is Full) → `ValueRowMissingAtTarget`/`ValueMismatch`/
/// `ValueCheckSkipped`; otherwise `Match`.
pub(crate) fn classify(inputs: VerifyInputs) -> VerifyVerdict {
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

    // L2 runs under both Count and Full (Full implies Count).
    if matches!(inputs.mode, VerifyMode::Count | VerifyMode::Full)
        && let Some(L2Counts { pre, post }) = inputs.target_counts
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

    // L3 (mode=Full): runs only after L1 and L2 cleared, preserving
    // precedence source > L1 > L2 > L3. Within L3, a missing row (data
    // absent) outranks a value mismatch for the single verdict slot.
    if let Some(l3) = inputs.l3 {
        match l3 {
            L3Outcome::Skipped => return VerifyVerdict::ValueCheckSkipped,
            L3Outcome::Ran {
                value_mismatches,
                rows_missing_at_target,
            } => {
                if rows_missing_at_target > 0 {
                    return VerifyVerdict::ValueRowMissingAtTarget(rows_missing_at_target);
                }
                if value_mismatches > 0 {
                    return VerifyVerdict::ValueMismatch(value_mismatches);
                }
            }
        }
    }

    VerifyVerdict::Match
}

/// `SELECT COUNT(*)` for L2 pre/post. Reuses
/// `Pool::qualified_table_name` so identifier quoting matches the rest
/// of the loader.
pub(crate) async fn count_table_rows(pool: &Pool, schema: &str, table: &str) -> Result<u64> {
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
        let target_counts = match (pre, post) {
            (Some(pre), Some(post)) => Some(L2Counts { pre, post }),
            (None, None) => None,
            _ => panic!("test helper: pre/post must both be Some or both None"),
        };
        VerifyInputs {
            mode,
            on_conflict,
            source_rows,
            records_loaded: loaded,
            records_failed: failed,
            target_counts,
            l3: None,
        }
    }

    /// L1+L2-clean inputs at `mode=Full` carrying an L3 outcome — the base
    /// for the value-verdict tests. Counts are set so L1 and L2 both pass,
    /// so the verdict is driven purely by `l3`.
    fn full_with_l3(l3: L3Outcome) -> VerifyInputs {
        VerifyInputs {
            l3: Some(l3),
            ..inputs(
                VerifyMode::Full,
                OnConflict::Error,
                Some(100),
                100,
                0,
                Some(0),
                Some(100),
            )
        }
    }

    #[test]
    fn l3_clean_is_match() {
        let v = classify(full_with_l3(L3Outcome::Ran {
            value_mismatches: 0,
            rows_missing_at_target: 0,
        }));
        assert_eq!(v, VerifyVerdict::Match);
    }

    #[test]
    fn l3_value_mismatch_reports_total() {
        let v = classify(full_with_l3(L3Outcome::Ran {
            value_mismatches: 7,
            rows_missing_at_target: 0,
        }));
        assert_eq!(v, VerifyVerdict::ValueMismatch(7));
    }

    #[test]
    fn l3_missing_at_target_reports_total() {
        let v = classify(full_with_l3(L3Outcome::Ran {
            value_mismatches: 0,
            rows_missing_at_target: 3,
        }));
        assert_eq!(v, VerifyVerdict::ValueRowMissingAtTarget(3));
    }

    #[test]
    fn l3_missing_takes_precedence_over_mismatch() {
        // A table with both missing rows and value mismatches: missing
        // (data absent) is the more fundamental signal and wins the single
        // verdict slot. The per-PK detail list carries both.
        let v = classify(full_with_l3(L3Outcome::Ran {
            value_mismatches: 5,
            rows_missing_at_target: 2,
        }));
        assert_eq!(v, VerifyVerdict::ValueRowMissingAtTarget(2));
    }

    #[test]
    fn l3_skipped_is_value_check_skipped() {
        // mode=Full requested but L3 couldn't run (no usable PK, or a
        // non-exact-count format) — explicit "not checked", never a silent
        // pass.
        let v = classify(full_with_l3(L3Outcome::Skipped));
        assert_eq!(v, VerifyVerdict::ValueCheckSkipped);
    }

    #[test]
    fn l1_drop_takes_precedence_over_l3() {
        // L1 shortfall must win over any L3 result (source > L1 > L2 > L3).
        let mut i = full_with_l3(L3Outcome::Ran {
            value_mismatches: 9,
            rows_missing_at_target: 0,
        });
        i.source_rows = Some(100);
        i.records_loaded = 90;
        i.records_failed = 0;
        assert_eq!(classify(i), VerifyVerdict::LoaderDropped(10));
    }

    #[test]
    fn l2_shortfall_takes_precedence_over_l3() {
        // L2 shortfall must win over L3 (precedence L2 > L3).
        let mut i = full_with_l3(L3Outcome::Ran {
            value_mismatches: 9,
            rows_missing_at_target: 0,
        });
        i.target_counts = Some(L2Counts { pre: 0, post: 95 });
        assert_eq!(classify(i), VerifyVerdict::MissingTarget(5));
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
        for m in [VerifyMode::Off, VerifyMode::Count, VerifyMode::Full] {
            let s = m.to_string();
            assert_eq!(s.parse::<VerifyMode>().unwrap(), m);
        }
    }

    #[test]
    fn verify_mode_rejects_unknown() {
        assert!("strict".parse::<VerifyMode>().is_err());
    }
}
