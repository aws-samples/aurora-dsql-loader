//! Wrap `dsql_lint::fix_sql` so the migrate orchestrator gets a small,
//! loader-shaped result type instead of the full `dsql_lint::FixOutput` /
//! `dsql_lint::Diagnostic` surface. The wrapper lets us:
//!
//! 1. Split diagnostics into `changes` (auto-fix applied) vs `unfixable`
//!    (operator action required) since the migrate report needs both
//!    counts and the orchestrator decides whether `unfixable > 0` blocks
//!    the apply step.
//! 2. Keep the `dsql_lint::LintRule` enum out of the loader's public API
//!    (it is documented as opaque and may grow over time); we only carry
//!    its serde string form, so a future rule rename can't break our
//!    consumers.

use dsql_lint::{FixResult as LintFixResult, fix_sql};

/// Outcome of running `dsql_lint::fix_sql` on a slice of DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformResult {
    /// DSQL-compatible DDL produced by `fix_sql`. Empty input round-trips
    /// to empty output.
    pub fixed_sql: String,
    /// Diagnostics where `dsql-lint` could and did rewrite the SQL
    /// (`Fixed` / `FixedWithWarning`). The orchestrator surfaces these as
    /// "changes applied"; counts feed the migrate report.
    pub changes: Vec<Diagnostic>,
    /// Diagnostics where `dsql-lint` flagged a problem but could NOT
    /// automatically fix it. The orchestrator must NOT silently apply DDL
    /// while these are non-empty: the user has to edit the dump first.
    pub unfixable: Vec<Diagnostic>,
}

/// A single finding from `dsql-lint`, flattened to the fields we surface.
/// Carries the rule as its serde string form rather than the upstream
/// `LintRule` enum so we don't re-export `dsql-lint`'s public API and so
/// upstream rule renames don't ripple into our callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    /// Stable string id for the rule that fired (e.g. `"serial_sequence_idiom"`,
    /// `"foreign_key"`). Treat as opaque — `dsql-lint` may add or rename
    /// rules in any release.
    pub rule: String,
    /// 1-based line number in the input DDL where the offending statement starts.
    pub line: usize,
    /// Human-readable description of the problem.
    pub message: String,
    /// Suggested manual fix (always populated, even when the rule could be
    /// auto-fixed — useful for log output and `--dry-run`).
    pub suggestion: String,
    /// When auto-fixable, additional warning text describing what the fix
    /// did and any side-effects the operator must be aware of (e.g.
    /// "identity counter not advanced"). `None` for unfixable rules.
    pub fix_detail: Option<String>,
}

/// Run `dsql_lint::fix_sql` on the supplied DDL and split its diagnostics
/// into the changes/unfixable pair the migrate orchestrator expects.
pub fn transform_ddl(ddl: &str) -> TransformResult {
    let out = fix_sql(ddl);
    let mut changes = Vec::new();
    let mut unfixable = Vec::new();

    for d in out.diagnostics {
        let (bucket, fix_detail) = match &d.fix_result {
            LintFixResult::Fixed(detail) | LintFixResult::FixedWithWarning(detail) => {
                (&mut changes, Some(detail.clone()))
            }
            LintFixResult::Unfixable => (&mut unfixable, None),
        };
        bucket.push(Diagnostic {
            rule: rule_id(&d.rule),
            line: d.line,
            message: d.message,
            suggestion: d.suggestion,
            fix_detail,
        });
    }

    TransformResult {
        fixed_sql: out.sql,
        changes,
        unfixable,
    }
}

/// Convert a `dsql_lint::LintRule` into its serde-snake_case string id.
/// Done by serializing through `serde_json` because `dsql-lint` does not
/// expose the kebab/snake mapping any other way; the cost is one tiny
/// allocation per diagnostic.
fn rule_id(rule: &dsql_lint::LintRule) -> String {
    serde_json::to_value(rule)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        // `LintRule` derives `Serialize` with `rename_all = "snake_case"`,
        // so the success path is the only path; `Debug` is just a defense
        // against a future feature flag turning serialization off.
        .unwrap_or_else(|| format!("{rule:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// REAL pg_dump output for a SERIAL PK table — the four-statement
    /// idiom we built dsql-lint to handle. The test asserts that `transform_ddl`
    /// produces DSQL-compatible DDL: a single CREATE TABLE with an inline
    /// identity column, with the CREATE SEQUENCE / OWNED BY / SET DEFAULT
    /// statements gone. This is the proof that the whole stack composes —
    /// hand-written inline `SERIAL` would be unrepresentative because that
    /// path was already covered before pg_dump support existed.
    #[test]
    fn real_pgdump_serial_idiom_collapses_to_inline_identity() {
        let ddl = "\
CREATE TABLE public.events (id integer NOT NULL, payload text);
CREATE SEQUENCE public.events_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id;
ALTER TABLE ONLY public.events ALTER COLUMN id SET DEFAULT nextval('public.events_id_seq'::regclass);
";
        let r = transform_ddl(ddl);

        let upper = r.fixed_sql.to_uppercase();
        assert!(
            upper.contains("BIGINT") && upper.contains("GENERATED BY DEFAULT AS IDENTITY"),
            "fixed DDL should have inline identity column, got:\n{}",
            r.fixed_sql
        );
        assert!(
            !upper.contains("CREATE SEQUENCE"),
            "CREATE SEQUENCE should be collapsed away, got:\n{}",
            r.fixed_sql
        );
        assert!(
            !upper.contains("ALTER SEQUENCE"),
            "ALTER SEQUENCE OWNED BY should be removed, got:\n{}",
            r.fixed_sql
        );
        assert!(
            !upper.contains("SET DEFAULT"),
            "SET DEFAULT nextval should be collapsed away, got:\n{}",
            r.fixed_sql
        );

        // Exactly one change diagnostic: the SERIAL idiom collapse.
        assert_eq!(
            r.changes.len(),
            1,
            "expected exactly one change diagnostic, got: {:?}",
            r.changes
        );
        assert_eq!(r.changes[0].rule, "serial_sequence_idiom");
        assert!(
            r.changes[0]
                .fix_detail
                .as_ref()
                .is_some_and(|d| d.to_lowercase().contains("identity")),
            "fix_detail should mention identity replacement, got: {:?}",
            r.changes[0].fix_detail
        );
        assert!(
            r.unfixable.is_empty(),
            "no unfixable diagnostics expected, got: {:?}",
            r.unfixable
        );
    }

    /// A clean CREATE TABLE that needs no transformation must round-trip
    /// (ignoring trivial whitespace canonicalization) and produce no
    /// diagnostics. Guards against the wrapper accidentally dropping or
    /// re-flagging already-DSQL-compatible DDL.
    #[test]
    fn dsql_compatible_ddl_passes_through_clean() {
        let ddl = "CREATE TABLE public.t (id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1));";
        let r = transform_ddl(ddl);
        assert!(
            r.fixed_sql.contains("GENERATED BY DEFAULT AS IDENTITY"),
            "fixed_sql should preserve the identity declaration, got:\n{}",
            r.fixed_sql
        );
        assert!(r.changes.is_empty(), "no changes expected: {:?}", r.changes);
        assert!(
            r.unfixable.is_empty(),
            "no unfixable expected: {:?}",
            r.unfixable
        );
    }

    /// Inline foreign keys are supported by DSQL and must pass through
    /// without a lossy rewrite or diagnostic.
    #[test]
    fn inline_foreign_key_passes_through() {
        let ddl = "\
CREATE TABLE public.parent (id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1) PRIMARY KEY);
CREATE TABLE public.child (id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1), pid BIGINT REFERENCES public.parent(id));
";
        let r = transform_ddl(ddl);
        assert!(
            r.unfixable.is_empty(),
            "FK should be supported: {:?}",
            r.unfixable
        );
        assert!(
            r.changes.is_empty(),
            "FK should not be rewritten: {:?}",
            r.changes
        );
        assert!(
            r.fixed_sql.to_uppercase().contains("REFERENCES"),
            "REFERENCES clause should be retained, got:\n{}",
            r.fixed_sql
        );
    }

    #[test]
    fn alter_added_foreign_key_becomes_not_valid() {
        let ddl = "\
CREATE TABLE public.parent (id bigint PRIMARY KEY);
CREATE TABLE public.child (id bigint PRIMARY KEY, parent_id bigint);
ALTER TABLE public.child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES public.parent(id);
";
        let r = transform_ddl(ddl);
        assert!(r.unfixable.is_empty());
        assert!(r.changes.iter().any(|d| d.rule == "foreign_key_not_valid"));
        assert!(r.fixed_sql.to_uppercase().contains("FOREIGN KEY"));
        assert!(r.fixed_sql.to_uppercase().contains("NOT VALID"));
    }

    /// An unsupported array type must surface as `unfixable`; the
    /// orchestrator uses this to refuse to apply incompatible DDL.
    #[test]
    fn array_type_surfaces_as_unfixable() {
        let ddl = "CREATE TABLE public.t (items integer[]);";
        let r = transform_ddl(ddl);
        assert!(
            !r.unfixable.is_empty(),
            "expected at least one unfixable diagnostic, got: {:?}",
            r
        );
        let array = r
            .unfixable
            .iter()
            .find(|d| d.rule == "array_type")
            .unwrap_or_else(|| panic!("expected an array_type unfixable: {:?}", r.unfixable));
        assert_eq!(array.line, 1);
        assert!(
            array.fix_detail.is_none(),
            "unfixable must not carry fix_detail"
        );
    }

    /// Empty input round-trips to empty output with no diagnostics.
    /// Guards against the orchestrator panicking on an empty extract_ddl
    /// result (e.g. a `--data-only` dump fed by mistake).
    #[test]
    fn empty_input_round_trips() {
        let r = transform_ddl("");
        assert_eq!(r.fixed_sql, "");
        assert!(r.changes.is_empty());
        assert!(r.unfixable.is_empty());
    }
}
