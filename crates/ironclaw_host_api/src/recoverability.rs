//! The §5.3.4 recoverability contract, expressed as a type.
//!
//! `docs/reborn/2026-07-17-architecture-simplification-dto-dyn-local.md`
//! **§5.3.4 ("The recoverability contract on the resolution channels, issue
//! #6284")** folds nearai/ironclaw#6284 onto the five-channel resolution model:
//! every mid-run error must satisfy **(a)** the run survives it, **(b)** the
//! model sees it, **(c)** what the model sees carries the cause *and* the
//! remediation, **(d)** the model gets a turn to act on it. Terminal failure is
//! reserved for genuine invariants only — cancellation, budget exhaustion,
//! `DriverBug`.
//!
//! **§11.7 ("Interface conformance harnesses")** names the enforcement: for
//! every variant of every error enum (`CapabilityFailureKind`,
//! `RuntimeDispatchErrorKind`, `AgentLoopHostErrorKind`, `ModelErrorClass`,
//! `LoopFailureKind`, provider categories) an exhaustive-match test proves it
//! maps to *retry* / *a model-visible observation carrying a non-empty
//! remediation hint* / *a park* — never an unclassified terminal bork, and a
//! new variant fails CI until classified.
//!
//! [`RecoverabilityClass`] is those cells. [`RemediationHint`] is the second
//! axis §11.7's sentence demands — "carrying a non-empty remediation hint" —
//! recorded separately because today most model-visible kinds do *not* carry
//! one (#6284 item 4), and the count of the ones that do not is the measurable
//! progress marker for that item.
//!
//! # This records what the code does today, including where that is wrong
//!
//! Every classifier is derived by tracing the live path, not by reading intent
//! out of doc comments. Where today's behavior violates §5.3.4 the arm is still
//! recorded honestly and annotated as a known defect. Later PRs in the epic
//! flip rows; the diff of those rows is the review evidence that the epic is
//! progressing.
//!
//! # Where the classifiers live
//!
//! Each classifier lives beside the enum it classifies, so its exhaustive
//! `match` is compile-forced in the crate that can add a variant:
//!
//! | Enum | Crate |
//! | --- | --- |
//! | [`crate::RuntimeDispatchErrorKind`] | `ironclaw_host_api` (this crate, `dispatch.rs`) |
//! | `RuntimeFailureKind` | `ironclaw_host_runtime` |
//! | `CapabilityFailureKind`, `AgentLoopHostErrorKind`, `LoopFailureKind` | `ironclaw_turns` |
//! | `CapabilityErrorClass`, `ModelErrorClass` | `ironclaw_agent_loop` |
//!
//! **`RuntimeDispatchErrorKind` and `RuntimeFailureKind` are different enums on
//! different layers, and both are real.** §11.7 names only the former.
//! `RuntimeDispatchErrorKind` (this crate) is the redacted category a *runtime
//! lane* reports out of dispatch; `RuntimeFailureKind`
//! (`ironclaw_host_runtime`) is the sanitized category the *host* hands the
//! loop, and it also carries kinds no lane can produce (`Cancelled`,
//! `GateDeclined`, `Transient`). `From<DispatchFailureKind> for
//! RuntimeFailureKind` (`ironclaw_host_runtime/src/production.rs`) folds the
//! first into the second — so the doc is not wrong, it names the upstream enum
//! and the downstream one it folds into needs classifying too. Both are
//! classified, and a conformance test in `ironclaw_host_runtime` pins that the
//! fold preserves the class rather than leaving this crate's table asserted.
//! The fold is lossy on the [`RemediationHint`] axis (several dispatch kinds
//! collapse onto one runtime kind), so that half is pinned as "the upstream
//! table may be more pessimistic, never more optimistic".

use serde::{Deserialize, Serialize};

/// Which §5.3.4 cell a failure kind lands in — the four outcomes §11.7's
/// matrix admits.
///
/// Deliberately **not** `#[non_exhaustive]`: this enum is the classification
/// target, and every consumer must be forced to handle a newly added class
/// rather than silently bucketing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoverabilityClass {
    /// The loop retries internally; the model never sees the failure.
    ///
    /// Satisfies (a); (b)/(d) are moot because there is nothing yet for the
    /// model to act on. Retry budgets are finite, so a kind is classified here
    /// when the *best* outcome the model can reach is a silent retry; the arm's
    /// comment records what happens at budget exhaustion (usually a degrade to
    /// [`Self::ModelVisible`], sometimes [`Self::Terminal`] — §5.3.4's
    /// "`Transient` retries under a per-class budget; when the budget exhausts,
    /// the model is told").
    Retry,
    /// The model observes the failure and gets a turn to act on it.
    ///
    /// Corresponds to `Resolution::Done(Outcome)` carrying a failure verdict —
    /// §5.3.4's "default landing zone" for a failure the model could fix by
    /// acting differently — and to `Resolution::Denied`, which is terminal
    /// *policy* yet still model-visible and obliged to carry what would unlock
    /// the call. Satisfies (a), (b), (d); whether it satisfies (c) is the
    /// separate [`RemediationHint`] axis.
    ModelVisible,
    /// The turn parks awaiting an external actor and re-enters when the gate
    /// resolves.
    ///
    /// Corresponds to `Resolution::Blocked` (approval / auth / resource gate)
    /// and `Resolution::Suspended` (process, dependent run, external tool).
    /// §5.3.4: "`Blocked` already satisfies (a)–(d) structurally."
    Park,
    /// The run ends — the `HostFailure` channel, which §5.3.4 calls "the
    /// *narrow* terminal channel, and only genuinely so".
    ///
    /// # `Terminal` is a defect marker, not an endorsement
    ///
    /// A run ending is legitimate for exactly three invariants:
    ///
    /// 1. cancellation (the user or host asked for it),
    /// 2. budget exhaustion (the run is out of its sanctioned resources), and
    /// 3. `DriverBug` (the loop's own contract was violated).
    ///
    /// Any other kind classified `Terminal` is a **known defect** on the epic's
    /// list — the classifier records it so it is countable and reviewable, not
    /// because it is correct. Moving a kind *into* `Terminal` requires naming
    /// which of the three invariants it satisfies.
    Terminal,
}

impl RecoverabilityClass {
    /// Stable, snake_case identifier matching the serde wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Retry => "retry",
            Self::ModelVisible => "model_visible",
            Self::Park => "park",
            Self::Terminal => "terminal",
        }
    }

    /// Whether the run survives this class — clause (a) of the contract.
    pub const fn run_survives(self) -> bool {
        !matches!(self, Self::Terminal)
    }
}

impl std::fmt::Display for RecoverabilityClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether a [`RecoverabilityClass::ModelVisible`] kind ships the *non-empty
/// remediation hint* §11.7 requires — clause (c) of §5.3.4.
///
/// This is the second recorded axis, and it is a **ratchet, not an assert**.
/// Today most model-visible kinds land on [`Self::Absent`]: the capability
/// observation renderer hands every kind but one a fixed
/// `CapabilityRecoveryHint::RespectFailureConstraint` with an always-empty
/// `repairs` vec, and denials reach the loop with no observation at all.
/// Fixing that is #6284 item 4 and a separate PR. Recording it here makes that
/// item's progress *measurable*: each enum pins its count of [`Self::Absent`]
/// kinds in a test whose comment says the number may only go DOWN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemediationHint {
    /// The model-visible observation for this kind carries an actionable
    /// remediation — what would make the operation succeed, not merely that it
    /// failed. Clause (c) satisfied.
    Substantive,
    /// The kind is model-visible, but what reaches the model is a bare category
    /// plus a generic retry constraint, with no actionable repair. §5.3.4: "A
    /// bare category with no cause is not recoverable." Clause (c) unmet —
    /// #6284 item 4.
    Absent,
    /// Not [`RecoverabilityClass::ModelVisible`], so clause (c) does not apply:
    /// a retried kind has nothing to hint at yet, a parked kind's remediation
    /// *is* the gate, and a terminal kind ends the run.
    NotApplicable,
}

impl RemediationHint {
    /// Stable, snake_case identifier matching the serde wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Substantive => "substantive",
            Self::Absent => "absent",
            Self::NotApplicable => "not_applicable",
        }
    }

    /// Whether this records an unmet clause (c) — a model-visible kind that
    /// ships the model no remediation. This is what the per-enum ratchet counts.
    pub const fn is_hintless_model_visible(self) -> bool {
        matches!(self, Self::Absent)
    }
}

impl std::fmt::Display for RemediationHint {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recoverability_class_wire_name_matches_as_str() {
        for class in [
            RecoverabilityClass::Retry,
            RecoverabilityClass::ModelVisible,
            RecoverabilityClass::Park,
            RecoverabilityClass::Terminal,
        ] {
            let value = serde_json::to_value(class).expect("serialize");
            assert_eq!(value, serde_json::json!(class.as_str()));
            let restored: RecoverabilityClass = serde_json::from_value(value).expect("deserialize");
            assert_eq!(restored, class);
        }
    }

    #[test]
    fn remediation_hint_wire_name_matches_as_str() {
        for hint in [
            RemediationHint::Substantive,
            RemediationHint::Absent,
            RemediationHint::NotApplicable,
        ] {
            let value = serde_json::to_value(hint).expect("serialize");
            assert_eq!(value, serde_json::json!(hint.as_str()));
            let restored: RemediationHint = serde_json::from_value(value).expect("deserialize");
            assert_eq!(restored, hint);
        }
    }

    #[test]
    fn only_terminal_ends_the_run() {
        assert!(RecoverabilityClass::Retry.run_survives());
        assert!(RecoverabilityClass::ModelVisible.run_survives());
        assert!(RecoverabilityClass::Park.run_survives());
        assert!(!RecoverabilityClass::Terminal.run_survives());
    }

    /// Only [`RemediationHint::Absent`] counts against #6284 item 4:
    /// `NotApplicable` marks a non-model-visible kind, which owes no hint.
    #[test]
    fn only_absent_counts_as_an_unmet_remediation_clause() {
        assert!(RemediationHint::Absent.is_hintless_model_visible());
        assert!(!RemediationHint::Substantive.is_hintless_model_visible());
        assert!(!RemediationHint::NotApplicable.is_hintless_model_visible());
    }
}
