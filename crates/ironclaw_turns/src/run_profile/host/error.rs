//! Sanitized agent-loop host error type, its kinds/reason-kinds, and the shared
//! `unsupported host method` constructor used by fail-closed port defaults.

use ironclaw_host_api::{RecoverabilityClass, RemediationHint};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{LoopDiagnosticRef, LoopGateRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoopHostErrorKind {
    Unauthorized,
    /// Host-owned credential acquisition failed for the requested provider/model.
    /// The error summary must stay sanitized and must not expose secret material,
    /// token refresh details, or backend-specific credential-store errors.
    CredentialUnavailable,
    ScopeMismatch,
    StaleSurface,
    InvalidInvocation,
    /// The request payload itself is well-formed but its content is invalid in
    /// the current host state (e.g. schema id/version mismatch on checkpoint load).
    Invalid,
    /// The model/provider output was structurally invalid for the active loop contract.
    InvalidOutput,
    /// The provider refused to produce the completion because its content
    /// filter rejected the request or response.
    ContentFiltered,
    PolicyDenied,
    BudgetExceeded,
    /// The model call would push utilization past the configured pause
    /// threshold. Callers surface an approval gate (foreground or
    /// background) and retry after the user resolves it.
    BudgetApprovalRequired,
    /// Durable budget accounting (reservation read/write/reconcile)
    /// failed. Distinct from `BudgetExceeded`/`BudgetApprovalRequired`
    /// because the failure is in the governor itself, not in the budget
    /// outcome — callers must fail closed.
    BudgetAccountingFailed,
    Unavailable,
    Cancelled,
    CheckpointRejected,
    TranscriptWriteFailed,
    Internal,
}

impl AgentLoopHostErrorKind {
    /// Which §5.3.4 cell this host error lands in on the **model** stage today.
    /// See [`ironclaw_host_api::recoverability`] for the contract and the §11.7
    /// matrix this row belongs to.
    ///
    /// # Why this is stage-scoped
    ///
    /// `AgentLoopHostError` is the `Err` arm of *every* host port, and the same
    /// kind has materially different fates depending on which stage produced
    /// it. A single stage-agnostic `recoverability_class()` would be a lie: on the
    /// model stage `Unavailable` is retried twelve times, while on the
    /// capability stage it kills the run outright. So there are two
    /// classifiers, and
    /// [`Self::capability_stage_recoverability_class`] is the other one. (The epic's
    /// `LoopProgressEvent::FailureRecovered` already carries a `stage` field
    /// for the same reason.)
    ///
    /// Derived from the live path:
    /// `ironclaw_agent_loop::executor::model` handles `Cancelled` and
    /// gate-shaped `BudgetApprovalRequired` structurally, asks
    /// `executor::mapping::model_error_class` for the rest, and turns a `None`
    /// class into a terminal `HostUnavailableWithDiagnostics{Model}`. A
    /// `Some(class)` goes to `DefaultRecoveryStrategy::on_model_error`.
    ///
    /// The match is exhaustive with no `_` arm on purpose: a new variant fails
    /// to compile here until it is classified.
    pub const fn model_stage_recoverability_class(self) -> RecoverabilityClass {
        match self {
            // `on_model_error` -> `retry_or_abort` on the deep availability
            // budget (12 attempts with backoff).
            //
            // AUDIT: budget exhaustion aborts with no observation — the model
            // never learns the provider was down (epic item 2, "only 3 of 10
            // `ModelErrorClass` variants ever produce an observation").
            Self::Unavailable | Self::Internal => RecoverabilityClass::Retry,
            // -> `InvalidOutput` -> `retry_observe_or_abort`: silent repair
            // retries first, then one typed observation, then abort.
            Self::InvalidOutput => RecoverabilityClass::Retry,
            // -> `ContextOverflow` -> `retry_observe_or_abort` at iteration
            // scope with `ShrinkContext`, then one typed observation, then
            // abort. Classified by the best outcome the model can reach: the
            // shrink succeeding, which the model never sees.
            //
            // AUDIT: `BudgetExceeded` is overloaded (epic item 2) — real spend
            // budget exhaustion and context-size overflow both land here, so a
            // genuine budget kill burns two shrink retries and is reported as
            // the wrong category.
            Self::BudgetExceeded => RecoverabilityClass::Retry,
            // -> `StaleRequest` -> `retry_or_abort` at iteration scope; the
            // rebuild of surface + prompt bundle is the fix. Exhaustion aborts
            // with the precise `model_stale_request` category.
            Self::StaleSurface => RecoverabilityClass::Retry,
            // -> `ContentFiltered` -> `observe_once_or_abort`: the model's
            // first and only sight of it is the typed observation it can act on.
            Self::ContentFiltered => RecoverabilityClass::ModelVisible,
            // The executor short-circuits to `budget_approval_blocked_exit`.
            //
            // AUDIT: only when the error actually carries a `gate_ref`. Without
            // one it falls through to the unclassified path and becomes
            // terminal — the parked outcome is not structurally guaranteed.
            Self::BudgetApprovalRequired => RecoverabilityClass::Park,
            // Sanctioned terminal invariant.
            Self::Cancelled => RecoverabilityClass::Terminal,
            // KNOWN DEFECTS (nearai/ironclaw#6284 items 1 and 2). These are
            // `RecoveryOutcome::Abort` with a precise category — user-actionable
            // and honest, but the run still dies and the model never gets a turn.
            Self::Unauthorized | Self::CheckpointRejected | Self::TranscriptWriteFailed => {
                RecoverabilityClass::Terminal
            }
            // KNOWN DEFECTS: `model_error_class` returns `None`, so these reach
            // the runner as `HostUnavailableWithDiagnostics{Model}` and end the
            // run. `CredentialUnavailable` and `BudgetAccountingFailed` are
            // unclassified on purpose (the runner derives a precise category
            // from kind + reason_kind), but "precise category" is not
            // "recovered".
            Self::CredentialUnavailable
            | Self::ScopeMismatch
            | Self::InvalidInvocation
            | Self::Invalid
            | Self::PolicyDenied
            | Self::BudgetAccountingFailed => RecoverabilityClass::Terminal,
        }
    }

    /// Whether this kind's **model**-stage observation carries the non-empty
    /// remediation hint §11.7 requires — clause (c) of §5.3.4.
    ///
    /// `ContentFiltered` is the model stage's only model-visible row, and it is
    /// one of the few places in the system that already satisfies clause (c):
    /// `ModelErrorRecoveryObservation::content_filtered()` renders "provide a
    /// policy compliant alternative without reproducing blocked content" — what
    /// would make the operation succeed, not merely that it failed. Every other
    /// kind is retried, parked, or terminal, so the clause does not apply.
    ///
    /// There is deliberately no capability-stage counterpart: that stage has no
    /// model-visible row at all (see
    /// [`Self::capability_stage_recoverability_class`]), so a hint method there
    /// would be a constant. The vacuous case is asserted in the conformance
    /// test instead of encoded as API.
    pub const fn model_stage_remediation_hint(self) -> RemediationHint {
        match self {
            Self::ContentFiltered => RemediationHint::Substantive,
            Self::Unavailable
            | Self::Internal
            | Self::InvalidOutput
            | Self::BudgetExceeded
            | Self::StaleSurface
            | Self::BudgetApprovalRequired
            | Self::Cancelled
            | Self::Unauthorized
            | Self::CheckpointRejected
            | Self::TranscriptWriteFailed
            | Self::CredentialUnavailable
            | Self::ScopeMismatch
            | Self::InvalidInvocation
            | Self::Invalid
            | Self::PolicyDenied
            | Self::BudgetAccountingFailed => RemediationHint::NotApplicable,
        }
    }

    /// What the loop does with this host error on the **capability** stage
    /// today. See [`Self::model_stage_recoverability_class`] for why the
    /// classification is stage-scoped.
    ///
    /// Every arm is [`RecoverabilityClass::Terminal`], and that is the single largest
    /// remaining bork surface in nearai/ironclaw#6284 (item 1, "the capability
    /// path was never touched"):
    /// `ironclaw_agent_loop::executor::mapping::capability_host_error` maps
    /// `Cancelled` to `AgentLoopExecutorError::Cancelled` and **every other
    /// kind** to `HostUnavailable{Capability}` — `Unauthorized`,
    /// `ScopeMismatch`, `InvalidInvocation`, `CredentialUnavailable`, and
    /// `BudgetAccountingFailed` all kill the run invisibly. Roughly fifty
    /// `Err(AgentLoopHostError::new(..))` sites in
    /// `ironclaw_loop_host::capability_port` feed it.
    ///
    /// This records today's behavior. Later PRs in the epic flip these rows;
    /// the table diff is the review evidence.
    ///
    /// Written as an exhaustive match rather than a constant so that a new
    /// variant still fails to compile until somebody looks at this path.
    pub const fn capability_stage_recoverability_class(self) -> RecoverabilityClass {
        match self {
            // Sanctioned terminal invariant: mapped to
            // `AgentLoopExecutorError::Cancelled`.
            Self::Cancelled => RecoverabilityClass::Terminal,
            // KNOWN DEFECTS: all mapped to `HostUnavailable{Capability}`.
            //
            // AUDIT: `BudgetApprovalRequired` is `Parked` on the model stage but
            // terminal here — the capability stage has no gate short-circuit for
            // it at all.
            Self::Unauthorized
            | Self::CredentialUnavailable
            | Self::ScopeMismatch
            | Self::StaleSurface
            | Self::InvalidInvocation
            | Self::Invalid
            | Self::InvalidOutput
            | Self::ContentFiltered
            | Self::PolicyDenied
            | Self::BudgetExceeded
            | Self::BudgetApprovalRequired
            | Self::BudgetAccountingFailed
            | Self::Unavailable
            | Self::CheckpointRejected
            | Self::TranscriptWriteFailed
            | Self::Internal => RecoverabilityClass::Terminal,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::CredentialUnavailable => "credential_unavailable",
            Self::ScopeMismatch => "scope_mismatch",
            Self::StaleSurface => "stale_surface",
            Self::InvalidInvocation => "invalid_invocation",
            Self::Invalid => "invalid",
            Self::InvalidOutput => "invalid_output",
            Self::ContentFiltered => "content_filtered",
            Self::PolicyDenied => "policy_denied",
            Self::BudgetExceeded => "budget_exceeded",
            Self::BudgetApprovalRequired => "budget_approval_required",
            Self::BudgetAccountingFailed => "budget_accounting_failed",
            Self::Unavailable => "unavailable",
            Self::Cancelled => "cancelled",
            Self::CheckpointRejected => "checkpoint_rejected",
            Self::TranscriptWriteFailed => "transcript_write_failed",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoopHostErrorReasonKind {
    ModelCreditsExhausted,
}

impl AgentLoopHostErrorReasonKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ModelCreditsExhausted => "model_credits_exhausted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("agent loop host {kind:?}: {safe_summary}")]
pub struct AgentLoopHostError {
    pub kind: AgentLoopHostErrorKind,
    pub safe_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_kind: Option<AgentLoopHostErrorReasonKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_ref: Option<LoopGateRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic_ref: Option<LoopDiagnosticRef>,
    /// Model-visible, secret-scrubbed raw cause. Unlike `safe_summary`, this
    /// carries the original error text (paths, codes, schema refs) so the model
    /// can retry or explain. Secret VALUES are redacted by the producer via
    /// [`sanitize_model_visible_text`](super::sanitize_model_visible_text); the
    /// word/delimiter ban is NOT applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl AgentLoopHostError {
    pub fn new(kind: AgentLoopHostErrorKind, safe_summary: impl Into<String>) -> Self {
        Self {
            kind,
            safe_summary: safe_summary.into(),
            reason_kind: None,
            gate_ref: None,
            diagnostic_ref: None,
            detail: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_reason_kind(mut self, reason_kind: AgentLoopHostErrorReasonKind) -> Self {
        self.reason_kind = Some(reason_kind);
        self
    }

    pub fn with_gate_ref(mut self, gate_ref: LoopGateRef) -> Self {
        self.gate_ref = Some(gate_ref);
        self
    }

    pub fn with_diagnostic_ref(mut self, diagnostic_ref: LoopDiagnosticRef) -> Self {
        self.diagnostic_ref = Some(diagnostic_ref);
        self
    }
}

pub(crate) fn unsupported_host_method(method: &'static str) -> AgentLoopHostError {
    AgentLoopHostError::new(
        AgentLoopHostErrorKind::Unavailable,
        format!("agent loop host method {method} is unavailable"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_loop_host_error_carries_optional_detail() {
        let path = "missing input_schema_ref at /system/extensions/google-calendar/list_calendars.input.v1.json";
        let error = AgentLoopHostError::new(
            AgentLoopHostErrorKind::InvalidInvocation,
            "host runtime rejected capability request",
        )
        .with_detail(path);
        assert_eq!(error.detail.as_deref(), Some(path));

        let plain = AgentLoopHostError::new(AgentLoopHostErrorKind::Internal, "boom");
        assert_eq!(plain.detail, None);
    }

    /// Every `AgentLoopHostErrorKind` variant. The exhaustive `match` in each
    /// conformance test below is what fails to compile when a variant is
    /// added; this array is what makes the new variant actually get asserted.
    const ALL_KINDS: [AgentLoopHostErrorKind; 17] = [
        AgentLoopHostErrorKind::Unauthorized,
        AgentLoopHostErrorKind::CredentialUnavailable,
        AgentLoopHostErrorKind::ScopeMismatch,
        AgentLoopHostErrorKind::StaleSurface,
        AgentLoopHostErrorKind::InvalidInvocation,
        AgentLoopHostErrorKind::Invalid,
        AgentLoopHostErrorKind::InvalidOutput,
        AgentLoopHostErrorKind::ContentFiltered,
        AgentLoopHostErrorKind::PolicyDenied,
        AgentLoopHostErrorKind::BudgetExceeded,
        AgentLoopHostErrorKind::BudgetApprovalRequired,
        AgentLoopHostErrorKind::BudgetAccountingFailed,
        AgentLoopHostErrorKind::Unavailable,
        AgentLoopHostErrorKind::Cancelled,
        AgentLoopHostErrorKind::CheckpointRejected,
        AgentLoopHostErrorKind::TranscriptWriteFailed,
        AgentLoopHostErrorKind::Internal,
    ];

    /// §11.7 recoverability-matrix row for the **model** stage.
    ///
    /// This test is currently the only consumer of
    /// `model_stage_recoverability_class` — that is expected and deliberate. The
    /// epic's item-7 gate is a *compile-forced* classification: the value is
    /// that a new variant cannot land without a recorded recoverability class.
    /// `LoopProgressEvent::FailureRecovered` (nearai/ironclaw#6284 item 7, a
    /// later PR) becomes the production consumer. **Do not delete this test as
    /// "dead code" before then.**
    #[test]
    fn every_host_error_kind_has_a_recorded_model_stage_recoverability_class() {
        use AgentLoopHostErrorKind as K;

        const fn expected(kind: AgentLoopHostErrorKind) -> RecoverabilityClass {
            match kind {
                // `model_error_class` -> retried by the recovery strategy.
                K::Unavailable | K::Internal => RecoverabilityClass::Retry,
                K::InvalidOutput | K::BudgetExceeded | K::StaleSurface => {
                    RecoverabilityClass::Retry
                }
                // One typed observation the model acts on.
                K::ContentFiltered => RecoverabilityClass::ModelVisible,
                // Gate: the turn parks awaiting the budget approval.
                K::BudgetApprovalRequired => RecoverabilityClass::Park,
                // `model_error_class` -> `Some(precise terminal class)` -> Abort.
                K::Unauthorized | K::CheckpointRejected | K::TranscriptWriteFailed => {
                    RecoverabilityClass::Terminal
                }
                // `model_error_class` -> `None` -> `HostUnavailableWithDiagnostics`.
                K::CredentialUnavailable
                | K::ScopeMismatch
                | K::InvalidInvocation
                | K::Invalid
                | K::PolicyDenied
                | K::BudgetAccountingFailed => RecoverabilityClass::Terminal,
                // Sanctioned terminal invariant.
                K::Cancelled => RecoverabilityClass::Terminal,
            }
        }

        for kind in ALL_KINDS {
            assert_eq!(
                kind.model_stage_recoverability_class(),
                expected(kind),
                "model-stage recoverability class for {kind:?} changed"
            );
            // Clause (c) applies to exactly the model-visible rows.
            assert_eq!(
                kind.model_stage_remediation_hint() == RemediationHint::NotApplicable,
                kind.model_stage_recoverability_class() != RecoverabilityClass::ModelVisible,
                "remediation-hint applicability disagrees with the model-stage \
                 class for {kind:?}"
            );
        }
    }

    /// §11.7 recoverability-matrix row for the **capability** stage. See
    /// [`every_host_error_kind_has_a_recorded_model_stage_recoverability_class`] for
    /// why the classifier has no production consumer yet.
    #[test]
    fn every_host_error_kind_has_a_recorded_capability_stage_recoverability_class() {
        for kind in ALL_KINDS {
            // `capability_host_error` has exactly two outcomes and both end the
            // run: `Cancelled` -> `AgentLoopExecutorError::Cancelled`, and every
            // other kind -> `HostUnavailable{Capability}`.
            assert_eq!(
                kind.capability_stage_recoverability_class(),
                RecoverabilityClass::Terminal,
                "capability-stage recoverability class for {kind:?} changed — this PR \
                 records today's behavior; flipping a row is a later PR's job"
            );
        }
    }

    /// Ratchet pin: how many `AgentLoopHostErrorKind` variants end the run, and
    /// how many model-visible ones ship the model no remediation.
    ///
    /// The terminal rows are the epic's remaining defects
    /// (nearai/ironclaw#6284 items 1 and 2). Only `Cancelled` is a sanctioned
    /// terminal invariant; every other terminal row is a known defect, not an
    /// endorsement. The capability-stage 17-of-17 figure is the epic's item-1
    /// measurement and the largest remaining bork surface in the system.
    ///
    /// The hint axis is clean on both stages, for opposite reasons: the model
    /// stage's one model-visible row already carries an actionable instruction,
    /// and the capability stage has no model-visible row to owe one.
    /// **Every number here may only go DOWN.**
    #[test]
    fn host_error_kind_terminal_and_hintless_counts_only_ratchet_down() {
        let model_terminal = ALL_KINDS
            .iter()
            .filter(|kind| kind.model_stage_recoverability_class() == RecoverabilityClass::Terminal)
            .count();
        assert_eq!(
            model_terminal, 10,
            "expected 10 terminal model-stage kinds; this count may only decrease"
        );

        let capability_terminal = ALL_KINDS
            .iter()
            .filter(|kind| {
                kind.capability_stage_recoverability_class() == RecoverabilityClass::Terminal
            })
            .count();
        assert_eq!(
            capability_terminal,
            ALL_KINDS.len(),
            "the capability stage is entirely terminal today (epic item 1, \
             `capability_host_error`); this count may only decrease"
        );

        let model_hintless = ALL_KINDS
            .iter()
            .filter(|kind| {
                kind.model_stage_remediation_hint()
                    .is_hintless_model_visible()
            })
            .count();
        assert_eq!(
            model_hintless, 0,
            "the model stage's only model-visible kind (ContentFiltered) \
             already carries an actionable instruction; this count may only \
             decrease"
        );

        // The capability stage owes no hints because it has no model-visible
        // row at all — which is a defect, not a clean bill of health. It is
        // asserted here rather than as a classifier method so that the moment
        // item 1 makes a capability-stage kind model-visible, this assertion
        // is what forces the hint question to be answered.
        let capability_model_visible = ALL_KINDS
            .iter()
            .filter(|kind| {
                kind.capability_stage_recoverability_class() == RecoverabilityClass::ModelVisible
            })
            .count();
        assert_eq!(
            capability_model_visible, 0,
            "a capability-stage kind became model-visible: give it a \
             remediation-hint classification (§5.3.4 clause (c)) before \
             updating this count"
        );
    }
}
