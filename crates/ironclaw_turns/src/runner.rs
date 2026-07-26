use serde::{Deserialize, Serialize};

use crate::{
    BlockedReason, CapabilityActivityId, LoopExitMapping, ResolvedRunProfile, SanitizedFailure,
    TurnCheckpointId, TurnLeaseToken, TurnRunId, TurnRunState, TurnRunnerId, TurnScope,
    TurnTimestamp,
    run_profile::{LoopCheckpointStateRef, LoopModelRouteSnapshot},
};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRunRequest {
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
    pub scope_filter: Option<TurnScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimRunsRequest {
    pub runner_id: TurnRunnerId,
    pub scope_filter: Option<TurnScope>,
    pub max_runs: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimedTurnRun {
    pub state: TurnRunState,
    pub resolved_run_profile: ResolvedRunProfile,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverExpiredLeasesRequest {
    pub now: TurnTimestamp,
    pub scope_filter: Option<TurnScope>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverExpiredLeasesResponse {
    pub recovered: Vec<TurnRunState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordModelRouteSnapshotRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
    pub snapshot: LoopModelRouteSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRunRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
    pub checkpoint_id: TurnCheckpointId,
    pub state_ref: LoopCheckpointStateRef,
    pub reason: BlockedReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteRunRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailRunRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
    pub failure: SanitizedFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRunCompletionRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordRunnerFailureRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
    pub failure: SanitizedFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelinquishRunRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyValidatedLoopExitRequest {
    pub run_id: TurnRunId,
    pub runner_id: TurnRunnerId,
    pub lease_token: TurnLeaseToken,
    pub mapping: LoopExitMapping,
    /// Cumulative provider-reported token usage the loop accumulated for this
    /// run, carried from the `LoopExit` so a terminal transition can persist it
    /// on the run record. `None` when the exit reported no usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_usage: Option<crate::run_profile::LoopModelUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnRunnerOutcome {
    Completed,
    Cancelled,
    Blocked {
        checkpoint_id: TurnCheckpointId,
        state_ref: LoopCheckpointStateRef,
        reason: BlockedReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blocked_activity_id: Option<CapabilityActivityId>,
    },
    Failed {
        failure: SanitizedFailure,
    },
}
