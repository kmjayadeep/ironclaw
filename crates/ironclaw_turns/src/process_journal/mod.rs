//! Agent-turn adapter for the canonical process journal.
//!
//! The process journal vocabulary lives in `ironclaw_processes`. This module
//! only maps the existing turn-run records and runner envelopes into that
//! neutral process vocabulary while the turn store is still the backing
//! implementation.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

use ironclaw_host_api::{ProcessId, ResourceScope, SYSTEM_RESERVED_ID};
use ironclaw_processes::{
    ClaimProcessRequest, ClaimProcessesRequest, ClaimedProcess, FailProcessRequest,
    GetProcessSnapshotRequest, JournaledProcessSnapshot, ProcessCheckpointRef,
    ProcessJournalCursor, ProcessJournalEntry, ProcessJournalKind, ProcessJournalPage,
    ProcessJournalSource, ProcessKind, ProcessLeaseRequest, ProcessLeaseSnapshot,
    ProcessLeaseToken, ProcessLifecycleStatus, ProcessOutcome, ProcessSuspension,
    ProcessSuspensionKind, ProcessTransitionPort, ProcessWorkerId,
    RecoverExpiredProcessLeasesRequest, RecoverExpiredProcessLeasesResponse, SuspendProcessRequest,
};

use crate::{
    AcceptedMessageRef, BlockedReason, GateKind, GateResumeDisposition, LoopExitMapping,
    ProductTurnContext, ReplyTargetBindingRef, ResolvedRunProfile, RunProfileId, RunProfileVersion,
    SourceBindingRef, TurnActor, TurnCheckpointId, TurnError, TurnEventKind, TurnLifecycleEvent,
    TurnRunId, TurnRunRecord, TurnRunState, TurnRunnerId, TurnScope, TurnStatus,
    events::{
        EventCursor, TurnBlockedGateKind, TurnBlockedGateMetadata, TurnEventPage,
        TurnEventProjectionSource,
    },
    run_profile::{LoopModelRouteSnapshot, LoopModelUsage},
    runner::{
        ApplyValidatedLoopExitRequest, BlockRunRequest, CancelRunCompletionRequest,
        ClaimRunRequest, ClaimRunsRequest, ClaimedTurnRun, CompleteRunRequest, FailRunRequest,
        HeartbeatRequest, RecordModelRouteSnapshotRequest, RecordRunnerFailureRequest,
        RecoverExpiredLeasesRequest, RecoverExpiredLeasesResponse, RelinquishRunRequest,
        TurnRunTransitionPort, TurnRunnerOutcome,
    },
};

pub const AGENT_TURN_PROCESS_KIND: &str = "agent_turn";

mod store_adapter;
pub use store_adapter::{
    ProcessJournalStoreTurnAdapter, turn_error_from_process_journal_store_error,
};

#[derive(Clone)]
pub struct ProcessBackedTurnRunTransitionPort {
    transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
    journal: Arc<dyn ProcessJournalSource<Error = TurnError>>,
}

impl ProcessBackedTurnRunTransitionPort {
    pub fn new(
        transitions: Arc<dyn ProcessTransitionPort<Error = TurnError>>,
        journal: Arc<dyn ProcessJournalSource<Error = TurnError>>,
    ) -> Self {
        Self {
            transitions,
            journal,
        }
    }

    async fn process_state(&self, process_id: ProcessId) -> Result<TurnRunState, TurnError> {
        let snapshot = self
            .journal
            .get_process_snapshot(GetProcessSnapshotRequest {
                scope: ResourceScope::system(),
                process_id,
            })
            .await?;
        turn_run_state_from_process_snapshot(snapshot)
    }
}

#[async_trait]
impl TurnRunTransitionPort for ProcessBackedTurnRunTransitionPort {
    async fn claim_next_run(
        &self,
        request: ClaimRunRequest,
    ) -> Result<Option<ClaimedTurnRun>, TurnError> {
        let claimed = self
            .transitions
            .claim_next_process(ClaimProcessRequest {
                worker_id: ProcessWorkerId::from_trusted(request.runner_id.to_wire_string()),
                lease_token: ProcessLeaseToken::from_trusted(request.lease_token.to_wire_string()),
                scope_filter: request.scope_filter.map(|scope| scope.to_resource_scope()),
            })
            .await?;
        claimed.map(claimed_turn_run_from_process_claim).transpose()
    }

    async fn claim_next_runs(
        &self,
        request: ClaimRunsRequest,
    ) -> Result<Vec<ClaimedTurnRun>, TurnError> {
        let claimed = self
            .transitions
            .claim_next_processes(ClaimProcessesRequest {
                worker_id: ProcessWorkerId::from_trusted(request.runner_id.to_wire_string()),
                scope_filter: request.scope_filter.map(|scope| scope.to_resource_scope()),
                max_processes: request.max_runs,
            })
            .await?;
        claimed
            .into_iter()
            .map(claimed_turn_run_from_process_claim)
            .collect()
    }

    async fn heartbeat(&self, request: HeartbeatRequest) -> Result<EventCursor, TurnError> {
        let cursor = self
            .transitions
            .heartbeat_process(process_lease_request_from_turn(
                request.run_id,
                request.runner_id,
                request.lease_token,
            ))
            .await?;
        Ok(EventCursor(cursor.0))
    }

    async fn recover_expired_leases(
        &self,
        request: RecoverExpiredLeasesRequest,
    ) -> Result<RecoverExpiredLeasesResponse, TurnError> {
        let response = self
            .transitions
            .recover_expired_process_leases(process_recover_request_from_turn(request))
            .await?;
        Ok(RecoverExpiredLeasesResponse {
            recovered: response
                .recovered
                .into_iter()
                .map(turn_run_state_from_process_snapshot)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    async fn record_model_route_snapshot(
        &self,
        request: RecordModelRouteSnapshotRequest,
    ) -> Result<TurnRunState, TurnError> {
        self.process_state(process_id_from_turn_run_id(request.run_id))
            .await
    }

    async fn block_run(&self, request: BlockRunRequest) -> Result<TurnRunState, TurnError> {
        let snapshot = self
            .transitions
            .suspend_process(SuspendProcessRequest {
                process_id: process_id_from_turn_run_id(request.run_id),
                worker_id: ProcessWorkerId::from_trusted(request.runner_id.to_wire_string()),
                lease_token: ProcessLeaseToken::from_trusted(request.lease_token.to_wire_string()),
                checkpoint_ref: process_checkpoint_ref(request.checkpoint_id),
                suspension: process_suspension_from_blocked_reason(request.reason),
            })
            .await?;
        turn_run_state_from_process_snapshot(snapshot)
    }

    async fn complete_run(&self, request: CompleteRunRequest) -> Result<TurnRunState, TurnError> {
        let snapshot = self
            .transitions
            .complete_process(process_lease_request_from_turn(
                request.run_id,
                request.runner_id,
                request.lease_token,
            ))
            .await?;
        turn_run_state_from_process_snapshot(snapshot)
    }

    async fn cancel_run(
        &self,
        request: CancelRunCompletionRequest,
    ) -> Result<TurnRunState, TurnError> {
        let snapshot = self
            .transitions
            .cancel_process(process_lease_request_from_turn(
                request.run_id,
                request.runner_id,
                request.lease_token,
            ))
            .await?;
        turn_run_state_from_process_snapshot(snapshot)
    }

    async fn fail_run(&self, request: FailRunRequest) -> Result<TurnRunState, TurnError> {
        let snapshot = self
            .transitions
            .fail_process(FailProcessRequest {
                process_id: process_id_from_turn_run_id(request.run_id),
                worker_id: ProcessWorkerId::from_trusted(request.runner_id.to_wire_string()),
                lease_token: ProcessLeaseToken::from_trusted(request.lease_token.to_wire_string()),
                failure: request.failure,
            })
            .await?;
        turn_run_state_from_process_snapshot(snapshot)
    }

    async fn record_runner_failure(
        &self,
        request: RecordRunnerFailureRequest,
    ) -> Result<TurnRunState, TurnError> {
        self.fail_run(FailRunRequest {
            run_id: request.run_id,
            runner_id: request.runner_id,
            lease_token: request.lease_token,
            failure: request.failure,
        })
        .await
    }

    async fn relinquish_run(
        &self,
        request: RelinquishRunRequest,
    ) -> Result<TurnRunState, TurnError> {
        let snapshot = self
            .transitions
            .relinquish_process(process_lease_request_from_turn(
                request.run_id,
                request.runner_id,
                request.lease_token,
            ))
            .await?;
        turn_run_state_from_process_snapshot(snapshot)
    }

    async fn apply_validated_loop_exit(
        &self,
        request: ApplyValidatedLoopExitRequest,
    ) -> Result<TurnRunState, TurnError> {
        match request.mapping {
            LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Completed) => {
                self.complete_run(CompleteRunRequest {
                    run_id: request.run_id,
                    runner_id: request.runner_id,
                    lease_token: request.lease_token,
                })
                .await
            }
            LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Cancelled) => {
                self.cancel_run(CancelRunCompletionRequest {
                    run_id: request.run_id,
                    runner_id: request.runner_id,
                    lease_token: request.lease_token,
                })
                .await
            }
            LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Blocked {
                checkpoint_id,
                reason,
                ..
            }) => {
                self.block_run(BlockRunRequest {
                    run_id: request.run_id,
                    runner_id: request.runner_id,
                    lease_token: request.lease_token,
                    checkpoint_id,
                    state_ref: crate::run_profile::LoopCheckpointStateRef::legacy_unknown(),
                    reason,
                })
                .await
            }
            LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Failed { failure })
            | LoopExitMapping::RecoveryRequired { failure } => {
                self.fail_run(FailRunRequest {
                    run_id: request.run_id,
                    runner_id: request.runner_id,
                    lease_token: request.lease_token,
                    failure,
                })
                .await
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTurnProcessMetadata {
    pub turn_id: crate::TurnId,
    pub accepted_message_ref: AcceptedMessageRef,
    pub source_binding_ref: SourceBindingRef,
    pub reply_target_binding_ref: ReplyTargetBindingRef,
    pub resolved_run_profile_id: RunProfileId,
    pub resolved_run_profile_version: RunProfileVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_model_route: Option<LoopModelRouteSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_usage: Option<LoopModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_context: Option<ProductTurnContext>,
    #[serde(
        rename = "auth_resume_disposition",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub resume_disposition: Option<GateResumeDisposition>,
}

impl AgentTurnProcessMetadata {
    fn from_record(record: &TurnRunRecord) -> Self {
        Self {
            turn_id: record.turn_id,
            accepted_message_ref: record.accepted_message_ref.clone(),
            source_binding_ref: record.source_binding_ref.clone(),
            reply_target_binding_ref: record.reply_target_binding_ref.clone(),
            resolved_run_profile_id: record.profile.id.clone(),
            resolved_run_profile_version: record.profile.version,
            resolved_model_route: record.resolved_model_route.clone(),
            model_usage: record.model_usage,
            product_context: record.product_context.clone(),
            resume_disposition: record.resume_disposition.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTurnProcessStateMetadata {
    pub turn_id: crate::TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<TurnActor>,
    pub accepted_message_ref: AcceptedMessageRef,
    pub source_binding_ref: SourceBindingRef,
    pub reply_target_binding_ref: ReplyTargetBindingRef,
    pub resolved_run_profile_id: RunProfileId,
    pub resolved_run_profile_version: RunProfileVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_run_profile: Option<ResolvedRunProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_model_route: Option<LoopModelRouteSnapshot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_usage: Option<LoopModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_context: Option<ProductTurnContext>,
    #[serde(
        rename = "auth_resume_disposition",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub resume_disposition: Option<GateResumeDisposition>,
}

impl AgentTurnProcessStateMetadata {
    fn from_state(state: &TurnRunState) -> Self {
        Self {
            turn_id: state.turn_id,
            actor: state.actor.clone(),
            accepted_message_ref: state.accepted_message_ref.clone(),
            source_binding_ref: state.source_binding_ref.clone(),
            reply_target_binding_ref: state.reply_target_binding_ref.clone(),
            resolved_run_profile_id: state.resolved_run_profile_id.clone(),
            resolved_run_profile_version: state.resolved_run_profile_version,
            resolved_run_profile: None,
            resolved_model_route: state.resolved_model_route.clone(),
            model_usage: state.model_usage,
            product_context: state.product_context.clone(),
            resume_disposition: state.resume_disposition.clone(),
        }
    }

    fn from_claimed(claimed: &ClaimedTurnRun) -> Self {
        Self {
            resolved_run_profile: Some(claimed.resolved_run_profile.clone()),
            ..Self::from_state(&claimed.state)
        }
    }
}

pub trait TurnRunProcessExt {
    fn to_process_snapshot(&self) -> JournaledProcessSnapshot;
}

impl TurnRunProcessExt for TurnRunRecord {
    fn to_process_snapshot(&self) -> JournaledProcessSnapshot {
        JournaledProcessSnapshot {
            process_id: process_id_from_turn_run_id(self.run_id),
            process_kind: ProcessKind::AgentTurn,
            scope: self.scope.to_resource_scope(),
            status: process_status_from_turn_status(self.status),
            suspension: process_suspension_from_record(self),
            checkpoint_ref: self.checkpoint_id.map(process_checkpoint_ref),
            failure: self.failure.clone(),
            journal_cursor: ProcessJournalCursor(self.event_cursor.0),
            lease: process_lease_from_record(self),
            created_at: self.received_at,
            owner_user_id: self.scope.explicit_owner_user_id().cloned(),
            parent_process_id: self.parent_run_id.map(process_id_from_turn_run_id),
            root_process_id: self.spawn_tree_root_run_id.map(process_id_from_turn_run_id),
            metadata: json!({ "agent_turn": AgentTurnProcessMetadata::from_record(self) }),
        }
    }
}

pub trait TurnRunStateProcessExt {
    fn to_process_state_snapshot(&self) -> JournaledProcessSnapshot;
}

impl TurnRunStateProcessExt for TurnRunState {
    fn to_process_state_snapshot(&self) -> JournaledProcessSnapshot {
        JournaledProcessSnapshot {
            process_id: process_id_from_turn_run_id(self.run_id),
            process_kind: ProcessKind::AgentTurn,
            scope: self.scope.to_resource_scope(),
            status: process_status_from_turn_status(self.status),
            suspension: process_suspension_from_state(self),
            checkpoint_ref: self.checkpoint_id.map(process_checkpoint_ref),
            failure: self.failure.clone(),
            journal_cursor: ProcessJournalCursor(self.event_cursor.0),
            lease: None,
            created_at: self.received_at,
            owner_user_id: self
                .scope
                .explicit_owner_user_id()
                .cloned()
                .or_else(|| self.actor.as_ref().map(|actor| actor.user_id.clone())),
            parent_process_id: None,
            root_process_id: None,
            metadata: json!({ "agent_turn": AgentTurnProcessStateMetadata::from_state(self) }),
        }
    }
}

pub trait TurnLifecycleProcessExt {
    fn to_process_journal_entry(&self) -> ProcessJournalEntry;
}

impl TurnLifecycleProcessExt for TurnLifecycleEvent {
    fn to_process_journal_entry(&self) -> ProcessJournalEntry {
        ProcessJournalEntry {
            cursor: ProcessJournalCursor(self.cursor.0),
            process_id: process_id_from_turn_run_id(self.run_id),
            process_kind: ProcessKind::AgentTurn,
            scope: self.scope.to_resource_scope(),
            occurred_at: self.occurred_at,
            owner_user_id: self.owner_user_id.clone(),
            status: process_status_from_turn_status(self.status),
            kind: process_journal_kind_from_turn_event_kind(self.kind.clone()),
            suspension: process_suspension_from_event(self),
            sanitized_reason: self.sanitized_reason.clone(),
            retryable: self.retryable,
            detail: self.detail.clone(),
            metadata: Value::Null,
        }
    }
}

pub fn process_id_from_turn_run_id(run_id: TurnRunId) -> ProcessId {
    ProcessId::from_uuid(run_id.as_uuid())
}

fn process_checkpoint_ref(checkpoint_id: TurnCheckpointId) -> ProcessCheckpointRef {
    ProcessCheckpointRef::from_trusted(checkpoint_id.as_uuid().to_string())
}

fn process_lease_request_from_turn(
    run_id: TurnRunId,
    runner_id: TurnRunnerId,
    lease_token: crate::TurnLeaseToken,
) -> ProcessLeaseRequest {
    ProcessLeaseRequest {
        process_id: process_id_from_turn_run_id(run_id),
        worker_id: ProcessWorkerId::from_trusted(runner_id.to_wire_string()),
        lease_token: ProcessLeaseToken::from_trusted(lease_token.to_wire_string()),
    }
}

pub fn process_status_from_turn_status(status: TurnStatus) -> ProcessLifecycleStatus {
    match status {
        TurnStatus::Queued => ProcessLifecycleStatus::Queued,
        TurnStatus::Running => ProcessLifecycleStatus::Running,
        TurnStatus::BlockedApproval
        | TurnStatus::BlockedAuth
        | TurnStatus::BlockedResource
        | TurnStatus::BlockedDependentRun
        | TurnStatus::BlockedExternalTool => ProcessLifecycleStatus::Suspended,
        TurnStatus::CancelRequested => ProcessLifecycleStatus::CancelRequested,
        TurnStatus::Cancelled => ProcessLifecycleStatus::Cancelled,
        TurnStatus::Completed => ProcessLifecycleStatus::Completed,
        TurnStatus::Failed => ProcessLifecycleStatus::Failed,
        TurnStatus::RecoveryRequired => ProcessLifecycleStatus::RecoveryRequired,
    }
}

pub fn process_suspension_kind_from_gate_kind(kind: GateKind) -> ProcessSuspensionKind {
    match kind {
        GateKind::Approval => ProcessSuspensionKind::Approval,
        GateKind::Auth => ProcessSuspensionKind::Authorization,
        GateKind::Resource => ProcessSuspensionKind::Resource,
        GateKind::AwaitDependentRun => ProcessSuspensionKind::AwaitingChildProcess,
        GateKind::ExternalTool => ProcessSuspensionKind::ExternalTool,
    }
}

pub fn process_suspension_kind_from_turn_blocked_gate_kind(
    kind: TurnBlockedGateKind,
) -> ProcessSuspensionKind {
    match kind {
        TurnBlockedGateKind::Approval => ProcessSuspensionKind::Approval,
        TurnBlockedGateKind::Auth => ProcessSuspensionKind::Authorization,
        TurnBlockedGateKind::Resource => ProcessSuspensionKind::Resource,
        TurnBlockedGateKind::AwaitDependentRun => ProcessSuspensionKind::AwaitingChildProcess,
        TurnBlockedGateKind::ExternalTool => ProcessSuspensionKind::ExternalTool,
    }
}

pub fn process_journal_kind_from_turn_event_kind(kind: TurnEventKind) -> ProcessJournalKind {
    match kind {
        TurnEventKind::Submitted => ProcessJournalKind::Submitted,
        TurnEventKind::Resumed => ProcessJournalKind::Resumed,
        TurnEventKind::RunnerClaimed => ProcessJournalKind::Claimed,
        TurnEventKind::RunnerHeartbeat => ProcessJournalKind::Heartbeat,
        TurnEventKind::RecoveryRequired => ProcessJournalKind::RecoveryRequired,
        TurnEventKind::Blocked => ProcessJournalKind::Suspended,
        TurnEventKind::CancelRequested => ProcessJournalKind::CancelRequested,
        TurnEventKind::Cancelled => ProcessJournalKind::Cancelled,
        TurnEventKind::Completed => ProcessJournalKind::Completed,
        TurnEventKind::Failed => ProcessJournalKind::Failed,
    }
}

pub(crate) fn process_suspension_from_record(record: &TurnRunRecord) -> Option<ProcessSuspension> {
    let kind = GateKind::from_status(record.status).map(process_suspension_kind_from_gate_kind)?;
    Some(ProcessSuspension {
        kind,
        gate_ref: record.gate_ref.clone(),
        activity_id: record.blocked_activity_id,
        credential_requirements: record.credential_requirements.clone(),
        detail: None,
    })
}

fn process_suspension_from_state(state: &TurnRunState) -> Option<ProcessSuspension> {
    let kind = GateKind::from_status(state.status).map(process_suspension_kind_from_gate_kind)?;
    Some(ProcessSuspension {
        kind,
        gate_ref: state.gate_ref.clone(),
        activity_id: state.blocked_activity_id,
        credential_requirements: state.credential_requirements.clone(),
        detail: None,
    })
}

fn process_suspension_from_event(event: &TurnLifecycleEvent) -> Option<ProcessSuspension> {
    let gate = event.blocked_gate.as_ref()?;
    Some(ProcessSuspension {
        kind: process_suspension_kind_from_turn_blocked_gate_kind(gate.gate_kind),
        gate_ref: Some(gate.gate_ref.clone()),
        activity_id: gate.activity_id,
        credential_requirements: gate.credential_requirements.clone(),
        detail: None,
    })
}

fn process_lease_from_record(record: &TurnRunRecord) -> Option<ProcessLeaseSnapshot> {
    Some(ProcessLeaseSnapshot {
        worker_id: ProcessWorkerId::from_trusted(record.runner_id?.to_wire_string()),
        lease_token: ProcessLeaseToken::from_trusted(record.lease_token?.to_wire_string()),
        lease_expires_at: record.lease_expires_at,
        last_heartbeat_at: record.last_heartbeat_at,
        claim_count: record.claim_count,
    })
}

trait TurnUuidWire {
    fn to_wire_string(self) -> String;
}

impl TurnUuidWire for TurnRunnerId {
    fn to_wire_string(self) -> String {
        self.as_uuid().to_string()
    }
}

impl TurnUuidWire for crate::TurnLeaseToken {
    fn to_wire_string(self) -> String {
        self.as_uuid().to_string()
    }
}

fn turn_runner_id_from_worker(worker_id: &ProcessWorkerId) -> Result<TurnRunnerId, TurnError> {
    Uuid::parse_str(worker_id.as_str())
        .map(TurnRunnerId::from_uuid)
        .map_err(|error| TurnError::InvalidRequest {
            reason: format!("invalid process worker id: {error}"),
        })
}

fn turn_lease_token_from_process(
    lease_token: &ProcessLeaseToken,
) -> Result<crate::TurnLeaseToken, TurnError> {
    Uuid::parse_str(lease_token.as_str())
        .map(crate::TurnLeaseToken::from_uuid)
        .map_err(|error| TurnError::InvalidRequest {
            reason: format!("invalid process lease token: {error}"),
        })
}

fn turn_run_id_from_process_id(process_id: ProcessId) -> TurnRunId {
    TurnRunId::from_uuid(process_id.as_uuid())
}

fn turn_checkpoint_id_from_process_ref(
    checkpoint_ref: ProcessCheckpointRef,
) -> Result<TurnCheckpointId, TurnError> {
    Uuid::parse_str(checkpoint_ref.as_str())
        .map(TurnCheckpointId::from_uuid)
        .map_err(|error| TurnError::InvalidRequest {
            reason: format!("invalid process checkpoint ref: {error}"),
        })
}

fn agent_turn_metadata_from_process_snapshot(
    snapshot: &JournaledProcessSnapshot,
) -> Result<AgentTurnProcessStateMetadata, TurnError> {
    let Some(metadata) = snapshot.metadata.get("agent_turn") else {
        return Err(TurnError::InvalidRequest {
            reason: "agent-turn process snapshot missing agent_turn metadata".to_string(),
        });
    };
    serde_json::from_value(metadata.clone()).map_err(|error| TurnError::InvalidRequest {
        reason: format!("invalid agent-turn process metadata: {error}"),
    })
}

pub fn turn_run_state_from_process_snapshot(
    snapshot: JournaledProcessSnapshot,
) -> Result<TurnRunState, TurnError> {
    if snapshot.process_kind != ProcessKind::AgentTurn {
        return Err(TurnError::InvalidRequest {
            reason: "process snapshot is not an agent turn".to_string(),
        });
    }
    let metadata = agent_turn_metadata_from_process_snapshot(&snapshot)?;
    let status = turn_status_from_process_status(snapshot.status, snapshot.suspension.as_ref())?;
    Ok(TurnRunState {
        scope: turn_scope_from_process_scope(snapshot.scope)?,
        actor: metadata.actor,
        turn_id: metadata.turn_id,
        run_id: turn_run_id_from_process_id(snapshot.process_id),
        status,
        accepted_message_ref: metadata.accepted_message_ref,
        source_binding_ref: metadata.source_binding_ref,
        reply_target_binding_ref: metadata.reply_target_binding_ref,
        resolved_run_profile_id: metadata.resolved_run_profile_id,
        resolved_run_profile_version: metadata.resolved_run_profile_version,
        resolved_model_route: metadata.resolved_model_route,
        model_usage: metadata.model_usage,
        received_at: snapshot.created_at,
        checkpoint_id: snapshot
            .checkpoint_ref
            .map(turn_checkpoint_id_from_process_ref)
            .transpose()?,
        gate_ref: snapshot
            .suspension
            .as_ref()
            .and_then(|suspension| suspension.gate_ref.clone()),
        blocked_activity_id: snapshot
            .suspension
            .as_ref()
            .and_then(|suspension| suspension.activity_id),
        credential_requirements: snapshot
            .suspension
            .map(|suspension| suspension.credential_requirements)
            .unwrap_or_default(),
        failure: snapshot.failure,
        event_cursor: EventCursor(snapshot.journal_cursor.0),
        product_context: metadata.product_context,
        resume_disposition: metadata.resume_disposition,
    })
}

pub fn claimed_turn_run_from_process_claim(
    claimed: ClaimedProcess,
) -> Result<ClaimedTurnRun, TurnError> {
    let metadata = agent_turn_metadata_from_process_snapshot(&claimed.state)?;
    let Some(resolved_run_profile) = metadata.resolved_run_profile else {
        return Err(TurnError::InvalidRequest {
            reason: "claimed agent-turn process missing resolved_run_profile metadata".to_string(),
        });
    };
    let state = turn_run_state_from_process_snapshot(claimed.state)?;
    Ok(ClaimedTurnRun {
        state,
        resolved_run_profile,
        runner_id: turn_runner_id_from_worker(&claimed.worker_id)?,
        lease_token: turn_lease_token_from_process(&claimed.lease_token)?,
    })
}

impl From<&ClaimedTurnRun> for ClaimedProcess {
    fn from(claimed: &ClaimedTurnRun) -> Self {
        let mut state = claimed.state.to_process_state_snapshot();
        state.metadata =
            json!({ "agent_turn": AgentTurnProcessStateMetadata::from_claimed(claimed) });
        Self {
            state,
            worker_id: ProcessWorkerId::from_trusted(claimed.runner_id.to_wire_string()),
            lease_token: ProcessLeaseToken::from_trusted(claimed.lease_token.to_wire_string()),
        }
    }
}

#[derive(Clone)]
#[cfg(feature = "test-support")]
pub struct AgentTurnProcessTransitionAdapter {
    inner: Arc<dyn TurnRunTransitionPort>,
}

#[cfg(feature = "test-support")]
impl AgentTurnProcessTransitionAdapter {
    pub fn new(inner: Arc<dyn TurnRunTransitionPort>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Arc<dyn TurnRunTransitionPort> {
        &self.inner
    }
}

#[derive(Clone)]
pub struct TurnEventProjectionFromProcessJournal {
    source: Arc<dyn ProcessJournalSource<Error = TurnError>>,
}

impl TurnEventProjectionFromProcessJournal {
    pub fn new(source: Arc<dyn ProcessJournalSource<Error = TurnError>>) -> Self {
        Self { source }
    }
}

#[async_trait]
impl TurnEventProjectionSource for TurnEventProjectionFromProcessJournal {
    async fn read_turn_events_after(
        &self,
        scope: &TurnScope,
        owner_user_id: Option<&ironclaw_host_api::UserId>,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<TurnEventPage, TurnError> {
        let page = self
            .source
            .read_process_journal_after(
                &scope.to_resource_scope(),
                owner_user_id,
                after.map(|cursor| ProcessJournalCursor(cursor.0)),
                limit,
            )
            .await?;
        turn_event_page_from_process_journal(page)
    }

    async fn read_turn_event_log_after(
        &self,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<TurnEventPage, TurnError> {
        let page = self
            .source
            .read_process_journal_log_after(
                after.map(|cursor| ProcessJournalCursor(cursor.0)),
                limit,
            )
            .await?;
        turn_event_page_from_process_journal(page)
    }
}

#[cfg(feature = "test-support")]
fn turn_scope_filter_from_process(
    scope_filter: Option<ironclaw_host_api::ResourceScope>,
) -> Result<Option<TurnScope>, TurnError> {
    let Some(scope) = scope_filter else {
        return Ok(None);
    };
    turn_scope_from_process_scope(scope).map(Some)
}

fn turn_scope_from_process_scope(scope: ResourceScope) -> Result<TurnScope, TurnError> {
    let Some(thread_id) = scope.thread_id else {
        return Err(TurnError::InvalidRequest {
            reason: "process scope filter for agent turns requires thread_id".to_string(),
        });
    };
    if scope.user_id.as_str() == SYSTEM_RESERVED_ID {
        Ok(TurnScope::new(
            scope.tenant_id,
            scope.agent_id,
            scope.project_id,
            thread_id,
        ))
    } else {
        Ok(TurnScope::new_with_owner(
            scope.tenant_id,
            scope.agent_id,
            scope.project_id,
            thread_id,
            Some(scope.user_id),
        ))
    }
}

#[cfg(feature = "test-support")]
fn turn_recover_request_from_process(
    request: RecoverExpiredProcessLeasesRequest,
) -> Result<RecoverExpiredLeasesRequest, TurnError> {
    Ok(RecoverExpiredLeasesRequest {
        now: request.now,
        scope_filter: turn_scope_filter_from_process(request.scope_filter)?,
    })
}

#[async_trait]
#[cfg(feature = "test-support")]
impl ProcessTransitionPort for AgentTurnProcessTransitionAdapter {
    type Error = TurnError;

    async fn claim_next_process(
        &self,
        request: ClaimProcessRequest,
    ) -> Result<Option<ClaimedProcess>, Self::Error> {
        let claimed = self
            .inner
            .claim_next_run(ClaimRunRequest {
                runner_id: turn_runner_id_from_worker(&request.worker_id)?,
                lease_token: turn_lease_token_from_process(&request.lease_token)?,
                scope_filter: turn_scope_filter_from_process(request.scope_filter)?,
            })
            .await?;
        Ok(claimed.as_ref().map(ClaimedProcess::from))
    }

    async fn claim_next_processes(
        &self,
        request: ClaimProcessesRequest,
    ) -> Result<Vec<ClaimedProcess>, Self::Error> {
        let claimed = self
            .inner
            .claim_next_runs(ClaimRunsRequest {
                runner_id: turn_runner_id_from_worker(&request.worker_id)?,
                scope_filter: turn_scope_filter_from_process(request.scope_filter)?,
                max_runs: request.max_processes,
            })
            .await?;
        Ok(claimed.iter().map(ClaimedProcess::from).collect())
    }

    async fn heartbeat_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<ProcessJournalCursor, Self::Error> {
        let cursor = self
            .inner
            .heartbeat(HeartbeatRequest::try_from(request)?)
            .await?;
        Ok(ProcessJournalCursor(cursor.0))
    }

    async fn recover_expired_process_leases(
        &self,
        request: RecoverExpiredProcessLeasesRequest,
    ) -> Result<RecoverExpiredProcessLeasesResponse, Self::Error> {
        let response = self
            .inner
            .recover_expired_leases(turn_recover_request_from_process(request)?)
            .await?;
        Ok(process_recover_response_from_turn(&response))
    }

    async fn suspend_process(
        &self,
        request: SuspendProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let state = self
            .inner
            .block_run(BlockRunRequest::try_from(request)?)
            .await?;
        Ok(state.to_process_state_snapshot())
    }

    async fn complete_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let state = self
            .inner
            .complete_run(CompleteRunRequest::try_from(request)?)
            .await?;
        Ok(state.to_process_state_snapshot())
    }

    async fn cancel_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let state = self
            .inner
            .cancel_run(CancelRunCompletionRequest::try_from(request)?)
            .await?;
        Ok(state.to_process_state_snapshot())
    }

    async fn fail_process(
        &self,
        request: FailProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let state = self
            .inner
            .record_runner_failure(RecordRunnerFailureRequest::try_from(request)?)
            .await?;
        Ok(state.to_process_state_snapshot())
    }

    async fn relinquish_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let state = self
            .inner
            .relinquish_run(RelinquishRunRequest::try_from(request)?)
            .await?;
        Ok(state.to_process_state_snapshot())
    }
}

impl TryFrom<ProcessLeaseRequest> for HeartbeatRequest {
    type Error = TurnError;

    fn try_from(request: ProcessLeaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
        })
    }
}

impl TryFrom<ProcessLeaseRequest> for CompleteRunRequest {
    type Error = TurnError;

    fn try_from(request: ProcessLeaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
        })
    }
}

impl TryFrom<ProcessLeaseRequest> for CancelRunCompletionRequest {
    type Error = TurnError;

    fn try_from(request: ProcessLeaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
        })
    }
}

impl TryFrom<ProcessLeaseRequest> for RelinquishRunRequest {
    type Error = TurnError;

    fn try_from(request: ProcessLeaseRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
        })
    }
}

impl TryFrom<FailProcessRequest> for FailRunRequest {
    type Error = TurnError;

    fn try_from(request: FailProcessRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            failure: request.failure,
        })
    }
}

impl TryFrom<FailProcessRequest> for RecordRunnerFailureRequest {
    type Error = TurnError;

    fn try_from(request: FailProcessRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            failure: request.failure,
        })
    }
}

pub fn process_recover_request_from_turn(
    request: RecoverExpiredLeasesRequest,
) -> RecoverExpiredProcessLeasesRequest {
    RecoverExpiredProcessLeasesRequest {
        now: request.now,
        scope_filter: request.scope_filter.map(|scope| scope.to_resource_scope()),
    }
}

pub fn process_recover_response_from_turn(
    response: &RecoverExpiredLeasesResponse,
) -> RecoverExpiredProcessLeasesResponse {
    RecoverExpiredProcessLeasesResponse {
        recovered: response
            .recovered
            .iter()
            .map(TurnRunStateProcessExt::to_process_state_snapshot)
            .collect(),
    }
}

pub fn process_journal_page_from_turn(page: TurnEventPage) -> ProcessJournalPage {
    ProcessJournalPage {
        entries: page
            .entries
            .iter()
            .map(TurnLifecycleProcessExt::to_process_journal_entry)
            .collect(),
        next_cursor: ProcessJournalCursor(page.next_cursor.0),
        truncated: page.truncated,
        rebase_required: page
            .rebase_required
            .map(|cursor| ProcessJournalCursor(cursor.0)),
    }
}

pub fn turn_event_page_from_process_journal(
    page: ProcessJournalPage,
) -> Result<TurnEventPage, TurnError> {
    Ok(TurnEventPage {
        entries: page
            .entries
            .into_iter()
            .filter(|entry| entry.process_kind == ProcessKind::AgentTurn)
            .map(turn_lifecycle_event_from_process_journal_entry)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor: EventCursor(page.next_cursor.0),
        truncated: page.truncated,
        rebase_required: page.rebase_required.map(|cursor| EventCursor(cursor.0)),
    })
}

pub fn turn_lifecycle_event_from_process_journal_entry(
    entry: ProcessJournalEntry,
) -> Result<TurnLifecycleEvent, TurnError> {
    if entry.process_kind != ProcessKind::AgentTurn {
        return Err(TurnError::InvalidRequest {
            reason: "process journal entry is not an agent turn".to_string(),
        });
    }
    let status = turn_status_from_process_status(entry.status, entry.suspension.as_ref())?;
    let kind = turn_event_kind_from_process_journal_kind(entry.kind);
    let scope = turn_scope_from_process_scope(entry.scope)?;
    let blocked_gate = if kind == TurnEventKind::Blocked {
        entry
            .suspension
            .map(turn_blocked_gate_metadata_from_process_suspension)
            .transpose()?
    } else {
        None
    };
    Ok(TurnLifecycleEvent {
        cursor: EventCursor(entry.cursor.0),
        scope,
        occurred_at: entry.occurred_at,
        owner_user_id: entry.owner_user_id,
        run_id: turn_run_id_from_process_id(entry.process_id),
        status,
        kind,
        blocked_gate,
        sanitized_reason: entry.sanitized_reason,
        retryable: entry.retryable,
        detail: entry.detail,
    })
}

pub fn turn_status_from_process_status(
    status: ProcessLifecycleStatus,
    suspension: Option<&ProcessSuspension>,
) -> Result<TurnStatus, TurnError> {
    Ok(match status {
        ProcessLifecycleStatus::Queued => TurnStatus::Queued,
        ProcessLifecycleStatus::Running => TurnStatus::Running,
        ProcessLifecycleStatus::Suspended => {
            let Some(suspension) = suspension else {
                return Err(TurnError::InvalidRequest {
                    reason: "suspended agent-turn process requires suspension metadata".to_string(),
                });
            };
            turn_status_from_process_suspension_kind(suspension.kind)
        }
        ProcessLifecycleStatus::CancelRequested | ProcessLifecycleStatus::StopRequested => {
            TurnStatus::CancelRequested
        }
        ProcessLifecycleStatus::Stopped | ProcessLifecycleStatus::Completed => {
            TurnStatus::Completed
        }
        ProcessLifecycleStatus::Cancelled | ProcessLifecycleStatus::Killed => TurnStatus::Cancelled,
        ProcessLifecycleStatus::Failed => TurnStatus::Failed,
        ProcessLifecycleStatus::RecoveryRequired => TurnStatus::RecoveryRequired,
    })
}

fn turn_status_from_process_suspension_kind(kind: ProcessSuspensionKind) -> TurnStatus {
    match kind {
        ProcessSuspensionKind::Approval => TurnStatus::BlockedApproval,
        ProcessSuspensionKind::Authorization => TurnStatus::BlockedAuth,
        ProcessSuspensionKind::Resource => TurnStatus::BlockedResource,
        ProcessSuspensionKind::AwaitingChildProcess => TurnStatus::BlockedDependentRun,
        ProcessSuspensionKind::ExternalTool
        | ProcessSuspensionKind::ExternalProcess
        | ProcessSuspensionKind::ExtensionDefined => TurnStatus::BlockedExternalTool,
    }
}

fn turn_event_kind_from_process_journal_kind(kind: ProcessJournalKind) -> TurnEventKind {
    match kind {
        ProcessJournalKind::Submitted | ProcessJournalKind::Spawned => TurnEventKind::Submitted,
        ProcessJournalKind::Resumed => TurnEventKind::Resumed,
        ProcessJournalKind::Claimed => TurnEventKind::RunnerClaimed,
        ProcessJournalKind::Heartbeat => TurnEventKind::RunnerHeartbeat,
        ProcessJournalKind::RecoveryRequired => TurnEventKind::RecoveryRequired,
        ProcessJournalKind::Suspended => TurnEventKind::Blocked,
        ProcessJournalKind::CancelRequested | ProcessJournalKind::StopRequested => {
            TurnEventKind::CancelRequested
        }
        ProcessJournalKind::Cancelled
        | ProcessJournalKind::Stopped
        | ProcessJournalKind::Killed => TurnEventKind::Cancelled,
        ProcessJournalKind::Completed => TurnEventKind::Completed,
        ProcessJournalKind::Failed => TurnEventKind::Failed,
    }
}

fn turn_blocked_gate_metadata_from_process_suspension(
    suspension: ProcessSuspension,
) -> Result<TurnBlockedGateMetadata, TurnError> {
    let Some(gate_ref) = suspension.gate_ref else {
        return Err(TurnError::InvalidRequest {
            reason: "blocked agent-turn process requires gate_ref".to_string(),
        });
    };
    Ok(TurnBlockedGateMetadata {
        gate_ref,
        gate_kind: turn_blocked_gate_kind_from_process_suspension_kind(suspension.kind),
        activity_id: suspension.activity_id,
        credential_requirements: suspension.credential_requirements,
    })
}

fn turn_blocked_gate_kind_from_process_suspension_kind(
    kind: ProcessSuspensionKind,
) -> TurnBlockedGateKind {
    match kind {
        ProcessSuspensionKind::Approval => TurnBlockedGateKind::Approval,
        ProcessSuspensionKind::Authorization => TurnBlockedGateKind::Auth,
        ProcessSuspensionKind::Resource => TurnBlockedGateKind::Resource,
        ProcessSuspensionKind::AwaitingChildProcess => TurnBlockedGateKind::AwaitDependentRun,
        ProcessSuspensionKind::ExternalTool
        | ProcessSuspensionKind::ExternalProcess
        | ProcessSuspensionKind::ExtensionDefined => TurnBlockedGateKind::ExternalTool,
    }
}

pub fn process_outcome_from_turn_runner_outcome(outcome: TurnRunnerOutcome) -> ProcessOutcome {
    match outcome {
        TurnRunnerOutcome::Completed => ProcessOutcome::Completed,
        TurnRunnerOutcome::Cancelled => ProcessOutcome::Cancelled,
        TurnRunnerOutcome::Blocked {
            checkpoint_id,
            reason,
            ..
        } => ProcessOutcome::Suspended {
            checkpoint_ref: process_checkpoint_ref(checkpoint_id),
            suspension: process_suspension_from_blocked_reason(reason),
        },
        TurnRunnerOutcome::Failed { failure } => ProcessOutcome::Failed { failure },
    }
}

pub fn turn_runner_outcome_from_process_outcome(
    outcome: ProcessOutcome,
) -> Result<TurnRunnerOutcome, TurnError> {
    Ok(match outcome {
        ProcessOutcome::Completed | ProcessOutcome::Stopped => TurnRunnerOutcome::Completed,
        ProcessOutcome::Cancelled | ProcessOutcome::Killed { .. } => TurnRunnerOutcome::Cancelled,
        ProcessOutcome::Suspended { suspension, .. } => TurnRunnerOutcome::Blocked {
            checkpoint_id: TurnCheckpointId::new(),
            state_ref: crate::run_profile::LoopCheckpointStateRef::legacy_unknown(),
            reason: blocked_reason_from_process_suspension(suspension)?,
            blocked_activity_id: None,
        },
        ProcessOutcome::Failed { failure } => TurnRunnerOutcome::Failed { failure },
    })
}

fn process_suspension_from_blocked_reason(reason: BlockedReason) -> ProcessSuspension {
    let kind = process_suspension_kind_from_gate_kind(reason.gate_kind());
    ProcessSuspension {
        kind,
        gate_ref: Some(reason.gate_ref().clone()),
        activity_id: None,
        credential_requirements: reason.credential_requirements().to_vec(),
        detail: None,
    }
}

fn blocked_reason_from_process_suspension(
    suspension: ProcessSuspension,
) -> Result<BlockedReason, TurnError> {
    let Some(gate_ref) = suspension.gate_ref else {
        return Err(TurnError::InvalidRequest {
            reason: "process suspension cannot convert to turn blocked reason without gate_ref"
                .to_string(),
        });
    };
    Ok(match suspension.kind {
        ProcessSuspensionKind::Approval => BlockedReason::Approval { gate_ref },
        ProcessSuspensionKind::Authorization => BlockedReason::Auth {
            gate_ref,
            credential_requirements: suspension.credential_requirements,
        },
        ProcessSuspensionKind::Resource => BlockedReason::Resource { gate_ref },
        ProcessSuspensionKind::AwaitingChildProcess => {
            BlockedReason::AwaitDependentRun { gate_ref }
        }
        ProcessSuspensionKind::ExternalTool
        | ProcessSuspensionKind::ExternalProcess
        | ProcessSuspensionKind::ExtensionDefined => BlockedReason::ExternalTool { gate_ref },
    })
}

impl TryFrom<SuspendProcessRequest> for BlockRunRequest {
    type Error = TurnError;

    fn try_from(request: SuspendProcessRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            checkpoint_id: TurnCheckpointId::new(),
            state_ref: crate::run_profile::LoopCheckpointStateRef::legacy_unknown(),
            reason: blocked_reason_from_process_suspension(request.suspension)?,
        })
    }
}

#[cfg(test)]
mod tests;
