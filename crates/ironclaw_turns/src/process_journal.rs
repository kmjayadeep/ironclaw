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

use ironclaw_host_api::ProcessId;
use ironclaw_processes::{
    ClaimProcessRequest, ClaimProcessesRequest, ClaimedProcess, FailProcessRequest,
    JournaledProcessSnapshot, ProcessCheckpointRef, ProcessJournalCursor, ProcessJournalEntry,
    ProcessJournalKind, ProcessKind, ProcessLeaseRequest, ProcessLeaseSnapshot, ProcessLeaseToken,
    ProcessLifecycleStatus, ProcessOutcome, ProcessSuspension, ProcessSuspensionKind,
    ProcessTransitionPort, ProcessWorkerId, RecoverExpiredProcessLeasesRequest,
    RecoverExpiredProcessLeasesResponse, SuspendProcessRequest,
};

use crate::{
    AcceptedMessageRef, BlockedReason, GateKind, GateResumeDisposition, LoopExitMapping,
    ProductTurnContext, ReplyTargetBindingRef, ResolvedRunProfile, RunProfileId, RunProfileVersion,
    SourceBindingRef, TurnCheckpointId, TurnError, TurnEventKind, TurnLifecycleEvent, TurnRunId,
    TurnRunRecord, TurnRunState, TurnRunnerId, TurnScope, TurnStatus,
    events::TurnBlockedGateKind,
    run_profile::{LoopModelRouteSnapshot, LoopModelUsage},
    runner::{
        ApplyValidatedLoopExitRequest, BlockRunRequest, CancelRunCompletionRequest,
        ClaimRunRequest, ClaimRunsRequest, ClaimedTurnRun, CompleteRunRequest, FailRunRequest,
        HeartbeatRequest, RecordModelRouteSnapshotRequest, RecordRunnerFailureRequest,
        RecoverExpiredLeasesRequest, RecoverExpiredLeasesResponse, RelinquishRunRequest,
        TurnRunTransitionPort, TurnRunnerOutcome,
    },
};

pub type KernelProcessId = ProcessId;
pub type KernelProcessJournalCursor = ProcessJournalCursor;
pub type KernelProcessCheckpointRef = ProcessCheckpointRef;
pub type KernelProcessWorkerId = ProcessWorkerId;
pub type KernelProcessLeaseToken = ProcessLeaseToken;
pub type KernelProcessKind = ProcessKind;
pub type KernelProcessStatus = ProcessLifecycleStatus;
pub type KernelProcessSuspensionKind = ProcessSuspensionKind;
pub type KernelProcessSuspension = ProcessSuspension;
pub type KernelProcessJournalKind = ProcessJournalKind;
pub type KernelProcessLeaseSnapshot = ProcessLeaseSnapshot;
pub type KernelProcessSnapshot = JournaledProcessSnapshot;
pub type KernelProcessStateSnapshot = JournaledProcessSnapshot;
pub type KernelProcessJournalEntry = ProcessJournalEntry;
pub type KernelProcessLeaseRequest = ProcessLeaseRequest;
pub type KernelSuspendProcessRequest = SuspendProcessRequest;
pub type KernelRecoverExpiredLeasesRequest = RecoverExpiredProcessLeasesRequest;
pub type KernelRecoverExpiredLeasesResponse = RecoverExpiredProcessLeasesResponse;
pub type KernelProcessOutcome = ProcessOutcome;
pub type KernelClaimProcessRequest = ClaimProcessRequest;
pub type KernelClaimProcessesRequest = ClaimProcessesRequest;

pub const AGENT_TURN_PROCESS_KIND: &str = "agent_turn";

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

impl AgentTurnProcessStateMetadata {
    fn from_state(state: &TurnRunState) -> Self {
        Self {
            turn_id: state.turn_id,
            accepted_message_ref: state.accepted_message_ref.clone(),
            source_binding_ref: state.source_binding_ref.clone(),
            reply_target_binding_ref: state.reply_target_binding_ref.clone(),
            resolved_run_profile_id: state.resolved_run_profile_id.clone(),
            resolved_run_profile_version: state.resolved_run_profile_version,
            resolved_model_route: state.resolved_model_route.clone(),
            model_usage: state.model_usage,
            product_context: state.product_context.clone(),
            resume_disposition: state.resume_disposition.clone(),
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

fn process_suspension_from_record(record: &TurnRunRecord) -> Option<ProcessSuspension> {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KernelClaimedProcess {
    pub state: JournaledProcessSnapshot,
    pub resolved_run_profile: ResolvedRunProfile,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
}

impl From<&ClaimedTurnRun> for KernelClaimedProcess {
    fn from(claimed: &ClaimedTurnRun) -> Self {
        Self {
            state: claimed.state.to_process_state_snapshot(),
            resolved_run_profile: claimed.resolved_run_profile.clone(),
            worker_id: ProcessWorkerId::from_trusted(claimed.runner_id.to_wire_string()),
            lease_token: ProcessLeaseToken::from_trusted(claimed.lease_token.to_wire_string()),
        }
    }
}

impl From<&ClaimedTurnRun> for ClaimedProcess {
    fn from(claimed: &ClaimedTurnRun) -> Self {
        Self {
            state: claimed.state.to_process_state_snapshot(),
            worker_id: ProcessWorkerId::from_trusted(claimed.runner_id.to_wire_string()),
            lease_token: ProcessLeaseToken::from_trusted(claimed.lease_token.to_wire_string()),
        }
    }
}

#[derive(Clone)]
pub struct AgentTurnProcessTransitionAdapter {
    inner: Arc<dyn TurnRunTransitionPort>,
}

impl AgentTurnProcessTransitionAdapter {
    pub fn new(inner: Arc<dyn TurnRunTransitionPort>) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &Arc<dyn TurnRunTransitionPort> {
        &self.inner
    }
}

fn turn_scope_filter_from_process(
    scope_filter: Option<ironclaw_host_api::ResourceScope>,
) -> Result<Option<TurnScope>, TurnError> {
    let Some(scope) = scope_filter else {
        return Ok(None);
    };
    let Some(thread_id) = scope.thread_id else {
        return Err(TurnError::InvalidRequest {
            reason: "process scope filter for agent turns requires thread_id".to_string(),
        });
    };
    Ok(Some(TurnScope::new_with_owner(
        scope.tenant_id,
        scope.agent_id,
        scope.project_id,
        thread_id,
        Some(scope.user_id),
    )))
}

fn turn_recover_request_from_process(
    request: RecoverExpiredProcessLeasesRequest,
) -> Result<RecoverExpiredLeasesRequest, TurnError> {
    Ok(RecoverExpiredLeasesRequest {
        now: request.now,
        scope_filter: turn_scope_filter_from_process(request.scope_filter)?,
    })
}

#[async_trait]
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
            .fail_run(FailRunRequest::try_from(KernelFailProcessRequest {
                process_id: request.process_id,
                worker_id: request.worker_id,
                lease_token: request.lease_token,
                failure: request.failure,
            })?)
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelFailProcessRequest {
    pub process_id: ProcessId,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
    pub failure: crate::SanitizedFailure,
}

impl TryFrom<KernelFailProcessRequest> for FailRunRequest {
    type Error = TurnError;

    fn try_from(request: KernelFailProcessRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            failure: request.failure,
        })
    }
}

impl TryFrom<KernelFailProcessRequest> for RecordRunnerFailureRequest {
    type Error = TurnError;

    fn try_from(request: KernelFailProcessRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            failure: request.failure,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelRecordModelRouteRequest {
    pub process_id: ProcessId,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
    pub snapshot: LoopModelRouteSnapshot,
}

impl TryFrom<KernelRecordModelRouteRequest> for RecordModelRouteSnapshotRequest {
    type Error = TurnError;

    fn try_from(request: KernelRecordModelRouteRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            snapshot: request.snapshot,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KernelApplyValidatedExitRequest {
    pub process_id: ProcessId,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
    pub mapping: LoopExitMapping,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_usage: Option<LoopModelUsage>,
}

impl TryFrom<KernelApplyValidatedExitRequest> for ApplyValidatedLoopExitRequest {
    type Error = TurnError;

    fn try_from(request: KernelApplyValidatedExitRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            run_id: turn_run_id_from_process_id(request.process_id),
            runner_id: turn_runner_id_from_worker(&request.worker_id)?,
            lease_token: turn_lease_token_from_process(&request.lease_token)?,
            mapping: request.mapping,
            model_usage: request.model_usage,
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
mod tests {
    use chrono::Utc;
    use ironclaw_host_api::{AgentId, ProjectId, TenantId, ThreadId, UserId};
    use std::sync::Arc;

    use super::*;
    use crate::{
        AcceptedMessageRef, AdmissionRejection, CapabilityActivityId, EventCursor, GateRef,
        IdempotencyKey, InMemoryRunProfileResolver, ReplyTargetBindingRef, RunProfileRequest,
        RunProfileVersion, SourceBindingRef, SubmitTurnRequest, SubmitTurnResponse, TurnActor,
        TurnAdmissionPolicy, TurnId, TurnRunProfile, TurnScope, TurnStateStore,
    };

    struct AllowAllAdmissionPolicy;

    impl TurnAdmissionPolicy for AllowAllAdmissionPolicy {
        fn check_submit(&self, _request: &SubmitTurnRequest) -> Result<(), AdmissionRejection> {
            Ok(())
        }
    }

    fn scope() -> TurnScope {
        TurnScope::new(
            TenantId::new("tenant-process-journal").expect("tenant"),
            Some(AgentId::new("agent-process-journal").expect("agent")),
            Some(ProjectId::new("project-process-journal").expect("project")),
            ThreadId::new("thread-process-journal").expect("thread"),
        )
    }

    fn profile() -> TurnRunProfile {
        serde_json::from_value(serde_json::json!({
            "id": "default",
            "version": 1,
            "allow_steering": false,
            "auto_queue_followups": false,
        }))
        .expect("profile")
    }

    fn record_with_status(status: TurnStatus) -> TurnRunRecord {
        TurnRunRecord {
            run_id: TurnRunId::new(),
            turn_id: TurnId::new(),
            scope: scope(),
            accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
                .expect("accepted"),
            source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
            reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
                .expect("reply"),
            status,
            profile: profile(),
            resolved_model_route: None,
            model_usage: None,
            checkpoint_id: None,
            gate_ref: GateKind::from_status(status)
                .map(|_| GateRef::new("gate:process-journal").expect("gate")),
            blocked_activity_id: None,
            credential_requirements: Vec::new(),
            failure: None,
            event_cursor: EventCursor(7),
            runner_id: None,
            lease_token: None,
            lease_expires_at: None,
            last_heartbeat_at: None,
            claim_count: 0,
            received_at: Utc::now(),
            parent_run_id: None,
            subagent_depth: 0,
            spawn_tree_root_run_id: None,
            product_context: None,
            resume_disposition: None,
        }
    }

    fn process_lease_request() -> ProcessLeaseRequest {
        let runner_id = TurnRunnerId::new();
        let lease_token = crate::TurnLeaseToken::new();
        ProcessLeaseRequest {
            process_id: process_id_from_turn_run_id(TurnRunId::new()),
            worker_id: ProcessWorkerId::from_trusted(runner_id.to_wire_string()),
            lease_token: ProcessLeaseToken::from_trusted(lease_token.to_wire_string()),
        }
    }

    fn submit_request(run_id: TurnRunId) -> SubmitTurnRequest {
        SubmitTurnRequest {
            requested_model: None,
            scope: scope(),
            actor: TurnActor::new(UserId::new("user:process").expect("user")),
            accepted_message_ref: AcceptedMessageRef::new("accepted-process-transition")
                .expect("accepted"),
            source_binding_ref: SourceBindingRef::new("source-process-transition").expect("source"),
            reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-transition")
                .expect("reply"),
            requested_run_profile: Some(RunProfileRequest::new("default").expect("profile")),
            idempotency_key: IdempotencyKey::new("idem-process-transition").expect("idempotency"),
            received_at: Utc::now(),
            requested_run_id: Some(run_id),
            parent_run_id: None,
            subagent_depth: 0,
            spawn_tree_root_run_id: None,
            product_context: None,
        }
    }

    #[test]
    fn every_turn_status_maps_to_process_lifecycle_status() {
        let cases = [
            (TurnStatus::Queued, ProcessLifecycleStatus::Queued),
            (TurnStatus::Running, ProcessLifecycleStatus::Running),
            (
                TurnStatus::BlockedApproval,
                ProcessLifecycleStatus::Suspended,
            ),
            (TurnStatus::BlockedAuth, ProcessLifecycleStatus::Suspended),
            (
                TurnStatus::BlockedResource,
                ProcessLifecycleStatus::Suspended,
            ),
            (
                TurnStatus::BlockedDependentRun,
                ProcessLifecycleStatus::Suspended,
            ),
            (
                TurnStatus::BlockedExternalTool,
                ProcessLifecycleStatus::Suspended,
            ),
            (
                TurnStatus::CancelRequested,
                ProcessLifecycleStatus::CancelRequested,
            ),
            (TurnStatus::Cancelled, ProcessLifecycleStatus::Cancelled),
            (TurnStatus::Completed, ProcessLifecycleStatus::Completed),
            (TurnStatus::Failed, ProcessLifecycleStatus::Failed),
            (
                TurnStatus::RecoveryRequired,
                ProcessLifecycleStatus::RecoveryRequired,
            ),
        ];

        for (turn_status, process_status) in cases {
            assert_eq!(process_status_from_turn_status(turn_status), process_status);
            assert_eq!(
                process_status_from_turn_status(turn_status).keeps_active_lock(),
                turn_status.keeps_active_lock()
            );
        }
    }

    #[test]
    fn blocked_turn_statuses_map_to_process_suspension_kinds() {
        let cases = [
            (TurnStatus::BlockedApproval, ProcessSuspensionKind::Approval),
            (
                TurnStatus::BlockedAuth,
                ProcessSuspensionKind::Authorization,
            ),
            (TurnStatus::BlockedResource, ProcessSuspensionKind::Resource),
            (
                TurnStatus::BlockedDependentRun,
                ProcessSuspensionKind::AwaitingChildProcess,
            ),
            (
                TurnStatus::BlockedExternalTool,
                ProcessSuspensionKind::ExternalTool,
            ),
        ];

        for (turn_status, suspension_kind) in cases {
            let snapshot = record_with_status(turn_status).to_process_snapshot();
            assert_eq!(snapshot.status, ProcessLifecycleStatus::Suspended);
            assert_eq!(
                snapshot.suspension.expect("suspension").kind,
                suspension_kind
            );
        }
    }

    #[test]
    fn every_turn_event_kind_maps_to_process_journal_kind() {
        let cases = [
            (TurnEventKind::Submitted, ProcessJournalKind::Submitted),
            (TurnEventKind::Resumed, ProcessJournalKind::Resumed),
            (TurnEventKind::RunnerClaimed, ProcessJournalKind::Claimed),
            (
                TurnEventKind::RunnerHeartbeat,
                ProcessJournalKind::Heartbeat,
            ),
            (
                TurnEventKind::RecoveryRequired,
                ProcessJournalKind::RecoveryRequired,
            ),
            (TurnEventKind::Blocked, ProcessJournalKind::Suspended),
            (
                TurnEventKind::CancelRequested,
                ProcessJournalKind::CancelRequested,
            ),
            (TurnEventKind::Cancelled, ProcessJournalKind::Cancelled),
            (TurnEventKind::Completed, ProcessJournalKind::Completed),
            (TurnEventKind::Failed, ProcessJournalKind::Failed),
        ];

        for (turn_kind, process_kind) in cases {
            assert_eq!(
                process_journal_kind_from_turn_event_kind(turn_kind),
                process_kind
            );
        }
    }

    #[test]
    fn lifecycle_event_projects_to_process_journal_entry() {
        let state = crate::TurnRunState {
            scope: scope(),
            actor: Some(TurnActor::new(UserId::new("user:process").expect("user"))),
            turn_id: TurnId::new(),
            run_id: TurnRunId::new(),
            status: TurnStatus::BlockedAuth,
            accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
                .expect("accepted"),
            source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
            reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
                .expect("reply"),
            resolved_run_profile_id: RunProfileId::default_profile(),
            resolved_run_profile_version: RunProfileVersion::new(1),
            resolved_model_route: None,
            model_usage: None,
            received_at: Utc::now(),
            checkpoint_id: None,
            gate_ref: Some(GateRef::new("gate:process-journal").expect("gate")),
            blocked_activity_id: Some(CapabilityActivityId::new()),
            credential_requirements: Vec::new(),
            failure: None,
            event_cursor: EventCursor(9),
            product_context: None,
            resume_disposition: None,
        };
        let event = TurnLifecycleEvent::from_run_state(
            &state,
            TurnEventKind::Blocked,
            Some("auth_required".to_string()),
        );

        let entry = event.to_process_journal_entry();

        assert_eq!(entry.cursor, ProcessJournalCursor(9));
        assert_eq!(entry.process_id, process_id_from_turn_run_id(state.run_id));
        assert_eq!(entry.status, ProcessLifecycleStatus::Suspended);
        assert_eq!(entry.kind, ProcessJournalKind::Suspended);
        assert_eq!(
            entry.suspension.expect("suspension").kind,
            ProcessSuspensionKind::Authorization
        );
        assert_eq!(entry.sanitized_reason.as_deref(), Some("auth_required"));
    }

    #[test]
    fn claimed_turn_run_projects_to_process_claim() {
        let state = crate::TurnRunState {
            scope: scope(),
            actor: Some(TurnActor::new(UserId::new("user:process").expect("user"))),
            turn_id: TurnId::new(),
            run_id: TurnRunId::new(),
            status: TurnStatus::Running,
            accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
                .expect("accepted"),
            source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
            reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
                .expect("reply"),
            resolved_run_profile_id: RunProfileId::default_profile(),
            resolved_run_profile_version: RunProfileVersion::new(1),
            resolved_model_route: None,
            model_usage: None,
            received_at: Utc::now(),
            checkpoint_id: None,
            gate_ref: None,
            blocked_activity_id: None,
            credential_requirements: Vec::new(),
            failure: None,
            event_cursor: EventCursor(11),
            product_context: None,
            resume_disposition: None,
        };
        let claimed = ClaimedTurnRun {
            state: state.clone(),
            resolved_run_profile: profile().resolved,
            runner_id: TurnRunnerId::new(),
            lease_token: crate::TurnLeaseToken::new(),
        };

        let process = KernelClaimedProcess::from(&claimed);

        assert_eq!(
            process.state.process_id,
            process_id_from_turn_run_id(state.run_id)
        );
        assert_eq!(process.state.status, ProcessLifecycleStatus::Running);
        assert_eq!(
            process.state.metadata["agent_turn"]["turn_id"],
            json!(state.turn_id)
        );
    }

    #[tokio::test]
    async fn process_transition_adapter_drives_real_turn_store_transitions() {
        let store = Arc::new(crate::test_support::in_memory_turn_state_store());
        let transitions: Arc<dyn TurnRunTransitionPort> = store.clone();
        let adapter = AgentTurnProcessTransitionAdapter::new(transitions);
        let run_id = TurnRunId::new();
        let response = store
            .submit_turn(
                submit_request(run_id),
                &AllowAllAdmissionPolicy,
                &InMemoryRunProfileResolver::default(),
            )
            .await
            .expect("submit turn");
        let SubmitTurnResponse::Accepted {
            run_id: accepted_run_id,
            ..
        } = response;
        assert_eq!(accepted_run_id, run_id);

        let worker_id = ProcessWorkerId::from_trusted(TurnRunnerId::new().to_wire_string());
        let lease_token =
            ProcessLeaseToken::from_trusted(crate::TurnLeaseToken::new().to_wire_string());
        let claimed = adapter
            .claim_next_process(ClaimProcessRequest {
                worker_id: worker_id.clone(),
                lease_token: lease_token.clone(),
                scope_filter: None,
            })
            .await
            .expect("claim process")
            .expect("claimed process");
        assert_eq!(
            claimed.state.process_id,
            process_id_from_turn_run_id(run_id)
        );
        assert_eq!(claimed.state.status, ProcessLifecycleStatus::Running);
        assert_eq!(claimed.worker_id, worker_id);
        assert_eq!(claimed.lease_token, lease_token);

        let cursor = adapter
            .heartbeat_process(ProcessLeaseRequest {
                process_id: claimed.state.process_id,
                worker_id: worker_id.clone(),
                lease_token: lease_token.clone(),
            })
            .await
            .expect("heartbeat process");
        assert!(cursor.0 >= claimed.state.journal_cursor.0);

        let completed = adapter
            .complete_process(ProcessLeaseRequest {
                process_id: claimed.state.process_id,
                worker_id,
                lease_token,
            })
            .await
            .expect("complete process");
        assert_eq!(completed.status, ProcessLifecycleStatus::Completed);
    }

    #[test]
    fn process_lease_request_maps_to_runner_lease_requests() {
        let request = process_lease_request();

        let heartbeat = HeartbeatRequest::try_from(request.clone()).expect("heartbeat");
        assert_eq!(
            heartbeat.run_id,
            turn_run_id_from_process_id(request.process_id)
        );

        let complete = CompleteRunRequest::try_from(request.clone()).expect("complete");
        assert_eq!(
            complete.run_id,
            turn_run_id_from_process_id(request.process_id)
        );

        let cancel = CancelRunCompletionRequest::try_from(request.clone()).expect("cancel");
        assert_eq!(
            cancel.run_id,
            turn_run_id_from_process_id(request.process_id)
        );

        let relinquish = RelinquishRunRequest::try_from(request.clone()).expect("relinquish");
        assert_eq!(
            relinquish.run_id,
            turn_run_id_from_process_id(request.process_id)
        );
    }

    #[test]
    fn runner_outcomes_map_to_process_outcomes() {
        let failure = crate::SanitizedFailure::new("runner_failed").expect("failure");
        let blocked = TurnRunnerOutcome::Blocked {
            checkpoint_id: TurnCheckpointId::new(),
            state_ref: crate::run_profile::LoopCheckpointStateRef::new(
                "checkpoint:state-process-journal".to_string(),
            )
            .expect("state ref"),
            reason: BlockedReason::ExternalTool {
                gate_ref: GateRef::new("gate:process-journal").expect("gate"),
            },
            blocked_activity_id: Some(CapabilityActivityId::new()),
        };

        assert_eq!(
            process_outcome_from_turn_runner_outcome(TurnRunnerOutcome::Completed),
            ProcessOutcome::Completed
        );
        assert_eq!(
            process_outcome_from_turn_runner_outcome(TurnRunnerOutcome::Cancelled),
            ProcessOutcome::Cancelled
        );
        assert!(matches!(
            process_outcome_from_turn_runner_outcome(blocked),
            ProcessOutcome::Suspended { .. }
        ));
        assert_eq!(
            process_outcome_from_turn_runner_outcome(TurnRunnerOutcome::Failed {
                failure: failure.clone()
            }),
            ProcessOutcome::Failed { failure }
        );
    }

    #[test]
    fn validated_exit_request_preserves_agent_loop_residue() {
        let request = process_lease_request();
        let exit = KernelApplyValidatedExitRequest {
            process_id: request.process_id,
            worker_id: request.worker_id.clone(),
            lease_token: request.lease_token.clone(),
            mapping: LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Completed),
            model_usage: None,
        };

        let loop_exit = ApplyValidatedLoopExitRequest::try_from(exit).expect("loop exit");
        assert_eq!(
            loop_exit.run_id,
            turn_run_id_from_process_id(request.process_id)
        );
        assert_eq!(
            loop_exit.mapping,
            LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Completed)
        );
    }
}
