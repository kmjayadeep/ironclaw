//! Durable process journal contracts.
//!
//! These types describe the kernel-level process lifecycle independently from
//! any one process kind. Existing runtime capability execution records remain in
//! `types`; this module is the richer journal vocabulary used for queued,
//! leased, suspended, resumed, stopped, killed, recovered, and terminal
//! processes.

use async_trait::async_trait;
use ironclaw_host_api::{
    CapabilityActivityId, ProcessId, ResourceScope, RuntimeCredentialAuthRequirement,
    SanitizedFailure, TenantId, Timestamp, TurnGateRef, UserId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessJournalCursor(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessCheckpointRef(String);

impl ProcessCheckpointRef {
    pub fn new(value: impl Into<String>) -> Result<Self, ProcessJournalError> {
        let value = value.into();
        validate_opaque_ref("process checkpoint", &value)?;
        Ok(Self(value))
    }

    pub fn from_trusted(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessWorkerId(String);

impl ProcessWorkerId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProcessJournalError> {
        let value = value.into();
        validate_opaque_ref("process worker", &value)?;
        Ok(Self(value))
    }

    pub fn from_trusted(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessLeaseToken(String);

impl ProcessLeaseToken {
    pub fn new(value: impl Into<String>) -> Result<Self, ProcessJournalError> {
        let value = value.into();
        validate_opaque_ref("process lease token", &value)?;
        Ok(Self(value))
    }

    pub fn from_trusted(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProcessOperationId(String);

impl ProcessOperationId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProcessJournalError> {
        let value = value.into();
        validate_opaque_ref("process operation", &value)?;
        Ok(Self(value))
    }

    pub fn from_trusted(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessKind {
    AgentTurn,
    CapabilityInvocation,
    Internal,
    External,
    ExtensionDefined(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessLifecycleStatus {
    Queued,
    Running,
    Suspended,
    StopRequested,
    CancelRequested,
    Stopped,
    Cancelled,
    Completed,
    Failed,
    Killed,
    RecoveryRequired,
}

impl ProcessLifecycleStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Stopped
                | Self::Cancelled
                | Self::Completed
                | Self::Failed
                | Self::Killed
                | Self::RecoveryRequired
        )
    }

    pub fn keeps_active_lock(self) -> bool {
        !self.is_terminal()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessSuspensionKind {
    Approval,
    Authorization,
    Resource,
    AwaitingChildProcess,
    ExternalTool,
    ExternalProcess,
    ExtensionDefined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSuspension {
    pub kind: ProcessSuspensionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_ref: Option<TurnGateRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity_id: Option<CapabilityActivityId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credential_requirements: Vec<RuntimeCredentialAuthRequirement>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessJournalKind {
    Spawned,
    Submitted,
    Resumed,
    Claimed,
    Heartbeat,
    RecoveryRequired,
    Suspended,
    StopRequested,
    CancelRequested,
    Stopped,
    Cancelled,
    Completed,
    Failed,
    Killed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessLeaseSnapshot {
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<Timestamp>,
    pub claim_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimProcessesRequest {
    pub worker_id: ProcessWorkerId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_filter: Option<ResourceScope>,
    pub max_processes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournaledProcessSnapshot {
    pub process_id: ProcessId,
    pub process_kind: ProcessKind,
    pub scope: ResourceScope,
    pub status: ProcessLifecycleStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<ProcessSuspension>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_ref: Option<ProcessCheckpointRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<SanitizedFailure>,
    pub journal_cursor: ProcessJournalCursor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<ProcessLeaseSnapshot>,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_process_id: Option<ProcessId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_process_id: Option<ProcessId>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClaimedProcess {
    pub state: JournaledProcessSnapshot,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessJournalEntry {
    pub cursor: ProcessJournalCursor,
    pub process_id: ProcessId,
    pub process_kind: ProcessKind,
    pub scope: ResourceScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurred_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<UserId>,
    pub status: ProcessLifecycleStatus,
    pub kind: ProcessJournalKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspension: Option<ProcessSuspension>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sanitized_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

/// Scope-bound process journal cursor.
///
/// The scope rides with the cursor so product adapters cannot replay a cursor
/// minted for one scope as bearer authority for another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ProcessJournalProjectionCursor {
    pub journal: ProcessJournalCursor,
    pub scope: ResourceScope,
}

impl ProcessJournalProjectionCursor {
    pub fn for_scope(scope: ResourceScope, journal: ProcessJournalCursor) -> Self {
        Self { journal, scope }
    }

    pub fn origin_for_scope(scope: ResourceScope) -> Self {
        Self {
            journal: ProcessJournalCursor(0),
            scope,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetProcessSnapshotRequest {
    pub scope: ResourceScope,
    pub process_id: ProcessId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessJournalProjectionRequest {
    pub scope: ResourceScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<UserId>,
    pub after: Option<ProcessJournalProjectionCursor>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessJournalPage {
    pub entries: Vec<ProcessJournalEntry>,
    pub next_cursor: ProcessJournalCursor,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebase_required: Option<ProcessJournalCursor>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessJournalProjectionSnapshot {
    pub entries: Vec<ProcessJournalEntry>,
    pub next_cursor: ProcessJournalProjectionCursor,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessLifecycleLookupRequest {
    pub tenant_id: TenantId,
    pub process_id: ProcessId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessLifecycleLookupBatchRequest {
    pub processes: Vec<ProcessLifecycleLookupRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubmitProcessRequest {
    pub process_id: ProcessId,
    pub process_kind: ProcessKind,
    pub scope: ResourceScope,
    #[serde(default)]
    pub exclusive_within_scope: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<ProcessOperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_process_id: Option<ProcessId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_process_id: Option<ProcessId>,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProcessLifecycleLookupResult {
    Missing,
    Found {
        status: ProcessLifecycleStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        suspension: Option<ProcessSuspension>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessGateOwnerMatch {
    Explicit,
    ExplicitOrActor,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessGateQuery {
    pub scope: ResourceScope,
    pub gate_kind: ProcessSuspensionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gate_ref: Option<TurnGateRef>,
    #[serde(default)]
    pub owner_match: Option<ProcessGateOwnerMatch>,
    #[serde(default)]
    pub include_historical: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessGateRecord {
    pub process_id: ProcessId,
    pub scope: ResourceScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<UserId>,
    pub suspension: ProcessSuspension,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_target_ref: Option<String>,
    pub historical: bool,
}

#[async_trait]
pub trait ProcessLifecycleLookupSource: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn process_lifecycle_states(
        &self,
        request: ProcessLifecycleLookupBatchRequest,
    ) -> Vec<Result<ProcessLifecycleLookupResult, Self::Error>>;
}

#[async_trait]
pub trait ProcessGateQuerySource: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn query_process_gates(
        &self,
        request: ProcessGateQuery,
    ) -> Result<Vec<ProcessGateRecord>, Self::Error>;
}

#[async_trait]
pub trait ProcessJournalSource: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn get_process_snapshot(
        &self,
        request: GetProcessSnapshotRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;

    async fn read_process_journal_after(
        &self,
        scope: &ResourceScope,
        owner_user_id: Option<&UserId>,
        after: Option<ProcessJournalCursor>,
        limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error>;

    /// Read the authoritative process journal in global cursor order for
    /// host-owned durable projection consumers.
    async fn read_process_journal_log_after(
        &self,
        after: Option<ProcessJournalCursor>,
        limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error>;
}

#[async_trait]
pub trait ProcessSubmissionPort: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn submit_process(
        &self,
        request: SubmitProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessLeaseRequest {
    pub process_id: ProcessId,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessStateTransitionRequest {
    pub lease: ProcessLeaseRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuspendProcessRequest {
    pub process_id: ProcessId,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
    pub checkpoint_ref: ProcessCheckpointRef,
    pub suspension: ProcessSuspension,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeProcessRequest {
    pub scope: ResourceScope,
    pub process_id: ProcessId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<ProcessOperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_cursor: Option<ProcessJournalCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_ref: Option<ProcessCheckpointRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopProcessRequest {
    pub scope: ResourceScope,
    pub process_id: ProcessId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<ProcessOperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelProcessRequest {
    pub scope: ResourceScope,
    pub process_id: ProcessId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<ProcessOperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KillProcessRequest {
    pub scope: ResourceScope,
    pub process_id: ProcessId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<ProcessOperationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessControlResult {
    pub state: JournaledProcessSnapshot,
    pub changed: bool,
    pub already_terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailProcessRequest {
    pub process_id: ProcessId,
    pub worker_id: ProcessWorkerId,
    pub lease_token: ProcessLeaseToken,
    pub failure: SanitizedFailure,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverExpiredProcessLeasesRequest {
    pub now: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_filter: Option<ResourceScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoverExpiredProcessLeasesResponse {
    pub recovered: Vec<JournaledProcessSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessOutcome {
    Completed,
    Stopped,
    Cancelled,
    Suspended {
        checkpoint_ref: ProcessCheckpointRef,
        suspension: ProcessSuspension,
    },
    Failed {
        failure: SanitizedFailure,
    },
    Killed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

#[async_trait]
pub trait ProcessTransitionPort: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn claim_next_processes(
        &self,
        request: ClaimProcessesRequest,
    ) -> Result<Vec<ClaimedProcess>, Self::Error>;

    async fn heartbeat_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<ProcessJournalCursor, Self::Error>;

    async fn recover_expired_process_leases(
        &self,
        request: RecoverExpiredProcessLeasesRequest,
    ) -> Result<RecoverExpiredProcessLeasesResponse, Self::Error>;

    async fn suspend_process(
        &self,
        request: SuspendProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;

    async fn complete_process(
        &self,
        request: ProcessStateTransitionRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;

    async fn cancel_process(
        &self,
        request: ProcessStateTransitionRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;

    async fn fail_process(
        &self,
        request: FailProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;

    async fn relinquish_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error>;
}

#[async_trait]
pub trait ProcessControlPort: Send + Sync {
    type Error: Send + Sync + 'static;

    async fn resume_process(
        &self,
        request: ResumeProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error>;

    async fn stop_process(
        &self,
        request: StopProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error>;

    async fn request_cancel_process(
        &self,
        request: CancelProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error>;

    async fn kill_process(
        &self,
        request: KillProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProcessJournalError {
    #[error("invalid {kind} ref: must not be empty")]
    EmptyRef { kind: &'static str },
    #[error("invalid {kind} ref: must be at most 512 bytes")]
    RefTooLong { kind: &'static str },
    #[error("invalid {kind} ref: control characters are not allowed")]
    ControlCharacter { kind: &'static str },
}

fn validate_opaque_ref(kind: &'static str, value: &str) -> Result<(), ProcessJournalError> {
    if value.is_empty() {
        return Err(ProcessJournalError::EmptyRef { kind });
    }
    if value.len() > 512 {
        return Err(ProcessJournalError::RefTooLong { kind });
    }
    if value.chars().any(char::is_control) {
        return Err(ProcessJournalError::ControlCharacter { kind });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_status_terminality_separates_live_from_terminal() {
        for status in [
            ProcessLifecycleStatus::Queued,
            ProcessLifecycleStatus::Running,
            ProcessLifecycleStatus::Suspended,
            ProcessLifecycleStatus::StopRequested,
            ProcessLifecycleStatus::CancelRequested,
        ] {
            assert!(!status.is_terminal());
            assert!(status.keeps_active_lock());
        }

        for status in [
            ProcessLifecycleStatus::Stopped,
            ProcessLifecycleStatus::Cancelled,
            ProcessLifecycleStatus::Completed,
            ProcessLifecycleStatus::Failed,
            ProcessLifecycleStatus::Killed,
            ProcessLifecycleStatus::RecoveryRequired,
        ] {
            assert!(status.is_terminal());
            assert!(!status.keeps_active_lock());
        }
    }

    #[test]
    fn opaque_refs_reject_empty_oversized_and_control_values() {
        assert!(ProcessCheckpointRef::new("").is_err());
        assert!(ProcessWorkerId::new("worker\none").is_err());
        assert!(ProcessLeaseToken::new("x".repeat(513)).is_err());

        let checkpoint = ProcessCheckpointRef::new("checkpoint:ok").expect("checkpoint");
        assert_eq!(checkpoint.as_str(), "checkpoint:ok");
    }
}
