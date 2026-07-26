//! Compatibility projection from the legacy capability-process API to the
//! authoritative process journal.

use std::sync::Arc;

use async_trait::async_trait;
use ironclaw_events::sanitize_error_kind;
use ironclaw_filesystem::{RootFilesystem, ScopedFilesystem};
use ironclaw_host_api::{
    CapabilityId, CapabilitySet, ExtensionId, InvocationId, MountView,
    ProcessAuthorizedContinuation, ProcessId, ResourceEstimate, ResourceReservationId,
    ResourceScope, RuntimeKind, SanitizedFailure, UserId,
};
use serde::{Deserialize, Serialize};

use crate::types::{ProcessError, ProcessRecord, ProcessStart, ProcessStatus, ProcessStorePort};
use crate::{
    ClaimProcessesRequest, FailProcessRequest, GetProcessSnapshotRequest, KillProcessRequest,
    ProcessControlPort, ProcessJournalSource, ProcessJournalStore, ProcessJournalStoreError,
    ProcessKind, ProcessLeaseRequest, ProcessLifecycleStatus, ProcessSnapshotSource,
    ProcessStateTransitionRequest, ProcessSubmissionPort, ProcessTransitionPort, ProcessWorkerId,
    SubmitProcessRequest,
};

const BACKGROUND_WORKER_ID: &str = "capability-background";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CapabilityProcessMetadata {
    invocation_id: InvocationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authenticated_actor_user_id: Option<UserId>,
    extension_id: ExtensionId,
    capability_id: CapabilityId,
    #[serde(deserialize_with = "ironclaw_host_api::deserialize_trusted_runtime_kind")]
    runtime: RuntimeKind,
    grants: CapabilitySet,
    mounts: MountView,
    estimated_resources: ResourceEstimate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resource_reservation_id: Option<ResourceReservationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authorized_continuation: Option<ProcessAuthorizedContinuation>,
}

/// Temporary compatibility surface while capability callers move to
/// [`crate::ProcessRuntimePort`].
///
/// This store writes no lifecycle JSON records. Its mutations append journal
/// commands and its reads materialize journal snapshots.
pub struct JournalProcessStore<F>
where
    F: RootFilesystem,
{
    journal: Arc<ProcessJournalStore<F>>,
}

impl<F> JournalProcessStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    pub fn new(filesystem: Arc<ScopedFilesystem<F>>) -> Self {
        Self {
            journal: Arc::new(ProcessJournalStore::new(filesystem)),
        }
    }

    pub fn from_arc(filesystem: Arc<ScopedFilesystem<F>>) -> Self {
        Self::new(filesystem)
    }

    async fn snapshot(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<crate::JournaledProcessSnapshot, ProcessError> {
        self.journal
            .get_process_snapshot(GetProcessSnapshotRequest {
                scope: scope.clone(),
                process_id,
            })
            .await
            .map_err(map_journal_error)
    }

    fn record_from_snapshot(
        snapshot: crate::JournaledProcessSnapshot,
    ) -> Result<ProcessRecord, ProcessError> {
        let metadata: CapabilityProcessMetadata = serde_json::from_value(snapshot.metadata)
            .map_err(|error| ProcessError::Deserialization(error.to_string()))?;
        Ok(ProcessRecord {
            process_id: snapshot.process_id,
            parent_process_id: snapshot.parent_process_id,
            invocation_id: metadata.invocation_id,
            scope: snapshot.scope,
            authenticated_actor_user_id: metadata.authenticated_actor_user_id,
            extension_id: metadata.extension_id,
            capability_id: metadata.capability_id,
            runtime: metadata.runtime,
            status: process_status(snapshot.status),
            grants: metadata.grants,
            mounts: metadata.mounts,
            estimated_resources: metadata.estimated_resources,
            resource_reservation_id: metadata.resource_reservation_id,
            authorized_continuation: metadata.authorized_continuation,
            error_kind: snapshot.failure.map(SanitizedFailure::into_category),
        })
    }

    fn lease_request(
        snapshot: &crate::JournaledProcessSnapshot,
    ) -> Result<ProcessLeaseRequest, ProcessError> {
        let lease = snapshot
            .lease
            .as_ref()
            .ok_or_else(|| ProcessError::InvalidStoredRecord {
                reason: format!(
                    "running process {} has no journal lease",
                    snapshot.process_id
                ),
            })?;
        Ok(ProcessLeaseRequest {
            process_id: snapshot.process_id,
            worker_id: lease.worker_id.clone(),
            lease_token: lease.lease_token.clone(),
        })
    }
}

#[async_trait]
impl<F> ProcessStorePort for JournalProcessStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    async fn start(&self, start: ProcessStart) -> Result<ProcessRecord, ProcessError> {
        let process_id = start.process_id;
        let scope = start.scope.clone();
        let metadata = serde_json::to_value(CapabilityProcessMetadata {
            invocation_id: start.invocation_id,
            authenticated_actor_user_id: start.authenticated_actor_user_id,
            extension_id: start.extension_id,
            capability_id: start.capability_id,
            runtime: start.runtime,
            grants: start.grants,
            mounts: start.mounts,
            estimated_resources: start.estimated_resources,
            resource_reservation_id: start.resource_reservation_id,
            authorized_continuation: start.authorized_continuation,
        })
        .map_err(|error| ProcessError::Serialization(error.to_string()))?;
        self.journal
            .submit_process(SubmitProcessRequest {
                process_id,
                process_kind: ProcessKind::CapabilityInvocation,
                scope: scope.clone(),
                exclusive_within_scope: false,
                operation_id: None,
                owner_user_id: None,
                concurrency_class: None,
                parent_process_id: start.parent_process_id,
                root_process_id: None,
                spawn_tree_descendant_cap: None,
                checkpoint_ref: None,
                created_at: chrono::Utc::now(),
                metadata,
            })
            .await
            .map_err(map_journal_error)?;
        let snapshot = self
            .journal
            .claim_next_processes(ClaimProcessesRequest {
                worker_id: ProcessWorkerId::from_trusted(BACKGROUND_WORKER_ID),
                scope_filter: Some(scope),
                process_id_filter: Some(process_id),
                process_kind_filter: Some(ProcessKind::CapabilityInvocation),
                max_processes: 1,
            })
            .await
            .map_err(map_journal_error)?
            .into_iter()
            .next()
            .ok_or_else(|| ProcessError::InvalidStoredRecord {
                reason: format!("submitted process {process_id} was not claimable"),
            })?
            .state;
        Self::record_from_snapshot(snapshot)
    }

    async fn complete(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<ProcessRecord, ProcessError> {
        let snapshot = self.snapshot(scope, process_id).await?;
        if snapshot.status != ProcessLifecycleStatus::Running {
            return Err(ProcessError::InvalidTransition {
                process_id,
                from: process_status(snapshot.status),
                to: ProcessStatus::Completed,
            });
        }
        let snapshot = self
            .journal
            .complete_process(ProcessStateTransitionRequest {
                lease: Self::lease_request(&snapshot)?,
                metadata: None,
            })
            .await
            .map_err(map_journal_error)?;
        Self::record_from_snapshot(snapshot)
    }

    async fn fail(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
        error_kind: String,
    ) -> Result<ProcessRecord, ProcessError> {
        let snapshot = self.snapshot(scope, process_id).await?;
        if snapshot.status != ProcessLifecycleStatus::Running {
            return Err(ProcessError::InvalidTransition {
                process_id,
                from: process_status(snapshot.status),
                to: ProcessStatus::Failed,
            });
        }
        let lease = Self::lease_request(&snapshot)?;
        let sanitized = sanitize_error_kind(error_kind);
        let failure = SanitizedFailure::new(sanitized)
            .unwrap_or_else(|_| SanitizedFailure::from_trusted_static("unknown_failure"));
        let snapshot = self
            .journal
            .fail_process(FailProcessRequest {
                process_id,
                worker_id: lease.worker_id,
                lease_token: lease.lease_token,
                failure,
                metadata: None,
            })
            .await
            .map_err(map_journal_error)?;
        Self::record_from_snapshot(snapshot)
    }

    async fn kill(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<ProcessRecord, ProcessError> {
        let result = self
            .journal
            .kill_process(KillProcessRequest {
                scope: scope.clone(),
                process_id,
                operation_id: None,
                reason: None,
            })
            .await
            .map_err(map_journal_error)?;
        Self::record_from_snapshot(result.state)
    }

    async fn get(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<Option<ProcessRecord>, ProcessError> {
        match self.snapshot(scope, process_id).await {
            Ok(snapshot) => Self::record_from_snapshot(snapshot).map(Some),
            Err(ProcessError::UnknownProcess { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn records_for_scope(
        &self,
        scope: &ResourceScope,
    ) -> Result<Vec<ProcessRecord>, ProcessError> {
        self.journal
            .process_snapshots(scope)
            .await
            .map_err(map_journal_error)?
            .into_iter()
            .filter(|snapshot| snapshot.process_kind == ProcessKind::CapabilityInvocation)
            .map(Self::record_from_snapshot)
            .collect()
    }
}

fn process_status(status: ProcessLifecycleStatus) -> ProcessStatus {
    match status {
        ProcessLifecycleStatus::Queued
        | ProcessLifecycleStatus::Running
        | ProcessLifecycleStatus::Suspended
        | ProcessLifecycleStatus::StopRequested
        | ProcessLifecycleStatus::CancelRequested => ProcessStatus::Running,
        ProcessLifecycleStatus::Completed => ProcessStatus::Completed,
        ProcessLifecycleStatus::Failed | ProcessLifecycleStatus::RecoveryRequired => {
            ProcessStatus::Failed
        }
        ProcessLifecycleStatus::Stopped
        | ProcessLifecycleStatus::Cancelled
        | ProcessLifecycleStatus::Killed => ProcessStatus::Killed,
    }
}

fn map_journal_error(error: ProcessJournalStoreError) -> ProcessError {
    match error {
        ProcessJournalStoreError::UnknownProcess { process_id } => {
            ProcessError::UnknownProcess { process_id }
        }
        ProcessJournalStoreError::ProcessAlreadyExists { process_id } => {
            ProcessError::ProcessAlreadyExists { process_id }
        }
        ProcessJournalStoreError::InvalidTransition {
            process_id,
            from,
            to,
        } => ProcessError::InvalidTransition {
            process_id,
            from: process_status(from),
            to: process_status(to),
        },
        ProcessJournalStoreError::Filesystem(error) => ProcessError::Filesystem(error),
        ProcessJournalStoreError::Serialization(reason) => ProcessError::Serialization(reason),
        ProcessJournalStoreError::Deserialization(reason) => ProcessError::Deserialization(reason),
        other => ProcessError::InvalidStoredRecord {
            reason: other.to_string(),
        },
    }
}
