use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use ironclaw_filesystem::{
    CasExpectation, ContentType, Entry, FilesystemError, FilesystemOperation, RecordVersion,
    RootFilesystem, ScopedFilesystem,
};
use ironclaw_host_api::{ProcessId, ResourceScope, ScopedPath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::journal::{
    CancelProcessRequest, ClaimProcessesRequest, ClaimedProcess, FailProcessRequest,
    GetProcessSnapshotRequest, JournaledProcessSnapshot, KillProcessRequest, ProcessControlPort,
    ProcessControlResult, ProcessGateOwnerMatch, ProcessGateQuery, ProcessGateQuerySource,
    ProcessGateRecord, ProcessJournalCommit, ProcessJournalCommitObserver, ProcessJournalCursor,
    ProcessJournalEntry, ProcessJournalKind, ProcessJournalObserverRegistry, ProcessJournalPage,
    ProcessJournalSource, ProcessLeaseRequest, ProcessLeaseSnapshot, ProcessLeaseToken,
    ProcessLifecycleLookupBatchRequest, ProcessLifecycleLookupResult, ProcessLifecycleLookupSource,
    ProcessLifecycleStatus, ProcessOperationId, ProcessSubmissionPort, ProcessSuspension,
    ProcessTransitionPort, ProcessTreePort, ProcessTreeReservation, ProcessWorkerId,
    PruneReleasedProcessRequest, RecoverExpiredProcessLeasesRequest,
    RecoverExpiredProcessLeasesResponse, ReleaseProcessTreeRequest, ReserveProcessTreeRequest,
    ResumeProcessRequest, StopProcessRequest, SubmitProcessRequest, SuspendProcessRequest,
};
use crate::types::{invalid_path, same_scope_owner};

const JOURNAL_STATE_PATH: &str = "/processes/journal/state.json";
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(90);
const MAX_CAS_RETRIES: usize = 5;
const MAX_CONTROL_IDEMPOTENCY_RECORDS: usize = 4096;

#[derive(Debug, Error)]
pub enum ProcessJournalStoreError {
    #[error("unknown process {process_id}")]
    UnknownProcess { process_id: ProcessId },
    #[error("process {process_id} already exists")]
    ProcessAlreadyExists { process_id: ProcessId },
    #[error(
        "scope already has active {process_kind:?} process {process_id} in {status:?} at cursor {cursor:?}"
    )]
    ActiveProcessConflict {
        process_id: ProcessId,
        process_kind: crate::ProcessKind,
        status: ProcessLifecycleStatus,
        suspension: Option<Box<ProcessSuspension>>,
        cursor: ProcessJournalCursor,
    },
    #[error("process {process_id} cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        process_id: ProcessId,
        from: ProcessLifecycleStatus,
        to: ProcessLifecycleStatus,
    },
    #[error("process {process_id} lease is invalid")]
    InvalidLease { process_id: ProcessId },
    #[error("process scope is not authorized for lineage operation")]
    UnauthorizedScope,
    #[error("invalid process journal request: {0}")]
    InvalidRequest(String),
    #[error("process tree descendant capacity {cap} exceeded")]
    ProcessTreeCapacityExceeded { cap: u32 },
    #[error("process {process_id} changed after cursor {expected:?}; current cursor is {actual:?}")]
    StaleSnapshot {
        process_id: ProcessId,
        expected: ProcessJournalCursor,
        actual: ProcessJournalCursor,
    },
    #[error("invalid storage path: {0}")]
    InvalidPath(String),
    #[error("filesystem error: {0}")]
    Filesystem(#[from] FilesystemError),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("deserialization error: {0}")]
    Deserialization(String),
    #[error("process journal observer error: {0}")]
    Observer(String),
}

pub struct ProcessJournalStore<F>
where
    F: RootFilesystem,
{
    filesystem: Arc<ScopedFilesystem<F>>,
    mutation_lock: Mutex<()>,
    observers: StdMutex<Vec<Arc<dyn ProcessJournalCommitObserver>>>,
    lease_duration: Duration,
}

impl<F> ProcessJournalStore<F>
where
    F: RootFilesystem,
{
    pub fn new(filesystem: Arc<ScopedFilesystem<F>>) -> Self {
        Self {
            filesystem,
            mutation_lock: Mutex::new(()),
            observers: StdMutex::new(Vec::new()),
            lease_duration: DEFAULT_LEASE_DURATION,
        }
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    async fn submit_process_inner(
        &self,
        request: SubmitProcessRequest,
    ) -> Result<(JournaledProcessSnapshot, bool), ProcessJournalStoreError> {
        let replay_key = request
            .operation_id
            .as_ref()
            .map(|operation_id| {
                serde_json::to_string(&request.scope).map(|scope| {
                    format!(
                        "submit:{scope}:{:?}:{}",
                        request.process_kind,
                        operation_id.as_str()
                    )
                })
            })
            .transpose()
            .map_err(|error| ProcessJournalStoreError::Serialization(error.to_string()))?;
        let _guard = self.mutation_lock.lock().await;
        self.mutate(|state| {
            if let Some(snapshot) = replay_key
                .as_ref()
                .and_then(|key| state.submission_idempotency.get(key))
            {
                return Ok((snapshot.clone(), false));
            }
            if state.processes.contains_key(&request.process_id) {
                return Err(ProcessJournalStoreError::ProcessAlreadyExists {
                    process_id: request.process_id,
                });
            }
            if request.exclusive_within_scope
                && let Some(active) = state.processes.values().find(|snapshot| {
                    snapshot.process_kind == request.process_kind
                        && snapshot.status.keeps_active_lock()
                        && same_scope_owner(&snapshot.scope, &request.scope)
                })
            {
                return Err(ProcessJournalStoreError::ActiveProcessConflict {
                    process_id: active.process_id,
                    process_kind: active.process_kind.clone(),
                    status: active.status,
                    suspension: active.suspension.clone().map(Box::new),
                    cursor: active.journal_cursor,
                });
            }
            let tree_reservation = if let Some(parent_process_id) = request.parent_process_id {
                let parent = state.processes.get(&parent_process_id).ok_or(
                    ProcessJournalStoreError::UnknownProcess {
                        process_id: parent_process_id,
                    },
                )?;
                if !same_lineage_scope(&parent.scope, &request.scope) {
                    return Err(ProcessJournalStoreError::UnauthorizedScope);
                }
                let root_process_id = parent.root_process_id.unwrap_or(parent.process_id);
                if request.root_process_id != Some(root_process_id) {
                    return Err(ProcessJournalStoreError::InvalidRequest(
                        "child root_process_id does not match parent lineage".to_string(),
                    ));
                }
                let cap = request.spawn_tree_descendant_cap.ok_or_else(|| {
                    ProcessJournalStoreError::InvalidRequest(
                        "child process submission requires a descendant cap".to_string(),
                    )
                })?;
                let current = state
                    .tree_reservations
                    .get(&root_process_id)
                    .map(|reservation| reservation.descendant_count)
                    .unwrap_or(0);
                let next = current
                    .checked_add(1)
                    .ok_or(ProcessJournalStoreError::ProcessTreeCapacityExceeded { cap })?;
                if next > u64::from(cap) {
                    return Err(ProcessJournalStoreError::ProcessTreeCapacityExceeded { cap });
                }
                Some((root_process_id, next))
            } else {
                if request.root_process_id.is_some() || request.spawn_tree_descendant_cap.is_some()
                {
                    return Err(ProcessJournalStoreError::InvalidRequest(
                        "root lineage requires a parent process".to_string(),
                    ));
                }
                None
            };
            let cursor = state.next_cursor();
            let snapshot = JournaledProcessSnapshot {
                process_id: request.process_id,
                process_kind: request.process_kind.clone(),
                scope: request.scope.clone(),
                status: ProcessLifecycleStatus::Queued,
                suspension: None,
                checkpoint_ref: request.checkpoint_ref.clone(),
                failure: None,
                journal_cursor: cursor,
                lease: None,
                created_at: request.created_at,
                owner_user_id: request.owner_user_id.clone(),
                parent_process_id: request.parent_process_id,
                root_process_id: request.root_process_id,
                metadata: request.metadata.clone(),
            };
            state.push_entry(ProcessJournalEntry::from_snapshot(
                &snapshot,
                cursor,
                ProcessJournalKind::Submitted,
            ));
            state
                .processes
                .insert(snapshot.process_id, snapshot.clone());
            if let Some((root_process_id, descendant_count)) = tree_reservation {
                let released_processes = state
                    .tree_reservations
                    .get(&root_process_id)
                    .map(|reservation| reservation.released_processes.clone())
                    .unwrap_or_default();
                state.tree_reservations.insert(
                    root_process_id,
                    ProcessTreeReservation {
                        root_process_id,
                        descendant_count,
                        released_processes,
                    },
                );
            }
            state.remember_submission_result(replay_key.clone(), snapshot.clone());
            Ok((snapshot, true))
        })
        .await
    }

    async fn notify_process_commit(
        &self,
        state: JournaledProcessSnapshot,
        kind: ProcessJournalKind,
        sanitized_reason: Option<String>,
    ) -> Result<(), ProcessJournalStoreError> {
        let observers = self
            .observers
            .lock()
            .map_err(|_| {
                ProcessJournalStoreError::Observer(
                    "process journal observer registry mutex poisoned".to_string(),
                )
            })?
            .clone();
        let commit = ProcessJournalCommit {
            state,
            kind,
            sanitized_reason,
        };
        for observer in observers {
            observer
                .observe_process_commit(commit.clone())
                .await
                .map_err(ProcessJournalStoreError::Observer)?;
        }
        Ok(())
    }

    async fn mutate<T>(
        &self,
        mut apply: impl FnMut(
            &mut ProcessJournalMaterializedState,
        ) -> Result<T, ProcessJournalStoreError>,
    ) -> Result<T, ProcessJournalStoreError> {
        for _ in 0..MAX_CAS_RETRIES {
            let (mut state, version) = self.load_state().await?;
            let value = apply(&mut state)?;
            match self.write_state(&state, version).await {
                Ok(()) => return Ok(value),
                Err(ProcessJournalStoreError::Filesystem(FilesystemError::VersionMismatch {
                    ..
                })) => continue,
                Err(error) => return Err(error),
            }
        }
        let path = journal_state_path()?;
        let virtual_path = self.filesystem.resolve(&ResourceScope::system(), &path)?;
        Err(ProcessJournalStoreError::Filesystem(
            FilesystemError::Backend {
                path: virtual_path,
                operation: FilesystemOperation::WriteFile,
                reason: format!("process journal exhausted {MAX_CAS_RETRIES} CAS retries"),
            },
        ))
    }

    async fn load_state(
        &self,
    ) -> Result<(ProcessJournalMaterializedState, Option<RecordVersion>), ProcessJournalStoreError>
    {
        let path = journal_state_path()?;
        let Some(versioned) = self.filesystem.get(&ResourceScope::system(), &path).await? else {
            return Ok((ProcessJournalMaterializedState::default(), None));
        };
        let state = serde_json::from_slice(&versioned.entry.body)
            .map_err(|error| ProcessJournalStoreError::Deserialization(error.to_string()))?;
        Ok((state, Some(versioned.version)))
    }

    async fn write_state(
        &self,
        state: &ProcessJournalMaterializedState,
        version: Option<RecordVersion>,
    ) -> Result<(), ProcessJournalStoreError> {
        let path = journal_state_path()?;
        let body = serde_json::to_vec_pretty(state)
            .map_err(|error| ProcessJournalStoreError::Serialization(error.to_string()))?;
        let expectation = version.map_or(CasExpectation::Absent, CasExpectation::Version);
        let entry = Entry::bytes(body).with_content_type(ContentType::json());
        match self
            .filesystem
            .put(&ResourceScope::system(), &path, entry.clone(), expectation)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_unsupported(&error) && version.is_some() => self
                .filesystem
                .put(&ResourceScope::system(), &path, entry, CasExpectation::Any)
                .await
                .map(|_| ())
                .map_err(Into::into),
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl<F> ProcessSubmissionPort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn submit_process(
        &self,
        request: SubmitProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let (snapshot, changed) = self.submit_process_inner(request).await?;
        if changed {
            self.notify_process_commit(snapshot.clone(), ProcessJournalKind::Submitted, None)
                .await?;
        }
        Ok(snapshot)
    }
}

#[async_trait]
impl<F> ProcessTransitionPort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn claim_next_processes(
        &self,
        request: ClaimProcessesRequest,
    ) -> Result<Vec<ClaimedProcess>, Self::Error> {
        let claimed = {
            let _guard = self.mutation_lock.lock().await;
            self.mutate(|state| {
                let mut claimed = Vec::new();
                let process_ids = state.claimable_process_ids(request.scope_filter.as_ref());
                for process_id in process_ids.into_iter().take(request.max_processes) {
                    let now = Utc::now();
                    let cursor = state.next_cursor();
                    let Some(snapshot) = state.processes.get_mut(&process_id) else {
                        continue;
                    };
                    snapshot.status = ProcessLifecycleStatus::Running;
                    snapshot.suspension = None;
                    snapshot.lease = Some(ProcessLeaseSnapshot {
                        worker_id: request.worker_id.clone(),
                        lease_token: ProcessLeaseToken::from_trusted(
                            ProcessId::new().as_uuid().to_string(),
                        ),
                        lease_expires_at: chrono::Duration::from_std(self.lease_duration)
                            .ok()
                            .map(|duration| now + duration),
                        last_heartbeat_at: Some(now),
                        claim_count: snapshot
                            .lease
                            .as_ref()
                            .map(|lease| lease.claim_count)
                            .unwrap_or(0)
                            .saturating_add(1),
                    });
                    snapshot.journal_cursor = cursor;
                    let snapshot = snapshot.clone();
                    state.push_entry(ProcessJournalEntry::from_snapshot(
                        &snapshot,
                        cursor,
                        ProcessJournalKind::Claimed,
                    ));
                    let Some(lease) = snapshot.lease.clone() else {
                        continue;
                    };
                    claimed.push(ClaimedProcess {
                        state: snapshot,
                        worker_id: lease.worker_id,
                        lease_token: lease.lease_token,
                    });
                }
                Ok(claimed)
            })
            .await?
        };
        for process in &claimed {
            self.notify_process_commit(process.state.clone(), ProcessJournalKind::Claimed, None)
                .await?;
        }
        Ok(claimed)
    }

    async fn heartbeat_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<ProcessJournalCursor, Self::Error> {
        let snapshot = {
            let _guard = self.mutation_lock.lock().await;
            self.mutate(|state| {
                let now = Utc::now();
                let cursor = state.next_cursor();
                let snapshot = state.process_mut(request.process_id)?;
                ensure_transition(snapshot, ProcessLifecycleStatus::Running)?;
                ensure_lease(snapshot, &request.worker_id, &request.lease_token)?;
                if let Some(lease) = &mut snapshot.lease {
                    lease.last_heartbeat_at = Some(now);
                    lease.lease_expires_at = chrono::Duration::from_std(self.lease_duration)
                        .ok()
                        .map(|duration| now + duration);
                }
                snapshot.journal_cursor = cursor;
                let snapshot = snapshot.clone();
                state.push_entry(ProcessJournalEntry::from_snapshot(
                    &snapshot,
                    cursor,
                    ProcessJournalKind::Heartbeat,
                ));
                Ok(snapshot)
            })
            .await?
        };
        self.notify_process_commit(snapshot.clone(), ProcessJournalKind::Heartbeat, None)
            .await?;
        Ok(snapshot.journal_cursor)
    }

    async fn recover_expired_process_leases(
        &self,
        request: RecoverExpiredProcessLeasesRequest,
    ) -> Result<RecoverExpiredProcessLeasesResponse, Self::Error> {
        let response = {
            let _guard = self.mutation_lock.lock().await;
            self.mutate(|state| {
                let expired = state.expired_process_ids(request.scope_filter.as_ref(), request.now);
                let mut recovered = Vec::new();
                for process_id in expired {
                    let cursor = state.next_cursor();
                    let snapshot = state.process_mut(process_id)?;
                    snapshot.status = ProcessLifecycleStatus::RecoveryRequired;
                    snapshot.lease = None;
                    snapshot.journal_cursor = cursor;
                    let snapshot = snapshot.clone();
                    state.push_entry(ProcessJournalEntry::from_snapshot(
                        &snapshot,
                        cursor,
                        ProcessJournalKind::RecoveryRequired,
                    ));
                    recovered.push(snapshot);
                }
                Ok(RecoverExpiredProcessLeasesResponse { recovered })
            })
            .await?
        };
        for snapshot in &response.recovered {
            self.notify_process_commit(
                snapshot.clone(),
                ProcessJournalKind::RecoveryRequired,
                None,
            )
            .await?;
        }
        Ok(response)
    }

    async fn suspend_process(
        &self,
        request: SuspendProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            ProcessLeaseRequest {
                process_id: request.process_id,
                worker_id: request.worker_id,
                lease_token: request.lease_token,
            },
            ProcessTransitionMutation {
                status: ProcessLifecycleStatus::Suspended,
                kind: ProcessJournalKind::Suspended,
                suspension: Some(request.suspension),
                checkpoint_ref: Some(request.checkpoint_ref),
                failure: None,
                metadata: request.metadata,
            },
        )
        .await
    }

    async fn complete_process(
        &self,
        request: crate::ProcessStateTransitionRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            request.lease,
            ProcessTransitionMutation {
                metadata: request.metadata,
                ..ProcessTransitionMutation::new(
                    ProcessLifecycleStatus::Completed,
                    ProcessJournalKind::Completed,
                )
            },
        )
        .await
    }

    async fn cancel_process(
        &self,
        request: crate::ProcessStateTransitionRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            request.lease,
            ProcessTransitionMutation {
                metadata: request.metadata,
                ..ProcessTransitionMutation::new(
                    ProcessLifecycleStatus::Cancelled,
                    ProcessJournalKind::Cancelled,
                )
            },
        )
        .await
    }

    async fn fail_process(
        &self,
        request: FailProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            ProcessLeaseRequest {
                process_id: request.process_id,
                worker_id: request.worker_id,
                lease_token: request.lease_token,
            },
            ProcessTransitionMutation {
                failure: Some(request.failure),
                metadata: request.metadata,
                ..ProcessTransitionMutation::new(
                    ProcessLifecycleStatus::Failed,
                    ProcessJournalKind::Failed,
                )
            },
        )
        .await
    }

    async fn relinquish_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            request,
            ProcessTransitionMutation::new(
                ProcessLifecycleStatus::Queued,
                ProcessJournalKind::Heartbeat,
            ),
        )
        .await
    }
}

#[async_trait]
impl<F> ProcessControlPort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn resume_process(
        &self,
        request: ResumeProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(
            ProcessControlMutation {
                scope: request.scope,
                process_id: request.process_id,
                operation: "resume",
                operation_id: request.operation_id,
                expected_cursor: request.expected_cursor,
                reason: None,
                checkpoint_ref: request.checkpoint_ref,
                metadata: request.metadata,
            },
            |snapshot| {
                ensure_transition(snapshot, ProcessLifecycleStatus::Queued)?;
                Ok(Some((
                    ProcessLifecycleStatus::Queued,
                    ProcessJournalKind::Resumed,
                )))
            },
        )
        .await
    }

    async fn stop_process(
        &self,
        request: StopProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(
            ProcessControlMutation {
                scope: request.scope,
                process_id: request.process_id,
                operation: "stop",
                operation_id: request.operation_id,
                expected_cursor: None,
                reason: request.reason,
                checkpoint_ref: None,
                metadata: None,
            },
            |snapshot| {
                Ok((!snapshot.status.is_terminal())
                    .then_some((ProcessLifecycleStatus::Stopped, ProcessJournalKind::Stopped)))
            },
        )
        .await
    }

    async fn request_cancel_process(
        &self,
        request: CancelProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(
            ProcessControlMutation {
                scope: request.scope,
                process_id: request.process_id,
                operation: "cancel",
                operation_id: request.operation_id,
                expected_cursor: None,
                reason: request.reason,
                checkpoint_ref: None,
                metadata: None,
            },
            |snapshot| {
                let transition = match snapshot.status {
                    status if status.is_terminal() => None,
                    ProcessLifecycleStatus::Running | ProcessLifecycleStatus::CancelRequested => {
                        Some((
                            ProcessLifecycleStatus::CancelRequested,
                            ProcessJournalKind::CancelRequested,
                        ))
                    }
                    _ => Some((
                        ProcessLifecycleStatus::Cancelled,
                        ProcessJournalKind::Cancelled,
                    )),
                };
                Ok(transition)
            },
        )
        .await
    }

    async fn kill_process(
        &self,
        request: KillProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(
            ProcessControlMutation {
                scope: request.scope,
                process_id: request.process_id,
                operation: "kill",
                operation_id: request.operation_id,
                expected_cursor: None,
                reason: request.reason,
                checkpoint_ref: None,
                metadata: None,
            },
            |snapshot| {
                Ok((!snapshot.status.is_terminal())
                    .then_some((ProcessLifecycleStatus::Killed, ProcessJournalKind::Killed)))
            },
        )
        .await
    }
}

impl<F> ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    async fn control_transition(
        &self,
        mutation: ProcessControlMutation,
        decide: impl Fn(
            &JournaledProcessSnapshot,
        ) -> Result<
            Option<(ProcessLifecycleStatus, ProcessJournalKind)>,
            ProcessJournalStoreError,
        >,
    ) -> Result<ProcessControlResult, ProcessJournalStoreError> {
        let reason = mutation.reason.clone();
        let (result, committed_kind) = {
            let _guard = self.mutation_lock.lock().await;
            self.mutate(|state| {
                let replay_key = mutation.operation_id.as_ref().map(|id| {
                    format!(
                        "{}:{}:{}",
                        mutation.operation,
                        mutation.process_id,
                        id.as_str()
                    )
                });
                if let Some(result) = replay_key
                    .as_ref()
                    .and_then(|key| state.control_idempotency.get(key))
                {
                    if !process_scope_visible(&result.state.scope, &mutation.scope) {
                        return Err(ProcessJournalStoreError::UnknownProcess {
                            process_id: mutation.process_id,
                        });
                    }
                    return Ok((result.clone(), None));
                }
                let snapshot = state.process_mut(mutation.process_id)?;
                if !process_scope_visible(&snapshot.scope, &mutation.scope) {
                    return Err(ProcessJournalStoreError::UnknownProcess {
                        process_id: mutation.process_id,
                    });
                }
                if let Some(expected) = mutation.expected_cursor
                    && expected != snapshot.journal_cursor
                {
                    return Err(ProcessJournalStoreError::StaleSnapshot {
                        process_id: mutation.process_id,
                        expected,
                        actual: snapshot.journal_cursor,
                    });
                }
                let already_terminal = snapshot.status.is_terminal();
                let Some((status, kind)) = decide(snapshot)? else {
                    let result = ProcessControlResult {
                        state: snapshot.clone(),
                        changed: false,
                        already_terminal,
                    };
                    state.remember_control_result(replay_key, result.clone());
                    return Ok((result, None));
                };
                if status == snapshot.status {
                    let result = ProcessControlResult {
                        state: snapshot.clone(),
                        changed: false,
                        already_terminal,
                    };
                    state.remember_control_result(replay_key, result.clone());
                    return Ok((result, None));
                }
                let cursor = state.next_cursor();
                let snapshot = state.process_mut(mutation.process_id)?;
                snapshot.status = status;
                snapshot.suspension = None;
                if mutation.checkpoint_ref.is_some() {
                    snapshot.checkpoint_ref = mutation.checkpoint_ref.clone();
                }
                if let Some(metadata) = mutation.metadata.clone() {
                    snapshot.metadata = metadata;
                }
                snapshot.failure = None;
                if status != ProcessLifecycleStatus::CancelRequested {
                    snapshot.lease = None;
                }
                snapshot.journal_cursor = cursor;
                let snapshot = snapshot.clone();
                let mut entry = ProcessJournalEntry::from_snapshot(&snapshot, cursor, kind);
                entry.sanitized_reason = mutation.reason.clone();
                state.push_entry(entry);
                let result = ProcessControlResult {
                    state: snapshot,
                    changed: true,
                    already_terminal,
                };
                state.remember_control_result(replay_key, result.clone());
                Ok((result, Some(kind)))
            })
            .await?
        };
        if let Some(kind) = committed_kind {
            self.notify_process_commit(result.state.clone(), kind, reason)
                .await?;
        }
        Ok(result)
    }

    async fn leased_transition(
        &self,
        request: ProcessLeaseRequest,
        mutation: ProcessTransitionMutation,
    ) -> Result<JournaledProcessSnapshot, ProcessJournalStoreError> {
        let kind = mutation.kind;
        let snapshot = {
            let _guard = self.mutation_lock.lock().await;
            self.mutate(|state| {
                let cursor = state.next_cursor();
                let snapshot = state.process_mut(request.process_id)?;
                ensure_lease(snapshot, &request.worker_id, &request.lease_token)?;
                ensure_transition(snapshot, mutation.status)?;
                snapshot.status = mutation.status;
                snapshot.suspension = mutation.suspension.clone();
                if mutation.checkpoint_ref.is_some() {
                    snapshot.checkpoint_ref = mutation.checkpoint_ref.clone();
                }
                snapshot.failure = mutation.failure.clone();
                if let Some(metadata) = mutation.metadata.clone() {
                    snapshot.metadata = metadata;
                }
                if mutation.status != ProcessLifecycleStatus::Running {
                    snapshot.lease = None;
                }
                snapshot.journal_cursor = cursor;
                let snapshot = snapshot.clone();
                state.push_entry(ProcessJournalEntry::from_snapshot(&snapshot, cursor, kind));
                Ok(snapshot)
            })
            .await?
        };
        self.notify_process_commit(snapshot.clone(), kind, None)
            .await?;
        Ok(snapshot)
    }
}

impl<F> ProcessJournalObserverRegistry for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    fn subscribe_process_observer(
        &self,
        observer: Arc<dyn ProcessJournalCommitObserver>,
    ) -> Result<(), String> {
        let mut observers = self
            .observers
            .lock()
            .map_err(|_| "process journal observer registry mutex poisoned".to_string())?;
        observers.push(observer);
        Ok(())
    }
}

#[async_trait]
impl<F> ProcessTreePort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn child_processes(
        &self,
        scope: &ResourceScope,
        parent_process_id: ProcessId,
    ) -> Result<Vec<JournaledProcessSnapshot>, Self::Error> {
        let (state, _) = self.load_state().await?;
        let Some(parent) = state.processes.get(&parent_process_id) else {
            return Ok(Vec::new());
        };
        if !same_scope_owner(&parent.scope, scope) {
            return Ok(Vec::new());
        }
        let mut children = state
            .processes
            .values()
            .filter(|snapshot| snapshot.parent_process_id == Some(parent_process_id))
            .filter(|snapshot| same_lineage_scope(&snapshot.scope, scope))
            .cloned()
            .collect::<Vec<_>>();
        children.sort_by_key(|snapshot| snapshot.created_at);
        Ok(children)
    }

    async fn reserve_process_tree(
        &self,
        request: ReserveProcessTreeRequest,
    ) -> Result<ProcessTreeReservation, Self::Error> {
        if request.delta == 0 {
            return Err(ProcessJournalStoreError::InvalidRequest(
                "reservation delta must be greater than zero".to_string(),
            ));
        }
        let _guard = self.mutation_lock.lock().await;
        self.mutate(|state| {
            validate_tree_root(state, &request.scope, request.root_process_id)?;
            let current = state
                .tree_reservations
                .get(&request.root_process_id)
                .map(|reservation| reservation.descendant_count)
                .unwrap_or(0);
            let next = current.checked_add(u64::from(request.delta)).ok_or(
                ProcessJournalStoreError::ProcessTreeCapacityExceeded { cap: request.cap },
            )?;
            if next > u64::from(request.cap) {
                return Err(ProcessJournalStoreError::ProcessTreeCapacityExceeded {
                    cap: request.cap,
                });
            }
            let released_processes = state
                .tree_reservations
                .get(&request.root_process_id)
                .map(|reservation| reservation.released_processes.clone())
                .unwrap_or_default();
            let reservation = ProcessTreeReservation {
                root_process_id: request.root_process_id,
                descendant_count: next,
                released_processes,
            };
            state
                .tree_reservations
                .insert(request.root_process_id, reservation.clone());
            Ok(reservation)
        })
        .await
    }

    async fn release_process_tree(
        &self,
        request: ReleaseProcessTreeRequest,
    ) -> Result<(), Self::Error> {
        let _guard = self.mutation_lock.lock().await;
        self.mutate(|state| {
            validate_tree_root(state, &request.scope, request.root_process_id)?;
            let Some(reservation) = state.tree_reservations.get_mut(&request.root_process_id)
            else {
                return Ok(());
            };
            if !reservation
                .released_processes
                .insert(request.idempotency_process_id)
            {
                return Ok(());
            }
            if reservation.descendant_count < u64::from(request.delta) {
                reservation
                    .released_processes
                    .remove(&request.idempotency_process_id);
                return Err(ProcessJournalStoreError::InvalidRequest(
                    "release delta exceeds current reservation count".to_string(),
                ));
            }
            reservation.descendant_count -= u64::from(request.delta);
            if reservation.descendant_count == 0 {
                state.tree_reservations.remove(&request.root_process_id);
            }
            Ok(())
        })
        .await
    }

    async fn prune_released_process(
        &self,
        request: PruneReleasedProcessRequest,
    ) -> Result<(), Self::Error> {
        let _guard = self.mutation_lock.lock().await;
        self.mutate(|state| {
            if !state.processes.contains_key(&request.root_process_id) {
                return Ok(());
            }
            validate_tree_root(state, &request.scope, request.root_process_id)?;
            if let Some(reservation) = state.tree_reservations.get_mut(&request.root_process_id) {
                reservation.released_processes.remove(&request.process_id);
            }
            Ok(())
        })
        .await
    }
}

struct ProcessTransitionMutation {
    status: ProcessLifecycleStatus,
    kind: ProcessJournalKind,
    suspension: Option<ProcessSuspension>,
    checkpoint_ref: Option<crate::ProcessCheckpointRef>,
    failure: Option<ironclaw_host_api::SanitizedFailure>,
    metadata: Option<serde_json::Value>,
}

struct ProcessControlMutation {
    scope: ResourceScope,
    process_id: ProcessId,
    operation: &'static str,
    operation_id: Option<ProcessOperationId>,
    expected_cursor: Option<ProcessJournalCursor>,
    reason: Option<String>,
    checkpoint_ref: Option<crate::ProcessCheckpointRef>,
    metadata: Option<Value>,
}

impl ProcessTransitionMutation {
    fn new(status: ProcessLifecycleStatus, kind: ProcessJournalKind) -> Self {
        Self {
            status,
            kind,
            suspension: None,
            checkpoint_ref: None,
            failure: None,
            metadata: None,
        }
    }
}

#[async_trait]
impl<F> ProcessJournalSource for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn get_process_snapshot(
        &self,
        request: GetProcessSnapshotRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        let (state, _) = self.load_state().await?;
        let snapshot = state
            .processes
            .get(&request.process_id)
            .filter(|snapshot| process_scope_visible(&snapshot.scope, &request.scope))
            .cloned()
            .ok_or(ProcessJournalStoreError::UnknownProcess {
                process_id: request.process_id,
            })?;
        Ok(snapshot)
    }

    async fn read_process_journal_after(
        &self,
        scope: &ResourceScope,
        owner_user_id: Option<&ironclaw_host_api::UserId>,
        after: Option<ProcessJournalCursor>,
        limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error> {
        let (state, _) = self.load_state().await?;
        Ok(state.page_after(after, limit, |entry| {
            same_scope_owner(&entry.scope, scope)
                && owner_user_id.is_none_or(|owner| entry.owner_user_id.as_ref() == Some(owner))
        }))
    }

    async fn read_process_journal_log_after(
        &self,
        after: Option<ProcessJournalCursor>,
        limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error> {
        let (state, _) = self.load_state().await?;
        Ok(state.page_after(after, limit, |_| true))
    }
}

#[async_trait]
impl<F> ProcessLifecycleLookupSource for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn process_lifecycle_states(
        &self,
        request: ProcessLifecycleLookupBatchRequest,
    ) -> Vec<Result<ProcessLifecycleLookupResult, Self::Error>> {
        let state = match self.load_state().await {
            Ok((state, _)) => state,
            Err(error) => {
                let message = error.to_string();
                return request
                    .processes
                    .into_iter()
                    .map(|_| Err(ProcessJournalStoreError::Deserialization(message.clone())))
                    .collect();
            }
        };
        request
            .processes
            .into_iter()
            .map(|lookup| {
                let result = state
                    .processes
                    .get(&lookup.process_id)
                    .filter(|snapshot| snapshot.scope.tenant_id == lookup.tenant_id)
                    .map(|snapshot| ProcessLifecycleLookupResult::Found {
                        status: snapshot.status,
                        suspension: snapshot.suspension.clone(),
                    })
                    .unwrap_or(ProcessLifecycleLookupResult::Missing);
                Ok(result)
            })
            .collect()
    }
}

#[async_trait]
impl<F> ProcessGateQuerySource for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn query_process_gates(
        &self,
        request: ProcessGateQuery,
    ) -> Result<Vec<ProcessGateRecord>, Self::Error> {
        let (state, _) = self.load_state().await?;
        let mut records = state
            .processes
            .values()
            .filter(|snapshot| process_gate_snapshot_matches(snapshot, &request))
            .filter_map(|snapshot| {
                Some(ProcessGateRecord {
                    process_id: snapshot.process_id,
                    scope: snapshot.scope.clone(),
                    owner_user_id: snapshot.owner_user_id.clone(),
                    suspension: snapshot.suspension.clone()?,
                    resume_source_ref: snapshot
                        .metadata
                        .pointer("/agent_turn/source_binding_ref")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    reply_target_ref: snapshot
                        .metadata
                        .pointer("/agent_turn/reply_target_binding_ref")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    historical: false,
                })
            })
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.process_id.as_uuid());
        Ok(records)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessJournalMaterializedState {
    next_cursor: u64,
    processes: HashMap<ProcessId, JournaledProcessSnapshot>,
    journal: Vec<ProcessJournalEntry>,
    #[serde(default)]
    control_idempotency: HashMap<String, ProcessControlResult>,
    #[serde(default)]
    control_idempotency_order: VecDeque<String>,
    #[serde(default)]
    submission_idempotency: HashMap<String, JournaledProcessSnapshot>,
    #[serde(default)]
    submission_idempotency_order: VecDeque<String>,
    #[serde(default)]
    tree_reservations: HashMap<ProcessId, ProcessTreeReservation>,
}

impl Default for ProcessJournalMaterializedState {
    fn default() -> Self {
        Self {
            next_cursor: 1,
            processes: HashMap::new(),
            journal: Vec::new(),
            control_idempotency: HashMap::new(),
            control_idempotency_order: VecDeque::new(),
            submission_idempotency: HashMap::new(),
            submission_idempotency_order: VecDeque::new(),
            tree_reservations: HashMap::new(),
        }
    }
}

impl ProcessJournalMaterializedState {
    fn next_cursor(&mut self) -> ProcessJournalCursor {
        let cursor = ProcessJournalCursor(self.next_cursor);
        self.next_cursor = self.next_cursor.saturating_add(1);
        cursor
    }

    fn push_entry(&mut self, entry: ProcessJournalEntry) {
        self.journal.push(entry);
    }

    fn remember_control_result(&mut self, key: Option<String>, result: ProcessControlResult) {
        let Some(key) = key else {
            return;
        };
        if let Some(existing) = self.control_idempotency.get_mut(&key) {
            *existing = result;
            return;
        }
        while self.control_idempotency.len() >= MAX_CONTROL_IDEMPOTENCY_RECORDS {
            let Some(oldest) = self.control_idempotency_order.pop_front() else {
                self.control_idempotency.clear();
                break;
            };
            self.control_idempotency.remove(&oldest);
        }
        self.control_idempotency_order.push_back(key.clone());
        self.control_idempotency.insert(key, result);
    }

    fn remember_submission_result(
        &mut self,
        key: Option<String>,
        snapshot: JournaledProcessSnapshot,
    ) {
        let Some(key) = key else {
            return;
        };
        if let Some(existing) = self.submission_idempotency.get_mut(&key) {
            *existing = snapshot;
            return;
        }
        while self.submission_idempotency.len() >= MAX_CONTROL_IDEMPOTENCY_RECORDS {
            let Some(oldest) = self.submission_idempotency_order.pop_front() else {
                self.submission_idempotency.clear();
                break;
            };
            self.submission_idempotency.remove(&oldest);
        }
        self.submission_idempotency_order.push_back(key.clone());
        self.submission_idempotency.insert(key, snapshot);
    }

    fn process_mut(
        &mut self,
        process_id: ProcessId,
    ) -> Result<&mut JournaledProcessSnapshot, ProcessJournalStoreError> {
        self.processes
            .get_mut(&process_id)
            .ok_or(ProcessJournalStoreError::UnknownProcess { process_id })
    }

    fn claimable_process_ids(&self, scope_filter: Option<&ResourceScope>) -> Vec<ProcessId> {
        let mut ids = self
            .processes
            .values()
            .filter(|snapshot| snapshot.status == ProcessLifecycleStatus::Queued)
            .filter(|snapshot| {
                scope_filter.is_none_or(|scope| same_scope_owner(&snapshot.scope, scope))
            })
            .map(|snapshot| (snapshot.created_at, snapshot.process_id))
            .collect::<Vec<_>>();
        ids.sort_by_key(|(created_at, process_id)| (*created_at, process_id.as_uuid()));
        ids.into_iter().map(|(_, process_id)| process_id).collect()
    }

    fn expired_process_ids(
        &self,
        scope_filter: Option<&ResourceScope>,
        now: ironclaw_host_api::Timestamp,
    ) -> Vec<ProcessId> {
        self.processes
            .values()
            .filter(|snapshot| {
                matches!(
                    snapshot.status,
                    ProcessLifecycleStatus::Running | ProcessLifecycleStatus::CancelRequested
                )
            })
            .filter(|snapshot| {
                scope_filter.is_none_or(|scope| same_scope_owner(&snapshot.scope, scope))
            })
            .filter(|snapshot| {
                snapshot
                    .lease
                    .as_ref()
                    .and_then(|lease| lease.lease_expires_at)
                    .is_some_and(|expires_at| expires_at <= now)
            })
            .map(|snapshot| snapshot.process_id)
            .collect()
    }

    fn page_after(
        &self,
        after: Option<ProcessJournalCursor>,
        limit: usize,
        include: impl Fn(&ProcessJournalEntry) -> bool,
    ) -> ProcessJournalPage {
        let after = after.map(|cursor| cursor.0).unwrap_or(0);
        let mut entries = self
            .journal
            .iter()
            .filter(|entry| entry.cursor.0 > after)
            .filter(|entry| include(entry))
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let truncated = entries.len() > limit;
        if truncated {
            entries.truncate(limit);
        }
        let next_cursor = entries
            .last()
            .map(|entry| entry.cursor)
            .unwrap_or(ProcessJournalCursor(after));
        ProcessJournalPage {
            entries,
            next_cursor,
            truncated,
            rebase_required: None,
        }
    }
}

impl ProcessJournalEntry {
    fn from_snapshot(
        snapshot: &JournaledProcessSnapshot,
        cursor: ProcessJournalCursor,
        kind: ProcessJournalKind,
    ) -> Self {
        Self {
            cursor,
            process_id: snapshot.process_id,
            process_kind: snapshot.process_kind.clone(),
            scope: snapshot.scope.clone(),
            occurred_at: Some(Utc::now()),
            owner_user_id: snapshot.owner_user_id.clone(),
            status: snapshot.status,
            kind,
            suspension: snapshot.suspension.clone(),
            sanitized_reason: None,
            retryable: None,
            detail: None,
            metadata: snapshot.metadata.clone(),
        }
    }
}

fn ensure_transition(
    snapshot: &JournaledProcessSnapshot,
    to: ProcessLifecycleStatus,
) -> Result<(), ProcessJournalStoreError> {
    let valid = match (snapshot.status, to) {
        (ProcessLifecycleStatus::Queued, ProcessLifecycleStatus::Running)
        | (ProcessLifecycleStatus::Suspended, ProcessLifecycleStatus::Queued)
        | (ProcessLifecycleStatus::Running, ProcessLifecycleStatus::Running)
        | (ProcessLifecycleStatus::Running, ProcessLifecycleStatus::Suspended)
        | (ProcessLifecycleStatus::Running, ProcessLifecycleStatus::Completed)
        | (ProcessLifecycleStatus::Running, ProcessLifecycleStatus::Cancelled)
        | (ProcessLifecycleStatus::Running, ProcessLifecycleStatus::Failed)
        | (ProcessLifecycleStatus::Running, ProcessLifecycleStatus::Queued)
        | (ProcessLifecycleStatus::CancelRequested, ProcessLifecycleStatus::Cancelled) => true,
        (from, _) if from == to => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ProcessJournalStoreError::InvalidTransition {
            process_id: snapshot.process_id,
            from: snapshot.status,
            to,
        })
    }
}

fn ensure_lease(
    snapshot: &JournaledProcessSnapshot,
    worker_id: &ProcessWorkerId,
    lease_token: &ProcessLeaseToken,
) -> Result<(), ProcessJournalStoreError> {
    let Some(lease) = &snapshot.lease else {
        return Err(ProcessJournalStoreError::InvalidLease {
            process_id: snapshot.process_id,
        });
    };
    if &lease.worker_id == worker_id && &lease.lease_token == lease_token {
        Ok(())
    } else {
        Err(ProcessJournalStoreError::InvalidLease {
            process_id: snapshot.process_id,
        })
    }
}

fn process_gate_snapshot_matches(
    snapshot: &JournaledProcessSnapshot,
    request: &ProcessGateQuery,
) -> bool {
    let Some(suspension) = &snapshot.suspension else {
        return false;
    };
    snapshot.status == ProcessLifecycleStatus::Suspended
        && suspension.kind == request.gate_kind
        && same_scope_owner(&snapshot.scope, &request.scope)
        && request
            .gate_ref
            .as_ref()
            .is_none_or(|gate_ref| suspension.gate_ref.as_ref() == Some(gate_ref))
        && request.owner_user_id.as_ref().is_none_or(|owner| {
            match request
                .owner_match
                .unwrap_or(ProcessGateOwnerMatch::Explicit)
            {
                ProcessGateOwnerMatch::Explicit | ProcessGateOwnerMatch::ExplicitOrActor => {
                    snapshot.owner_user_id.as_ref() == Some(owner)
                }
            }
        })
}

fn process_scope_visible(stored: &ResourceScope, requested: &ResourceScope) -> bool {
    *requested == ResourceScope::system() || same_scope_owner(stored, requested)
}

fn same_lineage_scope(left: &ResourceScope, right: &ResourceScope) -> bool {
    left.tenant_id == right.tenant_id
        && left.user_id == right.user_id
        && left.agent_id == right.agent_id
        && left.project_id == right.project_id
        && left.mission_id == right.mission_id
}

fn validate_tree_root(
    state: &ProcessJournalMaterializedState,
    scope: &ResourceScope,
    root_process_id: ProcessId,
) -> Result<(), ProcessJournalStoreError> {
    let root =
        state
            .processes
            .get(&root_process_id)
            .ok_or(ProcessJournalStoreError::UnknownProcess {
                process_id: root_process_id,
            })?;
    if !same_lineage_scope(&root.scope, scope) {
        return Err(ProcessJournalStoreError::UnauthorizedScope);
    }
    if root.root_process_id.unwrap_or(root.process_id) != root.process_id {
        return Err(ProcessJournalStoreError::InvalidRequest(
            "root_process_id must identify the process tree root".to_string(),
        ));
    }
    Ok(())
}

fn journal_state_path() -> Result<ScopedPath, ProcessJournalStoreError> {
    ScopedPath::new(JOURNAL_STATE_PATH)
        .map_err(|error| ProcessJournalStoreError::InvalidPath(invalid_path(error).to_string()))
}

fn is_unsupported(error: &FilesystemError) -> bool {
    matches!(error, FilesystemError::Unsupported { .. })
}
