use std::{collections::HashMap, sync::Arc, time::Duration};

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
    ClaimProcessRequest, ClaimProcessesRequest, ClaimedProcess, FailProcessRequest,
    GetProcessSnapshotRequest, JournaledProcessSnapshot, ProcessGateOwnerMatch, ProcessGateQuery,
    ProcessGateQuerySource, ProcessGateRecord, ProcessJournalCursor, ProcessJournalEntry,
    ProcessJournalKind, ProcessJournalPage, ProcessJournalSource, ProcessLeaseRequest,
    ProcessLeaseSnapshot, ProcessLeaseToken, ProcessLifecycleLookupBatchRequest,
    ProcessLifecycleLookupResult, ProcessLifecycleLookupSource, ProcessLifecycleStatus,
    ProcessSubmissionPort, ProcessSuspension, ProcessTransitionPort, ProcessWorkerId,
    RecoverExpiredProcessLeasesRequest, RecoverExpiredProcessLeasesResponse, SubmitProcessRequest,
    SuspendProcessRequest,
};
use crate::types::{invalid_path, same_scope_owner};

const JOURNAL_STATE_PATH: &str = "/processes/journal/state.json";
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(90);
const MAX_CAS_RETRIES: usize = 5;

#[derive(Debug, Error)]
pub enum ProcessJournalStoreError {
    #[error("unknown process {process_id}")]
    UnknownProcess { process_id: ProcessId },
    #[error("process {process_id} already exists")]
    ProcessAlreadyExists { process_id: ProcessId },
    #[error("process {process_id} cannot transition from {from:?} to {to:?}")]
    InvalidTransition {
        process_id: ProcessId,
        from: ProcessLifecycleStatus,
        to: ProcessLifecycleStatus,
    },
    #[error("process {process_id} lease is invalid")]
    InvalidLease { process_id: ProcessId },
    #[error("invalid storage path: {0}")]
    InvalidPath(String),
    #[error("filesystem error: {0}")]
    Filesystem(#[from] FilesystemError),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("deserialization error: {0}")]
    Deserialization(String),
}

pub struct ProcessJournalStore<F>
where
    F: RootFilesystem,
{
    filesystem: Arc<ScopedFilesystem<F>>,
    mutation_lock: Mutex<()>,
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
    ) -> Result<JournaledProcessSnapshot, ProcessJournalStoreError> {
        let _guard = self.mutation_lock.lock().await;
        self.mutate(|state| {
            if state.processes.contains_key(&request.process_id) {
                return Err(ProcessJournalStoreError::ProcessAlreadyExists {
                    process_id: request.process_id,
                });
            }
            let cursor = state.next_cursor();
            let snapshot = JournaledProcessSnapshot {
                process_id: request.process_id,
                process_kind: request.process_kind.clone(),
                scope: request.scope.clone(),
                status: ProcessLifecycleStatus::Queued,
                suspension: None,
                checkpoint_ref: None,
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
            Ok(snapshot)
        })
        .await
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
        self.submit_process_inner(request).await
    }
}

#[async_trait]
impl<F> ProcessTransitionPort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn claim_next_process(
        &self,
        request: ClaimProcessRequest,
    ) -> Result<Option<ClaimedProcess>, Self::Error> {
        let mut claimed = self
            .claim_next_processes(ClaimProcessesRequest {
                worker_id: request.worker_id,
                scope_filter: request.scope_filter,
                max_processes: 1,
            })
            .await?;
        Ok(claimed.pop())
    }

    async fn claim_next_processes(
        &self,
        request: ClaimProcessesRequest,
    ) -> Result<Vec<ClaimedProcess>, Self::Error> {
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
        .await
    }

    async fn heartbeat_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<ProcessJournalCursor, Self::Error> {
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
            let entry_snapshot = snapshot.clone();
            state.push_entry(ProcessJournalEntry::from_snapshot(
                &entry_snapshot,
                cursor,
                ProcessJournalKind::Heartbeat,
            ));
            Ok(cursor)
        })
        .await
    }

    async fn recover_expired_process_leases(
        &self,
        request: RecoverExpiredProcessLeasesRequest,
    ) -> Result<RecoverExpiredProcessLeasesResponse, Self::Error> {
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
        .await
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
            ProcessLifecycleStatus::Suspended,
            ProcessJournalKind::Suspended,
            Some(request.suspension),
            Some(request.checkpoint_ref),
            None,
        )
        .await
    }

    async fn complete_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            request,
            ProcessLifecycleStatus::Completed,
            ProcessJournalKind::Completed,
            None,
            None,
            None,
        )
        .await
    }

    async fn cancel_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            request,
            ProcessLifecycleStatus::Cancelled,
            ProcessJournalKind::Cancelled,
            None,
            None,
            None,
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
            ProcessLifecycleStatus::Failed,
            ProcessJournalKind::Failed,
            None,
            None,
            Some(request.failure),
        )
        .await
    }

    async fn relinquish_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.leased_transition(
            request,
            ProcessLifecycleStatus::Queued,
            ProcessJournalKind::Heartbeat,
            None,
            None,
            None,
        )
        .await
    }
}

impl<F> ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    async fn leased_transition(
        &self,
        request: ProcessLeaseRequest,
        status: ProcessLifecycleStatus,
        kind: ProcessJournalKind,
        suspension: Option<ProcessSuspension>,
        checkpoint_ref: Option<crate::ProcessCheckpointRef>,
        failure: Option<ironclaw_host_api::SanitizedFailure>,
    ) -> Result<JournaledProcessSnapshot, ProcessJournalStoreError> {
        let _guard = self.mutation_lock.lock().await;
        self.mutate(|state| {
            let cursor = state.next_cursor();
            let snapshot = state.process_mut(request.process_id)?;
            ensure_lease(snapshot, &request.worker_id, &request.lease_token)?;
            ensure_transition(snapshot, status)?;
            snapshot.status = status;
            snapshot.suspension = suspension.clone();
            if checkpoint_ref.is_some() {
                snapshot.checkpoint_ref = checkpoint_ref.clone();
            }
            snapshot.failure = failure.clone();
            if status != ProcessLifecycleStatus::Running {
                snapshot.lease = None;
            }
            snapshot.journal_cursor = cursor;
            let snapshot = snapshot.clone();
            state.push_entry(ProcessJournalEntry::from_snapshot(&snapshot, cursor, kind));
            Ok(snapshot)
        })
        .await
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
}

impl Default for ProcessJournalMaterializedState {
    fn default() -> Self {
        Self {
            next_cursor: 1,
            processes: HashMap::new(),
            journal: Vec::new(),
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

fn journal_state_path() -> Result<ScopedPath, ProcessJournalStoreError> {
    ScopedPath::new(JOURNAL_STATE_PATH)
        .map_err(|error| ProcessJournalStoreError::InvalidPath(invalid_path(error).to_string()))
}

fn is_unsupported(error: &FilesystemError) -> bool {
    matches!(error, FilesystemError::Unsupported { .. })
}
