use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use ironclaw_filesystem::{
    FilesystemError, FilesystemOperation, RootFilesystem, ScopedFilesystem, SeqNo,
};
use ironclaw_host_api::{ProcessId, ResourceScope, ScopedPath};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::ProcessRuntimePort;
use crate::journal::{
    CancelProcessRequest, ClaimProcessesRequest, ClaimedProcess, CloseProcessDependencyRequest,
    FailProcessRequest, GetProcessCheckpointRequest, GetProcessSnapshotRequest,
    JournaledProcessSnapshot, KillProcessRequest, OpenProcessDependencyRequest,
    ProcessCheckpointPort, ProcessCheckpointRecord, ProcessConcurrencyLimits, ProcessControlPort,
    ProcessControlResult, ProcessDependencyPort, ProcessDependencyQuery, ProcessDependencyRecord,
    ProcessGateOwnerMatch, ProcessGateQuery, ProcessGateQuerySource, ProcessGateRecord,
    ProcessJournalCommit, ProcessJournalCommitObserver, ProcessJournalCursor, ProcessJournalEntry,
    ProcessJournalKind, ProcessJournalObserverRegistry, ProcessJournalPage, ProcessJournalSource,
    ProcessLeaseRequest, ProcessLeaseToken, ProcessLifecycleLookupBatchRequest,
    ProcessLifecycleLookupResult, ProcessLifecycleLookupSource, ProcessLifecycleStatus,
    ProcessOperationId, ProcessSubmissionPort, ProcessSuspension, ProcessTransitionPort,
    ProcessTreePort, ProcessTreeReservation, ProcessWorkerId, PruneReleasedProcessRequest,
    RecordProcessCheckpointRequest, RecoverExpiredProcessLeasesRequest,
    RecoverExpiredProcessLeasesResponse, ReleaseProcessTreeRequest, ReserveProcessTreeRequest,
    ResumeProcessRequest, SettleProcessDependencyRequest, StopProcessRequest, SubmitProcessRequest,
    SuspendProcessRequest,
};
use crate::types::{invalid_path, same_scope_owner};

mod state;
use state::ProcessJournalMaterializedState;

const JOURNAL_LOG_PATH: &str = "/processes/journal/records";
const LEGACY_JOURNAL_STATE_PATH: &str = "/processes/journal/state.json";
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(90);
const JOURNAL_READ_BATCH: usize = 1024;
const MAX_RECENT_OUTCOMES: usize = 4096;
const MAX_APPEND_BUSY_RETRIES: usize = 64;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "schema", content = "command", rename_all = "snake_case")]
enum StoredProcessJournalRecord {
    V1(StoredProcessCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StoredProcessCommand {
    ImportLegacyState(Box<ProcessJournalMaterializedState>),
    Submit(SubmitProcessRequest),
    Claim {
        request: ClaimProcessesRequest,
        now: ironclaw_host_api::Timestamp,
        lease_duration_millis: u64,
        lease_nonce: ProcessId,
        limits: ProcessConcurrencyLimits,
    },
    Heartbeat {
        request: ProcessLeaseRequest,
        now: ironclaw_host_api::Timestamp,
        lease_duration_millis: u64,
    },
    RecoverExpired(RecoverExpiredProcessLeasesRequest),
    LeasedTransition {
        request: ProcessLeaseRequest,
        mutation: ProcessTransitionMutation,
    },
    Control(ProcessControlMutation),
    ReserveTree(ReserveProcessTreeRequest),
    ReleaseTree(ReleaseProcessTreeRequest),
    PruneTree(PruneReleasedProcessRequest),
    OpenDependency(OpenProcessDependencyRequest),
    SettleDependency(SettleProcessDependencyRequest),
    ConsumeDependency(CloseProcessDependencyRequest),
    AbandonDependency(CloseProcessDependencyRequest),
    RecordCheckpoint(RecordProcessCheckpointRequest),
}

#[derive(Debug)]
enum StoredCommandOutcome {
    Imported,
    Submitted(JournaledProcessSnapshot, bool),
    Claimed(Vec<ClaimedProcess>),
    Heartbeat(JournaledProcessSnapshot),
    Recovered(RecoverExpiredProcessLeasesResponse),
    Transitioned(JournaledProcessSnapshot),
    Controlled(ProcessControlResult, Option<ProcessJournalKind>),
    TreeReserved(ProcessTreeReservation),
    TreeReleased,
    TreePruned,
    Dependency(Option<ProcessDependencyRecord>),
    Checkpointed(ProcessCheckpointRecord),
}

struct CachedProjection {
    state: ProcessJournalMaterializedState,
    applied_seq: SeqNo,
    outcomes: HashMap<u64, Result<StoredCommandOutcome, ProcessJournalStoreError>>,
    outcome_order: VecDeque<u64>,
}

impl Default for CachedProjection {
    fn default() -> Self {
        Self {
            state: ProcessJournalMaterializedState::default(),
            applied_seq: SeqNo::ZERO,
            outcomes: HashMap::new(),
            outcome_order: VecDeque::new(),
        }
    }
}

impl CachedProjection {
    fn remember_outcome(
        &mut self,
        seq: SeqNo,
        outcome: Result<StoredCommandOutcome, ProcessJournalStoreError>,
    ) {
        let key = seq.get();
        self.outcomes.insert(key, outcome);
        self.outcome_order.push_back(key);
        while self.outcome_order.len() > MAX_RECENT_OUTCOMES {
            if let Some(oldest) = self.outcome_order.pop_front() {
                self.outcomes.remove(&oldest);
            }
        }
    }

    fn outcome(
        &mut self,
        seq: SeqNo,
    ) -> Option<Result<StoredCommandOutcome, ProcessJournalStoreError>> {
        self.outcomes.remove(&seq.get())
    }
}

pub struct ProcessJournalStore<F>
where
    F: RootFilesystem,
{
    filesystem: Arc<ScopedFilesystem<F>>,
    projection: Mutex<CachedProjection>,
    legacy_checked: AtomicBool,
    observers: StdMutex<Vec<Arc<dyn ProcessJournalCommitObserver>>>,
    lease_duration: Duration,
    concurrency_limits: ProcessConcurrencyLimits,
}

impl<F> ProcessJournalStore<F>
where
    F: RootFilesystem,
{
    pub fn new(filesystem: Arc<ScopedFilesystem<F>>) -> Self {
        Self {
            filesystem,
            projection: Mutex::new(CachedProjection::default()),
            legacy_checked: AtomicBool::new(false),
            observers: StdMutex::new(Vec::new()),
            lease_duration: DEFAULT_LEASE_DURATION,
            concurrency_limits: ProcessConcurrencyLimits::default(),
        }
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    pub fn with_concurrency_limits(mut self, limits: ProcessConcurrencyLimits) -> Self {
        self.concurrency_limits = limits;
        self
    }

    async fn submit_process_inner(
        &self,
        request: SubmitProcessRequest,
    ) -> Result<(JournaledProcessSnapshot, bool), ProcessJournalStoreError> {
        match self.execute(StoredProcessCommand::Submit(request)).await? {
            StoredCommandOutcome::Submitted(snapshot, changed) => Ok((snapshot, changed)),
            outcome => Err(unexpected_outcome("submit", outcome)),
        }
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

    async fn execute(
        &self,
        command: StoredProcessCommand,
    ) -> Result<StoredCommandOutcome, ProcessJournalStoreError> {
        self.ensure_legacy_state_imported().await?;
        let payload = serde_json::to_vec(&StoredProcessJournalRecord::V1(command))
            .map_err(|error| ProcessJournalStoreError::Serialization(error.to_string()))?;
        let seq = self.append_with_busy_retry(payload).await?;
        let mut projection = self.projection.lock().await;
        self.refresh_projection_through(&mut projection, Some(seq))
            .await?;
        projection.outcome(seq).ok_or_else(|| {
            ProcessJournalStoreError::Deserialization(format!(
                "process journal record {} produced no command outcome",
                seq.get()
            ))
        })?
    }

    async fn load_state(
        &self,
    ) -> Result<ProcessJournalMaterializedState, ProcessJournalStoreError> {
        self.ensure_legacy_state_imported().await?;
        let path = journal_log_path()?;
        let head = self
            .filesystem
            .head_seq(&ResourceScope::system(), &path, SeqNo::ZERO)
            .await?;
        let mut projection = self.projection.lock().await;
        self.refresh_projection_through(&mut projection, head)
            .await?;
        Ok(projection.state.clone())
    }

    async fn ensure_legacy_state_imported(&self) -> Result<(), ProcessJournalStoreError> {
        if self.legacy_checked.load(Ordering::Acquire) {
            return Ok(());
        }
        let log_path = journal_log_path()?;
        if self
            .filesystem
            .head_seq(&ResourceScope::system(), &log_path, SeqNo::ZERO)
            .await?
            .is_some()
        {
            self.legacy_checked.store(true, Ordering::Release);
            return Ok(());
        }
        let legacy_path = legacy_journal_state_path()?;
        let Some(versioned) = self
            .filesystem
            .get(&ResourceScope::system(), &legacy_path)
            .await?
        else {
            self.legacy_checked.store(true, Ordering::Release);
            return Ok(());
        };
        let state = serde_json::from_slice(&versioned.entry.body)
            .map_err(|error| ProcessJournalStoreError::Deserialization(error.to_string()))?;
        let payload = serde_json::to_vec(&StoredProcessJournalRecord::V1(
            StoredProcessCommand::ImportLegacyState(Box::new(state)),
        ))
        .map_err(|error| ProcessJournalStoreError::Serialization(error.to_string()))?;
        self.append_with_busy_retry(payload).await?;
        self.legacy_checked.store(true, Ordering::Release);
        Ok(())
    }

    async fn append_with_busy_retry(
        &self,
        payload: Vec<u8>,
    ) -> Result<SeqNo, ProcessJournalStoreError> {
        let path = journal_log_path()?;
        for attempt in 0..MAX_APPEND_BUSY_RETRIES {
            match self
                .filesystem
                .append(&ResourceScope::system(), &path, payload.clone())
                .await
            {
                Ok(seq) => return Ok(seq),
                Err(FilesystemError::BackendBusy { .. })
                    if attempt + 1 < MAX_APPEND_BUSY_RETRIES =>
                {
                    let millis = 1_u64 << attempt.min(6);
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(ProcessJournalStoreError::Filesystem(
            FilesystemError::BackendBusy {
                path: self.filesystem.resolve(&ResourceScope::system(), &path)?,
                operation: FilesystemOperation::Append,
            },
        ))
    }

    async fn refresh_projection_through(
        &self,
        projection: &mut CachedProjection,
        target: Option<SeqNo>,
    ) -> Result<(), ProcessJournalStoreError> {
        let Some(target) = target else {
            return Ok(());
        };
        let path = journal_log_path()?;
        while projection.applied_seq < target {
            let records = self
                .filesystem
                .tail_bounded(
                    &ResourceScope::system(),
                    &path,
                    projection.applied_seq,
                    JOURNAL_READ_BATCH,
                )
                .await?;
            if records.is_empty() {
                return Err(ProcessJournalStoreError::Deserialization(format!(
                    "process journal ended before committed sequence {}",
                    target.get()
                )));
            }
            for record in records {
                if record.seq > target {
                    break;
                }
                let stored: StoredProcessJournalRecord = serde_json::from_slice(&record.payload)
                    .map_err(|error| {
                        ProcessJournalStoreError::Deserialization(error.to_string())
                    })?;
                let outcome = projection.state.apply_record(stored);
                projection.remember_outcome(record.seq, outcome);
                projection.applied_seq = record.seq;
            }
        }
        Ok(())
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
impl<F> crate::ProcessSnapshotSource for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn process_snapshots(
        &self,
        scope: &ResourceScope,
    ) -> Result<Vec<JournaledProcessSnapshot>, Self::Error> {
        let projection = self.load_state().await?;
        Ok(projection.snapshots_for_scope(scope))
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
        let lease_duration_millis =
            u64::try_from(self.lease_duration.as_millis()).map_err(|_| {
                ProcessJournalStoreError::InvalidRequest(
                    "process lease duration exceeds journal representation".to_string(),
                )
            })?;
        let claimed = match self
            .execute(StoredProcessCommand::Claim {
                request,
                now: Utc::now(),
                lease_duration_millis,
                lease_nonce: ProcessId::new(),
                limits: self.concurrency_limits.clone(),
            })
            .await?
        {
            StoredCommandOutcome::Claimed(claimed) => claimed,
            outcome => return Err(unexpected_outcome("claim", outcome)),
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
        let lease_duration_millis =
            u64::try_from(self.lease_duration.as_millis()).map_err(|_| {
                ProcessJournalStoreError::InvalidRequest(
                    "process lease duration exceeds journal representation".to_string(),
                )
            })?;
        let snapshot = match self
            .execute(StoredProcessCommand::Heartbeat {
                request,
                now: Utc::now(),
                lease_duration_millis,
            })
            .await?
        {
            StoredCommandOutcome::Heartbeat(snapshot) => snapshot,
            outcome => return Err(unexpected_outcome("heartbeat", outcome)),
        };
        self.notify_process_commit(snapshot.clone(), ProcessJournalKind::Heartbeat, None)
            .await?;
        Ok(snapshot.journal_cursor)
    }

    async fn recover_expired_process_leases(
        &self,
        request: RecoverExpiredProcessLeasesRequest,
    ) -> Result<RecoverExpiredProcessLeasesResponse, Self::Error> {
        let response = match self
            .execute(StoredProcessCommand::RecoverExpired(request))
            .await?
        {
            StoredCommandOutcome::Recovered(response) => response,
            outcome => return Err(unexpected_outcome("recover_expired", outcome)),
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
        self.control_transition(ProcessControlMutation {
            scope: request.scope,
            process_id: request.process_id,
            action: ProcessControlAction::Resume,
            operation_id: request.operation_id,
            expected_cursor: request.expected_cursor,
            reason: None,
            checkpoint_ref: request.checkpoint_ref,
            metadata: request.metadata,
        })
        .await
    }

    async fn stop_process(
        &self,
        request: StopProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(ProcessControlMutation {
            scope: request.scope,
            process_id: request.process_id,
            action: ProcessControlAction::Stop,
            operation_id: request.operation_id,
            expected_cursor: None,
            reason: request.reason,
            checkpoint_ref: None,
            metadata: None,
        })
        .await
    }

    async fn request_cancel_process(
        &self,
        request: CancelProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(ProcessControlMutation {
            scope: request.scope,
            process_id: request.process_id,
            action: ProcessControlAction::Cancel,
            operation_id: request.operation_id,
            expected_cursor: None,
            reason: request.reason,
            checkpoint_ref: None,
            metadata: None,
        })
        .await
    }

    async fn kill_process(
        &self,
        request: KillProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.control_transition(ProcessControlMutation {
            scope: request.scope,
            process_id: request.process_id,
            action: ProcessControlAction::Kill,
            operation_id: request.operation_id,
            expected_cursor: None,
            reason: request.reason,
            checkpoint_ref: None,
            metadata: None,
        })
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
    ) -> Result<ProcessControlResult, ProcessJournalStoreError> {
        let reason = mutation.reason.clone();
        let (result, committed_kind) = match self
            .execute(StoredProcessCommand::Control(mutation))
            .await?
        {
            StoredCommandOutcome::Controlled(result, kind) => (result, kind),
            outcome => return Err(unexpected_outcome("control", outcome)),
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
        let snapshot = match self
            .execute(StoredProcessCommand::LeasedTransition { request, mutation })
            .await?
        {
            StoredCommandOutcome::Transitioned(snapshot) => snapshot,
            outcome => return Err(unexpected_outcome("leased_transition", outcome)),
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
        let state = self.load_state().await?;
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
        match self
            .execute(StoredProcessCommand::ReserveTree(request))
            .await?
        {
            StoredCommandOutcome::TreeReserved(reservation) => Ok(reservation),
            outcome => Err(unexpected_outcome("reserve_tree", outcome)),
        }
    }

    async fn release_process_tree(
        &self,
        request: ReleaseProcessTreeRequest,
    ) -> Result<(), Self::Error> {
        match self
            .execute(StoredProcessCommand::ReleaseTree(request))
            .await?
        {
            StoredCommandOutcome::TreeReleased => Ok(()),
            outcome => Err(unexpected_outcome("release_tree", outcome)),
        }
    }

    async fn prune_released_process(
        &self,
        request: PruneReleasedProcessRequest,
    ) -> Result<(), Self::Error> {
        match self
            .execute(StoredProcessCommand::PruneTree(request))
            .await?
        {
            StoredCommandOutcome::TreePruned => Ok(()),
            outcome => Err(unexpected_outcome("prune_tree", outcome)),
        }
    }
}

#[async_trait]
impl<F> ProcessDependencyPort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn open_process_dependency(
        &self,
        request: OpenProcessDependencyRequest,
    ) -> Result<ProcessDependencyRecord, Self::Error> {
        match self
            .execute(StoredProcessCommand::OpenDependency(request))
            .await?
        {
            StoredCommandOutcome::Dependency(Some(record)) => Ok(record),
            StoredCommandOutcome::Dependency(None) => {
                Err(ProcessJournalStoreError::InvalidRequest(
                    "open dependency produced no record".to_string(),
                ))
            }
            outcome => Err(unexpected_outcome("open_dependency", outcome)),
        }
    }

    async fn settle_process_dependency(
        &self,
        request: SettleProcessDependencyRequest,
    ) -> Result<Option<ProcessDependencyRecord>, Self::Error> {
        match self
            .execute(StoredProcessCommand::SettleDependency(request))
            .await?
        {
            StoredCommandOutcome::Dependency(record) => Ok(record),
            outcome => Err(unexpected_outcome("settle_dependency", outcome)),
        }
    }

    async fn consume_process_dependency(
        &self,
        request: CloseProcessDependencyRequest,
    ) -> Result<Option<ProcessDependencyRecord>, Self::Error> {
        match self
            .execute(StoredProcessCommand::ConsumeDependency(request))
            .await?
        {
            StoredCommandOutcome::Dependency(record) => Ok(record),
            outcome => Err(unexpected_outcome("consume_dependency", outcome)),
        }
    }

    async fn abandon_process_dependency(
        &self,
        request: CloseProcessDependencyRequest,
    ) -> Result<Option<ProcessDependencyRecord>, Self::Error> {
        match self
            .execute(StoredProcessCommand::AbandonDependency(request))
            .await?
        {
            StoredCommandOutcome::Dependency(record) => Ok(record),
            outcome => Err(unexpected_outcome("abandon_dependency", outcome)),
        }
    }

    async fn query_process_dependencies(
        &self,
        request: ProcessDependencyQuery,
    ) -> Result<Vec<ProcessDependencyRecord>, Self::Error> {
        let state = self.load_state().await?;
        let mut records = state
            .dependencies
            .values()
            .filter(|record| same_lineage_scope(&record.scope, &request.scope))
            .filter(|record| {
                request
                    .dependent_process_id
                    .is_none_or(|process_id| record.dependent_process_id == process_id)
            })
            .filter(|record| {
                request
                    .group_ref
                    .as_ref()
                    .is_none_or(|group_ref| record.group_ref.as_ref() == Some(group_ref))
            })
            .filter(|record| {
                request.include_closed
                    || !matches!(
                        record.state,
                        crate::ProcessDependencyState::Consumed
                            | crate::ProcessDependencyState::Abandoned
                    )
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| {
            (
                record.dependent_process_id.as_uuid(),
                record.dependency_process_id.as_uuid(),
            )
        });
        Ok(records)
    }

    async fn unresolved_process_dependencies(
        &self,
    ) -> Result<Vec<ProcessDependencyRecord>, Self::Error> {
        let state = self.load_state().await?;
        let mut records = state
            .dependencies
            .values()
            .filter(|record| {
                !matches!(
                    record.state,
                    crate::ProcessDependencyState::Consumed
                        | crate::ProcessDependencyState::Abandoned
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| {
            (
                record.created_at,
                record.dependent_process_id.as_uuid(),
                record.dependency_process_id.as_uuid(),
            )
        });
        Ok(records)
    }
}

#[async_trait]
impl<F> ProcessCheckpointPort for ProcessJournalStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = ProcessJournalStoreError;

    async fn record_process_checkpoint(
        &self,
        request: RecordProcessCheckpointRequest,
    ) -> Result<ProcessCheckpointRecord, Self::Error> {
        match self
            .execute(StoredProcessCommand::RecordCheckpoint(request))
            .await?
        {
            StoredCommandOutcome::Checkpointed(record) => Ok(record),
            outcome => Err(unexpected_outcome("record_checkpoint", outcome)),
        }
    }

    async fn get_process_checkpoint(
        &self,
        request: GetProcessCheckpointRequest,
    ) -> Result<Option<ProcessCheckpointRecord>, Self::Error> {
        let state = self.load_state().await?;
        Ok(state
            .checkpoints
            .get(&request.checkpoint_id)
            .filter(|record| {
                record.process_id == request.process_id
                    && process_scope_visible(&record.scope, &request.scope)
            })
            .cloned())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessTransitionMutation {
    status: ProcessLifecycleStatus,
    kind: ProcessJournalKind,
    suspension: Option<ProcessSuspension>,
    checkpoint_ref: Option<crate::ProcessCheckpointRef>,
    failure: Option<ironclaw_host_api::SanitizedFailure>,
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessControlMutation {
    scope: ResourceScope,
    process_id: ProcessId,
    action: ProcessControlAction,
    operation_id: Option<ProcessOperationId>,
    expected_cursor: Option<ProcessJournalCursor>,
    reason: Option<String>,
    checkpoint_ref: Option<crate::ProcessCheckpointRef>,
    metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProcessControlAction {
    Resume,
    Stop,
    Cancel,
    Kill,
}

impl ProcessControlAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Resume => "resume",
            Self::Stop => "stop",
            Self::Cancel => "cancel",
            Self::Kill => "kill",
        }
    }
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
        let state = self.load_state().await?;
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
        let state = self.load_state().await?;
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
        let state = self.load_state().await?;
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
            Ok(state) => state,
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
        let state = self.load_state().await?;
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

impl<F> ProcessRuntimePort for ProcessJournalStore<F> where F: RootFilesystem + Send + Sync + 'static
{}

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
    let scope_matches = match request
        .scope_match
        .unwrap_or(crate::ProcessGateScopeMatch::Exact)
    {
        crate::ProcessGateScopeMatch::Exact => same_scope_owner(&snapshot.scope, &request.scope),
        crate::ProcessGateScopeMatch::Owner => {
            snapshot.scope.tenant_id == request.scope.tenant_id
                && snapshot.scope.user_id == request.scope.user_id
                && snapshot.scope.agent_id == request.scope.agent_id
                && snapshot.scope.project_id == request.scope.project_id
        }
    };
    snapshot.status == ProcessLifecycleStatus::Suspended
        && suspension.kind == request.gate_kind
        && scope_matches
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

fn process_claim_within_limits(
    state: &ProcessJournalMaterializedState,
    process_id: ProcessId,
    limits: &ProcessConcurrencyLimits,
) -> bool {
    let Some(candidate) = state.processes.get(&process_id) else {
        return false;
    };
    if let (Some(cap), Some(owner)) = (limits.max_running_per_owner, &candidate.owner_user_id) {
        let running_for_owner = state
            .processes
            .values()
            .filter(|snapshot| {
                snapshot.status == ProcessLifecycleStatus::Running
                    && snapshot.scope.tenant_id == candidate.scope.tenant_id
                    && snapshot.owner_user_id.as_ref() == Some(owner)
            })
            .count();
        if running_for_owner >= cap as usize {
            return false;
        }
    }
    let Some(class) = &candidate.concurrency_class else {
        return true;
    };
    let Some(cap) = limits.max_running_by_class.get(class) else {
        return true;
    };
    state
        .processes
        .values()
        .filter(|snapshot| {
            snapshot.status == ProcessLifecycleStatus::Running
                && snapshot.scope.tenant_id == candidate.scope.tenant_id
                && snapshot.concurrency_class.as_ref() == Some(class)
        })
        .count()
        < *cap as usize
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

fn journal_log_path() -> Result<ScopedPath, ProcessJournalStoreError> {
    ScopedPath::new(JOURNAL_LOG_PATH)
        .map_err(|error| ProcessJournalStoreError::InvalidPath(invalid_path(error).to_string()))
}

fn legacy_journal_state_path() -> Result<ScopedPath, ProcessJournalStoreError> {
    ScopedPath::new(LEGACY_JOURNAL_STATE_PATH)
        .map_err(|error| ProcessJournalStoreError::InvalidPath(invalid_path(error).to_string()))
}

fn unexpected_outcome(operation: &str, outcome: StoredCommandOutcome) -> ProcessJournalStoreError {
    ProcessJournalStoreError::Deserialization(format!(
        "process journal {operation} produced unexpected outcome {outcome:?}"
    ))
}
