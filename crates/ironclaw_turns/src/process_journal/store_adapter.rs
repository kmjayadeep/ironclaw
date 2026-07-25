use std::sync::Arc;

use async_trait::async_trait;
use ironclaw_host_api::ResourceScope;
use ironclaw_processes::{
    CancelProcessRequest, ClaimProcessesRequest, ClaimedProcess, FailProcessRequest,
    GetProcessSnapshotRequest, JournaledProcessSnapshot, KillProcessRequest, ProcessControlPort,
    ProcessControlResult, ProcessGateQuery, ProcessGateQuerySource, ProcessGateRecord,
    ProcessJournalCursor, ProcessJournalPage, ProcessJournalSource, ProcessJournalStoreError,
    ProcessLeaseRequest, ProcessLifecycleLookupBatchRequest, ProcessLifecycleLookupResult,
    ProcessLifecycleLookupSource, ProcessStateTransitionRequest, ProcessTransitionPort,
    RecoverExpiredProcessLeasesRequest, RecoverExpiredProcessLeasesResponse, ResumeProcessRequest,
    StopProcessRequest, SuspendProcessRequest,
};

use crate::TurnError;

#[derive(Clone)]
pub struct ProcessJournalStoreTurnAdapter {
    transitions: Arc<dyn ProcessTransitionPort<Error = ProcessJournalStoreError>>,
    controls: Arc<dyn ProcessControlPort<Error = ProcessJournalStoreError>>,
    journal: Arc<dyn ProcessJournalSource<Error = ProcessJournalStoreError>>,
    lifecycle: Arc<dyn ProcessLifecycleLookupSource<Error = ProcessJournalStoreError>>,
    gates: Arc<dyn ProcessGateQuerySource<Error = ProcessJournalStoreError>>,
}
impl ProcessJournalStoreTurnAdapter {
    pub fn new(
        transitions: Arc<dyn ProcessTransitionPort<Error = ProcessJournalStoreError>>,
        controls: Arc<dyn ProcessControlPort<Error = ProcessJournalStoreError>>,
        journal: Arc<dyn ProcessJournalSource<Error = ProcessJournalStoreError>>,
        lifecycle: Arc<dyn ProcessLifecycleLookupSource<Error = ProcessJournalStoreError>>,
        gates: Arc<dyn ProcessGateQuerySource<Error = ProcessJournalStoreError>>,
    ) -> Self {
        Self {
            transitions,
            controls,
            journal,
            lifecycle,
            gates,
        }
    }
}

#[async_trait]
impl ProcessControlPort for ProcessJournalStoreTurnAdapter {
    type Error = TurnError;

    async fn resume_process(
        &self,
        request: ResumeProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.controls
            .resume_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn stop_process(
        &self,
        request: StopProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.controls
            .stop_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn request_cancel_process(
        &self,
        request: CancelProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.controls
            .request_cancel_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn kill_process(
        &self,
        request: KillProcessRequest,
    ) -> Result<ProcessControlResult, Self::Error> {
        self.controls
            .kill_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }
}

#[async_trait]
impl ProcessTransitionPort for ProcessJournalStoreTurnAdapter {
    type Error = TurnError;

    async fn claim_next_processes(
        &self,
        request: ClaimProcessesRequest,
    ) -> Result<Vec<ClaimedProcess>, Self::Error> {
        self.transitions
            .claim_next_processes(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn heartbeat_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<ProcessJournalCursor, Self::Error> {
        self.transitions
            .heartbeat_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn recover_expired_process_leases(
        &self,
        request: RecoverExpiredProcessLeasesRequest,
    ) -> Result<RecoverExpiredProcessLeasesResponse, Self::Error> {
        self.transitions
            .recover_expired_process_leases(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn suspend_process(
        &self,
        request: SuspendProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.transitions
            .suspend_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn complete_process(
        &self,
        request: ProcessStateTransitionRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.transitions
            .complete_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn cancel_process(
        &self,
        request: ProcessStateTransitionRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.transitions
            .cancel_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn fail_process(
        &self,
        request: FailProcessRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.transitions
            .fail_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn relinquish_process(
        &self,
        request: ProcessLeaseRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.transitions
            .relinquish_process(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }
}

#[async_trait]
impl ProcessJournalSource for ProcessJournalStoreTurnAdapter {
    type Error = TurnError;

    async fn get_process_snapshot(
        &self,
        request: GetProcessSnapshotRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        self.journal
            .get_process_snapshot(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn read_process_journal_after(
        &self,
        scope: &ResourceScope,
        owner_user_id: Option<&ironclaw_host_api::UserId>,
        after: Option<ProcessJournalCursor>,
        limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error> {
        self.journal
            .read_process_journal_after(scope, owner_user_id, after, limit)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }

    async fn read_process_journal_log_after(
        &self,
        after: Option<ProcessJournalCursor>,
        limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error> {
        self.journal
            .read_process_journal_log_after(after, limit)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }
}

#[async_trait]
impl ProcessLifecycleLookupSource for ProcessJournalStoreTurnAdapter {
    type Error = TurnError;

    async fn process_lifecycle_states(
        &self,
        request: ProcessLifecycleLookupBatchRequest,
    ) -> Vec<Result<ProcessLifecycleLookupResult, Self::Error>> {
        self.lifecycle
            .process_lifecycle_states(request)
            .await
            .into_iter()
            .map(|result| result.map_err(turn_error_from_process_journal_store_error))
            .collect()
    }
}

#[async_trait]
impl ProcessGateQuerySource for ProcessJournalStoreTurnAdapter {
    type Error = TurnError;

    async fn query_process_gates(
        &self,
        request: ProcessGateQuery,
    ) -> Result<Vec<ProcessGateRecord>, Self::Error> {
        self.gates
            .query_process_gates(request)
            .await
            .map_err(turn_error_from_process_journal_store_error)
    }
}

pub fn turn_error_from_process_journal_store_error(error: ProcessJournalStoreError) -> TurnError {
    match error {
        ProcessJournalStoreError::UnknownProcess { .. } => TurnError::ScopeNotFound,
        ProcessJournalStoreError::ProcessAlreadyExists { .. }
        | ProcessJournalStoreError::StaleSnapshot { .. } => TurnError::Conflict {
            reason: error.to_string(),
        },
        ProcessJournalStoreError::InvalidTransition { from, to, .. } => TurnError::InvalidRequest {
            reason: format!("invalid process journal transition from {from:?} to {to:?}"),
        },
        ProcessJournalStoreError::InvalidLease { .. }
        | ProcessJournalStoreError::InvalidPath(_)
        | ProcessJournalStoreError::Filesystem(_)
        | ProcessJournalStoreError::Serialization(_)
        | ProcessJournalStoreError::Deserialization(_) => TurnError::Unavailable {
            reason: error.to_string(),
        },
    }
}
