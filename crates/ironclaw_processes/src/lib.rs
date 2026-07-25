//! Process lifecycle contracts for IronClaw Reborn.
//!
//! `ironclaw_processes` stores and manages host-tracked background capability
//! processes. It owns lifecycle mechanics, not capability authorization or
//! runtime dispatch policy.
//!
//! # Module map
//!
//! - [`types`] — public data types, errors, and core traits
//!   ([`ProcessStorePort`], [`ProcessResultStorePort`], [`ProcessExecutor`],
//!   [`ProcessManager`])
//! - [`cancellation`] — cooperative cancellation tokens + per-process registry
//! - [`host`] — read/poll/await/cancel surface ([`ProcessHost`],
//!   [`ProcessSubscription`])
//! - [`process_store`] — the process `ProcessStorePort` / `ProcessResultStorePort`
//!   (durable over libSQL/Postgres; in-memory-backed over `InMemoryBackend` in
//!   tests, via the `test-support` helpers — arch-simplification §4.3)
//! - [`wrappers`] — composable decorators ([`EventingProcessStore`],
//!   [`ResourceManagedProcessStore`])
//! - [`services`] — composition root ([`ProcessServices`]) and the
//!   production [`BackgroundProcessManager`]

mod cancellation;
mod host;
mod journal;
mod journal_store;
mod process_store;
mod services;
#[cfg(any(test, feature = "test-support"))]
mod test_support;
mod types;
mod wrappers;

pub use cancellation::{ProcessCancellationRegistry, ProcessCancellationToken};
pub use host::{ProcessHost, ProcessSubscription};
pub use journal::{
    CancelProcessRequest, ClaimProcessesRequest, ClaimedProcess, FailProcessRequest,
    GetProcessSnapshotRequest, JournaledProcessSnapshot, KillProcessRequest, ProcessCheckpointRef,
    ProcessControlPort, ProcessControlResult, ProcessGateOwnerMatch, ProcessGateQuery,
    ProcessGateQuerySource, ProcessGateRecord, ProcessJournalCommit, ProcessJournalCommitObserver,
    ProcessJournalCursor, ProcessJournalEntry, ProcessJournalError, ProcessJournalKind,
    ProcessJournalObserverRegistry, ProcessJournalPage, ProcessJournalProjectionCursor,
    ProcessJournalProjectionRequest, ProcessJournalProjectionSnapshot, ProcessJournalSource,
    ProcessKind, ProcessLeaseRequest, ProcessLeaseSnapshot, ProcessLeaseToken,
    ProcessLifecycleLookupBatchRequest, ProcessLifecycleLookupRequest,
    ProcessLifecycleLookupResult, ProcessLifecycleLookupSource, ProcessLifecycleStatus,
    ProcessOperationId, ProcessOutcome, ProcessStateTransitionRequest, ProcessSubmissionPort,
    ProcessSuspension, ProcessSuspensionKind, ProcessTransitionPort, ProcessWorkerId,
    RecoverExpiredProcessLeasesRequest, RecoverExpiredProcessLeasesResponse, ResumeProcessRequest,
    StopProcessRequest, SubmitProcessRequest, SuspendProcessRequest,
};
pub use journal_store::{ProcessJournalStore, ProcessJournalStoreError};
pub use process_store::{ProcessResultStore, ProcessStore};
pub use services::{
    BackgroundErrorHandler, BackgroundFailure, BackgroundFailureStage, BackgroundProcessManager,
    ProcessServices,
};
#[cfg(any(test, feature = "test-support"))]
pub use test_support::{
    in_memory_backed_process_result_store, in_memory_backed_process_services,
    in_memory_backed_process_store, in_memory_backed_processes_filesystem,
};
pub use types::{
    ProcessError, ProcessExecutionError, ProcessExecutionRequest, ProcessExecutionResult,
    ProcessExecutor, ProcessExit, ProcessManager, ProcessRecord, ProcessResultRecord,
    ProcessResultStorePort, ProcessStart, ProcessStatus, ProcessStorePort,
};
pub use wrappers::{EventingProcessStore, ResourceManagedProcessStore};
