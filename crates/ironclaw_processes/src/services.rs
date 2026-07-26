//! Composition + spawn surface for process services.
//!
//! - [`ProcessServices`] bundles a process store, a result store, and a
//!   shared [`ProcessCancellationRegistry`] so the host and background manager
//!   stay coordinated through a single graph.
//! - [`BackgroundProcessManager`] is the compatibility [`ProcessManager`] that
//!   journals capability work and registers its executor with the generic
//!   [`ProcessSupervisor`].
//!
//! Executor persistence failures can be observed by attaching a
//! [`with_error_handler`](BackgroundProcessManager::with_error_handler)
//! callback. Without a handler, those errors are silently dropped.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt;
use ironclaw_events::sanitize_error_kind;
use ironclaw_filesystem::{RootFilesystem, ScopedFilesystem};
use ironclaw_host_api::{ProcessId, ResourceReservation, ResourceScope};

use crate::cancellation::ProcessCancellationRegistry;
use crate::host::ProcessHost;
use crate::result_store::ProcessResultStore;
use crate::types::{
    ProcessError, ProcessExecutionRequest, ProcessExecutor, ProcessManager, ProcessRecord,
    ProcessResultStorePort, ProcessStart, ProcessStatus, ProcessStorePort,
};
use crate::{
    ClaimedProcess, GetProcessInputRequest, JournalProcessExecutor, JournalProcessStore,
    ProcessExecutorFailure, ProcessKind, ProcessRuntimePort, ProcessSupervisor,
    ProcessSupervisorConfig, ProcessSupervisorHandle,
};

/// Stage at which a background task failed to persist state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundFailureStage {
    /// `ProcessStorePort::get` failed during the post-execution status probe.
    StoreLookup,
    /// `ProcessStorePort::complete` failed when promoting to `Completed`.
    StoreComplete,
    /// `ProcessStorePort::fail` failed when promoting to `Failed`.
    StoreFail,
    /// `ProcessResultStorePort::complete` failed.
    ResultStoreComplete,
    /// `ProcessResultStorePort::fail` failed.
    ResultStoreFail,
}

/// Failure observed inside a [`BackgroundProcessManager`] spawned task.
///
/// The detached task cannot return errors to the original `spawn` caller, so
/// any failure surfaces here for an attached error handler. If no handler is
/// configured, the error is dropped — see
/// [`BackgroundProcessManager::with_error_handler`].
#[derive(Debug)]
pub struct BackgroundFailure {
    pub scope: ResourceScope,
    pub process_id: ProcessId,
    pub stage: BackgroundFailureStage,
    pub error: ProcessError,
}

/// Callback invoked for each [`BackgroundFailure`] in the spawned task.
pub type BackgroundErrorHandler = dyn Fn(BackgroundFailure) + Send + Sync;

pub struct ProcessServices<S, R>
where
    S: ProcessStorePort + 'static,
    R: ProcessResultStorePort + 'static,
{
    process_store: Arc<S>,
    result_store: Arc<R>,
    cancellation_registry: Arc<ProcessCancellationRegistry>,
}

impl<S, R> Clone for ProcessServices<S, R>
where
    S: ProcessStorePort + 'static,
    R: ProcessResultStorePort + 'static,
{
    fn clone(&self) -> Self {
        Self {
            process_store: Arc::clone(&self.process_store),
            result_store: Arc::clone(&self.result_store),
            cancellation_registry: Arc::clone(&self.cancellation_registry),
        }
    }
}

impl<S, R> ProcessServices<S, R>
where
    S: ProcessStorePort + 'static,
    R: ProcessResultStorePort + 'static,
{
    pub fn new(process_store: Arc<S>, result_store: Arc<R>) -> Self {
        Self::from_parts(
            process_store,
            result_store,
            Arc::new(ProcessCancellationRegistry::new()),
        )
    }

    pub fn from_parts(
        process_store: Arc<S>,
        result_store: Arc<R>,
        cancellation_registry: Arc<ProcessCancellationRegistry>,
    ) -> Self {
        Self {
            process_store,
            result_store,
            cancellation_registry,
        }
    }

    pub fn process_store(&self) -> Arc<S> {
        Arc::clone(&self.process_store)
    }

    pub fn result_store(&self) -> Arc<R> {
        Arc::clone(&self.result_store)
    }

    pub fn cancellation_registry(&self) -> Arc<ProcessCancellationRegistry> {
        Arc::clone(&self.cancellation_registry)
    }

    pub fn host(&self) -> ProcessHost<'_> {
        ProcessHost::new(self.process_store.as_ref())
            .with_cancellation_registry(Arc::clone(&self.cancellation_registry))
            .with_result_store(Arc::clone(&self.result_store))
    }

    pub fn background_manager<E>(&self, executor: Arc<E>) -> BackgroundProcessManager
    where
        E: ProcessExecutor + 'static,
    {
        BackgroundProcessManager::new(Arc::clone(&self.process_store), executor)
            .with_cancellation_registry(Arc::clone(&self.cancellation_registry))
            .with_result_store(Arc::clone(&self.result_store))
            .start_supervisor()
    }
}

impl<F> ProcessServices<JournalProcessStore<F>, ProcessResultStore<F>>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    pub fn filesystem(filesystem: Arc<ScopedFilesystem<F>>) -> Self {
        Self::new(
            Arc::new(JournalProcessStore::new(Arc::clone(&filesystem))),
            Arc::new(ProcessResultStore::from_arc(filesystem)),
        )
    }
}

#[cfg(any(test, feature = "test-support"))]
impl
    ProcessServices<
        JournalProcessStore<ironclaw_filesystem::InMemoryBackend>,
        ProcessResultStore<ironclaw_filesystem::InMemoryBackend>,
    >
{
    /// In-memory-backed process services for tests — the production
    /// [`filesystem`](Self::filesystem) store pair over one fresh
    /// `InMemoryBackend` `/processes` mount (arch-simplification §4.3).
    /// Replaces the deleted bespoke `InMemory*Store` pair; the two stores
    /// share one backend so externalized output (`output_ref`) reads back.
    pub fn in_memory() -> Self {
        Self::filesystem(crate::test_support::in_memory_backed_processes_filesystem())
    }
}

pub struct BackgroundProcessManager {
    store: Arc<dyn ProcessStorePort>,
    executor: Arc<dyn ProcessExecutor + 'static>,
    cancellation_registry: Option<Arc<ProcessCancellationRegistry>>,
    result_store: Option<Arc<dyn ProcessResultStorePort>>,
    error_handler: Option<Arc<BackgroundErrorHandler>>,
    supervisor: Mutex<Option<ProcessSupervisorHandle>>,
}

impl BackgroundProcessManager {
    pub fn new<S, E>(store: Arc<S>, executor: Arc<E>) -> Self
    where
        S: ProcessStorePort + 'static,
        E: ProcessExecutor + 'static,
    {
        Self {
            store,
            executor,
            cancellation_registry: None,
            result_store: None,
            error_handler: None,
            supervisor: Mutex::new(None),
        }
    }

    pub fn with_cancellation_registry(
        mut self,
        registry: Arc<ProcessCancellationRegistry>,
    ) -> Self {
        self.cancellation_registry = Some(registry);
        self
    }

    pub fn with_result_store<S>(mut self, store: Arc<S>) -> Self
    where
        S: ProcessResultStorePort + 'static,
    {
        self.result_store = Some(store);
        self
    }

    /// Attach a callback for executor-side store/result-store failures.
    pub fn with_error_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(BackgroundFailure) + Send + Sync + 'static,
    {
        self.error_handler = Some(Arc::new(handler));
        self
    }

    /// Starts polling immediately so queued work resumes after restart.
    pub fn start_supervisor(self) -> Self {
        if let Err(error) = self.wake_notifier() {
            tracing::error!(%error, "capability process supervisor failed to start");
        }
        self
    }

    fn wake_notifier(&self) -> Result<Arc<crate::ProcessWakeNotifier>, ProcessError> {
        let mut supervisor =
            self.supervisor
                .lock()
                .map_err(|_| ProcessError::InvalidStoredRecord {
                    reason: "process supervisor mutex poisoned".to_string(),
                })?;
        if supervisor.is_none() {
            let runtime = self.store.process_runtime();
            let executor = Arc::new(BackgroundJournalExecutor {
                runtime: Arc::clone(&runtime),
                store: Arc::clone(&self.store),
                executor: Arc::clone(&self.executor),
                cancellation_registry: self.cancellation_registry.clone(),
                result_store: self.result_store.clone(),
                error_handler: self.error_handler.clone(),
            });
            *supervisor = Some(
                ProcessSupervisor::new(
                    runtime,
                    executor,
                    ProcessKind::CapabilityInvocation,
                    ProcessSupervisorConfig::default(),
                )
                .start(),
            );
        }
        supervisor
            .as_ref()
            .map(ProcessSupervisorHandle::wake_notifier)
            .ok_or_else(|| ProcessError::InvalidStoredRecord {
                reason: "process supervisor failed to start".to_string(),
            })
    }
}

#[async_trait]
impl ProcessManager for BackgroundProcessManager {
    /// Journal the request, then wake the shared process supervisor.
    async fn spawn(&self, start: ProcessStart) -> Result<ProcessRecord, ProcessError> {
        let record = self.store.start(start).await?;
        if let Some(registry) = &self.cancellation_registry {
            registry.register(&record.scope, record.process_id);
        }
        let wake_result = self.wake_notifier().and_then(|notifier| {
            notifier
                .notify_scope(record.scope.clone())
                .map_err(|error| ProcessError::InvalidStoredRecord {
                    reason: error.to_string(),
                })
        });
        if wake_result.is_err()
            && let Some(registry) = &self.cancellation_registry
        {
            registry.unregister(&record.scope, record.process_id);
        }
        wake_result?;
        Ok(record)
    }
}

struct BackgroundJournalExecutor {
    runtime: Arc<dyn ProcessRuntimePort>,
    store: Arc<dyn ProcessStorePort>,
    executor: Arc<dyn ProcessExecutor>,
    cancellation_registry: Option<Arc<ProcessCancellationRegistry>>,
    result_store: Option<Arc<dyn ProcessResultStorePort>>,
    error_handler: Option<Arc<BackgroundErrorHandler>>,
}

impl BackgroundJournalExecutor {
    fn report(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
        stage: BackgroundFailureStage,
        error: ProcessError,
    ) {
        if let Some(handler) = &self.error_handler {
            handler(BackgroundFailure {
                scope: scope.clone(),
                process_id,
                stage,
                error,
            });
        }
    }

    async fn execution_request(
        &self,
        claimed: &ClaimedProcess,
    ) -> Result<ProcessExecutionRequest, ProcessError> {
        let process_id = claimed.state.process_id;
        let scope = &claimed.state.scope;
        let record = self
            .store
            .get(scope, process_id)
            .await?
            .ok_or(ProcessError::UnknownProcess { process_id })?;
        let input = self
            .runtime
            .get_process_input(GetProcessInputRequest {
                process_id,
                scope: scope.clone(),
            })
            .await
            .map_err(crate::compatibility::map_journal_error)?
            .ok_or_else(|| ProcessError::InvalidStoredRecord {
                reason: format!("process {process_id} has no durable input"),
            })
            .and_then(|record| {
                serde_json::from_slice(record.payload.as_bytes())
                    .map_err(|error| ProcessError::Deserialization(error.to_string()))
            })?;
        let cancellation = self
            .cancellation_registry
            .as_ref()
            .map(|registry| registry.register(scope, process_id))
            .unwrap_or_default();
        let resource_reservation = record
            .resource_reservation_id
            .map(|id| ResourceReservation {
                id,
                scope: record.scope.clone(),
                estimate: record.estimated_resources.clone(),
            });
        Ok(ProcessExecutionRequest {
            process_id,
            invocation_id: record.invocation_id,
            scope: record.scope,
            authenticated_actor_user_id: record.authenticated_actor_user_id,
            extension_id: record.extension_id,
            capability_id: record.capability_id,
            runtime: record.runtime,
            estimate: record.estimated_resources,
            mounts: record.mounts,
            resource_reservation,
            authorized_continuation: record.authorized_continuation,
            input,
            cancellation,
        })
    }

    async fn persist_outcome(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
        outcome: Result<
            Result<crate::ProcessExecutionResult, crate::ProcessExecutionError>,
            Box<dyn std::any::Any + Send>,
        >,
    ) {
        let still_running = match self.store.get(scope, process_id).await {
            Ok(Some(record)) => record.status == ProcessStatus::Running,
            Ok(None) => false,
            Err(error) => {
                self.report(
                    scope,
                    process_id,
                    BackgroundFailureStage::StoreLookup,
                    error,
                );
                false
            }
        };
        if !still_running {
            return;
        }
        match outcome {
            Ok(Ok(result)) => {
                let persisted = if let Some(result_store) = &self.result_store {
                    match result_store
                        .complete(scope, process_id, result.output)
                        .await
                    {
                        Ok(_) => true,
                        Err(error) => {
                            self.report(
                                scope,
                                process_id,
                                BackgroundFailureStage::ResultStoreComplete,
                                error,
                            );
                            false
                        }
                    }
                } else {
                    true
                };
                if persisted && let Err(error) = self.store.complete(scope, process_id).await {
                    self.report(
                        scope,
                        process_id,
                        BackgroundFailureStage::StoreComplete,
                        error,
                    );
                }
            }
            Ok(Err(error)) => {
                self.persist_failure(scope, process_id, sanitize_error_kind(error.kind))
                    .await;
            }
            Err(_) => {
                self.persist_failure(scope, process_id, "runtime_panic".to_string())
                    .await;
            }
        }
    }

    async fn persist_failure(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
        error_kind: String,
    ) {
        let persisted = if let Some(result_store) = &self.result_store {
            match result_store
                .fail(scope, process_id, error_kind.clone())
                .await
            {
                Ok(_) => true,
                Err(error) => {
                    self.report(
                        scope,
                        process_id,
                        BackgroundFailureStage::ResultStoreFail,
                        error,
                    );
                    false
                }
            }
        } else {
            true
        };
        if persisted && let Err(error) = self.store.fail(scope, process_id, error_kind).await {
            self.report(scope, process_id, BackgroundFailureStage::StoreFail, error);
        }
    }
}

#[async_trait]
impl JournalProcessExecutor for BackgroundJournalExecutor {
    async fn execute_claimed_process(
        &self,
        claimed: ClaimedProcess,
    ) -> Result<(), ProcessExecutorFailure> {
        let scope = claimed.state.scope.clone();
        let process_id = claimed.state.process_id;
        let request = self
            .execution_request(&claimed)
            .await
            .map_err(|_| ProcessExecutorFailure::new("process_request_invalid"))?;
        let outcome = std::panic::AssertUnwindSafe(self.executor.execute(request))
            .catch_unwind()
            .await;
        self.persist_outcome(&scope, process_id, outcome).await;
        if let Some(registry) = &self.cancellation_registry {
            registry.unregister(&scope, process_id);
        }
        Ok(())
    }
}
