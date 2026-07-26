//! Host-facing process query API.
//!
//! [`ProcessHost`] wraps the authoritative [`ProcessRuntimePort`](crate::ProcessRuntimePort) and an
//! optional [`ProcessResultStorePort`](crate::types::ProcessResultStorePort) and
//! [`ProcessCancellationRegistry`](crate::cancellation::ProcessCancellationRegistry).
//! It is the read/poll/await/cancel surface used by host runtimes; spawning
//! processes lives in [`crate::services`].

use std::{fmt, sync::Arc};

use ironclaw_host_api::{ProcessId, ResourceScope};
use serde_json::Value;
use tokio::time::{Duration, sleep};

use crate::cancellation::ProcessCancellationRegistry;
use crate::capability_process::{map_process_journal_error, process_record_from_snapshot};
use crate::types::{
    ProcessError, ProcessExit, ProcessRecord, ProcessResultRecord, ProcessResultStorePort,
    ProcessStatus,
};
use crate::{GetProcessSnapshotRequest, KillProcessRequest, ProcessRuntimePort};

/// Host-facing lifecycle API over process current state.
pub struct ProcessHost {
    runtime: Arc<dyn ProcessRuntimePort>,
    poll_interval: Duration,
    cancellation_registry: Option<Arc<ProcessCancellationRegistry>>,
    result_store: Option<Arc<dyn ProcessResultStorePort>>,
}

impl ProcessHost {
    pub fn new<R>(runtime: &R) -> Self
    where
        R: ProcessRuntimePort + Clone + 'static,
    {
        Self::from_runtime(Arc::new(runtime.clone()))
    }

    pub fn from_runtime(runtime: Arc<dyn ProcessRuntimePort>) -> Self {
        Self {
            runtime,
            poll_interval: Duration::from_millis(10),
            cancellation_registry: None,
            result_store: None,
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
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

    pub fn with_result_store_dyn(mut self, store: Arc<dyn ProcessResultStorePort>) -> Self {
        self.result_store = Some(store);
        self
    }

    fn result_store(&self) -> Result<&dyn ProcessResultStorePort, ProcessError> {
        self.result_store
            .as_deref()
            .ok_or(ProcessError::ProcessResultStoreUnavailable)
    }

    async fn process_record(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<Option<ProcessRecord>, ProcessError> {
        match self
            .runtime
            .get_process_snapshot(GetProcessSnapshotRequest {
                scope: scope.clone(),
                process_id,
            })
            .await
        {
            Ok(snapshot) => process_record_from_snapshot(snapshot).map(Some),
            Err(error) => match map_process_journal_error(error) {
                ProcessError::UnknownProcess { .. } => Ok(None),
                error => Err(error),
            },
        }
    }

    pub async fn status(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<Option<ProcessRecord>, ProcessError> {
        self.process_record(scope, process_id).await
    }

    pub async fn kill(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<ProcessRecord, ProcessError> {
        match self
            .runtime
            .kill_process(KillProcessRequest {
                scope: scope.clone(),
                process_id,
                operation_id: None,
                reason: None,
            })
            .await
            .map_err(map_process_journal_error)
            .and_then(|result| process_record_from_snapshot(result.state))
        {
            Ok(record) => {
                self.record_kill_side_effects(&record).await?;
                Ok(record)
            }
            Err(error @ ProcessError::InvalidTransition { .. }) => {
                if let Ok(Some(record)) = self.process_record(scope, process_id).await
                    && record.status == ProcessStatus::Killed
                {
                    self.record_kill_side_effects(&record).await?;
                    return Ok(record);
                }
                Err(error)
            }
            Err(error) => {
                if let Ok(Some(record)) = self.process_record(scope, process_id).await
                    && record.status == ProcessStatus::Killed
                {
                    self.record_kill_side_effects(&record).await?;
                }
                Err(error)
            }
        }
    }

    async fn record_kill_side_effects(&self, record: &ProcessRecord) -> Result<(), ProcessError> {
        if let Some(registry) = &self.cancellation_registry {
            registry.cancel(&record.scope, record.process_id);
        }
        if let Some(result_store) = &self.result_store {
            result_store.kill(&record.scope, record.process_id).await?;
        }
        Ok(())
    }

    pub async fn result(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<Option<ProcessResultRecord>, ProcessError> {
        self.result_store()?.get(scope, process_id).await
    }

    pub async fn output(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<Option<Value>, ProcessError> {
        self.result_store()?.output(scope, process_id).await
    }

    pub async fn await_result(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<ProcessResultRecord, ProcessError> {
        let mut terminal_without_result_seen = false;
        loop {
            if let Some(result) = self.result(scope, process_id).await? {
                return Ok(result);
            }
            let record = self
                .process_record(scope, process_id)
                .await?
                .ok_or(ProcessError::UnknownProcess { process_id })?;
            if record.status.is_terminal() {
                if self.result_store.is_none() || terminal_without_result_seen {
                    return Err(ProcessError::ProcessResultUnavailable { process_id });
                }
                terminal_without_result_seen = true;
            } else {
                terminal_without_result_seen = false;
            }
            sleep(self.poll_interval).await;
        }
    }

    pub async fn await_process(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<ProcessExit, ProcessError> {
        loop {
            let record = self
                .process_record(scope, process_id)
                .await?
                .ok_or(ProcessError::UnknownProcess { process_id })?;
            if record.status.is_terminal() {
                return Ok(ProcessExit::from_terminal(record));
            }
            sleep(self.poll_interval).await;
        }
    }

    pub async fn subscribe(
        &self,
        scope: &ResourceScope,
        process_id: ProcessId,
    ) -> Result<ProcessSubscription, ProcessError> {
        let initial_record = self
            .process_record(scope, process_id)
            .await?
            .ok_or(ProcessError::UnknownProcess { process_id })?;
        Ok(ProcessSubscription {
            runtime: Arc::clone(&self.runtime),
            scope: scope.clone(),
            process_id,
            poll_interval: self.poll_interval,
            last_status: Some(initial_record.status),
            pending_initial: Some(initial_record),
            finished: false,
        })
    }
}

/// Scoped subscription over process lifecycle status changes.
pub struct ProcessSubscription {
    runtime: Arc<dyn ProcessRuntimePort>,
    scope: ResourceScope,
    process_id: ProcessId,
    poll_interval: Duration,
    last_status: Option<ProcessStatus>,
    pending_initial: Option<ProcessRecord>,
    finished: bool,
}

impl fmt::Debug for ProcessSubscription {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProcessSubscription")
            .field("scope", &self.scope)
            .field("process_id", &self.process_id)
            .field("last_status", &self.last_status)
            .field(
                "pending_initial_status",
                &self.pending_initial.as_ref().map(|record| record.status),
            )
            .field("finished", &self.finished)
            .finish()
    }
}

impl ProcessSubscription {
    pub async fn next(&mut self) -> Result<Option<ProcessRecord>, ProcessError> {
        if let Some(record) = self.pending_initial.take() {
            if record.status.is_terminal() {
                self.finished = true;
            }
            return Ok(Some(record));
        }

        if self.finished {
            return Ok(None);
        }

        loop {
            let record = match self
                .runtime
                .get_process_snapshot(GetProcessSnapshotRequest {
                    scope: self.scope.clone(),
                    process_id: self.process_id,
                })
                .await
            {
                Ok(snapshot) => process_record_from_snapshot(snapshot)?,
                Err(error) => return Err(map_process_journal_error(error)),
            };
            if Some(record.status) != self.last_status {
                self.last_status = Some(record.status);
                if record.status.is_terminal() {
                    self.finished = true;
                }
                return Ok(Some(record));
            }
            sleep(self.poll_interval).await;
        }
    }
}
