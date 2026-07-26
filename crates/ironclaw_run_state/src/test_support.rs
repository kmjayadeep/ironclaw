//! In-memory-backed run-state / approval-request store constructors for tests.
//!
//! The Reborn architecture-simplification note
//! (`docs/reborn/contracts/run-state.md`)
//! replaces the hand-written `InMemory*Store` parallel implementations with the
//! one production `Filesystem*Store<F>` exercised over an in-memory backend:
//! "in-memory" stops being a store and becomes a filesystem backend
//! (`InMemoryBackend`). These helpers wire that seam once — a
//! `ScopedFilesystem<InMemoryBackend>` mounting both `/run-state` and
//! `/approvals` (the two aliases this crate persists under) — so tests
//! instantiate the same store a deployment runs.
//!
//! Note on isolation: the run-state/approval stores encode
//! agent/project/mission/thread in the path (structural under any mount) while
//! tenant/user isolation lives in the `MountView`. The single fixed mount below
//! therefore isolates by agent/project/mission/thread but not by tenant/user —
//! which matches single-tenant state-machine tests; cross-tenant isolation is
//! exercised by the per-tenant-mount tests in the contract suites.
//!
//! Run-state and approval records live under sibling aliases on **one** backend,
//! so a single `in_memory_backed_run_state_filesystem()` feeds both stores — an
//! approval resolution that reads the blocked run and its approval record sees a
//! consistent view.
//!
//! Gated behind `#[cfg(any(test, feature = "test-support"))]` and disabled by
//! default. Downstream crates should enable `test-support` only from their
//! `[dev-dependencies]`.

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use ironclaw_filesystem::{InMemoryBackend, ScopedFilesystem};
use ironclaw_host_api::{
    ApprovalRequest, InvocationId, MountAlias, MountGrant, MountPermissions, MountView,
    ResourceScope, VirtualPath,
};

use crate::{
    ApprovalRequestStore, GateRecordStore, RunRecord, RunStart, RunStateError, RunStateStorePort,
    RunStatus, same_scope_owner,
};

/// Minimal lifecycle fake retained for downstream seam tests. Production
/// invocation state is projected from `ironclaw_processes::ProcessJournalStore`.
pub struct RunStateStore<F> {
    records: Mutex<Vec<RunRecord>>,
    backend: PhantomData<F>,
}

impl<F> RunStateStore<F> {
    pub fn new(_filesystem: Arc<ScopedFilesystem<F>>) -> Self
    where
        F: ironclaw_filesystem::RootFilesystem,
    {
        Self {
            records: Mutex::new(Vec::new()),
            backend: PhantomData,
        }
    }

    fn update(
        &self,
        scope: &ResourceScope,
        invocation_id: InvocationId,
        mutate: impl FnOnce(&mut RunRecord),
    ) -> Result<RunRecord, RunStateError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| RunStateError::Backend("test run-state mutex poisoned".to_string()))?;
        let record = records
            .iter_mut()
            .find(|record| {
                record.invocation_id == invocation_id && same_scope_owner(&record.scope, scope)
            })
            .ok_or(RunStateError::UnknownInvocation { invocation_id })?;
        mutate(record);
        Ok(record.clone())
    }
}

#[async_trait]
impl<F> RunStateStorePort for RunStateStore<F>
where
    F: ironclaw_filesystem::RootFilesystem + Send + Sync,
{
    async fn start(&self, start: RunStart) -> Result<RunRecord, RunStateError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| RunStateError::Backend("test run-state mutex poisoned".to_string()))?;
        if records.iter().any(|record| {
            record.invocation_id == start.invocation_id
                && same_scope_owner(&record.scope, &start.scope)
        }) {
            return Err(RunStateError::InvocationAlreadyExists {
                invocation_id: start.invocation_id,
            });
        }
        let record = RunRecord {
            invocation_id: start.invocation_id,
            capability_id: start.capability_id,
            scope: start.scope,
            authenticated_actor_user_id: start.authenticated_actor_user_id,
            status: RunStatus::Running,
            approval_request_id: None,
            error_kind: None,
        };
        records.push(record.clone());
        Ok(record)
    }

    async fn block_approval(
        &self,
        scope: &ResourceScope,
        invocation_id: InvocationId,
        approval: ApprovalRequest,
    ) -> Result<RunRecord, RunStateError> {
        self.update(scope, invocation_id, |record| {
            record.status = RunStatus::BlockedApproval;
            record.approval_request_id = Some(approval.id);
            record.error_kind = None;
        })
    }

    async fn block_auth(
        &self,
        scope: &ResourceScope,
        invocation_id: InvocationId,
        error_kind: String,
    ) -> Result<RunRecord, RunStateError> {
        self.update(scope, invocation_id, |record| {
            record.status = RunStatus::BlockedAuth;
            record.approval_request_id = None;
            record.error_kind = Some(error_kind);
        })
    }

    async fn complete(
        &self,
        scope: &ResourceScope,
        invocation_id: InvocationId,
    ) -> Result<RunRecord, RunStateError> {
        self.update(scope, invocation_id, |record| {
            record.status = RunStatus::Completed;
            record.approval_request_id = None;
            record.error_kind = None;
        })
    }

    async fn fail(
        &self,
        scope: &ResourceScope,
        invocation_id: InvocationId,
        error_kind: String,
    ) -> Result<RunRecord, RunStateError> {
        self.update(scope, invocation_id, |record| {
            record.status = RunStatus::Failed;
            record.approval_request_id = None;
            record.error_kind = Some(error_kind);
        })
    }

    async fn get(
        &self,
        scope: &ResourceScope,
        invocation_id: InvocationId,
    ) -> Result<Option<RunRecord>, RunStateError> {
        let records = self
            .records
            .lock()
            .map_err(|_| RunStateError::Backend("test run-state mutex poisoned".to_string()))?;
        Ok(records
            .iter()
            .find(|record| {
                record.invocation_id == invocation_id && same_scope_owner(&record.scope, scope)
            })
            .cloned())
    }

    async fn records_for_scope(
        &self,
        scope: &ResourceScope,
    ) -> Result<Vec<RunRecord>, RunStateError> {
        let records = self
            .records
            .lock()
            .map_err(|_| RunStateError::Backend("test run-state mutex poisoned".to_string()))?;
        let mut visible = records
            .iter()
            .filter(|record| same_scope_owner(&record.scope, scope))
            .cloned()
            .collect::<Vec<_>>();
        visible.sort_by_key(|record| record.invocation_id.as_uuid());
        Ok(visible)
    }
}

/// A fresh, volatile `ScopedFilesystem<InMemoryBackend>` mounting `/run-state`,
/// `/approvals`, and `/gate-records` — the in-memory backend seam the run-state,
/// approval-request, and gate-record stores share in tests.
pub fn in_memory_backed_run_state_filesystem() -> Arc<ScopedFilesystem<InMemoryBackend>> {
    let mounts = MountView::new(vec![
        MountGrant::new(
            MountAlias::new("/run-state").expect("static valid mount alias"), // safety: test-support scaffolding, static literal
            VirtualPath::new("/engine/run-state").expect("static valid virtual path"), // safety: test-support scaffolding, static literal
            MountPermissions::read_write_list_delete(),
        ),
        MountGrant::new(
            MountAlias::new("/approvals").expect("static valid mount alias"), // safety: test-support scaffolding, static literal
            VirtualPath::new("/engine/approvals").expect("static valid virtual path"), // safety: test-support scaffolding, static literal
            MountPermissions::read_write_list_delete(),
        ),
        MountGrant::new(
            MountAlias::new("/gate-records").expect("static valid mount alias"), // safety: test-support scaffolding, static literal
            VirtualPath::new("/engine/gate-records").expect("static valid virtual path"), // safety: test-support scaffolding, static literal
            MountPermissions::read_write_list_delete(),
        ),
    ])
    .expect("static valid run-state mount view"); // safety: test-support scaffolding, static literal
    Arc::new(ScopedFilesystem::with_fixed_view(
        Arc::new(InMemoryBackend::new()),
        mounts,
    ))
}

/// The production run-state store over a fresh in-memory backend — the drop-in
/// replacement for the deleted `InMemoryRunStateStore`.
pub fn in_memory_backed_run_state_store() -> RunStateStore<InMemoryBackend> {
    RunStateStore::new(in_memory_backed_run_state_filesystem())
}

/// The production approval-request store over a fresh in-memory backend — the
/// drop-in replacement for the deleted `InMemoryApprovalRequestStore`.
pub fn in_memory_backed_approval_request_store() -> ApprovalRequestStore<InMemoryBackend> {
    ApprovalRequestStore::new(in_memory_backed_run_state_filesystem())
}

/// The production gate-record store over a fresh in-memory backend.
pub fn in_memory_backed_gate_record_store() -> GateRecordStore<InMemoryBackend> {
    GateRecordStore::new(in_memory_backed_run_state_filesystem())
}
