//! In-memory filesystem constructors for approval and gate-record tests.

use std::sync::Arc;

use ironclaw_filesystem::{InMemoryBackend, ScopedFilesystem};
use ironclaw_host_api::{MountAlias, MountGrant, MountPermissions, MountView, VirtualPath};

use crate::{ApprovalRequestStore, GateRecordStore};

/// A fresh filesystem mounting the approval-owned record aliases.
pub fn in_memory_backed_approval_filesystem() -> Arc<ScopedFilesystem<InMemoryBackend>> {
    let mounts = MountView::new(vec![
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
    .expect("static valid approval mount view"); // safety: test-support scaffolding, static literal
    Arc::new(ScopedFilesystem::with_fixed_view(
        Arc::new(InMemoryBackend::new()),
        mounts,
    ))
}

/// The production approval-request store over a fresh in-memory backend — the
/// drop-in replacement for the deleted `InMemoryApprovalRequestStore`.
pub fn in_memory_backed_approval_request_store() -> ApprovalRequestStore<InMemoryBackend> {
    ApprovalRequestStore::new(in_memory_backed_approval_filesystem())
}

/// The production gate-record store over a fresh in-memory backend.
pub fn in_memory_backed_gate_record_store() -> GateRecordStore<InMemoryBackend> {
    GateRecordStore::new(in_memory_backed_approval_filesystem())
}
