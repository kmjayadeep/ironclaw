use chrono::Utc;
use ironclaw_filesystem::{InMemoryBackend, ScopedFilesystem};
use ironclaw_host_api::{
    AgentId, InvocationId, MountAlias, MountGrant, MountPermissions, MountView, ProcessId,
    ProjectId, ResourceScope, TenantId, ThreadId, TurnGateRef, UserId, VirtualPath,
};
use ironclaw_processes::{
    CancelProcessRequest, ClaimProcessesRequest, GetProcessSnapshotRequest, KillProcessRequest,
    ProcessCheckpointRef, ProcessControlPort, ProcessGateOwnerMatch, ProcessGateQuery,
    ProcessGateQuerySource, ProcessJournalCursor, ProcessJournalSource, ProcessJournalStore,
    ProcessKind, ProcessLeaseRequest, ProcessLeaseToken, ProcessLifecycleLookupBatchRequest,
    ProcessLifecycleLookupRequest, ProcessLifecycleLookupResult, ProcessLifecycleLookupSource,
    ProcessLifecycleStatus, ProcessStateTransitionRequest, ProcessSubmissionPort,
    ProcessSuspension, ProcessSuspensionKind, ProcessTransitionPort, ProcessWorkerId,
    ResumeProcessRequest, StopProcessRequest, SubmitProcessRequest, SuspendProcessRequest,
};
use serde_json::json;

#[tokio::test]
async fn process_journal_store_owns_lifecycle_and_gate_projection() {
    let store = ProcessJournalStore::new(in_memory_backed_processes_filesystem());
    let scope = scope();
    let owner = scope.user_id.clone();
    let process_id = ProcessId::new();
    let worker_id = ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string());

    let submitted = store
        .submit_process(SubmitProcessRequest {
            process_id,
            process_kind: ProcessKind::AgentTurn,
            scope: scope.clone(),
            exclusive_within_scope: false,
            owner_user_id: Some(owner.clone()),
            parent_process_id: None,
            root_process_id: None,
            created_at: Utc::now(),
            metadata: json!({
                "agent_turn": {
                    "source_binding_ref": "source:journal-contract",
                    "reply_target_binding_ref": "reply:journal-contract"
                }
            }),
        })
        .await
        .expect("submit process");
    assert_eq!(submitted.status, ProcessLifecycleStatus::Queued);

    let claimed = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id: worker_id.clone(),
            scope_filter: Some(scope.clone()),
            max_processes: 1,
        })
        .await
        .expect("claim process");
    assert_eq!(claimed.len(), 1);
    let claim = &claimed[0];
    assert_eq!(claim.state.process_id, process_id);
    assert_eq!(claim.state.status, ProcessLifecycleStatus::Running);

    let lease = ProcessLeaseRequest {
        process_id,
        worker_id: claim.worker_id.clone(),
        lease_token: claim.lease_token.clone(),
    };
    store
        .heartbeat_process(lease.clone())
        .await
        .expect("heartbeat process");

    let gate_ref = TurnGateRef::new("gate:journal-contract").expect("gate ref");
    store
        .suspend_process(SuspendProcessRequest {
            process_id,
            worker_id: lease.worker_id.clone(),
            lease_token: lease.lease_token.clone(),
            checkpoint_ref: ProcessCheckpointRef::new("checkpoint:journal-contract")
                .expect("checkpoint ref"),
            suspension: ProcessSuspension {
                kind: ProcessSuspensionKind::Authorization,
                gate_ref: Some(gate_ref.clone()),
                activity_id: None,
                credential_requirements: Vec::new(),
                detail: None,
            },
            metadata: None,
        })
        .await
        .expect("suspend process");

    let lifecycle = store
        .process_lifecycle_states(ProcessLifecycleLookupBatchRequest {
            processes: vec![ProcessLifecycleLookupRequest {
                tenant_id: scope.tenant_id.clone(),
                process_id,
            }],
        })
        .await
        .pop()
        .expect("one lifecycle result")
        .expect("lifecycle lookup");
    assert!(matches!(
        lifecycle,
        ProcessLifecycleLookupResult::Found {
            status: ProcessLifecycleStatus::Suspended,
            ..
        }
    ));

    let gates = store
        .query_process_gates(ProcessGateQuery {
            scope: scope.clone(),
            gate_kind: ProcessSuspensionKind::Authorization,
            owner_user_id: Some(owner),
            gate_ref: Some(gate_ref.clone()),
            owner_match: Some(ProcessGateOwnerMatch::Explicit),
            include_historical: false,
        })
        .await
        .expect("query gates");
    assert_eq!(gates.len(), 1);
    assert_eq!(gates[0].process_id, process_id);
    assert_eq!(gates[0].suspension.gate_ref.as_ref(), Some(&gate_ref));
    assert_eq!(
        gates[0].resume_source_ref.as_deref(),
        Some("source:journal-contract")
    );
    assert_eq!(
        gates[0].reply_target_ref.as_deref(),
        Some("reply:journal-contract")
    );

    let snapshot = store
        .get_process_snapshot(GetProcessSnapshotRequest {
            scope: scope.clone(),
            process_id,
        })
        .await
        .expect("process snapshot");
    assert_eq!(snapshot.status, ProcessLifecycleStatus::Suspended);

    let page = store
        .read_process_journal_after(&scope, None, Some(ProcessJournalCursor(0)), 10)
        .await
        .expect("journal page");
    assert_eq!(page.entries.len(), 4);
    assert_eq!(page.entries[0].status, ProcessLifecycleStatus::Queued);
    assert_eq!(page.entries[3].status, ProcessLifecycleStatus::Suspended);
}

#[tokio::test]
async fn process_journal_store_completes_claimed_process() {
    let store = ProcessJournalStore::new(in_memory_backed_processes_filesystem());
    let scope = scope();
    let process_id = ProcessId::new();
    let worker_id = ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string());
    store
        .submit_process(SubmitProcessRequest {
            process_id,
            process_kind: ProcessKind::Internal,
            scope: scope.clone(),
            exclusive_within_scope: false,
            owner_user_id: Some(scope.user_id.clone()),
            parent_process_id: None,
            root_process_id: None,
            created_at: Utc::now(),
            metadata: serde_json::Value::Null,
        })
        .await
        .expect("submit process");
    let mut claimed = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id,
            scope_filter: Some(scope.clone()),
            max_processes: 1,
        })
        .await
        .expect("claim process");
    let claim = claimed.pop().expect("claimed process");
    let completed = store
        .complete_process(ProcessStateTransitionRequest {
            lease: ProcessLeaseRequest {
                process_id,
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
            },
            metadata: Some(json!({"projection": {"usage": 42}})),
        })
        .await
        .expect("complete process");
    assert_eq!(completed.status, ProcessLifecycleStatus::Completed);
    assert!(completed.lease.is_none());
    assert_eq!(completed.metadata["projection"]["usage"], 42);
}

#[tokio::test]
async fn process_journal_store_relinquishes_claim_with_fresh_reclaim_lease() {
    let store = ProcessJournalStore::new(in_memory_backed_processes_filesystem());
    let scope = scope();
    let process_id = ProcessId::new();
    let first_worker = ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string());
    let second_worker = ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string());
    store
        .submit_process(SubmitProcessRequest {
            process_id,
            process_kind: ProcessKind::Internal,
            scope: scope.clone(),
            exclusive_within_scope: false,
            owner_user_id: Some(scope.user_id.clone()),
            parent_process_id: None,
            root_process_id: None,
            created_at: Utc::now(),
            metadata: serde_json::Value::Null,
        })
        .await
        .expect("submit process");

    let mut first_claim = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id: first_worker,
            scope_filter: Some(scope.clone()),
            max_processes: 1,
        })
        .await
        .expect("claim process");
    let first_claim = first_claim.pop().expect("claimed process");
    let relinquished = store
        .relinquish_process(ProcessLeaseRequest {
            process_id,
            worker_id: first_claim.worker_id,
            lease_token: first_claim.lease_token.clone(),
        })
        .await
        .expect("relinquish process");
    assert_eq!(relinquished.status, ProcessLifecycleStatus::Queued);
    assert!(relinquished.lease.is_none());

    let mut second_claim = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id: second_worker.clone(),
            scope_filter: Some(scope),
            max_processes: 1,
        })
        .await
        .expect("reclaim process");
    let second_claim = second_claim.pop().expect("reclaimed process");
    assert_eq!(second_claim.worker_id, second_worker);
    assert_ne!(second_claim.lease_token, first_claim.lease_token);
}

#[tokio::test]
async fn process_journal_store_rejects_wrong_lease() {
    let store = ProcessJournalStore::new(in_memory_backed_processes_filesystem());
    let scope = scope();
    let process_id = ProcessId::new();
    store
        .submit_process(SubmitProcessRequest {
            process_id,
            process_kind: ProcessKind::Internal,
            scope: scope.clone(),
            exclusive_within_scope: false,
            owner_user_id: Some(scope.user_id.clone()),
            parent_process_id: None,
            root_process_id: None,
            created_at: Utc::now(),
            metadata: serde_json::Value::Null,
        })
        .await
        .expect("submit process");
    let mut claimed = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id: ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string()),
            scope_filter: Some(scope),
            max_processes: 1,
        })
        .await
        .expect("claim process");
    let claim = claimed.pop().expect("claimed process");
    let error = store
        .complete_process(ProcessStateTransitionRequest {
            lease: ProcessLeaseRequest {
                process_id,
                worker_id: claim.worker_id,
                lease_token: ProcessLeaseToken::from_trusted(
                    ProcessId::new().as_uuid().to_string(),
                ),
            },
            metadata: None,
        })
        .await
        .expect_err("wrong lease must fail");
    assert!(error.to_string().contains("lease is invalid"));
}

#[tokio::test]
async fn process_control_is_scoped_atomic_and_process_kind_neutral() {
    let store = ProcessJournalStore::new(in_memory_backed_processes_filesystem());
    let scope = scope();
    let process_id = ProcessId::new();
    let submitted = submit_internal_process(&store, &scope, process_id).await;
    let worker_id = ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string());
    let mut claimed = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id,
            scope_filter: Some(scope.clone()),
            max_processes: 1,
        })
        .await
        .expect("claim process");
    let claim = claimed.pop().expect("claimed process");
    let suspended = store
        .suspend_process(SuspendProcessRequest {
            process_id,
            worker_id: claim.worker_id,
            lease_token: claim.lease_token,
            checkpoint_ref: ProcessCheckpointRef::from_trusted("checkpoint:control"),
            suspension: ProcessSuspension {
                kind: ProcessSuspensionKind::ExternalProcess,
                gate_ref: None,
                activity_id: None,
                credential_requirements: Vec::new(),
                detail: None,
            },
            metadata: None,
        })
        .await
        .expect("suspend process");

    let stale = store
        .resume_process(ResumeProcessRequest {
            scope: scope.clone(),
            process_id,
            operation_id: None,
            expected_cursor: Some(submitted.journal_cursor),
            checkpoint_ref: None,
            metadata: None,
        })
        .await
        .expect_err("stale resume must fail");
    assert!(stale.to_string().contains("changed after cursor"));

    let mut wrong_scope = scope.clone();
    wrong_scope.user_id = UserId::new("other-user").expect("other user");
    let unauthorized = store
        .resume_process(ResumeProcessRequest {
            scope: wrong_scope,
            process_id,
            operation_id: None,
            expected_cursor: Some(suspended.journal_cursor),
            checkpoint_ref: None,
            metadata: None,
        })
        .await
        .expect_err("cross-scope resume must not disclose process");
    assert!(unauthorized.to_string().contains("unknown process"));

    let resumed = store
        .resume_process(ResumeProcessRequest {
            scope: scope.clone(),
            process_id,
            operation_id: Some(ironclaw_processes::ProcessOperationId::from_trusted(
                "resume:control",
            )),
            expected_cursor: Some(suspended.journal_cursor),
            checkpoint_ref: None,
            metadata: Some(json!({"resumed": true})),
        })
        .await
        .expect("resume process");
    assert!(resumed.changed);
    assert_eq!(resumed.state.status, ProcessLifecycleStatus::Queued);
    assert!(resumed.state.suspension.is_none());
    assert_eq!(resumed.state.metadata["resumed"], true);
    let replayed = store
        .resume_process(ResumeProcessRequest {
            scope: scope.clone(),
            process_id,
            operation_id: Some(ironclaw_processes::ProcessOperationId::from_trusted(
                "resume:control",
            )),
            expected_cursor: Some(suspended.journal_cursor),
            checkpoint_ref: None,
            metadata: None,
        })
        .await
        .expect("replay resume");
    assert_eq!(replayed, resumed);

    let mut reclaimed = store
        .claim_next_processes(ClaimProcessesRequest {
            worker_id: ProcessWorkerId::from_trusted(ProcessId::new().as_uuid().to_string()),
            scope_filter: Some(scope.clone()),
            max_processes: 1,
        })
        .await
        .expect("reclaim process");
    let claim = reclaimed.pop().expect("reclaimed process");
    let cancel_requested = store
        .request_cancel_process(CancelProcessRequest {
            scope: scope.clone(),
            process_id,
            operation_id: None,
            reason: Some("operator request".to_string()),
        })
        .await
        .expect("request cancellation");
    assert_eq!(
        cancel_requested.state.status,
        ProcessLifecycleStatus::CancelRequested
    );
    assert!(cancel_requested.state.lease.is_some());
    let cancelled = store
        .cancel_process(ProcessStateTransitionRequest {
            lease: ProcessLeaseRequest {
                process_id,
                worker_id: claim.worker_id,
                lease_token: claim.lease_token,
            },
            metadata: None,
        })
        .await
        .expect("complete cancellation");
    assert_eq!(cancelled.status, ProcessLifecycleStatus::Cancelled);

    let stopped_id = ProcessId::new();
    submit_internal_process(&store, &scope, stopped_id).await;
    let stopped = store
        .stop_process(StopProcessRequest {
            scope: scope.clone(),
            process_id: stopped_id,
            operation_id: None,
            reason: Some("shutdown".to_string()),
        })
        .await
        .expect("stop process");
    assert_eq!(stopped.state.status, ProcessLifecycleStatus::Stopped);

    let killed_id = ProcessId::new();
    submit_internal_process(&store, &scope, killed_id).await;
    let killed = store
        .kill_process(KillProcessRequest {
            scope,
            process_id: killed_id,
            operation_id: None,
            reason: Some("forced shutdown".to_string()),
        })
        .await
        .expect("kill process");
    assert_eq!(killed.state.status, ProcessLifecycleStatus::Killed);
}

#[tokio::test]
async fn exclusive_process_submission_uses_authoritative_live_projection() {
    let store = ProcessJournalStore::new(in_memory_backed_processes_filesystem());
    let scope = scope();
    let first_id = ProcessId::new();
    let request = |process_id| SubmitProcessRequest {
        process_id,
        process_kind: ProcessKind::AgentTurn,
        scope: scope.clone(),
        exclusive_within_scope: true,
        owner_user_id: Some(scope.user_id.clone()),
        parent_process_id: None,
        root_process_id: None,
        created_at: Utc::now(),
        metadata: serde_json::Value::Null,
    };
    store
        .submit_process(request(first_id))
        .await
        .expect("submit exclusive process");
    let conflict = store
        .submit_process(request(ProcessId::new()))
        .await
        .expect_err("second live process in scope must conflict");
    assert!(conflict.to_string().contains(&first_id.to_string()));

    store
        .stop_process(StopProcessRequest {
            scope: scope.clone(),
            process_id: first_id,
            operation_id: None,
            reason: None,
        })
        .await
        .expect("stop first process");
    let replacement = store
        .submit_process(request(ProcessId::new()))
        .await
        .expect("terminal process releases exclusive scope");
    assert_eq!(replacement.status, ProcessLifecycleStatus::Queued);
}

async fn submit_internal_process(
    store: &ProcessJournalStore<InMemoryBackend>,
    scope: &ResourceScope,
    process_id: ProcessId,
) -> ironclaw_processes::JournaledProcessSnapshot {
    store
        .submit_process(SubmitProcessRequest {
            process_id,
            process_kind: ProcessKind::Internal,
            scope: scope.clone(),
            exclusive_within_scope: false,
            owner_user_id: Some(scope.user_id.clone()),
            parent_process_id: None,
            root_process_id: None,
            created_at: Utc::now(),
            metadata: serde_json::Value::Null,
        })
        .await
        .expect("submit internal process")
}

fn scope() -> ResourceScope {
    ResourceScope {
        tenant_id: TenantId::new("tenant-journal").expect("tenant"),
        user_id: UserId::new("user-journal").expect("user"),
        agent_id: Some(AgentId::new("agent-journal").expect("agent")),
        project_id: Some(ProjectId::new("project-journal").expect("project")),
        mission_id: None,
        thread_id: Some(ThreadId::new("thread-journal").expect("thread")),
        invocation_id: InvocationId::new(),
    }
}

fn in_memory_backed_processes_filesystem() -> std::sync::Arc<ScopedFilesystem<InMemoryBackend>> {
    let mounts = MountView::new(vec![MountGrant::new(
        MountAlias::new("/processes").expect("mount alias"),
        VirtualPath::new("/engine/processes").expect("virtual path"),
        MountPermissions::read_write_list_delete(),
    )])
    .expect("mount view");
    std::sync::Arc::new(ScopedFilesystem::with_fixed_view(
        std::sync::Arc::new(InMemoryBackend::new()),
        mounts,
    ))
}
