use chrono::Utc;
use ironclaw_host_api::{AgentId, ProjectId, TenantId, ThreadId, UserId};
use ironclaw_processes::GetProcessSnapshotRequest;
use std::sync::Arc;

use super::*;
use crate::{
    AcceptedMessageRef, AdmissionRejection, CapabilityActivityId, EventCursor, GateRef,
    IdempotencyKey, InMemoryRunProfileResolver, LoopExitMapping, ReplyTargetBindingRef,
    RunProfileRequest, RunProfileVersion, SourceBindingRef, SubmitTurnRequest, SubmitTurnResponse,
    TurnActor, TurnAdmissionPolicy, TurnId, TurnRunProfile, TurnScope, TurnStateStore,
    runner::ApplyValidatedLoopExitRequest,
};

struct AllowAllAdmissionPolicy;

impl TurnAdmissionPolicy for AllowAllAdmissionPolicy {
    fn check_submit(&self, _request: &SubmitTurnRequest) -> Result<(), AdmissionRejection> {
        Ok(())
    }
}

fn scope() -> TurnScope {
    TurnScope::new(
        TenantId::new("tenant-process-journal").expect("tenant"),
        Some(AgentId::new("agent-process-journal").expect("agent")),
        Some(ProjectId::new("project-process-journal").expect("project")),
        ThreadId::new("thread-process-journal").expect("thread"),
    )
}

fn profile() -> TurnRunProfile {
    serde_json::from_value(serde_json::json!({
        "id": "default",
        "version": 1,
        "allow_steering": false,
        "auto_queue_followups": false,
    }))
    .expect("profile")
}

fn record_with_status(status: TurnStatus) -> TurnRunRecord {
    TurnRunRecord {
        run_id: TurnRunId::new(),
        turn_id: TurnId::new(),
        scope: scope(),
        accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
            .expect("accepted"),
        source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
        reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
            .expect("reply"),
        status,
        profile: profile(),
        resolved_model_route: None,
        model_usage: None,
        checkpoint_id: None,
        gate_ref: GateKind::from_status(status)
            .map(|_| GateRef::new("gate:process-journal").expect("gate")),
        blocked_activity_id: None,
        credential_requirements: Vec::new(),
        failure: None,
        event_cursor: EventCursor(7),
        runner_id: None,
        lease_token: None,
        lease_expires_at: None,
        last_heartbeat_at: None,
        claim_count: 0,
        received_at: Utc::now(),
        parent_run_id: None,
        subagent_depth: 0,
        spawn_tree_root_run_id: None,
        product_context: None,
        resume_disposition: None,
    }
}

fn process_lease_request() -> ProcessLeaseRequest {
    let runner_id = TurnRunnerId::new();
    let lease_token = crate::TurnLeaseToken::new();
    ProcessLeaseRequest {
        process_id: process_id_from_turn_run_id(TurnRunId::new()),
        worker_id: ProcessWorkerId::from_trusted(runner_id.to_wire_string()),
        lease_token: ProcessLeaseToken::from_trusted(lease_token.to_wire_string()),
    }
}

fn submit_request(run_id: TurnRunId) -> SubmitTurnRequest {
    SubmitTurnRequest {
        requested_model: None,
        scope: scope(),
        actor: TurnActor::new(UserId::new("user:process").expect("user")),
        accepted_message_ref: AcceptedMessageRef::new("accepted-process-transition")
            .expect("accepted"),
        source_binding_ref: SourceBindingRef::new("source-process-transition").expect("source"),
        reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-transition")
            .expect("reply"),
        requested_run_profile: Some(RunProfileRequest::new("default").expect("profile")),
        idempotency_key: IdempotencyKey::new("idem-process-transition").expect("idempotency"),
        received_at: Utc::now(),
        requested_run_id: Some(run_id),
        parent_run_id: None,
        subagent_depth: 0,
        spawn_tree_root_run_id: None,
        product_context: None,
    }
}

#[test]
fn every_turn_status_maps_to_process_lifecycle_status() {
    let cases = [
        (TurnStatus::Queued, ProcessLifecycleStatus::Queued),
        (TurnStatus::Running, ProcessLifecycleStatus::Running),
        (
            TurnStatus::BlockedApproval,
            ProcessLifecycleStatus::Suspended,
        ),
        (TurnStatus::BlockedAuth, ProcessLifecycleStatus::Suspended),
        (
            TurnStatus::BlockedResource,
            ProcessLifecycleStatus::Suspended,
        ),
        (
            TurnStatus::BlockedDependentRun,
            ProcessLifecycleStatus::Suspended,
        ),
        (
            TurnStatus::BlockedExternalTool,
            ProcessLifecycleStatus::Suspended,
        ),
        (
            TurnStatus::CancelRequested,
            ProcessLifecycleStatus::CancelRequested,
        ),
        (TurnStatus::Cancelled, ProcessLifecycleStatus::Cancelled),
        (TurnStatus::Completed, ProcessLifecycleStatus::Completed),
        (TurnStatus::Failed, ProcessLifecycleStatus::Failed),
        (
            TurnStatus::RecoveryRequired,
            ProcessLifecycleStatus::RecoveryRequired,
        ),
    ];

    for (turn_status, process_status) in cases {
        assert_eq!(process_status_from_turn_status(turn_status), process_status);
        assert_eq!(
            process_status_from_turn_status(turn_status).keeps_active_lock(),
            turn_status.keeps_active_lock()
        );
    }
}

#[test]
fn blocked_turn_statuses_map_to_process_suspension_kinds() {
    let cases = [
        (TurnStatus::BlockedApproval, ProcessSuspensionKind::Approval),
        (
            TurnStatus::BlockedAuth,
            ProcessSuspensionKind::Authorization,
        ),
        (TurnStatus::BlockedResource, ProcessSuspensionKind::Resource),
        (
            TurnStatus::BlockedDependentRun,
            ProcessSuspensionKind::AwaitingChildProcess,
        ),
        (
            TurnStatus::BlockedExternalTool,
            ProcessSuspensionKind::ExternalTool,
        ),
    ];

    for (turn_status, suspension_kind) in cases {
        let snapshot = record_with_status(turn_status).to_process_snapshot();
        assert_eq!(snapshot.status, ProcessLifecycleStatus::Suspended);
        assert_eq!(
            snapshot.suspension.expect("suspension").kind,
            suspension_kind
        );
    }
}

#[test]
fn every_turn_event_kind_maps_to_process_journal_kind() {
    let cases = [
        (TurnEventKind::Submitted, ProcessJournalKind::Submitted),
        (TurnEventKind::Resumed, ProcessJournalKind::Resumed),
        (TurnEventKind::RunnerClaimed, ProcessJournalKind::Claimed),
        (
            TurnEventKind::RunnerHeartbeat,
            ProcessJournalKind::Heartbeat,
        ),
        (
            TurnEventKind::RecoveryRequired,
            ProcessJournalKind::RecoveryRequired,
        ),
        (TurnEventKind::Blocked, ProcessJournalKind::Suspended),
        (
            TurnEventKind::CancelRequested,
            ProcessJournalKind::CancelRequested,
        ),
        (TurnEventKind::Cancelled, ProcessJournalKind::Cancelled),
        (TurnEventKind::Completed, ProcessJournalKind::Completed),
        (TurnEventKind::Failed, ProcessJournalKind::Failed),
    ];

    for (turn_kind, process_kind) in cases {
        assert_eq!(
            process_journal_kind_from_turn_event_kind(turn_kind),
            process_kind
        );
    }
}

#[test]
fn lifecycle_event_projects_to_process_journal_entry() {
    let state = crate::TurnRunState {
        scope: scope(),
        actor: Some(TurnActor::new(UserId::new("user:process").expect("user"))),
        turn_id: TurnId::new(),
        run_id: TurnRunId::new(),
        status: TurnStatus::BlockedAuth,
        accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
            .expect("accepted"),
        source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
        reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
            .expect("reply"),
        resolved_run_profile_id: RunProfileId::default_profile(),
        resolved_run_profile_version: RunProfileVersion::new(1),
        resolved_model_route: None,
        model_usage: None,
        received_at: Utc::now(),
        checkpoint_id: None,
        gate_ref: Some(GateRef::new("gate:process-journal").expect("gate")),
        blocked_activity_id: Some(CapabilityActivityId::new()),
        credential_requirements: Vec::new(),
        failure: None,
        event_cursor: EventCursor(9),
        product_context: None,
        resume_disposition: None,
    };
    let event = TurnLifecycleEvent::from_run_state(
        &state,
        TurnEventKind::Blocked,
        Some("auth_required".to_string()),
    );

    let entry = event.to_process_journal_entry();

    assert_eq!(entry.cursor, ProcessJournalCursor(9));
    assert_eq!(entry.process_id, process_id_from_turn_run_id(state.run_id));
    assert_eq!(entry.status, ProcessLifecycleStatus::Suspended);
    assert_eq!(entry.kind, ProcessJournalKind::Suspended);
    assert_eq!(
        entry.suspension.expect("suspension").kind,
        ProcessSuspensionKind::Authorization
    );
    assert_eq!(entry.sanitized_reason.as_deref(), Some("auth_required"));
}

#[test]
fn claimed_turn_run_projects_to_process_claim() {
    let state = crate::TurnRunState {
        scope: scope(),
        actor: Some(TurnActor::new(UserId::new("user:process").expect("user"))),
        turn_id: TurnId::new(),
        run_id: TurnRunId::new(),
        status: TurnStatus::Running,
        accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
            .expect("accepted"),
        source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
        reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
            .expect("reply"),
        resolved_run_profile_id: RunProfileId::default_profile(),
        resolved_run_profile_version: RunProfileVersion::new(1),
        resolved_model_route: None,
        model_usage: None,
        received_at: Utc::now(),
        checkpoint_id: None,
        gate_ref: None,
        blocked_activity_id: None,
        credential_requirements: Vec::new(),
        failure: None,
        event_cursor: EventCursor(11),
        product_context: None,
        resume_disposition: None,
    };
    let claimed = ClaimedTurnRun {
        state: state.clone(),
        resolved_run_profile: profile().resolved,
        runner_id: TurnRunnerId::new(),
        lease_token: crate::TurnLeaseToken::new(),
    };

    let process = ClaimedProcess::from(&claimed);

    assert_eq!(
        process.state.process_id,
        process_id_from_turn_run_id(state.run_id)
    );
    assert_eq!(process.state.status, ProcessLifecycleStatus::Running);
    assert_eq!(
        process.state.metadata["agent_turn"]["turn_id"],
        json!(state.turn_id)
    );
}

#[test]
fn claimed_process_round_trips_to_turn_executor_view() {
    let state = crate::TurnRunState {
        scope: scope(),
        actor: Some(TurnActor::new(UserId::new("user:process").expect("user"))),
        turn_id: TurnId::new(),
        run_id: TurnRunId::new(),
        status: TurnStatus::Running,
        accepted_message_ref: AcceptedMessageRef::new("accepted-process-journal")
            .expect("accepted"),
        source_binding_ref: SourceBindingRef::new("source-process-journal").expect("source"),
        reply_target_binding_ref: ReplyTargetBindingRef::new("reply-process-journal")
            .expect("reply"),
        resolved_run_profile_id: RunProfileId::default_profile(),
        resolved_run_profile_version: RunProfileVersion::new(1),
        resolved_model_route: None,
        model_usage: None,
        received_at: Utc::now(),
        checkpoint_id: None,
        gate_ref: None,
        blocked_activity_id: None,
        credential_requirements: Vec::new(),
        failure: None,
        event_cursor: EventCursor(12),
        product_context: None,
        resume_disposition: None,
    };
    let claimed = ClaimedTurnRun {
        state: state.clone(),
        resolved_run_profile: profile().resolved,
        runner_id: TurnRunnerId::new(),
        lease_token: crate::TurnLeaseToken::new(),
    };
    let process_claim = ClaimedProcess::from(&claimed);

    let round_trip = claimed_turn_run_from_process_claim(process_claim).expect("claimed turn view");

    assert_eq!(round_trip.state, state);
    assert_eq!(round_trip.runner_id, claimed.runner_id);
    assert_eq!(round_trip.lease_token, claimed.lease_token);
    assert_eq!(
        round_trip.resolved_run_profile,
        claimed.resolved_run_profile
    );
}

#[tokio::test]
async fn process_transition_adapter_drives_real_turn_store_transitions() {
    let store = Arc::new(crate::test_support::in_memory_turn_state_store());
    let transitions: Arc<dyn TurnRunTransitionPort> = store.clone();
    let adapter = AgentTurnProcessTransitionAdapter::new(transitions);
    let run_id = TurnRunId::new();
    let response = store
        .submit_turn(
            submit_request(run_id),
            &AllowAllAdmissionPolicy,
            &InMemoryRunProfileResolver::default(),
        )
        .await
        .expect("submit turn");
    let SubmitTurnResponse::Accepted {
        run_id: accepted_run_id,
        ..
    } = response;
    assert_eq!(accepted_run_id, run_id);

    let worker_id = ProcessWorkerId::from_trusted(TurnRunnerId::new().to_wire_string());
    let lease_token =
        ProcessLeaseToken::from_trusted(crate::TurnLeaseToken::new().to_wire_string());
    let claimed = adapter
        .claim_next_process(ClaimProcessRequest {
            worker_id: worker_id.clone(),
            lease_token: lease_token.clone(),
            scope_filter: None,
        })
        .await
        .expect("claim process")
        .expect("claimed process");
    assert_eq!(
        claimed.state.process_id,
        process_id_from_turn_run_id(run_id)
    );
    assert_eq!(claimed.state.status, ProcessLifecycleStatus::Running);
    assert_eq!(claimed.worker_id, worker_id);
    assert_eq!(claimed.lease_token, lease_token);

    let cursor = adapter
        .heartbeat_process(ProcessLeaseRequest {
            process_id: claimed.state.process_id,
            worker_id: worker_id.clone(),
            lease_token: lease_token.clone(),
        })
        .await
        .expect("heartbeat process");
    assert!(cursor.0 >= claimed.state.journal_cursor.0);

    let completed = adapter
        .complete_process(ProcessLeaseRequest {
            process_id: claimed.state.process_id,
            worker_id,
            lease_token,
        })
        .await
        .expect("complete process");
    assert_eq!(completed.status, ProcessLifecycleStatus::Completed);
}

#[tokio::test]
async fn turn_event_projection_can_be_a_view_over_process_journal() {
    let run_id = TurnRunId::new();
    let process_source: Arc<dyn ProcessJournalSource<Error = TurnError>> =
        Arc::new(FakeProcessJournalSource {
            page: ProcessJournalPage {
                entries: vec![ProcessJournalEntry {
                    cursor: ProcessJournalCursor(1),
                    process_id: process_id_from_turn_run_id(run_id),
                    process_kind: ProcessKind::AgentTurn,
                    scope: scope().to_resource_scope(),
                    occurred_at: Some(Utc::now()),
                    owner_user_id: None,
                    status: ProcessLifecycleStatus::Queued,
                    kind: ProcessJournalKind::Submitted,
                    suspension: None,
                    sanitized_reason: None,
                    retryable: None,
                    detail: None,
                    metadata: Value::Null,
                }],
                next_cursor: ProcessJournalCursor(1),
                truncated: false,
                rebase_required: None,
            },
        });
    let turn_view = TurnEventProjectionFromProcessJournal::new(process_source);

    let page = turn_view
        .read_turn_events_after(&scope(), None, None, 10)
        .await
        .expect("turn view page");
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].run_id, run_id);
    assert_eq!(page.entries[0].kind, TurnEventKind::Submitted);
    assert_eq!(page.entries[0].status, TurnStatus::Queued);
}

struct FakeProcessJournalSource {
    page: ProcessJournalPage,
}

#[async_trait]
impl ProcessJournalSource for FakeProcessJournalSource {
    type Error = TurnError;

    async fn get_process_snapshot(
        &self,
        _request: GetProcessSnapshotRequest,
    ) -> Result<JournaledProcessSnapshot, Self::Error> {
        Err(TurnError::InvalidRequest {
            reason: "fake process journal source does not serve snapshots".to_string(),
        })
    }

    async fn read_process_journal_after(
        &self,
        _scope: &ResourceScope,
        _owner_user_id: Option<&ironclaw_host_api::UserId>,
        _after: Option<ProcessJournalCursor>,
        _limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error> {
        Ok(self.page.clone())
    }

    async fn read_process_journal_log_after(
        &self,
        _after: Option<ProcessJournalCursor>,
        _limit: usize,
    ) -> Result<ProcessJournalPage, Self::Error> {
        Ok(self.page.clone())
    }
}

#[test]
fn process_lease_request_maps_to_runner_lease_requests() {
    let request = process_lease_request();

    let heartbeat = HeartbeatRequest::try_from(request.clone()).expect("heartbeat");
    assert_eq!(
        heartbeat.run_id,
        turn_run_id_from_process_id(request.process_id)
    );

    let complete = CompleteRunRequest::try_from(request.clone()).expect("complete");
    assert_eq!(
        complete.run_id,
        turn_run_id_from_process_id(request.process_id)
    );

    let cancel = CancelRunCompletionRequest::try_from(request.clone()).expect("cancel");
    assert_eq!(
        cancel.run_id,
        turn_run_id_from_process_id(request.process_id)
    );

    let relinquish = RelinquishRunRequest::try_from(request.clone()).expect("relinquish");
    assert_eq!(
        relinquish.run_id,
        turn_run_id_from_process_id(request.process_id)
    );
}

#[test]
fn runner_outcomes_map_to_process_outcomes() {
    let failure = crate::SanitizedFailure::new("runner_failed").expect("failure");
    let blocked = TurnRunnerOutcome::Blocked {
        checkpoint_id: TurnCheckpointId::new(),
        state_ref: crate::run_profile::LoopCheckpointStateRef::new(
            "checkpoint:state-process-journal".to_string(),
        )
        .expect("state ref"),
        reason: BlockedReason::ExternalTool {
            gate_ref: GateRef::new("gate:process-journal").expect("gate"),
        },
        blocked_activity_id: Some(CapabilityActivityId::new()),
    };

    assert_eq!(
        process_outcome_from_turn_runner_outcome(TurnRunnerOutcome::Completed),
        ProcessOutcome::Completed
    );
    assert_eq!(
        process_outcome_from_turn_runner_outcome(TurnRunnerOutcome::Cancelled),
        ProcessOutcome::Cancelled
    );
    assert!(matches!(
        process_outcome_from_turn_runner_outcome(blocked),
        ProcessOutcome::Suspended { .. }
    ));
    assert_eq!(
        process_outcome_from_turn_runner_outcome(TurnRunnerOutcome::Failed {
            failure: failure.clone()
        }),
        ProcessOutcome::Failed { failure }
    );
}

#[test]
fn validated_exit_request_preserves_agent_loop_residue() {
    let request = process_lease_request();
    let loop_exit = ApplyValidatedLoopExitRequest {
        run_id: turn_run_id_from_process_id(request.process_id),
        runner_id: turn_runner_id_from_worker(&request.worker_id).expect("runner id"),
        lease_token: turn_lease_token_from_process(&request.lease_token).expect("lease token"),
        mapping: LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Completed),
        model_usage: None,
    };

    assert_eq!(
        loop_exit.run_id,
        turn_run_id_from_process_id(request.process_id)
    );
    assert_eq!(
        loop_exit.mapping,
        LoopExitMapping::RunnerOutcome(TurnRunnerOutcome::Completed)
    );
}
