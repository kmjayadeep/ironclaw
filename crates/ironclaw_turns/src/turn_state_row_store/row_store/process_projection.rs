use async_trait::async_trait;
use ironclaw_filesystem::RootFilesystem;
use ironclaw_host_api::UserId;
use ironclaw_processes::{
    ProcessGateOwnerMatch, ProcessGateQuery, ProcessGateQuerySource, ProcessGateRecord,
    ProcessLifecycleLookupBatchRequest, ProcessLifecycleLookupResult, ProcessLifecycleLookupSource,
    ProcessSuspension, ProcessSuspensionKind,
};

use crate::process_journal::{
    process_id_from_turn_run_id, process_status_from_turn_status, process_suspension_from_record,
};
use crate::{TurnError, TurnScope, TurnStatus};

use super::TurnStateRowStore;

#[async_trait]
impl<F> ProcessLifecycleLookupSource for TurnStateRowStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = TurnError;

    async fn process_lifecycle_states(
        &self,
        request: ProcessLifecycleLookupBatchRequest,
    ) -> Vec<Result<ProcessLifecycleLookupResult, Self::Error>> {
        let snapshot = match self.persistence_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let reason = error.to_string();
                return request
                    .processes
                    .into_iter()
                    .map(|_| {
                        Err(TurnError::Unavailable {
                            reason: reason.clone(),
                        })
                    })
                    .collect();
            }
        };
        request
            .processes
            .into_iter()
            .map(|lookup| {
                let result = snapshot
                    .runs
                    .iter()
                    .find(|run| {
                        process_id_from_turn_run_id(run.run_id) == lookup.process_id
                            && run.scope.tenant_id == lookup.tenant_id
                    })
                    .map(|run| ProcessLifecycleLookupResult::Found {
                        status: process_status_from_turn_status(run.status),
                        suspension: process_suspension_from_record(run),
                    })
                    .unwrap_or(ProcessLifecycleLookupResult::Missing);
                Ok(result)
            })
            .collect()
    }
}

#[async_trait]
impl<F> ProcessGateQuerySource for TurnStateRowStore<F>
where
    F: RootFilesystem + Send + Sync + 'static,
{
    type Error = TurnError;

    async fn query_process_gates(
        &self,
        request: ProcessGateQuery,
    ) -> Result<Vec<ProcessGateRecord>, Self::Error> {
        let snapshot = self.persistence_snapshot().await?;
        let mut records = Vec::new();
        for run in snapshot.runs.iter().filter(|run| {
            run_gate_matches(request.gate_kind, run.status)
                && process_gate_scope_matches(&request.scope, &run.scope)
                && process_gate_ref_matches(request.gate_ref.as_ref(), run.gate_ref.as_ref())
                && process_gate_owner_matches(
                    request.owner_match,
                    request.owner_user_id.as_ref(),
                    &snapshot,
                    run,
                )
        }) {
            let Some(suspension) = process_suspension_from_record(run) else {
                continue;
            };
            records.push(ProcessGateRecord {
                process_id: process_id_from_turn_run_id(run.run_id),
                scope: run.scope.to_resource_scope(),
                owner_user_id: process_gate_owner_user_id(&snapshot, run).cloned(),
                suspension,
                resume_source_ref: Some(run.source_binding_ref.as_str().to_string()),
                reply_target_ref: Some(run.reply_target_binding_ref.as_str().to_string()),
                historical: false,
            });
        }

        if request.include_historical {
            for checkpoint in snapshot.checkpoints.iter().filter(|checkpoint| {
                run_gate_matches(request.gate_kind, checkpoint.status)
                    && process_gate_ref_matches(
                        request.gate_ref.as_ref(),
                        Some(&checkpoint.gate_ref),
                    )
            }) {
                let Some(run) = snapshot
                    .runs
                    .iter()
                    .find(|run| run.run_id == checkpoint.run_id)
                else {
                    continue;
                };
                if !checkpoint
                    .scope
                    .as_ref()
                    .is_none_or(|scope| process_gate_scope_matches(&request.scope, scope))
                    || !process_gate_scope_matches(&request.scope, &run.scope)
                    || !process_gate_owner_matches(
                        request.owner_match,
                        request.owner_user_id.as_ref(),
                        &snapshot,
                        run,
                    )
                {
                    continue;
                }
                records.push(ProcessGateRecord {
                    process_id: process_id_from_turn_run_id(run.run_id),
                    scope: run.scope.to_resource_scope(),
                    owner_user_id: process_gate_owner_user_id(&snapshot, run).cloned(),
                    suspension: ProcessSuspension {
                        kind: request.gate_kind,
                        gate_ref: Some(checkpoint.gate_ref.clone()),
                        activity_id: None,
                        credential_requirements: Vec::new(),
                        detail: None,
                    },
                    resume_source_ref: None,
                    reply_target_ref: None,
                    historical: true,
                });
            }
        }

        records.sort_by_key(|record| (record.process_id.as_uuid(), record.historical));
        records.dedup_by(|left, right| {
            left.process_id == right.process_id
                && left.suspension.gate_ref == right.suspension.gate_ref
                && left.historical == right.historical
        });
        Ok(records)
    }
}

fn run_gate_matches(kind: ProcessSuspensionKind, status: TurnStatus) -> bool {
    use ProcessSuspensionKind::{
        Approval, Authorization, AwaitingChildProcess, ExternalTool, Resource,
    };
    use TurnStatus::{
        BlockedApproval, BlockedAuth, BlockedDependentRun, BlockedExternalTool, BlockedResource,
    };
    matches!(
        (kind, status),
        (Approval, BlockedApproval)
            | (Authorization, BlockedAuth)
            | (Resource, BlockedResource)
            | (AwaitingChildProcess, BlockedDependentRun)
            | (ExternalTool, BlockedExternalTool)
    )
}

fn process_gate_ref_matches(
    requested: Option<&crate::GateRef>,
    candidate: Option<&crate::GateRef>,
) -> bool {
    requested.is_none_or(|requested| candidate == Some(requested))
}

fn process_gate_scope_matches(
    requested: &ironclaw_host_api::ResourceScope,
    scope: &TurnScope,
) -> bool {
    requested.tenant_id == scope.tenant_id
        && requested
            .agent_id
            .as_ref()
            .is_none_or(|agent_id| scope.agent_id.as_ref() == Some(agent_id))
        && requested
            .project_id
            .as_ref()
            .is_none_or(|project_id| scope.project_id.as_ref() == Some(project_id))
        && requested
            .thread_id
            .as_ref()
            .is_none_or(|thread_id| &scope.thread_id == thread_id)
}

fn process_gate_owner_matches(
    mode: Option<ProcessGateOwnerMatch>,
    requested: Option<&UserId>,
    snapshot: &crate::TurnPersistenceSnapshot,
    run: &crate::TurnRunRecord,
) -> bool {
    let Some(requested) = requested else {
        return true;
    };
    match mode.unwrap_or(ProcessGateOwnerMatch::Explicit) {
        ProcessGateOwnerMatch::Explicit => run.scope.explicit_owner_user_id() == Some(requested),
        ProcessGateOwnerMatch::ExplicitOrActor => {
            process_gate_owner_user_id(snapshot, run) == Some(requested)
        }
    }
}

fn process_gate_owner_user_id<'a>(
    snapshot: &'a crate::TurnPersistenceSnapshot,
    run: &'a crate::TurnRunRecord,
) -> Option<&'a UserId> {
    run.scope.explicit_owner_user_id().or_else(|| {
        snapshot
            .turns
            .iter()
            .find(|turn| turn.turn_id == run.turn_id && turn.scope.same_thread(&run.scope))
            .map(|turn| &turn.actor.user_id)
    })
}
