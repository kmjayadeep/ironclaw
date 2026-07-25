use std::sync::Arc;

use async_trait::async_trait;
use ironclaw_auth::{
    AuthFlowOwnerScope, AuthFlowRecord, AuthFlowRecordSource, AuthGateRef, TurnGateAuthFlowQuery,
    TurnRunRef, flow_matches_turn_gate_query,
};
use ironclaw_processes::{
    ProcessGateOwnerMatch, ProcessGateQuery, ProcessGateQuerySource, ProcessSuspensionKind,
};
use ironclaw_product::{
    AuthGateRecord, AuthInteractionReadModel, AuthInteractionRejectionKind, AuthInteractionScope,
    AuthInteractionService, ListPendingAuthInteractionsRequest,
    ListPendingAuthInteractionsResponse, ProductSurfaceFailure, ResolveAuthInteractionRequest,
    ResolveAuthInteractionResponse,
};
use ironclaw_turns::{GateRef, TurnRunId, TurnScope};

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockedAuthRun {
    run_id: TurnRunId,
    gate_ref: GateRef,
}

pub(super) struct ProcessGateAuthInteractionReadModel {
    gates: Arc<dyn ProcessGateQuerySource<Error = ironclaw_turns::TurnError>>,
    flow_records: Arc<dyn AuthFlowRecordSource>,
}

pub(super) struct UnavailableAuthInteractionService;

#[async_trait]
impl AuthInteractionService for UnavailableAuthInteractionService {
    async fn list_pending(
        &self,
        _request: ListPendingAuthInteractionsRequest,
    ) -> Result<ListPendingAuthInteractionsResponse, ProductSurfaceFailure> {
        Err(auth_read_model_unavailable())
    }

    async fn resolve(
        &self,
        _request: ResolveAuthInteractionRequest,
    ) -> Result<ResolveAuthInteractionResponse, ProductSurfaceFailure> {
        Err(auth_read_model_unavailable())
    }
}

impl ProcessGateAuthInteractionReadModel {
    pub(super) fn new(
        gates: Arc<dyn ProcessGateQuerySource<Error = ironclaw_turns::TurnError>>,
        flow_records: Arc<dyn AuthFlowRecordSource>,
    ) -> Self {
        Self {
            gates,
            flow_records,
        }
    }

    async fn query(
        &self,
        scope: &AuthInteractionScope,
        gate_ref: Option<GateRef>,
        include_historical: bool,
    ) -> Result<Vec<ironclaw_processes::ProcessGateRecord>, ProductSurfaceFailure> {
        self.gates
            .query_process_gates(ProcessGateQuery {
                scope: turn_scope_for_interaction(scope).to_resource_scope(),
                gate_kind: ProcessSuspensionKind::Authorization,
                owner_user_id: Some(scope.user_id.clone()),
                gate_ref,
                owner_match: Some(ProcessGateOwnerMatch::Explicit),
                include_historical,
            })
            .await
            .map_err(|error| {
                tracing::debug!(
                    %error,
                    "auth interaction read model could not read process gate projection"
                );
                auth_read_model_unavailable()
            })
    }

    async fn blocked_auth_runs(
        &self,
        scope: &AuthInteractionScope,
    ) -> Result<Vec<BlockedAuthRun>, ProductSurfaceFailure> {
        let mut runs = self
            .query(scope, None, false)
            .await?
            .into_iter()
            .filter_map(|record| {
                record.suspension.gate_ref.map(|gate_ref| BlockedAuthRun {
                    run_id: turn_run_id_from_process_id(record.process_id),
                    gate_ref,
                })
            })
            .collect::<Vec<_>>();
        runs.sort_by_key(|run| run.run_id.as_uuid());
        Ok(runs)
    }

    async fn auth_run_for_gate(
        &self,
        scope: &AuthInteractionScope,
        gate_ref: &GateRef,
    ) -> Result<Option<TurnRunId>, ProductSurfaceFailure> {
        let mut runs = self
            .query(scope, Some(gate_ref.clone()), true)
            .await?
            .into_iter()
            .map(|record| {
                (
                    record.historical,
                    turn_run_id_from_process_id(record.process_id),
                )
            })
            .collect::<Vec<_>>();
        runs.sort_by_key(|(historical, run_id)| (*historical, run_id.as_uuid()));
        runs.dedup_by_key(|(_, run_id)| *run_id);
        Ok(runs.into_iter().map(|(_, run_id)| run_id).next())
    }

    async fn flow_for_gate(
        &self,
        scope: &AuthInteractionScope,
        run_id: TurnRunId,
        gate_ref: &GateRef,
    ) -> Result<Option<AuthFlowRecord>, ProductSurfaceFailure> {
        self.flow_records
            .flow_for_turn_gate(turn_gate_query(scope, run_id, gate_ref)?)
            .await
            .map_err(|error| {
                tracing::warn!(
                    %error,
                    %run_id,
                    gate_ref = %gate_ref.as_str(),
                    "local-dev auth read model failed to query flow for turn gate"
                );
                auth_read_model_unavailable()
            })
    }
}

fn owner_scope_for_interaction(scope: &AuthInteractionScope) -> AuthFlowOwnerScope {
    AuthFlowOwnerScope {
        tenant_id: scope.tenant_id.clone(),
        user_id: scope.user_id.clone(),
        agent_id: scope.agent_id.clone(),
        project_id: scope.project_id.clone(),
        thread_id: scope.thread_id.clone(),
    }
}

fn turn_gate_query(
    scope: &AuthInteractionScope,
    run_id: TurnRunId,
    gate_ref: &GateRef,
) -> Result<TurnGateAuthFlowQuery, ProductSurfaceFailure> {
    Ok(TurnGateAuthFlowQuery {
        owner: owner_scope_for_interaction(scope),
        turn_run_ref: TurnRunRef::new(run_id.to_string())
            .map_err(|_| auth_read_model_unavailable())?,
        gate_ref: AuthGateRef::new(gate_ref.as_str()).map_err(|_| auth_read_model_unavailable())?,
        include_terminal: false,
    })
}

async fn flows_for_owner(
    source: &Arc<dyn AuthFlowRecordSource>,
    scope: &AuthInteractionScope,
) -> Result<Vec<AuthFlowRecord>, ProductSurfaceFailure> {
    source
        .flows_for_owner(owner_scope_for_interaction(scope))
        .await
        .map_err(|error| {
            tracing::warn!(
                %error,
                tenant_id = %scope.tenant_id.as_str(),
                user_id = %scope.user_id.as_str(),
                thread_id = %scope.thread_id.as_str(),
                "local-dev auth read model failed to query flows for owner"
            );
            auth_read_model_unavailable()
        })
}

fn matching_flow_for_run(
    flows: &[AuthFlowRecord],
    scope: &AuthInteractionScope,
    run_id: TurnRunId,
    gate_ref: &GateRef,
) -> Result<Option<AuthFlowRecord>, ProductSurfaceFailure> {
    let query = turn_gate_query(scope, run_id, gate_ref)?;
    Ok(flows
        .iter()
        .find(|flow| flow_matches_turn_gate_query(flow, &query))
        .cloned())
}

fn turn_run_id_from_process_id(process_id: ironclaw_host_api::ProcessId) -> TurnRunId {
    TurnRunId::from_uuid(process_id.as_uuid())
}

impl ProcessGateAuthInteractionReadModel {
    async fn owner_flows(
        &self,
        scope: &AuthInteractionScope,
    ) -> Result<Vec<AuthFlowRecord>, ProductSurfaceFailure> {
        flows_for_owner(&self.flow_records, scope).await
    }
}

#[async_trait]
impl AuthInteractionReadModel for ProcessGateAuthInteractionReadModel {
    async fn auth_gates(
        &self,
        scope: &AuthInteractionScope,
    ) -> Result<Vec<AuthGateRecord>, ProductSurfaceFailure> {
        let mut gates = Vec::new();
        let flows = self.owner_flows(scope).await?;
        for run in self.blocked_auth_runs(scope).await? {
            if let Some(flow) = matching_flow_for_run(&flows, scope, run.run_id, &run.gate_ref)? {
                gates.push(AuthGateRecord::new(run.run_id, run.gate_ref, flow)?);
            }
        }
        Ok(gates)
    }

    async fn auth_gate(
        &self,
        scope: &AuthInteractionScope,
        run_id_hint: Option<TurnRunId>,
        gate_ref: &GateRef,
    ) -> Result<Option<AuthGateRecord>, ProductSurfaceFailure> {
        let run_id = match run_id_hint {
            Some(run_id) => run_id,
            None => {
                let Some(run_id) = self.auth_run_for_gate(scope, gate_ref).await? else {
                    return Ok(None);
                };
                run_id
            }
        };
        let Some(flow) = self.flow_for_gate(scope, run_id, gate_ref).await? else {
            return Ok(None);
        };
        Ok(Some(AuthGateRecord::new(run_id, gate_ref.clone(), flow)?))
    }
}

fn turn_scope_for_interaction(scope: &AuthInteractionScope) -> TurnScope {
    TurnScope::new_with_owner(
        scope.tenant_id.clone(),
        scope.agent_id.clone(),
        scope.project_id.clone(),
        scope.thread_id.clone(),
        Some(scope.user_id.clone()),
    )
}

fn auth_read_model_unavailable() -> ProductSurfaceFailure {
    ProductSurfaceFailure::AuthInteractionRejected {
        kind: AuthInteractionRejectionKind::FlowUnavailable,
    }
}
