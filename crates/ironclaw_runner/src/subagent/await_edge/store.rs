//! Runner projection over process-journal dependencies.

use std::sync::Arc;

use chrono::Utc;
use ironclaw_host_api::ProcessId;
use ironclaw_processes::{
    CloseProcessDependencyRequest, ProcessDependencyPort, ProcessDependencyQuery,
    ProcessDependencyRecord, ProcessDependencyState, ProcessJournalStoreError,
    ProcessLifecycleStatus, ProcessTerminalEvidence, SettleProcessDependencyRequest,
};
use ironclaw_turns::{TurnRunId, TurnScope};

use super::{
    AwaitEdge, AwaitEdgeState, AwaitEdgeStoreError, EdgeTerminalKind, ReservationReleaseState,
};

pub struct AwaitEdgeStore {
    dependencies: Arc<dyn ProcessDependencyPort<Error = ProcessJournalStoreError>>,
}

impl AwaitEdgeStore {
    pub fn new(
        dependencies: Arc<dyn ProcessDependencyPort<Error = ProcessJournalStoreError>>,
    ) -> Self {
        Self { dependencies }
    }

    fn process_id(run_id: TurnRunId) -> ProcessId {
        ProcessId::from_uuid(run_id.as_uuid())
    }

    fn run_id(process_id: ProcessId) -> TurnRunId {
        TurnRunId::from_uuid(process_id.as_uuid())
    }

    fn query(
        scope: &TurnScope,
        parent_run_id: Option<TurnRunId>,
        group_ref: Option<String>,
        include_closed: bool,
    ) -> ProcessDependencyQuery {
        ProcessDependencyQuery {
            scope: scope.to_resource_scope(),
            dependent_process_id: parent_run_id.map(Self::process_id),
            group_ref,
            include_closed,
        }
    }

    fn edge_from_record(record: ProcessDependencyRecord) -> Result<AwaitEdge, AwaitEdgeStoreError> {
        let mut edge = match serde_json::from_value::<AwaitEdge>(record.metadata.clone()) {
            Ok(edge) => edge,
            Err(_) => {
                let submitted: ironclaw_loop_host::AwaitedChildSetRecord =
                    serde_json::from_value(record.metadata).map_err(|error| {
                        AwaitEdgeStoreError::Backend {
                            reason: format!(
                                "process dependency metadata deserialize failed: {error}"
                            ),
                        }
                    })?;
                AwaitEdge {
                    child_scope: submitted.child_scope,
                    child_thread_id: submitted.child_thread_id,
                    parent_thread_id: submitted.parent_run_context.thread_id.clone(),
                    parent_run_context: submitted.parent_run_context,
                    tree_root_run_id: submitted.tree_root_run_id,
                    gate_ref: submitted.gate_ref,
                    source_binding_ref: submitted.source_binding_ref,
                    reply_target_binding_ref: submitted.reply_target_binding_ref,
                    subagent_kind: submitted.subagent_kind,
                    spawn_capability_id: submitted.spawn_capability_id,
                    result_ref: submitted.result_ref,
                    mode: submitted.mode,
                    state: AwaitEdgeState::Open,
                    terminal_kind: None,
                    terminal_byte_len: None,
                    terminal_reason: None,
                    reservation_release: ReservationReleaseState::Unclaimed,
                    created_at: record.created_at,
                    settled_at: None,
                }
            }
        };
        edge.state = match record.state {
            ProcessDependencyState::Open => AwaitEdgeState::Open,
            ProcessDependencyState::Settled => AwaitEdgeState::Settled,
            ProcessDependencyState::Consumed => AwaitEdgeState::Drained,
            ProcessDependencyState::Abandoned => AwaitEdgeState::Abandoned,
        };
        edge.reservation_release = if matches!(
            record.state,
            ProcessDependencyState::Consumed | ProcessDependencyState::Abandoned
        ) {
            ReservationReleaseState::Released
        } else {
            ReservationReleaseState::Unclaimed
        };
        edge.settled_at = record.settled_at;
        if let Some(terminal) = record.terminal {
            edge.terminal_kind = EdgeTerminalKind::from_process_status(terminal.status);
            edge.terminal_byte_len = terminal.output_bytes;
            edge.terminal_reason = terminal.sanitized_reason;
        }
        Ok(edge)
    }

    pub async fn abandon(
        &self,
        scope: &TurnScope,
        parent_run_id: TurnRunId,
        child_run_id: TurnRunId,
    ) -> Result<(), AwaitEdgeStoreError> {
        self.dependencies
            .abandon_process_dependency(CloseProcessDependencyRequest {
                dependent_process_id: Self::process_id(parent_run_id),
                dependency_process_id: Self::process_id(child_run_id),
                scope: scope.to_resource_scope(),
                closed_at: Utc::now(),
            })
            .await
            .map(|_| ())
            .map_err(map_process_error)
    }

    pub async fn settle(
        &self,
        scope: &TurnScope,
        parent_run_id: TurnRunId,
        child_run_id: TurnRunId,
        terminal_kind: EdgeTerminalKind,
        terminal_byte_len: Option<u64>,
        terminal_reason: Option<String>,
    ) -> Result<Option<AwaitEdge>, AwaitEdgeStoreError> {
        self.dependencies
            .settle_process_dependency(SettleProcessDependencyRequest {
                dependent_process_id: Self::process_id(parent_run_id),
                dependency_process_id: Self::process_id(child_run_id),
                scope: scope.to_resource_scope(),
                terminal: ProcessTerminalEvidence {
                    status: terminal_kind.to_process_status(),
                    output_bytes: terminal_byte_len,
                    sanitized_reason: terminal_reason,
                },
                settled_at: Utc::now(),
            })
            .await
            .map_err(map_process_error)?
            .map(Self::edge_from_record)
            .transpose()
    }

    pub async fn list_group(
        &self,
        scope: &TurnScope,
        parent_run_id: TurnRunId,
        gate_ref: &ironclaw_turns::GateRef,
    ) -> Result<Vec<(TurnRunId, AwaitEdge)>, AwaitEdgeStoreError> {
        self.dependencies
            .query_process_dependencies(Self::query(
                scope,
                Some(parent_run_id),
                Some(gate_ref.as_str().to_string()),
                false,
            ))
            .await
            .map_err(map_process_error)?
            .into_iter()
            .map(|record| {
                let child_run_id = Self::run_id(record.dependency_process_id);
                Self::edge_from_record(record).map(|edge| (child_run_id, edge))
            })
            .collect()
    }

    pub async fn peek(
        &self,
        scope: &TurnScope,
        parent_run_id: TurnRunId,
        child_run_id: TurnRunId,
    ) -> Result<Option<AwaitEdge>, AwaitEdgeStoreError> {
        let child_process_id = Self::process_id(child_run_id);
        self.dependencies
            .query_process_dependencies(Self::query(scope, Some(parent_run_id), None, false))
            .await
            .map_err(map_process_error)?
            .into_iter()
            .find(|record| record.dependency_process_id == child_process_id)
            .map(Self::edge_from_record)
            .transpose()
    }

    pub async fn list_unclosed_for_scope(
        &self,
        scope: &TurnScope,
    ) -> Result<Vec<(TurnRunId, TurnRunId, AwaitEdge)>, AwaitEdgeStoreError> {
        self.dependencies
            .query_process_dependencies(Self::query(scope, None, None, false))
            .await
            .map_err(map_process_error)?
            .into_iter()
            .map(|record| {
                let parent = Self::run_id(record.dependent_process_id);
                let child = Self::run_id(record.dependency_process_id);
                Self::edge_from_record(record).map(|edge| (parent, child, edge))
            })
            .collect()
    }

    pub async fn consume(
        &self,
        scope: &TurnScope,
        parent_run_id: TurnRunId,
        child_run_id: TurnRunId,
    ) -> Result<(), AwaitEdgeStoreError> {
        self.dependencies
            .consume_process_dependency(CloseProcessDependencyRequest {
                dependent_process_id: Self::process_id(parent_run_id),
                dependency_process_id: Self::process_id(child_run_id),
                scope: scope.to_resource_scope(),
                closed_at: Utc::now(),
            })
            .await
            .map(|_| ())
            .map_err(map_process_error)
    }

    pub async fn close(
        &self,
        scope: &TurnScope,
        parent_run_id: TurnRunId,
        child_run_id: TurnRunId,
    ) -> Result<(), AwaitEdgeStoreError> {
        let Some(edge) = self.peek(scope, parent_run_id, child_run_id).await? else {
            return Ok(());
        };
        match edge.state {
            AwaitEdgeState::Settled => self.consume(scope, parent_run_id, child_run_id).await,
            AwaitEdgeState::Open | AwaitEdgeState::Drained | AwaitEdgeState::Abandoned => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl ironclaw_loop_host::AwaitEdgeWriter for AwaitEdgeStore {
    async fn abandon_awaited_child(
        &self,
        child_scope: &TurnScope,
        parent_run_id: TurnRunId,
        child_run_id: TurnRunId,
    ) -> Result<(), ironclaw_turns::run_profile::AgentLoopHostError> {
        self.abandon(child_scope, parent_run_id, child_run_id)
            .await
            .map_err(super::map_await_edge_error)
    }
}

#[async_trait::async_trait]
impl crate::loop_exit_applier::AwaitDependentRunEvidenceStore for AwaitEdgeStore {
    async fn has_awaited_child_gate(
        &self,
        scope: &TurnScope,
        run_id: TurnRunId,
        gate_ref: &ironclaw_turns::LoopGateRef,
    ) -> Result<bool, ironclaw_turns::TurnError> {
        let gate_ref = ironclaw_turns::GateRef::new(gate_ref.as_str()).map_err(|reason| {
            ironclaw_turns::TurnError::InvalidRequest {
                reason: format!("awaited child gate evidence has invalid gate ref: {reason}"),
            }
        })?;
        let group = self
            .list_group(scope, run_id, &gate_ref)
            .await
            .map_err(|error| ironclaw_turns::TurnError::Unavailable {
                reason: error.to_string(),
            })?;
        Ok(group
            .iter()
            .any(|(_, edge)| edge.mode == ironclaw_loop_host::SpawnSubagentMode::Blocking))
    }
}

impl EdgeTerminalKind {
    fn to_process_status(self) -> ProcessLifecycleStatus {
        match self {
            Self::Completed => ProcessLifecycleStatus::Completed,
            Self::Failed => ProcessLifecycleStatus::Failed,
            Self::Cancelled => ProcessLifecycleStatus::Cancelled,
            Self::RecoveryRequired => ProcessLifecycleStatus::RecoveryRequired,
        }
    }

    fn from_process_status(status: ProcessLifecycleStatus) -> Option<Self> {
        match status {
            ProcessLifecycleStatus::Completed => Some(Self::Completed),
            ProcessLifecycleStatus::Failed => Some(Self::Failed),
            ProcessLifecycleStatus::Cancelled => Some(Self::Cancelled),
            ProcessLifecycleStatus::RecoveryRequired => Some(Self::RecoveryRequired),
            _ => None,
        }
    }
}

fn map_process_error(error: ironclaw_processes::ProcessJournalStoreError) -> AwaitEdgeStoreError {
    AwaitEdgeStoreError::Backend {
        reason: error.to_string(),
    }
}
