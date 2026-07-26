use super::*;

/// delivery target registry: the id must resolve for the trigger creator (the
/// same ownership check the delivery layer applies at fire time). Fails
/// closed when no provider is registered or the id is unknown/foreign.
pub(super) async fn validate_trigger_delivery_target_against_registry(
    registry: &crate::outbound::MutableOutboundDeliveryTargetRegistry,
    scope: &ironclaw_host_api::ResourceScope,
    target: &ironclaw_triggers::TriggerDeliveryTargetId,
) -> Result<(), TriggerError> {
    let invalid = |reason: String| TriggerError::InvalidRecord {
        kind: ironclaw_triggers::TriggerRecordValidationKind::DeliveryTargetInvalid,
        reason,
    };
    let target_id =
        crate::outbound::OutboundDeliveryTargetId::new(target.as_str()).map_err(|error| {
            tracing::debug!(
                target = "ironclaw::reborn::trigger_create",
                %error,
                "per-trigger delivery target id failed outbound target id validation"
            );
            invalid("delivery target id is not a valid outbound target id".to_string())
        })?;
    let caller = crate::outbound::OutboundDeliveryTargetScope::new(
        scope.tenant_id.clone(),
        scope.user_id.clone(),
    );
    use crate::outbound::OutboundDeliveryTargetProvider as _;
    match registry
        .resolve_outbound_delivery_target(&caller, &target_id)
        .await
    {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(invalid(
            "delivery target is not available to this caller".to_string(),
        )),
        Err(error) => {
            tracing::warn!(
                target = "ironclaw::reborn::trigger_create",
                %error,
                "outbound delivery target lookup failed during trigger create validation"
            );
            Err(TriggerError::Backend {
                reason: "outbound delivery target lookup unavailable".to_string(),
            })
        }
    }
}

/// Late-rebindable [`TurnStateStore`] the trigger delivery-target service
/// reads. Production installs the runtime's own turn-state store and never
/// repoints it; a `test-support` harness repoints it (alongside the sibling
/// snapshot slot) so trigger creation can see runs recorded in the harness's
/// own store (#6520 delivery-target inheritance).
#[cfg(any(test, feature = "test-support"))]
#[allow(
    dead_code,
    reason = "constructed only by downstream test-support harnesses that rebind trigger stores"
)]
pub(super) struct LateBoundTriggerSourceTurnStateStore {
    pub(super) source_turn_state: Arc<std::sync::RwLock<Arc<dyn ironclaw_turns::TurnStateStore>>>,
}

#[cfg(any(test, feature = "test-support"))]
#[allow(
    dead_code,
    reason = "methods are used when the late-bound test-support store is installed"
)]
impl LateBoundTriggerSourceTurnStateStore {
    fn current(
        &self,
    ) -> Result<Arc<dyn ironclaw_turns::TurnStateStore>, ironclaw_turns::TurnError> {
        self.source_turn_state
            .read()
            .map(|source| Arc::clone(&*source))
            .map_err(|error| {
                tracing::warn!(
                    target = "ironclaw::reborn::trigger_create",
                    error = ?error,
                    "source turn-state resolver lock is unavailable"
                );
                ironclaw_turns::TurnError::Unavailable {
                    reason: "source turn-state resolver unavailable".to_string(),
                }
            })
    }
}

#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
impl ironclaw_turns::TurnStateStore for LateBoundTriggerSourceTurnStateStore {
    async fn submit_turn(
        &self,
        request: ironclaw_turns::SubmitTurnRequest,
        admission_policy: &dyn ironclaw_turns::TurnAdmissionPolicy,
        run_profile_resolver: &dyn ironclaw_turns::RunProfileResolver,
    ) -> Result<ironclaw_turns::SubmitTurnResponse, ironclaw_turns::TurnError> {
        self.current()?
            .submit_turn(request, admission_policy, run_profile_resolver)
            .await
    }

    async fn resume_turn(
        &self,
        request: ironclaw_turns::ResumeTurnRequest,
    ) -> Result<ironclaw_turns::ResumeTurnResponse, ironclaw_turns::TurnError> {
        self.current()?.resume_turn(request).await
    }

    async fn retry_turn(
        &self,
        request: ironclaw_turns::RetryTurnRequest,
    ) -> Result<ironclaw_turns::RetryTurnResponse, ironclaw_turns::TurnError> {
        self.current()?.retry_turn(request).await
    }

    async fn request_cancel(
        &self,
        request: ironclaw_turns::CancelRunRequest,
    ) -> Result<ironclaw_turns::CancelRunResponse, ironclaw_turns::TurnError> {
        self.current()?.request_cancel(request).await
    }

    async fn get_run_state(
        &self,
        request: ironclaw_turns::GetRunStateRequest,
    ) -> Result<ironclaw_turns::TurnRunState, ironclaw_turns::TurnError> {
        self.current()?.get_run_state(request).await
    }
}

pub(super) struct TriggerCreatorPairingHook {
    pub(super) outbound_delivery_targets:
        Arc<crate::outbound::MutableOutboundDeliveryTargetRegistry>,
    pub(super) source_turn_state: Arc<dyn TurnStateStore>,
    pub(super) scoped_filesystem: Arc<ScopedFilesystem<CompositeRootFilesystem>>,
    pub(super) conversations: tokio::sync::OnceCell<RebornFilesystemConversationServices>,
}

#[async_trait::async_trait]
impl TriggerCreateHook for TriggerCreatorPairingHook {
    async fn resolve_implicit_delivery_target(
        &self,
        scope: &ironclaw_host_api::ResourceScope,
        run_id: Option<RunId>,
    ) -> Result<Option<ironclaw_triggers::TriggerDeliveryTargetId>, TriggerError> {
        resolve_current_run_delivery_target(
            self.source_turn_state.as_ref(),
            &self.outbound_delivery_targets,
            scope,
            run_id,
        )
        .await
    }

    async fn validate_delivery_target(
        &self,
        scope: &ironclaw_host_api::ResourceScope,
        target: &ironclaw_triggers::TriggerDeliveryTargetId,
    ) -> Result<(), TriggerError> {
        validate_trigger_delivery_target_against_registry(
            &self.outbound_delivery_targets,
            scope,
            target,
        )
        .await
    }

    async fn after_trigger_persisted(&self, record: &TriggerRecord) -> Result<(), TriggerError> {
        let filesystem = Arc::clone(&self.scoped_filesystem);
        let conversations = self
            .conversations
            .get_or_try_init(|| async move {
                RebornFilesystemConversationServices::new(filesystem).await
            })
            .await
            .map_err(|error| {
                trigger_pairing_error(TriggerPairingFailureSource::ConversationInit, error)
            })?;
        pair_trigger_creator(conversations, record).await
    }
}

async fn resolve_current_run_delivery_target(
    turn_state: &dyn TurnStateStore,
    registry: &crate::outbound::MutableOutboundDeliveryTargetRegistry,
    scope: &ResourceScope,
    run_id: Option<RunId>,
) -> Result<Option<ironclaw_triggers::TriggerDeliveryTargetId>, TriggerError> {
    let Some(run_id) = run_id else {
        return Ok(None);
    };
    let Some(thread_id) = scope.thread_id.clone() else {
        return Ok(None);
    };
    let turn_scope = TurnScope::new_with_owner(
        scope.tenant_id.clone(),
        scope.agent_id.clone(),
        scope.project_id.clone(),
        thread_id,
        Some(scope.user_id.clone()),
    );
    let run_state = turn_state
        .get_run_state(GetRunStateRequest {
            scope: turn_scope,
            run_id: ironclaw_turns::TurnRunId::from_uuid(run_id.as_uuid()),
        })
        .await
        .map_err(|error| {
            tracing::warn!(
                target = "ironclaw::reborn::trigger_create",
                %error,
                %run_id,
                "source run lookup failed during implicit trigger delivery-target resolution"
            );
            TriggerError::Backend {
                reason: "source run lookup unavailable".to_string(),
            }
        })?;
    let caller = crate::outbound::OutboundDeliveryTargetScope::new(
        scope.tenant_id.clone(),
        scope.user_id.clone(),
    );
    use crate::outbound::OutboundDeliveryTargetProvider as _;
    let entry = registry
        .resolve_reply_target_binding(&caller, &run_state.reply_target_binding_ref)
        .await
        .map_err(|error| {
            tracing::warn!(
                target = "ironclaw::reborn::trigger_create",
                %error,
                %run_id,
                "outbound target lookup failed during implicit trigger delivery-target resolution"
            );
            TriggerError::Backend {
                reason: "outbound delivery target lookup unavailable".to_string(),
            }
        })?;
    entry
        .map(|entry| {
            ironclaw_triggers::TriggerDeliveryTargetId::new(
                entry.summary.target_id.as_str().to_string(),
            )
            .map_err(|reason| TriggerError::InvalidRecord {
                kind: ironclaw_triggers::TriggerRecordValidationKind::DeliveryTargetInvalid,
                reason,
            })
        })
        .transpose()
}

pub(super) async fn pair_trigger_creator(
    pairing: &dyn ConversationActorPairingService,
    record: &TriggerRecord,
) -> Result<(), TriggerError> {
    let adapter_kind = AdapterKind::new(TRIGGER_TRUSTED_ADAPTER_KIND).map_err(|error| {
        trigger_pairing_error(TriggerPairingFailureSource::TypedIdentity, error)
    })?;
    let adapter_installation_id =
        AdapterInstallationId::new(TRIGGER_TRUSTED_ADAPTER_INSTALLATION_ID).map_err(|error| {
            trigger_pairing_error(TriggerPairingFailureSource::TypedIdentity, error)
        })?;
    let external_actor_ref = ExternalActorRef::new(
        TRIGGER_TRUSTED_EXTERNAL_ACTOR_NAMESPACE,
        record.creator_user_id.as_str(),
    )
    .map_err(|error| trigger_pairing_error(TriggerPairingFailureSource::TypedIdentity, error))?;
    pairing
        .pair_external_actor(
            record.tenant_id.clone(),
            adapter_kind,
            adapter_installation_id,
            external_actor_ref,
            record.creator_user_id.clone(),
        )
        .await
        .map_err(|error| trigger_pairing_error(TriggerPairingFailureSource::ActorPairing, error))
}

enum TriggerPairingFailureSource {
    TypedIdentity,
    ConversationInit,
    ActorPairing,
}

impl TriggerPairingFailureSource {
    fn as_str(&self) -> &'static str {
        match self {
            Self::TypedIdentity => "typed_identity",
            Self::ConversationInit => "conversation_init",
            Self::ActorPairing => "actor_pairing",
        }
    }
}

fn trigger_pairing_error(
    source: TriggerPairingFailureSource,
    _error: impl std::fmt::Display,
) -> TriggerError {
    tracing::debug!(
        error_kind = "pairing_failure",
        error_source = source.as_str(),
        "trigger creator actor pairing failed"
    );
    TriggerError::Backend {
        reason: "trigger creator actor pairing failed".to_string(),
    }
}
