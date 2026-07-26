//! Extension-host assembly kept behind one composition builder.
//!
//! The generic extension host is created while the production substrate is
//! built. Once the run-world services exist, this builder completes the
//! channel-host half and registers its extension-specific runtime projections.

use std::sync::Arc;

use ironclaw_filesystem::RootFilesystem;
use ironclaw_host_api::UserId;
use ironclaw_product::{
    ApprovalInteractionService, ApprovalPromptContextSource, AuthChallengeProvider,
    AuthInteractionService, BlockedAuthFlowCanceller, BlockedAuthPromptSource, RunDeliverySettings,
};
use ironclaw_threads::{SessionThreadService, ThreadScope};
use ironclaw_turns::TurnCoordinator;

use crate::factory::RebornRuntimeStores;
use crate::outbound::MutableOutboundDeliveryTargetRegistry;

/// Run-world services and identity bound into the per-extension channel
/// workflows.
pub(crate) struct ChannelHostAssemblyWiring {
    pub(crate) thread_service: Arc<dyn SessionThreadService>,
    pub(crate) turn_coordinator: Arc<dyn TurnCoordinator>,
    pub(crate) approval_interaction: Option<Arc<dyn ApprovalInteractionService>>,
    pub(crate) auth_interaction: Option<Arc<dyn AuthInteractionService>>,
    pub(crate) identity: ironclaw_extension_host::channel_host::ChannelHostIdentity,
    pub(crate) approval_context: Option<Arc<dyn ApprovalPromptContextSource>>,
    pub(crate) blocked_auth_prompts: Option<Arc<dyn BlockedAuthPromptSource>>,
    pub(crate) auth_flow_cancel: Option<Arc<dyn BlockedAuthFlowCanceller>>,
    pub(crate) run_delivery_settings: RunDeliverySettings,
}

/// Remaining runtime inputs needed after the turn and projection services have
/// been composed.
pub(crate) struct RuntimeExtensionHostAssemblyWiring<'a> {
    pub(crate) thread_service: Arc<dyn SessionThreadService>,
    pub(crate) turn_coordinator: Arc<dyn TurnCoordinator>,
    pub(crate) approval_interaction: Arc<dyn ApprovalInteractionService>,
    pub(crate) auth_interaction: Arc<dyn AuthInteractionService>,
    pub(crate) thread_scope: &'a ThreadScope,
    pub(crate) actor_user_id: UserId,
    pub(crate) auth_challenges: Option<Arc<dyn AuthChallengeProvider>>,
    pub(crate) outbound_delivery_targets: Option<&'a Arc<MutableOutboundDeliveryTargetRegistry>>,
    pub(crate) local_runtime: Option<&'a RebornRuntimeStores>,
}

/// Concrete composition builder for the extension host's run-world half.
///
/// This is deliberately not a trait: composition has one production assembly
/// and the builder exists to keep its dependency graph explicit and localized.
pub(crate) struct ExtensionHostAssemblyBuilder<'a> {
    source: ChannelHostAssemblySource,
    services: Option<&'a RebornRuntimeStores>,
}

pub(crate) struct ChannelHostAssemblySource {
    pub(crate) generic_host: Arc<ironclaw_extension_host::ExtensionHost>,
    pub(crate) ingress_registry:
        Arc<ironclaw_extension_host::extension_ingress::ExtensionIngressRegistry>,
    pub(crate) workflow_filesystem: Arc<dyn RootFilesystem>,
    pub(crate) delivery_coordinator: Option<Arc<ironclaw_product::DeliveryCoordinator>>,
    pub(crate) outbound_state: Arc<dyn ironclaw_outbound::OutboundStateStorePort>,
    pub(crate) delivered_gate_routes: Arc<dyn ironclaw_outbound::DeliveredGateRouteStore>,
    pub(crate) outbound_preferences: Arc<dyn ironclaw_outbound::CommunicationPreferenceRepository>,
    pub(crate) identity_lookup: Arc<dyn ironclaw_host_api::RebornUserIdentityLookup>,
    pub(crate) deployment_channels: Arc<ironclaw_extension_host::DeploymentChannelRegistry>,
    pub(crate) channel_config: Arc<ironclaw_extension_host::ChannelConfigService>,
    pub(crate) channel_pairing:
        Option<Arc<ironclaw_extension_host::channel_pairing::ChannelPairingRegistry>>,
}

impl<'a> ExtensionHostAssemblyBuilder<'a> {
    pub(crate) fn new(services: &'a RebornRuntimeStores) -> Option<Self> {
        let source = ChannelHostAssemblySource {
            generic_host: services.extension_management.generic_host()?,
            ingress_registry: Arc::clone(&services.extension_ingress.as_ref()?.registry),
            workflow_filesystem: services.extension_filesystem.clone(),
            delivery_coordinator: services.delivery_coordinator.clone(),
            outbound_state: Arc::clone(&services.outbound_state),
            delivered_gate_routes: Arc::clone(&services.delivered_gate_routes),
            outbound_preferences: Arc::clone(&services.outbound_preferences),
            identity_lookup: Arc::clone(&services.channel_identity_store)
                as Arc<dyn ironclaw_host_api::RebornUserIdentityLookup>,
            deployment_channels: Arc::clone(&services.deployment_channels),
            channel_config: Arc::clone(&services.channel_config_service),
            channel_pairing: services.channel_pairing.clone(),
        };
        Some(Self {
            source,
            services: Some(services),
        })
    }

    pub(crate) fn from_source(source: ChannelHostAssemblySource) -> Self {
        Self {
            source,
            services: None,
        }
    }

    /// Start the generic channel host reconcile loop. `None` means this
    /// composition has no generic host or ingress registry.
    pub(crate) fn start_channel_host(
        &self,
        wiring: ChannelHostAssemblyWiring,
    ) -> Option<Arc<ironclaw_extension_host::channel_host::GenericChannelHostAssembly>> {
        use ironclaw_extension_host::channel_host::{
            ChannelHostDeliveryDeps, FilesystemChannelWorkflowStateFactory,
            GenericChannelHostAssembly, GenericChannelHostDeps,
        };

        let ChannelHostAssemblyWiring {
            thread_service,
            turn_coordinator,
            approval_interaction,
            auth_interaction,
            identity,
            approval_context,
            blocked_auth_prompts,
            auth_flow_cancel,
            run_delivery_settings,
        } = wiring;
        let ChannelHostAssemblySource {
            generic_host,
            ingress_registry: registry,
            workflow_filesystem,
            delivery_coordinator,
            outbound_state,
            delivered_gate_routes,
            outbound_preferences,
            identity_lookup,
            deployment_channels,
            channel_config,
            channel_pairing,
        } = &self.source;
        let workflow_state = Arc::new(FilesystemChannelWorkflowStateFactory::new(Arc::clone(
            workflow_filesystem,
        )));
        let delivery = delivery_coordinator
            .clone()
            .map(|coordinator| ChannelHostDeliveryDeps {
                coordinator,
                outbound_store: Arc::clone(outbound_state),
                route_store: Arc::clone(delivered_gate_routes),
                communication_preferences: Arc::clone(outbound_preferences),
                approval_context,
                blocked_auth_prompts,
                auth_flow_cancel,
                settings: run_delivery_settings,
            });
        let identity_lookup = Some(Arc::clone(identity_lookup));

        Some(GenericChannelHostAssembly::start(GenericChannelHostDeps {
            watch: generic_host.snapshot_watch(),
            deployment_channels: Arc::clone(deployment_channels),
            registry: Arc::clone(registry),
            channel_config: Arc::clone(channel_config),
            workflow_state,
            thread_service,
            turn_coordinator,
            approval_interaction,
            auth_interaction,
            identity,
            identity_lookup,
            delivery,
            channel_pairing: channel_pairing.clone(),
        }))
    }

    /// Complete extension-host assembly after the run-world services exist:
    /// start reconciliation, attach binding extras, and publish generic
    /// outbound-delivery targets.
    pub(crate) async fn build_runtime(
        &self,
        wiring: RuntimeExtensionHostAssemblyWiring<'_>,
    ) -> Option<Arc<ironclaw_extension_host::channel_host::GenericChannelHostAssembly>> {
        let RuntimeExtensionHostAssemblyWiring {
            thread_service,
            turn_coordinator,
            approval_interaction,
            auth_interaction,
            thread_scope,
            actor_user_id,
            auth_challenges,
            outbound_delivery_targets,
            local_runtime,
        } = wiring;
        let services = self.services?;
        let approval_context = Some(Arc::new(
            ironclaw_extension_host::run_delivery_ports::ProjectionApprovalPromptContextSource::new(
                Arc::clone(&services.approval_requests)
                    as Arc<dyn ironclaw_run_state::ApprovalRequestStorePort>,
            ),
        ) as Arc<dyn ApprovalPromptContextSource>);
        let blocked_auth_prompts = Some(Arc::new(
            ironclaw_extension_host::run_delivery_ports::ProductAuthBlockedAuthPromptSource::new(
                auth_challenges.clone(),
            ),
        ) as Arc<dyn BlockedAuthPromptSource>);
        let auth_flow_cancel = crate::runtime::blocked_auth_flow_canceller(&services.product_auth);
        let assembly = self.start_channel_host(ChannelHostAssemblyWiring {
            thread_service,
            turn_coordinator,
            approval_interaction: Some(approval_interaction),
            auth_interaction: Some(auth_interaction),
            identity: ironclaw_extension_host::channel_host::ChannelHostIdentity {
                tenant_id: thread_scope.tenant_id.clone(),
                agent_id: thread_scope.agent_id.clone(),
                project_id: thread_scope.project_id.clone(),
                operator_user_id: actor_user_id,
            },
            approval_context,
            blocked_auth_prompts,
            auth_flow_cancel,
            run_delivery_settings: ironclaw_product::triggered_run_delivery_settings(),
        });

        if let Some(assembly) = assembly.as_ref() {
            for binding in &services.channel_extension_bindings {
                assembly
                    .register_extras(
                        &binding.extension_id,
                        ironclaw_extension_host::channel_host::ChannelExtras {
                            classifier: None,
                            preference_target_codec: binding.preference_target_codec.clone(),
                            subject_route_resolver: None,
                            storage_roots: None,
                        },
                    )
                    .await;
            }
        }

        if let (Some(registry), Some(assembly), Some(local_runtime)) =
            (outbound_delivery_targets, assembly.as_ref(), local_runtime)
        {
            ironclaw_extension_host::channel_outbound_targets::register_generic_channel_outbound_targets(
                registry,
                ironclaw_extension_host::channel_outbound_targets::GenericChannelOutboundTargetDeps {
                    watch: assembly.snapshot_watch(),
                    assembly: Arc::clone(assembly),
                    channel_config: Arc::clone(&local_runtime.channel_config_service),
                    dm_targets: local_runtime.channel_dm_target_store.clone(),
                    identity: ironclaw_extension_host::channel_outbound_targets::ChannelOutboundTargetIdentity {
                        tenant_id: thread_scope.tenant_id.clone(),
                        agent_id: thread_scope.agent_id.clone(),
                        project_id: thread_scope.project_id.clone(),
                    },
                },
            );
        }

        assembly
    }
}
