// arch-exempt: large_file, needs Reborn composition helper extraction, plan #4469
use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::AtomicBool,
};

use crate::backend_store_assembly::{
    ProductionStoreBundle, SecretCredentialStores, build_filesystem_secret_credential_stores,
    filesystem_resource_governor, owner_turn_state_filesystem, production_turn_state_store,
    resolve_explicit_or_keychain_master_key, trigger_repository_for_durable_backend,
};
#[cfg(any(test, feature = "test-support"))]
use crate::builtin_capability_policy::BuiltinCapabilityPolicy;
use crate::builtin_capability_policy::builtin_capability_policy;
use crate::capability_authorization::{StoreApprovalSettingsProvider, capability_authorizer};
use crate::deployment::TrafficPolicy;
use crate::extension_host_assembly::{
    BackendChannelPairingAssemblyBuilder, BackendChannelPairingAssemblyInput,
    BackendExtensionHostAssemblyBuilder, BackendExtensionHostAssemblyInput,
};
#[cfg(any(test, feature = "test-support"))]
use crate::filesystem_assembly::build_default_database_roots;
#[cfg(test)]
use crate::filesystem_assembly::mount_descriptor;
use crate::filesystem_assembly::{
    DurableBackend, DurableStorageInput, FilesystemAssemblyBuilder,
    open_standalone_libsql_database, production_database_root_filesystem, standalone_db_path,
};
#[cfg(test)]
use crate::host_access_assembly::validate_workspace_skill_isolation;
use crate::host_access_assembly::{HostAccessAssemblyBuilder, WorkspaceFilesystems};
use crate::input::{
    LibsqlConnectionConfig, OAuthDcrCallbackConfig, OAuthProviderBackendConfig, PostgresPoolSource,
    RebornLocalRuntimeIdentity, RebornRuntimeProcessBinding, RebornStorageInput,
};
use crate::operator_tool_catalog::ActiveRegistryOperatorToolCatalog;
use crate::outbound::outbound_preferences_capability::{
    extend_builtin_first_party_package as extend_builtin_outbound_preferences_package,
    insert_handler as insert_outbound_preferences_handler,
};
use crate::outbound::{
    outbound_delivery_synthetic_provider, outbound_delivery_target_set_operator_tool_info,
};
use crate::outbound_store_assembly::OutboundStoreAssemblyBuilder;
use crate::runtime_input::RebornRuntimeIdentity;
use crate::runtime_mounts::{
    ambient_workspace_mount_view, memory_mount_view, scoped_skill_context_mount_view,
    skill_management_mount_view, workspace_mount_view,
};
use crate::standalone_bootstrap_assembly::HostBootstrapAssemblyBuilder;
#[cfg(test)]
use crate::standalone_bootstrap_assembly::{
    LEGACY_SKILLS_BACKFILL_MARKER, backfill_legacy_user_skills,
};
use crate::support::fs::RebornProjectService;
use crate::{
    RebornBuildError, RebornCompositionProfile, RebornHostBindings, RebornReadiness,
    RebornServiceReadiness, RebornWorkerReadiness,
};
use ironclaw_approvals::{
    AutoApproveSettingStore, PersistentApprovalPolicyStore, ToolPermissionOverrideStore,
};
use ironclaw_auth::RebornProductAuthServicePorts;
use ironclaw_auth::product_auth::durable::{
    FilesystemAuthProductServices, UnavailableAuthProviderClient,
};
use ironclaw_auth::product_auth::oauth::oauth_gate::OAuthGateFlowDriver;
use ironclaw_auth::{
    AuthEngine, AuthEngineDeps, AuthProductError, AuthProductScope, AuthProviderClient,
    AuthRecipeResolver, AuthSurface, CredentialAccountStatus, EngineCallbackBase,
    EngineClientCredentialsSource, EngineOAuthClientMaterial, OAuthClientId,
    RebornAuthContinuationDispatcher, RebornProductAuthServices,
    RuntimeCredentialAccountRefreshService, RuntimeCredentialAccountSelectionService,
    StaticAuthRecipeResolver, map_account_error, runtime_credential_account_selection_request,
};
use ironclaw_authorization::CapabilityLeaseStore;
use ironclaw_authorization::GrantAuthorizer;
use ironclaw_capabilities::{
    CapabilityObligationAbortRequest, CapabilityObligationHandler, CapabilityObligationOutcome,
    CapabilityObligationPhase, CapabilityObligationRequest,
};
use ironclaw_conversations::RebornFilesystemConversationServices;
use ironclaw_conversations::{
    AdapterInstallationId, AdapterKind, ConversationActorPairingService, ExternalActorRef,
};
use ironclaw_events::{DurableAuditLog, DurableEventLog};
use ironclaw_extension_host::channel_pairing::ChannelPairingRegistry;
use ironclaw_extension_host::{
    ActiveExtensionPublisher, AdminConfigurationCatalogUse, AdminConfigurationService,
    AvailableExtensionCatalog, ChannelConfigService, ExtensionRemovalCleanupAdapter,
    ExtensionRemovalCleanupRegistry, FilesystemAdminConfigurationStore, FirstPartyRegistrarContext,
    ProviderInstanceReadinessInput, first_party_reserved_extension_ids, hosted_http_mcp_runtime,
    product_extension_host_api_contract_registry, provider_instance_readiness_map,
    restore_extension_lifecycle_state,
};
use ironclaw_extension_host::{
    admin_configuration::{
        ComposedAdminConfigurationService, ComposedExtensionAdminConfigurationResolver,
    },
    admin_configuration_capability::{
        extend_builtin_first_party_package as extend_builtin_admin_configuration_package,
        insert_handler as insert_admin_configuration_handler,
    },
    extension_lifecycle::{
        ExtensionCredentialCleanup, RebornLocalExtensionManagementPort,
        RebornProductAuthCredentialCleanup,
    },
    extension_lifecycle_capabilities::{
        extend_builtin_first_party_package, insert_handlers as insert_extension_lifecycle_handlers,
    },
    operator_config_capability::{
        extend_builtin_first_party_package as extend_builtin_operator_config_package,
        insert_handler as insert_operator_config_handler,
    },
    skill_auto_activate_capability::{
        extend_builtin_first_party_package as extend_builtin_skill_auto_activate_package,
        insert_handler as insert_skill_auto_activate_handler,
    },
};
use ironclaw_extensions::{
    ExtensionInstallationStore, ExtensionInstallationStorePort, ExtensionLifecycleService,
    ExtensionRegistry, SharedExtensionRegistry,
};
use ironclaw_filesystem::ScopedFilesystem;
#[cfg(test)]
use ironclaw_filesystem::{
    BackendCapabilities, BackendKind, ContentKind, DiskFilesystem, IndexPolicy, StorageClass,
};
use ironclaw_filesystem::{CompositeRootFilesystem, LibSqlRootFilesystem, RootFilesystem};
use ironclaw_host_api::runtime_policy::{
    DeploymentMode, EffectiveRuntimePolicy, FilesystemBackendKind, NetworkMode, ProcessBackendKind,
    SecretMode,
};
use ironclaw_host_api::{
    CapabilitySet, CorrelationId, CredentialStageError, ExtensionId, HostApiError, InvocationId,
    MountAlias, MountGrant, MountPermissions, MountView, NetworkPolicy, Obligation, PackageId,
    RecipeClientCredentials, ResourceEstimate, ResourceScope, RunId, RuntimeHttpEgress,
    RuntimeHttpEgressError, RuntimeHttpEgressRequest, RuntimeHttpEgressResponse, RuntimeKind,
    TrustClass, UserId, VendorId, VirtualPath, sha256_digest_token,
};
use ironclaw_host_runtime::memory_provider::MemoryServiceResolver;
use ironclaw_host_runtime::{
    CapabilitySurfaceVersion, FirstPartyCapabilityRegistry, HostProcessPort, HostRuntimeServices,
    NATIVE_MEMORY_FIRST_PARTY_PROVIDER, PostEditCheckConfig, ProductAuthProviderRuntimePorts,
    RuntimeCredentialAccessSecret, RuntimeCredentialAccountRequest,
    RuntimeCredentialAccountResolver, TriggerCreateHook, builtin_first_party_package,
    native_memory_first_party_package,
};
use ironclaw_host_runtime::{
    builtin_first_party_handlers_with_trigger_create_hook_for_process_backend_and_memory_resolver,
    builtin_first_party_package_for_process_backend,
};
use ironclaw_loop_host::CheckpointStateStore;
use ironclaw_outbound::CommunicationPreferenceRepository;
use ironclaw_outbound::{
    DeliveredGateRouteStore, OutboundStateStorePort, TriggeredRunDeliveryStore,
};
use ironclaw_processes::ProcessServices;
use ironclaw_product::{
    ChannelConnectionNoticePolicy, ChannelConnectionRequirement, ExtensionAccountSetupDescriptor,
    ExtensionAccountSetupRegistry, LifecycleProductSurfaceContext,
    OutboundPreferencesProductService, ProductAuthTurnGateResumeDispatcher, ProjectService,
};
use ironclaw_projects::ProjectRepository;
use ironclaw_resources::InMemoryResourceGovernor;
use ironclaw_resources::{
    BroadcastBudgetEventSink, BudgetGateStore, BudgetGateStorePort, FilesystemResourceGovernor,
    ResourceGovernor,
};
use ironclaw_run_state::ApprovalRequestStore;
use ironclaw_secrets::{SecretStore, SecretStorePort};
use ironclaw_skills::ScopedSkillManagementPort;
use ironclaw_threads::FilesystemSessionThreadService;
use ironclaw_threads::SessionThreadService;
use ironclaw_triggers::{
    TRIGGER_TRUSTED_ADAPTER_INSTALLATION_ID, TRIGGER_TRUSTED_ADAPTER_KIND,
    TRIGGER_TRUSTED_EXTERNAL_ACTOR_NAMESPACE, TriggerActiveRunLookup, TriggerError, TriggerRecord,
    TriggerRepository,
};
use ironclaw_trust::{AdminConfig, AdminEntry, HostTrustAssignment, HostTrustPolicy};
use ironclaw_turns::TurnStateRowStore;
use ironclaw_turns::{
    CheckpointStateStorePort, ExternalToolCatalog, InMemoryExternalToolCatalog, LoopCheckpointStore,
};
use ironclaw_turns::{GetRunStateRequest, InMemoryRunProfileResolver, TurnScope, TurnStateStore};
use secrecy::SecretString;

mod auth_engine_assembly;
pub(crate) use auth_engine_assembly::auth_continuation_dispatcher;
use auth_engine_assembly::{
    AdminConfigurationCredentialSlot, AuthContinuationFromProduct,
    ProductAuthRuntimeCredentialResolver, ProductAuthServicesCompositionInput,
    compose_product_auth_services, compose_provider_client, product_auth_continuation_dispatcher,
};
mod trigger_creation_assembly;
#[cfg(any(test, feature = "test-support"))]
use trigger_creation_assembly::LateBoundTriggerSourceTurnStateStore;
use trigger_creation_assembly::LocalRuntimeTriggerCreatorPairingHook;
#[cfg(test)]
use trigger_creation_assembly::{
    pair_trigger_creator, validate_trigger_delivery_target_against_registry,
};

/// Filename of the cached standalone secrets master-key dotfile under a
/// Reborn home / standalone root directory. `pub` (re-exported from `lib.rs`)
/// so onboarding (`ironclaw_reborn_cli::commands::onboard`) can check for its
/// presence without duplicating the literal.
pub const STANDALONE_SECRETS_MASTER_KEY_PATH: &str = ".reborn-local-dev-secrets-master-key";

/// The ONE construction seam for host HTTP egress: policy enforcement over
/// the reqwest transport, honoring the env-gated test-only host rewrite map
/// ([`ironclaw_network::TEST_HTTP_REWRITE_MAP_ENV`]). Every composition path
/// builds its vendor egress here so test runs redirect ALL vendor calls
/// identically. Fail-closed: a set-but-invalid map refuses composition.
fn default_host_http_egress() -> Result<
    ironclaw_network::PolicyNetworkHttpEgress<
        ironclaw_network::RewriteNetworkTransport<ironclaw_network::ReqwestNetworkTransport>,
    >,
    RebornBuildError,
> {
    ironclaw_network::default_policy_http_egress().map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: error.to_string(),
        }
    })
}

/// Test-support pass-through so a `#[cfg]`-gated injected
/// `Arc<dyn NetworkHttpEgress>` (there is no blanket `NetworkHttpEgress` impl on
/// `Arc<dyn …>`) satisfies the generic `try_with_host_http_egress_with_body_store`
/// bound. Consumes `RebornHostBindings::network_http_egress_for_test`, letting a
/// unit/integration test drive hosted-MCP discovery and any host HTTP egress
/// over a fake transport instead of the real network. Restores the consumer
/// dropped in commit 975bcd2ce ("Unify reborn runtime assembly"), which
/// collapsed the two build paths and left the injected egress unread.
#[cfg(any(test, feature = "test-support"))]
struct TestNetworkHttpEgress(Arc<dyn ironclaw_network::NetworkHttpEgress>);

#[cfg(any(test, feature = "test-support"))]
#[async_trait::async_trait]
impl ironclaw_network::NetworkHttpEgress for TestNetworkHttpEgress {
    async fn execute(
        &self,
        request: ironclaw_network::NetworkHttpRequest,
    ) -> Result<ironclaw_network::NetworkHttpResponse, ironclaw_network::NetworkHttpError> {
        self.0.execute(request).await
    }
}

pub(crate) type ComposedResourceGovernor = FilesystemResourceGovernor<CompositeRootFilesystem>;

pub(crate) type ComposedApprovalRequestStore = ApprovalRequestStore<CompositeRootFilesystem>;

pub(crate) type ComposedCapabilityLeaseStore = CapabilityLeaseStore<CompositeRootFilesystem>;

pub(crate) type ComposedPersistentApprovalPolicyStore =
    PersistentApprovalPolicyStore<CompositeRootFilesystem>;

pub(crate) type ComposedToolPermissionOverrideStore =
    ToolPermissionOverrideStore<CompositeRootFilesystem>;

pub(crate) type ComposedAutoApproveSettingStore = AutoApproveSettingStore<CompositeRootFilesystem>;

fn apply_post_edit_check_from_env<F, G, S, R>(
    services: HostRuntimeServices<F, G, S, R>,
) -> Result<HostRuntimeServices<F, G, S, R>, RebornBuildError>
where
    F: ironclaw_filesystem::RootFilesystem + 'static,
    G: ironclaw_resources::ResourceGovernor + 'static,
    S: ironclaw_processes::ProcessStorePort + 'static,
    R: ironclaw_processes::ProcessResultStorePort + 'static,
{
    match PostEditCheckConfig::from_env() {
        Ok(Some(post_edit_check)) => Ok(services.with_post_edit_check(post_edit_check)),
        Ok(None) => Ok(services),
        Err(error) => Err(RebornBuildError::InvalidConfig {
            reason: error.to_string(),
        }),
    }
}

fn require_product_auth_runtime_ports<F, G, S, R>(
    services: &HostRuntimeServices<F, G, S, R>,
) -> Result<ProductAuthProviderRuntimePorts, RebornBuildError>
where
    F: ironclaw_filesystem::RootFilesystem + 'static,
    G: ironclaw_resources::ResourceGovernor + 'static,
    S: ironclaw_processes::ProcessStorePort + 'static,
    R: ironclaw_processes::ProcessResultStorePort + 'static,
{
    services
        .product_auth_provider_runtime_ports()
        .ok_or_else(|| RebornBuildError::InvalidConfig {
            reason: "product auth runtime ports unavailable; host runtime must be configured with HTTP egress and a secret store".to_string(),
        })
}

fn attach_hosted_mcp_runtime<F, G, S, R>(
    services: HostRuntimeServices<F, G, S, R>,
) -> Result<HostRuntimeServices<F, G, S, R>, RebornBuildError>
where
    F: ironclaw_filesystem::RootFilesystem + 'static,
    G: ironclaw_resources::ResourceGovernor + 'static,
    S: ironclaw_processes::ProcessStorePort + 'static,
    R: ironclaw_processes::ProcessResultStorePort + 'static,
{
    // Soft-disable when host runtime HTTP egress is absent. Builds without
    // egress — in-memory test services, minimal compositions — must still
    // succeed; only hosted MCP capabilities go dark.
    let Some(runtime_ports) = services.product_auth_provider_runtime_ports() else {
        tracing::debug!(
            "skipping hosted MCP runtime: host runtime HTTP egress absent \
             (only affects hosted MCP extensions, e.g. Notion, NEAR AI)"
        );
        return Ok(services);
    };
    let runtime_http_egress = runtime_ports.runtime_http_egress();
    let registry = services.shared_extension_registry();

    Ok(services.with_mcp_runtime(Arc::new(hosted_http_mcp_runtime(
        registry,
        runtime_http_egress,
    ))))
}

fn attach_wasm_runtime<F, G, S, R>(
    services: HostRuntimeServices<F, G, S, R>,
) -> Result<HostRuntimeServices<F, G, S, R>, RebornBuildError>
where
    F: ironclaw_filesystem::RootFilesystem + 'static,
    G: ironclaw_resources::ResourceGovernor + 'static,
    S: ironclaw_processes::ProcessStorePort + 'static,
    R: ironclaw_processes::ProcessResultStorePort + 'static,
{
    services
        .try_with_default_wasm_runtime()
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("WASM runtime could not be initialized: {error}"),
        })
}

pub(crate) fn apply_production_runtime_process_binding<F, G, S, R>(
    services: HostRuntimeServices<F, G, S, R>,
    binding: RebornRuntimeProcessBinding,
) -> HostRuntimeServices<F, G, S, R>
where
    F: ironclaw_filesystem::RootFilesystem + 'static,
    G: ironclaw_resources::ResourceGovernor + 'static,
    S: ironclaw_processes::ProcessStorePort + 'static,
    R: ironclaw_processes::ProcessResultStorePort + 'static,
{
    match binding {
        RebornRuntimeProcessBinding::None => services,
        RebornRuntimeProcessBinding::TenantSandbox { process_port } => {
            services.with_production_tenant_sandbox_process_port(process_port)
        }
    }
}

pub(crate) struct RebornRuntimeStores {
    pub(crate) host_runtime: Arc<dyn ironclaw_host_runtime::HostRuntime>,
    #[cfg(test)]
    pub(crate) turn_coordinator: Arc<dyn ironclaw_turns::TurnCoordinator>,
    pub(crate) product_auth: Arc<RebornProductAuthServices>,
    pub(crate) readiness: RebornReadiness,
    pub(crate) skill_management: Arc<ScopedSkillManagementPort>,
    pub(crate) extension_lifecycle_surface_context: LifecycleProductSurfaceContext,
    pub(crate) owner_user_id: UserId,
    pub(crate) approval_requests: Arc<ComposedApprovalRequestStore>,
    pub(crate) capability_leases: Arc<ComposedCapabilityLeaseStore>,
    pub(crate) external_tool_catalog: Arc<dyn ExternalToolCatalog>,
    pub(crate) runtime_policy: Option<EffectiveRuntimePolicy>,
    pub(crate) persistent_approval_policies: Arc<ComposedPersistentApprovalPolicyStore>,
    pub(crate) tool_permission_overrides: Arc<ComposedToolPermissionOverrideStore>,
    pub(crate) auto_approve_settings: Arc<ComposedAutoApproveSettingStore>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) capability_policy: Arc<BuiltinCapabilityPolicy>,
    pub(crate) outbound_preferences: Arc<dyn CommunicationPreferenceRepository>,
    pub(crate) outbound_delivery_targets:
        Arc<crate::outbound::MutableOutboundDeliveryTargetRegistry>,
    pub(crate) skill_auto_activate_learned: Arc<AtomicBool>,
    pub(crate) outbound_state: Arc<dyn OutboundStateStorePort>,
    pub(crate) delivered_gate_routes: Arc<dyn DeliveredGateRouteStore>,
    pub(crate) triggered_run_delivery: Arc<dyn TriggeredRunDeliveryStore>,
    /// Late-rebindable turn-run source the trigger active-run lookup reads
    /// (`crate::turn_run_snapshot`). Production points it at this runtime's own
    /// turn-state store; a `test-support` harness can repoint it at its own
    /// store so its runs are visible to the trigger subsystem.
    #[cfg(any(test, feature = "test-support"))]
    #[allow(
        dead_code,
        reason = "held for test-support rebinding after runtime construction"
    )]
    pub(crate) trigger_source_turn_state:
        Arc<std::sync::RwLock<Arc<dyn crate::turn_run_snapshot::TurnRunSnapshotSource>>>,
    /// Sibling rebindable slot, `TurnStateStore`-typed, read by the trigger
    /// delivery-target service; repointed together with the snapshot slot.
    #[cfg(any(test, feature = "test-support"))]
    #[allow(
        dead_code,
        reason = "held for test-support rebinding after runtime construction"
    )]
    pub(crate) trigger_source_turn_state_store:
        Arc<std::sync::RwLock<Arc<dyn ironclaw_turns::TurnStateStore>>>,
    pub(crate) extension_management: Arc<RebornLocalExtensionManagementPort>,
    pub(crate) admin_configuration: Arc<ComposedAdminConfigurationService>,
    pub(crate) admin_configuration_uses: Arc<Vec<AdminConfigurationCatalogUse>>,
    /// Deployment-first current delivery-target resolver (extension-runtime
    /// §5.4): the run-delivery observer half reads it to route a run's final
    /// reply to the caller's active channel target.
    pub(crate) channel_config_service: Arc<ChannelConfigService>,
    pub(crate) channel_identity_store: Arc<ironclaw_extension_host::FilesystemChannelIdentityStore>,
    pub(crate) channel_dm_target_store:
        Arc<ironclaw_extension_host::FilesystemChannelDmTargetStore>,
    pub(crate) channel_disconnect_slot:
        Arc<std::sync::OnceLock<Arc<dyn ironclaw_product::ChannelConnectionService>>>,
    pub(crate) runtime_http_egress: Option<Arc<dyn RuntimeHttpEgress>>,
    pub(crate) skill_mounts: MountView,
    pub(crate) memory_mounts: MountView,
    pub(crate) system_extensions_lifecycle_mounts: MountView,
    pub(crate) skill_filesystem: Arc<ScopedFilesystem<CompositeRootFilesystem>>,
    pub(crate) workspace_filesystem: Arc<ScopedFilesystem<CompositeRootFilesystem>>,
    pub(crate) extension_filesystem: Arc<CompositeRootFilesystem>,
    /// Single memory provider resolver (issue #3537). Both the memory tools and
    /// the standalone profile source build their `MemoryService` through this, so
    /// profile reads and tools agree on the bound provider (native, or
    /// degrade-to-empty for disabled/third-party).
    pub(crate) memory_service_resolver: MemoryServiceResolver,
    pub(crate) workspace_mounts: MountView,
    pub(crate) standalone_storage_root: Option<PathBuf>,
    pub(crate) default_system_prompt_path: Option<PathBuf>,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) in_memory_budget_event_sink: Arc<ironclaw_resources::InMemoryBudgetEventSink>,
    pub(crate) extension_registry: Arc<ExtensionRegistry>,
    pub(crate) shared_extension_registry: Arc<SharedExtensionRegistry>,
    pub(crate) scoped_filesystem: Arc<ScopedFilesystem<CompositeRootFilesystem>>,
    pub(crate) turn_state: Arc<TurnStateRowStore<CompositeRootFilesystem>>,
    pub(crate) checkpoint_state_store: Arc<dyn CheckpointStateStorePort>,
    pub(crate) loop_checkpoint_store: Arc<dyn LoopCheckpointStore>,
    pub(crate) thread_service: Arc<dyn SessionThreadService>,
    pub(crate) trigger_repository: Arc<dyn TriggerRepository>,
    pub(crate) resource_governor: Arc<dyn ResourceGovernor>,
    pub(crate) budget_gate_store: Arc<dyn BudgetGateStorePort>,
    pub(crate) broadcast_budget_event_sink: Arc<BroadcastBudgetEventSink>,
    pub(crate) event_log: Arc<dyn DurableEventLog>,
    pub(crate) audit_log: Arc<dyn DurableAuditLog>,
    pub(crate) admin_secret_provisioner: Arc<dyn crate::admin_secrets::AdminSecretProvisioner>,
    pub(crate) project_service: Arc<dyn ProjectService>,
    pub(crate) trigger_conversation_services: RebornFilesystemConversationServices,
    /// Pre-minted scheduler wake wiring for the production composition path.
    /// Minted in `build_production_shaped` so the notifier can satisfy
    /// `HostRuntimeServices.with_turn_run_wake_notifier_dyn` before
    /// `build_default_planned_runtime` runs; consumed by `build_reborn_runtime`
    /// via `DefaultPlannedRuntimeParts.scheduler_wake_wiring` so the scheduler
    /// loop driven by that function shares the exact same channel.
    pub(crate) production_scheduler_wake: Option<ironclaw_runner::runtime::SchedulerWakeWiring>,
    /// Shared scoped secret store. Exposed so runtime-level features (e.g.
    /// operator LLM-key storage) can reuse the same instance product-auth uses
    /// rather than standing up a second authority.
    pub(crate) secret_store: Arc<dyn SecretStorePort>,
    #[cfg(test)]
    pub(crate) standalone_wasm_runtime_credential_provider_captured: bool,
    /// Readiness of the background credential keepalive worker (B1). Carries the
    /// worker's dependencies together so "both deps present or neither" is a type
    /// invariant rather than a runtime check. MUST stay private — the worker is
    /// the only consumer; this field must never leak through any public facade.
    pub(crate) credential_refresh_worker: CredentialRefreshWorkerReady,
    /// The binary-assembled channel-extension bindings (extension-runtime
    /// DEL-7): adapters were handed to the generic host at build; the extras
    /// are consumed by `build_reborn_runtime` when the channel host assembly
    /// starts.
    pub(crate) channel_extension_bindings: Vec<crate::input::ChannelExtensionBinding>,
    /// Manifest-declared deployment channel surfaces, independent of user
    /// installation/activation state.
    pub(crate) deployment_channels: Arc<ironclaw_extension_host::DeploymentChannelRegistry>,
    /// The composed generic channel ingress (extension-runtime P4): the
    /// deployment-first router plus its active-snapshot compatibility lane and
    /// per-extension registration surface. `None` on composition paths that do
    /// not build the generic extension host.
    pub(crate) extension_ingress:
        Option<ironclaw_extension_host::extension_ingress::ExtensionIngressParts>,
    /// Pairing services for `WebGeneratedCode` channel extensions, built
    /// from the binary-assembled account-setup descriptors; the channel host
    /// assembly consumes it for sink gates and actor resolution.
    pub(crate) channel_pairing: Option<Arc<ChannelPairingRegistry>>,
    /// The generic delivery coordinator (extension-runtime §5.4): the sole
    /// writer of outbound delivery state, resolving channel adapters +
    /// policy egress from deployment bindings or the active compatibility
    /// snapshot. `None` when the composition path builds no channel egress
    /// transport.
    pub(crate) delivery_coordinator: Option<Arc<ironclaw_product::DeliveryCoordinator>>,
    /// The deployment-first channel delivery resolver behind the coordinator,
    /// exposed separately for host flows (e.g. DM target provisioning) that
    /// need one stable adapter + egress read outside a delivery.
    pub(crate) channel_delivery_resolver:
        Option<Arc<dyn ironclaw_product::ChannelDeliveryResolver>>,
    /// Registry of beta-era channel credential bridges (§11 compatibility):
    /// channel hosts whose secrets predate the extension-config store
    /// register resolution ports here.
    #[cfg(feature = "test-support")]
    pub(crate) channel_egress_credential_bridges:
        Option<Arc<ironclaw_extension_host::channel_egress::BridgedChannelEgressCredentials>>,
}

struct ChannelHostWiring {
    extension_ingress: Option<ironclaw_extension_host::extension_ingress::ExtensionIngressParts>,
    delivery_coordinator: Option<Arc<ironclaw_product::DeliveryCoordinator>>,
    channel_delivery_resolver: Option<Arc<dyn ironclaw_product::ChannelDeliveryResolver>>,
    #[cfg(feature = "test-support")]
    channel_egress_credential_bridges:
        Option<Arc<ironclaw_extension_host::channel_egress::BridgedChannelEgressCredentials>>,
}

/// Whether the engine-owned credential keepalive sweep
/// (`ironclaw_auth::keepalive`) can be started, with its dependencies bundled
/// so they cannot be partially wired.
///
/// The dependencies (cross-owner candidate enumeration + recipe data +
/// deployment-wide leader lock + refresh port) are only ever produced together
/// on the durable production path. Bundling them into one `Ready` variant
/// makes the half-configured state — which would silently disable proactive
/// refresh — unrepresentable, so the runtime spawn site is a clean two-arm
/// match with no "enabled but deps missing" branch to forget about.
pub(crate) enum CredentialRefreshWorkerReady {
    /// Deps fully wired (durable production path). The only state that can start
    /// the sweep; the `enabled` policy flag still gates the actual spawn.
    Ready {
        candidate_source: Arc<dyn ironclaw_auth::KeepaliveCandidateSource>,
        /// Active recipe data — declares which vendors carry an idle lifetime
        /// (`refresh.keepalive_idle_seconds`).
        recipes: Arc<dyn ironclaw_auth::AuthRecipeResolver>,
        leader_lock: ironclaw_auth::CredentialRefreshLeaderLock,
        refresh_port: Arc<RebornProductAuthServices>,
    },
    /// Deps intentionally absent: standalone (single-user, no cross-owner
    /// enumeration), or a caller-supplied `product_auth_ports` override/test
    /// path. The sweep never starts.
    Absent,
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support;

#[cfg(feature = "test-support")]
pub use test_support::RebornApprovalTestParts;
#[cfg(feature = "test-support")]
pub(crate) use test_support::{
    ActiveExtensionAuthorityForTest, active_extension_authority_for_test,
};
#[cfg(any(test, feature = "test-support"))]
pub use test_support::{AttachmentTestSupport, ChannelHostAssemblyTestWiring};

#[cfg(feature = "test-support")]
pub(crate) use test_support::{
    mount_default_database_roots, open_standalone_approval_request_store_for_test,
    open_standalone_approval_settings_stores_for_test,
    open_standalone_extension_installation_store_for_test,
    open_standalone_outbound_preferences_store_for_test, open_standalone_root_filesystem_for_test,
    open_standalone_trigger_repository_for_test,
};

impl std::fmt::Debug for RebornRuntimeStores {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = formatter.debug_struct("RebornRuntimeStores");
        debug
            .field("host_runtime", &"Arc<dyn HostRuntime>")
            .field("turn_coordinator", &cfg!(test))
            .field("product_auth", &"Arc<RebornProductAuthServices>")
            .field("readiness", &self.readiness)
            .field("extension_management", &true)
            .field("scoped_filesystem", &"Arc<ScopedFilesystem>")
            .field("turn_state", &"Arc<TurnStateRowStore>");
        debug.finish()
    }
}

pub(crate) fn filesystem_reborn_identity_store<F>(
    scoped_filesystem: Arc<ScopedFilesystem<F>>,
    tenant_id: ironclaw_host_api::TenantId,
    actor_user_id: UserId,
    agent_id: ironclaw_host_api::AgentId,
    project_id: Option<ironclaw_host_api::ProjectId>,
) -> Arc<ironclaw_reborn_identity::RebornIdentityStore<F>>
where
    F: RootFilesystem + 'static,
{
    Arc::new(ironclaw_reborn_identity::RebornIdentityStore::new(
        scoped_filesystem,
        tenant_id,
        actor_user_id,
        agent_id,
        project_id,
    ))
}

pub(crate) async fn build_runtime_substrate(
    input: RebornHostBindings,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    tracing::debug!(
        profile = %input.profile(),
        owner_id = %input.owner_id(),
        "building Reborn composition facades"
    );
    // Substrate selection is deployment *data* (§4.4/§5.6), not a profile
    // match: the config says which substrate to assemble and this dispatches
    // on that value.
    let substrate = input.deployment().substrate();
    match substrate {
        crate::deployment::RuntimeSubstrate::None => Err(RebornBuildError::InvalidConfig {
            reason: format!(
                "profile={} does not configure a Reborn runtime substrate",
                input.profile()
            ),
        }),
        crate::deployment::RuntimeSubstrate::ProductionShaped => {
            build_production_shaped(input).await
        }
    }
}

/// Whether a Google OAuth backend is configured, from the composition-side
/// signal `GsuiteFirstPartyHandler` uses to short-circuit dispatch with a
/// "not configured" tool result instead of reaching credential resolution.
/// Shared by `build_local_runtime` and its production-build-context
/// counterpart so the check doesn't drift between the two call sites.
fn google_oauth_configured(
    oauth_provider_configs: &[crate::input::OAuthProviderBackendConfig],
) -> bool {
    oauth_provider_configs
        .iter()
        .any(|config| config.vendor == ironclaw_auth::GOOGLE_PROVIDER_ID)
}

fn production_config(
    required_runtime_backends: Vec<ironclaw_host_api::RuntimeKind>,
    require_runtime_http_egress: bool,
    require_wasm_credentials: bool,
) -> ironclaw_host_runtime::ProductionWiringConfig {
    let mut config = ironclaw_host_runtime::ProductionWiringConfig::new(required_runtime_backends);
    if require_runtime_http_egress {
        config = config.require_runtime_http_egress();
    }
    if require_wasm_credentials {
        config = config.require_wasm_credentials();
    }
    config.require_credential_broker()
}

/// Build the safe single-tenant runtime surface used by standalone and
/// hosted-single-tenant. Hosted single-tenant supplies a durable Postgres
/// backend through `RebornStorageInput::HostedSingleTenantPostgres`; standalone
/// keeps its historical local filesystem/libSQL default.
fn extension_lifecycle_surface_context(
    owner_user_id: UserId,
    local_runtime_identity: Option<&RebornLocalRuntimeIdentity>,
) -> Result<LifecycleProductSurfaceContext, RebornBuildError> {
    let default_identity = RebornRuntimeIdentity::reborn_cli();
    let default_tenant_id =
        ironclaw_host_api::TenantId::new(default_identity.tenant_id).map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: error.to_string(),
            }
        })?;
    let default_agent_id =
        ironclaw_host_api::AgentId::new(default_identity.agent_id).map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: error.to_string(),
            }
        })?;
    let tenant_id = local_runtime_identity
        .map(|identity| identity.tenant_id.clone())
        .unwrap_or(default_tenant_id);
    let agent_id = local_runtime_identity
        .map(|identity| identity.agent_id.clone())
        .unwrap_or(default_agent_id);
    Ok(LifecycleProductSurfaceContext {
        tenant_id,
        user_id: owner_user_id,
        agent_id: Some(agent_id),
        project_id: None,
    })
}

fn owner_scope_from_runtime_identity(
    owner_user_id: UserId,
    tenant_id: ironclaw_host_api::TenantId,
    agent_id: ironclaw_host_api::AgentId,
) -> ResourceScope {
    ResourceScope {
        tenant_id,
        user_id: owner_user_id,
        agent_id: Some(agent_id),
        project_id: None,
        mission_id: None,
        thread_id: None,
        invocation_id: InvocationId::new(),
    }
}

fn default_runtime_owner_scope(
    owner_user_id: UserId,
) -> Result<ResourceScope, ironclaw_host_api::HostApiError> {
    let identity = RebornRuntimeIdentity::reborn_cli();
    let tenant_id = ironclaw_host_api::TenantId::new(identity.tenant_id)?;
    let agent_id = ironclaw_host_api::AgentId::new(identity.agent_id)?;
    Ok(owner_scope_from_runtime_identity(
        owner_user_id,
        tenant_id,
        agent_id,
    ))
}

fn configured_runtime_owner_scope(
    owner_user_id: UserId,
    local_runtime_identity: &RebornLocalRuntimeIdentity,
) -> ResourceScope {
    owner_scope_from_runtime_identity(
        owner_user_id,
        local_runtime_identity.tenant_id.clone(),
        local_runtime_identity.agent_id.clone(),
    )
}

/// Validate a per-trigger delivery target against the runtime's outbound
/// delivery target registry: the id must resolve for the trigger creator (the
/// same ownership check the delivery layer applies at fire time). Fails
/// closed when no provider is registered or the id is unknown/foreign.

struct BudgetSinks {
    budget_event_sink: Arc<dyn ironclaw_resources::BudgetEventSink>,
    #[cfg(any(test, feature = "test-support"))]
    in_memory_budget_event_sink: Arc<ironclaw_resources::InMemoryBudgetEventSink>,
    broadcast_budget_event_sink: Arc<ironclaw_resources::BroadcastBudgetEventSink>,
}

fn build_budget_sinks() -> BudgetSinks {
    let in_memory_budget_event_sink = Arc::new(ironclaw_resources::InMemoryBudgetEventSink::new());
    let broadcast_budget_event_sink =
        Arc::new(ironclaw_resources::BroadcastBudgetEventSink::default());
    let budget_event_sink: Arc<dyn ironclaw_resources::BudgetEventSink> =
        Arc::new(ironclaw_resources::CompositeBudgetEventSink::new(vec![
            Arc::clone(&in_memory_budget_event_sink)
                as Arc<dyn ironclaw_resources::BudgetEventSink>,
            Arc::clone(&broadcast_budget_event_sink)
                as Arc<dyn ironclaw_resources::BudgetEventSink>,
        ]));
    BudgetSinks {
        budget_event_sink,
        #[cfg(any(test, feature = "test-support"))]
        in_memory_budget_event_sink,
        broadcast_budget_event_sink,
    }
}

/// The `HostRuntimeServices` wiring shared by the standalone and production
/// build paths (F4): the ten `.with_*` setters both paths always apply, plus
/// the fixed `TracingSecurityAuditSink`. Single-sourced as a macro because the
/// builder is generic over four backend type params and the setters are
/// value-generic (e.g. `with_trust_policy<T>`), so a function would have to
/// thread all of them; the macro defers typing to each expansion site.
/// Backend-specific setters (approval requests, resource governor, event
/// stores, the wake-notifier variant) are appended by the caller after this —
/// order is irrelevant because each setter writes an independent field.
macro_rules! with_shared_host_runtime_wiring {
    (
        $services:expr,
        trust_policy = $trust:expr,
        runtime_policy = $runtime_policy:expr,
        capability_leases = $leases:expr,
        persistent_approval_policies = $policies:expr,
        secret_store = $secret:expr,
        credential_broker = $broker:expr,
        filesystem_run_state = $fs:expr,
        turn_state_and_transition_port = $turn_state:expr,
        run_profile_resolver = $resolver:expr $(,)?
    ) => {
        $services
            .with_trust_policy($trust)
            .with_runtime_policy($runtime_policy)
            .with_capability_leases($leases)
            .with_persistent_approval_policies($policies)
            .with_security_audit_sink(::std::sync::Arc::new(
                ironclaw_events::TracingSecurityAuditSink,
            ))
            .with_secret_store($secret)
            .with_credential_broker($broker)
            .with_filesystem_run_state($fs)
            .with_turn_state_and_transition_port($turn_state)
            .with_run_profile_resolver($resolver)
    };
}

/// Open a PostgreSQL pool from a build-time [`PostgresPoolSource`] (Phase B).
///
/// Production (`*_from_config_and_env`) carries `Config` and the pool is opened
/// here, at build time, from declarative connection config — construction no
/// longer performs database I/O. The `Prebuilt` arm is the caller-supplied
/// test escape hatch and is preferred verbatim when present.
fn open_postgres_pool_from_source(
    source: PostgresPoolSource,
) -> Result<deadpool_postgres::Pool, RebornBuildError> {
    match source {
        PostgresPoolSource::Prebuilt(pool) => Ok(pool),
        PostgresPoolSource::Config(connection) => Ok(
            ironclaw_reborn_event_store::open_postgres_pool_with_tls_options(
                connection.url,
                connection.pool_max_size,
                connection.tls_options,
            )?,
        ),
    }
}

/// Open a libSQL database from a build-time [`LibsqlConnectionConfig`]
/// (Phase B). Scheme detection mirrors
/// `ironclaw_reborn_event_store`'s libsql backend: recognised remote schemes
/// (`libsql://`, `https://`, `http://`, case-insensitive) route through
/// `Builder::new_remote` with the auth token; everything else is a local file.
async fn open_libsql_database_from_connection(
    connection: &LibsqlConnectionConfig,
) -> Result<Arc<libsql::Database>, RebornBuildError> {
    use secrecy::ExposeSecret;

    let path_or_url = connection.path_or_url.as_str();
    let build_result = if is_remote_libsql_target(path_or_url) {
        libsql::Builder::new_remote(
            path_or_url.to_string(),
            connection
                .auth_token
                .as_ref()
                .map(|token| token.expose_secret().to_string())
                .unwrap_or_default(),
        )
        .build()
        .await
    } else {
        libsql::Builder::new_local(path_or_url).build().await
    };
    build_result
        .map(Arc::new)
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("libSQL database could not be opened: {error}"),
        })
}

/// Detect a remote libSQL endpoint by recognised URL scheme, case-insensitively
/// (mirrors `ironclaw_reborn_event_store::libsql_backed::is_remote_libsql`).
fn is_remote_libsql_target(path_or_url: &str) -> bool {
    let Some(scheme_end) = path_or_url.find("://") else {
        return false;
    };
    let scheme = &path_or_url[..scheme_end];
    scheme.eq_ignore_ascii_case("libsql")
        || scheme.eq_ignore_ascii_case("https")
        || scheme.eq_ignore_ascii_case("http")
}

pub(crate) async fn build_secret_store<F>(
    root: &Path,
    scoped_filesystem: Arc<ScopedFilesystem<F>>,
    explicit_master_key: Option<ironclaw_secrets::SecretMaterial>,
) -> Result<(Arc<SecretStore<F>>, Arc<ironclaw_secrets::SecretsCrypto>), RebornBuildError>
where
    F: RootFilesystem + 'static,
{
    let master_key = match explicit_master_key {
        Some(master_key) => master_key,
        None => resolve_standalone_secret_master_key(root).await?,
    };
    // The crypto is returned alongside the store so the admin secret
    // provisioner (`admin_secrets.rs`) can build per-target-user stores that
    // share the SAME master key — secrets written admin-side decrypt under the
    // user's own store and vice versa.
    let crypto = Arc::new(ironclaw_secrets::SecretsCrypto::new(master_key)?);
    let store = Arc::new(SecretStore::new(scoped_filesystem, Arc::clone(&crypto)));
    Ok((store, crypto))
}

/// Open the `/secrets` store alone, without building the rest of the
/// standalone [`CompositeRootFilesystem`] (project mounts, extension mounts,
/// trigger/project repositories, …).
///
/// - Pre-composition entry point `ironclaw-reborn onboard` needs: it must
///   write a provider API key before a full build-input-driven build exists,
///   and reconstructing the whole composite just to reach one mount is
///   heavy and risks silently diverging from `serve`'s copy.
/// - `/secrets`'s physical backing is the same standalone libSQL file
///   `build_standalone_root_filesystem` opens for `/tenants` in production —
///   a key written here is immediately visible to `serve`, no extra
///   coordination needed.
/// - Uses the same resolver chain as production (env -> cached dotfile ->
///   OS keychain -> generate-and-cache, via [`build_secret_store`]).
/// - `run_migrations()` here and again on `serve`'s later open is safe —
///   already relied on as idempotent elsewhere in this module's tests.
pub async fn open_standalone_secret_store(
    root: &Path,
) -> Result<Arc<dyn SecretStorePort>, RebornBuildError> {
    let db = open_standalone_libsql_database(root).await?;
    let filesystem = Arc::new(LibSqlRootFilesystem::new(db));
    filesystem.run_migrations().await?;
    let scoped = crate::wrap_scoped(filesystem);
    let (store, _crypto) = build_secret_store(root, scoped, None).await?;
    Ok(store as Arc<dyn SecretStorePort>)
}

/// Where a resolved standalone master key came from, used to name the source in
/// fail-loud error messages.
enum MasterKeySource {
    File(PathBuf),
    Env,
    Keychain,
}

/// Validate a resolved master key against the same rules `SecretsCrypto::new`
/// enforces, mapping a rejection to a `RebornBuildError` that names *where the
/// key came from* and the offending path/env var.
///
/// Without this, a corrupt cached key file or a malformed `SECRETS_MASTER_KEY`
/// env value surfaces only as the opaque "Invalid master key" raised several
/// layers deep in `SecretsCrypto::new`, with no pointer to the file the
/// operator must fix. See `.claude/rules/error-handling.md` (fail loud, name
/// the operation).
fn validate_resolved_master_key(
    key: &str,
    source: &MasterKeySource,
) -> Result<(), RebornBuildError> {
    ironclaw_secrets::validate_master_key_material(key.as_bytes()).map_err(|error| {
        let location = match source {
            MasterKeySource::File(path) => format!("file {}", path.display()),
            MasterKeySource::Env => format!(
                "env var {}",
                ironclaw_secrets::keychain::SECRETS_MASTER_KEY_ENV
            ),
            MasterKeySource::Keychain => "the OS keychain".to_string(),
        };
        RebornBuildError::InvalidConfig {
            reason: format!(
                "standalone secrets master key from {location} is malformed: {error}; \
                 it must be at least 32 bytes with at least 8 distinct byte values. \
                 Remove or replace it and retry."
            ),
        }
    })
}

async fn resolve_standalone_secret_master_key(
    root: &Path,
) -> Result<ironclaw_secrets::SecretMaterial, RebornBuildError> {
    // Fail closed on an explicitly-set-but-unusable master key: only an
    // *absent* env var is "not configured". A non-Unicode value must not be
    // silently dropped (via `.ok()`) and fall through to generating a fresh
    // key, which would encrypt standalone secrets under an unintended key the
    // operator never chose.
    let env_key = match std::env::var(ironclaw_secrets::keychain::SECRETS_MASTER_KEY_ENV) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(RebornBuildError::InvalidConfig {
                reason: format!(
                    "standalone secrets master key env var {} is set but not valid UTF-8",
                    ironclaw_secrets::keychain::SECRETS_MASTER_KEY_ENV
                ),
            });
        }
    };
    resolve_standalone_secret_master_key_with_env(root, env_key).await
}

/// Inner resolver that takes the `SECRETS_MASTER_KEY` env value as a parameter
/// so the write-before-validate invariant can be exercised through this real
/// caller in tests without mutating process-global env (which is racy under
/// `cargo test`'s parallel harness).
///
/// Resolution order: cached dotfile -> explicit/env key -> OS keychain
/// (suppressed under test/CI, see
/// `ironclaw_secrets::keychain::get_master_key`) -> generate a fresh key and
/// persist it to the dotfile. The env key is VALIDATED up front so a bad
/// explicit value fails closed regardless of cached state, but a valid cached
/// dotfile deliberately wins over it: the existing secret store is encrypted
/// under the cached key, and silently switching to a different env key would
/// make that store undecryptable. A keychain hit is returned as-is and never
/// written to the dotfile — the dotfile and keychain are alternative sources
/// for the same secret, not layered, so writing both would mean the two
/// copies must agree forever.
async fn resolve_standalone_secret_master_key_with_env(
    root: &Path,
    env_key: Option<String>,
) -> Result<ironclaw_secrets::SecretMaterial, RebornBuildError> {
    // Fully resolve and VALIDATE an explicitly-set env value UP FRONT, before
    // the cached file read. Otherwise a rebuild where
    // `.reborn-local-dev-secrets-master-key` already exists returns the cached
    // key and silently ignores the operator's bad explicit env config — whether
    // it is empty OR a malformed non-empty value (e.g. `0000...`). Validating
    // here means any explicit-but-unusable env key fails closed regardless of
    // cached state.
    let env_key = match env_key {
        Some(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                return Err(RebornBuildError::InvalidConfig {
                    reason: format!(
                        "standalone secrets master key env var {} is set but empty",
                        ironclaw_secrets::keychain::SECRETS_MASTER_KEY_ENV
                    ),
                });
            }
            validate_resolved_master_key(&trimmed, &MasterKeySource::Env)?;
            Some(trimmed)
        }
        None => None,
    };

    let key_path = root.join(STANDALONE_SECRETS_MASTER_KEY_PATH);
    match std::fs::read_to_string(&key_path) {
        Ok(existing) => {
            let key = existing.trim().to_string();
            validate_resolved_master_key(&key, &MasterKeySource::File(key_path.clone()))?;
            return Ok(ironclaw_secrets::SecretMaterial::from(key));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(RebornBuildError::InvalidConfig {
                reason: format!(
                    "standalone secrets master key at {} could not be read: {error}",
                    key_path.display()
                ),
            });
        }
    }

    // No cached file. Prefer the explicit (already-validated) env key.
    if let Some(key) = env_key {
        write_standalone_secret_master_key(&key_path, &key)?;
        return Ok(ironclaw_secrets::SecretMaterial::from(key));
    }

    // No env key either. Try the OS keychain next (suppressed under test/CI —
    // see `ironclaw_secrets::keychain::get_master_key`, which returns
    // `NotFound` when suppressed so this falls through exactly as it would
    // for a genuinely empty keychain). Deliberately calling `get_master_key`
    // directly rather than `resolve_master_key_material`: this resolver
    // already owns the env-var branch above, and `resolve_master_key_material`
    // re-checks the env var itself — calling it here would mean two
    // independent env-precedence implementations that could disagree.
    match ironclaw_secrets::keychain::get_master_key().await {
        Ok(key_bytes) => {
            let key_hex = key_bytes
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            validate_resolved_master_key(&key_hex, &MasterKeySource::Keychain)?;
            // Keychain hit: return as-is, do not also write the dotfile — the
            // dotfile and keychain are alternative sources, not layered.
            return Ok(ironclaw_secrets::SecretMaterial::from(key_hex));
        }
        Err(_) => {
            // Miss or error (including suppressed-under-test): fall through
            // to generating a fresh key, unchanged from prior behavior.
            //
            // Accepted risk: intentionally blanket — this collapses "no key
            // in the keychain yet" and "keychain unreachable" into the same
            // fallback. Headless containers (e.g. Railway) have no
            // secret-service daemon at all, so `get_master_key` returns a
            // generic `SecretError::KeychainError` there, not a distinguishable
            // `NotFound`; narrowing this match to only fall through on
            // `NotFound` would make every container boot fail closed instead
            // of falling back to the dotfile. Worst case of the current
            // broad match: a transient keychain error on a real desktop
            // causes a wrongly-regenerated dotfile key, which just means
            // re-entering one API key on the next `onboard`/`serve` run.
        }
    }

    // No cached file, no env key, no keychain hit. Generate a fresh key.
    let key = ironclaw_secrets::keychain::generate_master_key_hex();
    write_standalone_secret_master_key(&key_path, &key)?;
    Ok(ironclaw_secrets::SecretMaterial::from(key))
}

fn write_standalone_secret_master_key(path: &Path, key: &str) -> Result<(), RebornBuildError> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("standalone secrets master key could not be created: {error}"),
            })?;
        file.write_all(key.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("standalone secrets master key could not be written: {error}"),
            })
    }
    #[cfg(windows)]
    {
        use std::io::Write as _;

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("standalone secrets master key could not be created: {error}"),
            })?;
        let account = std::env::var("USERDOMAIN")
            .ok()
            .filter(|domain| !domain.trim().is_empty())
            .zip(
                std::env::var("USERNAME")
                    .ok()
                    .filter(|user| !user.trim().is_empty()),
            )
            .map(|(domain, user)| format!("{domain}\\{user}"))
            .or_else(|| std::env::var("USERNAME").ok())
            .ok_or_else(|| RebornBuildError::InvalidConfig {
                reason: "standalone secrets master key could not be restricted: USERNAME is unset"
                    .to_string(),
            })?;
        let status = std::process::Command::new("icacls")
            .arg(path)
            .arg("/inheritance:r")
            .arg("/grant:r")
            .arg(format!("{account}:F"))
            .status()
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!(
                    "standalone secrets master key permissions could not be set: {error}"
                ),
            })?;
        if !status.success() {
            let _ = std::fs::remove_file(path);
            return Err(RebornBuildError::InvalidConfig {
                reason: format!(
                    "standalone secrets master key permissions could not be set: icacls exited with {status}"
                ),
            });
        }
        file.write_all(key.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("standalone secrets master key could not be written: {error}"),
            })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        let _ = key;
        Err(RebornBuildError::InvalidConfig {
            reason:
                "standalone filesystem secret persistence requires Unix permissions or Windows ACLs"
                    .to_string(),
        })
    }
}

/// Outcome of provisioning a standalone secrets master key directly into the
/// OS keychain (as opposed to `resolve_standalone_secret_master_key_with_env`'s
/// full resolution chain, which is only consulted at boot time). Used by
/// `onboard`'s standalone keychain-provisioning step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeychainMasterKeyOutcome {
    /// The OS keychain already has a master key from a prior onboarding run.
    AlreadyPresent,
    /// A fresh key was generated and stored in the OS keychain.
    Provisioned,
    /// The OS keychain is unavailable (suppressed under test/CI, or the OS
    /// denied the write).
    Suppressed,
}

/// Facade over `ironclaw_secrets::keychain` for onboarding's OS-keychain
/// master-key provisioning step.
///
/// - Lets callers outside this crate (`ironclaw_reborn_cli`) avoid their own
///   `ironclaw_secrets` dependency — pinned by
///   `reborn_dependency_boundaries.rs::reborn_cli_binary_crate_stays_separate_from_v1_root`.
/// - No key yet -> generate + store; already populated -> no-op `AlreadyPresent`.
/// - Never returns an error: unavailable/denied keychain reports `Suppressed`,
///   matching `resolve_standalone_secret_master_key_with_env`'s env/dotfile fallback.
pub async fn provision_standalone_keychain_master_key() -> KeychainMasterKeyOutcome {
    // `has_master_key()` collapses "no key yet" and "backend/permission/locked
    // error probing the keychain" into the same `false` — a false negative
    // here falls through to `generate` + `store` below, which overwrites
    // whatever key the keychain actually holds. Same accepted-risk class as
    // the TOCTOU documented on this function's only caller
    // (`ironclaw_reborn_cli::commands::onboard::master_key::provision_master_key`):
    // Standalone, single-operator, run-once-by-hand; worst case is a
    // wrongly-regenerated key recoverable by re-entering one API key.
    if ironclaw_secrets::keychain::has_master_key().await {
        return KeychainMasterKeyOutcome::AlreadyPresent;
    }
    let key = ironclaw_secrets::keychain::generate_master_key();
    match ironclaw_secrets::keychain::store_master_key(&key).await {
        Ok(()) => KeychainMasterKeyOutcome::Provisioned,
        Err(error) => {
            tracing::debug!(
                %error,
                "OS keychain store of standalone secrets master key failed during onboarding; \
                 falling back to env/dotfile resolution"
            );
            KeychainMasterKeyOutcome::Suppressed
        }
    }
}

/// The host-owned outbound target registry always exposes the WebApp
/// final-reply destination (#6520 run-scoped delivery): channel extensions add
/// their targets at activation, but "store the answer in run history" is a
/// host affordance that must exist even with zero channels active.
fn host_owned_outbound_delivery_target_registry()
-> Result<Arc<crate::outbound::MutableOutboundDeliveryTargetRegistry>, RebornBuildError> {
    let registry = Arc::new(crate::outbound::MutableOutboundDeliveryTargetRegistry::default());
    let web_app = ironclaw_outbound::OutboundDeliveryTargetSummary::new(
        ironclaw_outbound::OutboundDeliveryTargetId::new(
            ironclaw_outbound::WEB_APP_OUTBOUND_DELIVERY_TARGET_ID,
        )
        .map_err(|reason| RebornBuildError::InvalidConfig {
            reason: format!("host-owned WebApp target id is invalid: {reason}"),
        })?,
        "web_app",
        "Web app only",
        Some("Store the final answer in run history without external delivery.".to_string()),
    )
    .map_err(|reason| RebornBuildError::InvalidConfig {
        reason: format!("host-owned WebApp delivery target is invalid: {reason}"),
    })?;
    registry
        .register_provider(
            ironclaw_outbound::WEB_APP_OUTBOUND_DELIVERY_TARGET_ID,
            Arc::new(
                ironclaw_outbound::HostOwnedOutboundDeliveryTargetProvider::new(
                    web_app,
                    ironclaw_outbound::DeliveryTargetCapabilities {
                        final_replies: true,
                        progress: false,
                        gate_prompts: false,
                        auth_prompts: false,
                        modalities: Vec::new(),
                    },
                    ironclaw_outbound::RunFinalReplyDestination::WebApp,
                ),
            ),
        )
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("host-owned WebApp delivery target registration failed: {error}"),
        })?;
    Ok(registry)
}

pub(crate) fn builtin_extension_registry() -> Result<ExtensionRegistry, RebornBuildError> {
    // Shared by standalone and production composition so host-owned first-party
    // capabilities expose the same built-in package contract in both profiles.
    let mut registry = ExtensionRegistry::new();
    registry
        .insert(
            builtin_first_party_package().map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("built-in first-party package is invalid: {error}"),
            })?,
        )
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("built-in first-party registry is invalid: {error}"),
        })?;
    insert_native_memory_package(&mut registry)?;
    Ok(registry)
}

/// Insert the always-on `ironclaw.memory` package into a registry that
/// already holds the builtin package. Native memory rides the same always-on
/// lane as builtin (not the catalog/lifecycle lane), so it is registered here
/// directly rather than discovered from the extension catalog.
fn insert_native_memory_package(registry: &mut ExtensionRegistry) -> Result<(), RebornBuildError> {
    registry
        .insert(native_memory_first_party_package().map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: format!("native memory first-party package is invalid: {error}"),
            }
        })?)
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("native memory first-party registry is invalid: {error}"),
        })
}

fn production_builtin_extension_registry(
    process_backend: ProcessBackendKind,
) -> Result<ExtensionRegistry, RebornBuildError> {
    let mut registry = ExtensionRegistry::new();
    let package =
        builtin_first_party_package_for_process_backend(process_backend).map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: format!("built-in first-party package is invalid: {error}"),
            }
        })?;
    let package = extend_builtin_first_party_package(package).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("extension lifecycle package is invalid: {error}"),
        }
    })?;
    let package = extend_builtin_admin_configuration_package(package).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("administrator configuration package is invalid: {error}"),
        }
    })?;
    let package = extend_builtin_operator_config_package(package).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("operator configuration package is invalid: {error}"),
        }
    })?;
    let package = extend_builtin_outbound_preferences_package(package).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("outbound preferences package is invalid: {error}"),
        }
    })?;
    let package = extend_builtin_skill_auto_activate_package(package).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("skill auto-activation package is invalid: {error}"),
        }
    })?;
    registry
        .insert(package)
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("built-in first-party registry is invalid: {error}"),
        })?;
    insert_native_memory_package(&mut registry)?;
    Ok(registry)
}

fn production_first_party_registry_with_trigger_create_hook(
    trigger_repository: Arc<dyn TriggerRepository>,
    trigger_create_hook: Arc<dyn TriggerCreateHook>,
    active_run_lookup: Arc<dyn TriggerActiveRunLookup>,
    process_backend: ProcessBackendKind,
    memory_resolver: MemoryServiceResolver,
) -> Result<FirstPartyCapabilityRegistry, RebornBuildError> {
    builtin_first_party_handlers_with_trigger_create_hook_for_process_backend_and_memory_resolver(
        trigger_repository,
        trigger_create_hook,
        active_run_lookup,
        process_backend,
        memory_resolver,
    )
    .map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("built-in first-party handlers are invalid: {error}"),
    })
}

fn manifest_channel_account_setup_descriptors(
    manifests: &[Arc<ironclaw_extensions::ResolvedExtensionManifest>],
) -> Vec<ExtensionAccountSetupDescriptor> {
    manifests
        .iter()
        .filter_map(|manifest| {
            let channel = manifest.channel.as_ref()?;
            let connection = channel.connection.as_ref()?;
            if connection.strategy != ironclaw_host_api::ChannelConnectionStrategy::WebGeneratedCode
            {
                return None;
            }
            Some(ExtensionAccountSetupDescriptor {
                extension_id: manifest.id.clone(),
                auth_requirement: ironclaw_host_api::RuntimeCredentialAuthRequirement {
                    provider: connection.provider.clone(),
                    setup: ironclaw_host_api::RuntimeCredentialAccountSetup::Pairing,
                    requester_extension: manifest.id.clone(),
                    provider_scopes: Vec::new(),
                },
                connection_requirement: ChannelConnectionRequirement {
                    channel: manifest.id.as_str().to_string(),
                    display_name: manifest.name.clone(),
                    strategy: ironclaw_product::RebornChannelConnectStrategy::WebGeneratedCode,
                    instructions: connection.instructions.clone(),
                    input_placeholder: connection.input_placeholder.clone(),
                    submit_label: connection.submit_label.clone(),
                    error_message: connection.error_message.clone(),
                },
                connection_notices: ChannelConnectionNoticePolicy {
                    connect_required: connection.notices.connect_required.clone(),
                    paired: connection.notices.paired.clone(),
                    already_paired_same_user: connection.notices.already_paired_same_user.clone(),
                    already_bound_to_other_user: connection
                        .notices
                        .already_bound_to_other_user
                        .clone(),
                    expired_or_unknown: connection.notices.expired_or_unknown.clone(),
                },
                activation_success_message: connection.connection_success_message.clone(),
                pairing_deep_link_template: connection.deep_link_template.clone(),
                inbound_code_prefixes: connection.inbound_code_prefixes.clone(),
            })
        })
        .collect()
}

/// Build the production first-party trust policy from the binary-injected
/// neutral bundle set (extension-runtime DEL-7). The provider entry comes from
/// `builtin_capability_policy` (no first-party dependency); each package's host
/// authority grant is sourced from its injected `trust_effects` instead of a
/// direct `ironclaw_first_party_extensions` call. Every entry is byte-identical
/// to the one the inventory-driven builder produced — same id, local-manifest
/// path, manifest digest, and effect list — so behavior is preserved exactly.
pub fn production_first_party_trust_policy(
    bundles: &[ironclaw_extension_host::FirstPartyPackageBundle],
) -> Result<HostTrustPolicy, RebornBuildError> {
    let policy = builtin_capability_policy().map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("standalone capability policy is invalid: {error}"),
    })?;
    let mut entries = vec![
        AdminEntry::for_local_manifest(
            policy.provider.id,
            policy.provider.manifest_path,
            None,
            HostTrustAssignment::first_party(),
            // Sourced from builtin_capability_policy.toml `[provider]
            // authority_effects`, which includes `external_write` — required by
            // builtin.trace_commons.onboard (operator-invite enrollment posts to
            // an external onboarding server).
            policy.provider.authority_effects,
            None,
        ),
        // Native memory rides the always-on first-party lane alongside builtin
        // (it is registered into the builtin extension registry, not discovered
        // from the catalog), so it carries its own first-party trust entry. The
        // path is a stable identifier only — `for_local_manifest` does not read
        // it — because native memory is constructed in code, not from a bundled
        // manifest file. Its effects are the document-store provider's needs.
        AdminEntry::for_local_manifest(
            PackageId::new(NATIVE_MEMORY_FIRST_PARTY_PROVIDER).map_err(|error| {
                RebornBuildError::InvalidConfig {
                    reason: format!("native memory first-party package id is invalid: {error}"),
                }
            })?,
            "/system/extensions/ironclaw.memory/manifest.toml".to_string(),
            None,
            HostTrustAssignment::first_party(),
            vec![
                ironclaw_host_api::EffectKind::DispatchCapability,
                ironclaw_host_api::EffectKind::ReadFilesystem,
                ironclaw_host_api::EffectKind::WriteFilesystem,
            ],
            None,
        ),
    ];
    // Packages supply their own trust grant as data (`trust_effects`);
    // composition still owns the decision (`first_party`) and the policy
    // construction. Packages with `None` (WASM tools, channel-only) draw trust
    // from the extension registry instead and are skipped here.
    for bundle in bundles {
        let Some(effects) = bundle.trust_effects.clone() else {
            continue;
        };
        entries.push(AdminEntry::for_local_manifest(
            PackageId::new(bundle.id.as_str()).map_err(|error| {
                RebornBuildError::InvalidConfig {
                    reason: format!("first-party package id '{}' is invalid: {error}", bundle.id),
                }
            })?,
            format!("/system/extensions/{}/manifest.toml", bundle.id),
            Some(sha256_digest_token(bundle.manifest_toml.as_bytes())),
            HostTrustAssignment::first_party(),
            effects,
            None,
        ));
    }
    HostTrustPolicy::new(vec![Box::new(AdminConfig::with_entries(entries))]).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("built-in first-party trust policy is invalid: {error}"),
        }
    })
}

/// Inventory-driven trust policy for composition's own unit tests (mirrors the
/// production builder, sourcing the neutral bundle set from the concrete
/// inventory). Gated `#[cfg(test)]` because it names
/// `ironclaw_first_party_extensions`, a dev-dependency; integration tests build
/// their trust policy from `production_first_party_trust_policy` plus bundles
/// they convert themselves (see `tests/support/first_party.rs`).
#[cfg(test)]
pub(crate) fn builtin_first_party_trust_policy() -> Result<HostTrustPolicy, RebornBuildError> {
    production_first_party_trust_policy(
        &ironclaw_extension_host::test_support::first_party_bundles_from_inventory(),
    )
}

#[cfg(test)]
fn nearai_allowed_effects() -> Vec<ironclaw_host_api::EffectKind> {
    vec![
        ironclaw_host_api::EffectKind::DispatchCapability,
        ironclaw_host_api::EffectKind::Network,
        ironclaw_host_api::EffectKind::UseSecret,
    ]
}

async fn build_production_shaped(
    input: RebornHostBindings,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    let RebornHostBindings {
        deployment,
        storage,
        production_trust_policy,
        // The notifier field on `RebornHostBindings` is kept for backward
        // compatibility with test callers that pre-mint one, but the
        // production-shaped build now mints its own notifier internally so the
        // coordinator and scheduler always share the exact same channel.
        turn_run_wake_notifier: _,
        runtime_process_binding,
        product_auth_ports,
        native_extension_factories,
        channel_extension_bindings,
        first_party_registrars,
        credential_account_visibility_policy,
        #[cfg(any(test, feature = "test-support"))]
        network_http_egress_for_test,
        #[cfg(any(test, feature = "test-support"))]
        trust_fixture_extensions_for_test,
        memory_binding_policy,
        memory_provider_connection,
        ..
    } = input;
    // The declarative DATA now lives on the deployment (Phase A). Clone the
    // fields this build path consumes by value; `deployment` stays in scope for
    // its substrate/traffic/readiness axes below.
    let owner_id = deployment.owner_id.clone();
    let local_runtime_identity = deployment.local_runtime_identity.clone();
    let runtime_policy = deployment.runtime_policy.clone();
    let account_setup_descriptors = deployment.account_setup_descriptors.clone();
    let oauth_provider_configs = deployment.oauth_provider_configs.clone();
    let oauth_dcr_callback = deployment.oauth_dcr_callback.clone();
    let nearai_mcp_bootstrap_config = deployment.nearai_mcp_bootstrap_config.clone();
    let turn_state_store_limits = deployment.turn_state_store_limits;
    let first_party_bundles = deployment.first_party_bundles.clone();
    let traffic_policy = deployment.traffic();
    // Build the single memory provider resolver for this runtime (issue #3537):
    // the memory tools and the standalone profile source build their
    // `MemoryService` through it. For a standalone workspace, bound mem0 memory to
    // this workspace (issue #5264) so memories from one standalone root never leak
    // into another sharing the same mem0 server; production keeps `app_id` from
    // config. An explicitly-configured `app_id` always wins.
    let memory_service_resolver = {
        let mut memory_provider_connection = memory_provider_connection;
        if memory_provider_connection.app_id.is_none()
            && let crate::input::RebornStorageInput::LocalFilesystem { root, .. } = &storage
        {
            use std::hash::{DefaultHasher, Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            root.hash(&mut hasher);
            memory_provider_connection.app_id = Some(format!("ws-{:016x}", hasher.finish()));
        }
        crate::build_memory_service_resolver(
            memory_binding_policy,
            &crate::MemoryProviderDeps::for_third_party(memory_provider_connection),
        )
    };
    // Label for logging/errors; behaviour reads `deployment`'s axes.
    let profile = deployment.profile();
    let wiring_config = production_config(
        deployment.required_runtime_backends.clone(),
        deployment.require_runtime_http_egress,
        deployment.require_wasm_credentials,
    );
    // The built-in first-party trust policy is composed here, at BUILD time,
    // from the binary-injected neutral bundle set (extension-runtime DEL-7) when
    // the caller did not pre-supply one — construction time (input.rs) predates
    // bundle injection. Same grants as the inventory-driven builder, sourced
    // from injected data instead of a direct `ironclaw_first_party_extensions`
    // call.
    let production_trust_policy = match production_trust_policy {
        Some(policy) => Some(policy),
        None => Some(Arc::new(production_first_party_trust_policy(
            &first_party_bundles,
        )?)),
    };
    match storage {
        RebornStorageInput::Disabled => Err(RebornBuildError::InvalidConfig {
            reason: format!(
                "profile={} requires durable database-backed Reborn storage",
                profile
            ),
        }),
        RebornStorageInput::LocalFilesystem {
            root,
            workspace_root,
            host_home_root,
        } => {
            let scheduler_wake_wiring = ironclaw_runner::runtime::SchedulerWakeWiring::channel();
            let runtime_policy_for_local_process = runtime_policy.clone();
            let production_wiring = production_wiring(
                traffic_policy,
                production_trust_policy,
                runtime_policy,
                scheduler_wake_wiring.notifier(),
                runtime_process_binding,
            )?;
            let context = RebornProductionBuildContext {
                profile,
                wiring_config,
                production_wiring,
                local_process_port: None,
                product_auth_ports,
                oauth_provider_configs,
                oauth_dcr_callback,
                owner_id,
                local_runtime_identity,
                turn_state_store_limits,
                memory_resolver: memory_service_resolver.clone(),
                scheduler_wake_wiring,
                account_setup_descriptors,
                nearai_mcp_bootstrap_config,
                native_extension_factories,
                channel_extension_bindings,
                first_party_bundles,
                first_party_registrars,
                credential_account_visibility_policy,
                workspace_filesystems: None,
                standalone_storage_root: None,
                default_system_prompt_path: None,
                #[cfg(any(test, feature = "test-support"))]
                network_http_egress_for_test: network_http_egress_for_test.clone(),
                #[cfg(any(test, feature = "test-support"))]
                trust_fixture_extensions_for_test,
            };
            build_local_storage_production_shaped(
                context,
                LocalStorageProductionInput {
                    root,
                    workspace_root,
                    host_home_root,
                    storage_backend_input: DurableStorageInput::EmbeddedLibsql,
                    explicit_secret_master_key: None,
                    runtime_policy_for_local_process,
                    postgres_resource_governor_singleton: None,
                },
            )
            .await
        }
        RebornStorageInput::HostedSingleTenantPostgres {
            root,
            workspace_root,
            host_home_root,
            pool_source,
            secret_master_key,
            process_local_resource_governor_singleton,
        } => {
            // Phase B: open (or accept the test-supplied) pool at build time.
            let pool = open_postgres_pool_from_source(pool_source)?;
            let scheduler_wake_wiring = ironclaw_runner::runtime::SchedulerWakeWiring::channel();
            let runtime_policy_for_local_process = runtime_policy.clone();
            let production_wiring = production_wiring(
                traffic_policy,
                production_trust_policy,
                runtime_policy,
                scheduler_wake_wiring.notifier(),
                runtime_process_binding,
            )?;
            let context = RebornProductionBuildContext {
                profile,
                wiring_config,
                production_wiring,
                local_process_port: None,
                product_auth_ports,
                oauth_provider_configs,
                oauth_dcr_callback,
                owner_id,
                local_runtime_identity,
                turn_state_store_limits,
                memory_resolver: memory_service_resolver.clone(),
                scheduler_wake_wiring,
                account_setup_descriptors,
                nearai_mcp_bootstrap_config,
                native_extension_factories,
                channel_extension_bindings,
                first_party_bundles,
                first_party_registrars,
                credential_account_visibility_policy,
                workspace_filesystems: None,
                standalone_storage_root: None,
                default_system_prompt_path: None,
                #[cfg(any(test, feature = "test-support"))]
                network_http_egress_for_test: network_http_egress_for_test.clone(),
                #[cfg(any(test, feature = "test-support"))]
                trust_fixture_extensions_for_test,
            };
            build_local_storage_production_shaped(
                context,
                LocalStorageProductionInput {
                    root,
                    workspace_root,
                    host_home_root,
                    storage_backend_input: DurableStorageInput::Postgres(pool),
                    explicit_secret_master_key: Some(secret_master_key),
                    runtime_policy_for_local_process,
                    postgres_resource_governor_singleton: Some(
                        process_local_resource_governor_singleton,
                    ),
                },
            )
            .await
        }
        RebornStorageInput::Libsql {
            connection,
            prebuilt_db,
            secret_master_key,
            process_local_resource_governor_singleton,
        } => {
            // Mint the scheduler wake wiring here, before building the coordinator, so:
            // 1. The notifier can satisfy `HostRuntimeServices.with_turn_run_wake_notifier_dyn`
            //    (required by `validate_production_wiring` / `turn_coordinator_for_production`).
            // 2. The wiring is threaded through `RebornRuntimeStores` →
            //    `DefaultPlannedRuntimeParts.scheduler_wake_wiring` so the
            //    `build_default_planned_runtime` scheduler loop consumes the exact same channel,
            //    ensuring the coordinator's notifier and the scheduler share a live queue.
            let scheduler_wake_wiring = ironclaw_runner::runtime::SchedulerWakeWiring::channel();
            let production_wiring = production_wiring(
                traffic_policy,
                production_trust_policy,
                runtime_policy,
                scheduler_wake_wiring.notifier(),
                runtime_process_binding,
            )?;
            let secret_master_key = resolve_secret_master_key(secret_master_key).await?;
            // Phase B: prefer the test-supplied handle; otherwise open the
            // database from the declarative connection config at build time.
            let db = match prebuilt_db {
                Some(db) => db,
                None => open_libsql_database_from_connection(&connection).await?,
            };
            let context = RebornProductionBuildContext {
                profile,
                wiring_config,
                production_wiring,
                local_process_port: None,
                product_auth_ports,
                oauth_provider_configs,
                oauth_dcr_callback,
                owner_id,
                local_runtime_identity,
                turn_state_store_limits,
                memory_resolver: memory_service_resolver.clone(),
                scheduler_wake_wiring,
                account_setup_descriptors,
                nearai_mcp_bootstrap_config,
                native_extension_factories,
                channel_extension_bindings,
                first_party_bundles,
                first_party_registrars,
                credential_account_visibility_policy,
                workspace_filesystems: None,
                standalone_storage_root: None,
                default_system_prompt_path: None,
                #[cfg(any(test, feature = "test-support"))]
                network_http_egress_for_test: network_http_egress_for_test.clone(),
                #[cfg(any(test, feature = "test-support"))]
                trust_fixture_extensions_for_test,
            };
            build_libsql_production(
                context,
                db,
                connection.path_or_url,
                connection.auth_token,
                secret_master_key,
                process_local_resource_governor_singleton,
            )
            .await
        }
        RebornStorageInput::Postgres {
            pool_source,
            secret_master_key,
            process_local_resource_governor_singleton,
        } => {
            // Phase B: open (or accept the test-supplied) pool at build time.
            let pool = open_postgres_pool_from_source(pool_source)?;
            // Mint the scheduler wake wiring here, before building the coordinator, so:
            // 1. The notifier can satisfy `HostRuntimeServices.with_turn_run_wake_notifier_dyn`
            //    (required by `validate_production_wiring` / `turn_coordinator_for_production`).
            // 2. The wiring is threaded through `RebornRuntimeStores` →
            //    `DefaultPlannedRuntimeParts.scheduler_wake_wiring` so the
            //    `build_default_planned_runtime` scheduler loop consumes the exact same channel,
            //    ensuring the coordinator's notifier and the scheduler share a live queue.
            let scheduler_wake_wiring = ironclaw_runner::runtime::SchedulerWakeWiring::channel();
            let production_wiring = production_wiring(
                traffic_policy,
                production_trust_policy,
                runtime_policy,
                scheduler_wake_wiring.notifier(),
                runtime_process_binding,
            )?;
            let secret_master_key = resolve_secret_master_key(secret_master_key).await?;
            let context = RebornProductionBuildContext {
                profile,
                wiring_config,
                production_wiring,
                local_process_port: None,
                product_auth_ports,
                oauth_provider_configs,
                oauth_dcr_callback,
                owner_id,
                local_runtime_identity,
                turn_state_store_limits,
                memory_resolver: memory_service_resolver.clone(),
                scheduler_wake_wiring,
                account_setup_descriptors,
                nearai_mcp_bootstrap_config,
                native_extension_factories,
                channel_extension_bindings,
                first_party_bundles,
                first_party_registrars,
                credential_account_visibility_policy,
                workspace_filesystems: None,
                standalone_storage_root: None,
                default_system_prompt_path: None,
                #[cfg(any(test, feature = "test-support"))]
                network_http_egress_for_test: network_http_egress_for_test.clone(),
                #[cfg(any(test, feature = "test-support"))]
                trust_fixture_extensions_for_test,
            };
            build_postgres_production(
                context,
                pool,
                secret_master_key,
                process_local_resource_governor_singleton,
            )
            .await
        }
    }
}

async fn resolve_secret_master_key(
    explicit: Option<ironclaw_secrets::SecretMaterial>,
) -> Result<ironclaw_secrets::SecretMaterial, RebornBuildError> {
    resolve_explicit_or_keychain_master_key(explicit)
        .await?
        .ok_or(RebornBuildError::MissingSecretMasterKey)
}

/// Local-storage bring-up inputs for [`build_local_storage_production_shaped`],
/// bundled so the builder keeps a two-argument shape (`context` + these) rather
/// than a positional-argument sprawl.
struct LocalStorageProductionInput {
    root: PathBuf,
    workspace_root: Option<PathBuf>,
    host_home_root: Option<PathBuf>,
    storage_backend_input: DurableStorageInput,
    explicit_secret_master_key: Option<ironclaw_secrets::SecretMaterial>,
    runtime_policy_for_local_process: Option<EffectiveRuntimePolicy>,
    postgres_resource_governor_singleton: Option<bool>,
}

async fn build_local_storage_production_shaped(
    mut context: RebornProductionBuildContext,
    input: LocalStorageProductionInput,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    let LocalStorageProductionInput {
        root,
        workspace_root,
        host_home_root,
        storage_backend_input,
        explicit_secret_master_key,
        runtime_policy_for_local_process,
        postgres_resource_governor_singleton,
    } = input;
    let host_access = HostAccessAssemblyBuilder::new(
        root,
        workspace_root,
        host_home_root,
        runtime_policy_for_local_process,
    )
    .build()?;
    let root = &host_access.storage_root;
    let workspace_root = &host_access.workspace_root;
    let host_home_root = host_access.host_home_root.as_ref();
    let owner_user_id =
        UserId::new(context.owner_id.clone()).map_err(|error| RebornBuildError::InvalidConfig {
            reason: error.to_string(),
        })?;
    let bootstrap = HostBootstrapAssemblyBuilder::new(root, &owner_user_id)
        .build()
        .await?;

    let filesystem_bundle =
        FilesystemAssemblyBuilder::new(root, workspace_root, storage_backend_input)
            .with_host_home_root(host_home_root)
            .build()
            .await?;
    let trigger_repository =
        trigger_repository_for_durable_backend(&filesystem_bundle.durable_backend).await?;
    let refresh_lock_pool = match &filesystem_bundle.durable_backend {
        DurableBackend::LibSql(_) => None,
        DurableBackend::Postgres(pool) => Some(pool.clone()),
    };
    let event_store = match &filesystem_bundle.durable_backend {
        DurableBackend::LibSql(_) => ironclaw_reborn_event_store::RebornEventStoreConfig::Libsql {
            path_or_url: standalone_db_path(root).to_string_lossy().into_owned(),
            auth_token: None,
        },
        DurableBackend::Postgres(pool) => {
            ironclaw_reborn_event_store::RebornEventStoreConfig::PostgresPool { pool: pool.clone() }
        }
    };
    let filesystem = filesystem_bundle.filesystem;
    context.workspace_filesystems =
        Some(host_access.build_workspace_filesystems(Arc::clone(&filesystem))?);
    context.local_process_port = host_access.process_port;
    context.standalone_storage_root = Some(root.clone());
    context.default_system_prompt_path = Some(bootstrap.default_system_prompt_path);
    let scoped_filesystem = crate::wrap_scoped(Arc::clone(&filesystem));
    let (_secret_store, crypto) = build_secret_store(
        root,
        Arc::clone(&scoped_filesystem),
        explicit_secret_master_key,
    )
    .await?;
    let secret_credentials = SecretCredentialStores::new(scoped_filesystem, crypto);
    let resource_governor = filesystem_resource_governor(&filesystem);
    if let Some(singleton) = postgres_resource_governor_singleton {
        ensure_postgres_resource_governor_authority_for_build(singleton)?;
    }
    let stores = ProductionStoreBundle::with_secret_credentials(
        filesystem,
        resource_governor,
        secret_credentials,
        event_store,
    )
    .await?;
    build_backend_production(
        context,
        stores,
        trigger_repository,
        match refresh_lock_pool {
            Some(pool) => ironclaw_auth::CredentialRefreshLeaderLock::for_postgres(pool),
            None => ironclaw_auth::CredentialRefreshLeaderLock::always_leader_for_single_writer(),
        },
    )
    .await
}

struct RebornProductionWiring {
    trust_policy: Arc<HostTrustPolicy>,
    runtime_policy: EffectiveRuntimePolicy,
    turn_run_wake_notifier: Arc<dyn ironclaw_turns::TurnRunWakeNotifier>,
    runtime_process_binding: RebornRuntimeProcessBinding,
}

struct RebornProductionBuildContext {
    profile: RebornCompositionProfile,
    wiring_config: ironclaw_host_runtime::ProductionWiringConfig,
    production_wiring: RebornProductionWiring,
    local_process_port: Option<HostProcessPort>,
    product_auth_ports: Option<RebornProductAuthServicePorts>,
    oauth_provider_configs: Vec<crate::input::OAuthProviderBackendConfig>,
    oauth_dcr_callback: Option<crate::input::OAuthDcrCallbackConfig>,
    owner_id: String,
    local_runtime_identity: Option<RebornLocalRuntimeIdentity>,
    turn_state_store_limits: ironclaw_turns::TurnStateStoreLimits,
    /// Memory provider resolver (issue #3537), carried so the standalone profile
    /// source and the memory tools build providers through one resolver.
    memory_resolver: MemoryServiceResolver,
    /// The pre-minted scheduler wake wiring to carry to `RebornRuntimeStores` so
    /// `build_reborn_runtime` can hand it to `build_default_planned_runtime` via
    /// `DefaultPlannedRuntimeParts.scheduler_wake_wiring`.
    scheduler_wake_wiring: ironclaw_runner::runtime::SchedulerWakeWiring,
    account_setup_descriptors: Vec<ironclaw_product::ExtensionAccountSetupDescriptor>,
    nearai_mcp_bootstrap_config:
        Option<ironclaw_operator::llm_admin::nearai_mcp::NearAiMcpBootstrapConfig>,
    native_extension_factories: Vec<Arc<dyn ironclaw_extension_host::NativeExtensionFactory>>,
    channel_extension_bindings: Vec<crate::input::ChannelExtensionBinding>,
    /// Binary-injected neutral first-party bundle set (extension-runtime DEL-7):
    /// feeds the available-extension catalog, vendor auth recipes, and the
    /// reserved host-bundled id set.
    first_party_bundles: Vec<ironclaw_extension_host::FirstPartyPackageBundle>,
    /// Binary-injected first-party capability handler registrars (GSuite,
    /// web tooling).
    first_party_registrars: Vec<Arc<dyn ironclaw_extension_host::FirstPartyHandlerRegistrar>>,
    /// Injected credential-account visibility policy (see the build-input field).
    credential_account_visibility_policy:
        Option<Arc<dyn ironclaw_auth::RuntimeCredentialAccountVisibilityPolicy>>,
    workspace_filesystems: Option<WorkspaceFilesystems>,
    standalone_storage_root: Option<PathBuf>,
    default_system_prompt_path: Option<PathBuf>,
    /// Test-support host HTTP egress override (see `TestNetworkHttpEgress`).
    /// Carried from `RebornHostBindings::network_http_egress_for_test` so the
    /// unified production-shaped build honors an injected fake transport.
    #[cfg(any(test, feature = "test-support"))]
    network_http_egress_for_test: Option<Arc<dyn ironclaw_network::NetworkHttpEgress>>,
    /// Test-support only: allow trusted fixture packages copied into
    /// `/system/extensions` to validate as host-bundled.
    #[cfg(any(test, feature = "test-support"))]
    trust_fixture_extensions_for_test: bool,
}

fn production_wiring(
    traffic_policy: TrafficPolicy,
    trust_policy: Option<Arc<HostTrustPolicy>>,
    runtime_policy: Option<EffectiveRuntimePolicy>,
    turn_run_wake_notifier: Arc<ironclaw_runner::turn_scheduler::SchedulerTurnRunWakeNotifier>,
    runtime_process_binding: RebornRuntimeProcessBinding,
) -> Result<RebornProductionWiring, RebornBuildError> {
    let trust_policy = trust_policy.ok_or(RebornBuildError::MissingProductionTrustPolicy)?;
    if !trust_policy.has_sources() {
        return Err(RebornBuildError::EmptyProductionTrustPolicy);
    }
    let runtime_policy = runtime_policy.ok_or(RebornBuildError::MissingRuntimePolicy)?;
    if traffic_policy.requires_production_runtime_policy_preflight() {
        validate_production_runtime_policy(&runtime_policy)?;
    }
    validate_production_process_binding(&runtime_policy, &runtime_process_binding)?;
    let turn_run_wake_notifier: Arc<dyn ironclaw_turns::TurnRunWakeNotifier> =
        turn_run_wake_notifier;
    Ok(RebornProductionWiring {
        trust_policy,
        runtime_policy,
        turn_run_wake_notifier,
        runtime_process_binding,
    })
}

fn validate_production_runtime_policy(
    runtime_policy: &EffectiveRuntimePolicy,
) -> Result<(), RebornBuildError> {
    let mut issues = Vec::new();
    if let Some(reason) = local_only_runtime_policy_reason(runtime_policy) {
        issues.push(ironclaw_host_runtime::ProductionWiringIssue::new(
            ironclaw_host_runtime::ProductionWiringComponent::RuntimePolicy,
            ironclaw_host_runtime::ProductionWiringIssueKind::LocalOnlyImplementation,
            Some(reason),
        ));
    }
    if runtime_policy.process_backend == ProcessBackendKind::LocalHost {
        issues.push(ironclaw_host_runtime::ProductionWiringIssue::new(
            ironclaw_host_runtime::ProductionWiringComponent::RuntimeProcessPort,
            ironclaw_host_runtime::ProductionWiringIssueKind::LocalOnlyImplementation,
            Some("local_host_process"),
        ));
    }
    if issues.is_empty() {
        Ok(())
    } else {
        Err(RebornBuildError::ProductionWiring {
            report: ironclaw_host_runtime::ProductionWiringReport::new(issues),
        })
    }
}

fn local_only_runtime_policy_reason(policy: &EffectiveRuntimePolicy) -> Option<&'static str> {
    if matches!(policy.deployment, DeploymentMode::LocalSingleUser) {
        return Some("local_single_user_deployment");
    }
    if matches!(
        policy.filesystem_backend,
        FilesystemBackendKind::HostWorkspace | FilesystemBackendKind::HostWorkspaceAndHome
    ) {
        return Some("host_workspace_filesystem");
    }
    if matches!(policy.process_backend, ProcessBackendKind::LocalHost) {
        return Some("local_host_process");
    }
    if matches!(policy.network_mode, NetworkMode::Direct) {
        return Some("direct_network");
    }
    if matches!(
        policy.secret_mode,
        SecretMode::ScrubbedEnv | SecretMode::InheritedEnv
    ) {
        return Some("local_secret_environment");
    }
    None
}

fn validate_production_process_binding(
    runtime_policy: &EffectiveRuntimePolicy,
    binding: &RebornRuntimeProcessBinding,
) -> Result<(), RebornBuildError> {
    binding
        .validate_for_production_policy(runtime_policy)
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: error.to_string(),
        })
}

fn planned_run_profile_resolver() -> Result<Arc<InMemoryRunProfileResolver>, RebornBuildError> {
    Ok(Arc::new(
        ironclaw_runner::planned_driver_factory::default_planned_run_profile_resolver().map_err(
            |error| RebornBuildError::PlannedRunProfileResolver {
                reason: error.to_string(),
            },
        )?,
    ))
}

type FilesystemProductionHostRuntimeServices<F> = HostRuntimeServices<
    F,
    FilesystemResourceGovernor<F>,
    ironclaw_processes::ProcessStore<F>,
    ironclaw_processes::ProcessResultStore<F>,
>;

fn substrate_only_default_owner_id() -> Result<UserId, crate::RebornCompositionError> {
    let identity = RebornRuntimeIdentity::reborn_cli();
    // The substrate-only builders do not receive app/runtime owner input.
    // Preserve their legacy location under the default `reborn-cli` owner.
    UserId::new(identity.tenant_id).map_err(crate::RebornCompositionError::Mount)
}

pub(crate) async fn build_libsql_production_host_runtime_services<TPolicy, TWake>(
    config: crate::LibSqlProductionSubstrateConfig<TPolicy, TWake>,
) -> Result<crate::LibSqlProductionHostRuntimeServices, crate::RebornCompositionError>
where
    TPolicy: ironclaw_trust::TrustPolicy + 'static,
    TWake: ironclaw_turns::TurnRunWakeNotifier + 'static,
{
    ensure_libsql_resource_governor_authority(config.process_local_resource_governor_singleton)?;
    let filesystem = Arc::new(LibSqlRootFilesystem::new(Arc::clone(&config.database)));
    filesystem.run_migrations().await?;
    let scoped_filesystem = crate::wrap_scoped(Arc::clone(&filesystem));
    let resource_governor = FilesystemResourceGovernor::new(scoped_filesystem);
    build_filesystem_production_host_runtime_services(
        FilesystemProductionHostRuntimeServicesInput {
            filesystem,
            resource_governor,
            event_store: ProductionEventStoresInput::Config(config.event_store),
            secret_master_key: config.secret_master_key,
            trust_policy: config.trust_policy,
            runtime_policy: config.runtime_policy,
            turn_run_wake_notifier: config.turn_run_wake_notifier,
            surface_version: config.surface_version,
        },
    )
    .await
}

fn ensure_libsql_resource_governor_authority(
    process_local_singleton: bool,
) -> Result<(), crate::RebornCompositionError> {
    if process_local_singleton {
        return Ok(());
    }
    Err(crate::RebornCompositionError::InvalidConfig {
        reason: "libSQL production FilesystemResourceGovernor uses process-local tallies; configure a singleton or elected resource-governor owner before sharing one database across runtime processes".to_string(),
    })
}

fn ensure_libsql_resource_governor_authority_for_build(
    process_local_singleton: bool,
) -> Result<(), RebornBuildError> {
    if process_local_singleton {
        return Ok(());
    }
    Err(RebornBuildError::InvalidConfig {
        reason: "libSQL FilesystemResourceGovernor uses process-local tallies; configure a singleton or elected resource-governor owner before sharing one database across runtime processes".to_string(),
    })
}

pub(crate) async fn build_postgres_production_host_runtime_services<TPolicy, TWake>(
    config: crate::PostgresProductionSubstrateConfig<TPolicy, TWake>,
) -> Result<crate::PostgresProductionHostRuntimeServices, crate::RebornCompositionError>
where
    TPolicy: ironclaw_trust::TrustPolicy + 'static,
    TWake: ironclaw_turns::TurnRunWakeNotifier + 'static,
{
    let pool = config.pool;
    ensure_postgres_resource_governor_authority(config.process_local_resource_governor_singleton)?;
    let filesystem = Arc::new(ironclaw_filesystem::PostgresRootFilesystem::new(
        pool.clone(),
    ));
    ensure_postgres_event_store_config(&config.event_store)?;
    filesystem.run_migrations().await?;
    let resource_governor = filesystem_resource_governor(&filesystem);
    let event_store = ironclaw_reborn_event_store::build_reborn_event_stores_from_root_filesystem(
        Arc::clone(&filesystem),
    )?;
    build_filesystem_production_host_runtime_services(
        FilesystemProductionHostRuntimeServicesInput {
            filesystem,
            resource_governor,
            event_store: ProductionEventStoresInput::Prebuilt(event_store),
            secret_master_key: config.secret_master_key,
            trust_policy: config.trust_policy,
            runtime_policy: config.runtime_policy,
            turn_run_wake_notifier: config.turn_run_wake_notifier,
            surface_version: config.surface_version,
        },
    )
    .await
}

fn ensure_postgres_resource_governor_authority(
    process_local_singleton: bool,
) -> Result<(), crate::RebornCompositionError> {
    if process_local_singleton {
        return Ok(());
    }
    Err(crate::RebornCompositionError::InvalidConfig {
        reason: "Postgres production FilesystemResourceGovernor uses process-local tallies; configure a singleton or elected resource-governor owner before sharing one database across runtime processes".to_string(),
    })
}

fn ensure_postgres_resource_governor_authority_for_build(
    process_local_singleton: bool,
) -> Result<(), RebornBuildError> {
    if process_local_singleton {
        return Ok(());
    }
    Err(RebornBuildError::InvalidConfig {
        reason: "Postgres FilesystemResourceGovernor uses process-local tallies; configure a singleton or elected resource-governor owner before sharing one database across runtime processes".to_string(),
    })
}

struct FilesystemProductionHostRuntimeServicesInput<F, TPolicy, TWake>
where
    F: RootFilesystem + 'static,
{
    filesystem: Arc<F>,
    resource_governor: FilesystemResourceGovernor<F>,
    event_store: ProductionEventStoresInput,
    secret_master_key: Option<ironclaw_secrets::SecretMaterial>,
    trust_policy: Arc<TPolicy>,
    runtime_policy: crate::RebornProductionRuntimePolicy,
    turn_run_wake_notifier: Arc<TWake>,
    surface_version: CapabilitySurfaceVersion,
}

enum ProductionEventStoresInput {
    Config(ironclaw_reborn_event_store::RebornEventStoreConfig),
    Prebuilt(ironclaw_reborn_event_store::RebornEventStores),
}

fn ensure_postgres_event_store_config(
    config: &ironclaw_reborn_event_store::RebornEventStoreConfig,
) -> Result<(), crate::RebornCompositionError> {
    match config {
        ironclaw_reborn_event_store::RebornEventStoreConfig::Postgres { .. } => Ok(()),
        ironclaw_reborn_event_store::RebornEventStoreConfig::PostgresPool { .. } => Ok(()),
        _ => Err(crate::RebornCompositionError::InvalidConfig {
            reason: "PostgreSQL production substrate requires a PostgreSQL event store".to_string(),
        }),
    }
}

async fn warm_resource_governor_with_error<F, E, J>(
    resource_governor: FilesystemResourceGovernor<F>,
    map_join_error: J,
) -> Result<FilesystemResourceGovernor<F>, E>
where
    F: RootFilesystem + 'static,
    E: From<ironclaw_resources::ResourceError>,
    J: FnOnce(tokio::task::JoinError) -> E,
{
    let resource_governor = tokio::task::spawn_blocking(move || {
        resource_governor.warm_authority()?;
        Ok::<_, ironclaw_resources::ResourceError>(resource_governor)
    })
    .await
    .map_err(map_join_error)??;
    Ok(resource_governor)
}

async fn warm_resource_governor_for_composition<F>(
    resource_governor: FilesystemResourceGovernor<F>,
) -> Result<FilesystemResourceGovernor<F>, crate::RebornCompositionError>
where
    F: RootFilesystem + 'static,
{
    warm_resource_governor_with_error(resource_governor, |error| {
        crate::RebornCompositionError::InvalidConfig {
            reason: format!("resource governor warm-up task failed: {error}"),
        }
    })
    .await
}

async fn build_filesystem_production_host_runtime_services<F, TPolicy, TWake>(
    input: FilesystemProductionHostRuntimeServicesInput<F, TPolicy, TWake>,
) -> Result<FilesystemProductionHostRuntimeServices<F>, crate::RebornCompositionError>
where
    F: RootFilesystem + 'static,
    TPolicy: ironclaw_trust::TrustPolicy + 'static,
    TWake: ironclaw_turns::TurnRunWakeNotifier + 'static,
{
    let FilesystemProductionHostRuntimeServicesInput {
        filesystem,
        resource_governor,
        event_store,
        secret_master_key,
        trust_policy,
        runtime_policy,
        turn_run_wake_notifier,
        surface_version,
    } = input;
    let scoped_filesystem = crate::wrap_scoped(Arc::clone(&filesystem));
    let owner_user_id = substrate_only_default_owner_id()?;
    let owner_scope =
        default_runtime_owner_scope(owner_user_id).map_err(crate::RebornCompositionError::Mount)?;
    let turn_state_filesystem = owner_turn_state_filesystem(Arc::clone(&filesystem), &owner_scope)
        .map_err(crate::RebornCompositionError::Mount)?;
    let turn_state = Arc::new(production_turn_state_store(
        Arc::clone(&turn_state_filesystem),
        ironclaw_turns::TurnStateStoreLimits::default(),
    ));
    let process_services = ProcessServices::filesystem(Arc::clone(&scoped_filesystem));
    let secret_credentials = build_filesystem_secret_credential_stores(
        Arc::clone(&scoped_filesystem),
        secret_master_key,
    )
    .await?;
    let resource_governor = warm_resource_governor_for_composition(resource_governor).await?;
    let governor = Arc::new(resource_governor);
    let capability_leases = Arc::new(CapabilityLeaseStore::new(Arc::clone(&scoped_filesystem)));
    let persistent_approval_policies = Arc::new(PersistentApprovalPolicyStore::new(Arc::clone(
        &scoped_filesystem,
    )));
    let (runtime_policy, process_binding) = runtime_policy.into_parts();

    let services = with_shared_host_runtime_wiring!(
        HostRuntimeServices::new(
            Arc::new(ExtensionRegistry::new()),
            filesystem,
            governor,
            Arc::new(GrantAuthorizer::new()),
            process_services,
            surface_version,
        ),
        trust_policy = trust_policy,
        runtime_policy = runtime_policy,
        capability_leases = capability_leases,
        persistent_approval_policies = persistent_approval_policies,
        secret_store = Arc::clone(&secret_credentials.secret_store),
        credential_broker = secret_credentials.credential_broker,
        filesystem_run_state = Arc::clone(&scoped_filesystem),
        turn_state_and_transition_port = turn_state,
        run_profile_resolver = Arc::new(
            ironclaw_runner::planned_driver_factory::default_planned_run_profile_resolver()?,
        ),
    )
    .with_turn_run_wake_notifier(turn_run_wake_notifier);
    let services = match event_store {
        ProductionEventStoresInput::Config(config) => {
            services
                .with_reborn_event_store_config(
                    ironclaw_reborn_event_store::RebornProfile::Production,
                    config,
                )
                .await?
        }
        ProductionEventStoresInput::Prebuilt(stores) => {
            services.with_production_reborn_event_stores(stores)
        }
    };
    let services = apply_production_runtime_process_binding(services, process_binding);
    // Wire the operator post-edit check in production too (off unless
    // IRONCLAW_POST_EDIT_CHECK is set). It runs isolated in the tenant sandbox
    // per the runtime process binding applied above; the resolver routes it to
    // the tenant-sandbox process port rather than the provider host.
    let services = match PostEditCheckConfig::from_env() {
        Ok(Some(config)) => services.with_post_edit_check(config),
        Ok(None) => services,
        Err(error) => {
            return Err(crate::RebornCompositionError::InvalidConfig {
                reason: error.to_string(),
            });
        }
    };

    let services = services
        .try_with_host_http_egress_with_body_store(
            default_host_http_egress().map_err(|error| {
                crate::RebornCompositionError::InvalidConfig {
                    reason: error.to_string(),
                }
            })?,
            Arc::clone(&scoped_filesystem),
        )
        .map_err(crate::RebornCompositionError::from)?;

    Ok(services)
}

pub(crate) fn production_skill_management_mount_view(
    scope: &ResourceScope,
) -> Result<MountView, HostApiError> {
    MountView::new(vec![
        MountGrant::new(
            MountAlias::new("/skills")?,
            VirtualPath::new(format!(
                "/tenants/{}/users/{}/skills",
                scope.tenant_id.as_str(),
                scope.user_id.as_str()
            ))?,
            MountPermissions::read_write_list_delete(),
        ),
        MountGrant::new(
            MountAlias::new("/system/skills")?,
            VirtualPath::new("/system/skills")?,
            MountPermissions::read_only(),
        ),
    ])
}

pub(crate) fn production_system_extensions_lifecycle_mount_view() -> Result<MountView, HostApiError>
{
    MountView::new(vec![MountGrant::new(
        MountAlias::new("/system/extensions")?,
        VirtualPath::new("/system/extensions")?,
        MountPermissions::read_write_list_delete(),
    )])
}

async fn build_backend_production(
    context: RebornProductionBuildContext,
    stores: ProductionStoreBundle,
    trigger_repository: Arc<dyn TriggerRepository>,
    // Leader lock for the background credential keepalive worker. The worker
    // uses this to elect one process per tick as the sweep leader. `None`
    // pool → always-leader (libsql / single-process). Stays private.
    leader_lock: ironclaw_auth::CredentialRefreshLeaderLock,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    let RebornProductionBuildContext {
        profile,
        wiring_config,
        production_wiring,
        local_process_port,
        product_auth_ports,
        oauth_provider_configs,
        oauth_dcr_callback,
        owner_id,
        local_runtime_identity,
        turn_state_store_limits,
        memory_resolver,
        scheduler_wake_wiring,
        mut account_setup_descriptors,
        nearai_mcp_bootstrap_config,
        native_extension_factories,
        channel_extension_bindings,
        first_party_bundles,
        first_party_registrars,
        credential_account_visibility_policy,
        workspace_filesystems,
        standalone_storage_root,
        default_system_prompt_path,
        #[cfg(any(test, feature = "test-support"))]
        network_http_egress_for_test,
        #[cfg(any(test, feature = "test-support"))]
        trust_fixture_extensions_for_test,
    } = context;
    // Select the non-validating local-testing host runtime for a standalone
    // deployment. The pre-`975bcd2ce` dedicated standalone builder always used
    // `host_runtime_for_local_testing()`; the unified path keyed only on a wired
    // local host process port (`local_process_port.is_some()`), which is `None`
    // whenever the standalone deployment uses a non-`LocalHost` process backend
    // (e.g. an injected `TenantSandbox` port — the multi-user-safe default). That
    // wrongly routed such standalone builds through `host_runtime_for_production`,
    // whose `validate_production_wiring` rejects the `LocalSingleUser` deployment
    // mode. Key the choice on the deployment mode too: a `LocalSingleUser` policy
    // is exactly the shape production validation would reject, so it must use the
    // local-testing runtime regardless of process backend. (Production
    // deployments never resolve to `LocalSingleUser` — see
    // `.claude/rules/safety-and-sandbox.md`.)
    let deployment_is_local_single_user = matches!(
        production_wiring.runtime_policy.deployment,
        DeploymentMode::LocalSingleUser
    );
    let uses_local_host_runtime = local_process_port.is_some() || deployment_is_local_single_user;
    // The reserved host-bundled id set consulted during filesystem catalog
    // load and by the upload-import path, sourced from the injected bundles.
    let first_party_reserved_ids = first_party_reserved_extension_ids(&first_party_bundles);
    // Computed before `oauth_provider_configs` is consumed by
    // `compose_provider_client` below — see `google_oauth_configured`.
    let google_oauth_configured = google_oauth_configured(&oauth_provider_configs);
    let google_provider = VendorId::new(ironclaw_auth::GOOGLE_PROVIDER_ID).map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("provider instance readiness map could not be built: {error}"),
        }
    })?;
    let provider_instance_readiness =
        provider_instance_readiness_map([ProviderInstanceReadinessInput {
            provider: google_provider,
            configured: google_oauth_configured,
            remediation: "configure Google OAuth credentials".to_string(),
        }]);
    let owner_user_id = UserId::new(owner_id).map_err(|error| RebornBuildError::InvalidConfig {
        reason: error.to_string(),
    })?;
    let turn_state_scope = match local_runtime_identity.as_ref() {
        Some(identity) => configured_runtime_owner_scope(owner_user_id.clone(), identity),
        None => {
            default_runtime_owner_scope(owner_user_id.clone()).map_err(RebornBuildError::Mount)?
        }
    };
    let turn_state_filesystem =
        owner_turn_state_filesystem(Arc::clone(&stores.filesystem), &turn_state_scope)
            .map_err(RebornBuildError::Mount)?;
    let secret_store: Arc<dyn SecretStorePort> = stores.secret_credentials.secret_store.clone();
    let skill_management_filesystem: Arc<dyn RootFilesystem> = stores.filesystem.clone();
    let skill_management = Arc::new(ScopedSkillManagementPort::new_with_mount_resolver(
        owner_user_id.clone(),
        skill_management_filesystem,
        Arc::new(production_skill_management_mount_view),
    ));
    let extension_lifecycle_surface_context = extension_lifecycle_surface_context(
        owner_user_id.clone(),
        local_runtime_identity.as_ref(),
    )?;
    let channel_egress_scope = turn_state_scope.clone();
    let (skill_filesystem, workspace_filesystem, runtime_workspace_mounts) =
        match workspace_filesystems {
            Some(filesystems) => filesystems,
            None => {
                let read_only_workspace_mounts =
                    workspace_mount_view(MountPermissions::read_only(), &[]).map_err(|error| {
                        RebornBuildError::InvalidConfig {
                            reason: error.to_string(),
                        }
                    })?;
                let runtime_workspace_mounts =
                    ambient_workspace_mount_view(MountPermissions::read_write(), &[], &[])
                        .map_err(|error| RebornBuildError::InvalidConfig {
                            reason: error.to_string(),
                        })?;
                (
                    Arc::new(ScopedFilesystem::new(
                        Arc::clone(&stores.filesystem),
                        scoped_skill_context_mount_view,
                    )),
                    Arc::new(ScopedFilesystem::with_fixed_view(
                        Arc::clone(&stores.filesystem),
                        read_only_workspace_mounts,
                    )),
                    runtime_workspace_mounts,
                )
            }
        };
    let skill_mounts =
        skill_management_mount_view().map_err(|error| RebornBuildError::InvalidConfig {
            reason: error.to_string(),
        })?;
    let memory_mounts =
        memory_mount_view(MountPermissions::read_write_list_delete()).map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: error.to_string(),
            }
        })?;
    let system_extensions_lifecycle_mounts = production_system_extensions_lifecycle_mount_view()
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: error.to_string(),
        })?;
    let approval_requests = Arc::new(ApprovalRequestStore::new(Arc::clone(
        &stores.scoped_filesystem,
    )));
    let capability_policy =
        Arc::new(
            builtin_capability_policy().map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("capability policy is invalid: {error}"),
            })?,
        );
    let tool_permission_overrides = Arc::new(ComposedToolPermissionOverrideStore::new(Arc::clone(
        &stores.scoped_filesystem,
    )));
    let auto_approve_settings = Arc::new(ComposedAutoApproveSettingStore::new(Arc::clone(
        &stores.scoped_filesystem,
    )));
    let persistent_approval_policies_for_settings: Arc<
        dyn ironclaw_approvals::PersistentApprovalPolicyStorePort,
    > = Arc::clone(&stores.persistent_approval_policies)
        as Arc<dyn ironclaw_approvals::PersistentApprovalPolicyStorePort>;
    let approval_settings_provider = Arc::new(StoreApprovalSettingsProvider::new(
        Arc::clone(&tool_permission_overrides)
            as Arc<dyn ironclaw_approvals::ToolPermissionOverrideStorePort>,
        Arc::clone(&auto_approve_settings)
            as Arc<dyn ironclaw_approvals::AutoApproveSettingStorePort>,
        persistent_approval_policies_for_settings,
    ));
    let runtime_policy = production_wiring.runtime_policy.clone();
    let runtime_policy_for_return = Some(runtime_policy.clone());
    let authorizer = capability_authorizer(
        Some(&runtime_policy),
        Arc::clone(&capability_policy),
        approval_settings_provider,
    );
    let outbound_stores = OutboundStoreAssemblyBuilder::new(Arc::clone(&stores.filesystem)).build();
    let outbound_delivery_targets = host_owned_outbound_delivery_target_registry()?;
    let skill_auto_activate_learned = Arc::new(AtomicBool::new(true));
    let process_backend = production_wiring.runtime_policy.process_backend;
    let extension_registry = production_builtin_extension_registry(process_backend)?;
    let extension_registry = Arc::new(extension_registry);
    let BudgetSinks {
        budget_event_sink,
        #[cfg(any(test, feature = "test-support"))]
        in_memory_budget_event_sink,
        broadcast_budget_event_sink,
        ..
    } = build_budget_sinks();
    let turn_state = Arc::new(production_turn_state_store(
        Arc::clone(&turn_state_filesystem),
        turn_state_store_limits,
    ));
    // Rebindable source-turn-state slot for the trigger delivery-target
    // service — same repoint seam as the sibling snapshot slot below.
    #[cfg(any(test, feature = "test-support"))]
    let trigger_source_turn_state_store: Arc<
        std::sync::RwLock<Arc<dyn ironclaw_turns::TurnStateStore>>,
    > = Arc::new(std::sync::RwLock::new(
        Arc::clone(&turn_state) as Arc<dyn ironclaw_turns::TurnStateStore>
    ));
    #[cfg(any(test, feature = "test-support"))]
    let trigger_create_source_turn_state: Arc<dyn ironclaw_turns::TurnStateStore> =
        Arc::new(LateBoundTriggerSourceTurnStateStore {
            source_turn_state: Arc::clone(&trigger_source_turn_state_store),
        });
    #[cfg(not(any(test, feature = "test-support")))]
    let trigger_create_source_turn_state: Arc<dyn ironclaw_turns::TurnStateStore> =
        Arc::clone(&turn_state) as Arc<dyn ironclaw_turns::TurnStateStore>;
    let trigger_create_hook = Arc::new(LocalRuntimeTriggerCreatorPairingHook {
        outbound_delivery_targets: Arc::clone(&outbound_delivery_targets),
        source_turn_state: trigger_create_source_turn_state,
        scoped_filesystem: Arc::clone(&stores.scoped_filesystem),
        conversations: tokio::sync::OnceCell::new(),
    });
    let checkpoint_state_store: Arc<dyn CheckpointStateStorePort> = Arc::new(
        CheckpointStateStore::new(Arc::clone(&stores.scoped_filesystem)),
    );
    let thread_service: Arc<dyn SessionThreadService> = Arc::new(
        FilesystemSessionThreadService::new(Arc::clone(&stores.scoped_filesystem)),
    );
    let resource_governor = Arc::new(
        stores
            .resource_governor
            .with_event_sink(Arc::clone(&budget_event_sink)),
    );
    let production_resource_governor: Arc<dyn ResourceGovernor> = resource_governor.clone();
    let budget_gate_store: Arc<dyn BudgetGateStorePort> =
        Arc::new(BudgetGateStore::new(Arc::clone(&stores.scoped_filesystem)));
    let event_stores = ironclaw_reborn_event_store::build_reborn_event_stores(
        profile.to_event_store_profile(),
        stores.event_store,
    )
    .await?;
    let event_log = Arc::clone(&event_stores.events);
    let audit_log = Arc::clone(&event_stores.audit);
    // Admin per-user secret provisioner over the raw production root and the
    // SAME crypto the runtime's own secret store uses, so material written for
    // a target user decrypts under that user's own store (mirrors the local
    // substrate's `admin_secret_provisioner`; see `admin_secrets.rs`).
    let admin_secret_provisioner: Arc<dyn crate::admin_secrets::AdminSecretProvisioner> =
        Arc::new(crate::admin_secrets::FilesystemAdminSecretProvisioner::new(
            Arc::clone(&stores.filesystem),
            Arc::clone(&stores.secret_credentials.crypto),
        ));
    // Projects persist over the production scoped filesystem (tenant supplied
    // per call; the scope carries only the control-plane owner/agent identity),
    // exactly as the local substrate builds them — see the local runtime stores'
    // project repository. Production is always durable, so there is no
    // in-memory fallback arm here.
    let project_agent_id = ironclaw_host_api::AgentId::new("reborn-projects").map_err(|error| {
        RebornBuildError::InvalidConfig {
            reason: format!("invalid project agent id: {error}"),
        }
    })?;
    let project_repository: Arc<dyn ProjectRepository> =
        Arc::new(ironclaw_projects::FilesystemProjectRepository::new(
            Arc::clone(&stores.scoped_filesystem),
            owner_user_id.clone(),
            project_agent_id,
        ));
    let project_service: Arc<dyn ProjectService> =
        Arc::new(RebornProjectService::new(project_repository));
    // Trigger conversation services over the production scoped filesystem —
    // the substrate-agnostic trigger poller (`runtime.rs`) sources the
    // materializer/submitter/pairing roles from here for production profiles,
    // exactly as the local substrate serves them from its own conversation
    // services. Built eagerly (production is always durable); the underlying
    // `InboundTurnError` cause is preserved in the mapped build error.
    let trigger_conversation_services =
        RebornFilesystemConversationServices::new(Arc::clone(&stores.scoped_filesystem))
            .await
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("trigger conversation services unavailable: {error}"),
            })?;
    // Same store-backed lookup the WebUI automations panel builds from the
    // runtime's turn-state snapshot source (#5886). Read through a rebindable
    // source so a test-support harness can repoint the trigger subsystem at its
    // own turn store; production installs this runtime's own store and never
    // repoints it.
    let trigger_source_turn_state: Arc<
        std::sync::RwLock<Arc<dyn crate::turn_run_snapshot::TurnRunSnapshotSource>>,
    > = Arc::new(std::sync::RwLock::new(
        Arc::clone(&turn_state) as Arc<dyn crate::turn_run_snapshot::TurnRunSnapshotSource>
    ));
    let trigger_active_run_lookup: Arc<dyn TriggerActiveRunLookup> = Arc::new(
        crate::automation::trigger_poller::SnapshotActiveRunLookup::new(Arc::new(
            crate::turn_run_snapshot::RebindableTurnRunSnapshotSource::new(Arc::clone(
                &trigger_source_turn_state,
            )),
        )
            as Arc<dyn crate::turn_run_snapshot::TurnRunSnapshotSource>),
    );
    let mut first_party_registry = production_first_party_registry_with_trigger_create_hook(
        Arc::clone(&trigger_repository),
        trigger_create_hook,
        trigger_active_run_lookup,
        process_backend,
        memory_resolver.clone(),
    )?;
    let product_auth_filesystem = Arc::clone(&stores.scoped_filesystem);
    let services = with_shared_host_runtime_wiring!(
        HostRuntimeServices::new(
            Arc::clone(&extension_registry),
            Arc::clone(&stores.filesystem),
            Arc::new(InMemoryResourceGovernor::new()),
            authorizer,
            ProcessServices::filesystem(Arc::clone(&stores.scoped_filesystem)),
            CapabilitySurfaceVersion::new("reborn-app-v1")?,
        ),
        trust_policy = Arc::clone(&production_wiring.trust_policy),
        runtime_policy = runtime_policy,
        capability_leases = Arc::clone(&stores.leases),
        persistent_approval_policies = Arc::clone(&stores.persistent_approval_policies),
        secret_store = Arc::clone(&stores.secret_credentials.secret_store),
        credential_broker = stores.secret_credentials.credential_broker,
        filesystem_run_state = Arc::clone(&stores.scoped_filesystem),
        turn_state_and_transition_port = Arc::clone(&turn_state),
        run_profile_resolver = planned_run_profile_resolver()?,
    )
    .with_approval_requests(Arc::clone(&approval_requests))
    .with_resource_governor(Arc::clone(&resource_governor))
    .with_production_reborn_event_stores(event_stores)
    .with_turn_run_wake_notifier_dyn(production_wiring.turn_run_wake_notifier);
    // Honor an injected test egress (hosted-MCP discovery / DM provisioning over
    // a fake transport) when present; otherwise the real policy egress. Restores
    // the consumer dropped in commit 975bcd2ce — without it every standalone test
    // reaches the real network. `TestNetworkHttpEgress` adapts the injected
    // `Arc<dyn NetworkHttpEgress>` to the generic method bound.
    #[cfg(any(test, feature = "test-support"))]
    let services = match network_http_egress_for_test {
        Some(test_egress) => {
            services.try_with_host_http_egress(TestNetworkHttpEgress(test_egress))?
        }
        None => services.try_with_host_http_egress(default_host_http_egress()?)?,
    };
    #[cfg(not(any(test, feature = "test-support")))]
    let services = services.try_with_host_http_egress(default_host_http_egress()?)?;
    let product_auth_runtime_ports = require_product_auth_runtime_ports(&services)?;
    let services = attach_hosted_mcp_runtime(services)?;
    let admin_configuration_credential_slot = AdminConfigurationCredentialSlot::default();
    let provider_composition = compose_provider_client(
        oauth_provider_configs,
        oauth_dcr_callback,
        Arc::clone(&secret_store),
        product_auth_runtime_ports.clone(),
        admin_configuration_credential_slot.clone(),
        &first_party_bundles,
    )?;
    let services = if let Some(process_port) = local_process_port {
        services.with_runtime_process_port(Arc::new(process_port))
    } else {
        services
    };
    let services = apply_production_runtime_process_binding(
        services,
        production_wiring.runtime_process_binding,
    );
    // Wire the operator post-edit check in production too (off unless
    // IRONCLAW_POST_EDIT_CHECK is set); it runs isolated in the tenant sandbox
    // per the process binding applied above.
    let services = apply_post_edit_check_from_env(services)?;
    let security_audit_sink = services.security_audit_sink();

    let turn_coordinator: Arc<dyn ironclaw_turns::TurnCoordinator> =
        Arc::new(services.turn_coordinator_for_production()?);
    // B1: track the durable FilesystemAuthProductServices so the engine
    // keepalive sweep can enumerate candidates across all owners. When a
    // caller pre-supplies product_auth_ports, we do not create a durable
    // instance here, so the candidate source is None (sweep finds no
    // candidates, which is safe for override/test callers).
    let credential_refresh_candidate_source: Option<
        Arc<dyn ironclaw_auth::KeepaliveCandidateSource>,
    >;
    // The durable auth-flow record projection this builder wires for its own
    // durable service (`None` arm). Left `None` for a caller-supplied bundle so
    // that path's WebUI auth interaction surface stays explicitly unavailable
    // (restores wiring dropped in commit 975bcd2ce).
    let product_auth_flow_record_source: Option<Arc<dyn ironclaw_auth::AuthFlowRecordSource>>;
    let product_auth_ports = match product_auth_ports {
        Some(ports) => {
            credential_refresh_candidate_source = None;
            product_auth_flow_record_source = None;
            ports
        }
        None => {
            let durable = Arc::new(FilesystemAuthProductServices::new_with_root(
                product_auth_filesystem,
                Arc::clone(&stores.filesystem),
                Arc::clone(&secret_store),
            ));
            credential_refresh_candidate_source =
                Some(Arc::clone(&durable) as Arc<dyn ironclaw_auth::KeepaliveCandidateSource>);
            product_auth_flow_record_source =
                Some(Arc::clone(&durable) as Arc<dyn ironclaw_auth::AuthFlowRecordSource>);
            RebornProductAuthServicePorts::from_shared_with_provider(
                durable,
                provider_composition
                    .client
                    .clone()
                    .unwrap_or_else(|| Arc::new(UnavailableAuthProviderClient)),
            )
        }
    };
    // The sweep resolves per-vendor idle lifetimes through the same recipe
    // data the auth engine executes; capture it before `provider_composition`
    // moves into `compose_product_auth_services`.
    let keepalive_recipes = provider_composition
        .engine
        .as_ref()
        .map(|engine| Arc::clone(engine.recipes()));
    // Two-phase product auth: the CORE is composed here so its
    // dispatcher-independent services (credential selection/refresh, cleanup)
    // can feed extension management, and the final services Arc is minted
    // below with `lifecycle_auth_continuation_dispatcher` wrapped around the
    // base dispatcher — extension-card OAuth (LifecycleActivation
    // continuations) reconciles readiness before the fan-out.
    let (product_auth_core, base_auth_continuation) =
        compose_product_auth_services(ProductAuthServicesCompositionInput {
            ports: product_auth_ports,
            turn_coordinator: turn_coordinator.clone(),
            // Blocked-auth fan-out over this builder's own durable turn-state
            // store: a completed connect resumes every run the same owner has
            // parked on the same provider, matching the standalone builder. The
            // blanket `TurnRunSnapshotSource` impl covers the generic
            // filesystem store directly.
            blocked_auth_snapshot_source: Some(Arc::clone(&turn_state)
                as Arc<dyn crate::blocked_auth_resume::BlockedAuthSnapshotSource>),
            provider_composition,
            security_audit_sink,
            secret_store: Arc::clone(&secret_store),
            nearai_mcp_host_managed_scope: Some(AuthProductScope::new(
                channel_egress_scope.clone(),
                AuthSurface::Api,
            )),
            credential_account_visibility_policy,
            flow_record_source: product_auth_flow_record_source,
        })?;
    // Dispatcher-independent view sharing every inner service (including the
    // continuation-dispatch inflight set) with the final wrapped Arc below.
    let product_auth_dependencies = Arc::new(product_auth_core.clone());
    let product_auth_ready = true;
    // Wire ProductAuthAccount runtime credential resolver before
    // host_runtime_for_production so WASM extensions whose manifest declares a
    // ProductAuthAccount runtime credential source resolve through
    // CredentialAccountService. Unconditional in production: product_auth_services
    // always exists (durable filesystem fallback from #4234).
    let mut services = services.with_runtime_credential_account_resolver(Arc::new(
        ProductAuthRuntimeCredentialResolver::new_with_refresh(
            product_auth_dependencies.runtime_credential_account_selection_service(),
            product_auth_dependencies.runtime_credential_account_refresh_service(),
        ),
    ));
    services = attach_wasm_runtime(services)?;
    // Install every binary-assembled first-party capability handler (GSuite,
    // web tooling) through the generic registrar seam (extension-runtime DEL-7).
    // Composition owns the loop and the shared context; the concrete executors
    // live in the assembling binary.
    let first_party_registrar_context = FirstPartyRegistrarContext {
        credential_account_service: product_auth_dependencies.credential_account_service(),
        credential_account_record_source: product_auth_dependencies
            .credential_account_record_source(),
        product_auth_runtime_ports: product_auth_runtime_ports.clone(),
        oauth_backend_configured: google_oauth_configured,
    };
    for registrar in &first_party_registrars {
        registrar
            .register(&mut first_party_registry, &first_party_registrar_context)
            .map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!("first-party capability handlers are invalid: {error}"),
            })?;
    }
    let extensions_root = VirtualPath::new("/system/extensions")?;
    #[cfg(any(test, feature = "test-support"))]
    let filesystem_catalog = if trust_fixture_extensions_for_test {
        AvailableExtensionCatalog::from_trusted_fixture_filesystem_root(
            stores.filesystem.as_ref(),
            &extensions_root,
            &first_party_reserved_ids,
        )
        .await
    } else {
        AvailableExtensionCatalog::from_filesystem_root(
            stores.filesystem.as_ref(),
            &extensions_root,
            &first_party_reserved_ids,
        )
        .await
    };
    #[cfg(not(any(test, feature = "test-support")))]
    let filesystem_catalog = AvailableExtensionCatalog::from_filesystem_root(
        stores.filesystem.as_ref(),
        &extensions_root,
        &first_party_reserved_ids,
    )
    .await;
    let mut available_extensions =
        filesystem_catalog.map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("available extension catalog could not be loaded: {error}"),
        })?;
    let nearai_mcp_catalog_config = nearai_mcp_bootstrap_config
        .clone()
        .map(|config| {
            let endpoint = config
                .endpoint()
                .map_err(|error| format!("NEAR AI MCP catalog endpoint is invalid: {error}"))?;
            ironclaw_extension_host::NearAiMcpBootstrapConfig::new(
                endpoint.url,
                config.into_api_key(),
            )
            .map_err(|error| error.to_string())
        })
        .transpose()
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("nearai MCP catalog config is invalid: {error}"),
        })?;
    available_extensions.extend(
        AvailableExtensionCatalog::from_first_party_assets_with_nearai_mcp_config(
            nearai_mcp_catalog_config.as_ref(),
            &first_party_bundles,
        )
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("first-party extension catalog could not be loaded: {error}"),
        })?,
    );
    // Carry the reserved first-party id set onto the composed catalog so the
    // upload-import path can reject reserved ids without re-deriving the
    // inventory.
    available_extensions =
        available_extensions.with_reserved_bundled_ids(first_party_reserved_ids.clone());
    // Manifest-derived account-setup declarations (#6520): every catalog
    // package's `[account_setup]` projection joins the binary-injected extras
    // from the deployment seam. Duplicates fail loudly at `declare()` below.
    let admin_configuration_uses = available_extensions.admin_configuration_uses();
    let mut admin_configuration_consumers = std::collections::BTreeMap::new();
    for usage in &admin_configuration_uses {
        let extension_id =
            ironclaw_host_api::ExtensionId::new(usage.package_id.clone()).map_err(|error| {
                RebornBuildError::InvalidConfig {
                    reason: format!(
                        "administrator configuration consumer `{}` has an invalid extension id: {error}",
                        usage.package_id
                    ),
                }
            })?;
        admin_configuration_consumers
            .entry(usage.descriptor.group_id.clone())
            .or_insert_with(std::collections::BTreeSet::new)
            .insert(extension_id);
    }
    let available_manifests = available_extensions.resolved_manifests();
    account_setup_descriptors.extend(manifest_channel_account_setup_descriptors(
        &available_manifests,
    ));
    let deployment_bindings = available_manifests
        .iter()
        .filter(|manifest| {
            manifest
                .channel
                .as_ref()
                .is_some_and(|channel| channel.inbound && channel.ingress.is_some())
        })
        .filter_map(|manifest| {
            channel_extension_bindings
                .iter()
                .find(|binding| binding.extension_id == manifest.id.as_str())
                .map(|binding| {
                    ironclaw_extension_host::DeploymentChannelBinding::new(
                        Arc::clone(manifest),
                        Arc::clone(&binding.adapter),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("deployment channel registry could not be built: {error}"),
        })?;
    let deployment_channels = Arc::new(
        ironclaw_extension_host::DeploymentChannelRegistry::try_new(deployment_bindings).map_err(
            |error| RebornBuildError::InvalidConfig {
                reason: format!("deployment channel registry could not be built: {error}"),
            },
        )?,
    );
    let admin_configuration_filesystem: Arc<dyn RootFilesystem> = stores.filesystem.clone();
    let admin_configuration = Arc::new(
        AdminConfigurationService::new(
            FilesystemAdminConfigurationStore::new(Arc::new(ScopedFilesystem::new(
                admin_configuration_filesystem,
                crate::invocation_mount_view,
            ))),
            Arc::clone(&secret_store),
            admin_configuration_uses
                .iter()
                .map(|usage| usage.descriptor.clone()),
        )
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("admin configuration service could not be built: {error}"),
        })?,
    );
    let extension_filesystem: Arc<dyn RootFilesystem> = stores.filesystem.clone();
    let extension_host_ports =
        ironclaw_host_runtime::default_host_port_catalog().map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: format!("extension host port catalog could not be loaded: {error}"),
            }
        })?;
    let extension_host_api_contracts =
        product_extension_host_api_contract_registry().map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: format!("extension host API contracts could not be loaded: {error}"),
            }
        })?;
    let extension_installation_state_path = ExtensionInstallationStore::default_state_path()
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("extension installation state path is invalid: {error}"),
        })?;
    let extension_installation_store: Arc<dyn ExtensionInstallationStorePort> = Arc::new(
        ExtensionInstallationStore::load_at(
            extension_filesystem.clone(),
            extension_installation_state_path,
            extension_host_ports,
            extension_host_api_contracts,
        )
        .await
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("extension installation state could not be loaded: {error}"),
        })?,
    );
    let extension_lifecycle_service = Arc::new(tokio::sync::Mutex::new(
        ExtensionLifecycleService::new(services.shared_extension_registry().snapshot_owned()),
    ));
    let active_extensions = ActiveExtensionPublisher::new(
        services.shared_extension_registry(),
        Arc::clone(&production_wiring.trust_policy),
        Arc::new(ironclaw_trust::InvalidationBus::new()),
    );
    restore_extension_lifecycle_state(
        &available_extensions,
        &extension_filesystem,
        &extension_installation_store,
        &extension_lifecycle_service,
        &active_extensions,
        &owner_user_id,
    )
    .await
    .map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("extension lifecycle state could not be restored: {error}"),
    })?;
    let removal_cleanup_adapters: Vec<Arc<dyn ExtensionRemovalCleanupAdapter>> = Vec::new();
    let removal_cleanup = Arc::new(
        ExtensionRemovalCleanupRegistry::try_from_adapters(removal_cleanup_adapters).map_err(
            |error| RebornBuildError::InvalidConfig {
                reason: format!("extension removal cleanup registry could not be built: {error}"),
            },
        )?,
    );
    let account_setups = ExtensionAccountSetupRegistry::default();
    let channel_disconnect_slot: Arc<
        std::sync::OnceLock<Arc<dyn ironclaw_product::ChannelConnectionService>>,
    > = Arc::new(std::sync::OnceLock::new());
    let extension_management = Arc::new(
        RebornLocalExtensionManagementPort::new(
            extension_filesystem,
            available_extensions,
            extension_installation_store,
            extension_lifecycle_service,
            active_extensions,
            Some(Arc::new(RebornProductAuthCredentialCleanup::new(Arc::clone(
                &product_auth_dependencies,
            ))) as Arc<dyn ExtensionCredentialCleanup>),
            channel_egress_scope.user_id.clone(),
        )
        .with_account_setup_registry(account_setups.clone())
        .with_removal_cleanup_registry(removal_cleanup)
        .with_provider_instance_readiness(provider_instance_readiness)
        .with_channel_disconnect_slot(Arc::clone(&channel_disconnect_slot)),
    );
    let nearai_mcp_bootstrap_outcome = crate::llm_admin::nearai_mcp::bootstrap_nearai_mcp(
        nearai_mcp_bootstrap_config,
        &product_auth_dependencies,
        &extension_management,
        channel_egress_scope.clone(),
    )
    .await?;
    nearai_mcp_bootstrap_outcome.log_completion();
    // Read-side service for manifest-declared administrator configuration.
    // Production reads and writes both use the canonical Admin Configuration
    // service; installation membership carries no deployment-owned values.
    let admin_configuration_resolver = Arc::new(
        ChannelConfigService::new(
            extension_management.installation_store_handle(),
            Arc::clone(&secret_store),
            channel_egress_scope.clone(),
            Arc::clone(&extension_management)
                as Arc<dyn ironclaw_extension_host::ChannelConfigReactivation>,
        )
        .with_admin_configuration(
            Arc::clone(&admin_configuration),
            channel_egress_scope.clone(),
        )
        .with_available_manifests(available_manifests.clone()),
    );
    extension_management.attach_channel_config(&admin_configuration_resolver);
    admin_configuration_credential_slot.fill(Arc::clone(&admin_configuration_resolver));
    // Mint the FINAL product-auth services with the lifecycle-activation
    // continuation composed over the base dispatcher: extension-card OAuth
    // completions re-enter the canonical lifecycle command (readiness
    // reconciliation) before the provider-blocked-run fan-out, instead of
    // being durably fenced un-activated.
    let lifecycle_continuation_facade: Arc<dyn ironclaw_product::LifecycleProductService> =
        Arc::new(
            ironclaw_extension_host::ExtensionHostLifecycleProductService::new(Arc::clone(
                &skill_management,
            ))
            .with_extension_management(Arc::clone(&extension_management))
            .with_channel_config(Arc::clone(&admin_configuration_resolver))
            .with_runtime_http_egress(product_auth_runtime_ports.runtime_http_egress())
            .with_runtime_credential_accounts(
                product_auth_dependencies.runtime_credential_account_selection_service(),
            ),
        );
    let base_product_continuation: Arc<dyn ironclaw_product::ProductAuthContinuationDispatcher> =
        product_auth_continuation_dispatcher(base_auth_continuation);
    let lifecycle_wrapped_product_continuation =
        ironclaw_product::lifecycle_auth_continuation_dispatcher(
            lifecycle_continuation_facade,
            base_product_continuation,
        );
    let lifecycle_wrapped_auth_continuation: Arc<dyn RebornAuthContinuationDispatcher> = Arc::new(
        AuthContinuationFromProduct::new(Arc::clone(&lifecycle_wrapped_product_continuation)),
    );
    let product_auth_services = Arc::new(
        product_auth_core
            .with_continuation_dispatcher(Arc::clone(&lifecycle_wrapped_auth_continuation)),
    );
    // Bundle the keepalive sweep deps so they are wired all-or-nothing. The
    // candidate source is present only when this path built a durable instance
    // (no caller-supplied product_auth_ports); recipes are present only when
    // the auth engine was composed; the leader lock and refresh port are
    // always available here. The refresh port holds the WRAPPED services so a
    // refresh-driven flow reconcile runs the same lifecycle continuation.
    let credential_refresh_worker = match (credential_refresh_candidate_source, keepalive_recipes) {
        (Some(candidate_source), Some(recipes)) => CredentialRefreshWorkerReady::Ready {
            candidate_source,
            recipes,
            leader_lock,
            refresh_port: Arc::clone(&product_auth_services),
        },
        _ => CredentialRefreshWorkerReady::Absent,
    };
    let fold_filesystem: Arc<dyn RootFilesystem> = stores.filesystem.clone();
    let channel_identity_store = Arc::new(
        ironclaw_extension_host::FilesystemChannelIdentityStore::new(
            Arc::clone(&fold_filesystem),
            channel_egress_scope.tenant_id.clone(),
            channel_egress_scope.user_id.clone(),
        ),
    );
    let channel_dm_target_store = Arc::new(
        ironclaw_extension_host::FilesystemChannelDmTargetStore::new(
            Arc::clone(&fold_filesystem),
            channel_egress_scope.tenant_id.clone(),
            channel_egress_scope.user_id.clone(),
        ),
    );
    let runtime_http_egress = Some(product_auth_runtime_ports.runtime_http_egress());
    let host_runtime_http_egress = services.host_runtime_http_egress_port();
    // The first-party capability handlers were installed above through the
    // binary-supplied `first_party_registrars` loop (extension-runtime DEL-7);
    // composition names no concrete first-party executor here.
    insert_extension_lifecycle_handlers(
        &mut first_party_registry,
        Arc::clone(&extension_management),
        product_auth_services.runtime_credential_account_selection_service(),
        runtime_http_egress.clone(),
    )
    .map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("extension lifecycle handlers are invalid: {error}"),
    })?;
    insert_admin_configuration_handler(
        &mut first_party_registry,
        Arc::clone(&admin_configuration),
        channel_egress_scope.user_id.clone(),
        Arc::clone(&extension_management)
            as Arc<dyn ironclaw_extension_host::ChannelConfigReactivation>,
        admin_configuration_consumers,
    )
    .map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("admin configuration handler is invalid: {error}"),
    })?;
    let operator_auto_approve_settings: Arc<dyn ironclaw_approvals::AutoApproveSettingStorePort> =
        Arc::clone(&auto_approve_settings)
            as Arc<dyn ironclaw_approvals::AutoApproveSettingStorePort>;
    let operator_tool_permission_overrides: Arc<
        dyn ironclaw_approvals::ToolPermissionOverrideStorePort,
    > = Arc::clone(&tool_permission_overrides)
        as Arc<dyn ironclaw_approvals::ToolPermissionOverrideStorePort>;
    let operator_persistent_approval_policies: Arc<
        dyn ironclaw_approvals::PersistentApprovalPolicyStorePort,
    > = Arc::clone(&stores.persistent_approval_policies)
        as Arc<dyn ironclaw_approvals::PersistentApprovalPolicyStorePort>;
    let operator_synthetic_tools = {
        let provider = outbound_delivery_synthetic_provider().map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: format!("outbound delivery synthetic provider id is invalid: {error}"),
            }
        })?;
        vec![
            outbound_delivery_target_set_operator_tool_info(provider).map_err(|error| {
                RebornBuildError::InvalidConfig {
                    reason: format!("outbound delivery operator tool is invalid: {error}"),
                }
            })?,
        ]
    };
    let operator_tool_catalog: Arc<dyn ironclaw_product::RebornOperatorToolCatalog> =
        Arc::new(ActiveRegistryOperatorToolCatalog::new(
            services.shared_extension_registry(),
            operator_synthetic_tools,
            Some(Arc::clone(&extension_management)),
        ));
    insert_operator_config_handler(
        &mut first_party_registry,
        operator_auto_approve_settings,
        operator_tool_permission_overrides,
        operator_persistent_approval_policies,
        operator_tool_catalog,
    )
    .map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("operator configuration handler is invalid: {error}"),
    })?;
    let outbound_target_provider = Arc::clone(&outbound_delivery_targets)
        as Arc<dyn crate::outbound::OutboundDeliveryTargetProvider>;
    let outbound_preferences_facade: Arc<dyn OutboundPreferencesProductService> =
        Arc::new(crate::outbound::RebornOutboundPreferencesService::new(
            Arc::clone(&outbound_stores.outbound_preferences),
            outbound_target_provider,
        ));
    insert_outbound_preferences_handler(&mut first_party_registry, outbound_preferences_facade)
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("outbound preferences handler is invalid: {error}"),
        })?;
    insert_skill_auto_activate_handler(
        &mut first_party_registry,
        Arc::clone(&skill_auto_activate_learned),
    )
    .map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("skill auto-activation handler is invalid: {error}"),
    })?;
    services = services.with_first_party_capabilities(Arc::new(first_party_registry));
    let admin_configuration_resolver_for_generic = Arc::clone(&admin_configuration_resolver);
    let channel_pairing_registry;
    let channel_host_wiring = {
        let reserved_capability_ids: std::collections::BTreeSet<_> = services
            .shared_extension_registry()
            .snapshot()
            .capabilities()
            .filter(|descriptor| {
                descriptor.provider.as_str() == ironclaw_host_runtime::BUILTIN_FIRST_PARTY_PROVIDER
            })
            .map(|descriptor| descriptor.id.clone())
            .collect();
        let generic_installation_store = extension_management.installation_store_handle();
        let backend_extension_host =
            BackendExtensionHostAssemblyBuilder::new(BackendExtensionHostAssemblyInput {
                binder: services.extension_lane_tool_binder(),
                native_factories: native_extension_factories,
                channel_bindings: channel_extension_bindings.clone(),
                installation_store: generic_installation_store,
                admin_configuration_resolver: Arc::clone(&admin_configuration_resolver_for_generic),
                resource_governor: Arc::clone(&resource_governor)
                    as Arc<dyn ironclaw_resources::ResourceGovernor>,
                reserved_capability_ids,
                host_runtime_http_egress,
                channel_egress_scope: channel_egress_scope.clone(),
                deployment_channels: Arc::clone(&deployment_channels),
                filesystem: Arc::clone(&stores.filesystem),
                outbound_state: Arc::clone(&outbound_stores.outbound_state)
                    as Arc<dyn ironclaw_outbound::OutboundStateStorePort>,
            })
            .build()
            .await?;
        let pairing_installation_store = Arc::clone(&backend_extension_host.installation_store);
        extension_management.attach_generic_host(Arc::clone(&backend_extension_host.generic_host));
        if let Some(ports) = services.product_auth_provider_runtime_ports() {
            extension_management.attach_discovery_runtime_ports(ports.clone());
        }
        services.set_extension_tool_resolver(backend_extension_host.resolver);
        let channel_pairing_registry_built =
            BackendChannelPairingAssemblyBuilder::new(BackendChannelPairingAssemblyInput {
                descriptors: account_setup_descriptors,
                account_setups,
                filesystem: Arc::clone(&fold_filesystem),
                scope: channel_egress_scope.clone(),
                installation_store: pairing_installation_store,
                admin_configuration_resolver: Arc::clone(&admin_configuration_resolver_for_generic),
                continuation: lifecycle_wrapped_auth_continuation,
                identity_store: Arc::clone(&channel_identity_store),
                dm_targets: Arc::clone(&channel_dm_target_store),
                credential_cleanup: Arc::clone(&product_auth_services)
                    as Arc<
                        dyn ironclaw_extension_host::channel_connection::ChannelCredentialCleanup,
                    >,
                account_status_reader: Arc::clone(&product_auth_services)
                    as Arc<
                        dyn ironclaw_extension_host::channel_connection::ChannelAccountStatusReader,
                    >,
                disconnect_slot: Arc::clone(&channel_disconnect_slot),
            })
            .build()
            .await?;
        channel_pairing_registry = Some(Arc::clone(&channel_pairing_registry_built));
        ChannelHostWiring {
            extension_ingress: Some(backend_extension_host.ingress),
            delivery_coordinator: backend_extension_host.delivery_coordinator,
            channel_delivery_resolver: backend_extension_host.channel_delivery_resolver,
            #[cfg(feature = "test-support")]
            channel_egress_credential_bridges: Some(
                backend_extension_host.channel_egress_credential_bridges,
            ),
        }
    };
    let shared_extension_registry = services.shared_extension_registry();

    #[cfg(test)]
    let standalone_wasm_runtime_credential_provider_captured =
        services.wasm_runtime_credential_provider_captured_for_test();
    let host_runtime: Arc<dyn ironclaw_host_runtime::HostRuntime> = if uses_local_host_runtime {
        Arc::new(services.host_runtime_for_local_testing())
    } else {
        Arc::new(services.host_runtime_for_production(&wiring_config)?)
    };

    Ok(RebornRuntimeStores {
        host_runtime,
        #[cfg(test)]
        turn_coordinator,
        readiness: readiness_for(profile, true, true, product_auth_ready),
        product_auth: product_auth_services,
        skill_management,
        extension_lifecycle_surface_context,
        owner_user_id,
        approval_requests: Arc::clone(&approval_requests),
        capability_leases: Arc::clone(&stores.leases),
        external_tool_catalog: Arc::new(InMemoryExternalToolCatalog::new()),
        runtime_policy: runtime_policy_for_return,
        persistent_approval_policies: Arc::clone(&stores.persistent_approval_policies),
        tool_permission_overrides: Arc::clone(&tool_permission_overrides),
        auto_approve_settings: Arc::clone(&auto_approve_settings),
        #[cfg(any(test, feature = "test-support"))]
        capability_policy: Arc::clone(&capability_policy),
        outbound_preferences: outbound_stores.outbound_preferences,
        outbound_delivery_targets: Arc::clone(&outbound_delivery_targets),
        skill_auto_activate_learned: Arc::clone(&skill_auto_activate_learned),
        outbound_state: outbound_stores.outbound_state,
        delivered_gate_routes: outbound_stores.delivered_gate_routes,
        triggered_run_delivery: outbound_stores.triggered_run_delivery,
        #[cfg(any(test, feature = "test-support"))]
        trigger_source_turn_state,
        #[cfg(any(test, feature = "test-support"))]
        trigger_source_turn_state_store,
        extension_management,
        admin_configuration,
        admin_configuration_uses: Arc::new(admin_configuration_uses),
        channel_config_service: Arc::clone(&admin_configuration_resolver),
        channel_identity_store,
        channel_dm_target_store,
        channel_disconnect_slot,
        runtime_http_egress,
        skill_mounts,
        memory_mounts,
        system_extensions_lifecycle_mounts,
        skill_filesystem,
        workspace_filesystem,
        extension_filesystem: Arc::clone(&stores.filesystem),
        memory_service_resolver: memory_resolver,
        workspace_mounts: runtime_workspace_mounts,
        standalone_storage_root,
        default_system_prompt_path,
        #[cfg(any(test, feature = "test-support"))]
        in_memory_budget_event_sink,
        extension_registry: Arc::clone(&extension_registry),
        shared_extension_registry,
        scoped_filesystem: Arc::clone(&stores.scoped_filesystem),
        turn_state: Arc::clone(&turn_state),
        checkpoint_state_store,
        loop_checkpoint_store: Arc::clone(&turn_state) as Arc<dyn LoopCheckpointStore>,
        thread_service,
        trigger_repository: Arc::clone(&trigger_repository),
        resource_governor: production_resource_governor,
        budget_gate_store,
        broadcast_budget_event_sink,
        event_log,
        audit_log,
        admin_secret_provisioner,
        project_service,
        trigger_conversation_services,
        production_scheduler_wake: Some(scheduler_wake_wiring),
        secret_store,
        #[cfg(test)]
        standalone_wasm_runtime_credential_provider_captured,
        // `Ready` only when this path built a durable candidate source (i.e. no
        // caller-supplied product_auth_ports override); `Absent` otherwise. The
        // leader lock is always available on this production path.
        credential_refresh_worker,
        channel_extension_bindings,
        deployment_channels,
        extension_ingress: channel_host_wiring.extension_ingress,
        channel_pairing: channel_pairing_registry,
        delivery_coordinator: channel_host_wiring.delivery_coordinator,
        channel_delivery_resolver: channel_host_wiring.channel_delivery_resolver,
        #[cfg(feature = "test-support")]
        channel_egress_credential_bridges: channel_host_wiring.channel_egress_credential_bridges,
    })
}

/// Common tail of the libsql/postgres production build paths. After each
/// backend assembles its unified `CompositeRootFilesystem`, trigger repository,
/// event-store config, and refresh leader lock, this single-sources the
/// resource-governor + `ProductionStoreBundle` + backend build so the two paths
/// cannot drift on the store-assembly recipe.
async fn finish_production_backend(
    context: RebornProductionBuildContext,
    filesystem: Arc<CompositeRootFilesystem>,
    trigger_repository: Arc<dyn TriggerRepository>,
    secret_master_key: ironclaw_secrets::SecretMaterial,
    event_store_config: ironclaw_reborn_event_store::RebornEventStoreConfig,
    leader_lock: ironclaw_auth::CredentialRefreshLeaderLock,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    let resource_governor = filesystem_resource_governor(&filesystem);
    let stores = ProductionStoreBundle::new(
        filesystem,
        resource_governor,
        secret_master_key,
        event_store_config,
    )
    .await?;
    build_backend_production(context, stores, trigger_repository, leader_lock).await
}

async fn build_libsql_production(
    context: RebornProductionBuildContext,
    db: Arc<libsql::Database>,
    path_or_url: String,
    auth_token: Option<ironclaw_secrets::SecretMaterial>,
    secret_master_key: ironclaw_secrets::SecretMaterial,
    process_local_resource_governor_singleton: bool,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    use ironclaw_filesystem::LibSqlRootFilesystem;

    ensure_libsql_resource_governor_authority_for_build(process_local_resource_governor_singleton)?;
    let database_filesystem = Arc::new(LibSqlRootFilesystem::new(Arc::clone(&db)));
    database_filesystem.run_migrations().await?;
    let trigger_repository = Arc::new(ironclaw_triggers::LibSqlTriggerRepository::new(db));
    trigger_repository
        .run_migrations()
        .await
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("libSQL trigger repository migrations failed: {error}"),
        })?;
    let filesystem =
        production_database_root_filesystem(database_filesystem, "production-libsql-reborn-state")?;
    finish_production_backend(
        context,
        filesystem,
        trigger_repository,
        secret_master_key,
        ironclaw_reborn_event_store::RebornEventStoreConfig::Libsql {
            path_or_url,
            auth_token,
        },
        ironclaw_auth::CredentialRefreshLeaderLock::always_leader_for_single_writer(),
    )
    .await
}

async fn build_postgres_production(
    context: RebornProductionBuildContext,
    pool: deadpool_postgres::Pool,
    secret_master_key: ironclaw_secrets::SecretMaterial,
    process_local_resource_governor_singleton: bool,
) -> Result<RebornRuntimeStores, RebornBuildError> {
    use ironclaw_filesystem::PostgresRootFilesystem;

    ensure_postgres_resource_governor_authority_for_build(
        process_local_resource_governor_singleton,
    )?;
    // A4: Clone the pool before it is moved into PostgresTriggerRepository so we
    // can thread it to the credential keepalive worker as a leader-lock for
    // sweep serialization.
    // This clone stays PRIVATE — it is never exposed through any public facade.
    let pool_for_refresh_lock = pool.clone();
    let database_filesystem = Arc::new(PostgresRootFilesystem::new(pool.clone()));
    database_filesystem.run_migrations().await?;
    let trigger_repository = Arc::new(ironclaw_triggers::PostgresTriggerRepository::new(
        pool.clone(),
    ));
    trigger_repository
        .run_migrations()
        .await
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("PostgreSQL trigger repository migrations failed: {error}"),
        })?;
    let filesystem = production_database_root_filesystem(
        database_filesystem,
        "production-postgres-reborn-state",
    )?;
    finish_production_backend(
        context,
        filesystem,
        trigger_repository,
        secret_master_key,
        ironclaw_reborn_event_store::RebornEventStoreConfig::PostgresPool { pool },
        ironclaw_auth::CredentialRefreshLeaderLock::for_postgres(pool_for_refresh_lock),
    )
    .await
}

fn readiness_for(
    profile: RebornCompositionProfile,
    host_runtime: bool,
    turn_coordinator: bool,
    product_auth: bool,
) -> RebornReadiness {
    let (state, diagnostics) = crate::readiness::readiness_contract_for_profile(profile);

    RebornReadiness {
        profile,
        state,
        services: RebornServiceReadiness {
            host_runtime,
            turn_coordinator,
            product_auth,
        },
        workers: RebornWorkerReadiness {
            turn_runner: false,
            trigger_poller: false,
        },
        diagnostics,
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod auth_tests;
#[cfg(test)]
mod capability_host_tests;
