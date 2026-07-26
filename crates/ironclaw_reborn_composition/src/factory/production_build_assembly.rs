use super::*;

pub(super) async fn build_production_shaped(
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

pub(super) struct RebornProductionWiring {
    pub(super) trust_policy: Arc<HostTrustPolicy>,
    pub(super) runtime_policy: EffectiveRuntimePolicy,
    pub(super) turn_run_wake_notifier: Arc<dyn ironclaw_turns::TurnRunWakeNotifier>,
    pub(super) runtime_process_binding: RebornRuntimeProcessBinding,
}

pub(super) struct RebornProductionBuildContext {
    pub(super) profile: RebornCompositionProfile,
    pub(super) wiring_config: ironclaw_host_runtime::ProductionWiringConfig,
    pub(super) production_wiring: RebornProductionWiring,
    pub(super) local_process_port: Option<HostProcessPort>,
    pub(super) product_auth_ports: Option<RebornProductAuthServicePorts>,
    pub(super) oauth_provider_configs: Vec<crate::input::OAuthProviderBackendConfig>,
    pub(super) oauth_dcr_callback: Option<crate::input::OAuthDcrCallbackConfig>,
    pub(super) owner_id: String,
    pub(super) local_runtime_identity: Option<RebornLocalRuntimeIdentity>,
    pub(super) turn_state_store_limits: ironclaw_turns::TurnStateStoreLimits,
    /// Memory provider resolver (issue #3537), carried so the standalone profile
    /// source and the memory tools build providers through one resolver.
    pub(super) memory_resolver: MemoryServiceResolver,
    /// The pre-minted scheduler wake wiring to carry to `RebornRuntimeStores` so
    /// `build_reborn_runtime` can hand it to `build_default_planned_runtime` via
    /// `DefaultPlannedRuntimeParts.scheduler_wake_wiring`.
    pub(super) scheduler_wake_wiring: ironclaw_runner::runtime::SchedulerWakeWiring,
    pub(super) account_setup_descriptors: Vec<ironclaw_product::ExtensionAccountSetupDescriptor>,
    pub(super) nearai_mcp_bootstrap_config:
        Option<ironclaw_operator::llm_admin::nearai_mcp::NearAiMcpBootstrapConfig>,
    pub(super) native_extension_factories:
        Vec<Arc<dyn ironclaw_extension_host::NativeExtensionFactory>>,
    pub(super) channel_extension_bindings: Vec<crate::input::ChannelExtensionBinding>,
    /// Binary-injected neutral first-party bundle set (extension-runtime DEL-7):
    /// feeds the available-extension catalog, vendor auth recipes, and the
    /// reserved host-bundled id set.
    pub(super) first_party_bundles: Vec<ironclaw_extension_host::FirstPartyPackageBundle>,
    /// Binary-injected first-party capability handler registrars (GSuite,
    /// web tooling).
    pub(super) first_party_registrars:
        Vec<Arc<dyn ironclaw_extension_host::FirstPartyHandlerRegistrar>>,
    /// Injected credential-account visibility policy (see the build-input field).
    pub(super) credential_account_visibility_policy:
        Option<Arc<dyn ironclaw_auth::RuntimeCredentialAccountVisibilityPolicy>>,
    pub(super) workspace_filesystems: Option<WorkspaceFilesystems>,
    pub(super) standalone_storage_root: Option<PathBuf>,
    pub(super) default_system_prompt_path: Option<PathBuf>,
    /// Test-support host HTTP egress override (see `TestNetworkHttpEgress`).
    /// Carried from `RebornHostBindings::network_http_egress_for_test` so the
    /// unified production-shaped build honors an injected fake transport.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) network_http_egress_for_test: Option<Arc<dyn ironclaw_network::NetworkHttpEgress>>,
    /// Test-support only: allow trusted fixture packages copied into
    /// `/system/extensions` to validate as host-bundled.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) trust_fixture_extensions_for_test: bool,
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

pub(super) fn planned_run_profile_resolver()
-> Result<Arc<InMemoryRunProfileResolver>, RebornBuildError> {
    Ok(Arc::new(
        ironclaw_runner::planned_driver_factory::default_planned_run_profile_resolver().map_err(
            |error| RebornBuildError::PlannedRunProfileResolver {
                reason: error.to_string(),
            },
        )?,
    ))
}

pub(super) type FilesystemProductionHostRuntimeServices<F> = HostRuntimeServices<
    F,
    FilesystemResourceGovernor<F>,
    ironclaw_processes::ProcessStore<F>,
    ironclaw_processes::ProcessResultStore<F>,
>;

pub(super) fn substrate_only_default_owner_id() -> Result<UserId, crate::RebornCompositionError> {
    let identity = RebornRuntimeIdentity::reborn_cli();
    // The substrate-only builders do not receive app/runtime owner input.
    // Preserve their legacy location under the default `reborn-cli` owner.
    UserId::new(identity.tenant_id).map_err(crate::RebornCompositionError::Mount)
}
