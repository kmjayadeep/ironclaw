// arch-exempt: large_file, filesystem-v2 migration and compatibility contract coverage remains colocated through rollout, plan #6637
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;
use ironclaw_extensions::{
    CapabilityProviderHostApiContract, ExtensionCredentialBinding, ExtensionCredentialHandle,
    ExtensionHealthMessage, ExtensionHealthSnapshot, ExtensionHealthStatus, ExtensionInstallation,
    ExtensionInstallationError, ExtensionInstallationId, ExtensionInstallationPersistedParts,
    ExtensionInstallationStore, ExtensionInstallationStorePort, ExtensionManifestRecord,
    ExtensionManifestRef, HostApiContractRegistry, InstallationOwner, MANIFEST_SCHEMA_VERSION,
    ManifestHash, ManifestSource, ManifestV2Error, MembershipDeactivation,
};
use ironclaw_filesystem::{
    CasExpectation, Fault, FaultInjecting, FilesystemOperation, Filter, InMemoryBackend,
    LibSqlRootFilesystem, Page, PostgresRootFilesystem, RootFilesystem,
};
use ironclaw_host_api::{ExtensionId, HostPortCatalog, SecretHandle, UserId, VirtualPath};

fn extension_id(value: &str) -> ExtensionId {
    ExtensionId::new(value).unwrap()
}

fn installation_id(value: &str) -> ExtensionInstallationId {
    ExtensionInstallationId::new(value).unwrap()
}

fn manifest_hash(value: &str) -> ManifestHash {
    ManifestHash::new(value).unwrap()
}

async fn installation_store() -> ExtensionInstallationStore {
    ExtensionInstallationStore::load_at(
        Arc::new(InMemoryBackend::new()),
        VirtualPath::new("/system/extensions/.installations/test").unwrap(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap()
}

fn raw_capability_provider_manifest() -> String {
    format!(
        r#"
schema_version = "{schema}"
id = "acme-tools"
name = "Acme Tools"
version = "0.1.0"
description = "test"
trust = "third_party"

[runtime]
kind = "wasm"
module = "wasm/acme.wasm"

[[host_api]]
id = "ironclaw.capability_provider/v1"
section = "capability_provider.tools"

[capability_provider.tools]

[[capability_provider.tools.capabilities]]
id = "acme-tools.echo"
description = "Echoes input"
default_permission = "allow"
visibility = "model"
input_schema_ref = "schemas/acme/echo.input.v1.json"
output_schema_ref = "schemas/acme/echo.output.v1.json"
prompt_doc_ref = "prompts/acme/echo.md"
"#,
        schema = MANIFEST_SCHEMA_VERSION,
    )
}

fn contracts() -> HostApiContractRegistry {
    let mut registry = HostApiContractRegistry::new();
    registry
        .register(std::sync::Arc::new(
            CapabilityProviderHostApiContract::new().unwrap(),
        ))
        .unwrap();
    registry
}

fn manifest(hash: &str) -> ExtensionManifestRecord {
    ExtensionManifestRecord::from_toml(
        raw_capability_provider_manifest(),
        ManifestSource::HostBundled,
        &HostPortCatalog::empty(),
        Some(manifest_hash(hash)),
        &contracts(),
    )
    .unwrap()
}

fn installation(hash: &str) -> ExtensionInstallation {
    ExtensionInstallation::new(
        installation_id("acme-tools-prod"),
        extension_id("acme-tools"),
        ExtensionManifestRef::new(extension_id("acme-tools"), Some(manifest_hash(hash))),
        vec![],
        Utc::now(),
        InstallationOwner::Tenant,
    )
    .unwrap()
}

fn normalized_installation(hash: &str) -> ExtensionInstallation {
    ExtensionInstallation::new(
        installation_id("acme-tools-prod"),
        extension_id("acme-tools"),
        ExtensionManifestRef::new(extension_id("acme-tools"), Some(manifest_hash(hash))),
        vec![ExtensionCredentialBinding::new(
            ExtensionCredentialHandle::new("api-token").unwrap(),
            SecretHandle::new("secret-api-token").unwrap(),
        )],
        chrono::DateTime::parse_from_rfc3339("2026-07-24T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        InstallationOwner::user(UserId::new("alice").unwrap()),
    )
    .unwrap()
}

fn installation_with_manifest_hash(hash: Option<&str>) -> ExtensionInstallation {
    ExtensionInstallation::new(
        installation_id("acme-tools-prod"),
        extension_id("acme-tools"),
        ExtensionManifestRef::new(extension_id("acme-tools"), hash.map(manifest_hash)),
        vec![],
        Utc::now(),
        InstallationOwner::Tenant,
    )
    .unwrap()
}

#[test]
fn top_level_capabilities_are_rejected_for_every_source() {
    // The legacy manifest form is gone: capabilities are declared under an
    // ironclaw.capability_provider/v1 host_api section, for host-bundled
    // manifests exactly as for installed ones.
    let legacy = raw_capability_provider_manifest()
        .replace(
            "[[host_api]]\nid = \"ironclaw.capability_provider/v1\"\nsection = \"capability_provider.tools\"\n\n[capability_provider.tools]\n\n",
            "",
        )
        .replace("[[capability_provider.tools.capabilities]]", "[[capabilities]]");
    for source in [
        ManifestSource::InstalledLocal,
        ManifestSource::RegistryInstalled,
        ManifestSource::HostBundled,
    ] {
        let err = ExtensionManifestRecord::from_toml(
            legacy.clone(),
            source,
            &HostPortCatalog::empty(),
            Some(manifest_hash("sha256:abc")),
            &contracts(),
        )
        .unwrap_err();
        match err {
            ExtensionInstallationError::Manifest(ManifestV2Error::Invalid { reason }) => {
                assert!(
                    reason.contains("top-level [[capabilities]] is not supported"),
                    "{source:?}: {reason}"
                );
            }
            other => panic!("{source:?}: expected Invalid, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn upsert_installation_rejects_unknown_manifest() {
    let store = installation_store().await;

    let err = store
        .upsert_installation(
            ExtensionInstallation::new(
                installation_id("missing-prod"),
                extension_id("missing-tools"),
                ExtensionManifestRef::new(
                    extension_id("missing-tools"),
                    Some(manifest_hash("sha256:missing")),
                ),
                vec![],
                Utc::now(),
                InstallationOwner::Tenant,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        ExtensionInstallationError::UnknownManifest { .. }
    ));
}

#[tokio::test]
async fn upsert_manifest_rejects_manifest_hash_change_for_existing_installation() {
    let store = installation_store().await;
    store.upsert_manifest(manifest("sha256:old")).await.unwrap();
    store
        .upsert_installation(installation("sha256:old"))
        .await
        .unwrap();

    let err = store
        .upsert_manifest(manifest("sha256:new"))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        ExtensionInstallationError::ManifestHashMismatch { .. }
    ));
}

#[tokio::test]
async fn upsert_manifest_and_installation_replaces_coherent_manifest_hash_pair() {
    let store = installation_store().await;
    store.upsert_manifest(manifest("sha256:old")).await.unwrap();
    store
        .upsert_installation(installation("sha256:old"))
        .await
        .unwrap();

    store
        .upsert_manifest_and_installation(manifest("sha256:new"), installation("sha256:new"))
        .await
        .unwrap();

    let manifest = store
        .get_manifest(&extension_id("acme-tools"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manifest.manifest_hash(), Some(&manifest_hash("sha256:new")));
    let installation = store
        .get_installation(&installation_id("acme-tools-prod"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        installation.manifest_ref().manifest_hash(),
        Some(&manifest_hash("sha256:new"))
    );
}

#[tokio::test]
async fn upsert_manifest_and_installation_rejects_mismatched_manifest_hash_pair() {
    let store = installation_store().await;

    let err = store
        .upsert_manifest_and_installation(manifest("sha256:new"), installation("sha256:old"))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        ExtensionInstallationError::ManifestHashMismatch { .. }
    ));
    assert!(
        store
            .get_manifest(&extension_id("acme-tools"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .get_installation(&installation_id("acme-tools-prod"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn missing_installation_mutations_return_not_found() {
    let store = installation_store().await;
    let missing = installation_id("missing-prod");

    let health_err = store
        .update_health(&missing, ExtensionHealthSnapshot::healthy())
        .await
        .unwrap_err();
    assert!(matches!(
        health_err,
        ExtensionInstallationError::InstallationNotFound { .. }
    ));
}

#[tokio::test]
async fn manifest_hash_presence_mismatch_is_rejected() {
    let store = installation_store().await;
    store.upsert_manifest(manifest("sha256:abc")).await.unwrap();

    let missing_ref_hash = store
        .upsert_installation(installation_with_manifest_hash(None))
        .await
        .unwrap_err();
    assert!(matches!(
        missing_ref_hash,
        ExtensionInstallationError::ManifestHashMismatch { .. }
    ));

    let store = installation_store().await;
    let manifest_without_hash = ExtensionManifestRecord::from_toml(
        raw_capability_provider_manifest(),
        ManifestSource::HostBundled,
        &HostPortCatalog::empty(),
        None,
        &contracts(),
    )
    .unwrap();
    store.upsert_manifest(manifest_without_hash).await.unwrap();

    let unexpected_ref_hash = store
        .upsert_installation(installation_with_manifest_hash(Some("sha256:abc")))
        .await
        .unwrap_err();
    assert!(matches!(
        unexpected_ref_hash,
        ExtensionInstallationError::ManifestHashMismatch { .. }
    ));
}

#[test]
fn extension_health_message_redacts_public_renderings() {
    let message = ExtensionHealthMessage::new("provider stack trace with /host/path secret-token");
    let snapshot = ExtensionHealthSnapshot::new(
        ExtensionHealthStatus::Degraded,
        Some(message),
        chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
    );

    assert_eq!(
        format!("{:?}", snapshot.message().unwrap()),
        ExtensionHealthMessage::placeholder()
    );
    assert_eq!(
        snapshot.message().unwrap().to_string(),
        ExtensionHealthMessage::placeholder()
    );
    let json = serde_json::to_string(&snapshot).unwrap();
    assert!(json.contains(ExtensionHealthMessage::placeholder()));
    assert!(!json.contains("secret-token"));
    assert!(!json.contains("/host/path"));
}

#[test]
fn extension_health_message_round_trip_stays_redacted() {
    let json = r#"
{
  "status": "degraded",
  "message": "provider stack trace with /host/path secret-token",
  "checked_at": "2026-01-01T00:00:00Z"
}
"#;

    let snapshot: ExtensionHealthSnapshot = serde_json::from_str(json).unwrap();
    assert_eq!(
        snapshot.message().unwrap().to_string(),
        ExtensionHealthMessage::placeholder()
    );

    let serialized = serde_json::to_string(&snapshot).unwrap();
    assert!(serialized.contains(ExtensionHealthMessage::placeholder()));
    assert!(!serialized.contains("secret-token"));
    assert!(!serialized.contains("/host/path"));
}

#[test]
fn extension_installation_identifiers_reject_empty_and_control_chars() {
    assert!(matches!(
        ManifestHash::new(""),
        Err(ExtensionInstallationError::InvalidValue { .. })
    ));
    assert!(matches!(
        ExtensionInstallationId::new("install\nbad"),
        Err(ExtensionInstallationError::InvalidValue { .. })
    ));
    assert!(matches!(
        ironclaw_extensions::ExtensionCredentialHandle::new("credential\rbad"),
        Err(ExtensionInstallationError::InvalidValue { .. })
    ));

    assert!(serde_json::from_str::<ManifestHash>("\"\"").is_err());
    assert!(serde_json::from_str::<ExtensionInstallationId>(r#""install\nbad""#).is_err());
    assert!(
        serde_json::from_str::<ironclaw_extensions::ExtensionCredentialHandle>(
            r#""credential\rbad""#
        )
        .is_err()
    );
}

#[test]
fn new_installation_uses_updated_at_for_initial_health_timestamp() {
    let updated_at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let installation = ExtensionInstallation::new(
        installation_id("acme-tools-prod"),
        extension_id("acme-tools"),
        ExtensionManifestRef::new(
            extension_id("acme-tools"),
            Some(manifest_hash("sha256:abc")),
        ),
        vec![],
        updated_at,
        InstallationOwner::Tenant,
    )
    .unwrap();

    assert_eq!(installation.health().checked_at(), updated_at);
}

#[test]
fn persisted_reconstruction_preserves_health_timestamp_and_bindings() {
    let extension_id = extension_id("acme-tools");
    let checked_at = chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let updated_at = chrono::DateTime::parse_from_rfc3339("2026-01-03T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let health = ExtensionHealthSnapshot::new(
        ExtensionHealthStatus::Degraded,
        Some(ExtensionHealthMessage::new("redacted diagnostic")),
        checked_at,
    );
    let binding = ExtensionCredentialBinding::new(
        ExtensionCredentialHandle::new("api").unwrap(),
        SecretHandle::new("api-secret").unwrap(),
    );
    let owner = InstallationOwner::users(BTreeSet::from([UserId::new("alice").unwrap()])).unwrap();

    let installation =
        ExtensionInstallation::from_persisted_parts(ExtensionInstallationPersistedParts {
            installation_id: installation_id("acme-tools"),
            extension_id: extension_id.clone(),
            manifest_ref: ExtensionManifestRef::new(extension_id, None),
            credential_bindings: vec![binding.clone()],
            health: health.clone(),
            updated_at,
            owner: owner.clone(),
        })
        .unwrap();

    assert_eq!(installation.health(), &health);
    assert_eq!(installation.updated_at(), updated_at);
    assert_eq!(installation.credential_bindings(), &[binding]);
    assert_eq!(installation.owner(), &owner);
}

#[tokio::test]
async fn installations_sort_by_id() {
    let older = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let newer = chrono::DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let store = installation_store().await;
    store.upsert_manifest(manifest("sha256:abc")).await.unwrap();

    for (id, updated_at) in [
        ("acme-tools-b", older),
        ("acme-tools-c", newer),
        ("acme-tools-a", older),
    ] {
        store
            .upsert_installation(
                ExtensionInstallation::new(
                    installation_id(id),
                    extension_id("acme-tools"),
                    ExtensionManifestRef::new(
                        extension_id("acme-tools"),
                        Some(manifest_hash("sha256:abc")),
                    ),
                    vec![],
                    updated_at,
                    InstallationOwner::Tenant,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    let ids: Vec<_> = store
        .list_installations()
        .await
        .unwrap()
        .into_iter()
        .map(|installation| installation.installation_id().as_str().to_owned())
        .collect();
    assert_eq!(ids, ["acme-tools-a", "acme-tools-b", "acme-tools-c"]);
}

#[tokio::test]
async fn installation_store_persists_manifest_and_installation_as_rows() {
    let filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let root = VirtualPath::new("/system/extensions/.installations/reload").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        ironclaw_extensions::HostApiContractRegistry::new(),
    )
    .await
    .unwrap();

    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installation("sha256:abc"))
        .await
        .unwrap();

    let manifest_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/manifests", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    assert_eq!(manifest_rows.len(), 1);
    assert_eq!(
        manifest_rows[0].entry.kind.as_ref().unwrap().as_str(),
        "extension_manifest_record"
    );
    let installation_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/installations", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    assert_eq!(installation_rows.len(), 1);
    assert_eq!(
        installation_rows[0].entry.kind.as_ref().unwrap().as_str(),
        "extension_installation_record"
    );

    let reloaded = ExtensionInstallationStore::load_at(
        filesystem,
        root,
        HostPortCatalog::empty(),
        ironclaw_extensions::HostApiContractRegistry::new(),
    )
    .await
    .unwrap();
    assert!(
        reloaded
            .get_installation(&installation_id("acme-tools-prod"))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn normalized_v2_layout_separates_mutable_extension_state_and_reopens_the_aggregate() {
    let filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let root = VirtualPath::new("/system/extensions/.installations/normalized-v2").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let expected = normalized_installation("sha256:abc");

    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), expected.clone())
        .await
        .unwrap();

    let collection_rows = [
        ("manifests", "extension_manifest_record_v2"),
        ("installations", "extension_installation_record_v2"),
        ("memberships", "extension_membership_record_v2"),
        (
            "credential-bindings",
            "extension_credential_binding_record_v2",
        ),
        ("health", "extension_health_record_v2"),
    ];
    for (collection, expected_kind) in collection_rows {
        let rows = filesystem
            .query(
                &VirtualPath::new(format!("{}/v2/{collection}", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "expected one v2 {collection} row");
        assert_eq!(
            rows[0].entry.kind.as_ref().unwrap().as_str(),
            expected_kind,
            "unexpected {collection} record kind"
        );
        let body: serde_json::Value = rows[0].entry.parse_json().unwrap();
        assert_eq!(body["schema_version"], "extension_state.v2");
    }

    let installation_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/installations", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    let core: serde_json::Value = installation_rows[0].entry.parse_json().unwrap();
    for aggregate_field in ["owner", "credential_bindings", "health"] {
        assert!(
            core.get(aggregate_field).is_none(),
            "installation core must not embed {aggregate_field}: {core}"
        );
    }

    let membership_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/memberships", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    let membership: serde_json::Value = membership_rows[0].entry.parse_json().unwrap();
    assert_eq!(membership["installation_id"], "acme-tools-prod");
    assert_eq!(membership["user_id"], "alice");
    assert_eq!(membership["status"], "active");
    assert_eq!(
        membership_rows[0]
            .path
            .as_str()
            .trim_start_matches(format!("{}/v2/memberships/", root.as_str()).as_str())
            .split('/')
            .count(),
        2,
        "membership rows must be nested by installation then user token"
    );

    let binding_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/credential-bindings", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    let binding: serde_json::Value = binding_rows[0].entry.parse_json().unwrap();
    assert_eq!(binding["installation_id"], "acme-tools-prod");
    assert_eq!(binding["credential_handle"], "api-token");
    assert_eq!(binding["secret_handle"], "secret-api-token");
    assert_eq!(
        binding_rows[0]
            .path
            .as_str()
            .trim_start_matches(format!("{}/v2/credential-bindings/", root.as_str()).as_str())
            .split('/')
            .count(),
        2,
        "binding rows must be nested by installation then binding token"
    );

    let all_v2_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(20),
        )
        .await
        .unwrap();
    assert_eq!(all_v2_rows.len(), 5);
    assert!(
        all_v2_rows
            .iter()
            .all(|row| row.entry.kind.is_some()
                && row.entry.content_type.as_str() == "application/json"),
        "v2 lifecycle state must contain records, not package bytes"
    );

    let reloaded = ExtensionInstallationStore::load_at(
        filesystem,
        root,
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    assert_eq!(
        reloaded
            .get_installation(&installation_id("acme-tools-prod"))
            .await
            .unwrap(),
        Some(expected)
    );
}

#[tokio::test]
async fn normalized_v2_soft_removal_preserves_records_and_allows_reactivation() {
    let filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let root = VirtualPath::new("/system/extensions/.installations/normalized-v2-remove").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();

    store
        .delete_installation(installed.installation_id())
        .await
        .unwrap();
    assert!(
        store
            .get_installation(installed.installation_id())
            .await
            .unwrap()
            .is_none(),
        "soft-removed installations are hidden from the live store view"
    );

    for (collection, expected_status) in [
        ("installations", Some("removed")),
        ("memberships", Some("removed")),
        ("credential-bindings", Some("removed")),
        ("health", None),
    ] {
        let rows = filesystem
            .query(
                &VirtualPath::new(format!("{}/v2/{collection}", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "soft removal must retain the {collection} row"
        );
        if let Some(expected_status) = expected_status {
            let body: serde_json::Value = rows[0].entry.parse_json().unwrap();
            assert_eq!(body["status"], expected_status);
        }
    }

    store.upsert_installation(installed.clone()).await.unwrap();
    assert_eq!(
        store
            .get_installation(installed.installation_id())
            .await
            .unwrap(),
        Some(installed.clone()),
        "reinstall must reactivate the retained record identities"
    );

    store
        .delete_installation(installed.installation_id())
        .await
        .unwrap();
    store
        .delete_manifest(installed.extension_id())
        .await
        .unwrap();
    assert!(
        store
            .get_manifest(installed.extension_id())
            .await
            .unwrap()
            .is_none()
    );
    let manifest_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/manifests", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    assert_eq!(manifest_rows.len(), 1);
    let manifest_body: serde_json::Value = manifest_rows[0].entry.parse_json().unwrap();
    assert_eq!(manifest_body["status"], "removed");
}

#[tokio::test]
async fn normalized_v2_bootstrap_imports_legacy_rows_and_repairs_compatibility_views() {
    let source_filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let source_root = VirtualPath::new("/system/extensions/.installations/legacy-source").unwrap();
    let source_store = ExtensionInstallationStore::load_at(
        Arc::clone(&source_filesystem),
        source_root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    source_store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();

    let target_filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let target_root = VirtualPath::new("/system/extensions/.installations/legacy-target").unwrap();
    for collection in ["manifests", "installations"] {
        let source_prefix =
            VirtualPath::new(format!("{}/{collection}", source_root.as_str())).unwrap();
        let rows = source_filesystem
            .query(&source_prefix, &Filter::All, Page::first(10))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "legacy fixture must contain {collection}");
        let relative = rows[0]
            .path
            .as_str()
            .strip_prefix(source_root.as_str())
            .unwrap();
        let target_path = VirtualPath::new(format!("{}{relative}", target_root.as_str())).unwrap();
        target_filesystem
            .put(&target_path, rows[0].entry.clone(), CasExpectation::Absent)
            .await
            .unwrap();
    }

    let migrated = ExtensionInstallationStore::load_at(
        Arc::clone(&target_filesystem),
        target_root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    assert_eq!(
        migrated
            .get_installation(installed.installation_id())
            .await
            .unwrap(),
        Some(installed)
    );
    let v2_rows = target_filesystem
        .query(
            &VirtualPath::new(format!("{}/v2", target_root.as_str())).unwrap(),
            &Filter::All,
            Page::first(20),
        )
        .await
        .unwrap();
    assert_eq!(v2_rows.len(), 5, "legacy aggregate must expand into v2");

    let compatibility_rows = target_filesystem
        .query(
            &VirtualPath::new(format!("{}/installations", target_root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    assert_eq!(compatibility_rows.len(), 1);
    target_filesystem
        .delete(&compatibility_rows[0].path)
        .await
        .unwrap();

    let reopened = ExtensionInstallationStore::load_at(
        Arc::clone(&target_filesystem),
        target_root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    assert!(
        reopened
            .get_installation(&installation_id("acme-tools-prod"))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        target_filesystem
            .query(
                &VirtualPath::new(format!("{}/installations", target_root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap()
            .len(),
        1,
        "startup must repair the compatibility aggregate from v2"
    );
}

#[tokio::test]
async fn interrupted_v2_install_stays_invisible_and_retry_repairs_it() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::WriteFile)
                .path("/v2/installations/")
                .nth(1)
                .backend("interrupt before installation commit"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root = VirtualPath::new("/system/extensions/.installations/interrupted-v2").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let expected = normalized_installation("sha256:abc");

    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), expected.clone())
        .await
        .expect_err("the injected core-row failure must interrupt the first install");

    assert!(
        store
            .get_manifest(&extension_id("acme-tools"))
            .await
            .unwrap()
            .is_none(),
        "a failed paired install must restore the prior manifest state"
    );
    assert!(
        store
            .get_installation(expected.installation_id())
            .await
            .unwrap()
            .is_none(),
        "children written before the core commit must not expose a partial installation"
    );
    assert_eq!(
        filesystem
            .query(
                &VirtualPath::new(format!("{}/v2/memberships", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap()
            .len(),
        1,
        "the interrupted child row remains available for an idempotent repair"
    );

    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), expected.clone())
        .await
        .expect("retry completes the normalized installation");
    assert_eq!(
        store
            .get_installation(expected.installation_id())
            .await
            .unwrap()
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn interrupted_active_aggregate_update_rolls_back_before_readers_resume() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::WriteFile)
                .path("/v2/health/")
                .nth(2)
                .backend("interrupt an active aggregate update after membership writes"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root = VirtualPath::new("/system/extensions/.installations/interrupted-update").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();
    let updated = installed.clone().with_owner(
        InstallationOwner::users(BTreeSet::from([
            UserId::new("alice").unwrap(),
            UserId::new("bob").unwrap(),
        ]))
        .unwrap(),
    );

    store
        .upsert_installation(updated)
        .await
        .expect_err("the injected health failure must interrupt the aggregate update");

    let visible = store
        .get_installation(installed.installation_id())
        .await
        .unwrap()
        .expect("the prior aggregate must be restored before the error returns");
    assert_eq!(
        visible.owner().members().unwrap(),
        &BTreeSet::from([UserId::new("alice").unwrap()])
    );
    let membership_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/memberships", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    let bob = membership_rows
        .iter()
        .map(|row| row.entry.parse_json::<serde_json::Value>().unwrap())
        .find(|body| body["user_id"] == "bob")
        .expect("the interrupted child remains as a tombstone");
    assert_eq!(bob["status"], "removed");
}

#[tokio::test]
async fn interrupted_soft_removal_is_idempotently_completed_by_same_store_retry() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::WriteFile)
                .path("/v2/memberships/")
                .nth(2)
                .backend("interrupt after installation tombstone"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root = VirtualPath::new("/system/extensions/.installations/interrupted-remove").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();

    store
        .delete_installation(installed.installation_id())
        .await
        .expect_err("the injected child-row failure must interrupt removal");
    assert!(
        store
            .get_installation(installed.installation_id())
            .await
            .unwrap()
            .is_none(),
        "the authoritative core tombstone hides an interrupted removal"
    );

    store
        .delete_installation(installed.installation_id())
        .await
        .expect("an immediate retry resumes the removal");
    assert!(
        store
            .get_installation(installed.installation_id())
            .await
            .unwrap()
            .is_none()
    );
    for collection in ["memberships", "credential-bindings"] {
        let rows = filesystem
            .query(
                &VirtualPath::new(format!("{}/v2/{collection}", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let body: serde_json::Value = rows[0].entry.parse_json().unwrap();
        assert_eq!(
            body["status"], "removed",
            "the retry must finish the {collection} tombstone"
        );
    }
}

#[tokio::test]
async fn interrupted_soft_removal_rolls_back_from_compatibility_snapshot_on_reopen() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::WriteFile)
                .path("/v2/memberships/")
                .nth(2)
                .backend("interrupt after removal reservation"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root =
        VirtualPath::new("/system/extensions/.installations/interrupted-remove-reopen").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();
    store
        .delete_installation(installed.installation_id())
        .await
        .expect_err("the injected child-row failure must interrupt removal");
    drop(store);

    let reopened = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .expect("startup rolls the reserved removal back to its compatibility snapshot");
    assert_eq!(
        reopened
            .get_installation(installed.installation_id())
            .await
            .unwrap(),
        Some(installed)
    );
    for collection in ["memberships", "credential-bindings"] {
        let rows = filesystem
            .query(
                &VirtualPath::new(format!("{}/v2/{collection}", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap();
        let body: serde_json::Value = rows[0].entry.parse_json().unwrap();
        assert_eq!(
            body["status"], "active",
            "startup must restore the {collection} row from the compatibility snapshot"
        );
    }
}

async fn assert_normalized_backend_contract(
    filesystem: Arc<dyn RootFilesystem>,
    root: VirtualPath,
) {
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let expected = normalized_installation("sha256:abc");
    let alice = UserId::new("alice").unwrap();
    let bob = UserId::new("bob").unwrap();
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), expected.clone())
        .await
        .unwrap();
    store
        .activate_membership(expected.installation_id(), &bob)
        .await
        .unwrap();
    store
        .deactivate_membership(expected.installation_id(), &alice)
        .await
        .unwrap();
    drop(store);

    let reopened = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installation = reopened
        .get_installation(expected.installation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        installation.owner().members().unwrap(),
        &BTreeSet::from([bob])
    );

    reopened
        .delete_installation(expected.installation_id())
        .await
        .unwrap();
    reopened
        .delete_manifest(expected.extension_id())
        .await
        .unwrap();
    assert!(
        reopened
            .get_installation(expected.installation_id())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        reopened
            .get_manifest(expected.extension_id())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        filesystem
            .query(
                &VirtualPath::new(format!("{}/v2/memberships", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap()
            .len(),
        2,
        "both users' soft-removed membership history remains queryable"
    );
}

#[tokio::test]
async fn normalized_v2_contract_runs_on_libsql() {
    let dir = tempfile::tempdir().unwrap();
    let database = Arc::new(
        libsql::Builder::new_local(dir.path().join("extensions.db"))
            .build()
            .await
            .unwrap(),
    );
    let filesystem = Arc::new(LibSqlRootFilesystem::new(database));
    filesystem.run_migrations().await.unwrap();
    let filesystem: Arc<dyn RootFilesystem> = filesystem;
    assert_normalized_backend_contract(
        filesystem,
        VirtualPath::new("/system/extensions/.installations/libsql-v2").unwrap(),
    )
    .await;
}

#[tokio::test]
async fn normalized_v2_contract_runs_on_postgres_when_configured() {
    let Ok(url) = std::env::var("IRONCLAW_FILESYSTEM_POSTGRES_URL") else {
        eprintln!(
            "skipping Postgres extension-state contract: \
             IRONCLAW_FILESYSTEM_POSTGRES_URL not set"
        );
        return;
    };
    let config = url.parse::<tokio_postgres::Config>().unwrap();
    let manager = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    let pool = deadpool_postgres::Pool::builder(manager)
        .max_size(4)
        .build()
        .unwrap();
    if let Err(error) = pool.get().await {
        eprintln!("skipping Postgres extension-state contract: database unavailable ({error})");
        return;
    }
    let filesystem = Arc::new(PostgresRootFilesystem::new(pool));
    filesystem.run_migrations().await.unwrap();
    let filesystem: Arc<dyn RootFilesystem> = filesystem;
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    assert_normalized_backend_contract(
        filesystem,
        VirtualPath::new(format!(
            "/system/extensions/.installations/postgres-v2-{suffix}"
        ))
        .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn membership_v2_activation_and_deactivation_mutate_only_the_target_user_row() {
    let filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let root = VirtualPath::new("/system/extensions/.installations/membership-v2").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();
    let bob = UserId::new("bob").unwrap();
    let alice = UserId::new("alice").unwrap();

    store
        .activate_membership(installed.installation_id(), &bob)
        .await
        .unwrap();
    store
        .activate_membership(installed.installation_id(), &bob)
        .await
        .unwrap();
    let joined = store
        .get_installation(installed.installation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        joined.owner().members().unwrap(),
        &BTreeSet::from([alice.clone(), bob.clone()])
    );

    store
        .deactivate_membership(installed.installation_id(), &alice)
        .await
        .unwrap();
    store
        .deactivate_membership(installed.installation_id(), &alice)
        .await
        .unwrap();
    let remaining = store
        .get_installation(installed.installation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        remaining.owner().members().unwrap(),
        &BTreeSet::from([bob.clone()])
    );

    let rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/memberships", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    let statuses = rows
        .iter()
        .map(|row| {
            let body: serde_json::Value = row.entry.parse_json().unwrap();
            (
                body["user_id"].as_str().unwrap().to_string(),
                body["status"].as_str().unwrap().to_string(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(statuses.get("alice").map(String::as_str), Some("removed"));
    assert_eq!(statuses.get("bob").map(String::as_str), Some("active"));

    let final_member_result = store
        .deactivate_membership(installed.installation_id(), &bob)
        .await
        .unwrap();
    assert_eq!(
        final_member_result,
        MembershipDeactivation::FinalMemberReserved
    );
    assert!(
        store
            .get_installation(installed.installation_id())
            .await
            .unwrap()
            .is_none(),
        "the final-member reservation must hide the aggregate until removal or rollback"
    );
    store
        .upsert_installation(remaining)
        .await
        .expect("restoring the reserved aggregate must be idempotent");
}

#[tokio::test]
async fn membership_deactivation_backend_failure_restores_the_prior_aggregate() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::Query)
                .path("/v2/memberships/")
                .nth(3)
                .backend("interrupt after the removal reservation"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root =
        VirtualPath::new("/system/extensions/.installations/deactivation-compensation").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root,
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let alice = UserId::new("alice").unwrap();
    let bob = UserId::new("bob").unwrap();
    let installed = normalized_installation("sha256:abc").with_owner(
        InstallationOwner::users(BTreeSet::from([alice.clone(), bob.clone()])).unwrap(),
    );
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();

    assert!(matches!(
        store
            .deactivate_membership(installed.installation_id(), &alice)
            .await
            .unwrap_err(),
        ExtensionInstallationError::StoreUnavailable { .. }
    ));
    assert_eq!(
        store
            .get_installation(installed.installation_id())
            .await
            .unwrap()
            .unwrap()
            .owner()
            .members()
            .unwrap(),
        &BTreeSet::from([alice.clone(), bob.clone()])
    );
    assert!(matches!(
        store
            .deactivate_membership(installed.installation_id(), &alice)
            .await
            .unwrap(),
        MembershipDeactivation::MembershipRemoved(_)
    ));
}

#[tokio::test]
async fn membership_v2_concurrent_distinct_activations_lose_no_members() {
    let store = Arc::new(installation_store().await);
    let installed = normalized_installation("sha256:abc");
    store.upsert_manifest(manifest("sha256:abc")).await.unwrap();
    store.upsert_installation(installed.clone()).await.unwrap();
    let bob = UserId::new("bob").unwrap();
    let carol = UserId::new("carol").unwrap();

    let (bob_result, carol_result) = tokio::join!(
        store.activate_membership(installed.installation_id(), &bob),
        store.activate_membership(installed.installation_id(), &carol),
    );
    bob_result.unwrap();
    carol_result.unwrap();

    let reloaded = store
        .get_installation(installed.installation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reloaded.owner().members().unwrap(),
        &BTreeSet::from([UserId::new("alice").unwrap(), bob, carol])
    );
}

#[tokio::test]
async fn final_member_reservation_blocks_a_join_from_a_second_store() {
    let filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let root = VirtualPath::new("/system/extensions/.installations/final-member-race").unwrap();
    let first = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let second = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root,
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    let bob = UserId::new("bob").unwrap();
    first
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();

    assert_eq!(
        first
            .deactivate_membership(installed.installation_id(), &UserId::new("alice").unwrap())
            .await
            .unwrap(),
        MembershipDeactivation::FinalMemberReserved
    );
    assert!(matches!(
        second
            .activate_membership(installed.installation_id(), &bob)
            .await
            .unwrap_err(),
        ExtensionInstallationError::MembershipMutationInProgress { .. }
    ));
    first
        .delete_installation(installed.installation_id())
        .await
        .unwrap();
    assert!(
        first
            .get_installation(installed.installation_id())
            .await
            .unwrap()
            .is_none()
    );
}

/// Review finding: two processes that both observe "no active core" (fresh
/// install or reinstall race) must not have the later creator's component
/// sync tombstone the earlier creator's just-activated membership. Creation
/// merges membership rows; it never sweeps rows it did not write.
#[tokio::test]
async fn concurrent_creation_does_not_tombstone_the_other_creators_membership() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::WriteFile)
                .path("/v2/installations/")
                .nth(1)
                .backend("interrupt the first creator before its core commit"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root = VirtualPath::new("/system/extensions/.installations/creation-race").unwrap();
    let first = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let second = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let alice_install = normalized_installation("sha256:abc");
    let bob = UserId::new("bob").unwrap();
    let bob_install = alice_install
        .clone()
        .with_owner(InstallationOwner::user(bob.clone()));

    first
        .upsert_manifest_and_installation(manifest("sha256:abc"), alice_install)
        .await
        .expect_err("the injected core-commit failure must interrupt the first creator");
    second
        .upsert_manifest_and_installation(manifest("sha256:abc"), bob_install)
        .await
        .expect("the second creator completes the installation");

    let membership_rows = filesystem
        .query(
            &VirtualPath::new(format!("{}/v2/memberships", root.as_str())).unwrap(),
            &Filter::All,
            Page::first(10),
        )
        .await
        .unwrap();
    let statuses = membership_rows
        .iter()
        .map(|row| {
            let body: serde_json::Value = row.entry.parse_json().unwrap();
            (
                body["user_id"].as_str().unwrap().to_string(),
                body["status"].as_str().unwrap().to_string(),
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        statuses.get("alice").map(String::as_str),
        Some("active"),
        "creation must not sweep a concurrent creator's membership row"
    );
    assert_eq!(statuses.get("bob").map(String::as_str), Some("active"));
    let merged = second
        .get_installation(&installation_id("acme-tools-prod"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        merged.owner().members().unwrap(),
        &BTreeSet::from([UserId::new("alice").unwrap(), bob])
    );
}

/// Review finding: once the v2 records (the authority) are committed, a
/// failure writing the legacy compatibility projection must not fail the
/// operation — the caller would compensate against a live install and leave a
/// ghost. The projection is repaired at the next startup instead.
#[tokio::test]
async fn aggregate_projection_write_failure_does_not_fail_a_committed_v2_install() {
    let backend = Arc::new(
        FaultInjecting::new(InMemoryBackend::new()).with_fault(
            Fault::on(FilesystemOperation::WriteFile)
                .path("projection-loss/installations/")
                .nth(1)
                .backend("interrupt the compatibility projection write"),
        ),
    );
    let filesystem: Arc<dyn RootFilesystem> = backend;
    let root = VirtualPath::new("/system/extensions/.installations/projection-loss").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let expected = normalized_installation("sha256:abc");

    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), expected.clone())
        .await
        .expect("a committed v2 install succeeds even when the projection write fails");
    assert_eq!(
        store
            .get_installation(expected.installation_id())
            .await
            .unwrap(),
        Some(expected.clone())
    );
    assert!(
        filesystem
            .query(
                &VirtualPath::new(format!("{}/installations", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap()
            .is_empty(),
        "the failed projection write leaves no legacy row behind"
    );

    drop(store);
    let reopened = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .get_installation(expected.installation_id())
            .await
            .unwrap(),
        Some(expected)
    );
    assert_eq!(
        filesystem
            .query(
                &VirtualPath::new(format!("{}/installations", root.as_str())).unwrap(),
                &Filter::All,
                Page::first(10),
            )
            .await
            .unwrap()
            .len(),
        1,
        "startup must repair the compatibility projection from v2"
    );
}

/// Review finding: a health update is a diagnostic write and must never
/// rewrite the installation core — a read-then-rewrite of the whole aggregate
/// can race a final removal and resurrect a removed core. The core row must
/// stay byte- and version-identical across a health update, and a health
/// update against a removed installation reports it as missing.
#[tokio::test]
async fn update_health_mutates_only_the_health_row() {
    let filesystem: Arc<dyn RootFilesystem> = Arc::new(InMemoryBackend::new());
    let root = VirtualPath::new("/system/extensions/.installations/health-isolated").unwrap();
    let store = ExtensionInstallationStore::load_at(
        Arc::clone(&filesystem),
        root.clone(),
        HostPortCatalog::empty(),
        contracts(),
    )
    .await
    .unwrap();
    let installed = normalized_installation("sha256:abc");
    store
        .upsert_manifest_and_installation(manifest("sha256:abc"), installed.clone())
        .await
        .unwrap();
    let core_path = VirtualPath::new(format!("{}/v2/installations", root.as_str())).unwrap();
    let core_before = filesystem
        .query(&core_path, &Filter::All, Page::first(10))
        .await
        .unwrap();
    assert_eq!(core_before.len(), 1);

    let degraded = ExtensionHealthSnapshot::new(
        ExtensionHealthStatus::Unhealthy,
        Some(ExtensionHealthMessage::new("activation failed")),
        Utc::now(),
    );
    store
        .update_health(installed.installation_id(), degraded.clone())
        .await
        .unwrap();

    let core_after = filesystem
        .query(&core_path, &Filter::All, Page::first(10))
        .await
        .unwrap();
    assert_eq!(
        core_after[0].version, core_before[0].version,
        "a health update must not rewrite the installation core row"
    );
    let refreshed = store
        .get_installation(installed.installation_id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.health().status(), degraded.status());
    assert_eq!(refreshed.health().checked_at(), degraded.checked_at());
    assert_eq!(
        refreshed.health().message().unwrap().to_string(),
        ExtensionHealthMessage::placeholder(),
        "persisted health messages stay redacted"
    );

    store
        .delete_installation(installed.installation_id())
        .await
        .unwrap();
    assert!(matches!(
        store
            .update_health(installed.installation_id(), degraded)
            .await
            .unwrap_err(),
        ExtensionInstallationError::InstallationNotFound { .. }
    ));
    let core_removed = filesystem
        .query(&core_path, &Filter::All, Page::first(10))
        .await
        .unwrap();
    let body: serde_json::Value = core_removed[0].entry.parse_json().unwrap();
    assert_eq!(
        body["status"], "removed",
        "a health update must never resurrect a removed core"
    );
}
