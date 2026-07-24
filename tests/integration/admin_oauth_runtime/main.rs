//! Whole-path contract for manifest administrator OAuth configuration:
//! operator WebUI save -> durable manifest configuration -> OAuth start.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::extract::ConnectInfo;
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use chrono::{Duration as ChronoDuration, Utc};
use ironclaw_auth::{
    GOOGLE_GMAIL_MODIFY_SCOPE, GOOGLE_GMAIL_READONLY_SCOPE, GOOGLE_GMAIL_SEND_SCOPE,
};
use ironclaw_filesystem::{CasExpectation, Entry};
use ironclaw_host_api::runtime_policy::{
    ApprovalPolicy, AuditMode, DeploymentMode, EffectiveRuntimePolicy, FilesystemBackendKind,
    NetworkMode, ProcessBackendKind, RuntimeProfile, SecretMode,
};
use ironclaw_host_api::{AgentId, InvocationId, TenantId, UserId, VirtualPath};
use ironclaw_loop_host::{
    HostManagedModelError, HostManagedModelErrorKind, HostManagedModelGateway,
    HostManagedModelRequest, HostManagedModelResponse,
};
use ironclaw_network::{
    NetworkHttpEgress, NetworkHttpError, NetworkHttpRequest, NetworkHttpResponse, NetworkUsage,
};
use ironclaw_reborn_composition::{
    LOCAL_DEV_SECRETS_MASTER_KEY_PATH, OAuthClientConfig, PollSettings, ProductAuthRouteState,
    RebornRuntime, RebornRuntimeIdentity, RebornRuntimeInput, build_reborn_runtime,
    local_dev_build_input, product_auth_route_mount,
};
use ironclaw_reborn_config::{RebornBootConfig, RebornHome, RebornProfile};
use ironclaw_webui::{WebuiAuthentication, WebuiAuthenticator, WebuiServeConfig, webui_v2_app};
use serde_json::{Value, json};
use tower::ServiceExt;

const TOKEN: &str = "admin-oauth-runtime-token";
const TENANT: &str = "admin-oauth-runtime-tenant";
const USER: &str = "admin-oauth-runtime-operator";
const AGENT: &str = "admin-oauth-runtime-agent";

fn gmail_scopes() -> String {
    [
        GOOGLE_GMAIL_READONLY_SCOPE,
        GOOGLE_GMAIL_SEND_SCOPE,
        GOOGLE_GMAIL_MODIFY_SCOPE,
    ]
    .join(" ")
}

#[derive(Debug)]
struct UnusedModelGateway;

#[async_trait]
impl HostManagedModelGateway for UnusedModelGateway {
    async fn stream_model(
        &self,
        _request: HostManagedModelRequest,
    ) -> Result<HostManagedModelResponse, HostManagedModelError> {
        Err(HostManagedModelError::safe(
            HostManagedModelErrorKind::InvalidRequest,
            "admin OAuth runtime test does not invoke the model",
        ))
    }
}

struct OperatorToken;

#[async_trait]
impl WebuiAuthenticator for OperatorToken {
    async fn authenticate(&self, token: &str) -> Option<WebuiAuthentication> {
        (token == TOKEN)
            .then(|| WebuiAuthentication::operator(UserId::new(USER).expect("valid operator user")))
    }

    fn mounts_operator_webui_config_routes(&self) -> bool {
        true
    }
}

struct Harness {
    runtime: RebornRuntime,
    router: axum::Router,
    token_egress: Arc<OAuthTokenEgress>,
    _root: tempfile::TempDir,
}

#[derive(Debug, Default)]
struct OAuthTokenEgress {
    request_bodies: Mutex<Vec<Vec<u8>>>,
}

impl OAuthTokenEgress {
    fn forms(&self) -> Vec<std::collections::HashMap<String, String>> {
        self.request_bodies
            .lock()
            .expect("token request lock")
            .iter()
            .map(|body| url::form_urlencoded::parse(body).into_owned().collect())
            .collect()
    }
}

#[async_trait]
impl NetworkHttpEgress for OAuthTokenEgress {
    async fn execute(
        &self,
        request: NetworkHttpRequest,
    ) -> Result<NetworkHttpResponse, NetworkHttpError> {
        assert_eq!(request.url, "https://oauth2.googleapis.com/token");
        self.request_bodies
            .lock()
            .expect("token request lock")
            .push(request.body.clone());
        let body = json!({
            "access_token": "google-access-token",
            "refresh_token": "google-refresh-token",
            "expires_in": 3600,
            "scope": gmail_scopes(),
        })
        .to_string()
        .into_bytes();
        Ok(NetworkHttpResponse {
            status: 200,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            usage: NetworkUsage {
                request_bytes: request.body.len() as u64,
                response_bytes: body.len() as u64,
                resolved_ip: None,
            },
            body,
        })
    }
}

fn local_dev_policy() -> EffectiveRuntimePolicy {
    EffectiveRuntimePolicy {
        deployment: DeploymentMode::LocalSingleUser,
        requested_profile: RuntimeProfile::LocalDev,
        resolved_profile: RuntimeProfile::LocalDev,
        filesystem_backend: FilesystemBackendKind::HostWorkspace,
        process_backend: ProcessBackendKind::LocalHost,
        network_mode: NetworkMode::DirectLogged,
        secret_mode: SecretMode::ScrubbedEnv,
        approval_policy: ApprovalPolicy::AskDestructive,
        audit_mode: AuditMode::LocalMinimal,
    }
}

async fn build_harness() -> Harness {
    let root = tempfile::tempdir().expect("temporary runtime root");
    let storage_root = root.path().join("local-dev");
    std::fs::create_dir_all(&storage_root).expect("create storage root");
    std::fs::write(
        storage_root.join(LOCAL_DEV_SECRETS_MASTER_KEY_PATH),
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )
    .expect("write deterministic test secrets key");
    let home = RebornHome::resolve_from_env_parts(
        Some(root.path().join("reborn-home").into_os_string()),
        None,
        None,
    )
    .expect("valid reborn home");
    let boot = RebornBootConfig::new(home, RebornProfile::LocalDev);
    let boot_client = OAuthClientConfig::new(
        "boot.apps.googleusercontent.com",
        "http://127.0.0.1:3000/api/reborn/product-auth/oauth/google/callback",
        Some(secrecy::SecretString::from("boot-google-secret")),
    )
    .expect("valid boot Google OAuth client");
    let token_egress = Arc::new(OAuthTokenEgress::default());
    let services = local_dev_build_input(USER, storage_root)
        .with_runtime_policy(local_dev_policy())
        .with_bundled_first_party_for_test()
        .with_dcr_oauth_callback("http://127.0.0.1:3000")
        .expect("loopback callback origin")
        .with_vendor_oauth_client(ironclaw_auth::GOOGLE_PROVIDER_ID, boot_client)
        .with_network_http_egress_for_test(Arc::clone(&token_egress) as Arc<dyn NetworkHttpEgress>);
    let input = RebornRuntimeInput::from_build_input(services)
        .with_identity(RebornRuntimeIdentity {
            tenant_id: TENANT.to_string(),
            agent_id: AGENT.to_string(),
            source_binding_id: "admin-oauth-runtime-source".to_string(),
            reply_target_binding_id: "admin-oauth-runtime-reply".to_string(),
        })
        .with_poll_settings(PollSettings {
            interval: Duration::from_millis(10),
            max_total: Duration::from_secs(10),
        })
        .with_model_gateway_override(Arc::new(UnusedModelGateway))
        .with_boot_config(boot);

    let runtime = build_reborn_runtime(input).await.expect("runtime builds");
    let product_surface = runtime.product_surface(None).expect("product surface");
    let tenant_id = TenantId::new(TENANT).expect("valid tenant");
    let agent_id = AgentId::new(AGENT).expect("valid agent");
    let product_auth_mount = product_auth_route_mount(
        ProductAuthRouteState::new(
            runtime.product_auth_services(),
            tenant_id.clone(),
            Some(agent_id.clone()),
            None,
        )
        .with_product_surface(Arc::clone(&product_surface)),
    );
    let config = WebuiServeConfig::new(
        tenant_id,
        Arc::new(OperatorToken),
        vec![HeaderValue::from_static("http://localhost:0")],
    )
    .with_default_agent_id(agent_id)
    .with_split_route_mount(product_auth_mount);
    let router = webui_v2_app(product_surface, config).expect("WebUI router builds");
    Harness {
        runtime,
        router,
        token_egress,
        _root: root,
    }
}

fn operator_post(uri: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("valid operator POST request")
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("bounded response body");
    serde_json::from_slice(&bytes).expect("JSON response")
}

async fn install_gmail(router: &axum::Router) {
    let response = router
        .clone()
        .oneshot(operator_post(
            "/api/webchat/v2/extensions/install",
            json!({
                "package_ref": {"kind": "extension", "id": "gmail"},
                "client_action_id": "admin-oauth-runtime-install-gmail",
            }),
        ))
        .await
        .expect("install Gmail request");
    assert_eq!(response.status(), StatusCode::OK);
}

async fn save_google_admin_configuration(
    router: &axum::Router,
    client_id: &str,
    client_secret: &str,
    expected_revision: u64,
) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/webchat/v2/operator/extension-configuration/vendor.google")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({
                        "values": [
                            {"handle": "google_oauth_client_id", "value": client_id},
                            {"handle": "google_oauth_client_secret", "value": client_secret},
                        ],
                        "expected_revision": expected_revision,
                        "idempotency_key": format!("admin-oauth-runtime-{expected_revision}"),
                    })
                    .to_string(),
                ))
                .expect("valid administrator configuration request"),
        )
        .await
        .expect("save administrator configuration");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "administrator configuration save failed: {}",
        String::from_utf8_lossy(
            &to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("bounded error response")
        )
    );
}

async fn start_gmail_oauth_response(router: &axum::Router) -> axum::response::Response {
    router
        .clone()
        .oneshot(operator_post(
            "/api/webchat/v2/extensions/gmail/setup/oauth/start",
            json!({
                "requirement": "gmail_account",
                "expires_at": (Utc::now() + ChronoDuration::minutes(5)).to_rfc3339(),
                "invocation_id": InvocationId::new().to_string(),
            }),
        ))
        .await
        .expect("start Gmail OAuth request")
}

async fn start_gmail_oauth(router: &axum::Router) -> Value {
    let response = start_gmail_oauth_response(router).await;
    let status = response.status();
    let body = response_json(response).await;
    assert_eq!(status, StatusCode::OK, "OAuth start body: {body}");
    body
}

fn assert_authorization_client(
    started: &Value,
    expected_client_id: &str,
    forbidden_client_secret: &str,
) {
    let authorization_url = started["authorization_url"]
        .as_str()
        .expect("authorization URL");
    let parsed = url::Url::parse(authorization_url).expect("valid authorization URL");
    assert!(
        parsed
            .query_pairs()
            .any(|(name, value)| name == "client_id" && value == expected_client_id),
        "authorization URL must use client id {expected_client_id}: {authorization_url}"
    );
    assert!(
        !authorization_url.contains(forbidden_client_secret),
        "authorization URL must not expose the client secret"
    );
    assert!(
        !started.to_string().contains(forbidden_client_secret),
        "OAuth start response must not expose the client secret"
    );
}

fn authorization_state(started: &Value) -> String {
    let authorization_url = started["authorization_url"]
        .as_str()
        .expect("authorization URL");
    url::Url::parse(authorization_url)
        .expect("valid authorization URL")
        .query_pairs()
        .find_map(|(name, value)| (name == "state").then(|| value.into_owned()))
        .expect("authorization state")
}

async fn complete_gmail_oauth(router: &axum::Router, started: &Value) {
    let state = authorization_state(started);
    let mut callback =
        url::Url::parse("http://localhost/api/reborn/product-auth/oauth/google/callback")
            .expect("callback URL");
    callback
        .query_pairs_mut()
        .append_pair("state", &state)
        .append_pair("code", "google-authorization-code")
        .append_pair("scope", &gmail_scopes());
    let uri = callback[url::Position::BeforePath..].to_string();
    let mut request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .expect("valid OAuth callback request");
    request.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:41000"
            .parse::<std::net::SocketAddr>()
            .expect("valid callback peer"),
    ));
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("complete Gmail OAuth request");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "OAuth callback failed: {}",
        String::from_utf8_lossy(
            &to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("bounded callback error response")
        )
    );
}

#[tokio::test]
async fn webui_admin_configuration_overrides_boot_pair_and_rotates_without_restart() {
    let harness = build_harness().await;
    install_gmail(&harness.router).await;

    let boot = start_gmail_oauth(&harness.router).await;
    assert_authorization_client(
        &boot,
        "boot.apps.googleusercontent.com",
        "boot-google-secret",
    );

    save_google_admin_configuration(
        &harness.router,
        "admin.apps.googleusercontent.com",
        "admin-google-secret",
        0,
    )
    .await;
    let admin = start_gmail_oauth(&harness.router).await;
    assert_authorization_client(
        &admin,
        "admin.apps.googleusercontent.com",
        "admin-google-secret",
    );

    save_google_admin_configuration(
        &harness.router,
        "rotated.apps.googleusercontent.com",
        "rotated-google-secret",
        1,
    )
    .await;
    complete_gmail_oauth(&harness.router, &admin).await;

    let rotated = start_gmail_oauth(&harness.router).await;
    assert_authorization_client(
        &rotated,
        "rotated.apps.googleusercontent.com",
        "rotated-google-secret",
    );
    complete_gmail_oauth(&harness.router, &rotated).await;

    let forms = harness.token_egress.forms();
    assert_eq!(forms.len(), 2);
    for (form, expected_id, expected_secret) in [
        (
            &forms[0],
            "admin.apps.googleusercontent.com",
            "admin-google-secret",
        ),
        (
            &forms[1],
            "rotated.apps.googleusercontent.com",
            "rotated-google-secret",
        ),
    ] {
        assert_eq!(form.get("client_id").map(String::as_str), Some(expected_id));
        assert_eq!(
            form.get("client_secret").map(String::as_str),
            Some(expected_secret)
        );
    }

    harness
        .runtime
        .shutdown()
        .await
        .expect("runtime shutdown clean");
}

#[tokio::test]
async fn admin_configuration_lookup_failure_is_retryable_and_never_uses_boot_credentials() {
    let harness = build_harness().await;
    install_gmail(&harness.router).await;
    save_google_admin_configuration(
        &harness.router,
        "admin.apps.googleusercontent.com",
        "admin-google-secret",
        0,
    )
    .await;

    let record_path = VirtualPath::new(format!(
        "/tenants/{TENANT}/shared/extension-admin-configuration/groups/vendor.google.json"
    ))
    .expect("valid administrator configuration record path");
    let filesystem = harness
        .runtime
        .admin_configuration_root_filesystem_for_test();
    filesystem
        .put(
            &record_path,
            Entry::bytes(b"{invalid-administrator-record".to_vec()),
            CasExpectation::Any,
        )
        .await
        .expect("corrupt administrator configuration fixture");

    let response = start_gmail_oauth_response(&harness.router).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = response_json(response).await;
    assert_eq!(body["code"], "backend_unavailable");
    assert_eq!(body["retryable"], true);
    let serialized = body.to_string();
    assert!(!serialized.contains("boot.apps.googleusercontent.com"));
    assert!(!serialized.contains("boot-google-secret"));
    assert!(!serialized.contains("admin-google-secret"));

    harness
        .runtime
        .shutdown()
        .await
        .expect("runtime shutdown clean");
}
