//! The authenticated intent-detail read (attested-signing Phase C §C3).
//!
//! `GET /api/webchat/v2/intents/{intent_id}` — what the `/review/:intentId`
//! SPA page calls once the public link has redirected it there and the session
//! layer has established who is asking.
//!
//! ## The token showed nothing; this is where authorization happens
//!
//! The public `/intent/{token}` route only turns a token into a redirect. It
//! reveals no transaction detail, because a link forwarded into a group chat
//! would otherwise expose one. Every authorization for this flow lands here,
//! against a session, in
//! [`ironclaw_attestation::authorize_view`]: the session user must equal the
//! intent's bound approver AND the session tenant must equal the intent's
//! tenant. A token holder who is not the approver gets exactly what a stranger
//! gets.
//!
//! ## Uniform 404, same as the public route
//!
//! Unknown id, wrong tenant, wrong user, expired, and backend failure are one
//! response. Anything else turns this into an oracle for which intent ids
//! exist and who their approvers are — and an authenticated attacker probing
//! ids is exactly the caller this endpoint has to assume.
//!
//! ## What the DTO deliberately omits
//!
//! The signature, the review-token hash, and the agent key id never leave the
//! server. The page renders a transaction for a human to compare against their
//! device screen; none of those three help with that, and each is material an
//! attacker would rather have. `approved_tx_hash` IS included: it is the value
//! the Ledger will display, so the human needs it to compare.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};

use ironclaw_attestation::{IntentId, IntentRecord, IntentStore, ReviewCaller, authorize_view};
use ironclaw_host_api::NetworkMethod;
use ironclaw_host_api::ingress::{
    AllowedEffectPath, AuditTraceClass, BodyLimitPolicy, CorsPolicy, IngressAuthPolicy,
    IngressAuthScheme, IngressPolicy, IngressPolicyParts, IngressRouteDescriptor,
    IngressScopeSource, ListenerClass, RateLimitPolicy, RateLimitScope, StreamingMode,
    WebSocketOriginPolicy,
};
use ironclaw_product_workflow::WebUiAuthenticatedCaller;
use ironclaw_signing_provider::{TenantId as SigningTenantId, UserId as SigningUserId};
use serde::Serialize;

use crate::webui::route_mounts::ProtectedRouteMount;

/// The detail path. `{intent_id}` is a placeholder in the route id.
pub(crate) const INTENT_DETAIL_PATH: &str = "/api/webchat/v2/intents/{intent_id}";

/// Per-caller ceiling. Generous for a human reading one page, tight enough that
/// an authenticated session cannot sweep the id space quickly.
const INTENT_DETAIL_MAX_REQUESTS: std::num::NonZero<u32> =
    std::num::NonZero::new(60).expect("nonzero literal"); // safety: const-evaluated — a zero literal fails the build, never runtime
const INTENT_DETAIL_RATE_WINDOW_SECONDS: std::num::NonZero<u32> =
    std::num::NonZero::new(60).expect("nonzero literal"); // safety: const-evaluated — a zero literal fails the build, never runtime

#[derive(Clone)]
struct IntentDetailState {
    intents: Arc<dyn IntentStore>,
}

/// What the review page renders.
///
/// Sanitized by construction: there is no field here the server would have to
/// remember to strip. See the module note on what is omitted and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct IntentDetailDto {
    /// The intent's id, echoed so the page can assert it got what it asked for.
    pub intent_id: String,
    /// Lifecycle projection: `pending`, `approved`, `rejected`, `expired`.
    pub state: String,
    /// CAIP-2 chain id, so the page can name the network.
    pub chain_id: String,
    /// The hash the device will display. The human compares this against their
    /// Ledger screen, so it is the one credential-shaped value that belongs
    /// here.
    pub approved_tx_hash: String,
    /// When the approval window closes (unix millis), for the countdown.
    pub expires_at_ms: i64,
    /// The decoded transaction, exactly as the authoritative decode produced
    /// it. The page renders it; it does not re-derive anything from it.
    pub decoded_tx: serde_json::Value,
}

/// Build the protected detail mount.
pub(crate) fn intent_detail_mount(intents: Arc<dyn IntentStore>) -> ProtectedRouteMount {
    let router = Router::new()
        .route(INTENT_DETAIL_PATH, get(handle_intent_detail))
        .with_state(IntentDetailState { intents });
    ProtectedRouteMount::new(router, vec![intent_detail_descriptor()])
}

/// Resolve an intent for its bound approver, or a uniform 404.
async fn handle_intent_detail(
    State(state): State<IntentDetailState>,
    Extension(caller): Extension<WebUiAuthenticatedCaller>,
    Path(intent_id): Path<String>,
) -> Response {
    let intent_id = IntentId::from_string(intent_id);
    // The session carries host-api identities; the attestation stores speak the
    // signing-provider newtypes. Bridge once, here, rather than letting either
    // spelling leak into the other layer.
    let tenant = SigningTenantId::new(caller.tenant_id.as_str());
    let user = SigningUserId::new(caller.user_id.as_str());

    // Tenant-qualified at the store, then authorized against the session. The
    // store read alone is not the authorization: it proves the intent belongs
    // to the caller's tenant, not that the caller is its approver.
    let record = match state.intents.get(&tenant, &intent_id).await {
        Ok(record) => record,
        Err(error) => {
            tracing::debug!(%error, "intent detail lookup did not resolve");
            return not_found();
        }
    };

    let caller = ReviewCaller {
        user: &user,
        tenant: &tenant,
    };
    match authorize_view(&record, caller, now_unix_millis()) {
        Ok(record) => match detail_of(record) {
            Ok(dto) => (StatusCode::OK, Json(dto)).into_response(),
            Err(error) => {
                tracing::debug!(%error, "intent detail did not serialize");
                not_found()
            }
        },
        // Wrong tenant, wrong user, and expired are one answer.
        Err(_) => not_found(),
    }
}

/// Project a record onto the DTO.
fn detail_of(record: &IntentRecord) -> Result<IntentDetailDto, serde_json::Error> {
    let intent = record.intent.intent();
    Ok(IntentDetailDto {
        intent_id: intent.intent_id.as_str().to_string(),
        state: state_str(record.state).to_string(),
        chain_id: intent.chain_id.as_str().to_string(),
        approved_tx_hash: hex::encode(intent.approved_tx_hash.as_bytes()),
        expires_at_ms: intent.expires_at_ms,
        decoded_tx: serde_json::to_value(&intent.decoded_tx)?,
    })
}

fn state_str(state: ironclaw_attestation::IntentState) -> &'static str {
    use ironclaw_attestation::IntentState;
    match state {
        IntentState::Pending => "pending",
        IntentState::Approved => "approved",
        IntentState::Rejected => "rejected",
        IntentState::Expired => "expired",
    }
}

/// The single refusal shape, carrying no body.
fn not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

fn now_unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn intent_detail_descriptor() -> IngressRouteDescriptor {
    let policy = IngressPolicy::new(IngressPolicyParts {
        listener_class: ListenerClass::LocalGateway,
        auth: IngressAuthPolicy::Required {
            schemes: vec![IngressAuthScheme::BearerToken],
        },
        scope_source: IngressScopeSource::AuthenticatedCaller,
        // A GET with no body.
        body_limit: BodyLimitPolicy::NoBody,
        rate_limit: RateLimitPolicy::Limited {
            scope: RateLimitScope::PerCaller,
            max_requests: INTENT_DETAIL_MAX_REQUESTS,
            window_seconds: INTENT_DETAIL_RATE_WINDOW_SECONDS,
        },
        cors: CorsPolicy::SameOriginOnly,
        websocket_origin: WebSocketOriginPolicy::NotApplicable,
        streaming: StreamingMode::None,
        // A human opened their review page.
        audit: AuditTraceClass::UserAction,
        // A read: it resolves nothing and claims nothing.
        effect_path: AllowedEffectPath::NoEffect,
    })
    .expect("intent detail policy must validate"); // safety: local-gateway + bearer auth + NoEffect + no-body is a validated shape.
    IngressRouteDescriptor::new(
        "webui.v2.intent_detail".to_string(),
        NetworkMethod::Get,
        INTENT_DETAIL_PATH.to_string(),
        policy,
    )
    .expect("intent detail route descriptor must validate at startup") // safety: id/pattern are crate-local literals; the policy comes from the helper above.
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use ironclaw_attestation::{
        AgentKeyId, DecodedTransaction, EvmAddress, EvmTransaction, INTENT_SIGNATURE_LEN,
        InMemoryIntentStore, IntentRecord, IntentState, RenderingSchemaVersion, ReviewTokenHash,
        UnsignedIntent,
    };
    use ironclaw_signing_provider::{ApprovedTxHash, ChainId, GateRef, TenantId, UserId};
    use tower::ServiceExt as _;

    const ID: &str = "01J0000000000000000000DETAIL";

    fn record(
        tenant: &str,
        approver: &str,
        state: IntentState,
        expires_at_ms: i64,
    ) -> IntentRecord {
        let intent = UnsignedIntent {
            intent_id: IntentId::from_string(ID),
            tenant: TenantId::new(tenant),
            agent_key_id: AgentKeyId::new(TenantId::new(tenant), "agent-1", 1),
            approver: UserId::new(approver),
            chain_id: ChainId::new("eip155:11155111"),
            approved_tx_hash: ApprovedTxHash::from_bytes([0x77; 32]),
            decoded_tx: DecodedTransaction::Evm(EvmTransaction {
                chain_id: 11155111,
                nonce: 7,
                tx_type: 2,
                to: Some(EvmAddress([0x99; 20])),
                value: vec![],
                data: vec![],
                gas_limit: 21_000,
                gas_price: None,
                max_fee_per_gas: Some(vec![0x09]),
                max_priority_fee_per_gas: Some(vec![0x3b]),
                access_list: vec![],
                max_fee_per_blob_gas: None,
                blob_versioned_hashes: vec![],
            }),
            created_at_ms: 0,
            expires_at_ms,
            schema_version: RenderingSchemaVersion::CURRENT,
        };
        let mut record = IntentRecord::pending(
            intent.into_signed([0xEE; INTENT_SIGNATURE_LEN]),
            GateRef::new("gate:attested-detail"),
            ReviewTokenHash::of_token("a-secret-review-token"),
        );
        record.state = state;
        record
    }

    /// Far enough out that the wall clock inside the handler cannot expire it.
    fn far_future() -> i64 {
        now_unix_millis() + 86_400_000
    }

    async fn get_as(store: Arc<dyn IntentStore>, tenant: &str, user: &str, id: &str) -> Response {
        // Host-api identities: this is the session shape the auth layer
        // installs, which is exactly what the handler has to bridge.
        let caller = WebUiAuthenticatedCaller::new(
            ironclaw_host_api::TenantId::new(tenant).expect("test tenant id"),
            ironclaw_host_api::UserId::new(user).expect("test user id"),
            None,
            None,
        );
        let request = Request::builder()
            .uri(format!("/api/webchat/v2/intents/{id}"))
            .extension(caller)
            .body(Body::empty())
            .expect("request");
        intent_detail_mount(store)
            .router
            .oneshot(request)
            .await
            .expect("response")
    }

    async fn store_with(record: IntentRecord) -> Arc<dyn IntentStore> {
        let store = InMemoryIntentStore::new();
        store.put(record).await.expect("put");
        Arc::new(store)
    }

    async fn body_of(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("json")
    }

    #[tokio::test]
    async fn the_bound_approver_sees_the_transaction() {
        let store = store_with(record(
            "tenant-a",
            "alice",
            IntentState::Pending,
            far_future(),
        ))
        .await;
        let response = get_as(store, "tenant-a", "alice", ID).await;
        assert_eq!(response.status(), StatusCode::OK);

        let body = body_of(response).await;
        assert_eq!(body["intent_id"], ID);
        assert_eq!(body["state"], "pending");
        assert_eq!(body["chain_id"], "eip155:11155111");
        // The hash the device will show, so the human can compare it.
        assert_eq!(body["approved_tx_hash"], "77".repeat(32));
        assert_eq!(body["decoded_tx"]["nonce"], 7);
    }

    /// The whole point of Q4: holding the link is not being the approver.
    #[tokio::test]
    async fn a_different_user_in_the_same_tenant_gets_the_uniform_404() {
        let store = store_with(record(
            "tenant-a",
            "alice",
            IntentState::Pending,
            far_future(),
        ))
        .await;
        let response = get_as(store, "tenant-a", "mallory", ID).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_cross_tenant_caller_gets_the_uniform_404() {
        let store = store_with(record(
            "tenant-a",
            "alice",
            IntentState::Pending,
            far_future(),
        ))
        .await;
        // Same user name under another tenant is a different principal.
        let response = get_as(store, "tenant-b", "alice", ID).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    /// Unknown id, wrong user, wrong tenant, and expired must be ONE response —
    /// otherwise an authenticated caller can probe which ids exist and who
    /// approves them.
    #[tokio::test]
    async fn every_refusal_is_indistinguishable() {
        let live = record("tenant-a", "alice", IntentState::Pending, far_future());
        let expired = record("tenant-a", "alice", IntentState::Pending, 1);

        let cases: Vec<(&str, Response)> = vec![
            (
                "unknown id",
                get_as(
                    store_with(live.clone()).await,
                    "tenant-a",
                    "alice",
                    "01J000000000000000000ABSENT",
                )
                .await,
            ),
            (
                "wrong user",
                get_as(store_with(live.clone()).await, "tenant-a", "mallory", ID).await,
            ),
            (
                "wrong tenant",
                get_as(store_with(live).await, "tenant-b", "alice", ID).await,
            ),
            (
                "expired",
                get_as(store_with(expired).await, "tenant-a", "alice", ID).await,
            ),
        ];

        for (label, response) in cases {
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{label} must be the uniform refusal"
            );
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("body");
            assert!(
                bytes.is_empty(),
                "{label} must carry no body to distinguish it"
            );
        }
    }

    /// The DTO is sanitized by construction. If a future field lands on it,
    /// this fails and forces the question to be answered deliberately.
    #[tokio::test]
    async fn the_response_never_carries_the_signature_token_hash_or_key_id() {
        let store = store_with(record(
            "tenant-a",
            "alice",
            IntentState::Pending,
            far_future(),
        ))
        .await;
        let body = body_of(get_as(store, "tenant-a", "alice", ID).await).await;

        let rendered = body.to_string();
        assert!(
            !rendered.contains(&"ee".repeat(32)),
            "the intent signature must not reach the page"
        );
        assert!(
            !rendered.contains("review_token"),
            "the review token hash must not reach the page"
        );
        assert!(
            !rendered.contains("agent_key_id") && !rendered.contains("agent-1"),
            "the agent key id must not reach the page"
        );

        let object = body.as_object().expect("a JSON object");
        let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "approved_tx_hash",
                "chain_id",
                "decoded_tx",
                "expires_at_ms",
                "intent_id",
                "state"
            ],
            "a new field on the review DTO must be a deliberate decision"
        );
    }

    /// A terminal intent still renders — the approver who just signed should
    /// see the outcome rather than a 404. (Only `authorize_proof_submission`
    /// refuses terminal states; viewing is not submitting.)
    #[tokio::test]
    async fn a_resolved_intent_still_renders_its_outcome() {
        let store = store_with(record(
            "tenant-a",
            "alice",
            IntentState::Approved,
            far_future(),
        ))
        .await;
        let response = get_as(store, "tenant-a", "alice", ID).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_of(response).await["state"], "approved");
    }
}
