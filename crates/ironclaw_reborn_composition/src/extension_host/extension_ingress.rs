//! Host HTTP route mount for the generic channel ingress router.
//!
//! The router, registry, and inbound sink live in
//! `ironclaw_extension_host::ingress` — that crate is deliberately
//! transport-neutral. This module is the axum half: the one
//! `PublicRouteMount` serving
//! `/webhooks/extensions/{extension_id}/{route_suffix}` for every active
//! extension, with route resolution per request through the snapshot watch so
//! activations/removals need no HTTP-server rebuild.

use std::collections::BTreeSet;
use std::sync::Arc;

use ironclaw_extension_host::ingress::ExtensionIngressRouter;
use ironclaw_extension_host::ingress::sink::{ExtensionIngressParts, ExtensionIngressRegistry};

/// Fixed host route paths inside the extension ingress namespace
/// (`/webhooks/extensions/…`). An extension whose canonical route collides
/// with one of these fails activation (`SnapshotConflict::ReservedRoute`).
///
/// Empty today: no fixed host route lives under the extension namespace, and
/// legacy fixed webhook paths cannot collide with a canonical extension path
/// by construction. Any future fixed mount under `/webhooks/extensions/` MUST
/// be added here in the same change that mounts it.
pub(crate) fn reserved_fixed_ingress_routes() -> BTreeSet<String> {
    BTreeSet::new()
}

pub use serve_mount::{EXTENSION_INGRESS_ROUTE_PATTERN, extension_ingress_route_mount};

mod serve_mount {
    use std::num::{NonZeroU32, NonZeroU64};
    use std::pin::Pin;

    use axum::{
        Router,
        body::Bytes,
        extract::{Path, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
    };
    use ironclaw_extension_host::ingress::{IngressRequest, IngressResponse};
    use ironclaw_host_api::NetworkMethod;
    use ironclaw_host_api::ingress::{
        AllowedEffectPath, AuditTraceClass, BodyLimitPolicy, CorsPolicy, IngressAuthPolicy,
        IngressAuthScheme, IngressPolicy, IngressPolicyParts, IngressRouteDescriptor,
        IngressScopeSource, ListenerClass, RateLimitPolicy, RateLimitScope, StreamingMode,
        WebSocketOriginPolicy,
    };

    use super::*;
    use ironclaw_host_ingress::{PublicRouteDrain, PublicRouteMount};

    /// The canonical generic ingress route pattern (axum path params).
    pub const EXTENSION_INGRESS_ROUTE_PATTERN: &str =
        "/webhooks/extensions/{extension_id}/{route_suffix}";

    const EXTENSION_INGRESS_ROUTE_ID: &str = "extensions.channel_ingress";

    /// Host ceiling for any extension channel body (per-extension limits from
    /// the channel descriptor are enforced inside the router, and are
    /// expected to be at or below this).
    const EXTENSION_INGRESS_BODY_CEILING_BYTES: u64 = 8 * 1024 * 1024;

    /// Host policy floor for public webhook ingress (mirrors the previous
    /// per-channel mounts). Compile-time non-zero.
    const PUBLIC_WEBHOOK_MAX_REQUESTS: NonZeroU32 = match NonZeroU32::new(12_000) {
        Some(value) => value,
        None => unreachable!(),
    };
    const PUBLIC_WEBHOOK_WINDOW_SECONDS: NonZeroU32 = match NonZeroU32::new(60) {
        Some(value) => value,
        None => unreachable!(),
    };

    /// Build the single `PublicRouteMount` serving every extension channel's
    /// ingress. Mounted once; route resolution follows deployment bindings
    /// first and active snapshot bindings second.
    pub fn extension_ingress_route_mount(
        parts: &ExtensionIngressParts,
    ) -> Result<PublicRouteMount, crate::RebornBuildError> {
        let descriptor =
            ingress_route_descriptor(EXTENSION_INGRESS_ROUTE_ID, EXTENSION_INGRESS_ROUTE_PATTERN)?;

        let router = Router::new()
            .route(EXTENSION_INGRESS_ROUTE_PATTERN, post(ingress_handler))
            .with_state(Arc::clone(&parts.router));
        Ok(
            PublicRouteMount::new(router, vec![descriptor]).with_drain(Arc::new(RegistryDrain {
                registry: Arc::clone(&parts.registry),
            })),
        )
    }

    fn ingress_route_descriptor(
        route_id: &'static str,
        path: &'static str,
    ) -> Result<IngressRouteDescriptor, crate::RebornBuildError> {
        let policy = IngressPolicy::new(IngressPolicyParts {
            listener_class: ListenerClass::PublicWebhook,
            auth: IngressAuthPolicy::Required {
                schemes: vec![IngressAuthScheme::WebhookSignature],
            },
            scope_source: IngressScopeSource::HostResolved,
            body_limit: BodyLimitPolicy::Limited {
                max_bytes: NonZeroU64::new(EXTENSION_INGRESS_BODY_CEILING_BYTES)
                    .unwrap_or(NonZeroU64::MIN),
            },
            rate_limit: RateLimitPolicy::Limited {
                scope: RateLimitScope::Global,
                max_requests: PUBLIC_WEBHOOK_MAX_REQUESTS,
                window_seconds: PUBLIC_WEBHOOK_WINDOW_SECONDS,
            },
            cors: CorsPolicy::NotApplicable,
            websocket_origin: WebSocketOriginPolicy::NotApplicable,
            streaming: StreamingMode::None,
            audit: AuditTraceClass::PublicCallback,
            effect_path: AllowedEffectPath::ProductSurface,
        })
        .map_err(|error| crate::RebornBuildError::InvalidConfig {
            reason: format!("extension ingress policy invalid: {error}"),
        })?;
        IngressRouteDescriptor::new(route_id, NetworkMethod::Post, path, policy).map_err(|error| {
            crate::RebornBuildError::InvalidConfig {
                reason: format!("extension ingress descriptor invalid: {error}"),
            }
        })
    }

    struct RegistryDrain {
        registry: Arc<ExtensionIngressRegistry>,
    }

    impl PublicRouteDrain for RegistryDrain {
        fn drain<'a>(&'a self) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
            Box::pin(self.registry.drain())
        }
    }

    async fn ingress_handler(
        State(router): State<Arc<ExtensionIngressRouter>>,
        Path((extension_id, route_suffix)): Path<(String, String)>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let response = router
            .handle(ingress_request(
                "POST",
                extension_id,
                route_suffix,
                &headers,
                body,
            ))
            .await;
        into_axum_response(response)
    }

    fn ingress_request(
        method: &str,
        extension_id: String,
        route_suffix: String,
        headers: &HeaderMap,
        body: Bytes,
    ) -> IngressRequest {
        IngressRequest {
            method: method.to_string(),
            extension_id,
            route_suffix,
            headers: headers
                .iter()
                .map(|(name, value)| (name.as_str().to_string(), value.as_bytes().to_vec()))
                .collect(),
            body: body.to_vec(),
        }
    }

    fn into_axum_response(response: IngressResponse) -> Response {
        let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::BAD_GATEWAY);
        match response.content_type {
            Some(content_type) => {
                (status, [("content-type", content_type)], response.body).into_response()
            }
            None => (status, response.body).into_response(),
        }
    }
}
