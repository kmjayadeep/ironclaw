use std::sync::atomic::{AtomicUsize, Ordering};

use ironclaw_host_api::UserId;
use ironclaw_host_api::{
    RestrictedEgress, RestrictedEgressError, RestrictedEgressRequest, RestrictedEgressResponse,
};
use ironclaw_product::{
    ChannelAdapter, ChannelAttachmentRef, ExternalActorRef, ExternalConversationRef,
    ExternalEventId, ParsedProductInbound, ProductAttachmentDescriptor, ProductAttachmentKind,
    ProductInboundPayload, ProductTriggerReason, TrustedInboundContext, UserMessagePayload,
};
use ironclaw_product::{ChannelInboundSurfaceAdmission, ChannelInboundSurfaceOutcome};
use ironclaw_turns::{AcceptedMessageRef, TurnRunId};

use super::*;

/// Records pairing outcomes for assertions. An ordinary double now that the
/// observer is a trait — it used to be a `#[cfg(test)]` variant compiled into
/// the production enum.
pub(crate) struct RecordingPairingOutcomeObserver {
    pub(crate) outcomes: Arc<std::sync::Mutex<Vec<ChannelPairingConsumeOutcome>>>,
}

#[async_trait]
impl ChannelPairingOutcomeObserver for RecordingPairingOutcomeObserver {
    async fn observe_pairing_outcome(
        &self,
        _conversation: ExternalConversationRef,
        _event_id: ExternalEventId,
        outcome: ChannelPairingConsumeOutcome,
    ) {
        match self.outcomes.lock() {
            Ok(mut outcomes) => outcomes.push(outcome),
            Err(poisoned) => poisoned.into_inner().push(outcome),
        }
    }
}
use crate::extension_host::channel_pairing::ChannelPairingConsumeOutcome;

struct CountingSurface {
    submissions: AtomicUsize,
    transfer_submissions: AtomicUsize,
}

impl CountingSurface {
    fn new() -> Self {
        Self {
            submissions: AtomicUsize::new(0),
            transfer_submissions: AtomicUsize::new(0),
        }
    }

    fn submit_count(&self) -> usize {
        self.submissions.load(Ordering::SeqCst)
    }

    fn transfer_submit_count(&self) -> usize {
        self.transfer_submissions.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ChannelInboundProductSurface for CountingSurface {
    async fn admit_channel_inbound(
        &self,
        request: ChannelInboundSurfaceRequest,
    ) -> ChannelInboundSurfaceOutcome {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        let payload = match request.classification {
            Some(classification) => classification.into(),
            None => ProductInboundPayload::UserMessage(
                UserMessagePayload::new(
                    request.message.text.clone(),
                    request
                        .message
                        .attachments
                        .iter()
                        .map(|attachment| attachment.descriptor.clone())
                        .collect(),
                    request.message.trigger,
                )
                .expect("user message payload"),
            ),
        };
        let ack = ProductInboundAck::Accepted {
            accepted_message_ref: AcceptedMessageRef::new("msg:extension-ingress-test")
                .expect("accepted message ref"),
            submitted_run_id: TurnRunId::new(),
        };
        let envelope = ProductInboundEnvelope::from_trusted_parse(
            TrustedInboundContext::from_verified_evidence_with_source_channel(
                request.adapter_id,
                request.source_channel,
                request.installation_id,
                request.received_at,
                &request.evidence,
            )
            .expect("verified evidence"),
            ParsedProductInbound::new(
                request.message.event_id,
                request.message.actor,
                request.message.conversation,
                payload,
            )
            .expect("parsed inbound"),
        )
        .expect("trusted envelope");
        ChannelInboundSurfaceOutcome::Admitted(Box::new(ChannelInboundSurfaceAdmission {
            envelope,
            ack,
        }))
    }

    async fn admit_channel_inbound_with_attachment_transfer(
        &self,
        request: ChannelInboundSurfaceRequest,
        _channel_adapter: Arc<dyn ChannelAdapter>,
        _channel_egress: Arc<dyn ironclaw_host_api::RestrictedEgress>,
    ) -> ChannelInboundSurfaceOutcome {
        self.transfer_submissions.fetch_add(1, Ordering::SeqCst);
        self.admit_channel_inbound(request).await
    }
}

/// A surface that panics on the bytes-free door and inherits the default
/// (fail-closed) attachment-transfer door.
struct DefaultingAttachmentSurface;

#[async_trait]
impl ChannelInboundProductSurface for DefaultingAttachmentSurface {
    async fn admit_channel_inbound(
        &self,
        _request: ChannelInboundSurfaceRequest,
    ) -> ChannelInboundSurfaceOutcome {
        panic!("attachment admission must use the channel-transfer entrypoint")
    }
}

struct TestRestrictedEgress;

#[async_trait]
impl RestrictedEgress for TestRestrictedEgress {
    async fn send(
        &self,
        _request: RestrictedEgressRequest,
    ) -> Result<RestrictedEgressResponse, RestrictedEgressError> {
        Err(RestrictedEgressError::PolicyDenied)
    }
}

struct ScriptedPairingInterceptor {
    interception: ChannelPairingInterception,
}

#[async_trait]
impl ChannelPairingInterceptor for ScriptedPairingInterceptor {
    async fn intercept(
        &self,
        _installation_id: &AdapterInstallationId,
        _message: &NormalizedInboundMessage,
    ) -> ChannelPairingInterception {
        self.interception.clone()
    }
}

fn admission_for(text: &str) -> InboundAdmission {
    InboundAdmission {
        extension_id: "vendorx".to_string(),
        installation_id: "install".to_string(),
        message: NormalizedInboundMessage {
            actor: ExternalActorRef::new("vendor_user", "user-1", None::<&str>).expect("actor"),
            conversation: ExternalConversationRef::new(None, "chat-1", None, None)
                .expect("conversation"),
            event_id: ExternalEventId::new("evt-1").expect("event"),
            text: text.to_string(),
            trigger: ProductTriggerReason::DirectChat,
            attachments: Vec::new(),
            reply_context: None,
        },
        channel_adapter: Arc::new(
            ironclaw_extension_host::test_support::FakeChannelAdapter::default(),
        ),
        channel_egress: None,
    }
}

fn admission_with_attachment() -> InboundAdmission {
    let mut admission = admission_for("review the attached report");
    admission.message.attachments.push(ChannelAttachmentRef {
        descriptor: ProductAttachmentDescriptor::new(
            "file-1",
            "application/pdf",
            Some("report.pdf".to_string()),
            Some(4),
            ProductAttachmentKind::Document,
        )
        .expect("attachment descriptor"),
        vendor_ref: "opaque-provider-file-reference".to_string(),
    });
    admission.channel_egress = Some(Arc::new(TestRestrictedEgress));
    admission
}

fn pairing_sink(
    interception: ChannelPairingInterception,
) -> (
    GenericChannelInboundSink,
    Arc<CountingSurface>,
    Arc<std::sync::Mutex<Vec<ChannelPairingConsumeOutcome>>>,
) {
    let workflow = Arc::new(CountingSurface::new());
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = GenericChannelInboundSink::new(ChannelInboundSinkConfig {
        adapter_id: ProductAdapterId::new("vendorx").expect("adapter id"),
        evidence: VerifiedEvidenceMint::SharedSecretHeader {
            header: "X-Vendor-Secret".to_string(),
        },
        classifier: None,
        surface: Arc::clone(&workflow) as Arc<dyn ChannelInboundProductSurface>,
        observer: None,
    })
    .with_pairing(
        Arc::new(ScriptedPairingInterceptor { interception }),
        Some(Arc::new(RecordingPairingOutcomeObserver {
            outcomes: Arc::clone(&outcomes),
        }) as Arc<dyn ChannelPairingOutcomeObserver>),
    );
    (sink, workflow, outcomes)
}

struct FailingSink;

#[async_trait]
impl InboundSink for FailingSink {
    async fn admit(
        &self,
        _admission: InboundAdmission,
    ) -> Result<InboundAdmissionAck, InboundSinkError> {
        Err(InboundSinkError {
            retryable: true,
            reason: "test sink".to_string(),
        })
    }
}

fn registration(secret: &[u8]) -> ChannelIngressRegistration {
    ChannelIngressRegistration {
        secrets: Arc::new(StaticIngressSecrets::new(vec![VerificationCandidate {
            installation_id: "install".to_string(),
            secret: secret.to_vec(),
        }])),
        sink: Arc::new(FailingSink),
        drain: None,
    }
}

async fn registered_secret(registry: &ExtensionIngressRegistry, extension_id: &str) -> Vec<u8> {
    registry
        .verification_candidates(extension_id, "install", None)
        .await
        .expect("registration present")
        .first()
        .expect("one candidate")
        .secret
        .clone()
}

#[tokio::test]
async fn managed_registration_never_replaces_a_lane_owned_entry() {
    let registry = ExtensionIngressRegistry::default();
    registry.register("vendorx", registration(b"lane"));

    assert!(matches!(
        registry.register_managed("vendorx", registration(b"managed")),
        ManagedRegistrationOutcome::SkippedUnmanaged
    ));
    assert_eq!(registered_secret(&registry, "vendorx").await, b"lane");
    assert!(
        registry.unregister_managed("vendorx").is_none(),
        "a lane-owned entry must survive managed unregistration"
    );
    assert!(registry.is_registered("vendorx"));
}

#[tokio::test]
async fn managed_registration_installs_replaces_and_unregisters_managed_entries() {
    let registry = ExtensionIngressRegistry::default();
    assert!(!registry.is_registered("vendorx"));

    let ManagedRegistrationOutcome::Registered { replaced } =
        registry.register_managed("vendorx", registration(b"one"))
    else {
        panic!("empty slot must accept a managed entry");
    };
    assert!(replaced.is_none());
    assert_eq!(registered_secret(&registry, "vendorx").await, b"one");

    let ManagedRegistrationOutcome::Registered { replaced } =
        registry.register_managed("vendorx", registration(b"two"))
    else {
        panic!("a managed entry must be replaceable by the assembly");
    };
    assert!(
        replaced.is_some(),
        "the replaced managed entry is returned for draining"
    );
    assert_eq!(registered_secret(&registry, "vendorx").await, b"two");

    assert!(registry.unregister_managed("vendorx").is_some());
    assert!(!registry.is_registered("vendorx"));
}

#[tokio::test]
async fn lane_registration_reclaims_a_managed_slot() {
    let registry = ExtensionIngressRegistry::default();
    let ManagedRegistrationOutcome::Registered { .. } =
        registry.register_managed("vendorx", registration(b"managed"))
    else {
        panic!("empty slot must accept a managed entry");
    };

    registry.register("vendorx", registration(b"lane"));
    assert_eq!(registered_secret(&registry, "vendorx").await, b"lane");
    assert!(matches!(
        registry.register_managed("vendorx", registration(b"managed-again")),
        ManagedRegistrationOutcome::SkippedUnmanaged
    ));
}

#[tokio::test]
async fn pairing_interception_preserves_every_typed_consume_outcome_for_the_observer() {
    let user_id = UserId::new("paired-user").expect("user id");
    for outcome in [
        ChannelPairingConsumeOutcome::Paired {
            user_id: user_id.clone(),
        },
        ChannelPairingConsumeOutcome::AlreadyPairedSameUser {
            user_id: user_id.clone(),
        },
        ChannelPairingConsumeOutcome::AlreadyBoundToOtherUser,
        ChannelPairingConsumeOutcome::ExpiredOrUnknown,
    ] {
        let (sink, workflow, observer) =
            pairing_sink(ChannelPairingInterception::Consumed(outcome.clone()));

        let ack = sink
            .admit(admission_for("ABCDEFGH"))
            .await
            .expect("admitted");
        assert_eq!(ack, InboundAdmissionAck::Accepted);
        sink.drain().await;
        assert_eq!(workflow.submit_count(), 0);
        assert_eq!(observer.lock().expect("outcomes lock").pop(), Some(outcome));
    }
}

#[tokio::test]
async fn pairing_not_handled_continues_to_the_product_surface() {
    let (sink, workflow, observer) = pairing_sink(ChannelPairingInterception::NotHandled);

    let ack = sink.admit(admission_for("hello")).await.expect("admitted");
    assert_eq!(ack, InboundAdmissionAck::Accepted);
    sink.drain().await;
    assert_eq!(workflow.submit_count(), 1);
    assert_eq!(observer.lock().expect("outcomes lock").pop(), None);
}

// Door selection on the happy path is covered end to end by the telegram
// journey in `tests/integration/extension_delivery.rs`: taking the
// bytes-free door there produces no vendor `getFile`/download traffic and
// no landed `/workspace/attachments/...` ref, which the journey asserts
// directly. A local duplicate would only re-assert the test double's own
// payload construction.
#[tokio::test]
async fn attachment_admission_without_channel_egress_is_retryable() {
    let (sink, workflow, _observer) = pairing_sink(ChannelPairingInterception::NotHandled);
    let mut admission = admission_with_attachment();
    admission.channel_egress = None;

    let error = sink
        .admit(admission)
        .await
        .expect_err("missing channel egress must not claim durable acceptance");

    assert!(error.retryable);
    assert_eq!(error.reason, "channel attachment egress is unavailable");
    assert_eq!(workflow.submit_count(), 0);
    assert_eq!(workflow.transfer_submit_count(), 0);
}

/// A surface that does not implement attachment transfer will not
/// implement it for a redelivery of the same message, so the inherited
/// default settles permanently. A retryable outcome here left the vendor
/// redelivering forever while the user received nothing at all — not even
/// the message text — and it disagreed with the equivalent default on the
/// inbound turn service, which was already permanent for this condition.
#[tokio::test]
async fn inherited_attachment_transfer_failure_settles_permanently() {
    let sink = GenericChannelInboundSink::new(ChannelInboundSinkConfig {
        adapter_id: ProductAdapterId::new("vendorx").expect("adapter id"),
        evidence: VerifiedEvidenceMint::SharedSecretHeader {
            header: "X-Vendor-Secret".to_string(),
        },
        classifier: None,
        surface: Arc::new(DefaultingAttachmentSurface),
        observer: None,
    });

    let error = sink
        .admit(admission_with_attachment())
        .await
        .expect_err("an inherited unsupported transfer must not be admitted");

    assert!(
        !error.retryable,
        "a structural transfer gap must not ask the vendor to redeliver"
    );
}
