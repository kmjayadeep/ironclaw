//! Pairing vocabulary shared by the ingress sink and the pairing service.
//!
//! The service that produces these outcomes (CAS claim → identity bind →
//! completion fan-out) stays in composition; only the outcome vocabulary the
//! generic sink matches on lives here.

use ironclaw_host_api::UserId;

/// What consuming a pairing code resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelPairingConsumeOutcome {
    Paired { user_id: UserId },
    AlreadyPairedSameUser { user_id: UserId },
    AlreadyBoundToOtherUser,
    ExpiredOrUnknown,
}
