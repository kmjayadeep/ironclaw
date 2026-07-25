//! The production command admission policy.
//!
//! Commands execute with the bound user's authority, so admission is
//! deliberately conservative for the first shipped slice:
//!
//! - **Direct conversations only.** A command typed in a shared/group route is
//!   rejected; group command policy is a future, separate decision.
//! - **Manifest-declared commands only.** The generic sink already classifies
//!   only declared commands; this check is defense in depth for command
//!   payloads that enter the workflow by other paths (e.g. a synthetic
//!   envelope), so an undeclared command can never execute.
//!
//! Actor pairing is enforced structurally upstream and downstream, not
//! re-checked here: pairing-gated channels intercept unbound actors at the
//! sink before classification, and command dispatch resolves the conversation
//! binding (fail-closed) before any operation executes.

use std::collections::BTreeSet;

use async_trait::async_trait;
use ironclaw_host_api::ProductSurfaceError;

use crate::binding::route_kind_for_trigger;
use crate::command_dispatch::{
    ProductCommandAdmission, ProductCommandAdmissionService, ProductCommandContext,
};
use crate::commands::ProductCommand;
use crate::{ProductConversationRouteKind, ProductRejection, ProductRejectionKind};

/// Admission policy for one extension's channel surface: direct conversations
/// only, manifest-declared commands only.
pub struct PairedDmCommandAdmission {
    declared: BTreeSet<String>,
}

impl PairedDmCommandAdmission {
    pub fn new(declared: impl IntoIterator<Item = String>) -> Self {
        Self {
            declared: declared.into_iter().collect(),
        }
    }
}

#[async_trait]
impl ProductCommandAdmissionService for PairedDmCommandAdmission {
    async fn admit(
        &self,
        context: &ProductCommandContext,
        command: &ProductCommand,
    ) -> Result<ProductCommandAdmission, ProductSurfaceError> {
        if route_kind_for_trigger(context.trigger) != ProductConversationRouteKind::Direct {
            return Ok(ProductCommandAdmission::Rejected(
                ProductRejection::permanent(
                    ProductRejectionKind::PolicyDenied,
                    "commands are limited to direct conversations",
                ),
            ));
        }
        let canonical = command
            .descriptor()
            .map(|descriptor| descriptor.name.to_string())
            .unwrap_or_else(|| command.name().to_string());
        if !self.declared.contains(&canonical) {
            // InvalidRequest: user-correctable; downstream feedback composes
            // the inventory help exactly like the unknown-command reply.
            return Ok(ProductCommandAdmission::Rejected(
                ProductRejection::permanent(
                    ProductRejectionKind::InvalidRequest,
                    format!("command not declared by this channel: {canonical}"),
                ),
            ));
        }
        Ok(ProductCommandAdmission::Allowed)
    }
}
