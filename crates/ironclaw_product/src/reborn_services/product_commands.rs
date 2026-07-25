//! Product slash-command facade surface: the WebUI-facing inventory/execute
//! methods and the shared command-result presentation helpers. Split from the
//! parent per the submodule precedent (`llm_config.rs`, `log_views.rs`).

use ironclaw_host_api::{ProductSurfaceCaller, ProductSurfaceError, ProductSurfaceErrorCode};

use super::{
    ProductCapabilityInvoker, RebornGetRunStateRequest, RebornServices, RebornViewProvider,
    llm_config, parse_thread_id_field,
};
use crate::commands::{CommandResultField, CommandResultView, ProductStatusCommandInput};
use crate::{
    ProductModelCommand, RebornCommandRejection, RebornExecuteProductCommandRequest,
    RebornExecuteProductCommandResponse, RebornProductCommandInfo,
    RebornProductCommandListResponse,
};
use ironclaw_turns::TurnStatus;

impl<I, V> RebornServices<I, V>
where
    I: ProductCapabilityInvoker + Clone + 'static,
    V: RebornViewProvider + Clone + 'static,
{
    pub(super) async fn execute_product_model_command(
        &self,
        caller: ProductSurfaceCaller,
        action: ProductModelCommand,
    ) -> Result<CommandResultView, ProductSurfaceError> {
        match action {
            ProductModelCommand::Status => {
                let snapshot = self.build_llm_config_view(caller).await?;
                Ok(model_command_view("Model", &snapshot))
            }
            ProductModelCommand::Set { model } => {
                let snapshot = self.build_llm_config_view(caller.clone()).await?;
                let provider_id = snapshot
                    .active
                    .map(|active| active.provider_id)
                    .ok_or_else(llm_config::llm_config_unavailable)?;
                self.invoke_llm_active_set(
                    caller.clone(),
                    serde_json::json!({
                        "provider_id": provider_id,
                        "model": model,
                    }),
                )
                .await?;
                let snapshot = self.build_llm_config_view(caller).await?;
                Ok(model_command_view("Model updated", &snapshot))
            }
            ProductModelCommand::SetProvider { provider, model } => {
                self.invoke_llm_active_set(
                    caller.clone(),
                    serde_json::json!({
                        "provider_id": provider,
                        "model": model,
                    }),
                )
                .await?;
                let snapshot = self.build_llm_config_view(caller).await?;
                Ok(model_command_view("Model updated", &snapshot))
            }
        }
    }

    /// `/status` — report the conversation's current assistant activity from
    /// the thread's latest run-linked message plus canonical run state.
    pub(super) async fn execute_product_status_command(
        &self,
        caller: ProductSurfaceCaller,
        input: ProductStatusCommandInput,
    ) -> Result<CommandResultView, ProductSurfaceError> {
        let thread_id = parse_thread_id_field("thread_id", input.thread_id)?;
        let scope = caller.turn_scope(thread_id.clone());
        // Reuses the caller-ownership probe (with the automation-trigger
        // fallback) exactly like the timeline read; browsers and channels
        // cannot status-probe threads they cannot read.
        let (_thread_scope, history) = self
            .resolve_thread_history_for_caller(caller.clone(), &scope)
            .await?;
        let latest_run = history
            .messages
            .iter()
            .rev()
            .find_map(|message| message.turn_run_id.clone());
        let Some(run_id) = latest_run else {
            return Ok(CommandResultView {
                title: "Status".to_string(),
                fields: vec![command_result_field("State", "idle")],
                lines: vec!["No assistant activity in this conversation yet.".to_string()],
            });
        };
        let state = self
            .get_run_state(
                caller,
                RebornGetRunStateRequest {
                    thread_id: thread_id.to_string(),
                    run_id,
                },
            )
            .await?;
        let (state_label, detail) = describe_turn_status(state.status);
        let mut fields = vec![command_result_field("State", state_label)];
        fields.push(command_result_field("Run", state.run_id.to_string()));
        fields.push(command_result_field(
            "Since",
            state
                .received_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        ));
        let mut lines = Vec::new();
        if let Some(detail) = detail {
            lines.push(detail.to_string());
        }
        Ok(CommandResultView {
            title: "Status".to_string(),
            fields,
            lines,
        })
    }

    /// The full command inventory for the WebUI slash menu. The browser is
    /// the operator surface, so it sees every standardized command (channel
    /// surfaces are additionally gated by their manifest declarations).
    pub async fn list_product_commands(
        &self,
        _caller: ProductSurfaceCaller,
    ) -> Result<RebornProductCommandListResponse, ProductSurfaceError> {
        Ok(RebornProductCommandListResponse {
            commands: crate::commands::product_command_descriptors()
                .map(|descriptor| RebornProductCommandInfo {
                    name: descriptor.name.to_string(),
                    aliases: descriptor
                        .aliases
                        .iter()
                        .map(|alias| alias.to_string())
                        .collect(),
                    title: descriptor.title.to_string(),
                    description: descriptor.description.to_string(),
                    usage: descriptor.usage.to_string(),
                })
                .collect(),
        })
    }

    /// Execute composer slash text through the same shared parser, typed
    /// command model, and handlers the channel dispatch uses. Text that
    /// parses as a command but cannot execute (unknown name, unwired family)
    /// returns 200 with a user-safe rejection body (kind + inventory help)
    /// so the composer renders it like any command result; text that is not
    /// well-formed slash input at all is a 400 — the composer only submits
    /// inventory-matched slash text, so that is a client contract breach.
    pub async fn execute_product_command(
        &self,
        caller: ProductSurfaceCaller,
        request: RebornExecuteProductCommandRequest,
    ) -> Result<RebornExecuteProductCommandResponse, ProductSurfaceError> {
        let invalid = || {
            ProductSurfaceError::from_status(ProductSurfaceErrorCode::InvalidRequest, 400, false)
        };
        let payload = crate::parse_product_slash_command(
            &request.text,
            crate::ProductTriggerReason::DirectChat,
        )
        .map_err(|error| {
            tracing::debug!(%error, "composer slash text failed shared parsing");
            invalid()
        })?
        .ok_or_else(invalid)?;
        let command = match crate::commands::ProductCommand::from_payload(&payload) {
            Ok(command) => command,
            Err(rejection) => {
                tracing::debug!(kind = ?rejection.kind, "composer command parse rejected");
                return Ok(command_rejection_response(payload.command));
            }
        };
        let name = command.name().to_string();
        match command {
            crate::commands::ProductCommand::Model { action } => {
                let result = self.execute_product_model_command(caller, action).await?;
                Ok(RebornExecuteProductCommandResponse {
                    command: name,
                    result: Some(result),
                    rejection: None,
                })
            }
            crate::commands::ProductCommand::Status => {
                let result = self
                    .execute_product_status_command(
                        caller,
                        ProductStatusCommandInput {
                            thread_id: request.thread_id,
                        },
                    )
                    .await?;
                Ok(RebornExecuteProductCommandResponse {
                    command: name,
                    result: Some(result),
                    rejection: None,
                })
            }
            crate::commands::ProductCommand::Lifecycle { .. }
            | crate::commands::ProductCommand::Unknown { .. } => {
                Ok(command_rejection_response(name))
            }
        }
    }
}

/// User-safe rejection body for slash text that parsed but cannot execute on
/// this surface (unknown, malformed, or not-yet-wired command families).
fn command_rejection_response(command: impl Into<String>) -> RebornExecuteProductCommandResponse {
    RebornExecuteProductCommandResponse {
        command: command.into(),
        result: None,
        rejection: Some(RebornCommandRejection {
            kind: crate::ProductRejectionKind::InvalidRequest,
            message: crate::commands::command_unavailable_reply(),
        }),
    }
}

fn command_result_field(label: &str, value: impl Into<String>) -> CommandResultField {
    CommandResultField {
        label: label.to_string(),
        value: value.into(),
    }
}

/// Presentational summary of the merged LLM catalog for `/model` results.
fn model_command_view(title: &str, snapshot: &llm_config::LlmConfigSnapshot) -> CommandResultView {
    let mut fields = Vec::new();
    let mut lines = Vec::new();
    match &snapshot.active {
        Some(active) => {
            fields.push(command_result_field("Provider", active.provider_id.clone()));
            fields.push(command_result_field(
                "Model",
                active
                    .model
                    .clone()
                    .unwrap_or_else(|| "provider default".to_string()),
            ));
        }
        None => lines.push("No active model configured.".to_string()),
    }
    if !snapshot.providers.is_empty() {
        lines.push(format!(
            "Providers: {}",
            snapshot
                .providers
                .iter()
                .map(|provider| provider.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    CommandResultView {
        title: title.to_string(),
        fields,
        lines,
    }
}

/// User-facing label (+ optional hint) for a run's current status.
fn describe_turn_status(status: TurnStatus) -> (&'static str, Option<&'static str>) {
    match status {
        TurnStatus::Queued => ("queued", None),
        TurnStatus::Running => ("working", None),
        TurnStatus::BlockedApproval => (
            "waiting for approval",
            Some("Reply `approve` or `deny` to continue."),
        ),
        TurnStatus::BlockedAuth => (
            "waiting for authentication",
            Some("Complete the pending connection to continue."),
        ),
        TurnStatus::CancelRequested => ("cancelling", None),
        TurnStatus::Completed => ("idle", Some("The last task completed.")),
        TurnStatus::Failed => ("idle", Some("The last task failed.")),
        TurnStatus::Cancelled => ("idle", Some("The last task was cancelled.")),
        // Remaining blocked shapes (resource, dependent run, external tool)
        // and any future non-terminal status read as in-progress.
        _ => ("working", None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ironclaw_host_api::product_commands::PRODUCT_COMMANDS;

    /// The WebUI DTO is a sanctioned wire mirror of the canonical descriptor
    /// (see its doc comment). Pin the projection by serialized key set so a
    /// new descriptor column cannot silently fail to project — per-field
    /// equality would not catch an added column.
    #[test]
    fn descriptor_projection_covers_every_descriptor_column() {
        let descriptor = PRODUCT_COMMANDS
            .iter()
            .find(|descriptor| !descriptor.aliases.is_empty())
            .expect("an aliased descriptor exists (status/progress)");
        let descriptor_keys: std::collections::BTreeSet<String> = serde_json::to_value(descriptor)
            .expect("descriptor serializes")
            .as_object()
            .expect("descriptor is an object")
            .keys()
            .cloned()
            .collect();
        let info = RebornProductCommandInfo {
            name: descriptor.name.to_string(),
            aliases: descriptor
                .aliases
                .iter()
                .map(|alias| alias.to_string())
                .collect(),
            title: descriptor.title.to_string(),
            description: descriptor.description.to_string(),
            usage: descriptor.usage.to_string(),
        };
        let info_keys: std::collections::BTreeSet<String> = serde_json::to_value(&info)
            .expect("info serializes")
            .as_object()
            .expect("info is an object")
            .keys()
            .cloned()
            .collect();
        assert_eq!(
            descriptor_keys, info_keys,
            "descriptor and WebUI projection diverged — update RebornProductCommandInfo and list_product_commands"
        );
    }
}
