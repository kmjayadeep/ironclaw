//! A provider with a manual-token user credential requirement (GitHub) must
//! raise the normal per-account `BlockedAuth` gate.
//!
//! Uses "github", not telegram: telegram is feature-available here, but
//! empirically (verified against this exact harness) its setup resolves
//! through a SEPARATE `ExtensionAccountSetupRegistry`/pairing mechanism that
//! needs a live `AccountConnectionStatusSource` this bare harness never
//! mounts (`telegram/telegram_host_beta.rs`'s `connect()` call is a
//! production/serve-time wiring step) — so an unseeded install here hits
//! a pre-existing, unrelated "host unavailable" error instead of the
//! per-account credential gate this test targets, and would misrepresent the
//! contract under test. github resolves through the SAME generic
//! product-auth credential-account mechanism as google/notion (the
//! mechanism `scenario_extension_install_reauth_gate` already proves
//! raises a real `BlockedAuth` gate for an unsatisfied requirement), so it
//! is the in-catalog manual-token provider used by this scenario.
//! Runs on github's existing setup-needed install from Scenario 1, never
//! credentialed there—no fresh install, mirroring how Scenario 7
//! reconciles the same pre-existing install.

use super::reborn_support::group::{HarnessResult, RebornIntegrationGroup};
use super::reborn_support::reply::RebornScriptedReply;

pub async fn run(_g: &RebornIntegrationGroup) -> HarnessResult<()> {
    let isolated = RebornIntegrationGroup::extension_lifecycle().await?;
    let activator = isolated
        .thread("github-normal-auth-gate")
        .script([
            RebornScriptedReply::tool_call(
                "builtin.extension_install",
                serde_json::json!({"extension_id": "github"}),
            ),
            RebornScriptedReply::text("github needs a credential"),
        ])
        .build()
        .await?;

    let (run_id, gate_ref) = activator
        .submit_turn_until_auth_blocked("set up github")
        .await?;
    let state = activator
        .wait_for_status(run_id, ironclaw_turns::TurnStatus::BlockedAuth)
        .await?;
    if state.credential_requirements.is_empty() {
        return Err(
            "github install must open a real, renderable auth gate (populated \
             credential_requirements), not an unsubmittable empty gate"
                .into(),
        );
    }
    // The user-owned credential requirement must land on the ordinary auth
    // gate without producing a configuration-shaped Failed result.
    activator.assert_no_error_shaped_tool_result().await?;

    activator.deny_auth_gate(run_id, &gate_ref).await?;
    activator
        .wait_for_status(run_id, ironclaw_turns::TurnStatus::Completed)
        .await?;
    Ok(())
}
