# Product Commands Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the dormant product-command pipeline live: `/model` and `/status`
work from Telegram, Slack, and WebChat with zero per-command logic outside the
central declaration, per the approved spec
(`docs/plans/2026-07-24-product-commands-pipeline.md`).

**Architecture:** Descriptor metadata inventory in `ironclaw_host_api`
(vocabulary); behavior table + hub + admission in `ironclaw_product`;
slash classification once in the generic channel sink
(`ironclaw_reborn_composition` `extension_ingress.rs`), membership-checked
against the manifest-declared `channel.commands`; results delivered by the
run-delivery observer; WebUI derives menu + execution + rendering from the
inventory. No adapter changes.

**Tech Stack:** Rust workspace (axum, serde, tokio), React/TS SPA (vitest),
TOML v3 extension manifests.

## Global Constraints

- Admission policy: Verified claim + actor resolves to a bound (paired) user +
  direct (DM) conversation only. Fail closed on resolver errors.
- Lifecycle family (`extension_*`, `skill_*`) parses but is rejected with the
  generic help text (PR 2 wires it).
- Commands are not turns: never enter the agent loop; unknown-to-inventory
  slash text stays an ordinary user message.
- No `.unwrap()`/`.expect()` in production code; `thiserror`; typed errors.
- Every task: red test first, then green, then `cargo fmt`, then commit.
- Zero-warning clippy across the three lanes before PR (review-discipline).

---

### Task 1: Command descriptor inventory in `ironclaw_host_api`

**Files:**
- Create: `crates/ironclaw_host_api/src/product_commands.rs`
- Modify: `crates/ironclaw_host_api/src/lib.rs` (module + re-export)
- Modify: `crates/ironclaw_product/src/commands.rs` (delete local
  `ProductCommandDescriptor`, import the host_api one; `COMMAND_SPECS`
  references inventory entries by name)
- Modify: `crates/ironclaw_product/src/lib.rs` (re-export path)

**Interfaces (produces):**

```rust
// ironclaw_host_api::product_commands
pub struct ProductCommandDescriptor {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub title: &'static str,
    pub description: &'static str,
    pub usage: &'static str,
}
/// The canonical inventory: model, status(+progress), extension_search,
/// extension_list, extension_install, extension_remove, skill_search,
/// skill_install, skill_remove.
pub const PRODUCT_COMMANDS: &[ProductCommandDescriptor];
/// Resolve by name or alias (input already lowercased by the slash parser).
pub fn find_product_command(name: &str) -> Option<&'static ProductCommandDescriptor>;
pub fn is_product_command_name(name: &str) -> bool; // name only, not alias
```

- [ ] Red: unit tests in `product_commands.rs` — unique names+aliases across
      the whole inventory; every entry has non-empty title/description/usage;
      `find_product_command("progress")` returns the `status` descriptor;
      `is_product_command_name("progress")` is false.
- [ ] Green: implement table + helpers.
- [ ] In `ironclaw_product`: contract test
      `commands.rs::tests::inventory_matches_behavior_table` — every
      `PRODUCT_COMMANDS` name is either a `LifecycleCommandKind` command name
      or a `COMMAND_SPECS` name, and vice versa (1:1).
- [ ] `cargo test -p ironclaw_host_api -p ironclaw_product`; fmt; commit.

### Task 2: `channel.commands` manifest field + validation

**Files:**
- Modify: `crates/ironclaw_host_api/src/channel.rs` (`ChannelDescriptor` gains
  `#[serde(default, skip_serializing_if = "Vec::is_empty")] pub commands:
  Vec<String>`; `validate()` rejects empty/duplicate entries)
- Modify: `crates/ironclaw_extensions/src/v3.rs` (validate each declared name
  with `is_product_command_name`; unknown → `ManifestV3Error::Invalid`)
- Modify: Slack + Telegram first-party channel manifests under
  `crates/ironclaw_first_party_extensions/assets/{slack,telegram}/` — add
  `commands = ["model", "status"]` to the `[channel]` section.

**Interfaces (consumes):** Task 1 `is_product_command_name`.

- [ ] Red: v3 test — manifest with `commands = ["model", "nonsense"]` fails
      validation naming `nonsense`; `["model", "status"]` passes; duplicate
      `["model", "model"]` fails; alias `["progress"]` fails (names only).
- [ ] Green: field + validation.
- [ ] First-party manifest load tests still pass with the new field
      (`cargo test -p ironclaw_first_party_extensions -p ironclaw_extensions`).
- [ ] fmt; commit.

### Task 3: Sink-level command classification

**Files:**
- Modify: `crates/ironclaw_host_api/src/product_adapter/inbound.rs`
  (`ChannelInboundClassification::Command(InboundCommandPayload)` + `From`
  arm → `ProductInboundPayload::Command`)
- Modify: `crates/ironclaw_reborn_composition/src/extension_host/extension_ingress.rs`
  (`ChannelInboundSinkConfig` gains `pub commands: Vec<String>`; in
  `InboundSink::admit`, when the interaction parse yields `None`, run
  `parse_product_slash_command(&message.text, message.trigger)`; if the parsed
  name/alias resolves via `find_product_command` → classify
  `Command(payload)`; unresolved or parse error → ordinary message)
- Modify: `crates/ironclaw_reborn_composition/src/extension_host/channel_host.rs`
  (`build_generic_graph` passes `channel.commands.clone()` into the sink
  config)

**Interfaces (produces):** every generic-channel message with a known slash
command reaches the workflow as `ProductInboundPayload::Command` — no adapter
involvement. Declared-set enforcement is admission's job (Task 5), so the sink
classifies any inventory command; membership travels in the config for
admission construction.

- [ ] Red: sink tests in `extension_ingress.rs` test module — `/model` text →
      envelope payload is `Command` with name `model`, args preserved;
      `/progress x` → `Command` named `progress` (alias resolution happens in
      `ProductCommand::from_payload`, not the sink); `/notacommand` → ordinary
      user-message payload; `approve <gate>` still classifies as approval
      resolution (precedence); `/`-only → ordinary message.
- [ ] Green; `cargo test -p ironclaw_reborn_composition`; fmt; commit.

### Task 4: `CommandResultView` + hub in `ironclaw_product`

**Files:**
- Modify: `crates/ironclaw_product/src/commands.rs` — add:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResultView {
    pub title: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<CommandResultField>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lines: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResultField { pub label: String, pub value: String }
pub fn command_help_text() -> String; // from PRODUCT_COMMANDS: "/name — title (usage)"
pub fn render_command_result_text(view: &CommandResultView) -> String;
```

- Create: `crates/ironclaw_product/src/command_hub.rs`:

```rust
pub struct ProductCommandHub { /* Arc<dyn LlmConfigService>, run-state read port,
    Arc<dyn ConversationBindingService> (channel scope resolution) */ }
impl ProductCommandHub {
    /// Shared core used by both the channel port and the WebUI facade.
    pub async fn execute_for_caller(
        &self,
        caller: ProductSurfaceCaller,
        thread: Option<ThreadRef>, // WebUI passes its thread; channels resolve via binding
        command: ProductCommand,
    ) -> Result<CommandResultView, ProductRejection>;
}
#[async_trait]
impl ProductCommandService for ProductCommandHub { /* resolve binding → caller/thread → execute_for_caller → CommandResult ack */ }
```

Arms: `Model::Status` → `LlmConfigService::snapshot`; `Model::Set`/
`SetProvider` → `set_active`; `Status` → run-record read for the resolved
thread scope (active run id, state, started-at; "no active run" otherwise);
`Lifecycle`/`Unknown` → `ProductRejection::permanent(InvalidRequest,
command_help_text())`.

- [ ] Red: hub unit tests with `test-support` fakes — model status returns a
      view naming the active provider/model; model set calls `set_active` with
      the parsed request; status with an active run includes the run id;
      status with none says idle; lifecycle + unknown reject with help text
      containing `/model`; binding-resolution failure → rejection (fail
      closed), never a panic.
- [ ] Green; `cargo test -p ironclaw_product`; fmt; commit.

### Task 5: Admission impl + composition wiring

**Files:**
- Create: `crates/ironclaw_product/src/command_admission.rs` —
  `PairedDmCommandAdmission { binding: Arc<dyn ConversationBindingService>,
  declared: BTreeSet<String> }` implementing `ProductCommandAdmissionService`:
  reject when the resolved command name ∉ `declared` (help text), when the
  route kind for the trigger/conversation is not direct, or when the actor
  does not resolve to a current bound user; resolver errors reject fail-closed.
- Modify: `crates/ironclaw_reborn_composition/src/extension_host/channel_host.rs`
  — `build_generic_graph` constructs per-extension
  `PairedDmCommandAdmission` (declared set from the manifest) and
  `ProductCommandHub` (global ports from deps + this extension's binding), and
  wires both via `with_product_command_admission_service` /
  `with_product_command_service`. `ChannelHostDeps` gains the hub's global
  ports (LLM config service + run-state read), supplied where the runtime
  assembly already owns them.
- Delete: `crates/ironclaw_reborn_composition/src/llm_admin/provider_admin_product_command.rs`
  + its `lib.rs` re-export; repurpose its workflow test to drive the hub.

**Interfaces (consumes):** Tasks 1, 4.

- [ ] Red: admission unit tests — undeclared command rejected with help text;
      non-direct conversation rejected; unbound actor rejected; bound + direct
      + declared admitted; binding error → rejected.
- [ ] Red: composition test (repurposed `provider_admin_product_command.rs` →
      `product_command_workflow.rs`) — a Command envelope through
      `DefaultProductSurface` with the real hub returns a `CommandResult` ack
      for `/model`, and a rejection for an undeclared command.
- [ ] Green; `cargo test -p ironclaw_reborn_composition -p ironclaw_product`;
      fmt; commit.

### Task 6: Observer delivery of command results and rejections

**Files:**
- Modify: `crates/ironclaw_product/src/run_delivery/observer.rs` — in
  `observe_ack`, handle `ProductInboundAck::CommandResult { command, payload }`:
  deserialize `CommandResultView` from the payload, render via
  `render_command_result_text`, post to the source conversation using the same
  delivery mechanism and (conversation, event_id) dedupe as the busy hint.
  Handle `ProductInboundAck::Rejected` for command envelopes by posting the
  rejection message (help text) one-shot.

**Interfaces (consumes):** Task 4 view + renderer.

- [ ] Red: observer tests (existing observer test module patterns) — a
      CommandResult ack posts exactly one message containing the rendered
      view; a transport-retry duplicate does not repost; a command rejection
      ack posts the rejection text; a user-message ack still posts nothing new.
- [ ] Green; `cargo test -p ironclaw_product`; fmt; commit.

### Task 7: WebUI backend (facade + routes)

**Files:**
- Modify: `crates/ironclaw_product/src/reborn_services.rs` +
  `reborn_services/types.rs` — builder `with_product_command_hub(Arc<ProductCommandHub>)`;
  methods:

```rust
pub async fn list_product_commands(&self, caller: ProductSurfaceCaller)
    -> Result<RebornProductCommandListResponse, ProductSurfaceError>; // full inventory
pub async fn execute_product_command(&self, caller: ProductSurfaceCaller,
    request: RebornExecuteProductCommandRequest) // { thread_id: String, text: String }
    -> Result<RebornExecuteProductCommandResponse, ProductSurfaceError>;
// Response: { command: String, result: Option<CommandResultView-DTO>, rejection: Option<{kind, message}> }
```

  `execute_product_command`: bind thread via the caller-scope rules
  (`caller.turn_scope` + `thread_scope_from_turn_scope`, same as
  `get_timeline`), parse with `parse_product_slash_command` +
  `ProductCommand::from_payload`, run `hub.execute_for_caller`. Not-a-command
  text → validation error; unknown command → 200 with rejection body.
- Modify: `crates/ironclaw_webui/src/webui_v2/descriptors.rs` (+ contract
  test), `router.rs`, `handlers.rs` — `GET /commands` → `list_product_commands`;
  `POST /threads/{thread_id}/commands` → `execute_product_command`.
- Modify: composition `build_webui_services` to wire the hub into the facade.

**Interfaces (consumes):** Task 4 hub + view.

- [ ] Red: facade tests — list returns all 9 descriptors with metadata;
      execute `/model` on an owned thread returns a result view; on a foreign
      thread → ownership error; `/nonsense` → rejection body with help text;
      non-slash text → validation error.
- [ ] Red: webui route tests (existing handler-test patterns +
      `webui_v2_descriptors_contract`).
- [ ] Green; `cargo test -p ironclaw_product -p ironclaw_webui`; fmt; commit.

### Task 8: Frontend (derived menu, intercept, generic result rendering)

**Files:**
- Modify: `crates/ironclaw_webui/frontend/src/lib/api.ts` —
  `listChatCommands(): Promise<ChatCommandDescriptor[]>` (GET, mirrors
  `listThreads`), `executeChatCommand(threadId, text)` (POST, mirrors
  `sendMessage`).
- Create: `crates/ironclaw_webui/frontend/src/pages/chat/hooks/useChatCommands.ts`
  — fetch-once inventory; `matchCommand(text)` (first token, name or alias).
- Modify: `pages/chat/components/chat-input.tsx` — in `handleSend`: if
  `matchCommand` hits, call `onCommand(text)` instead of `onSend`. Slash menu
  rendered above the composer when the draft starts with `/` (filtered by
  prefix; Enter completes; Escape dismisses) — visual model:
  `command-palette.tsx` list styling.
- Modify: `pages/chat/chat.tsx` + `pages/chat/hooks/useChat.ts` — `onCommand`
  posts via `executeChatCommand`, appends a SYSTEM-role local message with the
  rendered result (new factory `createCommandResultChatMessage` in
  `pages/chat/lib/message-types.ts`; result view → markdown: title, `label:
  value` lines, plain lines). Rejections render the same way.
- Modify: `frontend/src/i18n/en.ts` — chrome keys only (menu aria-label,
  command failed fallback).

**Interfaces (consumes):** Task 7 endpoints.

- [ ] Red: vitest — `matchCommand` matches `/model` and alias `/progress`,
      rejects `/notacommand` and plain text; chat-input intercept test: a
      matching command never calls `onSend`; result-view → markdown renderer
      test.
- [ ] Green; `npm test` (frontend suite) passes; commit.

### Task 9: Reborn integration scenario

**Files:**
- Create: `tests/integration/channel_command.rs`; register in root
  `Cargo.toml` as `[[test]] name = "reborn_integration_channel_command"`,
  `path = "tests/integration/channel_command.rs"` (sibling pattern:
  `extension_ingress` at root Cargo.toml ~295-304). Includes boilerplate
  copied from `greeting.rs:16-24`.
- Modify: `tests/integration/support/test_adapter.rs` — add
  `verified_command_envelope(event_id, user_id, thread_id, command, arguments,
  trigger)` mirroring `verified_text_envelope_with_trigger` (line 111) but
  wrapping `ProductInboundPayload::Command(InboundCommandPayload::new(...)?)`
  (for the admission-focused cases that inject envelopes directly).
- Modify: `tests/fixtures/extensions/acme-messenger/manifest.toml` — add
  `commands = ["model", "status"]` to `[channel]`.

**Template:** `tests/integration/extension_ingress.rs` (real
`extension_ingress_route_mount` over `GenericChannelInboundSink`, acme
channel adapter, `RecordingAdmissionObserver` capturing `ProductInboundAck`)
plus the `extension_delivery.rs` wire seam (`RecordingNetworkHttpEgress`,
`captured_network_requests()`) for asserting the delivered reply text. The
scenario must traverse the production `build_generic_graph` wiring (channel
host), not a hand-assembled surface, so it proves the composition actually
chains the hub/admission (integration-first rule).

- [ ] Red: end-to-end through the production channel ingress path —
      paired-DM `/model` → CommandResult delivered back on the channel
      (assert message content, not just status); `/status` idle and with an
      active run; `/notacommand` → becomes an ordinary user turn (not a
      command); undeclared-but-known command → rejection help delivered;
      unpaired actor → pairing interception (no command execution);
      non-DM conversation → admission rejection.
- [ ] Green; update `tests/integration/coverage-floor.toml` per its same-PR
      recapture instructions if the ratchet moves.
- [ ] fmt; commit.

### Task 10: Quality gates + PR

- [ ] `cargo fmt` (workspace clean).
- [ ] `cargo clippy --all --tests --examples -- -D warnings` and
      `cargo clippy --all --tests --examples --all-features -- -D warnings`
      (both lanes; plus the libsql lane if touched files intersect).
- [ ] `cargo test -p ironclaw_architecture` (host_api field + dependency edges).
- [ ] Full unfiltered suites for every touched crate (`--no-fail-fast`, no
      head/tail piping).
- [ ] Frontend: `npm test` + `tsc` lane per webui CLAUDE.md.
- [ ] `scripts/pre-commit-safety.sh`.
- [ ] Push branch to origin (nearai/ironclaw), open PR to `main` titled
      `feat(reborn): bring the product command pipeline live (/model, /status)`
      with the spec + this plan linked, scope description per
      review-discipline, and the PR-train roadmap.
