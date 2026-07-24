# Product Commands: Bring the Pipeline Live (PR 1 of a train)

**Status:** Approved design, pre-implementation
**Owner:** Ben Kurrek
**Scope:** One PR. Later PRs add commands as single table rows.

## Goal

`/model` and `/status` work end-to-end from Telegram, Slack, and WebChat, with
zero per-command logic in any adapter or frontend. Every slash command is
declared exactly once — name, aliases, title, description, usage, parse,
execute, and result presentation — in one central table in `ironclaw_product`.
Channels opt into commands through their extension manifest. The WebUI derives
its command surface (slash menu, execution, result rendering) entirely from the
central inventory. Unknown or unadmitted commands produce a clean, generic
help/rejection reply.

## Background

Reborn already owns the dormant machinery:

- `ironclaw_product/src/commands.rs` — `ProductCommand::from_payload`, the
  `COMMAND_SPECS` inventory (`model`, `status` parsers exist), lifecycle family.
- `ironclaw_product/src/command_dispatch.rs` — `ProductCommandAdmissionService`
  and `ProductCommandService` ports; the workflow (`workflow.rs:1100`) already
  runs parse → admit → execute for every channel envelope. Production wires
  only the fail-closed `Rejecting*` defaults, so every command today returns
  "command routing unavailable".
- Telegram's adapter (`ironclaw_telegram_v2_adapter/src/payload.rs`) already
  has the target parser shape: a generic slash parser that membership-checks
  `recognized_commands` and emits a normalized `InboundCommandPayload` — but
  production constructs it with an empty list.
- The run-delivery observer (`ironclaw_product/src/run_delivery/observer.rs`)
  posts auth-denial/rejection/busy hints back to channels but has no
  `CommandResult` branch — an executed command would vanish silently.
- `LlmConfigService` (`ironclaw_product/src/reborn_services/llm_config.rs`)
  already exposes `snapshot` and `set_active` — everything `/model` needs.

This PR connects those seams. It adds no new architecture beyond one manifest
field; behavior lives in the crate that already owns the command contract.

## Design

### 1. Central command table (split by layering, each fact once)

The descriptor **metadata** inventory — name, aliases, title, description,
usage (static strs) — lives in `ironclaw_host_api` (the shared vocabulary
crate), because manifest validation in `ironclaw_extensions` depends only on
`host_api` and must see the legal command names. The **behavior** table —
parse + execute binding per descriptor — stays in
`ironclaw_product/src/commands.rs`. A contract test pins the two 1:1 (every
descriptor has exactly one behavior entry and vice versa), so each fact is
still declared once. The handler receives the ports bundle and a resolved `CommandScope`
(tenant/user/thread identity — the *only* context execution needs):

- `model` → `LlmConfigService::snapshot` (no args) / `set_active` (set,
  set-provider). No new port.
- `status` (alias `progress`) → run-state read for the scope's thread:
  active run id, run status (running / blocked-approval / blocked-auth /
  idle), started-at. Uses the run-state surface `ironclaw_product` already
  depends on.
- Lifecycle family, `Unknown` → typed `ProductRejection` whose message lists
  the available commands from the table (generic help; no `/help` command yet).

Each handler returns a **standardized result payload** — ordered presentational
lines/fields (`CommandResultView`: title + rows of label/value + optional
plain-text lines) — so channels and the frontend render any command with one
generic renderer. Command-specific JSON never leaks to a surface.

### 2. Hub service + admission (`ironclaw_product`)

- `ProductCommandHub` — the one concrete `ProductCommandService`: resolve
  `CommandScope` from `ProductCommandContext` via the extension's
  `ConversationBindingService`, look up the table entry, execute. A thin loop
  over the table, no per-command match outside it. Constructed with the ports
  bundle (`LlmConfigService`, run-state read, binding).
- `PairedDmCommandAdmission` — the one concrete
  `ProductCommandAdmissionService`: admit iff the auth claim is Verified, the
  external actor resolves to a bound (paired) user, and the conversation is
  direct (DM). Also enforces the manifest opt-in (§3): a command outside the
  extension's declared set is rejected as unknown. Fail-closed on any resolver
  error.

Composition's dormant `RebornProviderAdminProductCommandService` is deleted as
superseded (never wired).

### 3. Manifest opt-in (`channel.commands`)

`ChannelDescriptor` (`ironclaw_host_api/src/channel.rs`) gains
`commands: Vec<String>` (default empty = channel supports no commands).
Validation in `ironclaw_extensions/src/v3.rs`: every listed name must match a
descriptor in the `host_api` inventory (by name, not alias) — unknown names
fail package validation, fail-closed. Slack and Telegram first-party manifests
declare `commands = ["model", "status"]`.

The channel host (`channel_host.rs build_generic_graph`) reads the declared
set from the resolved manifest and supplies it to (a) the generic sink's
classification step (§5) and (b) the admission service. No adapter or host
code ever names a specific command.

### 4. Composition wiring (`ironclaw_reborn_composition`)

Assembly only: construct `ProductCommandHub` + `PairedDmCommandAdmission` with
profile-provided ports and pass them via the existing
`DefaultProductSurface::with_product_command_admission_service` /
`with_product_command_service` builders in `build_generic_graph`
(`extension_host/channel_host.rs:579`). ~A handful of lines.

### 5. Channel edges (generic parsers only)

- **Telegram:** populate `GroupTriggerPolicy.recognized_commands` from the
  manifest-declared set (plumbed through the channel bindings), replacing the
  empty `::default()` in `ironclaw_reborn_cli/src/runtime/native_extensions.rs`.
  Parser itself is unchanged.
- **Slack:** add the equivalent generic slash parse to
  `ironclaw_slack_extension/src/payload.rs` for direct messages: leading
  `/name` where `name` is in the supplied set → shared
  `InboundCommandPayload::new` (same validation Telegram routes through);
  otherwise the text remains an ordinary message. No command semantics.

### 5b. Channel-protocol commands are not product commands

Some slash strings are channel-protocol interactions, not standardized
product commands — Telegram's `/start` is the canonical case (Telegram
auto-sends it on first contact, optionally carrying a deep-link payload).
These are owned by the adapter's normalize/classification step (the existing
`ChannelInboundClassification` seam for protocol interactions), run *before*
the generic product-command membership check, and never appear in the central
table or a manifest `commands` list. In this PR, `/start` keeps today's
behavior (ordinary message; the pairing interceptor already answers unpaired
actors). A follow-up PR in the train adds the deep-link polish: `/start
<payload>` classifies as the pairing-code message so `t.me/<bot>?start=<code>`
gives one-tap WebGeneratedCode pairing; bare `/start` stays unchanged.

### 6. Result delivery to channels (once, generic)

`run_delivery/observer.rs` gains a `CommandResult` branch: render the
standardized `CommandResultView` to plain text with one shared function and
post it through the existing delivery port (same mechanism as the busy/auth
hints, one-shot, deduped like the auth-denial notice). Command rejections
(unknown, unadmitted, invalid args) post the generic help/rejection text via
the existing rejection-hint path, extended to cover command rejections.

### 7. WebUI (derived, no per-command logic)

- **Backend:** `RebornServices` facade method + webui_v2 route:
  - `GET  /api/v2/commands` → the descriptor inventory (name, aliases, title,
    description, usage) straight from the table. WebUI sees all commands by
    default (no manifest gate; the browser surface is the operator).
  - `POST /api/v2/commands/execute` `{ text, thread_id }` → parse with the
    same `ProductCommand::from_payload`, execute on the hub with a
    `CommandScope` built from the authenticated session (session operator is
    trivially "paired"; thread bound through the existing
    `SessionThreadService` scope rules) → returns the `CommandResultView` (or
    the typed rejection).
- **Frontend:** three generic pieces — slash-menu/autocomplete fed by the
  inventory endpoint; composer intercept for a leading `/` that calls execute
  and never submits a turn; one `CommandResult` renderer component for the
  view payload, shown as a local system-style timeline entry. i18n for chrome
  strings only (titles/descriptions come from the inventory).

## Error handling

- Admission and execution failures surface as typed `ProductRejection`s;
  channel users get the sanitized generic text, never backend detail
  (error-boundary rules apply — no paths, provider bodies, or internals).
- Manifest validation is fail-closed at package validation time.
- Resolver/store errors during admission deny (fail-closed), with server-side
  cause retained in logs per error-handling rules.
- Commands are not turns: they never enter the agent loop, never consume
  model tokens, and a busy thread does not block them.

## Testing (red first)

- **Integration (`tests/integration/`, new scenario file):** through the
  harness test channel — `/model` roundtrip (snapshot + set), `/status` with
  and without an active run, unknown command → help text delivered,
  unpaired-actor denial, non-DM denial, manifest-undeclared command denial,
  and the CommandResult-delivery seam (assert the delivered message, not just
  a completed status). Coverage-floor ratchet updated in the same PR if
  required.
- **Contract/unit:** central-table invariants (unique names/aliases, every
  entry has title/description/usage, lifecycle+unknown reject with help);
  manifest validation accept/reject in `ironclaw_extensions` v3 tests;
  Telegram payload tests extended for the populated-set path; Slack payload
  tests for the new DM slash parse (mirroring Telegram's cases, including
  shared-validator rejections); observer test for CommandResult rendering +
  dedupe.
- **WebUI:** route tests for both endpoints (auth, thread-scope binding,
  rejection shapes); frontend vitest for composer intercept (slash never
  submits a turn) and the generic result renderer.
- **Architecture tier:** `cargo test -p ironclaw_architecture` for the new
  host_api field + any dependency-edge changes.

## Out of scope (later PRs)

`/new`, `/stop`, `/compact`, `/undo`, `/thread`; wiring the lifecycle command
family (PR 2 candidate, stricter admission); group-chat commands; a dedicated
`/help` command; per-extension command *subsets* in the WebUI; command
autocomplete on channels (Telegram BotFather command registration etc.);
Telegram `/start` deep-link pairing (§5b follow-up).

## PR train context

PR 1 (this): the generic machine. PR 2+: each new command = one table row +
its underlying runtime operation if missing (`/stop` → cancel_run exists;
`/new` → thread rebind op; `/compact` → user-triggered compaction op;
`/undo` → mark-excluded timeline op — never delete, per LLM-data retention).
