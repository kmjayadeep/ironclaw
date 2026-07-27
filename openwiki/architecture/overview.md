---
type: "Reference"
title: "Architecture Overview"
openwiki_generated: true
---

# Architecture Overview

This page explains IronClaw's system design, the four-layer model, dependency structure, and where to build new features.

## High-Level System Design

IronClaw is a **secure personal AI assistant** that executes agent workflows in sandboxed environments with policy-enforced access to tools and external services. The system prioritizes:

1. **Security first:** Encrypted secrets, sandboxed execution, approval gates
2. **Modularity:** Clear component boundaries, composable layers, trait-based extensibility
3. **Durability:** Event sourcing, snapshot recovery, multi-backend support (PostgreSQL, libSQL)
4. **Observability:** Structured logging, event tracing, audit trails

## The Dual Stack: v1 and Reborn

IronClaw runs two architectures in parallel:

### v1 (Legacy, Maintenance Only)
- **Location:** `src/` directory (~10k LOC)
- **Model:** Monolith with tightly coupled modules
- **Status:** Deprecated; being phased out
- **When to touch:** Only to maintain existing v1 behavior
- **New features:** ❌ Do not add features to v1

### Reborn (Modern, Active Development)
- **Location:** `crates/` directory (65 focused crates)
- **Model:** Modular architecture with clear authority boundaries
- **Status:** Primary target for new development
- **When to touch:** All new features go here
- **Migration:** Reborn replaces v1 gradually without forking the user experience

**Key Rule:** Build new features in Reborn (`crates/`), not v1 (`src/`).

## The Four-Layer Model (Reborn)

Reborn uses a kernel-userland architecture inspired by operating systems:

```
┌─────────────────────────────────────────────────────────────┐
│                        Products Layer                        │
│  CLI, WebUI, Slack, Telegram, custom channels & adapters    │
│                  (UX and surface ownership)                  │
├──────────────── TurnCoordinator Boundary ──────────────────┤
│                  Userland: Agent Loops                       │
│  Planned Agentic, Text, CodeAct                             │
│  (request effects through host ports)                       │
├──────────────── CapabilityHost Boundary ──────────────────┤
│             Kernel: Authority & Policy Gates                │
│  Authorization (who can access what)                        │
│  Approvals (human sign-off for dangerous ops)              │
│  Safety (prompt injection, credential detection)           │
│  Secrets (encrypted storage, injected at transit)          │
│  Resources (bounded execution, cost tracking)              │
│  Filesystem (file scoping, integrity)                      │
├──────────────── Effect Subscription Boundary ───────────────┤
│              Substrates: Durable Primitives                 │
│  Events (immutable audit log)                              │
│  Threads (turn history and state)                          │
│  Filesystem (user data, attachments)                       │
│  Memory (embeddings, search index)                         │
│  Run State (checkpoints, recovery)                         │
└─────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

| Layer | Owns | Does NOT Own | Boundary |
|-------|------|--------------|----------|
| **Products** | CLI/WebUI/channel UX | Agent logic, tools, DB | HTTP, channel-to-turn translation |
| **Userland (Loops)** | Planning, reasoning, tool selection | Security, approvals, secrets | Host ports (request only) |
| **Kernel (Gates)** | Policy, security, approvals, secrets | Loop implementation | Effects isolation |
| **Substrates** | Persistence, durability, indexing | Policy, security, approval logic | Backend-agnostic traits |

### Core Principle

**The loop is NOT the security perimeter.** Loops request effects through host ports; the kernel **decides what's allowed**. This means:

- Loops cannot directly call databases, filesystems, or secrets
- Every capability request passes through authorization gates
- Approvals are scoped to exact invocations, not blanket grants
- The kernel can deny, modify, or delay any requested effect

## Crate Organization

IronClaw's 65 crates are organized by functional group. **See [Crate Reference](crates.md) for the complete breakdown** with detailed descriptions, dependencies, and guidance on when to modify each crate.

**Quick reference by functional area:**
- **Core Contracts** (3 crates): `host_api`, `common`, `prompt_envelope`
- **Authority & Gates** (6 crates): `authorization`, `trust`, `safety`, `auth`, `approvals`, `runtime_policy`
- **Capability Execution** (4 crates): `capabilities`, `dispatcher`, `resources`, `run_state`
- **Durable State & Events** (6 crates): `events`, `event_projections`, `event_streams`, `reborn_event_store`, `outbound`, `reborn_traces`
- **Products & Loops** (10 crates): `agent_loop`, `loop_host`, `product`, `reborn_openai_compat`, `reborn_cli`, `webui`, `slack_extension`, `telegram_extension`, `telegram_v2_adapter`, `operator`
- **Storage & Secrets** (2 crates): `filesystem`, `secrets`
- **Utilities & Observability** (7 crates): `observability`, `embeddings`, `llm`, `network`, `extractors`, `attachments`, `skills`
- **Conversation & State** (7 crates): `conversations`, `threads`, `turns`, `triggers`, `memory`, `memory_native`, `memory_mem0`
- **Extensions & Integrations** (5 crates): `extensions`, `extension_host`, `first_party_extensions`, `first_party_extension_ports`, `mcp`
- **Runtime & Execution** (7 crates): `wasm`, `wasm_limiter`, `process_sandbox`, `processes`, `hooks`, `runner`, `scripts`
- **Configuration & Composition** (8 crates): `reborn_config`, `reborn_composition`, `host_runtime`, `host_ingress`, `reborn_identity`, `projects`
- **Architecture & Special** (2 crates): `architecture`, `silk_decoder`

## Dependency Flow (Acyclic Upward)

Crate dependencies **flow upward only** — no cycles. The dependency order is:

```
Core Contracts (shared types)
    ↓
Substrates (events, filesystem, memory)
    ↓
Authority & Gates (safety, secrets, approval)
    ↓
Capability Execution (tools, dispatch, WASM)
    ↓
Durable State (events, threads, conversation)
    ↓
Products & Loops (agent, reborn, CLI)
    ↓
Surfaces (WebUI, channels, API)
```

**This ordering ensures:**
- Security decisions (gates, approvals) are isolated from loops
- Loops cannot bypass kernels through imports
- Testing lower layers doesn't require product infrastructure
- Refactoring products doesn't destabilize substrates

## Where to Build New Features

### Decision Tree

```
Is the feature runtime/execution/agent-related?
├─ YES: Goes in crates/ironclaw_reborn* or crates/ironclaw_product*
│  ├─ Agent executor behavior? → ironclaw_agent_loop or ironclaw_loop_host
│  ├─ Config/composition? → ironclaw_reborn_config or ironclaw_reborn_composition
│  ├─ WebUI/API? → ironclaw_webui or ironclaw_reborn_openai_compat
│  ├─ Workflows/missions? → ironclaw_product
│  └─ New channel (Slack, Discord, Telegram)? → ironclaw_slack_extension, ironclaw_telegram_extension, etc.
│
└─ NO: Is it a tool, sandbox, or capability?
   ├─ YES: Goes in ironclaw_capabilities, ironclaw_extensions, ironclaw_wasm, ironclaw_scripts, etc.
   │
   └─ NO: Is it a gate (safety, approval, secrets)?
      ├─ YES: Goes in ironclaw_safety, ironclaw_approvals, ironclaw_secrets, ironclaw_authorization, etc.
      │
      └─ NO: Is it a substrate (events, storage, filesystem)?
         ├─ YES: Goes in ironclaw_events, ironclaw_filesystem, ironclaw_threads, ironclaw_memory, etc.
         │
         └─ LEGACY v1: Very rarely touch src/. Only maintain existing v1 behavior.
```

### Example Feature Paths

| Feature | Target Crates | Why |
|---------|---------------|-----|
| "Add GitHub issue tool" | `ironclaw_first_party_extensions`, `ironclaw_capabilities` | Capability implementation, registration |
| "Require approval for file writes" | `ironclaw_approvals`, `ironclaw_safety` | Gate logic, policy enforcement |
| "Support Slack threads" | `ironclaw_slack_extension`, `ironclaw_threads` | Channel adapter, thread metadata |
| "Encrypt user files" | `ironclaw_secrets`, `ironclaw_filesystem` | Encryption logic, file storage |
| "Add cost tracking" | `ironclaw_resources`, `ironclaw_events` | Quota system, event projection |

## Architecture Patterns

### Pattern 1: Trait-Based Extensibility

Instead of hardcoding integrations, IronClaw uses traits and registries:

```rust
// Database trait — implement once per backend
pub trait Db: Send + Sync { ... }

// Registered at startup
let db = if postgres_enabled {
    Box::new(PostgresDb::new(...))
} else {
    Box::new(LibSqlDb::new(...))
};

// Loops are database-agnostic
executor.run(db).await
```

**Where to extend:** Add new implementations to the registry in `ironclaw_reborn_composition`.

### Pattern 2: Host Ports (Effect Requests)

Loops don't call the kernel; they request effects through host ports:

```rust
// Loop requests a tool capability
let result = host.request_capability(CapabilityRequest {
    name: "github_issue_create",
    params: {...},
}).await?;

// Kernel gates the request
// (approval? security check? resource limit?)
// then executes if approved
```

**Where to extend:** Add new request types to `ironclaw_host_api`.

### Pattern 3: Event Sourcing

All state changes are immutable events; state is computed from projections:

```
User Action → Event(s) → Event Store
                           ↓
                        Projections
                           ↓
                        (Snapshots, caches, indexes)
                           ↓
                        (Loop queries snapshots, not full log)
```

**Where to extend:** Add event types to `ironclaw_events`, projection logic to `ironclaw_event_projections`.

### Pattern 4: Kernel-Userland Boundary

Every effect request crosses a security checkpoint:

```
Userland (Trustless)  ← Loop requests effect
         ↓ CapabilityRequest
    Kernel (Trusted)   ← Gates check: auth? approval? safety?
         ↓ Effect
   Substrate           ← Durable side effect
```

**Where to extend:** Add gates to `ironclaw_authorization`, `ironclaw_approvals`, `ironclaw_safety`.

## Cross-Crate Communication Patterns

### Event Subscriptions (Decoupled, Asynchronous)
Multiple subsystems listen to durable events:

```
Event Store →
  ├→ Projection System (computes snapshots)
  ├→ Memory Indexer (updates embeddings)
  ├→ Audit Logger (logs for security)
  └→ Trigger System (fires automations)
```

**Implementation:** Use `ironclaw_event_streams` for subscription.

### Host Ports (Loop-to-Kernel, Synchronous)
Loops request capabilities through ports; kernel enforces policy:

```
Loop → CapabilityRequest → Kernel Ports → Policy Checks → Execution
```

**Implementation:** Define request/response types in `ironclaw_host_api`, handle in `ironclaw_capabilities`.

### Trait Objects (Polymorphism)
Different implementations of the same behavior, selected at startup:

```
LlmProvider (Anthropic | OpenAI | Ollama | ...)
DbBackend (PostgreSQL | libSQL)
MemoryStore (Native | Bedrock | Pinecone | ...)
```

**Implementation:** Define trait in a core crate, register implementations at startup.

## Key Architectural Decisions

1. **Secrets never inline** — environment variable names only; actual secrets in env
2. **Loops are untrusted** — all loop requests pass through kernel gates
3. **Approval leases are exact-invocation scoped** — blanket approval is not possible
4. **Prompt templates live in files** — not hardcoded in Rust (allows rapid iteration)
5. **Event sourcing is immutable** — all state computable from events
6. **Backwards compatibility in event schema** — events are versioned and must be readable forever
7. **Dependency acyclic** — no circular imports; upward flow only
8. **Active-thread lock prevents duplicate work** — only one loop runs per thread at a time
9. **LoopExit is a claim, not trusted state** — kernel still checks permissions even if loop says "done"
10. **Large prompts in files, not code** — enables A/B testing and rapid iteration without rebuilding
11. **Logging discipline for REPL/TUI** — use `debug!()` not `info!()` to avoid spamming user output
12. **Test-first discipline** — every bug fix includes a regression test

## See Also

- **[Crate Reference](crates.md)** — Detailed breakdown of all 65 crates
- **[Data Model](data-model.md)** — Events, threads, turns, capabilities
- **[Security & Safety](security.md)** — Kernel boundary, threat model, approval gates
- **[AGENTS.md](/AGENTS.md)** — Quick rules and code discovery
- **[CLAUDE.md](/CLAUDE.md)** — Subsystem deep-dives by crate/module

---

**Last updated:** Auto-generated by OpenWiki. For corrections, file a PR.
