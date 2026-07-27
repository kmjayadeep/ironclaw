---
type: "Reference"
title: "Crate Reference"
description: "Complete reference for all 65 crates in IronClaw Reborn, organized by functional group with guidance on roles, dependencies, and when to modify each crate."
---

# Crate Reference

This page documents all **65 crates** in the IronClaw Reborn repository, organized by functional group. Use this as a reference when exploring code or deciding where to add new features.

## Quick Index

- [Core Contracts](#core-contracts-3-crates) — Shared types and traits
- [Authority & Gates](#authority--gates-6-crates) — Security and policy enforcement
- [Capability Execution](#capability-execution-4-crates) — Capability dispatch and execution
- [Durable State & Events](#durable-state--events-6-crates) — Event sourcing and persistence
- [Products & Loops](#products--loops-10-crates) — Agent loops, channels, and product surfaces
- [Storage & Secrets](#storage--secrets-2-crates) — Persistence backends
- [Utilities & Observability](#utilities--observability-7-crates) — Logging, embeddings, observability
- [Conversation & State](#conversation--state-7-crates) — Session threads, memory, triggers
- [Extensions & Integrations](#extensions--integrations-5-crates) — Extension system and MCP
- [Runtime & Execution](#runtime--execution-7-crates) — WASM, process sandbox, hooks
- [Configuration & Composition](#configuration--composition-8-crates) — DI, config, composition root
- [Architecture & Special](#architecture--special-2-crates) — Architecture validation, utilities

---

## Core Contracts (3 crates)

**Purpose:** Shared types, traits, and API contracts used across the system.

### ironclaw_host_api
**Description:** Shared host API contracts for IronClaw Reborn

**Role:** Loop-to-kernel communication contract; the canonical bridge between loop code and host effects
- Defines `HostPort` trait (how loops request effects)
- `CapabilityRequest`, `CapabilityResponse` types (capability invocation contract)
- Observer trait for event subscriptions
- **When to touch:** Adding new request types, changing effect semantics, or updating the loop-kernel boundary
- **Key modules:** `capabilities.rs`, `port.rs`
- **Depends on:** `ironclaw_common`
- **Tests:** Comprehensive; see `tests/` for contract tests

### ironclaw_common
**Description:** Shared types, paths, and platform helpers used across the IronClaw workspace

**Role:** Foundation library with shared types and utilities
- `Attachment`, `Event`, `Identity`, `Platform`, `Speaker` types
- Environment helpers, hashing, timezone utilities
- Provider transcript types
- **When to touch:** Adding shared utilities, types, or cross-crate constants
- **Key modules:** `attachment.rs`, `event.rs`, `identity.rs`, `platform.rs`
- **Depends on:** tokio, serde, chrono
- **Tests:** Unit tests for utilities

### ironclaw_prompt_envelope
**Description:** Shared envelope helper that wraps untrusted prompt content with a closed-vocabulary trust boundary

**Role:** Prompt injection defense and composition
- Prompt template system with variable substitution
- Placeholder handling and closure binding
- Injection safety validation and redaction
- **When to touch:** Changing how prompts are constructed, adding new template features, or updating safety checks
- **Key modules:** `envelope.rs`, `validation.rs`, `markers.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_safety`
- **Tests:** Safety scenario tests

---

## Authority & Gates (6 crates)

**Purpose:** Policy enforcement, security gates, and access control.

### ironclaw_authorization
**Description:** Access control and permission checking for capabilities (RBAC-based)

**Role:** Capability-based access control and permission enforcement
- RBAC: role-based access control
- Permission checks (who can invoke this capability?)
- Tenant isolation and resource boundaries
- **When to touch:** Adding new permission types, changing authorization policies, or adding new roles
- **Key modules:** `lib.rs`, `rbac.rs`, `permission.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`
- **Tests:** `tests/capability_access_contract.rs`, `tests/capability_lease_contract.rs`

### ironclaw_trust
**Description:** Host-controlled trust-class policy engine for IronClaw Reborn

**Role:** Trust assessment and identity verification
- Trust assessment (is this request trustworthy?)
- Identity verification (who are we talking to?)
- Tenant/user boundary enforcement
- **When to touch:** Changing trust models, updating identity verification logic, or adding new trust classes
- **Key modules:** `boundary.rs`, `assessment.rs`, `policy.rs`
- **Depends on:** `ironclaw_common`
- **Tests:** Trust boundary tests

### ironclaw_safety
**Description:** Prompt injection defense, input validation, secret leak detection, and safety policy enforcement

**Role:** Multi-layer safety enforcement and threat detection
- Prompt injection detection and prevention
- Credential detection (prevent leaking secrets in output)
- Input sanitization and validation
- Unsafe language pattern detection
- **When to touch:** Adding new safety checks, updating threat model, or changing validation rules
- **Key modules:** `injection.rs`, `credential_detector.rs`, `validation.rs`
- **Depends on:** `ironclaw_common`, regex libraries
- **Tests:** Extensive threat scenario tests in `tests/`

### ironclaw_auth
**Description:** Product-facing Reborn auth contracts and fake services

**Role:** Authentication and credential handling for product surfaces
- OAuth provider abstractions
- Credential mapping and session management
- Fake/test implementations
- **When to touch:** Adding new OAuth providers, changing auth flow, or updating credential handling
- **Key modules:** `lib.rs`, `oauth.rs`, `fake.rs`
- **Depends on:** `ironclaw_common`
- **Tests:** OAuth contract tests with fake providers

### ironclaw_approvals
**Description:** Human approval flows, lease management, and auto-approve rules

**Role:** Approval workflow and lease management
- Approval request creation and resolution
- Lease management (time-bound permissions)
- Auto-approve rules and policies
- CAS (compare-and-set) record tracking
- **When to touch:** Changing approval policies, updating lease terms, or adding new approval types
- **Key modules:** `auto_approve.rs`, `policy.rs`, `capability_permission.rs`
- **Depends on:** `ironclaw_runtime_policy`, `ironclaw_common`
- **Tests:** `tests/approval_resolution_contract.rs`

### ironclaw_runtime_policy
**Description:** Runtime profile resolver for IronClaw Reborn

**Role:** Policy profile definitions and enforcement
- Policy profile definitions (secure_default, local-dev, testing, etc.)
- Permission sets and resource limits
- Profile validation and normalization
- **When to touch:** Adding new policies, updating profile definitions, or changing resource limits
- **Key modules:** `profile.rs`, `permission.rs`, `limits.rs`
- **Depends on:** serde, toml, `ironclaw_common`
- **Tests:** Profile conformance tests

---

## Capability Execution (4 crates)

**Purpose:** Capability registration, dispatch, and execution.

### ironclaw_capabilities
**Description:** Capability registry and host API implementation

**Role:** Core capability execution and conformance checking
- Capability manifest (name, description, input/output schema)
- Profile conformance (which capabilities are allowed in this profile?)
- Host API implementation for effect execution
- Request/response handling and serialization
- **When to touch:** Adding new capability types, changing conformance rules, or updating execution semantics
- **Key modules:** `host.rs`, `conformance.rs`, `requests.rs`, `manifest.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`
- **Tests:** `tests/capability_host_contract.rs`

### ironclaw_dispatcher
**Description:** Multi-destination dispatch orchestration

**Role:** Capability request routing and dispatch
- Route requests to multiple handlers (tools, channels, subscriptions)
- Load balancing and failover logic
- Saga pattern for distributed transactions
- **When to touch:** Adding new dispatch destinations, changing routing rules, or updating failover behavior
- **Key modules:** `lib.rs`, `saga.rs`, `dispatch.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`
- **Tests:** `tests/dispatch_contract.rs`

### ironclaw_resources
**Description:** Resource reservation governor for IronClaw Reborn

**Role:** Resource governance and quota enforcement
- Quota enforcement (tokens, API calls, compute)
- Cost tracking per user/tenant
- Resource reserve/reconcile/release cycle
- **When to touch:** Adding new resource types, changing quota models, or updating cost calculation
- **Key modules:** `governor.rs`, `quota.rs`, `accounting.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`
- **Tests:** Quota enforcement tests

### ironclaw_run_state
**Description:** Execution state persistence for runs

**Role:** Run lifecycle and state tracking
- Run creation and state transitions
- Checkpoint and recovery
- Run metadata and results storage
- **When to touch:** Changing run lifecycle, updating state machine, or adding new state types
- **Key modules:** `lib.rs`, `state.rs`, `checkpoint.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_events`
- **Tests:** State machine contract tests

---

## Durable State & Events (6 crates)

**Purpose:** Event sourcing, persistence, and audit trails.

### ironclaw_events
**Description:** Event types and immutable event log

**Role:** Central event sourcing system
- Event types (capability executed, approval requested, etc.)
- Event serialization (JSONL format)
- Event cursor (position in log)
- Replayable event stream
- **When to touch:** Adding new event types, changing schema, or updating event metadata
- **Key modules:** `lib.rs`, `runtime_event.rs`, `event_types.rs`
- **Depends on:** serde_json, `ironclaw_common`
- **Tests:** `tests/durable_log_contract.rs`

### ironclaw_event_projections
**Description:** Event-to-readmodel projection system

**Role:** Materialized views and readmodels from events
- Project events to queryable state
- Update readmodels
- Snapshot and incremental updates
- **When to touch:** Adding new projections, changing query semantics, or updating readmodel schema
- **Key modules:** `lib.rs`, `projection.rs`, `readmodel.rs`
- **Depends on:** `ironclaw_events`, `ironclaw_common`
- **Tests:** Projection contract tests

### ironclaw_event_streams
**Description:** Transport-neutral Reborn projection stream manager

**Role:** Stream and subscription management
- Stream creation and management
- Filter and projection subscriptions
- Fan-out to multiple subscribers
- **When to touch:** Adding new stream types, changing subscription semantics, or adding new transports
- **Key modules:** `lib.rs`, `subscription.rs`, `fanout.rs`
- **Depends on:** `ironclaw_events`, `ironclaw_common`
- **Tests:** Stream contract tests

### ironclaw_reborn_event_store
**Description:** Reborn-owned durable event and audit store backends (PostgreSQL, libSQL)

**Role:** Persistent event storage backend
- PostgreSQL and libSQL adapter implementations
- Event append and query operations
- Transactions and ACID guarantees
- **When to touch:** Adding new storage backends, optimizing queries, or changing persistence layer
- **Key modules:** `postgres.rs`, `libsql.rs`, `migration.rs`, `operations.rs`
- **Depends on:** `tokio-postgres`, `rusqlite`, `ironclaw_events`, `ironclaw_common`
- **Tests:** Integration tests with Docker PostgreSQL and libSQL

### ironclaw_outbound
**Description:** Outbound egress policy and projection subscription management

**Role:** External event egress and routing
- Outbound subscription policies
- Event filtering for external delivery
- Saga correlation for distributed workflows
- **When to touch:** Adding new egress policies, changing external routing, or updating correlation logic
- **Key modules:** `lib.rs`, `subscription.rs`, `policy.rs`
- **Depends on:** `ironclaw_events`, `ironclaw_common`
- **Tests:** Egress policy tests

### ironclaw_reborn_traces
**Description:** Distributed tracing and audit logging

**Role:** Observability and audit trail
- Trace propagation across service boundaries
- Audit log creation and storage
- Distributed context (trace ID, span ID)
- **When to touch:** Adding new trace events, changing audit schema, or updating context propagation
- **Key modules:** `lib.rs`, `trace.rs`, `audit.rs`
- **Depends on:** tokio, tracing, `ironclaw_common`
- **Tests:** Trace propagation tests

---

## Products & Loops (10 crates)

**Purpose:** Agent loops, product surfaces, and channel integrations.

### ironclaw_agent_loop
**Description:** Agent-loop framework state and strategy contracts for IronClaw Reborn

**Role:** Agent loop abstraction and strategy
- Loop lifecycle management (start, execute, checkpoint, finish)
- Strategy selection (Planned, Text, CodeAct)
- Turn coordination and state machines
- **When to touch:** Adding new loop strategies, changing lifecycle, or updating turn semantics
- **Key modules:** `lib.rs`, `strategy.rs`, `lifecycle.rs`, `turn.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`
- **Tests:** Loop lifecycle contract tests

### ironclaw_loop_host
**Description:** Loop host adapters for IronClaw Reborn AgentLoopHost implementations

**Role:** Loop execution host and runtime
- Loop instantiation and lifecycle
- Loop host API implementation
- Strategy routing and invocation
- **When to touch:** Adding new loop host implementations, changing execution semantics, or updating strategy dispatch
- **Key modules:** `lib.rs`, `host.rs`, `executor.rs`
- **Depends on:** `ironclaw_agent_loop`, `ironclaw_host_api`, `ironclaw_common`
- **Tests:** Loop host contract tests

### ironclaw_product
**Description:** Product-facing workflow service for IronClaw Reborn

**Role:** Product layer workflows and orchestration
- Mission and project management
- Workflow execution and state
- Product API implementation
- **When to touch:** Adding new product workflows, changing mission types, or updating orchestration logic
- **Key modules:** `lib.rs`, `workflow.rs`, `mission.rs`, `project.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`, `ironclaw_events`
- **Tests:** Workflow execution tests

### ironclaw_reborn_openai_compat
**Description:** Reborn-native OpenAI-compatible API surface

**Role:** OpenAI API compatibility layer
- OpenAI chat completion API (v1/chat/completions)
- Message format mapping
- Streaming and non-streaming responses
- **When to touch:** Adding OpenAI API features, changing response format, or adding new endpoints
- **Key modules:** `lib.rs`, `chat.rs`, `format.rs`, `streaming.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`, axum, serde
- **Tests:** OpenAI API contract tests

### ironclaw_reborn_cli
**Description:** Secure personal AI assistant that protects your data and expands its capabilities on the fly

**Role:** Primary CLI and binary entry point
- Command-line interface (binary name: `ironclaw`)
- Reborn serve mode and WebUI hosting
- Configuration bootstrapping
- **When to touch:** Adding CLI commands, changing serve behavior, or updating binary deployment
- **Key modules:** `lib.rs`, `main.rs`, `cli.rs`, `serve.rs`
- **Depends on:** all major Reborn crates via composition root
- **Tests:** E2E tests via `tests/e2e/` directory

### ironclaw_webui
**Description:** Host-owned listener binding and serve loop for the Reborn WebChat v2 HTTP gateway

**Role:** WebUI HTTP gateway and frontend serving
- HTTP route mounting
- Frontend asset serving (React, CSS, JS)
- WebSocket connections for live updates
- **When to touch:** Adding HTTP routes, serving new frontend assets, or updating WebSocket protocol
- **Key modules:** `lib.rs`, `server.rs`, `routes.rs`, `frontend.rs`
- **Depends on:** `ironclaw_host_api`, axum, tokio, `ironclaw_common`
- **Tests:** HTTP route tests

### ironclaw_slack_extension
**Description:** Slack channel extension for IronClaw Reborn

**Role:** Slack channel adapter and integration
- Slack message handling
- Event subscription (app_mention, message, etc.)
- Two-way sync (Slack ↔ IronClaw)
- **When to touch:** Adding Slack features, updating message handling, or changing sync behavior
- **Key modules:** `lib.rs`, `event_handler.rs`, `sync.rs`
- **Depends on:** slack-bolt, `ironclaw_host_api`, `ironclaw_common`
- **Tests:** Slack event contract tests

### ironclaw_telegram_extension
**Description:** Telegram channel extension for IronClaw Reborn

**Role:** Telegram channel adapter and integration
- Telegram message handling
- Bot webhook management
- Two-way sync (Telegram ↔ IronClaw)
- **When to touch:** Adding Telegram features, updating message handling, or changing bot behavior
- **Key modules:** `lib.rs`, `webhook.rs`, `sync.rs`
- **Depends on:** teloxide, `ironclaw_host_api`, `ironclaw_common`
- **Tests:** Telegram bot contract tests

### ironclaw_telegram_v2_adapter
**Description:** Telegram WASM v2 ProductAdapter for IronClaw Reborn

**Role:** Telegram WASM-based product adapter
- WASM-compiled Telegram adapter
- Low-latency message delivery
- Compiled via `./scripts/build-extensions.sh`
- **When to touch:** Updating WASM-based adapter logic, optimizing for performance, or changing message protocol
- **Key modules:** `lib.rs`, `adapter.rs`
- **Depends on:** wasm-bindgen, `ironclaw_common`
- **Tests:** WASM contract tests

### ironclaw_operator
**Description:** Host/operator control-plane services for IronClaw Reborn

**Role:** Control plane and operational management
- Operator API endpoints
- Status monitoring and health checks
- Configuration hot-reload and updates
- **When to touch:** Adding operator commands, changing control plane API, or updating operational features
- **Key modules:** `lib.rs`, `api.rs`, `monitor.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`, axum
- **Tests:** Operator API tests

---

## Storage & Secrets (2 crates)

**Purpose:** Persistence backends and encrypted credential storage.

### ironclaw_filesystem
**Description:** Scoped filesystem service for IronClaw Reborn (universal persistence: local, PostgreSQL, libSQL)

**Role:** Central persistence abstraction for files and structured data
- Local filesystem backend
- PostgreSQL backend (content-addressed blob storage)
- libSQL backend (embedded SQLite)
- File scoping and access control
- Integrity checking (content-addressed storage)
- **When to touch:** Adding new storage backends, changing access control model, or optimizing storage
- **Key modules:** `backend.rs`, `catalog.rs`, `scoped.rs`, `local.rs`, `postgres.rs`, `libsql.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`, tokio-postgres, rusqlite
- **Tests:** Integration tests with all three backends

### ironclaw_secrets
**Description:** Encrypted secret storage (master key management, encryption at rest, credential injection)

**Role:** Encrypted credential vault
- Master key management and rotation
- Encryption at rest (user secrets, API keys)
- Credential injection at transit time (never at rest)
- Redaction (prevent logging of secrets)
- **When to touch:** Changing encryption schemes, updating key rotation, or adding new secret types
- **Key modules:** `encrypt.rs`, `redact.rs`, `vault.rs`, `key.rs`
- **Depends on:** `ironclaw_common`, ring, chacha20poly1305
- **Tests:** Secret handling contract tests

---

## Utilities & Observability (7 crates)

**Purpose:** Shared utilities, logging, and observability infrastructure.

### ironclaw_observability
**Description:** Low-level observability helpers for IronClaw

**Role:** Tracing and observability infrastructure
- Structured logging helpers
- Trace context propagation
- Span creation and management
- **When to touch:** Adding new trace events, changing logging format, or updating context propagation
- **Key modules:** `lib.rs`, `tracing.rs`, `context.rs`
- **Depends on:** tracing, tokio, `ironclaw_common`
- **Tests:** Context propagation tests

### ironclaw_embeddings
**Description:** Embedding-provider trait and implementations (OpenAI, NearAI, Ollama, AWS Bedrock) with LRU caching

**Role:** Multi-provider embeddings with caching
- Provider abstraction (OpenAI, NearAI, Ollama, AWS Bedrock)
- LRU cache for embeddings
- Async batch operations
- **When to touch:** Adding new embedding providers, changing cache strategy, or updating batch semantics
- **Key modules:** `lib.rs`, `provider.rs`, `cache.rs`, `openai.rs`, `ollama.rs`
- **Depends on:** `ironclaw_common`, reqwest, lru
- **Tests:** Provider integration tests

### ironclaw_llm
**Description:** Multi-provider LLM integration with retry, failover, circuit breaker, and response caching

**Role:** LLM provider abstraction and resilience
- Multi-provider LLM support (OpenAI, Claude, local models)
- Retry logic and exponential backoff
- Circuit breaker pattern
- Response caching
- **When to touch:** Adding new LLM providers, changing retry policy, or updating caching strategy
- **Key modules:** `lib.rs`, `provider.rs`, `retry.rs`, `cache.rs`, `circuit_breaker.rs`
- **Depends on:** `ironclaw_common`, reqwest, tokio
- **Tests:** Provider failover and circuit breaker tests

### ironclaw_network
**Description:** HTTP/network utilities and network sandbox

**Role:** Network access control and utilities
- HTTP client with policy enforcement
- DNS allowlist/denylist enforcement
- IP filtering (private network protection)
- TLS validation
- Network timeout policy
- **When to touch:** Adding network restrictions, updating policy, or adding new network features
- **Key modules:** `lib.rs`, `sandbox.rs`, `allowlist.rs`, `http.rs`
- **Depends on:** `ironclaw_host_api`, reqwest, `ironclaw_common`
- **Tests:** Network sandbox contract tests

### ironclaw_extractors
**Description:** Type-aware text extraction for IronClaw (PDF, OOXML, Office, RTF, text/code)

**Role:** File format extraction and text conversion
- PDF text extraction
- OOXML (Word, Excel, PowerPoint) extraction
- Legacy Office format support
- RTF and plain text handling
- **When to touch:** Adding new file formats, improving extraction quality, or updating dependencies
- **Key modules:** `lib.rs`, `pdf.rs`, `ooxml.rs`, `rtf.rs`, `text.rs`
- **Depends on:** pdfium, zip, regex, `ironclaw_common`
- **Tests:** Format-specific extraction tests

### ironclaw_attachments
**Description:** Channel-agnostic attachment landing for IronClaw Reborn

**Role:** Attachment upload and storage
- Write attachment bytes through scoped filesystem authority
- Return ScopedPath storage keys
- Multi-channel attachment handling
- **When to touch:** Changing attachment storage, updating validation, or adding new attachment types
- **Key modules:** `lib.rs`, `upload.rs`, `storage.rs`
- **Depends on:** `ironclaw_filesystem`, `ironclaw_common`
- **Tests:** Upload and storage contract tests

### ironclaw_skills
**Description:** Skill selection, scoring, and management for IronClaw

**Role:** Skill registry and selection engine
- Skill manifest and metadata
- Scoring and ranking logic
- Selection and invocation
- **When to touch:** Adding new skill types, updating selection algorithm, or changing skill metadata
- **Key modules:** `lib.rs`, `registry.rs`, `scoring.rs`, `selector.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_embeddings`
- **Tests:** Skill selection algorithm tests

---

## Conversation & State (7 crates)

**Purpose:** Session threads, memory, conversation binding, and scheduled triggers.

### ironclaw_conversations
**Description:** Conversation binding and session thread contracts for IronClaw Reborn

**Role:** Conversation-to-thread binding
- Map conversations to session threads
- Binding contracts and serialization
- **When to touch:** Changing conversation binding semantics, updating contracts, or adding new binding types
- **Key modules:** `lib.rs`, `binding.rs`, `contract.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_threads`
- **Tests:** Binding contract tests

### ironclaw_threads
**Description:** Canonical session thread and transcript service contracts for IronClaw Reborn

**Role:** Session thread management and transcripts
- Thread creation and lifecycle
- Transcript storage and retrieval
- Message ordering and timestamps
- **When to touch:** Changing thread lifecycle, updating transcript schema, or adding new message types
- **Key modules:** `lib.rs`, `thread.rs`, `transcript.rs`, `message.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_events`
- **Tests:** Thread lifecycle contract tests

### ironclaw_turns
**Description:** Host-layer turn coordination contracts for IronClaw Reborn

**Role:** Turn state and coordination
- Turn creation and state machine
- Checkpointing and recovery
- Turn metadata and results
- **When to touch:** Changing turn lifecycle, updating state machine, or adding new turn types
- **Key modules:** `lib.rs`, `turn.rs`, `state.rs`, `coordinator.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_events`
- **Tests:** Turn coordination contract tests

### ironclaw_triggers
**Description:** Scheduled trigger domain and source-provider contracts for IronClaw Reborn

**Role:** Scheduled task execution and triggering
- Trigger schedule definitions (cron, interval, etc.)
- Trigger source providers
- Execution scheduling and state
- **When to touch:** Adding new trigger types, changing schedule syntax, or updating scheduling logic
- **Key modules:** `lib.rs`, `trigger.rs`, `schedule.rs`, `provider.rs`
- **Depends on:** `ironclaw_common`, tokio, cron-parser
- **Tests:** Schedule parsing and execution tests

### ironclaw_memory
**Description:** Provider-neutral memory contract types for IronClaw Reborn

**Role:** Memory system abstraction
- Memory provider trait
- Memory document types (notes, facts, relationships)
- Query and update semantics
- **When to touch:** Adding new memory types, changing provider abstraction, or updating query semantics
- **Key modules:** `lib.rs`, `provider.rs`, `document.rs`, `query.rs`
- **Depends on:** `ironclaw_common`
- **Tests:** Memory contract tests

### ironclaw_memory_native
**Description:** Memory document service adapters for IronClaw Reborn

**Role:** Native memory storage backend
- Document persistence
- Embedding-based search
- Update and reconciliation
- **When to touch:** Changing storage backend, updating search algorithm, or optimizing queries
- **Key modules:** `lib.rs`, `storage.rs`, `search.rs`, `embedding.rs`
- **Depends on:** `ironclaw_memory`, `ironclaw_filesystem`, `ironclaw_embeddings`
- **Tests:** Memory storage and search tests

### ironclaw_memory_mem0
**Description:** mem0-backed memory provider adapter for IronClaw Reborn (third-party provider)

**Role:** mem0.ai memory provider integration
- mem0 API client
- Document mapping to mem0 format
- Async integration
- **When to touch:** Updating mem0 API integration, changing mapping logic, or optimizing calls
- **Key modules:** `lib.rs`, `client.rs`, `mapping.rs`
- **Depends on:** `ironclaw_memory`, reqwest, `ironclaw_common`
- **Tests:** mem0 API contract tests (mocked)

---

## Extensions & Integrations (5 crates)

**Purpose:** Extension system, lifecycle management, and extensibility.

### ironclaw_extensions
**Description:** Extension manifest and registry contracts for IronClaw Reborn

**Role:** Extension system abstraction and manifest handling
- Manifest parsing and validation (v1, v2 formats)
- Extension registry
- Metadata and capability definitions
- **When to touch:** Adding new manifest fields, changing registry semantics, or supporting new extension versions
- **Key modules:** `lib.rs`, `registry.rs`, `manifest.rs`, `v2.rs`
- **Depends on:** `ironclaw_common`, serde_json, toml
- **Tests:** `tests/extension_contract.rs`, `tests/manifest_v2_contract.rs`

### ironclaw_extension_host
**Description:** Generic extension lifecycle host, active snapshot, loaders, and resolvers

**Role:** Extension runtime and lifecycle management
- Extension loading and activation
- Active snapshot maintenance
- Hot-reload support
- Discovery and resolver
- **When to touch:** Changing extension lifecycle, adding new loader types, or updating snapshot semantics
- **Key modules:** `lib.rs`, `host.rs`, `loader.rs`, `snapshot.rs`, `resolver.rs`
- **Depends on:** `ironclaw_extensions`, `ironclaw_common`
- **Tests:** Extension lifecycle contract tests

### ironclaw_first_party_extensions
**Description:** First-party userland extension implementations for IronClaw

**Role:** Built-in tools (GitHub, Google Drive, Slack, Notion, etc.)
- GitHub tool (issues, PRs, repos)
- Google Drive, Sheets, Docs, Slides tools
- Notion tool
- Slack tool
- WASM-compiled tool implementations
- **When to touch:** Adding new tools, modifying tool schemas, or updating tool implementations
- **Key modules:** `assets/` (manifests, schemas, prompts)
- **Assets format:** Manifests in `assets/*/manifest.toml`, prompts in `assets/*/prompts/`, schemas in `assets/*/schemas/`
- **Build:** Requires WASM compilation; run `./scripts/build-extensions.sh`
- **Tests:** Tool integration tests

### ironclaw_first_party_extension_ports
**Description:** Loop-facing ports for first-party IronClaw extensions

**Role:** Extension port adapters and integration
- Port trait implementations for extensions
- Effect marshaling and invocation
- Loop ↔ extension protocol
- **When to touch:** Adding new port types, changing marshaling semantics, or updating extension integration
- **Key modules:** `lib.rs`, `port.rs`, `marshal.rs`
- **Depends on:** `ironclaw_agent_loop`, `ironclaw_extensions`, `ironclaw_common`
- **Tests:** Port contract tests

### ironclaw_mcp
**Description:** Model Context Protocol support for IronClaw Reborn

**Role:** MCP server discovery and integration
- MCP server discovery and listing
- Protocol implementation (v1)
- Tool mapping to MCP capabilities
- Request/response handling
- **When to touch:** Adding MCP features, supporting new protocol versions, or changing tool mapping
- **Key modules:** `lib.rs`, `discovery.rs`, `protocol.rs`, `tool_mapping.rs`
- **Depends on:** `ironclaw_extensions`, `ironclaw_common`, serde_json
- **Tests:** MCP protocol contract tests

---

## Runtime & Execution (7 crates)

**Purpose:** WASM, process sandbox, hooks, and script execution.

### ironclaw_wasm
**Description:** WASM sandbox runtime for tool execution

**Role:** WebAssembly sandbox and execution
- Tool execution in WASM sandbox
- Memory isolation (WASM linear memory)
- Host function bindings
- Sandbox initialization and cleanup
- **When to touch:** Adding new host functions, changing sandbox semantics, or optimizing execution
- **Key modules:** `lib.rs`, `sandbox.rs`, `host_functions.rs`
- **Depends on:** `wasmer`, `ironclaw_host_api`, `ironclaw_wasm_limiter`
- **Tests:** Sandbox contract tests

### ironclaw_wasm_limiter
**Description:** Resource limits in WASM (memory limits, time limits, instruction counting)

**Role:** WASM resource governance
- Memory limits (prevent unbounded allocation)
- Time limits (timeout enforcement)
- Instruction counting and metering
- **When to touch:** Changing resource limit policies, updating counting logic, or adding new limit types
- **Key modules:** `limiter.rs`, `memory.rs`, `meter.rs`
- **Depends on:** `wasmer`, `ironclaw_resources`
- **Tests:** Resource limit enforcement tests

### ironclaw_process_sandbox
**Description:** Process sandboxing (subprocess isolation, I/O capture, resource limits)

**Role:** System process sandboxing
- Subprocess isolation and confinement
- I/O capture and streaming
- Resource limits (memory, CPU)
- Signal handling and cleanup
- **When to touch:** Changing sandbox policy, updating I/O handling, or optimizing subprocess management
- **Key modules:** `lib.rs`, `sandbox.rs`, `io_capture.rs`, `limits.rs`
- **Depends on:** tokio, `ironclaw_resources`
- **Tests:** Subprocess contract tests

### ironclaw_processes
**Description:** Process state management for IronClaw Reborn

**Role:** Process lifecycle and state tracking
- Process spawning and lifecycle
- State persistence (running, completed, failed)
- Result collection and cleanup
- **When to touch:** Changing process lifecycle, updating state machine, or adding new process types
- **Key modules:** `lib.rs`, `state.rs`, `lifecycle.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_events`
- **Tests:** Process lifecycle tests

### ironclaw_hooks
**Description:** Reborn loop hook framework (trust-tiered points, sealed witnesses, dispatcher)

**Role:** Hook system and lifecycle events
- Hook system for startup, shutdown, events
- Trust-tier classification (kernel, userland, external)
- Observer trait implementations
- Plugin registration
- **When to touch:** Adding new hook points, changing trust tiers, or updating hook semantics
- **Key modules:** `lib.rs`, `observer.rs`, `trust_tier.rs`, `dispatcher.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_common`
- **Tests:** Hook dispatch contract tests

### ironclaw_runner
**Description:** Reborn runner control plane and loop-driver adapters for IronClaw

**Role:** Loop runner and orchestration
- Loop instantiation and execution
- Runner control plane API
- Strategy driver adapters
- Execution scheduling
- **When to touch:** Adding new runner types, changing execution semantics, or updating control plane
- **Key modules:** `lib.rs`, `runner.rs`, `control_plane.rs`, `driver.rs`
- **Depends on:** `ironclaw_agent_loop`, `ironclaw_loop_host`, `ironclaw_common`
- **Tests:** Runner orchestration tests

### ironclaw_scripts
**Description:** Script execution (Python, Bash, etc.) for CodeAct and inline scripts

**Role:** Code execution and scripting
- CodeAct (Code Action) execution
- Inline script support (Python, Bash, etc.)
- Script sandboxing and limits
- Output capture and serialization
- **When to touch:** Adding new script languages, changing execution model, or updating sandboxing
- **Key modules:** `lib.rs`, `executor.rs`, `language.rs`, `sandbox.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_process_sandbox`, `ironclaw_common`
- **Tests:** Script execution contract tests

---

## Configuration & Composition (8 crates)

**Purpose:** Dependency injection, configuration, and the composition root.

### ironclaw_reborn_config
**Description:** Boot configuration contracts for the standalone IronClaw Reborn binary

**Role:** Configuration parsing and validation
- TOML/JSON/YAML config file parsing
- Environment variable overrides
- Validation and normalization
- Default profiles (secure_default, local-dev, testing)
- **When to touch:** Adding new config options, changing validation, or adding new profile types
- **Key modules:** `lib.rs`, `parser.rs`, `profile.rs`, `validation.rs`
- **Depends on:** toml, serde, `ironclaw_runtime_policy`, `ironclaw_common`
- **Tests:** Config parsing and validation tests

### ironclaw_reborn_composition
**Description:** Composition-root production dependency injection root for IronClaw Reborn

**Role:** Dependency assembly and wiring
- Service factory creation
- Dependency graph assembly
- Provider registration (LLM, embeddings, memory, etc.)
- Composition verification
- **When to touch:** Adding new services, wiring new providers, or changing composition semantics
- **Key modules:** `lib.rs`, `composition.rs`, `factory.rs`, `provider_registry.rs`
- **Depends on:** ALL major crates (composition root)
- **Tests:** Composition verification tests

### ironclaw_host_runtime
**Description:** Host-side effect execution (shell, HTTP, file I/O, external services)

**Role:** Host-side effect execution engine
- Shell command execution
- HTTP requests (with policy enforcement)
- File I/O operations
- External service calls
- **When to touch:** Adding new effect types, changing execution semantics, or updating sandbox policies
- **Key modules:** `lib.rs`, `effects.rs`, `shell.rs`, `http.rs`, `file_io.rs`
- **Depends on:** `ironclaw_host_api`, `ironclaw_network`, `ironclaw_filesystem`, tokio, reqwest
- **Tests:** Effect execution contract tests

### ironclaw_host_ingress
**Description:** Host HTTP ingress route mount carriers for IronClaw Reborn

**Role:** HTTP route mounting and registration
- Route mount abstraction
- Path and method matching
- Route handler registration
- **When to touch:** Adding new route types, changing routing semantics, or adding new HTTP features
- **Key modules:** `lib.rs`, `mount.rs`, `route.rs`
- **Depends on:** `ironclaw_host_api`, axum, `ironclaw_common`
- **Tests:** Route mounting contract tests

### ironclaw_reborn_identity
**Description:** Canonical Reborn identity resolver (maps OAuth and external channels to UserIds)

**Role:** User identity resolution and mapping
- OAuth provider → User ID mapping
- External channel → User ID mapping (Slack, Telegram, Discord)
- Identity provider abstraction
- Stable ID generation
- **When to touch:** Adding new identity providers, changing ID generation, or updating mapping logic
- **Key modules:** `lib.rs`, `resolver.rs`, `provider.rs`, `mapping.rs`
- **Depends on:** `ironclaw_auth`, `ironclaw_common`
- **Tests:** Identity resolution contract tests

### ironclaw_projects
**Description:** First-class Project entity, membership, and access control

**Role:** Project management and RBAC
- Project creation and lifecycle
- Team membership management
- Role-based access control (member, admin, owner)
- Project isolation and data boundaries
- **When to touch:** Adding new project features, changing membership model, or updating RBAC
- **Key modules:** `lib.rs`, `entity.rs`, `membership.rs`, `access.rs`
- **Depends on:** `ironclaw_common`, `ironclaw_authorization`
- **Tests:** Project RBAC contract tests

---

## Architecture & Special (2 crates)

**Purpose:** Architecture validation and specialized utilities.

### ironclaw_architecture
**Description:** Architecture boundary tests and enforcement for IronClaw Reborn

**Role:** Dependency and composition boundary validation
- Dependency graph checking
- Composition boundary tests
- Reborn vs v1 boundary enforcement
- Crate layer validation
- **When to touch:** Refactoring crate dependencies, adding new crate dependencies, or changing architecture boundaries
- **Key modules:** `lib.rs`, `tests/reborn_composition_boundaries.rs`, `tests/dependency_checks.rs`
- **Tests:** Run with `cargo test -p ironclaw_architecture --test '*'`
- **Depends on:** `ironclaw_common`

### ironclaw_silk_decoder
**Description:** Standalone helper that decodes WeChat raw SILK v3 voice notes to WAV

**Role:** Audio format conversion utility
- SILK v3 decoding (WeChat voice messages)
- WAV output formatting
- Isolated from main build (no libclang dependency)
- **When to touch:** Adding new audio formats, updating SILK decoder, or optimizing audio conversion
- **Key modules:** `lib.rs`, `decoder.rs`, `wav.rs`
- **Depends on:** silk_codec, `ironclaw_common`
- **Tests:** Audio format conversion tests

---

## Key Architectural Principles

### Dependency Flow
Dependencies flow **upward only** (no circular dependencies):
```
Products Layer (CLI, WebUI, Slack, Telegram)
        ↓
Userland Layer (Agent loops)
        ↓
Kernel Layer (Authorization, Safety, Approvals)
        ↓
Substrate Layer (Events, Filesystem, Memory, Threads)
```

### Trait-Driven Extensibility
Most crates expose a trait-based abstraction:
- `ironclaw_llm`: `LlmProvider` trait with multiple implementations
- `ironclaw_embeddings`: `EmbeddingProvider` trait
- `ironclaw_memory`: `MemoryProvider` trait
- `ironclaw_auth`: `AuthProvider` trait
- `ironclaw_filesystem`: `FileSystemBackend` trait (local, PostgreSQL, libSQL)

### Test-First Discipline
- 57 of 65 crates have tests (87.7%)
- Test-support feature for fakes and doubles
- Integration tests for runtime behavior
- E2E tests in `tests/e2e/` for user-visible flows

### Most Depended-Upon Crates (Core Infrastructure)
1. `ironclaw_host_api` — Used by 40+ crates
2. `ironclaw_common` — Used by 35+ crates
3. `ironclaw_filesystem` — Used by 25+ crates
4. `ironclaw_host_runtime` — Used by 15+ crates

### High-Churn Zones (Expect Frequent Changes)
- `ironclaw_reborn_composition` — Wiring and provider registration
- `ironclaw_host_runtime` — New effect types and sandbox policies
- `ironclaw_first_party_extensions` — New tools and capabilities

### Stable Core (Rare Changes)
- `ironclaw_host_api` — Core API contracts
- `ironclaw_common` — Shared types
- `ironclaw_runtime_policy` — Policy enforcement

---

## Where to Add New Features

See [Architecture Overview](./overview.md#where-to-build-new-features) for guidance on which crate to modify for your feature type.
