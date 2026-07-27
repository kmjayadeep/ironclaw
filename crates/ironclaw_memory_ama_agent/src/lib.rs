//! AMA-Agent causality-graph memory provider for IronClaw Reborn.
//!
//! A Rust port of the memory system proposed in *AMA-Bench: Evaluating
//! Long-Horizon Memory for Agentic Applications* (arXiv:2602.22769) and shipped
//! as that paper's reference implementation
//! (`github.com/AMA-Bench/AMA-Bench`, `src/method/ama_agent*`). It plugs into
//! the third-party [`ironclaw_memory::MemoryService`] provider lane opened by
//! PR #6345, so a deployment can bind it instead of native memory or mem0.
//!
//! # Why this exists
//!
//! It is the third arm of a memory-backend comparison: ironclaw's own native
//! memory, mem0, and the paper's causality-graph design, all measured on the
//! same suites with the same model. Because the reference implementation is
//! Python and has no service mode, a faithful comparison inside ironclaw
//! requires a real native provider rather than a subprocess wrapper.
//!
//! # Two-stage design (from the paper)
//!
//! 1. **Construction** — each recorded interaction is LLM-abstracted into
//!    environment-state / task-state nodes plus causal and association edges,
//!    merged into a per-scope [`graph::CausalityGraph`].
//! 2. **Retrieval** — embed the query, take the top-K nearest nodes, then ask
//!    the model whether that evidence suffices; on `NEED_GRAPH` expand via the
//!    graph, on `NEED_AGGREGATE` run a structured aggregation query.
//!
//! # Deliberate divergences from upstream (all measured, not hidden)
//!
//! - **`GraphRetrievalMode`** — the paper describes `NEED_GRAPH` as walking the
//!   causality graph, but the reference implementation's `retrieve.py` never
//!   consults `causal_graph`; it returns neighbouring turns by index. Both
//!   strategies are implemented and selectable
//!   ([`graph::GraphRetrievalMode`]) precisely so the difference can be
//!   measured instead of assumed.
//! - **`NEED_AGGREGATE` replaces `NEED_CODE`** — upstream generates Python and
//!   executes it in a subprocess that inherits the parent environment. This
//!   crate answers the same class of counting/listing/pattern queries with a
//!   fixed native menu ([`graph::AggregateQuery`]), so a memory provider adds
//!   no arbitrary-code-execution surface. Bounded, documented fidelity loss
//!   against a paper-reported ~23.5% of queries.
//! - **Incremental construction** — upstream builds memory in one batch pass
//!   over a finished trajectory; ironclaw delivers turns one at a time through
//!   `record_interaction`, so construction folds turns in incrementally.
//!
//! # Mapping fidelity
//!
//! Which [`ironclaw_memory::MemoryService`] operations this provider supports,
//! following the same convention as `ironclaw_memory_mem0`'s table:
//!
//! | IronClaw op          | AMA-Agent mapping                                    | fidelity |
//! |----------------------|------------------------------------------------------|----------|
//! | `record_interaction` | LLM extraction -> nodes/edges/turn merged into graph  | primary  |
//! | `retrieve_context`   | embed -> top-K -> sufficiency -> graph \| aggregate   | primary  |
//! | `search`             | stage-1 similarity retrieval, reshaped                | good     |
//! | `write`              | unsupported — no addressable-document model           | none     |
//! | `read`               | unsupported (same reason)                             | none     |
//! | `tree`               | unsupported (same reason)                             | none     |
//! | `profile_set/read`   | unsupported — no profile concept in the paper         | none     |
//!
//! Unsupported ops are left on the trait's own fail-closed defaults rather than
//! stubbed, so a caller gets `unavailable` instead of silently wrong data.

pub mod chat;
pub mod config;
pub mod embedding;
pub mod error;
pub mod graph;
pub mod llm;
pub mod service;
pub mod store;
mod url_check;

/// Extension id this provider binds under, mirroring
/// `ironclaw_memory_mem0::MEM0_MEMORY_EXTENSION_ID`. Referenced by
/// `[memory].provider` in reborn config and by the composition factory arm.
pub const AMA_AGENT_MEMORY_EXTENSION_ID: &str = "ama-agent.local.memory";

pub use chat::OpenAiCompatChat;
pub use config::AmaAgentConfig;
pub use embedding::{AmaEmbeddingProvider, OpenAiCompatEmbedder};
pub use error::AmaAgentError;
pub use graph::{
    AggregateAnswer, AggregateQuery, CausalityGraph, GraphEdge, GraphNode, GraphRetrievalMode,
    NodeId, NodeKind, TurnRecord,
};
pub use service::AmaAgentMemoryService;

#[cfg(any(test, feature = "test-support"))]
pub use embedding::HashEmbedder;
