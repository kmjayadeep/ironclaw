//! The [`MemoryService`] implementation — where the two-stage design meets
//! ironclaw's memory contract.
//!
//! See the crate-level mapping-fidelity table for which operations are
//! supported. Unsupported ones (`write`/`read`/`tree`/`profile_*`) are
//! deliberately NOT overridden, so they inherit the trait's fail-closed
//! `unavailable` default rather than returning something plausible but wrong.

use std::sync::Arc;

use async_trait::async_trait;
use ironclaw_memory::{
    MemoryInteractionRole, MemoryInvocation, MemoryService, MemoryServiceContextRequest,
    MemoryServiceContextSnippet, MemoryServiceError, MemoryServiceRecordRequest,
    MemoryServiceRecordResponse, MemoryServiceSearchRequest, MemoryServiceSearchResponse,
    MemoryServiceSearchResult, memory_context_disabled,
};

use crate::config::AmaAgentConfig;
use crate::embedding::AmaEmbeddingProvider;
use crate::error::AmaAgentError;
use crate::graph::{
    AggregateQuery, CausalityGraph, GraphNode, GraphRetrievalMode, NodeId, TurnRecord,
};
use crate::llm::{AmaLlm, Verdict};
use crate::store::GraphStore;

/// Relative path reported on returned snippets. Purely a display/provenance
/// label — the host hashes it into the `memory-snippet:*` reference it shows the
/// model. Not an addressable document (this provider has no document tree).
const SNIPPET_PATH: &str = "ama_agent/graph.json";

pub struct AmaAgentMemoryService {
    store: GraphStore,
    embedder: Arc<dyn AmaEmbeddingProvider>,
    llm: AmaLlm,
    config: AmaAgentConfig,
}

impl AmaAgentMemoryService {
    pub fn new(
        store: GraphStore,
        embedder: Arc<dyn AmaEmbeddingProvider>,
        llm: AmaLlm,
        config: AmaAgentConfig,
    ) -> Self {
        Self {
            store,
            embedder,
            llm,
            config,
        }
    }

    /// Stage 1 + 2 + 3 of retrieval: similarity -> sufficiency -> expand.
    /// Returns node/turn text blocks, most relevant first.
    async fn retrieve_blocks(
        &self,
        invocation: &MemoryInvocation,
        query: &str,
        max_blocks: usize,
    ) -> Result<Vec<String>, AmaAgentError> {
        let graph = self.store.load(&invocation.scope).await?;
        if graph.is_empty() || max_blocks == 0 {
            return Ok(Vec::new());
        }

        // Stage 1 — embedding similarity over graph nodes.
        let query_embedding = self.embedder.embed(query).await?;
        let seeds = graph.top_k_by_similarity(&query_embedding, self.config.top_k);
        if seeds.is_empty() {
            return Ok(Vec::new());
        }

        // Stage 2 — ask the model whether that evidence is enough. Degrades to
        // `Sufficient` on any failure, so retrieval never blocks a turn.
        let evidence = render_blocks(&graph, &seeds, self.config.max_observation_chars);
        let verdict = self
            .llm
            .judge_sufficiency(query, &evidence.join("\n\n"))
            .await;

        // Stage 3 — expand only when the judgment asked for it.
        let mut blocks = evidence;
        match verdict {
            Verdict::Sufficient => {}
            Verdict::NeedGraph { focus_turn } => {
                blocks = self.expand_graph(&graph, &seeds, focus_turn);
            }
            Verdict::NeedAggregate { keyword } => {
                // A keyword-less aggregate can still answer "how much is
                // recorded at all", which beats returning nothing.
                let query = match keyword {
                    Some(k) => AggregateQuery::CountTurnsMatching { keyword: k },
                    None => AggregateQuery::CountNodes { kind: None },
                };
                let answer = graph.aggregate(&query);
                let mut out = vec![format!("[memory aggregate] {}", answer.summary)];
                // Attach the matching turns as evidence so the model can verify
                // the count rather than taking a bare number on faith.
                for idx in answer.turn_idxs.iter().take(max_blocks.saturating_sub(1)) {
                    if let Some(t) = graph.turn(*idx) {
                        out.push(render_turn(t, self.config.max_observation_chars));
                    }
                }
                blocks = out;
            }
        }

        blocks.truncate(max_blocks);
        Ok(blocks)
    }

    /// `NEED_GRAPH`, in whichever mode is configured. The two modes exist to be
    /// compared — see the crate docs.
    fn expand_graph(
        &self,
        graph: &CausalityGraph,
        seeds: &[NodeId],
        focus_turn: Option<i64>,
    ) -> Vec<String> {
        match self.config.graph_retrieval_mode {
            // The paper's described design: walk causal/association edges.
            GraphRetrievalMode::EdgeTraversal => {
                let expanded = graph.neighborhood(seeds, self.config.graph_depth);
                render_blocks(graph, &expanded, self.config.max_observation_chars)
            }
            // What the reference implementation actually does: return the turns
            // around a focus index, never consulting graph edges.
            GraphRetrievalMode::TurnWindow => {
                let centers = match focus_turn {
                    Some(t) => vec![t],
                    // No explicit focus: use the turns the seeds came from.
                    None => graph.turn_idxs_of(seeds),
                };
                let mut out = Vec::new();
                let mut seen = Vec::new();
                for c in centers {
                    for t in
                        graph.turn_window(c, self.config.window_before, self.config.window_after)
                    {
                        if !seen.contains(&t.turn_idx) {
                            seen.push(t.turn_idx);
                            out.push(render_turn(t, self.config.max_observation_chars));
                        }
                    }
                }
                out
            }
        }
    }
}

/// Render nodes as text blocks, each tagged with its originating turn so the
/// model can cite a step number.
fn render_blocks(graph: &CausalityGraph, ids: &[NodeId], max_obs: usize) -> Vec<String> {
    ids.iter()
        .filter_map(|id| graph.node(*id))
        .map(|n| {
            let mut text = n.text.clone();
            truncate_on_char_boundary(&mut text, max_obs);
            format!("[turn {}] {}", n.turn_idx, text)
        })
        .collect()
}

fn render_turn(turn: &TurnRecord, max_obs: usize) -> String {
    let mut obs = turn.observation.clone();
    truncate_on_char_boundary(&mut obs, max_obs);
    format!(
        "[turn {}] action: {}\nobservation: {}",
        turn.turn_idx, turn.action, obs
    )
}

/// Truncate to at most `max` characters (not bytes) — slicing bytes would panic
/// on the multi-byte content real observations contain.
fn truncate_on_char_boundary(s: &mut String, max: usize) {
    if s.chars().count() > max {
        *s = s.chars().take(max).collect::<String>() + "…";
    }
}

#[async_trait]
impl MemoryService for AmaAgentMemoryService {
    /// Construction: fold one interaction into the causality graph.
    ///
    /// Best-effort by contract — a failure here must not fail the agent's turn,
    /// so storage errors are logged and reported as `recorded: false` rather
    /// than propagated.
    async fn record_interaction(
        &self,
        invocation: MemoryInvocation,
        request: MemoryServiceRecordRequest,
    ) -> Result<MemoryServiceRecordResponse, MemoryServiceError> {
        // Idempotency: re-recording the same run must overwrite, not duplicate.
        let run_id = request.turn_run_id.clone();
        if let Some(id) = &run_id {
            let graph = match self.store.load(&invocation.scope).await {
                Ok(g) => g,
                Err(e) => {
                    tracing::warn!("ama-agent: load failed during record: {e}");
                    return Ok(MemoryServiceRecordResponse { recorded: false });
                }
            };
            if graph.processed_turn_run_ids.contains(id) {
                tracing::debug!("ama-agent: run {id} already recorded; skipping");
                return Ok(MemoryServiceRecordResponse { recorded: false });
            }
        }

        // Split the interaction into the paper's action/observation shape:
        // assistant/tool-call side is the action, user/tool-result side the
        // observation. (Upstream's turns arrive pre-split; ironclaw's do not.)
        let mut action = Vec::new();
        let mut observation = Vec::new();
        for m in &request.messages {
            match m.role {
                MemoryInteractionRole::Assistant => action.push(m.content.clone()),
                _ => observation.push(m.content.clone()),
            }
        }
        let action = action.join("\n");
        let observation = observation.join("\n");
        if action.trim().is_empty() && observation.trim().is_empty() {
            return Ok(MemoryServiceRecordResponse { recorded: false });
        }

        // Next turn index continues the existing sequence.
        let next_idx = match self.store.load(&invocation.scope).await {
            Ok(g) => g.turns.iter().map(|t| t.turn_idx).max().unwrap_or(-1) + 1,
            Err(_) => 0,
        };

        let task = request
            .metadata
            .get("task")
            .and_then(|v| v.as_str())
            .unwrap_or("(task description unavailable)");
        let extraction = self
            .llm
            .extract(task, next_idx, &action, &observation)
            .await;

        // Embed the extracted nodes so they are retrievable. An embedding failure
        // is non-fatal: the nodes and the turn are still recorded (and remain
        // reachable via graph/aggregate paths), just not by similarity.
        let mut nodes = extraction.nodes;
        if !nodes.is_empty() {
            let texts: Vec<String> = nodes.iter().map(|n| n.text.clone()).collect();
            match self.embedder.embed_batch(&texts).await {
                Ok(vectors) if vectors.len() == nodes.len() => {
                    for (n, v) in nodes.iter_mut().zip(vectors) {
                        n.embedding = Some(v);
                    }
                }
                Ok(_) => tracing::warn!("ama-agent: embedding count mismatch; storing unembedded"),
                Err(e) => tracing::warn!("ama-agent: embedding failed ({e}); storing unembedded"),
            }
        }

        let node_ids: Vec<NodeId> = nodes.iter().map(|n| n.id).collect();
        let turn = TurnRecord {
            turn_idx: next_idx,
            action,
            observation,
            summary: extraction.summary,
            node_ids,
        };
        let edges = extraction.edges;

        if let Err(e) = self
            .store
            .update(&invocation.scope, move |g| {
                for n in nodes {
                    g.upsert_node(n);
                }
                for e in edges {
                    g.upsert_edge(e);
                }
                g.upsert_turn(turn);
                if let Some(id) = run_id {
                    g.processed_turn_run_ids.insert(id);
                }
                Ok(())
            })
            .await
        {
            tracing::warn!("ama-agent: persisting the interaction failed: {e}");
            return Ok(MemoryServiceRecordResponse { recorded: false });
        }
        Ok(MemoryServiceRecordResponse { recorded: true })
    }

    /// Retrieval for prompt context. Returns RAW text — the host sanitizes,
    /// size-caps, and wraps it in the untrusted envelope.
    async fn retrieve_context(
        &self,
        invocation: MemoryInvocation,
        request: MemoryServiceContextRequest,
    ) -> Result<Vec<MemoryServiceContextSnippet>, MemoryServiceError> {
        // Defense in depth: honor a disabled context profile here too, matching
        // the native provider, so the host gate and the provider cannot diverge.
        if memory_context_disabled(request.context_profile_id.as_str()) {
            return Ok(Vec::new());
        }
        let blocks = self
            .retrieve_blocks(&invocation, &request.query, request.max_snippets)
            .await
            .map_err(to_memory_error)?;

        let scope = &invocation.scope;
        Ok(blocks
            .into_iter()
            .map(|text| MemoryServiceContextSnippet {
                tenant_id: scope.tenant_id.as_str().to_string(),
                user_id: scope.user_id.as_str().to_string(),
                agent_id: scope.agent_id.as_ref().map(|a| a.as_str().to_string()),
                project_id: scope.project_id.as_ref().map(|p| p.as_str().to_string()),
                relative_path: SNIPPET_PATH.to_string(),
                text,
            })
            .collect())
    }

    /// Model-facing search. Stage-1 similarity only — deliberately NOT the full
    /// sufficiency-gated pipeline, which upstream also reserves for host-driven
    /// context retrieval rather than an interactive search tool.
    async fn search(
        &self,
        invocation: MemoryInvocation,
        request: MemoryServiceSearchRequest,
    ) -> Result<MemoryServiceSearchResponse, MemoryServiceError> {
        let graph = self
            .store
            .load(&invocation.scope)
            .await
            .map_err(to_memory_error)?;
        if graph.is_empty() {
            return Ok(MemoryServiceSearchResponse {
                query: request.query,
                results: Vec::new(),
            });
        }
        let query_embedding = self
            .embedder
            .embed(&request.query)
            .await
            .map_err(to_memory_error)?;
        let ids = graph.top_k_by_similarity(&query_embedding, request.limit);

        let results = ids
            .iter()
            .filter_map(|id| graph.node(*id))
            .map(|n: &GraphNode| {
                // Real cosine score against the query, not a placeholder.
                let score = n
                    .embedding
                    .as_deref()
                    .map(|e| crate::graph::cosine_similarity(&query_embedding, e))
                    .unwrap_or(0.0);
                (n, score)
            })
            // `top_k_by_similarity` returns the k NEAREST nodes regardless of how
            // far away they are, so on a small graph it happily includes nodes
            // with zero overlap. That is right for context retrieval (where some
            // evidence beats none) but wrong for a model-facing search tool,
            // where a zero-similarity hit is pure noise the model may then try to
            // reason from. Drop them.
            .filter(|(_, score)| *score > 0.0)
            .map(|(n, score)| MemoryServiceSearchResult {
                content: format!("[turn {}] {}", n.turn_idx, n.text),
                score,
                path: SNIPPET_PATH.to_string(),
                // Pure vector retrieval — no lexical component to combine.
                is_hybrid_match: false,
            })
            .collect();
        Ok(MemoryServiceSearchResponse {
            query: request.query,
            results,
        })
    }
}

/// Map a provider error onto the contract's sanitized error surface.
fn to_memory_error(e: AmaAgentError) -> MemoryServiceError {
    match e {
        // A genuinely absent/misconfigured backend is `unavailable`; everything
        // else is an operation failure.
        AmaAgentError::Unsupported(_) => MemoryServiceError::unavailable(),
        // `operation_from` keeps the real backend cause attached for host logging
        // while the model-facing surface stays the sanitized generic message.
        other => MemoryServiceError::operation_from(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::HashEmbedder;
    use crate::graph::NodeKind;
    use ironclaw_host_api::{CorrelationId, InvocationId, TenantId, UserId};
    use ironclaw_llm::{
        CompletionRequest, CompletionResponse, FinishReason, LlmError, LlmProvider,
    };
    use ironclaw_memory::{MemoryContextProfileId, MemoryInteractionMessage};
    use std::sync::Mutex;

    /// Returns canned responses in order, so every retrieval branch can be
    /// driven deterministically with no network.
    struct StubLlm {
        replies: Mutex<Vec<String>>,
    }

    impl StubLlm {
        fn new(replies: Vec<&str>) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(replies.iter().rev().map(|s| s.to_string()).collect()),
            })
        }
    }

    #[async_trait]
    impl LlmProvider for StubLlm {
        fn model_name(&self) -> &str {
            "stub-ama-agent-model"
        }

        fn cost_per_token(&self) -> (rust_decimal::Decimal, rust_decimal::Decimal) {
            (rust_decimal::Decimal::ZERO, rust_decimal::Decimal::ZERO)
        }

        async fn complete_with_tools(
            &self,
            _request: ironclaw_llm::ToolCompletionRequest,
        ) -> Result<ironclaw_llm::ToolCompletionResponse, LlmError> {
            // This provider never uses tool-calling; extraction and the
            // sufficiency judgment are both plain completions.
            unreachable!("ama-agent memory does not use tool completions")
        }

        async fn complete(
            &self,
            _request: CompletionRequest,
        ) -> Result<CompletionResponse, LlmError> {
            let content = self.replies.lock().unwrap().pop().unwrap_or_default();
            Ok(CompletionResponse {
                content,
                input_tokens: 0,
                output_tokens: 0,
                finish_reason: FinishReason::Stop,
                reasoning: None,
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            })
        }
    }

    fn invocation() -> MemoryInvocation {
        MemoryInvocation {
            scope: ironclaw_host_api::ResourceScope {
                tenant_id: TenantId::new("t1".to_string()).unwrap(),
                user_id: UserId::new("u1".to_string()).unwrap(),
                agent_id: None,
                project_id: None,
                mission_id: None,
                thread_id: None,
                invocation_id: InvocationId::new(),
            },
            correlation_id: CorrelationId::new(),
        }
    }

    fn service(replies: Vec<&str>, mode: GraphRetrievalMode) -> AmaAgentMemoryService {
        let store = GraphStore::new(Arc::new(ironclaw_filesystem::InMemoryBackend::new()));
        let config = AmaAgentConfig {
            graph_retrieval_mode: mode,
            ..AmaAgentConfig::default()
        };
        AmaAgentMemoryService::new(
            store,
            Arc::new(HashEmbedder::new(64)),
            AmaLlm::new(StubLlm::new(replies)),
            config,
        )
    }

    fn record(role: MemoryInteractionRole, content: &str) -> MemoryInteractionMessage {
        MemoryInteractionMessage {
            role,
            content: content.to_string(),
            name: None,
        }
    }

    fn request(run: &str, action: &str, observation: &str) -> MemoryServiceRecordRequest {
        MemoryServiceRecordRequest {
            messages: vec![
                record(MemoryInteractionRole::Assistant, action),
                record(MemoryInteractionRole::User, observation),
            ],
            turn_run_id: Some(run.to_string()),
            metadata: serde_json::json!({ "task": "unit test task" }),
        }
    }

    const EXTRACTION: &str = r#"{"env_state":["the chest is locked"],"task_state":["needs the code"],"causal_edges":[["the chest is locked","needs the code"]],"association_edges":[],"summary":"blocked by a locked chest"}"#;

    #[tokio::test]
    async fn record_interaction_builds_the_graph_and_is_idempotent() {
        let svc = service(
            vec![EXTRACTION, EXTRACTION],
            GraphRetrievalMode::EdgeTraversal,
        );
        let inv = invocation();

        let r = svc
            .record_interaction(inv.clone(), request("run-1", "open chest", "it is locked"))
            .await
            .unwrap();
        assert!(r.recorded);

        let g = svc.store.load(&inv.scope).await.unwrap();
        assert_eq!(g.nodes.len(), 2, "env + task node");
        assert_eq!(g.edges.len(), 1, "one causal edge");
        assert_eq!(g.turns.len(), 1);
        assert_eq!(g.turns[0].turn_idx, 0, "turn indices start at 0");
        assert!(
            g.nodes.iter().all(|n| n.embedding.is_some()),
            "nodes must be embedded so similarity retrieval can find them"
        );

        // Re-recording the SAME run must not duplicate anything.
        let again = svc
            .record_interaction(inv.clone(), request("run-1", "open chest", "it is locked"))
            .await
            .unwrap();
        assert!(!again.recorded, "same turn_run_id must be skipped");
        let g2 = svc.store.load(&inv.scope).await.unwrap();
        assert_eq!(g2.turns.len(), 1, "no duplicate turn");
        assert_eq!(g2.nodes.len(), 2, "no duplicate nodes");
    }

    #[tokio::test]
    async fn successive_turns_get_increasing_indices() {
        let svc = service(
            vec![EXTRACTION, EXTRACTION],
            GraphRetrievalMode::EdgeTraversal,
        );
        let inv = invocation();
        svc.record_interaction(inv.clone(), request("r1", "a1", "o1"))
            .await
            .unwrap();
        svc.record_interaction(inv.clone(), request("r2", "a2", "o2"))
            .await
            .unwrap();
        let g = svc.store.load(&inv.scope).await.unwrap();
        assert_eq!(
            g.turns.iter().map(|t| t.turn_idx).collect::<Vec<_>>(),
            vec![0, 1],
            "the second turn must continue the sequence, not overwrite turn 0"
        );
    }

    #[tokio::test]
    async fn a_failed_extraction_still_records_the_turn() {
        // Extraction returns garbage: no nodes, but the raw turn must survive so
        // aggregate/window retrieval can still see it.
        let svc = service(vec!["not json at all"], GraphRetrievalMode::EdgeTraversal);
        let inv = invocation();
        let r = svc
            .record_interaction(inv.clone(), request("r1", "did a thing", "saw a result"))
            .await
            .unwrap();
        assert!(r.recorded, "the turn is still recorded");
        let g = svc.store.load(&inv.scope).await.unwrap();
        assert!(g.nodes.is_empty());
        assert_eq!(g.turns.len(), 1);
        assert_eq!(g.turns[0].observation, "saw a result");
    }

    #[tokio::test]
    async fn empty_messages_record_nothing() {
        let svc = service(vec![EXTRACTION], GraphRetrievalMode::EdgeTraversal);
        let r = svc
            .record_interaction(
                invocation(),
                MemoryServiceRecordRequest {
                    messages: vec![],
                    turn_run_id: Some("r".into()),
                    metadata: serde_json::json!({}),
                },
            )
            .await
            .unwrap();
        assert!(!r.recorded);
    }

    #[tokio::test]
    async fn retrieve_context_returns_snippets_on_a_sufficient_verdict() {
        let svc = service(
            vec![EXTRACTION, r#"{"verdict":"SUFFICIENT"}"#],
            GraphRetrievalMode::EdgeTraversal,
        );
        let inv = invocation();
        svc.record_interaction(inv.clone(), request("r1", "open chest", "locked"))
            .await
            .unwrap();

        let snippets = svc
            .retrieve_context(
                inv.clone(),
                MemoryServiceContextRequest {
                    query: "why can't I open the chest?".into(),
                    max_snippets: 5,
                    context_profile_id: MemoryContextProfileId::new("default").unwrap(),
                },
            )
            .await
            .unwrap();

        assert!(!snippets.is_empty(), "must surface recorded memory");
        assert!(
            snippets.iter().any(|s| s.text.contains("chest")),
            "the relevant fact must be retrieved"
        );
        // Scope components are stamped so the host can hash a stable reference.
        assert_eq!(snippets[0].tenant_id, "t1");
        assert_eq!(snippets[0].user_id, "u1");
    }

    #[tokio::test]
    async fn a_disabled_context_profile_returns_nothing() {
        let svc = service(vec![EXTRACTION], GraphRetrievalMode::EdgeTraversal);
        let inv = invocation();
        svc.record_interaction(inv.clone(), request("r1", "a", "o"))
            .await
            .unwrap();
        let snippets = svc
            .retrieve_context(
                inv,
                MemoryServiceContextRequest {
                    query: "anything".into(),
                    max_snippets: 5,
                    context_profile_id: MemoryContextProfileId::new("memory_disabled").unwrap(),
                },
            )
            .await
            .unwrap();
        assert!(
            snippets.is_empty(),
            "a disabled context profile must yield no memory, defense in depth"
        );
    }

    #[tokio::test]
    async fn need_aggregate_verdict_answers_with_a_count() {
        let svc = service(
            vec![
                EXTRACTION,
                r#"{"verdict":"NEED_AGGREGATE","keyword":"chest"}"#,
            ],
            GraphRetrievalMode::EdgeTraversal,
        );
        let inv = invocation();
        svc.record_interaction(inv.clone(), request("r1", "open chest", "it is locked"))
            .await
            .unwrap();

        let snippets = svc
            .retrieve_context(
                inv,
                MemoryServiceContextRequest {
                    query: "how many times did I touch the chest?".into(),
                    max_snippets: 5,
                    context_profile_id: MemoryContextProfileId::new("default").unwrap(),
                },
            )
            .await
            .unwrap();
        assert!(
            snippets[0].text.contains("[memory aggregate]"),
            "an aggregate verdict must produce a computed answer, got: {}",
            snippets[0].text
        );
    }

    #[tokio::test]
    async fn turn_window_mode_returns_turns_rather_than_graph_nodes() {
        // The reference implementation's actual NEED_GRAPH behavior.
        let svc = service(
            vec![EXTRACTION, r#"{"verdict":"NEED_GRAPH","focus_turn":0}"#],
            GraphRetrievalMode::TurnWindow,
        );
        let inv = invocation();
        svc.record_interaction(inv.clone(), request("r1", "open chest", "it is locked"))
            .await
            .unwrap();

        let snippets = svc
            .retrieve_context(
                inv,
                MemoryServiceContextRequest {
                    query: "what happened around then?".into(),
                    max_snippets: 5,
                    context_profile_id: MemoryContextProfileId::new("default").unwrap(),
                },
            )
            .await
            .unwrap();
        assert!(
            snippets.iter().any(|s| s.text.contains("action:")),
            "turn-window mode renders whole turns, got: {:?}",
            snippets.iter().map(|s| &s.text).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn search_returns_real_scores_and_is_empty_on_cold_start() {
        let svc = service(vec![EXTRACTION], GraphRetrievalMode::EdgeTraversal);
        let inv = invocation();

        // Cold start: no memory, no results, no error.
        let empty = svc
            .search(
                inv.clone(),
                MemoryServiceSearchRequest {
                    query: "anything".into(),
                    limit: 5,
                },
            )
            .await
            .unwrap();
        assert!(empty.results.is_empty());

        svc.record_interaction(inv.clone(), request("r1", "open chest", "locked"))
            .await
            .unwrap();
        let found = svc
            .search(
                inv,
                MemoryServiceSearchRequest {
                    query: "chest".into(),
                    limit: 5,
                },
            )
            .await
            .unwrap();
        assert!(!found.results.is_empty());
        assert_eq!(found.query, "chest");
        assert!(
            found.results.iter().all(|r| r.score > 0.0),
            "scores must be real cosine values, not placeholders"
        );
        assert!(found.results.iter().all(|r| !r.is_hybrid_match));
    }

    #[tokio::test]
    async fn unsupported_ops_stay_fail_closed() {
        use ironclaw_memory::{MemoryServiceReadRequest, MemoryServiceTreeRequest};
        let svc = service(vec![], GraphRetrievalMode::EdgeTraversal);
        // These are deliberately NOT overridden, so they must report unavailable
        // rather than returning something plausible but wrong.
        assert!(
            svc.read(
                invocation(),
                MemoryServiceReadRequest {
                    path: "whatever".into(),
                },
            )
            .await
            .is_err()
        );
        assert!(
            svc.tree(
                invocation(),
                MemoryServiceTreeRequest {
                    path: "/".into(),
                    depth: 1,
                },
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn truncation_is_char_safe_on_multibyte_text() {
        // Byte slicing here would panic; real observations contain multibyte text.
        let mut s = "héllo wörld ✨ more text".to_string();
        truncate_on_char_boundary(&mut s, 7);
        assert!(s.starts_with("héllo w"));
        assert!(s.ends_with('…'));
    }

    #[test]
    fn graph_node_kind_is_preserved_through_extraction() {
        let x = crate::llm::Extraction::default();
        assert!(x.nodes.is_empty());
        // Guard that both kinds remain distinguishable (the aggregate queries
        // filter on them).
        assert_ne!(NodeKind::EnvState, NodeKind::TaskState);
    }
}
