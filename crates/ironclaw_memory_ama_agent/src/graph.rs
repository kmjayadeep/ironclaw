//! The causality-graph data model and every retrieval primitive that operates
//! on it.
//!
//! Deliberately PURE: no async, no I/O, no LLM, no embedding calls. Everything
//! here is a plain function over in-memory data, so the retrieval semantics can
//! be unit-tested against hand-built fixtures without a network or a model. The
//! async/LLM/embedding/storage layers sit above this module and call into it.
//!
//! Ported from AMA-Bench's reference implementation
//! (`src/method/ama_agent_core/{construct,retrieve}.py`, arXiv:2602.22769).

use std::collections::{BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

/// Stable identifier for a graph node.
///
/// Derived from the node's normalized text so that re-extracting the same fact
/// from a re-recorded turn DEDUPES instead of appending a duplicate node. (The
/// upstream Python keeps a plain list and tolerates duplicates; we dedupe
/// because ironclaw's `record_interaction` can legitimately be re-invoked for
/// the same `turn_run_id` and the trait documents that re-recording a run must
/// overwrite idempotently rather than duplicate.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    /// FNV-1a over the normalized text. Chosen over SHA-2 because this is a
    /// dedupe/lookup key, never a security boundary, and it keeps the crate free
    /// of a hashing dependency.
    pub fn of_text(text: &str) -> Self {
        let normalized = normalize(text);
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in normalized.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        Self(hash)
    }
}

/// Lowercase + collapse whitespace. Used for node identity and for every
/// keyword/aggregation match, so `"Team  Echo"` and `"team echo"` are one thing.
pub fn normalize(s: &str) -> String {
    s.split_whitespace()
        .map(|w| w.to_lowercase())
        .collect::<Vec<_>>()
        .join(" ")
}

/// The two node families the paper's extraction step produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeKind {
    /// Environment state — what the world looks like (objects, positions, files,
    /// query results, error text).
    EnvState,
    /// Objective/task state — progress toward the goal, sub-goals, plans.
    TaskState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub id: NodeId,
    pub kind: NodeKind,
    /// Verbatim-preserved state/entity description. The extraction prompt asks
    /// the model to copy identifiers, commands, numbers and errors EXACTLY, so
    /// this must not be paraphrased downstream.
    pub text: String,
    /// Which trajectory turn this node was extracted from (provenance, and the
    /// join key for `TurnWindow` retrieval).
    pub turn_idx: i64,
    /// Latent-space position for similarity retrieval. `None` until embedded.
    pub embedding: Option<Vec<f32>>,
}

/// A directed causal dependency or an undirected association, matching the
/// paper's two edge families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphEdge {
    /// `from` caused / was a precondition of `to`.
    Causal { from: NodeId, to: NodeId },
    /// `a` and `b` co-occur / are associated, no direction implied.
    Association { a: NodeId, b: NodeId },
}

impl GraphEdge {
    /// Both endpoints, direction-agnostic — traversal walks causal edges in both
    /// directions because answering "what led to X" and "what did X lead to" are
    /// both legitimate memory queries.
    fn endpoints(&self) -> (NodeId, NodeId) {
        match *self {
            Self::Causal { from, to } => (from, to),
            Self::Association { a, b } => (a, b),
        }
    }
}

/// One recorded trajectory turn: the raw action/observation plus the one-line
/// summary the extraction prompt produces. This is also the index the
/// aggregation queries scan, standing in for the paper's generate-and-execute
/// Python fallback.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnRecord {
    pub turn_idx: i64,
    pub action: String,
    pub observation: String,
    pub summary: String,
    pub node_ids: Vec<NodeId>,
}

/// The whole per-scope memory: nodes, edges, and the turn index.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CausalityGraph {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub turns: Vec<TurnRecord>,
    /// `turn_run_id`s already folded in, so a re-recorded run is idempotent
    /// rather than duplicated.
    pub processed_turn_run_ids: BTreeSet<String>,
}

/// Which strategy answers a `NEED_GRAPH` verdict.
///
/// Both exist because the paper and its own reference implementation DISAGREE,
/// and we want to measure the difference rather than assume one:
/// - `EdgeTraversal` is what the paper describes (walk the causality graph).
/// - `TurnWindow` is what `retrieve.py` actually does today — it parses turn
///   indices out of the sufficiency verdict and returns neighbouring turns,
///   never consulting `causal_graph` at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphRetrievalMode {
    EdgeTraversal,
    TurnWindow,
}

impl Default for GraphRetrievalMode {
    /// Defaults to the paper's described design; the comparison run flips it.
    fn default() -> Self {
        Self::EdgeTraversal
    }
}

/// The fixed menu of structured aggregation queries that replaces the paper's
/// LLM-generated-and-executed Python (`NEED_CODE`).
///
/// The paper's own analysis says ~23.5% of queries take the code path, and what
/// those queries actually need is counting / listing / pattern-matching over
/// recorded turns — a closed set of operations. Enumerating them natively gets
/// that capability without embedding a Python interpreter (and a new
/// arbitrary-code-execution surface) inside a memory provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateQuery {
    /// How many turns mention `keyword`.
    CountTurnsMatching { keyword: String },
    /// How many nodes of a kind exist.
    CountNodes { kind: Option<NodeKind> },
    /// Distinct node texts of a kind (deduped, ordered by first appearance).
    ListDistinctNodes { kind: Option<NodeKind> },
    /// Turn indices whose action/observation/summary mention `keyword`.
    FindTurnsMatching { keyword: String },
    /// Every turn in an inclusive index range.
    TurnsInRange { start: i64, end: i64 },
}

/// Answer to an [`AggregateQuery`] — a short factual string plus the turns it
/// came from, so the caller can attach real evidence rather than a bare number.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateAnswer {
    pub summary: String,
    pub turn_idxs: Vec<i64>,
}

impl CausalityGraph {
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.turns.is_empty()
    }

    pub fn node(&self, id: NodeId) -> Option<&GraphNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub fn turn(&self, turn_idx: i64) -> Option<&TurnRecord> {
        self.turns.iter().find(|t| t.turn_idx == turn_idx)
    }

    /// Insert a node, merging into an existing one with the same id. An existing
    /// embedding is preserved when the incoming node has none, so re-extraction
    /// never silently drops an already-computed vector.
    pub fn upsert_node(&mut self, node: GraphNode) {
        if let Some(existing) = self.nodes.iter_mut().find(|n| n.id == node.id) {
            if node.embedding.is_some() {
                existing.embedding = node.embedding;
            }
            return;
        }
        self.nodes.push(node);
    }

    /// Insert an edge unless an identical one is already present.
    pub fn upsert_edge(&mut self, edge: GraphEdge) {
        if !self.edges.contains(&edge) {
            self.edges.push(edge);
        }
    }

    /// Insert/replace a turn record by `turn_idx`, keeping `turns` sorted so
    /// window queries can rely on ordering.
    pub fn upsert_turn(&mut self, turn: TurnRecord) {
        match self.turns.iter_mut().find(|t| t.turn_idx == turn.turn_idx) {
            Some(existing) => *existing = turn,
            None => {
                self.turns.push(turn);
                self.turns.sort_by_key(|t| t.turn_idx);
            }
        }
    }

    /// Stage 1 of retrieval: the `top_k` nodes closest to `query_embedding` by
    /// cosine similarity. Nodes without an embedding are skipped rather than
    /// treated as distance-zero, which would let un-embedded nodes crowd out
    /// real matches.
    pub fn top_k_by_similarity(&self, query_embedding: &[f32], top_k: usize) -> Vec<NodeId> {
        if query_embedding.is_empty() || top_k == 0 {
            return Vec::new();
        }
        let mut scored: Vec<(f32, NodeId)> = self
            .nodes
            .iter()
            .filter_map(|n| {
                let emb = n.embedding.as_deref()?;
                Some((cosine_similarity(query_embedding, emb), n.id))
            })
            .collect();
        // Descending by score; ties broken by NodeId so the result is
        // deterministic (important for reproducible benchmark runs).
        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        scored.into_iter().take(top_k).map(|(_, id)| id).collect()
    }

    /// `NEED_GRAPH` / `EdgeTraversal`: breadth-first expansion from `seeds` out
    /// to `depth` hops across BOTH edge families. This is the paper's described
    /// causality-aware retrieval.
    pub fn neighborhood(&self, seeds: &[NodeId], depth: usize) -> Vec<NodeId> {
        let mut seen: BTreeSet<NodeId> = seeds.iter().copied().collect();
        let mut order: Vec<NodeId> = seeds.to_vec();
        if depth == 0 {
            return order;
        }
        let mut frontier: VecDeque<(NodeId, usize)> =
            seeds.iter().map(|id| (*id, 0usize)).collect();
        while let Some((id, hops)) = frontier.pop_front() {
            if hops >= depth {
                continue;
            }
            for edge in &self.edges {
                let (a, b) = edge.endpoints();
                let next = if a == id {
                    b
                } else if b == id {
                    a
                } else {
                    continue;
                };
                if seen.insert(next) {
                    order.push(next);
                    frontier.push_back((next, hops + 1));
                }
            }
        }
        order
    }

    /// `NEED_GRAPH` / `TurnWindow`: the turns in `[turn_idx - before, turn_idx +
    /// after]`, matching what the reference implementation actually does.
    /// Returns existing turns only — a window running off either end is clamped
    /// rather than erroring.
    pub fn turn_window(&self, turn_idx: i64, before: i64, after: i64) -> Vec<&TurnRecord> {
        let lo = turn_idx.saturating_sub(before.max(0));
        let hi = turn_idx.saturating_add(after.max(0));
        self.turns
            .iter()
            .filter(|t| t.turn_idx >= lo && t.turn_idx <= hi)
            .collect()
    }

    /// The turn indices the given nodes came from, deduped and ordered. Bridges
    /// similarity retrieval (which returns nodes) into turn-window expansion
    /// (which needs turn indices).
    pub fn turn_idxs_of(&self, node_ids: &[NodeId]) -> Vec<i64> {
        let mut out: Vec<i64> = Vec::new();
        for id in node_ids {
            if let Some(n) = self.node(*id)
                && !out.contains(&n.turn_idx)
            {
                out.push(n.turn_idx);
            }
        }
        out.sort_unstable();
        out
    }

    /// `NEED_AGGREGATE`: run one structured query over the turn/node index.
    pub fn aggregate(&self, query: &AggregateQuery) -> AggregateAnswer {
        match query {
            AggregateQuery::CountTurnsMatching { keyword } => {
                let hits = self.matching_turn_idxs(keyword);
                AggregateAnswer {
                    summary: format!(
                        "{} turn(s) mention {keyword:?} (of {} recorded).",
                        hits.len(),
                        self.turns.len()
                    ),
                    turn_idxs: hits,
                }
            }
            AggregateQuery::CountNodes { kind } => {
                let n = self
                    .nodes
                    .iter()
                    .filter(|node| kind.is_none_or(|k| node.kind == k))
                    .count();
                let label = match kind {
                    Some(NodeKind::EnvState) => "environment-state",
                    Some(NodeKind::TaskState) => "task-state",
                    None => "total",
                };
                AggregateAnswer {
                    summary: format!("{n} {label} node(s) recorded."),
                    turn_idxs: Vec::new(),
                }
            }
            AggregateQuery::ListDistinctNodes { kind } => {
                let mut seen: Vec<&str> = Vec::new();
                for node in &self.nodes {
                    if kind.is_none_or(|k| node.kind == k)
                        && !seen.iter().any(|s| normalize(s) == normalize(&node.text))
                    {
                        seen.push(node.text.as_str());
                    }
                }
                AggregateAnswer {
                    summary: if seen.is_empty() {
                        "no matching nodes recorded.".to_string()
                    } else {
                        format!("{} distinct: {}", seen.len(), seen.join("; "))
                    },
                    turn_idxs: Vec::new(),
                }
            }
            AggregateQuery::FindTurnsMatching { keyword } => {
                let hits = self.matching_turn_idxs(keyword);
                AggregateAnswer {
                    summary: if hits.is_empty() {
                        format!("no turn mentions {keyword:?}.")
                    } else {
                        format!(
                            "turns mentioning {keyword:?}: {}",
                            hits.iter()
                                .map(i64::to_string)
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    },
                    turn_idxs: hits,
                }
            }
            AggregateQuery::TurnsInRange { start, end } => {
                let (lo, hi) = if start <= end {
                    (*start, *end)
                } else {
                    (*end, *start)
                };
                let hits: Vec<i64> = self
                    .turns
                    .iter()
                    .filter(|t| t.turn_idx >= lo && t.turn_idx <= hi)
                    .map(|t| t.turn_idx)
                    .collect();
                AggregateAnswer {
                    summary: format!("{} turn(s) in range {lo}..={hi}.", hits.len()),
                    turn_idxs: hits,
                }
            }
        }
    }

    fn matching_turn_idxs(&self, keyword: &str) -> Vec<i64> {
        let needle = normalize(keyword);
        if needle.is_empty() {
            return Vec::new();
        }
        self.turns
            .iter()
            .filter(|t| {
                normalize(&t.action).contains(&needle)
                    || normalize(&t.observation).contains(&needle)
                    || normalize(&t.summary).contains(&needle)
            })
            .map(|t| t.turn_idx)
            .collect()
    }
}

/// Cosine similarity, returning 0.0 for mismatched or zero-magnitude vectors
/// rather than NaN — a NaN would poison the sort and make retrieval
/// nondeterministic.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na <= 0.0 || nb <= 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(text: &str, kind: NodeKind, turn_idx: i64, emb: Option<Vec<f32>>) -> GraphNode {
        GraphNode {
            id: NodeId::of_text(text),
            kind,
            text: text.to_string(),
            turn_idx,
            embedding: emb,
        }
    }

    fn turn(idx: i64, action: &str, obs: &str, summary: &str) -> TurnRecord {
        TurnRecord {
            turn_idx: idx,
            action: action.to_string(),
            observation: obs.to_string(),
            summary: summary.to_string(),
            node_ids: Vec::new(),
        }
    }

    /// A linear causal chain: A -> B -> C, plus an unrelated island D.
    fn chain_graph() -> CausalityGraph {
        let a = node("took the key", NodeKind::TaskState, 1, Some(vec![1.0, 0.0]));
        let b = node(
            "chest unlocked",
            NodeKind::EnvState,
            2,
            Some(vec![0.9, 0.1]),
        );
        let c = node(
            "found the code",
            NodeKind::EnvState,
            3,
            Some(vec![0.0, 1.0]),
        );
        let d = node(
            "unrelated island",
            NodeKind::EnvState,
            9,
            Some(vec![-1.0, 0.0]),
        );
        let mut g = CausalityGraph::default();
        for n in [&a, &b, &c, &d] {
            g.upsert_node(n.clone());
        }
        g.upsert_edge(GraphEdge::Causal {
            from: a.id,
            to: b.id,
        });
        g.upsert_edge(GraphEdge::Causal {
            from: b.id,
            to: c.id,
        });
        for (i, (act, obs, sum)) in [
            ("take key", "you now hold the key", "picked up key"),
            ("unlock chest", "the chest opens", "chest unlocked"),
            ("read note", "the code is 1234", "found code"),
        ]
        .iter()
        .enumerate()
        {
            g.upsert_turn(turn(i as i64 + 1, act, obs, sum));
        }
        g.upsert_turn(turn(9, "wander", "nothing here", "no progress"));
        g
    }

    #[test]
    fn node_id_dedupes_on_normalized_text() {
        // Same fact, different spacing/case => one node, not two.
        assert_eq!(NodeId::of_text("Team  Echo"), NodeId::of_text("team echo"));
        assert_ne!(NodeId::of_text("team echo"), NodeId::of_text("team delta"));

        let mut g = CausalityGraph::default();
        g.upsert_node(node("Team  Echo", NodeKind::EnvState, 1, None));
        g.upsert_node(node("team echo", NodeKind::EnvState, 1, None));
        assert_eq!(g.nodes.len(), 1, "re-extraction must dedupe, not duplicate");
    }

    #[test]
    fn upsert_node_preserves_an_existing_embedding() {
        let mut g = CausalityGraph::default();
        g.upsert_node(node("x", NodeKind::EnvState, 1, Some(vec![1.0, 2.0])));
        // Re-extracted without an embedding: must NOT wipe the computed vector.
        g.upsert_node(node("x", NodeKind::EnvState, 1, None));
        assert_eq!(g.nodes[0].embedding.as_deref(), Some([1.0, 2.0].as_slice()));
    }

    #[test]
    fn top_k_ranks_by_cosine_and_skips_unembedded() {
        let mut g = chain_graph();
        g.upsert_node(node("no embedding here", NodeKind::EnvState, 4, None));
        let hits = g.top_k_by_similarity(&[1.0, 0.0], 2);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0], NodeId::of_text("took the key"), "closest first");
        assert_eq!(hits[1], NodeId::of_text("chest unlocked"));
        // The un-embedded node must never be admitted.
        assert!(!hits.contains(&NodeId::of_text("no embedding here")));
    }

    #[test]
    fn top_k_is_empty_for_empty_query_or_zero_k() {
        let g = chain_graph();
        assert!(g.top_k_by_similarity(&[], 5).is_empty());
        assert!(g.top_k_by_similarity(&[1.0, 0.0], 0).is_empty());
    }

    #[test]
    fn neighborhood_walks_causal_chain_to_depth_and_excludes_islands() {
        let g = chain_graph();
        let seed = vec![NodeId::of_text("took the key")];

        // depth 0 => seeds only.
        assert_eq!(g.neighborhood(&seed, 0), seed);

        // depth 1 => one hop (A -> B).
        let one = g.neighborhood(&seed, 1);
        assert!(one.contains(&NodeId::of_text("chest unlocked")));
        assert!(
            !one.contains(&NodeId::of_text("found the code")),
            "two hops away must not appear at depth 1"
        );

        // depth 2 => the whole chain, still excluding the disconnected island.
        let two = g.neighborhood(&seed, 2);
        assert!(two.contains(&NodeId::of_text("found the code")));
        assert!(
            !two.contains(&NodeId::of_text("unrelated island")),
            "an unconnected node must never be pulled in"
        );
    }

    #[test]
    fn neighborhood_traverses_causal_edges_in_both_directions() {
        // Answering "what led to C" must reach A, so traversal is not
        // direction-locked.
        let g = chain_graph();
        let back = g.neighborhood(&[NodeId::of_text("found the code")], 2);
        assert!(back.contains(&NodeId::of_text("took the key")));
    }

    #[test]
    fn turn_window_clamps_at_both_ends() {
        let g = chain_graph();
        let w = g.turn_window(2, 1, 1);
        assert_eq!(
            w.iter().map(|t| t.turn_idx).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // Running off the start is clamped, not an error.
        let w = g.turn_window(1, 5, 0);
        assert_eq!(w.iter().map(|t| t.turn_idx).collect::<Vec<_>>(), vec![1]);
        // A gap in turn indices is simply absent from the window.
        let w = g.turn_window(9, 3, 3);
        assert_eq!(w.iter().map(|t| t.turn_idx).collect::<Vec<_>>(), vec![9]);
    }

    #[test]
    fn turn_idxs_of_dedupes_and_sorts() {
        let g = chain_graph();
        let ids = vec![
            NodeId::of_text("found the code"),
            NodeId::of_text("took the key"),
            NodeId::of_text("took the key"),
        ];
        assert_eq!(g.turn_idxs_of(&ids), vec![1, 3]);
    }

    #[test]
    fn aggregate_counts_and_finds_turns_by_keyword() {
        let g = chain_graph();
        let a = g.aggregate(&AggregateQuery::CountTurnsMatching {
            keyword: "chest".into(),
        });
        // "unlock chest" (action) + "chest unlocked" (summary) => turn 2 only.
        assert_eq!(a.turn_idxs, vec![2]);
        assert!(a.summary.contains("1 turn(s)"));

        let f = g.aggregate(&AggregateQuery::FindTurnsMatching {
            keyword: "KEY".into(),
        });
        assert_eq!(f.turn_idxs, vec![1], "match must be case-insensitive");

        let none = g.aggregate(&AggregateQuery::FindTurnsMatching {
            keyword: "dragon".into(),
        });
        assert!(none.turn_idxs.is_empty());
        assert!(none.summary.contains("no turn"));
    }

    #[test]
    fn aggregate_counts_nodes_by_kind() {
        let g = chain_graph();
        assert!(
            g.aggregate(&AggregateQuery::CountNodes { kind: None })
                .summary
                .contains("4 total")
        );
        assert!(
            g.aggregate(&AggregateQuery::CountNodes {
                kind: Some(NodeKind::TaskState)
            })
            .summary
            .contains("1 task-state")
        );
    }

    #[test]
    fn aggregate_lists_distinct_nodes_and_range_is_order_agnostic() {
        let mut g = chain_graph();
        // A duplicate-by-normalization node must not appear twice in the list.
        g.upsert_node(node("Took The Key", NodeKind::TaskState, 1, None));
        let l = g.aggregate(&AggregateQuery::ListDistinctNodes {
            kind: Some(NodeKind::TaskState),
        });
        assert!(l.summary.starts_with("1 distinct:"), "got {}", l.summary);

        // Reversed bounds must behave the same as ordered ones.
        let fwd = g.aggregate(&AggregateQuery::TurnsInRange { start: 1, end: 3 });
        let rev = g.aggregate(&AggregateQuery::TurnsInRange { start: 3, end: 1 });
        assert_eq!(fwd.turn_idxs, vec![1, 2, 3]);
        assert_eq!(fwd.turn_idxs, rev.turn_idxs);
    }

    #[test]
    fn cosine_similarity_is_nan_free_on_degenerate_input() {
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(
            cosine_similarity(&[1.0], &[1.0, 2.0]),
            0.0,
            "length mismatch"
        );
        assert_eq!(
            cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]),
            0.0,
            "zero vector"
        );
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn upsert_turn_replaces_in_place_and_keeps_sorted() {
        let mut g = CausalityGraph::default();
        g.upsert_turn(turn(5, "a", "b", "c"));
        g.upsert_turn(turn(1, "a", "b", "c"));
        g.upsert_turn(turn(5, "REPLACED", "b", "c"));
        assert_eq!(g.turns.len(), 2, "same turn_idx replaces, not appends");
        assert_eq!(
            g.turns.iter().map(|t| t.turn_idx).collect::<Vec<_>>(),
            vec![1, 5],
            "turns stay sorted for window queries"
        );
        assert_eq!(g.turn(5).unwrap().action, "REPLACED");
    }
}
