//! The two LLM call sites the paper's design needs.
//!
//! 1. **Extraction** (construction time) — abstract one interaction into
//!    environment/task-state nodes plus causal and association edges. Adapted
//!    from upstream's `COMPRESS_PROMPT_TEMPLATE`.
//! 2. **Sufficiency judgment** (retrieval time) — decide whether the retrieved
//!    evidence answers the query, or whether to expand via the graph or run an
//!    aggregation. Adapted from upstream's
//!    `CHUNK_SUFFICIENCY_JUDGMENT_PROMPT_TEMPLATE`.
//!
//! # Prompts are adapted, not transcribed
//!
//! Upstream's prompts are tuned for a Python harness that parses loose marker
//! text (`**STATE_MEMORY**`, substring checks for `SUFFICIENT`/`NEED_`). We ask
//! for strict JSON instead and parse with `serde_json`, matching this codebase's
//! typed-contract convention. The *mechanism* is preserved (same extraction
//! targets, same three-way routing decision); the wire format is not.
//!
//! # Both call sites degrade rather than fail
//!
//! Memory must never break a turn. A malformed or failed extraction yields zero
//! nodes (the interaction is still recorded verbatim as a turn); a malformed or
//! failed judgment is treated as `Sufficient`, degrading to plain similarity
//! retrieval. This mirrors the trait's own contract, where `record_interaction`
//! defaults to an infallible no-op.

use std::sync::Arc;

use ironclaw_llm::LlmProvider;
use serde::Deserialize;

use crate::error::AmaAgentError;
use crate::graph::{GraphEdge, GraphNode, NodeId, NodeKind};

/// Extraction prompt. Mirrors upstream's compress template: pull out key state,
/// copy identifiers/commands/numbers/errors VERBATIM (never paraphrase them, or
/// later exact-recall questions become unanswerable), and emit one summary line.
const EXTRACTION_PROMPT: &str = r#"You are compressing one step of an agent trajectory into a state memory that a future reader can use to answer detailed questions about what happened.

Return ONLY a JSON object, no prose and no code fence, with this exact shape:
{
  "env_state": ["<environment/world facts observed at this step>"],
  "task_state": ["<progress, sub-goals, or plan state at this step>"],
  "causal_edges": [["<cause text>", "<effect text>"]],
  "association_edges": [["<text a>", "<text b>"]],
  "summary": "<one line describing this step's overall progress>"
}

Rules:
- Copy these VERBATIM, never paraphrased: commands, queries, code, file paths, URLs, table/column names, ids, entity names, numeric values, counts, dates, response codes, error messages.
- Record only task-relevant state. Discard boilerplate markup, stack-trace noise, and decoration.
- Every string in causal_edges/association_edges MUST exactly match one of the env_state/task_state strings.
- Use [] for any list with nothing to report. Never invent facts that are not present."#;

/// Sufficiency prompt. Mirrors upstream's three-way routing decision, but asks
/// for a JSON verdict rather than loose marker text.
const SUFFICIENCY_PROMPT: &str = r#"You are routing a question about an agent trajectory to the right retrieval stage.

You are shown a SMALL SUBSET of the trajectory, selected by similarity search — NOT the full history.

Return ONLY a JSON object, no prose and no code fence:
{ "verdict": "SUFFICIENT" | "NEED_GRAPH" | "NEED_AGGREGATE", "focus_turn": <int or null>, "keyword": "<string or null>" }

Choose:
- "NEED_AGGREGATE" if the question asks how many / how often / count / list all / every / total / a tally or pattern across many steps. Counting over this subset would UNDERCOUNT, so it must not be answered from it. Set "keyword" to the thing being counted.
- "NEED_GRAPH" if answering needs steps adjacent to, or causally connected with, what is shown (a precondition, a consequence, or the surrounding window). Set "focus_turn" to the step number to expand around.
- "SUFFICIENT" only if the shown evidence completely and accurately answers the question."#;

/// What the extraction call produced, already converted into graph shapes.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Extraction {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub summary: String,
}

/// The routing decision, mirroring upstream's three branches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Sufficient,
    /// Expand around `focus_turn` (turn-window mode) or from the seed nodes
    /// (edge-traversal mode).
    NeedGraph {
        focus_turn: Option<i64>,
    },
    /// Run a structured aggregation for `keyword`.
    NeedAggregate {
        keyword: Option<String>,
    },
}

#[derive(Deserialize)]
struct RawExtraction {
    #[serde(default)]
    env_state: Vec<String>,
    #[serde(default)]
    task_state: Vec<String>,
    #[serde(default)]
    causal_edges: Vec<Vec<String>>,
    #[serde(default)]
    association_edges: Vec<Vec<String>>,
    #[serde(default)]
    summary: String,
}

#[derive(Deserialize)]
struct RawVerdict {
    #[serde(default)]
    verdict: String,
    #[serde(default)]
    focus_turn: Option<i64>,
    #[serde(default)]
    keyword: Option<String>,
}

/// Wraps an injected [`LlmProvider`] with this provider's two prompts.
pub struct AmaLlm {
    provider: Arc<dyn LlmProvider>,
}

impl AmaLlm {
    pub fn new(provider: Arc<dyn LlmProvider>) -> Self {
        Self { provider }
    }

    /// Abstract one interaction into graph shapes.
    ///
    /// Returns `Ok(Extraction::default())` — NOT an error — when the model
    /// answers unparseably, so construction degrades to "recorded the turn,
    /// extracted no nodes" rather than failing the caller's turn.
    pub async fn extract(
        &self,
        task: &str,
        turn_idx: i64,
        action: &str,
        observation: &str,
    ) -> Extraction {
        let user = format!(
            "Task: {task}\n\nStep {turn_idx}\nAction: {action}\nObservation: {observation}"
        );
        let text = match self.complete(EXTRACTION_PROMPT, &user).await {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!("ama-agent extraction call failed, recording turn only: {e}");
                return Extraction::default();
            }
        };
        match parse_extraction(&text, turn_idx) {
            Some(x) => x,
            None => {
                tracing::debug!("ama-agent extraction was unparseable, recording turn only");
                Extraction::default()
            }
        }
    }

    /// Judge whether `evidence` answers `question`.
    ///
    /// Degrades to [`Verdict::Sufficient`] on failure or unparseable output, so a
    /// flaky judgment call can only cost retrieval quality, never the turn.
    pub async fn judge_sufficiency(&self, question: &str, evidence: &str) -> Verdict {
        let user = format!("Question: {question}\n\nRetrieved evidence (a subset):\n{evidence}");
        let text = match self.complete(SUFFICIENCY_PROMPT, &user).await {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!("ama-agent sufficiency call failed, assuming sufficient: {e}");
                return Verdict::Sufficient;
            }
        };
        parse_verdict(&text)
    }

    async fn complete(&self, system: &str, user: &str) -> Result<String, AmaAgentError> {
        use ironclaw_llm::{ChatMessage, CompletionRequest};
        let mut request =
            CompletionRequest::new(vec![ChatMessage::system(system), ChatMessage::user(user)]);
        // temperature 0 matches upstream's `configs/ama_agent.yaml`, so extraction
        // and routing are as deterministic as the provider allows.
        request.temperature = Some(0.0);
        let response = self
            .provider
            .complete(request)
            .await
            .map_err(|e| AmaAgentError::Llm(e.to_string()))?;
        Ok(response.content)
    }
}

/// Pull the first JSON object out of a model response.
///
/// Models wrap JSON in prose or fences despite instructions, so scan for the
/// outermost balanced `{...}` rather than trusting the whole body to parse.
/// String-aware so a brace inside a quoted value cannot end the scan early.
fn extract_json_object(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for (i, &c) in bytes.iter().enumerate().skip(start) {
        if in_string {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_string = false;
            }
            continue;
        }
        match c {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return text.get(start..=i);
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_extraction(text: &str, turn_idx: i64) -> Option<Extraction> {
    let raw: RawExtraction = serde_json::from_str(extract_json_object(text)?).ok()?;

    let mut out = Extraction {
        summary: raw.summary,
        ..Default::default()
    };
    let push = |text: &str, kind: NodeKind, out: &mut Extraction| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return;
        }
        out.nodes.push(GraphNode {
            id: NodeId::of_text(trimmed),
            kind,
            text: trimmed.to_string(),
            turn_idx,
            embedding: None,
        });
    };
    for s in &raw.env_state {
        push(s, NodeKind::EnvState, &mut out);
    }
    for s in &raw.task_state {
        push(s, NodeKind::TaskState, &mut out);
    }

    // Only admit an edge whose BOTH endpoints correspond to a node we extracted.
    // A hallucinated endpoint would otherwise create a dangling node id that
    // traversal follows into nothing.
    let known = |t: &str| -> Option<NodeId> {
        let id = NodeId::of_text(t.trim());
        out.nodes.iter().find(|n| n.id == id).map(|n| n.id)
    };
    for pair in &raw.causal_edges {
        if let [from, to] = pair.as_slice()
            && let (Some(f), Some(t)) = (known(from), known(to))
            && f != t
        {
            out.edges.push(GraphEdge::Causal { from: f, to: t });
        }
    }
    for pair in &raw.association_edges {
        if let [a, b] = pair.as_slice()
            && let (Some(x), Some(y)) = (known(a), known(b))
            && x != y
        {
            out.edges.push(GraphEdge::Association { a: x, b: y });
        }
    }
    Some(out)
}

fn parse_verdict(text: &str) -> Verdict {
    let Some(json) = extract_json_object(text) else {
        // Fall back to upstream's looser substring behavior before giving up —
        // a model that answered in prose still carried a usable signal.
        return loose_verdict(text);
    };
    match serde_json::from_str::<RawVerdict>(json) {
        Ok(raw) => match raw.verdict.trim().to_ascii_uppercase().as_str() {
            "NEED_GRAPH" => Verdict::NeedGraph {
                focus_turn: raw.focus_turn,
            },
            "NEED_AGGREGATE" | "NEED_CODE" => Verdict::NeedAggregate {
                keyword: raw.keyword.filter(|k| !k.trim().is_empty()),
            },
            _ => Verdict::Sufficient,
        },
        Err(_) => loose_verdict(text),
    }
}

/// Upstream's own classification style: substring match on the raw text.
fn loose_verdict(text: &str) -> Verdict {
    let upper = text.to_ascii_uppercase();
    if upper.contains("NEED_AGGREGATE") || upper.contains("NEED_CODE") {
        Verdict::NeedAggregate { keyword: None }
    } else if upper.contains("NEED_GRAPH") {
        Verdict::NeedGraph { focus_turn: None }
    } else {
        Verdict::Sufficient
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_nodes_edges_and_summary() {
        let raw = r#"{
          "env_state": ["the chest is locked", "a note reads code 1234"],
          "task_state": ["needs the code to open the chest"],
          "causal_edges": [["a note reads code 1234", "needs the code to open the chest"]],
          "association_edges": [["the chest is locked", "a note reads code 1234"]],
          "summary": "found a note while facing a locked chest"
        }"#;
        let x = parse_extraction(raw, 7).expect("parses");
        assert_eq!(x.nodes.len(), 3);
        assert_eq!(x.summary, "found a note while facing a locked chest");
        assert!(
            x.nodes.iter().all(|n| n.turn_idx == 7),
            "provenance stamped"
        );
        assert_eq!(
            x.nodes
                .iter()
                .filter(|n| n.kind == NodeKind::TaskState)
                .count(),
            1
        );
        assert_eq!(x.edges.len(), 2, "one causal + one association");
    }

    #[test]
    fn drops_edges_with_hallucinated_endpoints() {
        // "a fact never extracted" is not in env_state/task_state, so the edge
        // must be dropped rather than creating a dangling node id.
        let raw = r#"{
          "env_state": ["real fact"],
          "task_state": [],
          "causal_edges": [["real fact", "a fact never extracted"]],
          "association_edges": [["real fact", "real fact"]],
          "summary": "s"
        }"#;
        let x = parse_extraction(raw, 1).expect("parses");
        assert_eq!(x.nodes.len(), 1);
        assert!(
            x.edges.is_empty(),
            "dangling endpoint and self-edge must both be rejected"
        );
    }

    #[test]
    fn parses_json_wrapped_in_prose_or_a_fence() {
        let fenced = "Sure! Here you go:\n```json\n{\"env_state\":[\"x\"],\"summary\":\"s\"}\n```\nHope that helps.";
        let x = parse_extraction(fenced, 0).expect("must survive fences and prose");
        assert_eq!(x.nodes.len(), 1);
        assert_eq!(x.summary, "s");
    }

    #[test]
    fn json_scan_is_string_aware() {
        // A brace inside a quoted value must not terminate the object early.
        let tricky = r#"{"env_state":["contains a } brace"],"summary":"also { here"}"#;
        let x = parse_extraction(tricky, 0).expect("string-aware scan");
        assert_eq!(x.nodes[0].text, "contains a } brace");
        assert_eq!(x.summary, "also { here");
    }

    #[test]
    fn unparseable_extraction_yields_none_not_a_panic() {
        assert!(parse_extraction("total nonsense, no json", 0).is_none());
        assert!(parse_extraction("{ unbalanced", 0).is_none());
        // Valid JSON of the wrong shape degrades to empty rather than erroring.
        let x = parse_extraction(r#"{"unexpected":"shape"}"#, 0).expect("defaults apply");
        assert!(x.nodes.is_empty() && x.edges.is_empty());
    }

    #[test]
    fn verdict_parses_all_three_branches() {
        assert_eq!(
            parse_verdict(r#"{"verdict":"SUFFICIENT"}"#),
            Verdict::Sufficient
        );
        assert_eq!(
            parse_verdict(r#"{"verdict":"NEED_GRAPH","focus_turn":12}"#),
            Verdict::NeedGraph {
                focus_turn: Some(12)
            }
        );
        assert_eq!(
            parse_verdict(r#"{"verdict":"NEED_AGGREGATE","keyword":"retries"}"#),
            Verdict::NeedAggregate {
                keyword: Some("retries".into())
            }
        );
        // Upstream's own marker name still routes correctly.
        assert_eq!(
            parse_verdict(r#"{"verdict":"NEED_CODE"}"#),
            Verdict::NeedAggregate { keyword: None }
        );
    }

    #[test]
    fn verdict_degrades_to_sufficient_and_falls_back_to_substrings() {
        // No JSON at all, but a usable signal in prose.
        assert_eq!(
            parse_verdict("I think we NEED_GRAPH here"),
            Verdict::NeedGraph { focus_turn: None }
        );
        assert_eq!(
            parse_verdict("this requires NEED_AGGREGATE counting"),
            Verdict::NeedAggregate { keyword: None }
        );
        // Garbage must never block a turn — it degrades to plain similarity.
        assert_eq!(parse_verdict("¯\\_(ツ)_/¯"), Verdict::Sufficient);
        assert_eq!(parse_verdict(""), Verdict::Sufficient);
        // An empty keyword is normalized away rather than searched for.
        assert_eq!(
            parse_verdict(r#"{"verdict":"NEED_AGGREGATE","keyword":"  "}"#),
            Verdict::NeedAggregate { keyword: None }
        );
    }
}
