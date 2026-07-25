//! Behavior knobs for the provider.
//!
//! Defaults track upstream's `configs/ama_agent.yaml` where an equivalent
//! setting exists, so a comparison run starts from the paper's own operating
//! point rather than values invented here.

use serde::{Deserialize, Serialize};

use crate::graph::GraphRetrievalMode;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AmaAgentConfig {
    /// Stage-1 similarity retrieval width. Upstream `top_k: 5`.
    pub top_k: usize,

    /// Which strategy answers a `NEED_GRAPH` verdict. See
    /// [`GraphRetrievalMode`] — the paper and its reference implementation
    /// disagree, so this is selectable in order to measure the difference.
    pub graph_retrieval_mode: GraphRetrievalMode,

    /// Hops to expand in `EdgeTraversal` mode. 2 keeps a precondition and its
    /// consequence reachable without pulling in the whole graph.
    pub graph_depth: usize,

    /// Turns before/after the focus index in `TurnWindow` mode.
    pub window_before: i64,
    pub window_after: i64,

    /// Per-block character cap when rendering evidence. Upstream caps rendered
    /// observations at 3000 chars in `_format_chunks`.
    pub max_observation_chars: usize,
}

impl Default for AmaAgentConfig {
    fn default() -> Self {
        Self {
            top_k: 5,
            graph_retrieval_mode: GraphRetrievalMode::default(),
            graph_depth: 2,
            window_before: 2,
            window_after: 2,
            max_observation_chars: 3000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_papers_operating_point() {
        let c = AmaAgentConfig::default();
        assert_eq!(c.top_k, 5, "upstream configs/ama_agent.yaml top_k");
        assert_eq!(
            c.max_observation_chars, 3000,
            "upstream _format_chunks observation cap"
        );
        // Default to the paper's described design, not the reference impl's
        // actual turn-window behavior; the comparison flips this deliberately.
        assert_eq!(c.graph_retrieval_mode, GraphRetrievalMode::EdgeTraversal);
    }

    #[test]
    fn config_round_trips_and_rejects_unknown_keys() {
        let toml_like = serde_json::json!({
            "top_k": 8,
            "graph_retrieval_mode": "TurnWindow",
            "graph_depth": 1,
            "window_before": 3,
            "window_after": 1,
            "max_observation_chars": 500
        });
        let c: AmaAgentConfig = serde_json::from_value(toml_like).unwrap();
        assert_eq!(c.top_k, 8);
        assert_eq!(c.graph_retrieval_mode, GraphRetrievalMode::TurnWindow);

        // A typo'd key must fail loudly rather than silently running defaults —
        // otherwise a mis-typed comparison arm reports the wrong configuration.
        let bad = serde_json::json!({ "top_kk": 8 });
        assert!(serde_json::from_value::<AmaAgentConfig>(bad).is_err());
    }
}
