//! Per-scope persistence of the causality graph.
//!
//! Stores the whole [`CausalityGraph`] as one JSON document under a
//! scope-derived path, read-modify-write behind a mutex — the same shape
//! `ironclaw_memory_native` uses for its own `context/profile.json`, and reusing
//! the same [`ironclaw_filesystem::RootFilesystem`] substrate.
//!
//! # Scale ceiling (deliberate, documented)
//!
//! Every read and write loads and re-serializes the entire per-scope graph, and
//! similarity search is a linear scan. That is fine at benchmark scale (one
//! suite run's worth of interactions per scope) and is emphatically NOT a
//! general-purpose storage engine — a long-lived production memory would need an
//! index and incremental persistence. Sized for the comparison this crate
//! exists to run.

use std::sync::Arc;

use ironclaw_filesystem::RootFilesystem;
// `VirtualPath` is a host-api substrate type, not a filesystem one.
use ironclaw_host_api::{ResourceScope, VirtualPath};
use tokio::sync::Mutex;

use crate::error::AmaAgentError;
use crate::graph::CausalityGraph;

/// Loads/saves per-scope graphs through the host filesystem substrate.
pub struct GraphStore {
    filesystem: Arc<dyn RootFilesystem>,
    /// Serializes read-modify-write so two concurrent `record_interaction` calls
    /// on the same scope cannot lose one another's nodes. Coarse (one lock for
    /// all scopes) because benchmark concurrency is low and correctness here
    /// matters more than contention.
    write_lock: Mutex<()>,
}

impl GraphStore {
    pub fn new(filesystem: Arc<dyn RootFilesystem>) -> Self {
        Self {
            filesystem,
            write_lock: Mutex::new(()),
        }
    }

    /// Scope-derived storage path.
    ///
    /// Mirrors the native provider's layout convention
    /// (`/memory/tenants/{t}/users/{u}/agents/{a}/projects/{p}/...`) so a
    /// deployment's memory tree stays legible regardless of which provider wrote
    /// it. `_none` stands in for absent optional axes, matching native, so two
    /// different scopes can never collide on one path.
    fn graph_path(scope: &ResourceScope) -> Result<VirtualPath, AmaAgentError> {
        let agent = scope
            .agent_id
            .as_ref()
            .map(|a| a.as_str().to_string())
            .unwrap_or_else(|| "_none".to_string());
        let project = scope
            .project_id
            .as_ref()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "_none".to_string());
        let raw = format!(
            "/memory/tenants/{}/users/{}/agents/{}/projects/{}/ama_agent/graph.json",
            scope.tenant_id.as_str(),
            scope.user_id.as_str(),
            agent,
            project,
        );
        VirtualPath::new(&raw).map_err(|e| AmaAgentError::Storage(format!("bad graph path: {e}")))
    }

    /// Load the scope's graph, or an empty one when nothing is stored yet.
    ///
    /// A corrupt/unparseable document is a hard error rather than a silent reset:
    /// silently starting over would look like "the agent forgot everything" and
    /// would be indistinguishable from a real recall failure in a benchmark.
    pub async fn load(&self, scope: &ResourceScope) -> Result<CausalityGraph, AmaAgentError> {
        let path = Self::graph_path(scope)?;
        match self.filesystem.read_file(&path).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                AmaAgentError::Storage(format!("graph.json is not valid graph JSON: {e}"))
            }),
            // Absent == empty memory, the normal cold-start path.
            Err(_) => Ok(CausalityGraph::default()),
        }
    }

    /// Read-modify-write the scope's graph under the lock.
    ///
    /// `mutate` receives the loaded graph and may edit it in place; the result is
    /// persisted only if `mutate` returns `Ok`. Taking a closure (rather than
    /// exposing load/save separately) makes it impossible for a caller to
    /// accidentally write back a graph it loaded before someone else's write.
    pub async fn update<F>(&self, scope: &ResourceScope, mutate: F) -> Result<(), AmaAgentError>
    where
        F: FnOnce(&mut CausalityGraph) -> Result<(), AmaAgentError> + Send,
    {
        let _guard = self.write_lock.lock().await;
        let mut graph = self.load(scope).await?;
        mutate(&mut graph)?;
        let bytes = serde_json::to_vec(&graph)
            .map_err(|e| AmaAgentError::Storage(format!("serialize graph: {e}")))?;
        let path = Self::graph_path(scope)?;
        self.filesystem
            .write_file(&path, &bytes)
            .await
            .map_err(|e| AmaAgentError::Storage(format!("write graph.json: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{GraphNode, NodeId, NodeKind};
    use ironclaw_host_api::{AgentId, CorrelationId, InvocationId, ProjectId, TenantId, UserId};

    fn scope(
        tenant: &str,
        user: &str,
        agent: Option<&str>,
        project: Option<&str>,
    ) -> ResourceScope {
        ResourceScope {
            tenant_id: TenantId::new(tenant.to_string()).unwrap(),
            user_id: UserId::new(user.to_string()).unwrap(),
            agent_id: agent.map(|a| AgentId::new(a.to_string()).unwrap()),
            project_id: project.map(|p| ProjectId::new(p.to_string()).unwrap()),
            mission_id: None,
            thread_id: None,
            invocation_id: InvocationId::new(),
        }
    }

    fn node(text: &str) -> GraphNode {
        GraphNode {
            id: NodeId::of_text(text),
            kind: NodeKind::EnvState,
            text: text.to_string(),
            turn_idx: 0,
            embedding: None,
        }
    }

    fn store() -> GraphStore {
        GraphStore::new(Arc::new(ironclaw_filesystem::InMemoryBackend::new()))
    }

    #[tokio::test]
    async fn load_on_cold_start_is_empty_not_an_error() {
        let s = store();
        let g = s.load(&scope("t1", "u1", None, None)).await.unwrap();
        assert!(g.is_empty(), "absent storage must read as empty memory");
    }

    #[tokio::test]
    async fn update_then_load_round_trips() {
        let s = store();
        let sc = scope("t1", "u1", Some("a1"), Some("p1"));
        s.update(&sc, |g| {
            g.upsert_node(node("the chest is unlocked"));
            g.processed_turn_run_ids.insert("run-1".into());
            Ok(())
        })
        .await
        .unwrap();

        let g = s.load(&sc).await.unwrap();
        assert_eq!(g.nodes.len(), 1);
        assert_eq!(g.nodes[0].text, "the chest is unlocked");
        assert!(g.processed_turn_run_ids.contains("run-1"));
    }

    #[tokio::test]
    async fn successive_updates_accumulate_rather_than_overwrite() {
        let s = store();
        let sc = scope("t1", "u1", None, None);
        s.update(&sc, |g| {
            g.upsert_node(node("fact one"));
            Ok(())
        })
        .await
        .unwrap();
        s.update(&sc, |g| {
            g.upsert_node(node("fact two"));
            Ok(())
        })
        .await
        .unwrap();

        let g = s.load(&sc).await.unwrap();
        assert_eq!(g.nodes.len(), 2, "second write must not clobber the first");
    }

    #[tokio::test]
    async fn scopes_are_isolated_on_every_axis() {
        let s = store();
        let base = scope("t1", "u1", Some("a1"), Some("p1"));
        s.update(&base, |g| {
            g.upsert_node(node("tenant one secret"));
            Ok(())
        })
        .await
        .unwrap();

        // Changing ANY axis must land on a different document.
        for other in [
            scope("t2", "u1", Some("a1"), Some("p1")),
            scope("t1", "u2", Some("a1"), Some("p1")),
            scope("t1", "u1", Some("a2"), Some("p1")),
            scope("t1", "u1", Some("a1"), Some("p2")),
            scope("t1", "u1", None, Some("p1")),
            scope("t1", "u1", Some("a1"), None),
        ] {
            let g = s.load(&other).await.unwrap();
            assert!(
                g.is_empty(),
                "scope isolation breached — another scope's memory was visible"
            );
        }

        // And the original is still intact.
        assert_eq!(s.load(&base).await.unwrap().nodes.len(), 1);
    }

    #[tokio::test]
    async fn a_failing_mutation_persists_nothing() {
        let s = store();
        let sc = scope("t1", "u1", None, None);
        let err = s
            .update(&sc, |_g| {
                Err(AmaAgentError::Llm("extraction blew up".into()))
            })
            .await;
        assert!(err.is_err());
        assert!(
            s.load(&sc).await.unwrap().is_empty(),
            "a failed mutation must not write a partial graph"
        );
    }

    #[tokio::test]
    async fn corrupt_storage_is_an_error_not_a_silent_reset() {
        let fs: Arc<dyn RootFilesystem> = Arc::new(ironclaw_filesystem::InMemoryBackend::new());
        let sc = scope("t1", "u1", None, None);
        let path = GraphStore::graph_path(&sc).unwrap();
        fs.write_file(&path, b"{not json at all").await.unwrap();

        let s = GraphStore::new(fs);
        // Silently returning empty here would be indistinguishable from a
        // genuine recall failure in a benchmark, so it must surface.
        assert!(matches!(s.load(&sc).await, Err(AmaAgentError::Storage(_))));
    }

    #[test]
    fn correlation_id_type_is_available_for_invocations() {
        // Compile-time guard that the host-api id surface we depend on exists.
        let _ = CorrelationId::new();
    }
}
