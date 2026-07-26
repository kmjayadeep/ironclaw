//! Process-backed fixtures for cross-crate turn projection tests.

use std::sync::Arc;

use ironclaw_filesystem::{InMemoryBackend, ScopedFilesystem};
use ironclaw_host_api::{MountAlias, MountGrant, MountPermissions, MountView, VirtualPath};
use ironclaw_processes::{ProcessJournalStore, ProcessRuntimePort, ProcessTransitionPort};

use crate::{AgentTurnProcessRuntime, ProcessJournalStoreTurnAdapter, TurnError};

#[derive(Clone)]
pub struct InMemoryAgentTurnProcessSystem {
    store: Arc<ProcessJournalStore<InMemoryBackend>>,
    adapter: Arc<ProcessJournalStoreTurnAdapter>,
    runtime: AgentTurnProcessRuntime,
}

impl InMemoryAgentTurnProcessSystem {
    pub fn new() -> Self {
        let store = Arc::new(ProcessJournalStore::new(in_memory_processes_filesystem()));
        let adapter = Arc::new(ProcessJournalStoreTurnAdapter::new(
            Arc::clone(&store) as Arc<dyn ProcessRuntimePort>
        ));
        let runtime = AgentTurnProcessRuntime::from_process_adapter(Arc::clone(&adapter));
        Self {
            store,
            adapter,
            runtime,
        }
    }

    pub fn runtime(&self) -> AgentTurnProcessRuntime {
        self.runtime.clone()
    }

    pub fn transitions(&self) -> Arc<dyn ProcessTransitionPort<Error = TurnError>> {
        Arc::clone(&self.adapter) as Arc<dyn ProcessTransitionPort<Error = TurnError>>
    }

    pub fn store(&self) -> Arc<ProcessJournalStore<InMemoryBackend>> {
        Arc::clone(&self.store)
    }
}

impl Default for InMemoryAgentTurnProcessSystem {
    fn default() -> Self {
        Self::new()
    }
}

pub fn in_memory_agent_turn_process_system() -> InMemoryAgentTurnProcessSystem {
    InMemoryAgentTurnProcessSystem::new()
}

pub fn in_memory_processes_filesystem() -> Arc<ScopedFilesystem<InMemoryBackend>> {
    let mounts = MountView::new(vec![MountGrant::new(
        MountAlias::new("/processes").expect("processes alias"),
        VirtualPath::new("/engine/processes").expect("processes target"),
        MountPermissions::read_write_list_delete(),
    )])
    .expect("processes mount view");
    Arc::new(ScopedFilesystem::with_fixed_view(
        Arc::new(InMemoryBackend::new()),
        mounts,
    ))
}
