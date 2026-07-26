use std::collections::{HashMap, VecDeque};

use ironclaw_host_api::{ProcessId, ResourceScope};
use serde::{Deserialize, Serialize};

use super::ProcessJournalStoreError;
use crate::{
    JournaledProcessSnapshot, ProcessCheckpointId, ProcessCheckpointRecord, ProcessControlResult,
    ProcessJournalCursor, ProcessJournalEntry, ProcessJournalPage, ProcessLifecycleStatus,
    ProcessTreeReservation, types::same_scope_owner,
};

const MAX_IDEMPOTENCY_RECORDS: usize = 4096;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ProcessJournalMaterializedState {
    pub(super) next_cursor: u64,
    pub(super) processes: HashMap<ProcessId, JournaledProcessSnapshot>,
    pub(super) journal: Vec<ProcessJournalEntry>,
    #[serde(default)]
    pub(super) control_idempotency: HashMap<String, ProcessControlResult>,
    #[serde(default)]
    control_idempotency_order: VecDeque<String>,
    #[serde(default)]
    pub(super) submission_idempotency: HashMap<String, JournaledProcessSnapshot>,
    #[serde(default)]
    submission_idempotency_order: VecDeque<String>,
    #[serde(default)]
    pub(super) tree_reservations: HashMap<ProcessId, ProcessTreeReservation>,
    #[serde(default)]
    pub(super) checkpoints: HashMap<ProcessCheckpointId, ProcessCheckpointRecord>,
}

impl Default for ProcessJournalMaterializedState {
    fn default() -> Self {
        Self {
            next_cursor: 1,
            processes: HashMap::new(),
            journal: Vec::new(),
            control_idempotency: HashMap::new(),
            control_idempotency_order: VecDeque::new(),
            submission_idempotency: HashMap::new(),
            submission_idempotency_order: VecDeque::new(),
            tree_reservations: HashMap::new(),
            checkpoints: HashMap::new(),
        }
    }
}

impl ProcessJournalMaterializedState {
    pub(super) fn next_cursor(&mut self) -> ProcessJournalCursor {
        let cursor = ProcessJournalCursor(self.next_cursor);
        self.next_cursor = self.next_cursor.saturating_add(1);
        cursor
    }

    pub(super) fn push_entry(&mut self, entry: ProcessJournalEntry) {
        self.journal.push(entry);
    }

    pub(super) fn remember_control_result(
        &mut self,
        key: Option<String>,
        result: ProcessControlResult,
    ) {
        let Some(key) = key else {
            return;
        };
        if let Some(existing) = self.control_idempotency.get_mut(&key) {
            *existing = result;
            return;
        }
        while self.control_idempotency.len() >= MAX_IDEMPOTENCY_RECORDS {
            let Some(oldest) = self.control_idempotency_order.pop_front() else {
                self.control_idempotency.clear();
                break;
            };
            self.control_idempotency.remove(&oldest);
        }
        self.control_idempotency_order.push_back(key.clone());
        self.control_idempotency.insert(key, result);
    }

    pub(super) fn remember_submission_result(
        &mut self,
        key: Option<String>,
        snapshot: JournaledProcessSnapshot,
    ) {
        let Some(key) = key else {
            return;
        };
        if let Some(existing) = self.submission_idempotency.get_mut(&key) {
            *existing = snapshot;
            return;
        }
        while self.submission_idempotency.len() >= MAX_IDEMPOTENCY_RECORDS {
            let Some(oldest) = self.submission_idempotency_order.pop_front() else {
                self.submission_idempotency.clear();
                break;
            };
            self.submission_idempotency.remove(&oldest);
        }
        self.submission_idempotency_order.push_back(key.clone());
        self.submission_idempotency.insert(key, snapshot);
    }

    pub(super) fn process_mut(
        &mut self,
        process_id: ProcessId,
    ) -> Result<&mut JournaledProcessSnapshot, ProcessJournalStoreError> {
        self.processes
            .get_mut(&process_id)
            .ok_or(ProcessJournalStoreError::UnknownProcess { process_id })
    }

    pub(super) fn claimable_process_ids(
        &self,
        scope_filter: Option<&ResourceScope>,
    ) -> Vec<ProcessId> {
        let mut ids = self
            .processes
            .values()
            .filter(|snapshot| snapshot.status == ProcessLifecycleStatus::Queued)
            .filter(|snapshot| {
                scope_filter.is_none_or(|scope| same_scope_owner(&snapshot.scope, scope))
            })
            .map(|snapshot| (snapshot.created_at, snapshot.process_id))
            .collect::<Vec<_>>();
        ids.sort_by_key(|(created_at, process_id)| (*created_at, process_id.as_uuid()));
        ids.into_iter().map(|(_, process_id)| process_id).collect()
    }

    pub(super) fn expired_process_ids(
        &self,
        scope_filter: Option<&ResourceScope>,
        now: ironclaw_host_api::Timestamp,
    ) -> Vec<ProcessId> {
        self.processes
            .values()
            .filter(|snapshot| {
                matches!(
                    snapshot.status,
                    ProcessLifecycleStatus::Running | ProcessLifecycleStatus::CancelRequested
                )
            })
            .filter(|snapshot| {
                scope_filter.is_none_or(|scope| same_scope_owner(&snapshot.scope, scope))
            })
            .filter(|snapshot| {
                snapshot
                    .lease
                    .as_ref()
                    .and_then(|lease| lease.lease_expires_at)
                    .is_some_and(|expires_at| expires_at <= now)
            })
            .map(|snapshot| snapshot.process_id)
            .collect()
    }

    pub(super) fn page_after(
        &self,
        after: Option<ProcessJournalCursor>,
        limit: usize,
        include: impl Fn(&ProcessJournalEntry) -> bool,
    ) -> ProcessJournalPage {
        let after = after.map(|cursor| cursor.0).unwrap_or(0);
        let mut entries = self
            .journal
            .iter()
            .filter(|entry| entry.cursor.0 > after)
            .filter(|entry| include(entry))
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let truncated = entries.len() > limit;
        if truncated {
            entries.truncate(limit);
        }
        let next_cursor = entries
            .last()
            .map(|entry| entry.cursor)
            .unwrap_or(ProcessJournalCursor(after));
        ProcessJournalPage {
            entries,
            next_cursor,
            truncated,
            rebase_required: None,
        }
    }
}
