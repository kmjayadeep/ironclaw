use async_trait::async_trait;
use ironclaw_host_api::VirtualPath;
use tokio::sync::OwnedMutexGuard;

use super::{State, state_delete, state_get, state_put, state_reserve_sequence};
use crate::{
    CasExpectation, Entry, FilesystemError, FilesystemOperation, RecordVersion, SeqNo, StorageTxn,
    VersionedEntry,
};

pub(super) struct InMemoryStorageTxn {
    pub(super) state: Option<OwnedMutexGuard<State>>,
    pub(super) original: Option<State>,
    pub(super) prefix: VirtualPath,
}

impl InMemoryStorageTxn {
    fn check_path(&self, path: &VirtualPath) -> Result<(), FilesystemError> {
        if crate::path_prefix_matches(self.prefix.as_str(), path.as_str()) {
            Ok(())
        } else {
            Err(FilesystemError::PathOutsideMount { path: path.clone() })
        }
    }

    fn state(&mut self) -> Result<&mut State, FilesystemError> {
        self.state
            .as_deref_mut()
            .ok_or_else(|| FilesystemError::Backend {
                path: self.prefix.clone(),
                operation: FilesystemOperation::BeginTxn,
                reason: "in-memory transaction already finished".to_string(),
            })
    }

    fn restore(&mut self) {
        if let (Some(state), Some(original)) = (self.state.as_deref_mut(), self.original.take()) {
            *state = original;
        }
    }
}

#[async_trait]
impl StorageTxn for InMemoryStorageTxn {
    async fn put(
        &mut self,
        path: &VirtualPath,
        entry: Entry,
        cas: CasExpectation,
    ) -> Result<RecordVersion, FilesystemError> {
        self.check_path(path)?;
        state_put(self.state()?, path, entry, cas)
    }

    async fn get(&mut self, path: &VirtualPath) -> Result<Option<VersionedEntry>, FilesystemError> {
        self.check_path(path)?;
        Ok(state_get(self.state()?, path))
    }

    async fn delete(&mut self, path: &VirtualPath) -> Result<(), FilesystemError> {
        self.check_path(path)?;
        state_delete(self.state()?, path)
    }

    async fn reserve_sequence(&mut self, path: &VirtualPath) -> Result<SeqNo, FilesystemError> {
        self.check_path(path)?;
        Ok(state_reserve_sequence(self.state()?, path))
    }

    async fn commit(mut self: Box<Self>) -> Result<(), FilesystemError> {
        self.original = None;
        self.state = None;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) {
        self.restore();
        self.state = None;
    }
}

impl Drop for InMemoryStorageTxn {
    fn drop(&mut self) {
        self.restore();
    }
}
