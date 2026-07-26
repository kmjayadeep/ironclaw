use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use ironclaw_host_api::UserId;

use crate::RebornBuildError;
use crate::root::default_system_prompt::seed_default_system_prompt;

const DEFAULT_SYSTEM_PROMPT_PATH: &str = "system/prompts/default-system.md";
pub(crate) const LEGACY_SKILLS_BACKFILL_MARKER: &str = ".legacy-skills-backfilled";
const LEGACY_SKILLS_BACKFILL_MAX_DEPTH: usize = 64;

pub(crate) struct StandaloneBootstrapAssembly {
    pub(crate) default_system_prompt_path: PathBuf,
}

/// Initializes standalone host content after storage roots are prepared.
pub(crate) struct StandaloneBootstrapAssemblyBuilder<'a> {
    storage_root: &'a Path,
    owner_user_id: &'a UserId,
}

impl<'a> StandaloneBootstrapAssemblyBuilder<'a> {
    pub(crate) fn new(storage_root: &'a Path, owner_user_id: &'a UserId) -> Self {
        Self {
            storage_root,
            owner_user_id,
        }
    }

    pub(crate) async fn build(self) -> Result<StandaloneBootstrapAssembly, RebornBuildError> {
        let backfill_root = self.storage_root.to_path_buf();
        let backfill_owner_user_id = self.owner_user_id.clone();
        tokio::task::spawn_blocking(move || {
            backfill_legacy_user_skills(&backfill_root, &backfill_owner_user_id)
        })
        .await
        .map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!("legacy skill backfill task failed: {error}"),
        })??;

        let default_system_prompt_path = self.storage_root.join(DEFAULT_SYSTEM_PROMPT_PATH);
        seed_default_system_prompt(self.storage_root, &default_system_prompt_path).map_err(
            |error| RebornBuildError::InvalidConfig {
                reason: error.to_string(),
            },
        )?;
        ironclaw_extension_host::bundled_skills::ensure_bundled_reborn_skills_installed(
            self.storage_root,
        )
        .await?;

        Ok(StandaloneBootstrapAssembly {
            default_system_prompt_path,
        })
    }
}

pub(crate) fn backfill_legacy_user_skills(
    storage_root: &Path,
    owner_user_id: &UserId,
) -> Result<(), RebornBuildError> {
    let legacy_root = storage_root.join("skills");
    if !legacy_root.is_dir() {
        return Ok(());
    }

    for tenant_id in ["default", "reborn-cli"] {
        backfill_legacy_user_skills_for_tenant(
            &legacy_root,
            storage_root,
            tenant_id,
            owner_user_id,
        )?;
    }
    Ok(())
}

fn backfill_legacy_user_skills_for_tenant(
    legacy_root: &Path,
    storage_root: &Path,
    tenant_id: &str,
    owner_user_id: &UserId,
) -> Result<(), RebornBuildError> {
    let scoped_root = storage_root
        .join("tenants")
        .join(tenant_id)
        .join("users")
        .join(owner_user_id.as_str())
        .join("skills");
    let marker = scoped_root.join(LEGACY_SKILLS_BACKFILL_MARKER);
    if marker.exists() {
        return Ok(());
    }

    std::fs::create_dir_all(&scoped_root).map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!("scoped skill root could not be initialized: {error}"),
    })?;

    for entry in
        std::fs::read_dir(legacy_root).map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!(
                "legacy skills root '{}' could not be inspected: {error}",
                legacy_root.display()
            ),
        })?
    {
        let entry = entry.map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!(
                "legacy skills root '{}' could not be inspected: {error}",
                legacy_root.display()
            ),
        })?;
        let source = entry.path();
        let destination = scoped_root.join(entry.file_name());
        if destination.exists() {
            continue;
        }
        copy_legacy_skill_entry(&source, &destination)?;
    }
    std::fs::write(&marker, b"").map_err(|error| RebornBuildError::InvalidConfig {
        reason: format!(
            "legacy skill migration marker '{}' could not be written: {error}",
            marker.display()
        ),
    })?;
    Ok(())
}

fn copy_legacy_skill_entry(source: &Path, destination: &Path) -> Result<(), RebornBuildError> {
    let mut pending = VecDeque::from([(source.to_path_buf(), destination.to_path_buf(), 0usize)]);

    while let Some((source, destination, depth)) = pending.pop_front() {
        if depth > LEGACY_SKILLS_BACKFILL_MAX_DEPTH {
            return Err(RebornBuildError::InvalidConfig {
                reason: format!(
                    "legacy skill entry '{}' exceeds max copy depth {}",
                    source.display(),
                    LEGACY_SKILLS_BACKFILL_MAX_DEPTH
                ),
            });
        }

        let metadata = std::fs::symlink_metadata(&source).map_err(|error| {
            RebornBuildError::InvalidConfig {
                reason: format!(
                    "legacy skill entry '{}' could not be inspected: {error}",
                    source.display()
                ),
            }
        })?;
        if metadata.file_type().is_symlink() {
            tracing::warn!(
                path = %source.display(),
                "Skipping symlinked legacy skill entry during backfill"
            );
            continue;
        }
        if metadata.is_dir() {
            std::fs::create_dir_all(&destination).map_err(|error| {
                RebornBuildError::InvalidConfig {
                    reason: format!(
                        "scoped skill directory '{}' could not be initialized: {error}",
                        destination.display()
                    ),
                }
            })?;
            for entry in
                std::fs::read_dir(&source).map_err(|error| RebornBuildError::InvalidConfig {
                    reason: format!(
                        "legacy skill directory '{}' could not be inspected: {error}",
                        source.display()
                    ),
                })?
            {
                let entry = entry.map_err(|error| RebornBuildError::InvalidConfig {
                    reason: format!(
                        "legacy skill directory '{}' could not be inspected: {error}",
                        source.display()
                    ),
                })?;
                pending.push_back((
                    entry.path(),
                    destination.join(entry.file_name()),
                    depth.saturating_add(1),
                ));
            }
            continue;
        }

        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| RebornBuildError::InvalidConfig {
                reason: format!(
                    "scoped skill directory '{}' could not be initialized: {error}",
                    parent.display()
                ),
            })?;
        }
        std::fs::copy(&source, &destination).map_err(|error| RebornBuildError::InvalidConfig {
            reason: format!(
                "legacy skill file '{}' could not be migrated to '{}': {error}",
                source.display(),
                destination.display()
            ),
        })?;
    }
    Ok(())
}
