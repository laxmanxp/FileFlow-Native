use crate::vault::VaultStore;
use fileflow_catalog::{Catalog, CatalogError};
use fileflow_core::{
    BackupCreateReport, BackupRestoreReport, DuplicateActionReport, DuplicateMember,
    IntegrityReport, LogicalFileId, LogicalFileView, RevisionInfo, SearchHit,
};
use fileflow_rpc::{Request, Response, WatcherStatus};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::actor::{spawn_writer, Command};
use crate::health_status;
use crate::indexer::{index_folder, index_path};
use crate::watcher::WatcherHandle;

#[derive(Clone)]
pub struct FileFlowService {
    tx: mpsc::Sender<Command>,
    watcher: WatcherHandle,
    vault: Arc<VaultStore>,
    data_home: PathBuf,
    catalog_path: PathBuf,
}

impl FileFlowService {
    pub fn spawn(catalog: Catalog, data_home: PathBuf) -> Self {
        let catalog_path = data_home.join("catalog.sqlite");
        let vault_root = data_home.join("vault");
        let _ = std::fs::create_dir_all(&vault_root);
        let tx = spawn_writer(catalog);
        let (watcher, cmd_rx, snap) = WatcherHandle::pair();
        let svc = Self {
            tx,
            watcher,
            vault: Arc::new(VaultStore::new(vault_root)),
            data_home,
            catalog_path,
        };
        WatcherHandle::start(svc.clone(), cmd_rx, snap);
        let boot = svc.clone();
        tokio::spawn(async move {
            match boot.list_indexed_locations().await {
                Ok(roots) => {
                    for root in roots {
                        boot.watcher.add_root(root).await;
                    }
                }
                Err(err) => tracing::warn!(error = %err, "failed to restore indexed locations"),
            }
        });
        svc
    }

    pub async fn handle_rpc(&self, req: Request) -> Response {
        match self.dispatch(req).await {
            Ok(resp) => resp,
            Err(err) => Response::err(err.to_string()),
        }
    }

    async fn dispatch(&self, req: Request) -> Result<Response, CatalogError> {
        match req {
            Request::Health => {
                let (status, version) = health_status();
                Ok(Response::Health {
                    status: status.to_string(),
                    version: version.to_string(),
                })
            }
            Request::ResolvePath { path } => {
                let id = self.resolve_path(&path).await?;
                Ok(Response::LogicalId {
                    logical_file_id: id,
                })
            }
            Request::IndexPath { path } => {
                let report = index_path(self, &path).await?;
                Ok(Response::Indexed { report })
            }
            Request::IndexFolder { path } => {
                let stored = self.add_indexed_location(&path).await?;
                self.watcher.add_root(stored).await;
                let report = index_folder(self, &path).await?;
                Ok(Response::Indexed { report })
            }
            Request::GetFile { logical_file_id } => {
                let file = self.get_file(logical_file_id).await?;
                Ok(Response::File { file })
            }
            Request::Search { query } => {
                let hits = self.search(&query).await?;
                Ok(Response::SearchResults { hits })
            }
            Request::AddTag {
                logical_file_id,
                tag,
            } => {
                self.call(|reply| Command::AddTag {
                    id: logical_file_id,
                    tag,
                    reply,
                })
                .await?;
                Ok(Response::Ok)
            }
            Request::RemoveTag {
                logical_file_id,
                tag,
            } => {
                self.call(|reply| Command::RemoveTag {
                    id: logical_file_id,
                    tag,
                    reply,
                })
                .await?;
                Ok(Response::Ok)
            }
            Request::SetNote {
                logical_file_id,
                body,
            } => {
                self.call(|reply| Command::SetNote {
                    id: logical_file_id,
                    body,
                    reply,
                })
                .await?;
                Ok(Response::Ok)
            }
            Request::AddTodo {
                logical_file_id,
                title,
            } => {
                let todo_id = self
                    .call(|reply| Command::AddTodo {
                        id: logical_file_id,
                        title,
                        reply,
                    })
                    .await?;
                Ok(Response::TodoCreated { todo_id })
            }
            Request::SetTodoDone { todo_id, done } => {
                self.call(|reply| Command::SetTodoDone {
                    todo_id,
                    done,
                    reply,
                })
                .await?;
                Ok(Response::Ok)
            }
            Request::UpdatePath {
                logical_file_id,
                new_path,
            } => {
                self.update_path(logical_file_id, &new_path).await?;
                Ok(Response::Ok)
            }
            Request::PauseWatcher => {
                self.watcher.pause().await;
                Ok(Response::Ok)
            }
            Request::ResumeWatcher => {
                self.watcher.resume().await;
                Ok(Response::Ok)
            }
            Request::GetWatcherStatus => Ok(Response::WatcherStatus {
                status: self.watcher.status(),
            }),
            Request::ListIndexedLocations => {
                let roots = self.list_indexed_locations().await?;
                Ok(Response::IndexedLocations { roots })
            }
            Request::RemoveIndexedLocation { path } => {
                self.call(|reply| Command::RemoveIndexedLocation {
                    path: path.clone(),
                    reply,
                })
                .await?;
                self.watcher.remove_root(path).await;
                Ok(Response::Ok)
            }
            Request::AddToVault { logical_file_id } => {
                self.add_to_vault(logical_file_id).await?;
                Ok(Response::Ok)
            }
            Request::RemoveFromVault { logical_file_id } => {
                self.call(|reply| Command::SetVaulted {
                    id: logical_file_id,
                    vaulted: false,
                    reply,
                })
                .await?;
                Ok(Response::Ok)
            }
            Request::ListRevisions { logical_file_id } => {
                let revisions = self.list_revisions(logical_file_id).await?;
                Ok(Response::Revisions { revisions })
            }
            Request::GetRevision {
                logical_file_id,
                revision_id,
            } => {
                let revision = self
                    .call(|reply| Command::GetRevision {
                        id: logical_file_id,
                        revision_id,
                        reply,
                    })
                    .await?;
                Ok(Response::Revision { revision })
            }
            Request::SetCurrentRevision {
                logical_file_id,
                revision_id,
            } => {
                self.set_current_revision(logical_file_id, revision_id)
                    .await?;
                Ok(Response::Ok)
            }
            Request::ExportRevision {
                logical_file_id,
                revision_id,
                dest_path,
            } => {
                self.export_revision(logical_file_id, revision_id, &dest_path)
                    .await?;
                Ok(Response::Ok)
            }
            Request::PruneRevisions {
                logical_file_id,
                keep_last,
            } => {
                self.call(|reply| Command::SetVaultKeepLast {
                    id: logical_file_id,
                    keep_last: Some(keep_last),
                    reply,
                })
                .await?;
                self.prune(logical_file_id, keep_last).await?;
                Ok(Response::Ok)
            }
            Request::VerifyRevision {
                logical_file_id,
                revision_id,
            } => {
                let report = self.verify_revision(logical_file_id, revision_id).await?;
                Ok(Response::Integrity { report })
            }
            Request::VerifyContentObject { sha256 } => Ok(Response::Integrity {
                report: self.vault.verify(&sha256),
            }),
            Request::FindDuplicates {
                min_group_size,
                path_prefix,
                exclude_patterns,
                exclude_common_build_vcs_dirs,
                min_size,
                include_missing,
            } => {
                let query = fileflow_core::DuplicateQuery::resolved(
                    min_group_size,
                    path_prefix,
                    exclude_patterns,
                    exclude_common_build_vcs_dirs,
                    min_size,
                    include_missing,
                );
                let scan = self
                    .call(|reply| Command::FindDuplicates { query, reply })
                    .await?;
                Ok(Response::DuplicateScan { scan })
            }
            Request::ResolveDuplicateGroup {
                sha256,
                keep_logical_file_id,
                delete_logical_file_ids,
                confirm,
                confirm_permanent,
                allow_delete_all,
            } => {
                let report = self
                    .resolve_duplicate_group(
                        sha256,
                        keep_logical_file_id,
                        delete_logical_file_ids,
                        confirm,
                        confirm_permanent,
                        allow_delete_all,
                    )
                    .await?;
                Ok(Response::DuplicateAction { report })
            }
            Request::DeleteDuplicateMember {
                logical_file_id,
                confirm,
                confirm_permanent,
                allow_delete_last,
            } => {
                let report = self
                    .delete_duplicate_member(
                        logical_file_id,
                        confirm,
                        confirm_permanent,
                        allow_delete_last,
                    )
                    .await?;
                Ok(Response::DuplicateAction { report })
            }
            Request::CreateBackup {
                destination_path,
                include_vault,
            } => {
                let report = self
                    .create_backup(&destination_path, include_vault.unwrap_or(true))
                    .await?;
                Ok(Response::BackupCreated { report })
            }
            Request::VerifyBackup { backup_path } => Ok(Response::BackupVerified {
                report: crate::backup::verify_backup(Path::new(&backup_path)),
            }),
            Request::RestoreBackup {
                backup_path,
                target_data_home,
                confirm,
                force,
            } => {
                let report = self
                    .restore_backup(
                        &backup_path,
                        target_data_home.as_deref(),
                        confirm,
                        force.unwrap_or(false),
                    )
                    .await?;
                Ok(Response::BackupRestored { report })
            }
        }
    }

    pub async fn resolve_path(&self, path: &str) -> Result<Option<LogicalFileId>, CatalogError> {
        let path = path.to_string();
        self.call(|reply| Command::ResolvePath { path, reply })
            .await
    }

    pub async fn resolve_path_any(
        &self,
        path: &str,
    ) -> Result<Option<LogicalFileId>, CatalogError> {
        let path = path.to_string();
        self.call(|reply| Command::LookupAnyPath { path, reply })
            .await
    }

    pub async fn upsert_indexed(
        &self,
        path: String,
        sha256: String,
        size: u64,
    ) -> Result<LogicalFileId, CatalogError> {
        self.call(|reply| Command::UpsertIndexed {
            path,
            sha256,
            size,
            reply,
        })
        .await
    }

    pub async fn get_file(
        &self,
        id: LogicalFileId,
    ) -> Result<Option<LogicalFileView>, CatalogError> {
        self.call(|reply| Command::GetFile { id, reply }).await
    }

    pub async fn search(&self, query: &str) -> Result<Vec<SearchHit>, CatalogError> {
        let query = query.to_string();
        self.call(|reply| Command::Search { query, reply }).await
    }

    pub async fn update_path(&self, id: LogicalFileId, new_path: &str) -> Result<(), CatalogError> {
        let new_path = new_path.to_string();
        self.call(|reply| Command::UpdatePath {
            id,
            new_path,
            reply,
        })
        .await
    }

    pub async fn tombstone_path(&self, path: &str) -> Result<Option<LogicalFileId>, CatalogError> {
        let path = path.to_string();
        self.call(|reply| Command::TombstonePath { path, reply })
            .await
    }

    pub async fn add_indexed_location(&self, path: &str) -> Result<String, CatalogError> {
        let path = path.to_string();
        self.call(|reply| Command::AddIndexedLocation { path, reply })
            .await
    }

    pub async fn list_indexed_locations(&self) -> Result<Vec<String>, CatalogError> {
        self.call(|reply| Command::ListIndexedLocations { reply })
            .await
    }

    pub fn watcher_status(&self) -> WatcherStatus {
        self.watcher.status()
    }

    pub fn vault_root(&self) -> &Path {
        self.vault.root()
    }

    pub async fn list_revisions(
        &self,
        id: LogicalFileId,
    ) -> Result<Vec<RevisionInfo>, CatalogError> {
        self.call(|reply| Command::ListRevisions { id, reply })
            .await
    }

    pub async fn capture_vault_blob(
        &self,
        id: LogicalFileId,
        path: &Path,
        sha256: &str,
    ) -> Result<(), CatalogError> {
        let vaulted = self.call(|reply| Command::IsVaulted { id, reply }).await?;
        if !vaulted {
            return Ok(());
        }
        let src = path.to_path_buf();
        let sha = sha256.to_string();
        let vault = self.vault.clone();
        tokio::task::spawn_blocking(move || vault.put_from_file(&src, &sha))
            .await
            .map_err(|e| CatalogError::msg(e.to_string()))??;
        self.call(|reply| Command::MarkBlobPresent {
            sha256: sha256.to_string(),
            reply,
        })
        .await?;
        if let Some(view) = self.get_file(id).await? {
            if let Some(n) = view.vault_keep_last {
                self.prune(id, n).await?;
            }
        }
        Ok(())
    }

    async fn add_to_vault(&self, id: LogicalFileId) -> Result<(), CatalogError> {
        let view = self
            .get_file(id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("logical_file {id}")))?;
        let path = view
            .paths
            .first()
            .cloned()
            .ok_or_else(|| CatalogError::msg("AddToVault needs a current path"))?;
        let path_buf = PathBuf::from(&path);
        let (sha256, size) =
            tokio::task::spawn_blocking(move || fileflow_core::hash_file(&path_buf))
                .await
                .map_err(|e| CatalogError::msg(e.to_string()))?
                .map_err(|e| CatalogError::msg(e.to_string()))?;
        self.upsert_indexed(path.clone(), sha256.clone(), size)
            .await?;
        let vault = self.vault.clone();
        let src = PathBuf::from(&path);
        let sha_copy = sha256.clone();
        tokio::task::spawn_blocking(move || vault.put_from_file(&src, &sha_copy))
            .await
            .map_err(|e| CatalogError::msg(e.to_string()))??;
        self.call(|reply| Command::MarkBlobPresent {
            sha256: sha256.clone(),
            reply,
        })
        .await?;
        self.call(|reply| Command::SetVaulted {
            id,
            vaulted: true,
            reply,
        })
        .await?;
        Ok(())
    }

    async fn set_current_revision(
        &self,
        id: LogicalFileId,
        revision_id: i64,
    ) -> Result<(), CatalogError> {
        let info = self
            .call(|reply| Command::GetRevision {
                id,
                revision_id,
                reply,
            })
            .await?;
        if !self.vault.contains(&info.sha256) {
            return Err(CatalogError::msg(format!(
                "revision {} has no vault blob; vault the file first",
                revision_id
            )));
        }
        self.call(|reply| Command::SetCurrentRevision {
            id,
            revision_id,
            reply,
        })
        .await?;
        if let Some(view) = self.get_file(id).await? {
            if let Some(path) = view.paths.first() {
                let dest = PathBuf::from(path);
                let vault = self.vault.clone();
                let sha = info.sha256.clone();
                tokio::task::spawn_blocking(move || vault.restore_to(&sha, &dest))
                    .await
                    .map_err(|e| CatalogError::msg(e.to_string()))??;
            }
        }
        Ok(())
    }

    async fn export_revision(
        &self,
        id: LogicalFileId,
        revision_id: i64,
        dest: &str,
    ) -> Result<(), CatalogError> {
        let info = self
            .call(|reply| Command::GetRevision {
                id,
                revision_id,
                reply,
            })
            .await?;
        let dest = PathBuf::from(dest);
        let vault = self.vault.clone();
        let sha = info.sha256;
        tokio::task::spawn_blocking(move || vault.restore_to(&sha, &dest))
            .await
            .map_err(|e| CatalogError::msg(e.to_string()))??;
        Ok(())
    }

    async fn prune(&self, id: LogicalFileId, keep_last: u32) -> Result<(), CatalogError> {
        let unused = self
            .call(|reply| Command::PruneRevisions {
                id,
                keep_last,
                reply,
            })
            .await?;
        for sha in unused {
            self.vault.delete_object(&sha);
        }
        Ok(())
    }

    async fn verify_revision(
        &self,
        id: LogicalFileId,
        revision_id: i64,
    ) -> Result<IntegrityReport, CatalogError> {
        let info = self
            .call(|reply| Command::GetRevision {
                id,
                revision_id,
                reply,
            })
            .await?;
        Ok(self.vault.verify(&info.sha256))
    }

    async fn resolve_duplicate_group(
        &self,
        sha256: String,
        keep_logical_file_id: Option<LogicalFileId>,
        delete_logical_file_ids: Vec<LogicalFileId>,
        confirm: bool,
        confirm_permanent: bool,
        allow_delete_all: bool,
    ) -> Result<DuplicateActionReport, CatalogError> {
        if !confirm {
            return Err(CatalogError::msg(
                "ResolveDuplicateGroup requires confirm=true (review-first; no mass delete)",
            ));
        }
        let members = self
            .call(|reply| Command::MembersForHash {
                sha256: sha256.clone(),
                reply,
            })
            .await?;
        if members.is_empty() {
            return Err(CatalogError::NotFound(format!(
                "no current paths for hash {sha256}"
            )));
        }
        if let Some(keep) = keep_logical_file_id {
            if !members.iter().any(|m| m.logical_file_id == keep) {
                return Err(CatalogError::msg(
                    "keep target is not a current member of this duplicate group",
                ));
            }
        }
        let mut delete_ids: Vec<LogicalFileId> = if delete_logical_file_ids.is_empty() {
            members
                .iter()
                .map(|m| m.logical_file_id)
                .filter(|id| Some(*id) != keep_logical_file_id)
                .collect()
        } else {
            delete_logical_file_ids
        };
        if let Some(keep) = keep_logical_file_id {
            delete_ids.retain(|id| *id != keep);
        }
        delete_ids.sort_unstable();
        delete_ids.dedup();

        let remaining: Vec<_> = members
            .iter()
            .filter(|m| !delete_ids.contains(&m.logical_file_id))
            .collect();
        if remaining.is_empty() && !allow_delete_all {
            return Err(CatalogError::msg(
                "refusing to delete every copy in the group; keep one member or set allow_delete_all",
            ));
        }
        if keep_logical_file_id.is_none() && !allow_delete_all {
            return Err(CatalogError::msg(
                "keep_logical_file_id is required unless allow_delete_all is set",
            ));
        }

        self.delete_members(&members, &delete_ids, confirm_permanent)
            .await
            .map(|mut report| {
                report.kept_logical_file_id = keep_logical_file_id;
                report
            })
    }

    async fn delete_duplicate_member(
        &self,
        logical_file_id: LogicalFileId,
        confirm: bool,
        confirm_permanent: bool,
        allow_delete_last: bool,
    ) -> Result<DuplicateActionReport, CatalogError> {
        if !confirm {
            return Err(CatalogError::msg(
                "DeleteDuplicateMember requires confirm=true",
            ));
        }
        let view = self
            .get_file(logical_file_id)
            .await?
            .ok_or_else(|| CatalogError::NotFound(format!("logical_file {logical_file_id}")))?;
        let sha = view
            .sha256
            .ok_or_else(|| CatalogError::msg("file has no content hash"))?;
        let members = self
            .call(|reply| Command::MembersForHash {
                sha256: sha.clone(),
                reply,
            })
            .await?;
        if !members.iter().any(|m| m.logical_file_id == logical_file_id) {
            return Err(CatalogError::msg("no current path for that logical file"));
        }
        if members.len() <= 1 && !allow_delete_last {
            return Err(CatalogError::msg(
                "refusing to delete the last remaining copy; set allow_delete_last to override",
            ));
        }
        self.delete_members(&members, &[logical_file_id], confirm_permanent)
            .await
    }

    async fn delete_members(
        &self,
        members: &[DuplicateMember],
        delete_ids: &[LogicalFileId],
        confirm_permanent: bool,
    ) -> Result<DuplicateActionReport, CatalogError> {
        let mut deleted = Vec::new();
        let mut used_trash = false;
        let mut permanent = false;
        let mut messages = Vec::new();
        for member in members {
            if !delete_ids.contains(&member.logical_file_id) {
                continue;
            }
            let path = std::path::PathBuf::from(&member.path);
            let kind = tokio::task::spawn_blocking(move || {
                crate::trash::remove_path(&path, confirm_permanent)
            })
            .await
            .map_err(|e| CatalogError::msg(e.to_string()))??;
            match kind {
                crate::trash::RemovalKind::Trashed => used_trash = true,
                crate::trash::RemovalKind::Permanent => permanent = true,
            }
            self.tombstone_path(&member.path).await?;
            deleted.push(member.clone());
            messages.push(format!(
                "{} {}",
                match kind {
                    crate::trash::RemovalKind::Trashed => "trashed",
                    crate::trash::RemovalKind::Permanent => "deleted",
                },
                member.path
            ));
        }
        Ok(DuplicateActionReport {
            kept_logical_file_id: None,
            deleted,
            used_trash,
            permanent,
            messages,
        })
    }

    async fn create_backup(
        &self,
        destination_path: &str,
        include_vault: bool,
    ) -> Result<BackupCreateReport, CatalogError> {
        let dest = crate::backup::resolve_backup_path(Path::new(destination_path))?;
        self.watcher.pause().await;
        let staging = self.data_home.join(format!(
            ".backup-staging-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0)
        ));
        let result = self
            .create_backup_inner(&dest, include_vault, &staging)
            .await;
        let _ = std::fs::remove_dir_all(&staging);
        self.watcher.resume().await;
        result
    }

    async fn create_backup_inner(
        &self,
        dest: &Path,
        include_vault: bool,
        staging: &Path,
    ) -> Result<BackupCreateReport, CatalogError> {
        std::fs::create_dir_all(staging).map_err(|e| CatalogError::msg(e.to_string()))?;
        let snap = staging.join("fileflow.db.snapshot");
        self.call(|reply| Command::SnapshotTo {
            dest: snap.clone(),
            reply,
        })
        .await?;
        let sql_path = staging.join("fileflow.sql");
        let snap_clone = snap.clone();
        tokio::task::spawn_blocking(move || {
            let cat = fileflow_catalog::Catalog::open_readonly(&snap_clone)?;
            cat.export_sql(&sql_path)
        })
        .await
        .map_err(|e| CatalogError::msg(e.to_string()))??;

        let mut notes = vec![
            "SQLite snapshot used the backup API (not a naive hot copy).".into(),
            "Watcher was paused for the snapshot.".into(),
            "This package is FileFlow catalog/vault recovery, not a full disk image of user documents.".into(),
        ];
        let mut vault_objects = 0u64;
        if include_vault {
            let hashes = self.call(|reply| Command::BlobSha256s { reply }).await?;
            vault_objects = crate::backup::copy_vault_objects(
                self.vault.root(),
                &hashes,
                &staging.join("vault"),
                &mut notes,
            );
        }
        let staging = staging.to_path_buf();
        let dest = dest.to_path_buf();
        let source = self.data_home.clone();
        tokio::task::spawn_blocking(move || {
            crate::backup::pack_backup(
                &staging,
                &dest,
                &source,
                include_vault,
                vault_objects,
                notes,
            )
        })
        .await
        .map_err(|e| CatalogError::msg(e.to_string()))?
    }

    async fn restore_backup(
        &self,
        backup_path: &str,
        target_data_home: Option<&str>,
        confirm: bool,
        force: bool,
    ) -> Result<BackupRestoreReport, CatalogError> {
        if !confirm {
            return Err(CatalogError::msg(
                "RestoreBackup requires confirm=true (refuses to overwrite without confirmation)",
            ));
        }
        let backup = PathBuf::from(backup_path);
        let verify = crate::backup::verify_backup(&backup);
        if !verify.ok {
            return Err(CatalogError::msg(format!(
                "backup verify failed: {}",
                verify.failures.join("; ")
            )));
        }
        let target = target_data_home
            .map(PathBuf::from)
            .unwrap_or_else(|| self.data_home.clone());
        let live = same_path(&target, &self.data_home);
        let occupied = if live {
            self.call(|reply| Command::LogicalFileCount { reply })
                .await?
                > 0
                || crate::backup::target_has_user_state(&target)
        } else {
            crate::backup::target_has_user_state(&target)
        };
        if occupied && !force {
            return Err(CatalogError::msg(format!(
                "target data home already has catalog/vault data ({}); pass force=true with confirm to replace",
                target.display()
            )));
        }

        self.watcher.pause().await;
        let staging = target.join(format!(
            ".restore-staging-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(1)
        ));
        let result = self
            .restore_backup_inner(&backup, &target, live, &staging)
            .await;
        let _ = std::fs::remove_dir_all(&staging);
        if live {
            match self.list_indexed_locations().await {
                Ok(roots) => {
                    for root in roots {
                        self.watcher.add_root(root).await;
                    }
                }
                Err(err) => tracing::warn!(error = %err, "re-arm watcher after restore failed"),
            }
        }
        self.watcher.resume().await;
        result
    }

    async fn restore_backup_inner(
        &self,
        backup: &Path,
        target: &Path,
        live: bool,
        staging: &Path,
    ) -> Result<BackupRestoreReport, CatalogError> {
        let backup = backup.to_path_buf();
        let staging_owned = staging.to_path_buf();
        let (vault_n, mut notes) = tokio::task::spawn_blocking(move || {
            crate::backup::extract_restore(&backup, &staging_owned)
        })
        .await
        .map_err(|e| CatalogError::msg(e.to_string()))??;

        let snap = staging.join("catalog.sqlite");
        if live {
            self.call(|reply| Command::ReplaceFromSnapshot {
                snapshot: snap,
                live: self.catalog_path.clone(),
                reply,
            })
            .await?;
            let staged_vault = staging.join("vault");
            if staged_vault.exists() {
                let live_vault = self.data_home.join("vault");
                if live_vault.exists() {
                    let bak = self.data_home.join("vault.bak");
                    let _ = std::fs::remove_dir_all(&bak);
                    let _ = std::fs::rename(&live_vault, &bak);
                }
                std::fs::create_dir_all(live_vault.parent().unwrap_or(Path::new(".")))
                    .map_err(|e| CatalogError::msg(e.to_string()))?;
                std::fs::rename(&staged_vault, &live_vault)
                    .or_else(|_| copy_dir(&staged_vault, &live_vault))
                    .map_err(|e| CatalogError::msg(e.to_string()))?;
            }
            notes.push("live catalog connection reopened from snapshot".into());
            notes.push("watcher re-armed from indexed_locations".into());
        } else {
            std::fs::create_dir_all(target).map_err(|e| CatalogError::msg(e.to_string()))?;
            let dest_db = target.join("catalog.sqlite");
            std::fs::copy(&snap, &dest_db).map_err(|e| CatalogError::msg(e.to_string()))?;
            let staged_vault = staging.join("vault");
            if staged_vault.exists() {
                let dest_vault = target.join("vault");
                let _ = std::fs::remove_dir_all(&dest_vault);
                copy_dir(&staged_vault, &dest_vault)?;
            }
            notes.push("restored into target data home (restart service with FILEFLOW_DATA_HOME to use it, or restore into the live home)".into());
        }

        Ok(BackupRestoreReport {
            ok: true,
            target_data_home: target.display().to_string(),
            restored_vault_objects: vault_n,
            notes,
        })
    }

    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, CatalogError>>) -> Command,
    ) -> Result<T, CatalogError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(make(reply))
            .await
            .map_err(|_| CatalogError::msg("catalog writer stopped"))?;
        rx.await
            .map_err(|_| CatalogError::msg("catalog writer dropped reply"))?
    }
}

fn same_path(a: &Path, b: &Path) -> bool {
    if a == b {
        return true;
    }
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

fn copy_dir(src: &Path, dst: &Path) -> Result<(), CatalogError> {
    std::fs::create_dir_all(dst).map_err(|e| CatalogError::msg(e.to_string()))?;
    for entry in walkdir::WalkDir::new(src)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
    {
        let rel = entry.path().strip_prefix(src).unwrap_or(entry.path());
        let to = dst.join(rel);
        if let Some(parent) = to.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CatalogError::msg(e.to_string()))?;
        }
        std::fs::copy(entry.path(), &to).map_err(|e| CatalogError::msg(e.to_string()))?;
    }
    Ok(())
}
