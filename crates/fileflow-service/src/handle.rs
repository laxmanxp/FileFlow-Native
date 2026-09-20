use crate::vault::VaultStore;
use fileflow_catalog::{Catalog, CatalogError};
use fileflow_core::{
    DuplicateActionReport, DuplicateMember, IntegrityReport, LogicalFileId, LogicalFileView,
    RevisionInfo, SearchHit,
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
}

impl FileFlowService {
    pub fn spawn(catalog: Catalog, vault_root: PathBuf) -> Self {
        let tx = spawn_writer(catalog);
        let (watcher, cmd_rx, snap) = WatcherHandle::pair();
        let svc = Self {
            tx,
            watcher,
            vault: Arc::new(VaultStore::new(vault_root)),
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
