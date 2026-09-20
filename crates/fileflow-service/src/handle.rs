use crate::vault::VaultStore;
use fileflow_catalog::{Catalog, CatalogError};
use fileflow_core::{IntegrityReport, LogicalFileId, LogicalFileView, RevisionInfo, SearchHit};
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
