use fileflow_catalog::{Catalog, CatalogError};
use fileflow_core::{LogicalFileId, LogicalFileView, SearchHit};
use tokio::sync::{mpsc, oneshot};

use crate::actor::{spawn_writer, Command};
use crate::health_status;
use crate::indexer::{index_folder, index_path};
use crate::watcher::WatcherHandle;
use fileflow_rpc::{Request, Response, WatcherStatus};

/// Service handle. Catalog writes serialize through the writer actor.
#[derive(Clone)]
pub struct FileFlowService {
    tx: mpsc::Sender<Command>,
    watcher: WatcherHandle,
}

impl FileFlowService {
    pub fn spawn(catalog: Catalog) -> Self {
        let tx = spawn_writer(catalog);
        let (watcher, cmd_rx, snap) = WatcherHandle::pair();
        let svc = Self { tx, watcher };
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
