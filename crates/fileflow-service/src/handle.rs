use fileflow_catalog::{Catalog, CatalogError};
use fileflow_core::{LogicalFileId, LogicalFileView, SearchHit};
use tokio::sync::{mpsc, oneshot};

use crate::actor::{spawn_writer, Command};
use crate::health_status;
use crate::indexer::{index_folder, index_path};
use fileflow_rpc::{IndexReport, Request, Response};

/// Service handle. Catalog writes serialize through the writer actor.
#[derive(Clone)]
pub struct FileFlowService {
    tx: mpsc::Sender<Command>,
}

impl FileFlowService {
    pub fn spawn(catalog: Catalog) -> Self {
        Self {
            tx: spawn_writer(catalog),
        }
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
                self.call(|reply| Command::UpdatePath {
                    id: logical_file_id,
                    new_path,
                    reply,
                })
                .await?;
                Ok(Response::Ok)
            }
        }
    }

    pub async fn resolve_path(&self, path: &str) -> Result<Option<LogicalFileId>, CatalogError> {
        let path = path.to_string();
        self.call(|reply| Command::ResolvePath { path, reply })
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

pub async fn index_one_file(
    service: &FileFlowService,
    path: &std::path::Path,
) -> Result<LogicalFileId, CatalogError> {
    let path_owned = path.to_path_buf();
    let (sha256, size) = tokio::task::spawn_blocking(move || fileflow_core::hash_file(&path_owned))
        .await
        .map_err(|e| CatalogError::msg(e.to_string()))?
        .map_err(|e| CatalogError::msg(e.to_string()))?;
    service
        .upsert_indexed(path.to_string_lossy().into_owned(), sha256, size)
        .await
}

/// Used from indexer after hashing off the writer thread.
pub fn empty_report() -> IndexReport {
    IndexReport {
        indexed: 0,
        skipped: 0,
        last_logical_file_id: None,
    }
}
