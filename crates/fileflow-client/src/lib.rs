//! Thin client stub: IPC only. Never opens the catalog.

use fileflow_core::{
    Config, DuplicateActionReport, DuplicateScan, LogicalFileId, LogicalFileView, SearchHit,
};
use fileflow_rpc::{connect, IndexReport, Request, Response, RpcConnection, TransportError};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error("service error: {0}")]
    Service(String),
    #[error("unexpected response")]
    Unexpected,
}

pub struct FileFlowClient<S> {
    conn: RpcConnection<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> FileFlowClient<S> {
    pub fn new(conn: RpcConnection<S>) -> Self {
        Self { conn }
    }

    pub async fn health(&mut self) -> Result<(String, String), ClientError> {
        match self.conn.call(&Request::Health).await? {
            Response::Health { status, version } => Ok((status, version)),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn resolve_path(&mut self, path: &str) -> Result<Option<LogicalFileId>, ClientError> {
        match self
            .conn
            .call(&Request::ResolvePath {
                path: path.to_string(),
            })
            .await?
        {
            Response::LogicalId { logical_file_id } => Ok(logical_file_id),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn index_path(&mut self, path: &str) -> Result<IndexReport, ClientError> {
        match self
            .conn
            .call(&Request::IndexPath {
                path: path.to_string(),
            })
            .await?
        {
            Response::Indexed { report } => Ok(report),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn index_folder(&mut self, path: &str) -> Result<IndexReport, ClientError> {
        match self
            .conn
            .call(&Request::IndexFolder {
                path: path.to_string(),
            })
            .await?
        {
            Response::Indexed { report } => Ok(report),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn get_file(
        &mut self,
        logical_file_id: LogicalFileId,
    ) -> Result<Option<LogicalFileView>, ClientError> {
        match self
            .conn
            .call(&Request::GetFile { logical_file_id })
            .await?
        {
            Response::File { file } => Ok(file),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn search(&mut self, query: &str) -> Result<Vec<SearchHit>, ClientError> {
        match self
            .conn
            .call(&Request::Search {
                query: query.to_string(),
            })
            .await?
        {
            Response::SearchResults { hits } => Ok(hits),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn add_tag(
        &mut self,
        logical_file_id: LogicalFileId,
        tag: &str,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::AddTag {
            logical_file_id,
            tag: tag.to_string(),
        })
        .await
    }

    pub async fn remove_tag(
        &mut self,
        logical_file_id: LogicalFileId,
        tag: &str,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::RemoveTag {
            logical_file_id,
            tag: tag.to_string(),
        })
        .await
    }

    pub async fn set_note(
        &mut self,
        logical_file_id: LogicalFileId,
        body: &str,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::SetNote {
            logical_file_id,
            body: body.to_string(),
        })
        .await
    }

    pub async fn add_todo(
        &mut self,
        logical_file_id: LogicalFileId,
        title: &str,
    ) -> Result<i64, ClientError> {
        match self
            .conn
            .call(&Request::AddTodo {
                logical_file_id,
                title: title.to_string(),
            })
            .await?
        {
            Response::TodoCreated { todo_id } => Ok(todo_id),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn set_todo_done(&mut self, todo_id: i64, done: bool) -> Result<(), ClientError> {
        self.expect_ok(Request::SetTodoDone { todo_id, done }).await
    }

    pub async fn update_path(
        &mut self,
        logical_file_id: LogicalFileId,
        new_path: &str,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::UpdatePath {
            logical_file_id,
            new_path: new_path.to_string(),
        })
        .await
    }

    pub async fn pause_watcher(&mut self) -> Result<(), ClientError> {
        self.expect_ok(Request::PauseWatcher).await
    }

    pub async fn resume_watcher(&mut self) -> Result<(), ClientError> {
        self.expect_ok(Request::ResumeWatcher).await
    }

    pub async fn watcher_status(&mut self) -> Result<fileflow_rpc::WatcherStatus, ClientError> {
        match self.conn.call(&Request::GetWatcherStatus).await? {
            Response::WatcherStatus { status } => Ok(status),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn list_indexed_locations(&mut self) -> Result<Vec<String>, ClientError> {
        match self.conn.call(&Request::ListIndexedLocations).await? {
            Response::IndexedLocations { roots } => Ok(roots),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn add_to_vault(
        &mut self,
        logical_file_id: LogicalFileId,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::AddToVault { logical_file_id })
            .await
    }

    pub async fn remove_from_vault(
        &mut self,
        logical_file_id: LogicalFileId,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::RemoveFromVault { logical_file_id })
            .await
    }

    pub async fn list_revisions(
        &mut self,
        logical_file_id: LogicalFileId,
    ) -> Result<Vec<fileflow_core::RevisionInfo>, ClientError> {
        match self
            .conn
            .call(&Request::ListRevisions { logical_file_id })
            .await?
        {
            Response::Revisions { revisions } => Ok(revisions),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn get_revision(
        &mut self,
        logical_file_id: LogicalFileId,
        revision_id: i64,
    ) -> Result<fileflow_core::RevisionInfo, ClientError> {
        match self
            .conn
            .call(&Request::GetRevision {
                logical_file_id,
                revision_id,
            })
            .await?
        {
            Response::Revision { revision } => Ok(revision),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn prune_revisions(
        &mut self,
        logical_file_id: LogicalFileId,
        keep_last: u32,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::PruneRevisions {
            logical_file_id,
            keep_last,
        })
        .await
    }

    pub async fn set_current_revision(
        &mut self,
        logical_file_id: LogicalFileId,
        revision_id: i64,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::SetCurrentRevision {
            logical_file_id,
            revision_id,
        })
        .await
    }

    pub async fn export_revision(
        &mut self,
        logical_file_id: LogicalFileId,
        revision_id: i64,
        dest_path: &str,
    ) -> Result<(), ClientError> {
        self.expect_ok(Request::ExportRevision {
            logical_file_id,
            revision_id,
            dest_path: dest_path.to_string(),
        })
        .await
    }

    pub async fn verify_revision(
        &mut self,
        logical_file_id: LogicalFileId,
        revision_id: i64,
    ) -> Result<fileflow_core::IntegrityReport, ClientError> {
        match self
            .conn
            .call(&Request::VerifyRevision {
                logical_file_id,
                revision_id,
            })
            .await?
        {
            Response::Integrity { report } => Ok(report),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn verify_content_object(
        &mut self,
        sha256: &str,
    ) -> Result<fileflow_core::IntegrityReport, ClientError> {
        match self
            .conn
            .call(&Request::VerifyContentObject {
                sha256: sha256.to_string(),
            })
            .await?
        {
            Response::Integrity { report } => Ok(report),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn find_duplicates(
        &mut self,
        min_group_size: Option<u32>,
        path_prefix: Option<&str>,
        exclude_patterns: Option<Vec<String>>,
        exclude_common_build_vcs_dirs: Option<bool>,
        min_size: Option<u64>,
        include_missing: Option<bool>,
    ) -> Result<DuplicateScan, ClientError> {
        match self
            .conn
            .call(&Request::FindDuplicates {
                min_group_size,
                path_prefix: path_prefix.map(|s| s.to_string()),
                exclude_patterns,
                exclude_common_build_vcs_dirs,
                min_size,
                include_missing,
            })
            .await?
        {
            Response::DuplicateScan { scan } => Ok(scan),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn resolve_duplicate_group(
        &mut self,
        sha256: &str,
        keep_logical_file_id: Option<LogicalFileId>,
        delete_logical_file_ids: Vec<LogicalFileId>,
        confirm: bool,
        confirm_permanent: bool,
        allow_delete_all: bool,
    ) -> Result<DuplicateActionReport, ClientError> {
        match self
            .conn
            .call(&Request::ResolveDuplicateGroup {
                sha256: sha256.to_string(),
                keep_logical_file_id,
                delete_logical_file_ids,
                confirm,
                confirm_permanent,
                allow_delete_all,
            })
            .await?
        {
            Response::DuplicateAction { report } => Ok(report),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    pub async fn delete_duplicate_member(
        &mut self,
        logical_file_id: LogicalFileId,
        confirm: bool,
        confirm_permanent: bool,
        allow_delete_last: bool,
    ) -> Result<DuplicateActionReport, ClientError> {
        match self
            .conn
            .call(&Request::DeleteDuplicateMember {
                logical_file_id,
                confirm,
                confirm_permanent,
                allow_delete_last,
            })
            .await?
        {
            Response::DuplicateAction { report } => Ok(report),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }

    async fn expect_ok(&mut self, req: Request) -> Result<(), ClientError> {
        match self.conn.call(&req).await? {
            Response::Ok => Ok(()),
            Response::Error { message } => Err(ClientError::Service(message)),
            _ => Err(ClientError::Unexpected),
        }
    }
}

/// Connect using `FILEFLOW_SOCKET` / platform default.
pub async fn connect_from_env(
) -> Result<FileFlowClient<impl AsyncRead + AsyncWrite + Unpin>, ClientError> {
    let cfg = Config::from_env();
    let conn = connect(&cfg.socket).await?;
    Ok(FileFlowClient::new(conn))
}
