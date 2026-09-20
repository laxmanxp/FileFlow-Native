use fileflow_core::{LogicalFileId, LogicalFileView, SearchHit};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Health,
    ResolvePath {
        path: String,
    },
    IndexPath {
        path: String,
    },
    IndexFolder {
        path: String,
    },
    GetFile {
        logical_file_id: LogicalFileId,
    },
    Search {
        query: String,
    },
    AddTag {
        logical_file_id: LogicalFileId,
        tag: String,
    },
    RemoveTag {
        logical_file_id: LogicalFileId,
        tag: String,
    },
    SetNote {
        logical_file_id: LogicalFileId,
        body: String,
    },
    AddTodo {
        logical_file_id: LogicalFileId,
        title: String,
    },
    SetTodoDone {
        todo_id: i64,
        done: bool,
    },
    UpdatePath {
        logical_file_id: LogicalFileId,
        new_path: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexReport {
    pub indexed: u64,
    pub skipped: u64,
    pub last_logical_file_id: Option<LogicalFileId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Health {
        status: String,
        version: String,
    },
    LogicalId {
        logical_file_id: Option<LogicalFileId>,
    },
    File {
        file: Option<LogicalFileView>,
    },
    SearchResults {
        hits: Vec<SearchHit>,
    },
    Indexed {
        report: IndexReport,
    },
    TodoCreated {
        todo_id: i64,
    },
    Ok,
    Error {
        message: String,
    },
}

impl Response {
    pub fn err(msg: impl Into<String>) -> Self {
        Self::Error {
            message: msg.into(),
        }
    }
}
