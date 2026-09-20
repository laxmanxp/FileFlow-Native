use serde::{Deserialize, Serialize};

/// Stable identity of a logical file. Survives rename/move. Not a content hash.
pub type LogicalFileId = i64;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TodoItem {
    pub id: i64,
    pub title: String,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalFileView {
    pub logical_file_id: LogicalFileId,
    pub paths: Vec<String>,
    pub tags: Vec<String>,
    pub note: String,
    pub todos: Vec<TodoItem>,
    pub sha256: Option<String>,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchHit {
    pub logical_file_id: LogicalFileId,
    pub path: String,
    pub tags: Vec<String>,
    pub sha256: Option<String>,
}
