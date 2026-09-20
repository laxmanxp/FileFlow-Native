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
    pub vaulted: bool,
    pub current_revision_id: Option<i64>,
    /// `None` means keep every revision (default). `Some(n)` keeps last n plus current.
    pub vault_keep_last: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionInfo {
    pub revision_id: i64,
    pub sha256: String,
    pub size: u64,
    pub created_at: i64,
    pub is_current: bool,
    pub blob_present: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntegrityReport {
    pub ok: bool,
    pub expected_sha256: String,
    pub actual_sha256: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchHit {
    pub logical_file_id: LogicalFileId,
    pub path: String,
    pub tags: Vec<String>,
    pub sha256: Option<String>,
}
