use fileflow_core::{
    DuplicateActionReport, DuplicateScan, IntegrityReport, LogicalFileId, LogicalFileView,
    RevisionInfo, SearchHit,
};
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
    PauseWatcher,
    ResumeWatcher,
    GetWatcherStatus,
    ListIndexedLocations,
    RemoveIndexedLocation {
        path: String,
    },
    AddToVault {
        logical_file_id: LogicalFileId,
    },
    RemoveFromVault {
        logical_file_id: LogicalFileId,
    },
    ListRevisions {
        logical_file_id: LogicalFileId,
    },
    GetRevision {
        logical_file_id: LogicalFileId,
        revision_id: i64,
    },
    SetCurrentRevision {
        logical_file_id: LogicalFileId,
        revision_id: i64,
    },
    ExportRevision {
        logical_file_id: LogicalFileId,
        revision_id: i64,
        dest_path: String,
    },
    PruneRevisions {
        logical_file_id: LogicalFileId,
        keep_last: u32,
    },
    VerifyRevision {
        logical_file_id: LogicalFileId,
        revision_id: i64,
    },
    VerifyContentObject {
        sha256: String,
    },
    FindDuplicates {
        min_group_size: Option<u32>,
        path_prefix: Option<String>,
        exclude_patterns: Option<Vec<String>>,
        exclude_common_build_vcs_dirs: Option<bool>,
        min_size: Option<u64>,
        include_missing: Option<bool>,
    },
    ResolveDuplicateGroup {
        sha256: String,
        keep_logical_file_id: Option<LogicalFileId>,
        delete_logical_file_ids: Vec<LogicalFileId>,
        confirm: bool,
        confirm_permanent: bool,
        allow_delete_all: bool,
    },
    DeleteDuplicateMember {
        logical_file_id: LogicalFileId,
        confirm: bool,
        confirm_permanent: bool,
        allow_delete_last: bool,
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
    WatcherStatus {
        status: WatcherStatus,
    },
    IndexedLocations {
        roots: Vec<String>,
    },
    Revisions {
        revisions: Vec<RevisionInfo>,
    },
    Revision {
        revision: RevisionInfo,
    },
    Integrity {
        report: IntegrityReport,
    },
    DuplicateScan {
        scan: DuplicateScan,
    },
    DuplicateAction {
        report: DuplicateActionReport,
    },
    Ok,
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatcherStatus {
    pub paused: bool,
    pub roots: Vec<String>,
    pub queue_depth: usize,
    pub last_error: Option<String>,
}

impl Response {
    pub fn err(msg: impl Into<String>) -> Self {
        Self::Error {
            message: msg.into(),
        }
    }
}
