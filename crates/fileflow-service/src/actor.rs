use fileflow_catalog::{Catalog, CatalogError};
use fileflow_core::{LogicalFileId, LogicalFileView, SearchHit};
use tokio::sync::{mpsc, oneshot};

const QUEUE_BOUND: usize = 64;

type Reply<T> = oneshot::Sender<Result<T, CatalogError>>;

pub enum Command {
    ResolvePath {
        path: String,
        reply: Reply<Option<LogicalFileId>>,
    },
    UpsertIndexed {
        path: String,
        sha256: String,
        size: u64,
        reply: Reply<LogicalFileId>,
    },
    UpdatePath {
        id: LogicalFileId,
        new_path: String,
        reply: Reply<()>,
    },
    GetFile {
        id: LogicalFileId,
        reply: Reply<Option<LogicalFileView>>,
    },
    Search {
        query: String,
        reply: Reply<Vec<SearchHit>>,
    },
    AddTag {
        id: LogicalFileId,
        tag: String,
        reply: Reply<()>,
    },
    RemoveTag {
        id: LogicalFileId,
        tag: String,
        reply: Reply<()>,
    },
    SetNote {
        id: LogicalFileId,
        body: String,
        reply: Reply<()>,
    },
    AddTodo {
        id: LogicalFileId,
        title: String,
        reply: Reply<i64>,
    },
    SetTodoDone {
        todo_id: i64,
        done: bool,
        reply: Reply<()>,
    },
    AddIndexedLocation {
        path: String,
        reply: Reply<String>,
    },
    ListIndexedLocations {
        reply: Reply<Vec<String>>,
    },
    RemoveIndexedLocation {
        path: String,
        reply: Reply<()>,
    },
    TombstonePath {
        path: String,
        reply: Reply<Option<LogicalFileId>>,
    },
    LookupAnyPath {
        path: String,
        reply: Reply<Option<LogicalFileId>>,
    },
    SetVaulted {
        id: LogicalFileId,
        vaulted: bool,
        reply: Reply<()>,
    },
    IsVaulted {
        id: LogicalFileId,
        reply: Reply<bool>,
    },
    MarkBlobPresent {
        sha256: String,
        reply: Reply<()>,
    },
    ListRevisions {
        id: LogicalFileId,
        reply: Reply<Vec<fileflow_core::RevisionInfo>>,
    },
    GetRevision {
        id: LogicalFileId,
        revision_id: i64,
        reply: Reply<fileflow_core::RevisionInfo>,
    },
    SetCurrentRevision {
        id: LogicalFileId,
        revision_id: i64,
        reply: Reply<fileflow_core::RevisionInfo>,
    },
    SetVaultKeepLast {
        id: LogicalFileId,
        keep_last: Option<u32>,
        reply: Reply<()>,
    },
    PruneRevisions {
        id: LogicalFileId,
        keep_last: u32,
        reply: Reply<Vec<String>>,
    },
}

pub fn spawn_writer(mut catalog: Catalog) -> mpsc::Sender<Command> {
    let (tx, mut rx) = mpsc::channel::<Command>(QUEUE_BOUND);
    std::thread::spawn(move || {
        while let Some(cmd) = rx.blocking_recv() {
            match cmd {
                Command::ResolvePath { path, reply } => {
                    let _ = reply.send(catalog.resolve_path(&path));
                }
                Command::UpsertIndexed {
                    path,
                    sha256,
                    size,
                    reply,
                } => {
                    let _ = reply.send(catalog.upsert_indexed(&path, &sha256, size));
                }
                Command::UpdatePath {
                    id,
                    new_path,
                    reply,
                } => {
                    let _ = reply.send(catalog.update_path(id, &new_path));
                }
                Command::GetFile { id, reply } => {
                    let _ = reply.send(catalog.get_file(id));
                }
                Command::Search { query, reply } => {
                    let _ = reply.send(catalog.search(&query));
                }
                Command::AddTag { id, tag, reply } => {
                    let _ = reply.send(catalog.add_tag(id, &tag));
                }
                Command::RemoveTag { id, tag, reply } => {
                    let _ = reply.send(catalog.remove_tag(id, &tag));
                }
                Command::SetNote { id, body, reply } => {
                    let _ = reply.send(catalog.set_note(id, &body));
                }
                Command::AddTodo { id, title, reply } => {
                    let _ = reply.send(catalog.add_todo(id, &title));
                }
                Command::SetTodoDone {
                    todo_id,
                    done,
                    reply,
                } => {
                    let _ = reply.send(catalog.set_todo_done(todo_id, done));
                }
                Command::AddIndexedLocation { path, reply } => {
                    let _ = reply.send(catalog.add_indexed_location(&path));
                }
                Command::ListIndexedLocations { reply } => {
                    let _ = reply.send(catalog.list_indexed_locations());
                }
                Command::RemoveIndexedLocation { path, reply } => {
                    let _ = reply.send(catalog.remove_indexed_location(&path));
                }
                Command::TombstonePath { path, reply } => {
                    let _ = reply.send(catalog.tombstone_path(&path));
                }
                Command::LookupAnyPath { path, reply } => {
                    let _ = reply.send(catalog.resolve_path_any(&path));
                }
                Command::SetVaulted { id, vaulted, reply } => {
                    let _ = reply.send(catalog.set_vaulted(id, vaulted));
                }
                Command::IsVaulted { id, reply } => {
                    let _ = reply.send(catalog.is_vaulted(id));
                }
                Command::MarkBlobPresent { sha256, reply } => {
                    let _ = reply.send(catalog.mark_blob_present(&sha256));
                }
                Command::ListRevisions { id, reply } => {
                    let _ = reply.send(catalog.list_revisions(id));
                }
                Command::GetRevision {
                    id,
                    revision_id,
                    reply,
                } => {
                    let _ = reply.send(catalog.get_revision(id, revision_id));
                }
                Command::SetCurrentRevision {
                    id,
                    revision_id,
                    reply,
                } => {
                    let _ = reply.send(catalog.set_current_revision(id, revision_id));
                }
                Command::SetVaultKeepLast {
                    id,
                    keep_last,
                    reply,
                } => {
                    let _ = reply.send(catalog.set_vault_keep_last(id, keep_last));
                }
                Command::PruneRevisions {
                    id,
                    keep_last,
                    reply,
                } => {
                    let _ = reply.send(catalog.prune_revisions(id, keep_last));
                }
            }
        }
    });
    tx
}
