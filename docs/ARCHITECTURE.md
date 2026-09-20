# FileFlow architecture (v0)

FileFlow adds a **logical file** layer on top of the ordinary filesystem. The OS path is location and discovery information, not identity. After a path is resolved, every API uses `logical_file_id` (`i64`).

```
Logical File
  ├── current path(s)
  ├── tags
  ├── tasks/TODOs
  ├── notes
  └── revisions → SHA-256 content object
```

## Authority

**FileFlowService is the sole authority over persistent SQLite/catalog state and vault blobs.**

- Only `fileflow-catalog` links `rusqlite`.
- Only the service process opens `catalog.sqlite` and writes `$FILEFLOW_DATA_HOME/vault/`.
- `fileflow-shell` and `fileflow-client` never open the database or vault objects.
- UI mutations go through RPC (`AddTag`, `AddToVault`, `SetCurrentRevision`, …).

## Identity

| Concept | Role |
| --- | --- |
| `logical_file_id` | Stable identity of a user-facing file. Survives rename/move (`UpdatePath`). |
| Path | Location used for discovery (`IndexPath` / `IndexFolder`) and `ResolvePath(path) -> logical_file_id`. |
| SHA-256 | Content identity of a **revision** / content object. Used for hashing and future dedup, **not** as the logical file id. Two logical files may share a hash (copies). Re-indexing the same path with new bytes appends a revision. |

## Mutation path

1. Client sends an RPC request over the transport.  
2. Indexing **hashes with a streaming SHA-256** (`64 KiB` buffer) **outside** the database.  
3. Catalog writes enter a **single-threaded writer actor** (bounded `mpsc` queue, short transactions).  
4. Large files are never loaded fully into memory and are never hashed inside a long DB transaction.

Reads in v0 also go through the same actor so there is one owner of the `rusqlite::Connection`.

## Indexed locations and watcher

`IndexFolder` persists the folder in `indexed_locations`. FileFlowService watches those roots with the `notify` crate (recursive). Events are coalesced with a ~200ms debounce on a **bounded** queue (overflow drops events and records `last_error`).

| Disk event | Catalog |
| --- | --- |
| Create | Stream-hash and `upsert_indexed` (new logical id, or reuse if the path was tombstoned). |
| Modify | Re-hash outside the DB; new revision if SHA-256 changed. |
| Rename/move | `UpdatePath` — **same** `logical_file_id` when the watcher can correlate from→to. |
| Delete | **Tombstone**: `file_paths.is_current = 0`. Logical file + tags/notes/todos/revisions stay. Search hides it. Recreate at the same path reattaches the same id. |

The UI never watches the disk. RPC: `GetWatcherStatus`, `PauseWatcher`, `ResumeWatcher`, `ListIndexedLocations`. Resume re-watches roots and rescans.

Windows: `ReadDirectoryChangesW` is noisy and often emits remove+create instead of a native rename; the service pairs a same-directory delete+create in one debounce window as a rename. Prefer a slightly longer quiet period if a save-to-temp workflow mis-pairs.

## Vault and versions

Ordinary indexed files are **metadata only** (path, tags, notes, todos, and a catalog hash/revision row). **Vaulted** files also persist immutable content objects.

Layout:

```
$FILEFLOW_DATA_HOME/
  catalog.sqlite
  vault/objects/{aa}/{bb}/{sha256}
```

`aa`/`bb` are the first four hex characters of the SHA-256. Blob copy is streamed (64 KiB); hashing and blob I/O happen **outside** catalog transactions.

| RPC | Behavior |
| --- | --- |
| `AddToVault` | Mark vaulted; stream current path into CAS if readable; ensure a current revision. |
| `RemoveFromVault` | Clear membership. Historical objects remain until prune. |
| Watcher/Index hash change (vaulted) | Store new object if absent; append revision; update current pointer. |
| `ListRevisions` / `GetRevision` | Metadata: id, hash, size, timestamp, current flag, blob present. |
| `SetCurrentRevision` | Select which revision is current **and** restore bytes to the current path (temp + rename). Does not delete other versions. |
| `ExportRevision` | Write bytes to a destination path without changing current. |
| `PruneRevisions(keep_last)` | Keep last N plus the current revision. Default policy is keep-forever (`vault_keep_last` NULL). |
| `VerifyRevision` / `VerifyContentObject` | Re-hash the blob and compare. |

Revisions belong to `logical_file_id`. Tombstoned paths keep revision history. Restore can fail if the destination is locked by Excel/Photoshop/etc.

## IPC

Client/server **stub** pattern:

- Framing: `u32` big-endian length + JSON body.  
- Windows: named pipe (`\\.\pipe\FileFlow` by default).  
- Linux/macOS: Unix domain socket.  
- Transport is isolated in `fileflow-rpc`; swapping to another byte pipe should not change `Request` / `Response`.

## Process layout

```
fileflow-shell  --RPC-->  fileflow-service  --owns-->  catalog.sqlite
                                          --owns-->  vault/objects/…
fileflow-client  (same stub)
```

## Out of scope (v1)

Encrypted vault, cloud backup/`.ffbackup`, Explorer shell extension/NSIS, Office/CAD preview workers, duplicate-manager UX, email transport.
