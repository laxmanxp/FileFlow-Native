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

**FileFlowService is the sole authority over persistent SQLite/catalog state.**

- Only `fileflow-catalog` links `rusqlite`.
- Only the service process opens `catalog.sqlite`.
- `fileflow-shell` and `fileflow-client` never open the database for reads or writes.
- UI mutations go through RPC (`AddTag`, `SetNote`, `AddTodo`, …).

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

## IPC

Client/server **stub** pattern:

- Framing: `u32` big-endian length + JSON body.  
- Windows: named pipe (`\\.\pipe\FileFlow` by default).  
- Linux/macOS: Unix domain socket.  
- Transport is isolated in `fileflow-rpc`; swapping to another byte pipe should not change `Request` / `Response`.

## Process layout

```
fileflow-shell  --RPC-->  fileflow-service  --owns-->  catalog.sqlite
fileflow-client  (same stub)
```

## Out of scope (v0)

Full vault UX, Explorer shell extension/NSIS, cloud backup, Office preview parsers, duplicate-manager UX.
