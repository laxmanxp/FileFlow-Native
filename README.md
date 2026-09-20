# FileFlow

Local-first file intelligence: searchable metadata, stable identity, deduplication hashes, tasks, and notes on ordinary files — without replacing the filesystem.

FileFlowService is the only process that opens the SQLite catalog. Indexed folder roots persist in SQLite (`indexed_locations`) so the service can keep watching them across restarts. The desktop shell talks to the service over IPC and does not watch the filesystem itself.

## Workspace

| Crate | Role |
| --- | --- |
| `fileflow-core` | Types, config, streaming SHA-256, search query parsing |
| `fileflow-catalog` | SQLite schema and catalog operations (service-only) |
| `fileflow-rpc` | Length-prefixed JSON RPC + swappable transport |
| `fileflow-service` | Per-user service binary and mutation worker |
| `fileflow-client` | Thin client stub (no SQLite) |
| `fileflow-shell` | egui desktop UI over glutin/winit (no SQLite) |

## Build

```bash
cargo build --workspace
cargo test --workspace
```

Linux UI needs an OpenGL-capable display (X11). Core, catalog, RPC, client, and service tests do not require a GUI.

## Run (Linux)

In one terminal:

```bash
# optional overrides
export FILEFLOW_DATA_HOME="$HOME/.local/share/fileflow"
export FILEFLOW_SOCKET="${XDG_RUNTIME_DIR:-/tmp}/fileflow.sock"

cargo run -p fileflow-service
```

In another:

```bash
cargo run -p fileflow-shell
```

1. **Add** — paste or Browse a folder, then Index (registers an indexed location and starts watching).  
2. **Find** — search with free text plus `tag:`, `todo:`, `notes:`, `ext:`.  
3. **Open** / **Reveal** the selected file.  
4. **Edit** tags, notes, todos. Optionally **Vault** the file to keep immutable content revisions; **Make current** restores a prior version to the current path.  
5. **Duplicates** — scan catalog hashes, review groups (“N identical files”), **Keep this**, then confirm deleting the other copies (no “Clean all”).  
6. **Backup** — choose a destination (including another drive), Create backup, Verify, Restore with confirm.

Creates, renames/moves, edits, and deletes under indexed folders update the catalog automatically. Delete **tombstones** the path (`is_current = 0`) but keeps the logical file and its tags/notes/todos. Pause/Resume watch from the shell or RPC.

Vaulted files also store content under `$FILEFLOW_DATA_HOME/vault/objects/{aa}/{bb}/{sha256}`. Ordinary indexed files stay metadata-only. Removing vault membership does **not** delete historical blobs (use prune / keep-last-N). Default retention is **keep forever**. Restore uses temp+rename; it can fail if another app has the file locked.

**Duplicates** are exact SHA-256 matches among **current** catalog paths. Default scan excludes path segments `.git`, `node_modules`, `vendor`, `target`, `build`, `dist`, `.cache` (directory-name match, not a global “skip source code” rule). Source trees are not excluded; cleanup is report-first and requires a per-group confirmation. The service tries OS trash (Linux XDG Trash, macOS `~/.Trash`, Windows Recycle Bin). If trash fails, RPC needs `confirm_permanent=true` and the unlink is **permanent**. The last remaining copy in a hash group is not deleted unless `allow_delete_all` / `allow_delete_last` is set.

**Backup** writes `$dest/FileFlow-Recovery-YYYYMMDD-HHMMSS.ffbackup` (ZIP) with a consistent SQLite snapshot, `fileflow.sql`, `manifest.json`, `checksums.sha256`, and vault objects when `include_vault` is true (default). Restore requires `confirm=true` and `force=true` to replace an existing catalog with files. The service must be running; large vaults stream into the zip. This is not a backup of every document on disk—only FileFlow metadata and vaulted content objects.

## Run (Windows)

```bat
set FILEFLOW_DATA_HOME=%LOCALAPPDATA%\FileFlow
set FILEFLOW_SOCKET=\\.\pipe\FileFlow

cargo run -p fileflow-service
cargo run -p fileflow-shell
```

The default pipe name is `\\.\pipe\FileFlow`.

## Configuration

| Variable | Meaning |
| --- | --- |
| `FILEFLOW_DATA_HOME` | Directory for `catalog.sqlite` and `vault/` |
| `FILEFLOW_SOCKET` | Named pipe path (Windows) or Unix socket path (Linux/macOS) |

Defaults: `%LOCALAPPDATA%\FileFlow` and `\\.\pipe\FileFlow` on Windows; `~/.local/share/fileflow` and `$XDG_RUNTIME_DIR/fileflow.sock` on Linux.

## Tests

- Catalog: logical id survives path update; tags/notes/todos search; tombstone keeps metadata.  
- Core: streaming SHA-256 for files larger than the hash buffer.  
- Service: Health, ResolvePath, IndexFolder, tag/note/todo, and path rewrite over Linux UDS.  
- Watcher: temp tree create/rename/modify/delete over Linux UDS (`notify` + debounce).  
- Vault: AddToVault stores blob; modify appends revision; SetCurrentRevision restores bytes; verify detects tampering.  
- Duplicates: FindDuplicates groups; exclusions hide `node_modules`; ResolveDuplicateGroup keeps one copy; refuse delete-all.  
- Backup: CreateBackup snapshot; Verify detects checksum tamper; Restore recovers catalog + vault blob.  
- Shell: manifest must not depend on `rusqlite` or `fileflow-catalog`.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
