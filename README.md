# FileFlow

Local-first file intelligence: searchable metadata, stable identity, deduplication hashes, tasks, and notes on ordinary files — without replacing the filesystem.

FileFlowService is the only process that opens the SQLite catalog. The desktop shell talks to it over IPC (Windows named pipe, Linux Unix domain socket).

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

1. **Add** — paste or Browse a folder, then Index.  
2. **Find** — search with free text plus `tag:`, `todo:`, `notes:`, `ext:`.  
3. **Open** / **Reveal** the selected file.  
4. **Edit** tags, notes, and todos (all keyed by `logical_file_id`).

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
| `FILEFLOW_DATA_HOME` | Directory for `catalog.sqlite` |
| `FILEFLOW_SOCKET` | Named pipe path (Windows) or Unix socket path (Linux/macOS) |

Defaults: `%LOCALAPPDATA%\FileFlow` and `\\.\pipe\FileFlow` on Windows; `~/.local/share/fileflow` and `$XDG_RUNTIME_DIR/fileflow.sock` on Linux.

## Tests

- Catalog: logical id survives path update; tags/notes/todos search.  
- Core: streaming SHA-256 for files larger than the hash buffer.  
- Service: Health, ResolvePath, IndexFolder, tag/note/todo, and path rewrite over Linux UDS.  
- Shell: manifest must not depend on `rusqlite` or `fileflow-catalog`.

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
