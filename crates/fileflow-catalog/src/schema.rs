use rusqlite::Connection;

use crate::Result;

const MIGRATION_001: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS logical_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_paths (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_file_id INTEGER NOT NULL REFERENCES logical_files(id) ON DELETE CASCADE,
    path TEXT NOT NULL UNIQUE,
    is_current INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_file_paths_logical ON file_paths(logical_file_id);
CREATE INDEX IF NOT EXISTS idx_file_paths_current ON file_paths(is_current);

CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS file_tags (
    logical_file_id INTEGER NOT NULL REFERENCES logical_files(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (logical_file_id, tag_id)
);

CREATE TABLE IF NOT EXISTS notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_file_id INTEGER NOT NULL UNIQUE REFERENCES logical_files(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_file_id INTEGER NOT NULL REFERENCES logical_files(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_todos_logical ON todos(logical_file_id);

CREATE TABLE IF NOT EXISTS content_objects (
    sha256 TEXT PRIMARY KEY,
    size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS revisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_file_id INTEGER NOT NULL REFERENCES logical_files(id) ON DELETE CASCADE,
    sha256 TEXT NOT NULL REFERENCES content_objects(sha256),
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_revisions_logical ON revisions(logical_file_id);
"#;

const MIGRATION_002: &str = r#"
CREATE TABLE IF NOT EXISTS indexed_locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
);
"#;

const MIGRATION_003: &str = r#"
ALTER TABLE logical_files ADD COLUMN vaulted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE logical_files ADD COLUMN current_revision_id INTEGER;
ALTER TABLE logical_files ADD COLUMN vault_keep_last INTEGER;
ALTER TABLE content_objects ADD COLUMN blob_present INTEGER NOT NULL DEFAULT 0;
"#;

pub fn migrate(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "foreign_keys", "ON")?;
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    conn.execute_batch(MIGRATION_001)?;
    let applied: i64 = conn.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = 1",
        [],
        |row| row.get(0),
    )?;
    if applied == 0 {
        conn.execute("INSERT INTO schema_migrations (version) VALUES (1)", [])?;
    }
    conn.execute_batch(MIGRATION_002)?;
    let applied2: i64 = conn.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = 2",
        [],
        |row| row.get(0),
    )?;
    if applied2 == 0 {
        conn.execute("INSERT INTO schema_migrations (version) VALUES (2)", [])?;
    }
    let applied3: i64 = conn.query_row(
        "SELECT COUNT(*) FROM schema_migrations WHERE version = 3",
        [],
        |row| row.get(0),
    )?;
    if applied3 == 0 {
        conn.execute_batch(MIGRATION_003)?;
        conn.execute(
            "UPDATE logical_files SET current_revision_id = (
                SELECT r.id FROM revisions r
                WHERE r.logical_file_id = logical_files.id
                ORDER BY r.id DESC LIMIT 1
            )",
            [],
        )?;
        conn.execute("INSERT INTO schema_migrations (version) VALUES (3)", [])?;
    }
    Ok(())
}
