use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use fileflow_core::{parse_search, LogicalFileId, LogicalFileView, SearchHit, TodoItem};
use rusqlite::{params, Connection, OptionalExtension};

use crate::schema::migrate;
use crate::{CatalogError, Result};

/// Persistent catalog. Caller (FileFlowService) serializes writes.
pub struct Catalog {
    conn: Connection,
}

impl Catalog {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CatalogError::msg(e.to_string()))?;
        }
        let conn = Connection::open(path)?;
        migrate(&conn)?;
        Ok(Self { conn })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        migrate(&conn)?;
        Ok(Self { conn })
    }

    pub fn resolve_path(&self, path: &str) -> Result<Option<LogicalFileId>> {
        self.lookup_path(path, true)
    }

    /// Resolve even if the path is tombstoned (`is_current = 0`).
    pub fn resolve_path_any(&self, path: &str) -> Result<Option<LogicalFileId>> {
        self.lookup_path(path, false)
    }

    fn lookup_path(&self, path: &str, current_only: bool) -> Result<Option<LogicalFileId>> {
        let sql = if current_only {
            "SELECT logical_file_id FROM file_paths WHERE path = ?1 AND is_current = 1"
        } else {
            "SELECT logical_file_id FROM file_paths WHERE path = ?1"
        };
        for candidate in path_candidates(path) {
            let found = self
                .conn
                .query_row(sql, params![candidate], |row| row.get(0))
                .optional()?;
            if found.is_some() {
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// Record an indexed file. Hashing must already have happened outside this txn.
    pub fn upsert_indexed(&mut self, path: &str, sha256: &str, size: u64) -> Result<LogicalFileId> {
        let path = normalize_path(path);
        let now = now_secs();
        let tx = self.conn.transaction()?;

        tx.execute(
            "INSERT INTO content_objects (sha256, size) VALUES (?1, ?2)
             ON CONFLICT(sha256) DO UPDATE SET size = excluded.size",
            params![sha256, size as i64],
        )?;

        let existing: Option<LogicalFileId> = tx
            .query_row(
                "SELECT logical_file_id FROM file_paths WHERE path = ?1",
                params![path],
                |row| row.get(0),
            )
            .optional()?;

        let id = if let Some(id) = existing {
            tx.execute(
                "UPDATE logical_files SET updated_at = ?1 WHERE id = ?2",
                params![now, id],
            )?;
            tx.execute(
                "UPDATE file_paths SET is_current = 1 WHERE path = ?1",
                params![path],
            )?;
            id
        } else {
            tx.execute(
                "INSERT INTO logical_files (created_at, updated_at) VALUES (?1, ?1)",
                params![now],
            )?;
            let id = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO file_paths (logical_file_id, path, is_current) VALUES (?1, ?2, 1)",
                params![id, path],
            )?;
            id
        };

        let latest: Option<String> = tx
            .query_row(
                "SELECT sha256 FROM revisions WHERE logical_file_id = ?1 ORDER BY id DESC LIMIT 1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if latest.as_deref() != Some(sha256) {
            tx.execute(
                "INSERT INTO revisions (logical_file_id, sha256, created_at) VALUES (?1, ?2, ?3)",
                params![id, sha256, now],
            )?;
        }

        tx.commit()?;
        Ok(id)
    }

    /// Rewrite the current path; logical id is unchanged.
    pub fn update_path(&mut self, id: LogicalFileId, new_path: &str) -> Result<()> {
        let new_path = normalize_path(new_path);
        let now = now_secs();
        let tx = self.conn.transaction()?;
        let exists: Option<i64> = tx
            .query_row(
                "SELECT id FROM logical_files WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(CatalogError::NotFound(format!("logical_file {id}")));
        }
        tx.execute(
            "DELETE FROM file_paths WHERE logical_file_id = ?1 AND is_current = 1",
            params![id],
        )?;
        tx.execute(
            "INSERT INTO file_paths (logical_file_id, path, is_current) VALUES (?1, ?2, 1)",
            params![id, new_path],
        )?;
        tx.execute(
            "UPDATE logical_files SET updated_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Keep the logical file (tags/notes/todos/revisions). Clear live path(s).
    /// Recreating the same pathname later reuses the id via `upsert_indexed`.
    pub fn tombstone_path(&mut self, path: &str) -> Result<Option<LogicalFileId>> {
        let mut last = None;
        let tx = self.conn.transaction()?;
        let current: Vec<(LogicalFileId, String)> = {
            let mut stmt =
                tx.prepare("SELECT logical_file_id, path FROM file_paths WHERE is_current = 1")?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            let collected = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            collected
        };
        let candidates = path_candidates(path);
        for (id, stored) in current {
            let hit = candidates.iter().any(|c| {
                stored == *c
                    || stored.starts_with(&format!("{c}/"))
                    || stored.starts_with(&format!("{c}\\"))
            });
            if hit {
                tx.execute(
                    "UPDATE file_paths SET is_current = 0 WHERE path = ?1",
                    params![stored],
                )?;
                last = Some(id);
            }
        }
        tx.commit()?;
        Ok(last)
    }

    pub fn add_indexed_location(&mut self, path: &str) -> Result<String> {
        let stored = normalize_dir(path);
        let now = now_secs();
        self.conn.execute(
            "INSERT INTO indexed_locations (path, created_at) VALUES (?1, ?2)
             ON CONFLICT(path) DO NOTHING",
            params![stored, now],
        )?;
        Ok(stored)
    }

    pub fn remove_indexed_location(&mut self, path: &str) -> Result<()> {
        for candidate in path_candidates(path) {
            self.conn.execute(
                "DELETE FROM indexed_locations WHERE path = ?1",
                params![candidate],
            )?;
        }
        Ok(())
    }

    pub fn list_indexed_locations(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT path FROM indexed_locations ORDER BY path")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn add_tag(&mut self, id: LogicalFileId, tag: &str) -> Result<()> {
        self.require_file(id)?;
        let tag = tag.trim().to_lowercase();
        if tag.is_empty() {
            return Err(CatalogError::msg("tag must not be empty"));
        }
        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO tags (name) VALUES (?1) ON CONFLICT(name) DO NOTHING",
            params![tag],
        )?;
        let tag_id: i64 =
            tx.query_row("SELECT id FROM tags WHERE name = ?1", params![tag], |row| {
                row.get(0)
            })?;
        tx.execute(
            "INSERT OR IGNORE INTO file_tags (logical_file_id, tag_id) VALUES (?1, ?2)",
            params![id, tag_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_tag(&mut self, id: LogicalFileId, tag: &str) -> Result<()> {
        let tag = tag.trim().to_lowercase();
        self.conn.execute(
            "DELETE FROM file_tags WHERE logical_file_id = ?1 AND tag_id = (
                SELECT id FROM tags WHERE name = ?2
            )",
            params![id, tag],
        )?;
        Ok(())
    }

    pub fn set_note(&mut self, id: LogicalFileId, body: &str) -> Result<()> {
        self.require_file(id)?;
        let now = now_secs();
        self.conn.execute(
            "INSERT INTO notes (logical_file_id, body, updated_at) VALUES (?1, ?2, ?3)
             ON CONFLICT(logical_file_id) DO UPDATE SET body = excluded.body, updated_at = excluded.updated_at",
            params![id, body, now],
        )?;
        Ok(())
    }

    pub fn add_todo(&mut self, id: LogicalFileId, title: &str) -> Result<i64> {
        self.require_file(id)?;
        let title = title.trim();
        if title.is_empty() {
            return Err(CatalogError::msg("todo title must not be empty"));
        }
        let now = now_secs();
        self.conn.execute(
            "INSERT INTO todos (logical_file_id, title, done, created_at) VALUES (?1, ?2, 0, ?3)",
            params![id, title, now],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    pub fn set_todo_done(&mut self, todo_id: i64, done: bool) -> Result<()> {
        let n = self.conn.execute(
            "UPDATE todos SET done = ?1 WHERE id = ?2",
            params![done as i64, todo_id],
        )?;
        if n == 0 {
            return Err(CatalogError::NotFound(format!("todo {todo_id}")));
        }
        Ok(())
    }

    pub fn get_file(&self, id: LogicalFileId) -> Result<Option<LogicalFileView>> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM logical_files WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Ok(None);
        }
        Ok(Some(self.load_view(id)?))
    }

    pub fn search(&self, query: &str) -> Result<Vec<SearchHit>> {
        let parsed = parse_search(query);
        let mut sql = String::from(
            "SELECT lf.id, fp.path FROM logical_files lf
             JOIN file_paths fp ON fp.logical_file_id = lf.id AND fp.is_current = 1
             LEFT JOIN notes n ON n.logical_file_id = lf.id
             WHERE 1=1",
        );
        let mut binds: Vec<String> = Vec::new();

        for text in &parsed.text {
            sql.push_str(" AND lower(fp.path) LIKE ?");
            binds.push(format!("%{text}%"));
        }
        for ext in &parsed.extensions {
            sql.push_str(" AND lower(fp.path) LIKE ?");
            binds.push(format!("%.{ext}"));
        }
        for note in &parsed.notes {
            sql.push_str(" AND lower(COALESCE(n.body, '')) LIKE ?");
            binds.push(format!("%{note}%"));
        }
        for tag in &parsed.tags {
            sql.push_str(
                " AND lf.id IN (
                    SELECT ft.logical_file_id FROM file_tags ft
                    JOIN tags t ON t.id = ft.tag_id
                    WHERE lower(t.name) = ?
                )",
            );
            binds.push(tag.clone());
        }
        for todo in &parsed.todos {
            sql.push_str(
                " AND lf.id IN (
                    SELECT todos.logical_file_id FROM todos
                    WHERE lower(todos.title) LIKE ?
                )",
            );
            binds.push(format!("%{todo}%"));
        }
        sql.push_str(" ORDER BY fp.path LIMIT 500");

        let mut stmt = self.conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = binds
            .iter()
            .map(|s| s as &dyn rusqlite::types::ToSql)
            .collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut hits = Vec::new();
        for row in rows {
            let (id, path) = row?;
            let view = self.load_view(id)?;
            hits.push(SearchHit {
                logical_file_id: id,
                path,
                tags: view.tags,
                sha256: view.sha256,
            });
        }
        Ok(hits)
    }

    fn load_view(&self, id: LogicalFileId) -> Result<LogicalFileView> {
        let mut paths = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "SELECT path FROM file_paths WHERE logical_file_id = ?1 AND is_current = 1 ORDER BY path",
            )?;
            let iter = stmt.query_map(params![id], |row| row.get::<_, String>(0))?;
            for p in iter {
                paths.push(p?);
            }
        }

        let mut tags = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "SELECT t.name FROM tags t
                 JOIN file_tags ft ON ft.tag_id = t.id
                 WHERE ft.logical_file_id = ?1 ORDER BY t.name",
            )?;
            let iter = stmt.query_map(params![id], |row| row.get::<_, String>(0))?;
            for t in iter {
                tags.push(t?);
            }
        }

        let note: String = self
            .conn
            .query_row(
                "SELECT body FROM notes WHERE logical_file_id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_default();

        let mut todos = Vec::new();
        {
            let mut stmt = self.conn.prepare(
                "SELECT id, title, done FROM todos WHERE logical_file_id = ?1 ORDER BY id",
            )?;
            let iter = stmt.query_map(params![id], |row| {
                Ok(TodoItem {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    done: row.get::<_, i64>(2)? != 0,
                })
            })?;
            for t in iter {
                todos.push(t?);
            }
        }

        let content: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT r.sha256, c.size FROM revisions r
                 JOIN content_objects c ON c.sha256 = r.sha256
                 WHERE r.logical_file_id = ?1
                 ORDER BY r.id DESC LIMIT 1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        Ok(LogicalFileView {
            logical_file_id: id,
            paths,
            tags,
            note,
            todos,
            sha256: content.as_ref().map(|(s, _)| s.clone()),
            size: content.map(|(_, n)| n as u64),
        })
    }

    fn require_file(&self, id: LogicalFileId) -> Result<()> {
        let exists: Option<i64> = self
            .conn
            .query_row(
                "SELECT id FROM logical_files WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            Err(CatalogError::NotFound(format!("logical_file {id}")))
        } else {
            Ok(())
        }
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn normalize_path(path: &str) -> String {
    let p = Path::new(path);
    match std::fs::canonicalize(p) {
        Ok(c) => c.to_string_lossy().into_owned(),
        Err(_) => path.replace('\\', "/"),
    }
}

fn normalize_dir(path: &str) -> String {
    normalize_path(path)
}

fn path_candidates(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let slash = path.replace('\\', "/");
    if let Ok(c) = std::fs::canonicalize(path) {
        out.push(c.to_string_lossy().into_owned());
    }
    out.push(slash);
    if !out.iter().any(|p| p == path) {
        out.push(path.to_string());
    }
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn identity_survives_path_update() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let dir = tempdir().unwrap();
        let a = dir.path().join("alpha.txt");
        let b = dir.path().join("beta.txt");
        std::fs::write(&a, b"hello").unwrap();
        std::fs::write(&b, b"hello").unwrap();

        let id = cat
            .upsert_indexed(a.to_str().unwrap(), "abc123", 5)
            .unwrap();
        assert_eq!(cat.resolve_path(a.to_str().unwrap()).unwrap(), Some(id));

        cat.update_path(id, b.to_str().unwrap()).unwrap();
        assert_eq!(cat.resolve_path(b.to_str().unwrap()).unwrap(), Some(id));
        assert_eq!(cat.resolve_path(a.to_str().unwrap()).unwrap(), None);

        let view = cat.get_file(id).unwrap().unwrap();
        assert_eq!(view.logical_file_id, id);
        assert!(view
            .paths
            .iter()
            .any(|p| p.ends_with("beta.txt") || p.contains("beta.txt")));
    }

    #[test]
    fn tags_notes_todos_and_search() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("report.pdf");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"%PDF").unwrap();

        let id = cat
            .upsert_indexed(path.to_str().unwrap(), "deadbeef", 4)
            .unwrap();
        cat.add_tag(id, "Work").unwrap();
        cat.set_note(id, "Q4 draft").unwrap();
        cat.add_todo(id, "review").unwrap();

        let hits = cat
            .search("tag:work ext:pdf notes:draft todo:review")
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].logical_file_id, id);

        let view = cat.get_file(id).unwrap().unwrap();
        assert_eq!(view.tags, vec!["work"]);
        assert_eq!(view.note, "Q4 draft");
        assert_eq!(view.todos.len(), 1);
        assert_eq!(view.todos[0].title, "review");
    }

    #[test]
    fn same_path_keeps_logical_id_new_revision_on_hash_change() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("doc.txt");
        std::fs::write(&path, b"v1").unwrap();
        let p = path.to_str().unwrap();
        let id1 = cat.upsert_indexed(p, "hash1", 2).unwrap();
        let id2 = cat.upsert_indexed(p, "hash2", 2).unwrap();
        assert_eq!(id1, id2);
        let view = cat.get_file(id1).unwrap().unwrap();
        assert_eq!(view.sha256.as_deref(), Some("hash2"));
    }

    #[test]
    fn tombstone_keeps_logical_file_and_metadata() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let dir = tempdir().unwrap();
        let path = dir.path().join("keep-me.txt");
        std::fs::write(&path, b"x").unwrap();
        let p = path.to_str().unwrap();
        let id = cat.upsert_indexed(p, "h1", 1).unwrap();
        cat.add_tag(id, "saved").unwrap();
        cat.set_note(id, "still here").unwrap();

        std::fs::remove_file(&path).unwrap();
        let tomb = cat.tombstone_path(p).unwrap();
        assert_eq!(tomb, Some(id));
        assert_eq!(cat.resolve_path(p).unwrap(), None);
        let view = cat.get_file(id).unwrap().unwrap();
        assert!(view.paths.is_empty());
        assert_eq!(view.tags, vec!["saved"]);
        assert_eq!(view.note, "still here");

        std::fs::write(&path, b"x").unwrap();
        let id2 = cat.upsert_indexed(p, "h1", 1).unwrap();
        assert_eq!(id, id2);
        assert_eq!(cat.resolve_path(p).unwrap(), Some(id));
    }

    #[test]
    fn indexed_locations_persist() {
        let mut cat = Catalog::open_in_memory().unwrap();
        let dir = tempdir().unwrap();
        let stored = cat
            .add_indexed_location(dir.path().to_str().unwrap())
            .unwrap();
        let listed = cat.list_indexed_locations().unwrap();
        assert_eq!(listed, vec![stored.clone()]);
        cat.remove_indexed_location(&stored).unwrap();
        assert!(cat.list_indexed_locations().unwrap().is_empty());
    }
}
