use rusqlite::{params, Connection};
use std::sync::Mutex;

use crate::error::Result;

#[derive(Debug, Clone, serde::Serialize)]
pub struct LibraryItem {
    pub id: i64,
    pub magnet: String,
    pub title: Option<String>,
    pub state: String,
    pub added_at: String,
    pub file_path: Option<String>,
    pub size_bytes: Option<i64>,
    pub rqbit_id: Option<i64>,
    pub poster_path: Option<String>,
    pub year: Option<i32>,
    pub tmdb_id: Option<i64>,
    pub media_type: Option<String>,
    pub sequential: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TorrentStatus {
    pub id: i64,
    pub state: String,
    pub progress_pct: f64,
    pub download_speed_bps: u64,
    pub peers: u32,
    pub contiguous_bytes: u64,
    pub total_bytes: u64,
    pub is_local: bool,
}

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;",
        )?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS library (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                magnet TEXT NOT NULL,
                title TEXT,
                state TEXT DEFAULT 'connecting',
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                file_path TEXT,
                size_bytes INTEGER,
                rqbit_id INTEGER,
                poster_path TEXT,
                year INTEGER,
                tmdb_id INTEGER,
                media_type TEXT,
                sequential INTEGER NOT NULL DEFAULT 1
            );",
        )?;
        // Migrate: drop the obsolete "keep" column if a pre-removal database
        // still has it.
        let has_keep: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM pragma_table_info('library') WHERE name = 'keep')",
            [],
            |r| r.get(0),
        )?;
        if has_keep {
            conn.execute_batch("ALTER TABLE library DROP COLUMN keep")?;
        }
        // Migrate: add columns if missing (idempotent)
        for stmt in [
            "ALTER TABLE library ADD COLUMN poster_path TEXT",
            "ALTER TABLE library ADD COLUMN year INTEGER",
            "ALTER TABLE library ADD COLUMN tmdb_id INTEGER",
            "ALTER TABLE library ADD COLUMN media_type TEXT",
            "ALTER TABLE library ADD COLUMN sequential INTEGER NOT NULL DEFAULT 1",
        ] {
            let _ = conn.execute_batch(stmt);
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn insert_item(&self, magnet: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO library (magnet, state) VALUES (?1, 'connecting')",
            params![magnet],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn insert_movie(
        &self,
        title: &str,
        year: Option<i32>,
        poster_path: Option<&str>,
        tmdb_id: i64,
        media_type: &str,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO library (magnet, state, title, year, poster_path, tmdb_id, media_type)
             VALUES ('', 'ready', ?1, ?2, ?3, ?4, ?5)",
            params![title, year, poster_path, tmdb_id, media_type],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn get_item(&self, id: i64) -> Result<Option<LibraryItem>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, magnet, title, state, added_at, file_path, size_bytes, rqbit_id,
                    poster_path, year, tmdb_id, media_type, sequential
             FROM library WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(LibraryItem {
                id: row.get(0)?,
                magnet: row.get(1)?,
                title: row.get(2)?,
                state: row.get(3)?,
                added_at: row.get(4)?,
                file_path: row.get(5)?,
                size_bytes: row.get(6)?,
                rqbit_id: row.get(7)?,
                poster_path: row.get(8)?,
                year: row.get(9)?,
                tmdb_id: row.get(10)?,
                media_type: row.get(11)?,
                sequential: row.get(12)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn list_items(&self) -> Result<Vec<LibraryItem>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, magnet, title, state, added_at, file_path, size_bytes, rqbit_id,
                    poster_path, year, tmdb_id, media_type, sequential
             FROM library ORDER BY added_at DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(LibraryItem {
                id: row.get(0)?,
                magnet: row.get(1)?,
                title: row.get(2)?,
                state: row.get(3)?,
                added_at: row.get(4)?,
                file_path: row.get(5)?,
                size_bytes: row.get(6)?,
                rqbit_id: row.get(7)?,
                poster_path: row.get(8)?,
                year: row.get(9)?,
                tmdb_id: row.get(10)?,
                media_type: row.get(11)?,
                sequential: row.get(12)?,
            })
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn update_state(&self, id: i64, state: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET state = ?2 WHERE id = ?1",
            params![id, state],
        )?;
        Ok(())
    }

    pub fn update_title(&self, id: i64, title: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET title = ?2 WHERE id = ?1",
            params![id, title],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn update_rqbit_id(&self, id: i64, rqbit_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET rqbit_id = ?2 WHERE id = ?1",
            params![id, rqbit_id],
        )?;
        Ok(())
    }

    pub fn update_file_path(&self, id: i64, file_path: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET file_path = ?2 WHERE id = ?1",
            params![id, file_path],
        )?;
        Ok(())
    }

    pub fn update_size(&self, id: i64, size: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET size_bytes = ?2 WHERE id = ?1",
            params![id, size],
        )?;
        Ok(())
    }

    pub fn delete_item(&self, id: i64) -> Result<Option<LibraryItem>> {
        let item = self.get_item(id)?;
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM library WHERE id = ?1", params![id])?;
        Ok(item)
    }

    #[allow(dead_code)]
    pub fn find_by_rqbit_id(&self, rqbit_id: i64) -> Result<Option<LibraryItem>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, magnet, title, state, added_at, file_path, size_bytes, rqbit_id,
                    poster_path, year, tmdb_id, media_type, sequential
             FROM library WHERE rqbit_id = ?1",
        )?;
        let mut rows = stmt.query_map(params![rqbit_id], |row| {
            Ok(LibraryItem {
                id: row.get(0)?,
                magnet: row.get(1)?,
                title: row.get(2)?,
                state: row.get(3)?,
                added_at: row.get(4)?,
                file_path: row.get(5)?,
                size_bytes: row.get(6)?,
                rqbit_id: row.get(7)?,
                poster_path: row.get(8)?,
                year: row.get(9)?,
                tmdb_id: row.get(10)?,
                media_type: row.get(11)?,
                sequential: row.get(12)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn update_metadata(
        &self,
        id: i64,
        title: Option<&str>,
        year: Option<i32>,
        poster_path: Option<&str>,
        tmdb_id: Option<i64>,
        media_type: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET title = COALESCE(?2, title),
                    year = COALESCE(?3, year),
                    poster_path = COALESCE(?4, poster_path),
                    tmdb_id = COALESCE(?5, tmdb_id),
                    media_type = COALESCE(?6, media_type)
             WHERE id = ?1",
            params![id, title, year, poster_path, tmdb_id, media_type],
        )?;
        Ok(())
    }

    pub fn set_sequential(&self, id: i64, sequential: bool) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET sequential = ?2 WHERE id = ?1",
            params![id, sequential],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_db() -> (Database, std::path::PathBuf) {
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "chadtorrent_db_test_{}_{}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("test.db");
        let db = Database::new(db_path.to_str().unwrap()).unwrap();
        (db, dir)
    }

    fn cleanup(dir: &std::path::Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_insert_and_get() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc123").unwrap();
        assert!(id > 0);
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.magnet, "magnet:?xt=urn:btih:abc123");
        assert_eq!(item.state, "connecting");
        cleanup(&dir);
    }

    #[test]
    fn test_update_state() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc").unwrap();
        db.update_state(id, "downloading").unwrap();
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.state, "downloading");
        cleanup(&dir);
    }

    #[test]
    fn test_update_title() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc").unwrap();
        db.update_title(id, "My Torrent").unwrap();
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.title.unwrap(), "My Torrent");
        cleanup(&dir);
    }

    #[test]
    fn test_list_items() {
        let (db, dir) = temp_db();
        db.insert_item("magnet:?xt=urn:btih:abc1").unwrap();
        db.insert_item("magnet:?xt=urn:btih:abc2").unwrap();
        let items = db.list_items().unwrap();
        assert_eq!(items.len(), 2);
        cleanup(&dir);
    }

    #[test]
    fn test_delete_item() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc").unwrap();
        let deleted = db.delete_item(id).unwrap().unwrap();
        assert_eq!(deleted.id, id);
        assert!(db.get_item(id).unwrap().is_none());
        cleanup(&dir);
    }

    #[test]
    fn test_get_nonexistent() {
        let (db, dir) = temp_db();
        assert!(db.get_item(999).unwrap().is_none());
        cleanup(&dir);
    }

    #[test]
    fn test_update_rqbit_id() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc").unwrap();
        db.update_rqbit_id(id, 42).unwrap();
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.rqbit_id, Some(42));
        cleanup(&dir);
    }

    #[test]
    fn test_update_file_path() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc").unwrap();
        db.update_file_path(id, "/some/path/file.mp4").unwrap();
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.file_path, Some("/some/path/file.mp4".into()));
        cleanup(&dir);
    }

    #[test]
    fn test_find_by_rqbit_id() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:abc").unwrap();
        db.update_rqbit_id(id, 10).unwrap();
        let found = db.find_by_rqbit_id(10).unwrap().unwrap();
        assert_eq!(found.id, id);
        cleanup(&dir);
    }

    #[test]
    fn test_set_sequential() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:seq").unwrap();
        assert!(db.get_item(id).unwrap().unwrap().sequential);
        db.set_sequential(id, false).unwrap();
        assert!(!db.get_item(id).unwrap().unwrap().sequential);
        db.set_sequential(id, true).unwrap();
        assert!(db.get_item(id).unwrap().unwrap().sequential);
        cleanup(&dir);
    }

    #[test]
    fn test_migration_drops_keep_column() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:migration").unwrap();
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.state, "connecting");
        let conn = db.conn.lock().unwrap();
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('library')")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<String>, _>>()
            .unwrap();
        assert!(!cols.contains(&"keep".to_string()));
        cleanup(&dir);
    }
}
