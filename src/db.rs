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
    /// Saved playback position in seconds (per torrent file, not per movie).
    pub playhead: f64,
    #[serde(skip)]
    pub torrent_bytes: Option<Vec<u8>>,
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

/// The persisted active watch party (at most one per instance).
#[derive(Debug, Clone)]
pub struct PartyRow {
    pub relay: String,
    pub room: String,
    pub room_key: String,
    pub magnet: String,
    pub tmdb_id: Option<i64>,
    pub media_type: Option<String>,
    pub title: Option<String>,
    pub year: Option<i32>,
    pub poster_path: Option<String>,
    pub item_id: i64,
    pub name: String,
}

fn item_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LibraryItem> {
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
        playhead: row.get(14)?,
        torrent_bytes: row.get(13)?,
    })
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
                sequential INTEGER NOT NULL DEFAULT 1,
                torrent_bytes BLOB
            );
            CREATE TABLE IF NOT EXISTS pieces (
                info_hash TEXT PRIMARY KEY,
                haves BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS party (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                relay TEXT NOT NULL,
                room TEXT NOT NULL,
                room_key TEXT NOT NULL,
                magnet TEXT NOT NULL,
                tmdb_id INTEGER,
                media_type TEXT,
                title TEXT,
                year INTEGER,
                poster_path TEXT,
                item_id INTEGER NOT NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS skipped_files (
                item_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                PRIMARY KEY (item_id, file_path)
            );
            CREATE INDEX IF NOT EXISTS idx_skipped_files_item ON skipped_files(item_id);
            CREATE TABLE IF NOT EXISTS playheads (
                item_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                t REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (item_id, file_path)
            );
            CREATE INDEX IF NOT EXISTS idx_playheads_item ON playheads(item_id);
            CREATE TABLE IF NOT EXISTS tmdb_meta (
                media_type TEXT NOT NULL,
                tmdb_id INTEGER NOT NULL,
                overview TEXT,
                seasons_json TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY (media_type, tmdb_id)
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
            "ALTER TABLE library ADD COLUMN torrent_bytes BLOB",
            "ALTER TABLE library ADD COLUMN playhead REAL NOT NULL DEFAULT 0",
        ] {
            let _ = conn.execute_batch(stmt);
        }
        // Migrate: titles used to carry a " / <largest video file>" suffix
        // appended by an older register path. The file name is now surfaced
        // per-file instead, so strip the appended segment.
        let _ = conn.execute_batch(
            "UPDATE library SET title = trim(substr(title, 1, instr(title, ' / ') - 1))
             WHERE title LIKE '% / %'",
        )?;
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
                    poster_path, year, tmdb_id, media_type, sequential, torrent_bytes, playhead
             FROM library WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], item_from_row)?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn list_items(&self) -> Result<Vec<LibraryItem>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, magnet, title, state, added_at, file_path, size_bytes, rqbit_id,
                    poster_path, year, tmdb_id, media_type, sequential, torrent_bytes, playhead
             FROM library ORDER BY added_at DESC",
        )?;
        let rows = stmt.query_map([], item_from_row)?;
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
                    poster_path, year, tmdb_id, media_type, sequential, torrent_bytes, playhead
             FROM library WHERE rqbit_id = ?1",
        )?;
        let mut rows = stmt.query_map(params![rqbit_id], item_from_row)?;
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

    /// Save the playback position (seconds) for a torrent item.
    pub fn set_playhead(&self, id: i64, t: f64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET playhead = ?2 WHERE id = ?1",
            params![id, if t.is_finite() { t.max(0.0) } else { 0.0 }],
        )?;
        Ok(())
    }

    /// Persist the resolved .torrent file bytes so a torrent can be re-added
    /// without re-resolving its magnet over the network.
    pub fn update_torrent_bytes(&self, id: i64, torrent_bytes: &[u8]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET torrent_bytes = ?2 WHERE id = ?1",
            params![id, torrent_bytes],
        )?;
        Ok(())
    }

    /// Save the downloaded-pieces bitfield for an info hash.
    pub fn save_pieces(&self, info_hash: &str, haves: &[u8]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO pieces (info_hash, haves) VALUES (?1, ?2)
             ON CONFLICT(info_hash) DO UPDATE SET haves = excluded.haves",
            params![info_hash, haves],
        )?;
        Ok(())
    }

    /// Load the saved downloaded-pieces bitfield for an info hash.
    pub fn get_pieces(&self, info_hash: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT haves FROM pieces WHERE info_hash = ?1")?;
        let mut rows = stmt.query_map(params![info_hash], |row| row.get(0))?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Drop the saved bitfield for an info hash (torrent removed).
    pub fn delete_pieces(&self, info_hash: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM pieces WHERE info_hash = ?1", params![info_hash])?;
        Ok(())
    }

    // ---- watch party (single active party per instance) ----

    /// Persist the active watch party so it is rejoined after a restart.
    pub fn set_party(&self, p: &PartyRow) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO party (id, relay, room, room_key, magnet, tmdb_id, media_type, title,
                                year, poster_path, item_id, name)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
                relay = excluded.relay, room = excluded.room, room_key = excluded.room_key,
                magnet = excluded.magnet, tmdb_id = excluded.tmdb_id, media_type = excluded.media_type,
                title = excluded.title, year = excluded.year, poster_path = excluded.poster_path,
                item_id = excluded.item_id, name = excluded.name",
            params![
                p.relay,
                p.room,
                p.room_key,
                p.magnet,
                p.tmdb_id,
                p.media_type,
                p.title,
                p.year,
                p.poster_path,
                p.item_id,
                p.name
            ],
        )?;
        Ok(())
    }

    pub fn get_party(&self) -> Result<Option<PartyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT relay, room, room_key, magnet, tmdb_id, media_type, title, year,
                    poster_path, item_id, name
             FROM party WHERE id = 1",
        )?;
        let mut rows = stmt.query_map([], |row| {
            Ok(PartyRow {
                relay: row.get(0)?,
                room: row.get(1)?,
                room_key: row.get(2)?,
                magnet: row.get(3)?,
                tmdb_id: row.get(4)?,
                media_type: row.get(5)?,
                title: row.get(6)?,
                year: row.get(7)?,
                poster_path: row.get(8)?,
                item_id: row.get(9)?,
                name: row.get(10)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn clear_party(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM party WHERE id = 1", [])?;
        Ok(())
    }

    // ---- per-torrent-file selection & playhead ----

    /// Mark a torrent-relative file as skipped (not downloaded) or selected.
    pub fn set_file_skipped(&self, item_id: i64, file_path: &str, skipped: bool) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        if skipped {
            conn.execute(
                "INSERT OR IGNORE INTO skipped_files (item_id, file_path) VALUES (?1, ?2)",
                params![item_id, file_path],
            )?;
        } else {
            conn.execute(
                "DELETE FROM skipped_files WHERE item_id = ?1 AND file_path = ?2",
                params![item_id, file_path],
            )?;
        }
        Ok(())
    }

    /// All skipped (not-downloaded) file paths for an item.
    pub fn list_skipped_files(&self, item_id: i64) -> Result<std::collections::HashSet<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT file_path FROM skipped_files WHERE item_id = ?1",
        )?;
        let rows = stmt.query_map(params![item_id], |row| row.get(0))?;
        let mut out = std::collections::HashSet::new();
        for row in rows {
            out.insert(row?);
        }
        Ok(out)
    }

    /// Save the playback position (seconds) for one file of a torrent.
    pub fn set_file_playhead(&self, item_id: i64, file_path: &str, t: f64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO playheads (item_id, file_path, t) VALUES (?1, ?2, ?3)
             ON CONFLICT(item_id, file_path) DO UPDATE SET t = excluded.t",
            params![item_id, file_path, if t.is_finite() { t.max(0.0) } else { 0.0 }],
        )?;
        Ok(())
    }

    /// All saved per-file playheads for an item, as (file_path, seconds).
    pub fn list_playheads(&self, item_id: i64) -> Result<Vec<(String, f64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT file_path, t FROM playheads WHERE item_id = ?1")?;
        let rows = stmt.query_map(params![item_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Delete all per-file selection/playhead rows for an item (item deleted).
    pub fn clear_item_files(&self, item_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM skipped_files WHERE item_id = ?1", params![item_id])?;
        conn.execute("DELETE FROM playheads WHERE item_id = ?1", params![item_id])?;
        Ok(())
    }

    // ---- TMDB metadata cache ----

    /// Cached TMDB detail — the overview (description) paragraph plus the
    /// canonical TV-season list — so the sidebar serves from SQLite instead of
    /// re-hitting TMDB on every page refresh.
    pub fn get_tmdb_meta(
        &self,
        media_type: &str,
        tmdb_id: i64,
    ) -> Result<Option<(Option<String>, Vec<crate::metadata::SeasonInfo>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT overview, seasons_json FROM tmdb_meta WHERE media_type = ?1 AND tmdb_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![media_type, tmdb_id], |row| {
            let overview: Option<String> = row.get(0)?;
            let seasons_json: String = row.get(1)?;
            let seasons: Vec<crate::metadata::SeasonInfo> =
                serde_json::from_str(&seasons_json).unwrap_or_default();
            Ok((overview, seasons))
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn set_tmdb_meta(
        &self,
        media_type: &str,
        tmdb_id: i64,
        overview: Option<&str>,
        seasons: &[crate::metadata::SeasonInfo],
    ) -> Result<()> {
        let seasons_json = serde_json::to_string(seasons)
            .map_err(|e| crate::error::Error::BadRequest(format!("serialize seasons: {e}")))?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO tmdb_meta (media_type, tmdb_id, overview, seasons_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                 overview = excluded.overview,
                 seasons_json = excluded.seasons_json",
            params![media_type, tmdb_id, overview, seasons_json],
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
    fn test_pieces_roundtrip() {
        let (db, dir) = temp_db();
        let hash = "abcd1234";
        assert!(db.get_pieces(hash).unwrap().is_none());
        db.save_pieces(hash, &[0x01, 0x02, 0x03]).unwrap();
        assert_eq!(db.get_pieces(hash).unwrap().unwrap(), vec![0x01, 0x02, 0x03]);
        db.save_pieces(hash, &[0x04]).unwrap();
        assert_eq!(db.get_pieces(hash).unwrap().unwrap(), vec![0x04]);
        db.delete_pieces(hash).unwrap();
        assert!(db.get_pieces(hash).unwrap().is_none());
        cleanup(&dir);
    }

    #[test]
    fn test_playhead_roundtrip() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:ph").unwrap();
        assert_eq!(db.get_item(id).unwrap().unwrap().playhead, 0.0);
        db.set_playhead(id, 1234.5).unwrap();
        assert_eq!(db.get_item(id).unwrap().unwrap().playhead, 1234.5);
        db.set_playhead(id, -3.0).unwrap();
        assert_eq!(db.get_item(id).unwrap().unwrap().playhead, 0.0);
        db.set_playhead(id, 42.0).unwrap();
        drop(db); // close the connection
        // Re-open the same file: the column must exist via the migration and
        // keep its value.
        let db2 = Database::new(dir.join("test.db").to_str().unwrap()).unwrap();
        assert_eq!(db2.get_item(id).unwrap().unwrap().playhead, 42.0);
        cleanup(&dir);
    }

    #[test]
    fn test_update_torrent_bytes() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:tb").unwrap();
        assert!(db.get_item(id).unwrap().unwrap().torrent_bytes.is_none());
        db.update_torrent_bytes(id, &[9, 8, 7]).unwrap();
        assert_eq!(
            db.get_item(id).unwrap().unwrap().torrent_bytes.unwrap(),
            vec![9, 8, 7]
        );
        cleanup(&dir);
    }

    #[test]
    fn test_skipped_files_roundtrip() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:skip").unwrap();
        assert!(db.list_skipped_files(id).unwrap().is_empty());
        db.set_file_skipped(id, "Show.S01E02.mkv", true).unwrap();
        db.set_file_skipped(id, "Show.S01E03.mkv", true).unwrap();
        let set = db.list_skipped_files(id).unwrap();
        assert_eq!(set.len(), 2);
        assert!(set.contains("Show.S01E02.mkv"));
        assert!(set.contains("Show.S01E03.mkv"));
        db.set_file_skipped(id, "Show.S01E02.mkv", false).unwrap();
        let set = db.list_skipped_files(id).unwrap();
        assert_eq!(set.len(), 1);
        assert!(!set.contains("Show.S01E02.mkv"));
        // other items are isolated
        let id2 = db.insert_item("magnet:?xt=urn:btih:other").unwrap();
        assert!(db.list_skipped_files(id2).unwrap().is_empty());
        cleanup(&dir);
    }

    #[test]
    fn test_file_playhead_roundtrip() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:ph2").unwrap();
        assert!(db.list_playheads(id).unwrap().is_empty());
        db.set_file_playhead(id, "a.mkv", 12.5).unwrap();
        db.set_file_playhead(id, "b.mkv", -3.0).unwrap();
        let ph = db.list_playheads(id).unwrap();
        assert!(ph.contains(&("a.mkv".to_string(), 12.5)));
        assert!(ph.contains(&("b.mkv".to_string(), 0.0)));
        // upsert
        db.set_file_playhead(id, "a.mkv", 99.0).unwrap();
        let ph = db.list_playheads(id).unwrap();
        assert!(ph.contains(&("a.mkv".to_string(), 99.0)));
        assert_eq!(ph.len(), 2);
        cleanup(&dir);
    }

    #[test]
    fn test_clear_item_files() {
        let (db, dir) = temp_db();
        let id = db.insert_item("magnet:?xt=urn:btih:clear").unwrap();
        db.set_file_skipped(id, "x.mkv", true).unwrap();
        db.set_file_playhead(id, "x.mkv", 5.0).unwrap();
        db.clear_item_files(id).unwrap();
        assert!(db.list_skipped_files(id).unwrap().is_empty());
        assert!(db.list_playheads(id).unwrap().is_empty());
        cleanup(&dir);
    }

    #[test]
    fn test_party_row_roundtrip() {
        let (db, dir) = temp_db();
        assert!(db.get_party().unwrap().is_none());
        let row = PartyRow {
            relay: "ws://192.168.1.4:3000".into(),
            room: "cm9vbQ".into(),
            room_key: "a2V5".into(),
            magnet: "magnet:?xt=urn:btih:abc".into(),
            tmdb_id: Some(7),
            media_type: Some("movie".into()),
            title: Some("Title".into()),
            year: Some(2001),
            poster_path: Some("/p.jpg".into()),
            item_id: 3,
            name: "alice".into(),
        };
        db.set_party(&row).unwrap();
        let back = db.get_party().unwrap().unwrap();
        assert_eq!(back.relay, row.relay);
        assert_eq!(back.room, row.room);
        assert_eq!(back.room_key, row.room_key);
        assert_eq!(back.magnet, row.magnet);
        assert_eq!(back.tmdb_id, Some(7));
        assert_eq!(back.item_id, 3);
        assert_eq!(back.name, "alice");
        // Upsert path
        let mut row2 = row.clone();
        row2.name = "bob".into();
        db.set_party(&row2).unwrap();
        assert_eq!(db.get_party().unwrap().unwrap().name, "bob");
        db.clear_party().unwrap();
        assert!(db.get_party().unwrap().is_none());
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

    #[test]
    fn test_tmdb_meta_roundtrip() {
        let (db, dir) = temp_db();
        assert!(db.get_tmdb_meta("movie", 46648).unwrap().is_none());
        let seasons = vec![crate::metadata::SeasonInfo {
            season_number: 1,
            name: Some("Season 1".into()),
            episode_count: Some(8),
            air_date: Some("2014-01-12".into()),
        }];
        db.set_tmdb_meta("tv", 46648, Some("A cool show."), &seasons)
            .unwrap();
        let (overview, got) = db.get_tmdb_meta("tv", 46648).unwrap().unwrap();
        assert_eq!(overview.as_deref(), Some("A cool show."));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].season_number, 1);
        assert_eq!(got[0].name.as_deref(), Some("Season 1"));
        // Upsert replaces the stored payload.
        db.set_tmdb_meta("tv", 46648, Some("New desc."), &[]).unwrap();
        let (overview, got) = db.get_tmdb_meta("tv", 46648).unwrap().unwrap();
        assert_eq!(overview.as_deref(), Some("New desc."));
        assert!(got.is_empty());
        // A different media_type with the same id is isolated.
        assert!(db.get_tmdb_meta("movie", 46648).unwrap().is_none());
        cleanup(&dir);
    }
}
