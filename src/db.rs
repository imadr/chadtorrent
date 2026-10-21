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
    /// Synopsis. Torrent items leave it to the metadata cache; reading items
    /// (manga/comics) carry it from the search result they were added from.
    pub overview: Option<String>,
    /// The catalogue's native key when it isn't an integer (MangaDex UUID for
    /// manga rows); comic rows leave it empty (Comic Vine ids are integers).
    pub ext_key: Option<String>,
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
    /// Episode (torrent-relative file path) the party is watching
    /// (None for single-file items).
    pub file: Option<String>,
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
        overview: row.get(15)?,
        ext_key: row.get(16)?,
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
                torrent_bytes BLOB,
                overview TEXT,
                ext_key TEXT
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
                name TEXT NOT NULL,
                file TEXT
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
                episodes_json TEXT NOT NULL DEFAULT '[]',
                fetched_at INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (media_type, tmdb_id)
            );
            CREATE TABLE IF NOT EXISTS reader_source (
                media_type TEXT NOT NULL,
                tmdb_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                series_ref TEXT NOT NULL,
                series_title TEXT,
                PRIMARY KEY (media_type, tmdb_id)
            );
            CREATE TABLE IF NOT EXISTS chapter_progress (
                media_type TEXT NOT NULL,
                tmdb_id INTEGER NOT NULL,
                chapter TEXT NOT NULL,
                read INTEGER NOT NULL DEFAULT 0,
                page INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (media_type, tmdb_id, chapter)
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
            "ALTER TABLE tmdb_meta ADD COLUMN episodes_json TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE tmdb_meta ADD COLUMN fetched_at INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE party ADD COLUMN file TEXT",
            "ALTER TABLE library ADD COLUMN overview TEXT",
            "ALTER TABLE library ADD COLUMN ext_key TEXT",
        ] {
            let _ = conn.execute_batch(stmt);
        }
        conn.execute_batch("CREATE TABLE IF NOT EXISTS meta_migration (k TEXT PRIMARY KEY, v INTEGER NOT NULL)")?;
        // Release names can legitimately contain " / ". Retire the old suffix
        // cleanup rather than truncating saved titles every time the DB opens.
        // Recover old search selections from the original magnet display name
        // only when the saved title is missing or matches the internal torrent
        // name that the old registration code overwrote it with.
        let repaired: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM meta_migration WHERE k = 'release_titles')", [], |r| r.get(0),
        )?;
        if !repaired {
            let mut stmt = conn.prepare("SELECT id, magnet, title, torrent_bytes FROM library WHERE magnet != ''")?;
            let old = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?, r.get::<_, Option<Vec<u8>>>(3)?)))?;
            for row in old {
                let (id, magnet, title, bytes) = row?;
                let Some(release) = magnet_display_name(&magnet) else { continue };
                let internal = bytes.as_deref().and_then(|bytes| librqbit::torrent_from_bytes(bytes).ok())
                    .and_then(|torrent| torrent.info.data.validate().ok())
                    .and_then(|info| info.name().map(|name| name.into_owned()));
                if title.as_deref().is_none_or(|t| t.trim().is_empty() || t.starts_with("magnet:?"))
                    || (internal.is_some() && internal == title) {
                    conn.execute("UPDATE library SET title = ?2 WHERE id = ?1", params![id, release])?;
                }
            }
            conn.execute("INSERT INTO meta_migration (k, v) VALUES ('release_titles', 1)", [])?;
        }
        // One-time repair: manga/comic rows written before the reading sweep
        // existed could hold TMDB payloads (catalogue ids collide — TMDB id
        // 30002 is a Munsters special, a MangaDex id is Berserk). Drop
        // them so the next open re-sweeps from the right source.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta_migration (k TEXT PRIMARY KEY, v INTEGER NOT NULL)",
        )?;
        let reading_meta_repaired: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM meta_migration WHERE k = 'reading_meta_reset')",
            [],
            |r| r.get(0),
        )?;
        if !reading_meta_repaired {
            conn.execute("DELETE FROM tmdb_meta WHERE media_type IN ('manga', 'comic')", [])?;
            conn.execute(
                "INSERT INTO meta_migration (k, v) VALUES ('reading_meta_reset', 1)",
                [],
            )?;
        }
        // One-time repair: manga ids were 64-bit UUID hashes, which lose
        // precision in JavaScript (2^53) — the frontend requested metadata
        // for a *rounded* id and got nothing. Re-key every manga row that
        // carries its UUID (ext_key) with the 52-bit-safe hash, dragging the
        // group-scoped tables along. Rows without an ext_key keep their id:
        // there is nothing to recompute from.
        let manga_id_safe: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM meta_migration WHERE k = 'manga_id_52bit')",
            [],
            |r| r.get(0),
        )?;
        if !manga_id_safe {
            let pairs: Vec<(i64, String)> = {
                let mut stmt = conn.prepare(
                    "SELECT DISTINCT tmdb_id, ext_key FROM library
                     WHERE media_type = 'manga' AND ext_key IS NOT NULL AND tmdb_id IS NOT NULL",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?;
                rows.filter_map(|r| r.ok()).collect()
            };
            for (old, ext) in pairs {
                let new = crate::reading::mangadex_id_hash(&ext);
                if new == old {
                    continue;
                }
                conn.execute_batch(&format!(
                    "UPDATE OR IGNORE library SET tmdb_id = {new} WHERE media_type = 'manga' AND tmdb_id = {old};
                     UPDATE OR IGNORE tmdb_meta SET tmdb_id = {new} WHERE media_type = 'manga' AND tmdb_id = {old};
                     UPDATE OR IGNORE reader_source SET tmdb_id = {new} WHERE media_type = 'manga' AND tmdb_id = {old};
                     UPDATE OR IGNORE chapter_progress SET tmdb_id = {new} WHERE media_type = 'manga' AND tmdb_id = {old};"
                ))?;
            }
            conn.execute(
                "INSERT INTO meta_migration (k, v) VALUES ('manga_id_52bit', 1)",
                [],
            )?;
        }
        // One-time repair: the manga catalogue moved from MangaDex (which
        // delists licensed chapters) to Weeb Central. Manga meta rows hold
        // MangaDex payloads and reader_source rows point at the removed
        // MangaDex source — drop both; the next open re-sweeps from Weeb
        // Central and the sweep heals legacy rows onto their new ids.
        let wc_catalogue: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM meta_migration WHERE k = 'weebcentral_catalogue')",
            [],
            |r| r.get(0),
        )?;
        if !wc_catalogue {
            conn.execute_batch(
                "DELETE FROM tmdb_meta WHERE media_type = 'manga';
                 DELETE FROM reader_source WHERE source = 'mangadex';",
            )?;
            conn.execute(
                "INSERT INTO meta_migration (k, v) VALUES ('weebcentral_catalogue', 1)",
                [],
            )?;
        }
        // Repair: MangaDex cover file names already carry their extension;
        // an early importer appended a second ".jpg", producing dead URLs.
        let _ = conn.execute_batch(
            "UPDATE library
             SET poster_path = substr(poster_path, 1, length(poster_path) - 4)
             WHERE poster_path LIKE '%.jpg.jpg'",
        );
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    #[allow(dead_code)]
    pub fn insert_item(&self, magnet: &str) -> Result<i64> {
        self.insert_item_with_title(magnet, None)
    }

    pub fn insert_item_with_title(&self, magnet: &str, title: Option<&str>) -> Result<i64> {
        let title = title.filter(|t| !t.trim().is_empty()).map(str::to_owned)
            .or_else(|| magnet_display_name(magnet));
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO library (magnet, title, state) VALUES (?1, ?2, 'connecting')",
            params![magnet, title],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Insert a metadata-only placeholder row (movie placeholder, or a manga /
    /// comic entry — those never carry a torrent). `state` starts at `ready`
    /// and `magnet` stays empty.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_movie(
        &self,
        title: &str,
        year: Option<i32>,
        poster_path: Option<&str>,
        tmdb_id: i64,
        media_type: &str,
        overview: Option<&str>,
        ext_key: Option<&str>,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO library (magnet, state, title, year, poster_path, tmdb_id, media_type, overview, ext_key)
             VALUES ('', 'ready', ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![title, year, poster_path, tmdb_id, media_type, overview, ext_key],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// The reading catalogue's native key and title for a group, used by the
    /// chapter sweep: `(ext_key, title)`. `ext_key` is the MangaDex UUID for
    /// manga rows; `title` lets old rows (pre-ext_key) resolve by search.
    pub fn get_reading_source(
        &self,
        media_type: &str,
        tmdb_id: i64,
    ) -> Result<Option<(Option<String>, Option<String>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT ext_key, title FROM library
             WHERE media_type = ?1 AND tmdb_id = ?2
             ORDER BY (ext_key IS NULL), id LIMIT 1",
        )?;
        let mut rows = stmt.query_map(params![media_type, tmdb_id], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Store a refreshed cover for a whole reading group (manga rows share
    /// their catalogue id). Called by the chapter sweep when AniList yields
    /// art for the title, so pre-existing rows heal without a re-add.
    pub fn set_poster_for_group(
        &self,
        media_type: &str,
        tmdb_id: i64,
        poster: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE library SET poster_path = ?3
             WHERE media_type = ?1 AND tmdb_id = ?2",
            params![media_type, tmdb_id, poster],
        )?;
        Ok(changed)
    }

    pub fn get_item(&self, id: i64) -> Result<Option<LibraryItem>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, magnet, title, state, added_at, file_path, size_bytes, rqbit_id,
                    poster_path, year, tmdb_id, media_type, sequential, torrent_bytes, playhead, overview, ext_key
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
                    poster_path, year, tmdb_id, media_type, sequential, torrent_bytes, playhead, overview, ext_key
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

    #[allow(dead_code)]
    pub fn update_title(&self, id: i64, title: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET title = ?2 WHERE id = ?1",
            params![id, title],
        )?;
        Ok(())
    }

    /// Torrent metadata is only a fallback for raw magnets without a name.
    /// The SQL predicate also protects against a concurrent title assignment.
    pub fn set_torrent_title_if_missing(&self, id: i64, title: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE library SET title = ?2 WHERE id = ?1 AND (title IS NULL OR trim(title) = '')",
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
                    poster_path, year, tmdb_id, media_type, sequential, torrent_bytes, playhead, overview, ext_key
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
                                year, poster_path, item_id, name, file)
             VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(id) DO UPDATE SET
                relay = excluded.relay, room = excluded.room, room_key = excluded.room_key,
                magnet = excluded.magnet, tmdb_id = excluded.tmdb_id, media_type = excluded.media_type,
                title = excluded.title, year = excluded.year, poster_path = excluded.poster_path,
                item_id = excluded.item_id, name = excluded.name, file = excluded.file",
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
                p.name,
                p.file
            ],
        )?;
        Ok(())
    }

    pub fn get_party(&self) -> Result<Option<PartyRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT relay, room, room_key, magnet, tmdb_id, media_type, title, year,
                    poster_path, item_id, name, file
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
                file: row.get(11)?,
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

    /// The cached TMDB payload for one `media_type` + `tmdb_id` pair: overview
    /// (description) paragraph, the canonical TV-season list and every season's
    /// episode list — so opening a show paints its season/episode buttons from
    /// SQLite instead of round-tripping TMDB per season click.
    pub fn get_tmdb_meta(
        &self,
        media_type: &str,
        tmdb_id: i64,
    ) -> Result<Option<crate::metadata::TmdbMeta>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT overview, seasons_json, episodes_json, fetched_at
             FROM tmdb_meta WHERE media_type = ?1 AND tmdb_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![media_type, tmdb_id], |row| {
            let overview: Option<String> = row.get(0)?;
            let seasons_json: String = row.get(1)?;
            let episodes_json: String = row.get(2)?;
            let fetched_at: i64 = row.get(3)?;
            Ok(crate::metadata::TmdbMeta {
                overview,
                seasons: serde_json::from_str(&seasons_json).unwrap_or_default(),
                episodes: serde_json::from_str(&episodes_json).unwrap_or_default(),
                fetched_at,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    /// Upsert a TMDB payload. `fetched_at` (unix secs) is the time of the last
    /// *complete* read — callers pass the previous value when part of the sweep
    /// failed, which keeps the row eligible for an immediate retry.
    pub fn set_tmdb_meta(
        &self,
        media_type: &str,
        tmdb_id: i64,
        overview: Option<&str>,
        seasons: &[crate::metadata::SeasonInfo],
        episodes: &[crate::metadata::SeasonEpisodes],
        fetched_at: i64,
    ) -> Result<()> {
        let seasons_json = serde_json::to_string(seasons)
            .map_err(|e| crate::error::Error::BadRequest(format!("serialize seasons: {e}")))?;
        let episodes_json = serde_json::to_string(episodes)
            .map_err(|e| crate::error::Error::BadRequest(format!("serialize episodes: {e}")))?;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO tmdb_meta (media_type, tmdb_id, overview, seasons_json, episodes_json, fetched_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                 overview = excluded.overview,
                 seasons_json = excluded.seasons_json,
                 episodes_json = excluded.episodes_json,
                 fetched_at = excluded.fetched_at",
            params![media_type, tmdb_id, overview, seasons_json, episodes_json, fetched_at],
        )?;
        Ok(())
    }

    // ---- reading: source binding & chapter progress ----

    /// The reading source a manga/comic group is bound to (where chapter
    /// pages come from). One binding per group, chosen once — never per
    /// chapter.
    pub fn get_reader_source(&self, media_type: &str, tmdb_id: i64) -> Result<Option<ReaderSourceRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT source, series_ref, series_title
             FROM reader_source WHERE media_type = ?1 AND tmdb_id = ?2",
        )?;
        let mut rows = stmt.query_map(params![media_type, tmdb_id], |row| {
            Ok(ReaderSourceRow {
                source: row.get(0)?,
                series_ref: row.get(1)?,
                series_title: row.get(2)?,
            })
        })?;
        match rows.next() {
            Some(row) => Ok(Some(row?)),
            None => Ok(None),
        }
    }

    pub fn set_reader_source(
        &self,
        media_type: &str,
        tmdb_id: i64,
        source: &str,
        series_ref: &str,
        series_title: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO reader_source (media_type, tmdb_id, source, series_ref, series_title)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(media_type, tmdb_id) DO UPDATE SET
                 source = excluded.source,
                 series_ref = excluded.series_ref,
                 series_title = excluded.series_title",
            params![media_type, tmdb_id, source, series_ref, series_title],
        )?;
        Ok(())
    }

    /// Every chapter progress row of a group (read flags + last page for the
    /// reader's scroll restore), plus the most recently touched chapter.
    pub fn list_chapter_progress(&self, media_type: &str, tmdb_id: i64) -> Result<Vec<ChapterProgressRow>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT chapter, read, page, updated_at FROM chapter_progress
             WHERE media_type = ?1 AND tmdb_id = ?2",
        )?;
        let rows = stmt.query_map(params![media_type, tmdb_id], |row| {
            Ok(ChapterProgressRow {
                chapter: row.get(0)?,
                read: row.get::<_, i64>(1)? != 0,
                page: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Upsert one chapter's progress. `read` is sticky: a chapter that was
    /// finished stays finished unless explicitly cleared.
    pub fn set_chapter_progress(
        &self,
        media_type: &str,
        tmdb_id: i64,
        chapter: &str,
        read: Option<bool>,
        page: Option<i64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO chapter_progress (media_type, tmdb_id, chapter, read, page, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(media_type, tmdb_id, chapter) DO UPDATE SET
                 read = MAX(read, excluded.read),
                 page = excluded.page,
                 updated_at = excluded.updated_at",
            params![
                media_type,
                tmdb_id,
                chapter,
                read.unwrap_or(false) as i64,
                page.unwrap_or(0),
                now_secs()
            ],
        )?;
        Ok(())
    }

    /// Drop the reading state of a group (group deleted).
    pub fn clear_reading_state(&self, media_type: &str, tmdb_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM reader_source WHERE media_type = ?1 AND tmdb_id = ?2",
            params![media_type, tmdb_id],
        )?;
        conn.execute(
            "DELETE FROM chapter_progress WHERE media_type = ?1 AND tmdb_id = ?2",
            params![media_type, tmdb_id],
        )?;
        Ok(())
    }

    /// Heal a legacy reading group onto its Weeb Central series id: re-key
    /// the library rows (ext_key + derived numeric id) and drag the
    /// group-scoped meta/source/progress rows along. Used by the manga
    /// sweep when a row predates the Weeb Central catalogue.
    pub fn rekey_reading_group(&self, media_type: &str, old_tmdb_id: i64, new_ext: &str) -> Result<()> {
        let new_id = crate::reading::ext_id_hash(new_ext);
        if new_id == old_tmdb_id {
            return Ok(());
        }
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE OR IGNORE library SET tmdb_id = ?3, ext_key = ?4
             WHERE media_type = ?1 AND tmdb_id = ?2",
            params![media_type, old_tmdb_id, new_id, new_ext],
        )?;
        for table in ["tmdb_meta", "reader_source", "chapter_progress"] {
            conn.execute(
                &format!(
                    "UPDATE OR IGNORE {table} SET tmdb_id = ?3
                     WHERE media_type = ?1 AND tmdb_id = ?2"
                ),
                params![media_type, old_tmdb_id, new_id],
            )?;
        }
        Ok(())
    }
}

fn magnet_display_name(magnet: &str) -> Option<String> {
    reqwest::Url::parse(magnet).ok()?.query_pairs()
        .find(|(key, value)| key == "dn" && !value.trim().is_empty())
        .map(|(_, value)| value.into_owned())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A group's bound reading source.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReaderSourceRow {
    pub source: String,
    pub series_ref: String,
    pub series_title: Option<String>,
}

/// One chapter's saved progress.
#[derive(Debug, Clone)]
pub struct ChapterProgressRow {
    pub chapter: String,
    pub read: bool,
    pub page: i64,
    pub updated_at: i64,
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
    fn selected_release_title_is_atomic_and_survives_metadata_and_restart() {
        let (db, dir) = temp_db();
        let release = "[Judas] JoJo's Bizarre Adventure / Dual-Audio [1080p][HEVC]";
        let id = db.insert_item_with_title("magnet:?xt=urn:btih:abc&dn=short-file.mkv", Some(release)).unwrap();
        let item = db.get_item(id).unwrap().unwrap();
        assert_eq!(item.state, "connecting");
        assert_eq!(item.title.as_deref(), Some(release));
        for state in ["downloading", "paused", "ready"] {
            db.set_torrent_title_if_missing(id, "short-file.mkv").unwrap();
            db.update_state(id, state).unwrap();
            assert_eq!(db.get_item(id).unwrap().unwrap().title.as_deref(), Some(release));
        }
        drop(db);
        let reopened = Database::new(dir.join("test.db").to_str().unwrap()).unwrap();
        assert_eq!(reopened.get_item(id).unwrap().unwrap().title.as_deref(), Some(release));
        drop(reopened);
        cleanup(&dir);
    }

    #[test]
    fn old_internal_names_are_recovered_without_renaming_custom_titles() {
        let (db, dir) = temp_db();
        let release = "[Judas] JoJo's Bizarre Adventure [1080p][HEVC]";
        let mut magnet = reqwest::Url::parse("magnet:?xt=urn:btih:abc").unwrap();
        magnet.query_pairs_mut().append_pair("dn", release);
        let file = "episode.mkv";
        let bytes = format!("d4:infod6:lengthi1e4:name{}:{}12:piece lengthi16384e6:pieces20:01234567890123456789ee", file.len(), file).into_bytes();
        let old = db.insert_item_with_title(magnet.as_str(), Some(file)).unwrap();
        db.update_torrent_bytes(old, &bytes).unwrap();
        let custom = db.insert_item_with_title(magnet.as_str(), Some("My chosen title / HEVC")).unwrap();
        db.update_torrent_bytes(custom, &bytes).unwrap();
        db.conn.lock().unwrap().execute("DELETE FROM meta_migration WHERE k = 'release_titles'", []).unwrap();
        drop(db);
        let db = Database::new(dir.join("test.db").to_str().unwrap()).unwrap();
        assert_eq!(db.get_item(old).unwrap().unwrap().title.as_deref(), Some(release));
        assert_eq!(db.get_item(custom).unwrap().unwrap().title.as_deref(), Some("My chosen title / HEVC"));
        drop(db);
        cleanup(&dir);
    }

    #[test]
    fn test_insert_movie_overview_roundtrip() {
        let (db, dir) = temp_db();
        let id = db
            .insert_movie(
                "Berserk",
                Some(1989),
                Some("https://x/y.jpg"),
                123,
                "manga",
                Some("Guts."),
                Some("801513ba-a712-498c-8f57-cae55b38cc92"),
            )
            .unwrap();
        let it = db.get_item(id).unwrap().unwrap();
        assert_eq!(it.title.as_deref(), Some("Berserk"));
        assert_eq!(it.media_type.as_deref(), Some("manga"));
        assert_eq!(it.overview.as_deref(), Some("Guts."));
        assert_eq!(it.poster_path.as_deref(), Some("https://x/y.jpg"));
        assert_eq!(
            it.ext_key.as_deref(),
            Some("801513ba-a712-498c-8f57-cae55b38cc92")
        );
        assert!(it.magnet.is_empty());
        assert_eq!(it.state, "ready");
        // Survives a reopen (the column is created by the migration too).
        drop(db);
        let db2 = Database::new(dir.join("test.db").to_str().unwrap()).unwrap();
        assert_eq!(
            db2.get_item(id).unwrap().unwrap().overview.as_deref(),
            Some("Guts.")
        );
        cleanup(&dir);
    }

    /// The chapter sweep's source lookup: prefers rows carrying an ext_key,
    /// falls back to the title for legacy rows.
    #[test]
    fn test_get_reading_source() {
        let (db, dir) = temp_db();
        let a = db
            .insert_movie("Berserk", Some(1989), None, 30002, "manga", None, None)
            .unwrap();
        let b = db
            .insert_movie("Berserk", None, None, 30002, "manga", None,
                          Some("801513ba-a712-498c-8f57-cae55b38cc92"))
            .unwrap();
        // ext_key row wins over the legacy one, even though it's newer.
        let (ext, title) = db.get_reading_source("manga", 30002).unwrap().unwrap();
        assert_eq!(ext.as_deref(), Some("801513ba-a712-498c-8f57-cae55b38cc92"));
        assert_eq!(title.as_deref(), Some("Berserk"));
        assert_eq!(db.get_item(b).unwrap().unwrap().id, b);
        assert_eq!(db.get_item(a).unwrap().unwrap().ext_key, None);
        // Unknown catalogue entry.
        assert!(db.get_reading_source("manga", 999).unwrap().is_none());
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
            file: Some("Show/S01E01.mkv".into()),
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
        assert_eq!(back.file.as_deref(), Some("Show/S01E01.mkv"));
        // Upsert path
        let mut row2 = row.clone();
        row2.name = "bob".into();
        row2.file = None;
        db.set_party(&row2).unwrap();
        let back2 = db.get_party().unwrap().unwrap();
        assert_eq!(back2.name, "bob");
        assert_eq!(back2.file, None);
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
        use crate::metadata::{EpisodeInfo, SeasonEpisodes};
        let (db, dir) = temp_db();
        assert!(db.get_tmdb_meta("movie", 46648).unwrap().is_none());
        let seasons = vec![crate::metadata::SeasonInfo {
            season_number: 1,
            name: Some("Season 1".into()),
            episode_count: Some(2),
            air_date: Some("2014-01-12".into()),
        }];
        let episodes = vec![SeasonEpisodes {
            season_number: 1,
            episodes: (1..=2)
                .map(|n| EpisodeInfo {
                    episode_number: n,
                    name: Some(format!("Ep {n}")),
                    overview: None,
                    still_path: None,
                    air_date: None,
                })
                .collect(),
        }];
        db.set_tmdb_meta(
            "tv",
            46648,
            Some("A cool show."),
            &seasons,
            &episodes,
            1000,
        )
        .unwrap();
        let m = db.get_tmdb_meta("tv", 46648).unwrap().unwrap();
        assert_eq!(m.overview.as_deref(), Some("A cool show."));
        assert_eq!(m.fetched_at, 1000);
        assert_eq!(m.seasons.len(), 1);
        assert_eq!(m.seasons[0].season_number, 1);
        assert_eq!(m.seasons[0].name.as_deref(), Some("Season 1"));
        let by_season = m.episodes_by_season();
        assert_eq!(by_season.len(), 1);
        let eps = &by_season[&1];
        assert_eq!(eps.len(), 2);
        assert_eq!(eps[1].name.as_deref(), Some("Ep 2"));

        // Upsert replaces the stored payload.
        db.set_tmdb_meta("tv", 46648, Some("New desc."), &[], &[], 2000)
            .unwrap();
        let m = db.get_tmdb_meta("tv", 46648).unwrap().unwrap();
        assert_eq!(m.overview.as_deref(), Some("New desc."));
        assert_eq!(m.fetched_at, 2000);
        assert!(m.seasons.is_empty());
        assert!(m.episodes.is_empty());

        // A different media_type with the same id is isolated.
        assert!(db.get_tmdb_meta("movie", 46648).unwrap().is_none());
        cleanup(&dir);
    }

    /// One-time repair: manga/comic meta rows from before the reading sweep
    /// could hold TMDB payloads (catalogue ids collide). The first open must
    /// drop them; later opens must keep freshly-swept rows.
    #[test]
    fn test_reading_meta_rows_repaired_once() {
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "chadtorrent_db_test_{}_{}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        // A database from before the repair: poisoned reading rows, a legit
        // TMDB row, no marker table.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE tmdb_meta (
                    media_type TEXT NOT NULL,
                    tmdb_id INTEGER NOT NULL,
                    overview TEXT,
                    seasons_json TEXT NOT NULL DEFAULT '[]',
                    episodes_json TEXT NOT NULL DEFAULT '[]',
                    fetched_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (media_type, tmdb_id)
                );
                INSERT INTO tmdb_meta (media_type, tmdb_id, overview, fetched_at) VALUES
                    ('manga', 30002, 'Munsters junk', 1000),
                    ('comic', 50270, 'Also suspect', 1000),
                    ('movie', 680, 'Legit TMDB overview', 1000);",
            )
            .unwrap();
        }

        // First open: reading rows dropped, TMDB row survives, marker set.
        let db = Database::new(path.to_str().unwrap()).unwrap();
        assert!(db.get_tmdb_meta("manga", 30002).unwrap().is_none());
        assert!(db.get_tmdb_meta("comic", 50270).unwrap().is_none());
        assert!(db.get_tmdb_meta("movie", 680).unwrap().is_some());

        // Later opens must not keep deleting: a freshly swept reading row
        // survives a restart.
        db.set_tmdb_meta("manga", 30002, Some("Guts."), &[], &[], 2000)
            .unwrap();
        drop(db);
        let db2 = Database::new(path.to_str().unwrap()).unwrap();
        let m = db2.get_tmdb_meta("manga", 30002).unwrap().unwrap();
        assert_eq!(m.overview.as_deref(), Some("Guts."));
        cleanup(&dir);
    }

    /// One-time repair: manga ids were 64-bit UUID hashes, which JavaScript
    /// rounds above 2^53. The first open must re-key rows (and their
    /// group-scoped meta/source/progress rows) to the 52-bit hash; later
    /// opens must not touch anything.
    #[test]
    fn test_manga_ids_migrated_to_52bit_once() {
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "chadtorrent_db_test_{}_{}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        {
            // Pre-repair database: a manga row carrying the old 64-bit hash
            // plus meta/source/progress rows under the same id.
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE library (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    magnet TEXT NOT NULL, title TEXT, state TEXT,
                    added_at DATETIME, file_path TEXT, size_bytes INTEGER,
                    rqbit_id INTEGER, poster_path TEXT, year INTEGER,
                    tmdb_id INTEGER, media_type TEXT, sequential INTEGER,
                    torrent_bytes BLOB, playhead REAL, overview TEXT, ext_key TEXT);
                 CREATE TABLE tmdb_meta (
                    media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL, overview TEXT,
                    seasons_json TEXT NOT NULL DEFAULT '[]',
                    episodes_json TEXT NOT NULL DEFAULT '[]',
                    fetched_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (media_type, tmdb_id));
                 CREATE TABLE reader_source (
                    media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL,
                    source TEXT NOT NULL, series_ref TEXT NOT NULL, series_title TEXT,
                    PRIMARY KEY (media_type, tmdb_id));
                 CREATE TABLE chapter_progress (
                    media_type TEXT NOT NULL, tmdb_id INTEGER NOT NULL, chapter TEXT NOT NULL,
                    read INTEGER NOT NULL DEFAULT 0, page INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (media_type, tmdb_id, chapter));
                 CREATE TABLE meta_migration (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
                 CREATE TABLE party (id INTEGER, relay TEXT, room TEXT, room_key TEXT, magnet TEXT,
                    tmdb_id INTEGER, media_type TEXT, title TEXT, year INTEGER, poster_path TEXT,
                    item_id INTEGER, name TEXT, file TEXT);
                 INSERT INTO meta_migration (k, v) VALUES ('reading_meta_reset', 1);
                 INSERT INTO library (magnet, state, media_type, tmdb_id, ext_key) VALUES
                    ('', 'ready', 'manga', -9217439369956013684, '801513ba-a712-498c-8f57-cae55b38cc92');
                 INSERT INTO tmdb_meta (media_type, tmdb_id, overview) VALUES ('manga', -9217439369956013684, 'Guts.');
                 INSERT INTO reader_source (media_type, tmdb_id, source, series_ref) VALUES
                    ('manga', -9217439369956013684, 'mangadex', '801513ba-a712-498c-8f57-cae55b38cc92');
                 INSERT INTO chapter_progress (media_type, tmdb_id, chapter, read, page, updated_at) VALUES
                    ('manga', -9217439369956013684, '3', 1, 17, 100);",
            )
            .unwrap();
        }
        let want = crate::reading::mangadex_id_hash("801513ba-a712-498c-8f57-cae55b38cc92");
        assert_ne!(want, -9217439369956013684i64, "fixture uses the old hash");
        let db = Database::new(path.to_str().unwrap()).unwrap();
        let got: i64 = db
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT tmdb_id FROM library WHERE media_type = 'manga'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(got, want);
        // The Weeb Central catalogue swap (same first open) drops MangaDex-era
        // meta and source bindings; the sweep re-creates them from Weeb
        // Central and heals the row onto its new id. Progress survives.
        assert!(db.get_tmdb_meta("manga", want).unwrap().is_none());
        assert!(db.get_reader_source("manga", want).unwrap().is_none());
        assert_eq!(
            db.list_chapter_progress("manga", want).unwrap().len(),
            1
        );
        // Idempotent: a second open doesn't move anything.
        drop(db);
        let db2 = Database::new(path.to_str().unwrap()).unwrap();
        let got2: i64 = db2
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT tmdb_id FROM library WHERE media_type = 'manga'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(got2, want);
        cleanup(&dir);
    }

    #[test]
    fn test_tmdb_meta_episodes_migrate() {
        use crate::metadata::{EpisodeInfo, SeasonEpisodes};
        let n = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "chadtorrent_db_test_{}_{}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.db");
        // Simulate a pre-episodes database: a tmdb_meta row with only an
        // overview and season list.
        {
            let conn = Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE tmdb_meta (
                    media_type TEXT NOT NULL,
                    tmdb_id INTEGER NOT NULL,
                    overview TEXT,
                    seasons_json TEXT NOT NULL DEFAULT '[]',
                    PRIMARY KEY (media_type, tmdb_id)
                );
                INSERT INTO tmdb_meta (media_type, tmdb_id, overview, seasons_json)
                VALUES ('tv', 1, 'Old show.', '[]');",
            )
            .unwrap();
        }
        // Opening it runs the migrations: the new columns appear and the
        // existing row survives.
        let db = Database::new(path.to_str().unwrap()).unwrap();
        let episodes = vec![SeasonEpisodes {
            season_number: 2,
            episodes: vec![EpisodeInfo {
                episode_number: 1,
                name: None,
                overview: None,
                still_path: None,
                air_date: None,
            }],
        }];
        db.set_tmdb_meta("tv", 1, Some("Old show."), &[], &episodes, 5)
            .unwrap();
        let m = db.get_tmdb_meta("tv", 1).unwrap().unwrap();
        assert_eq!(m.overview.as_deref(), Some("Old show."));
        assert_eq!(m.fetched_at, 5);
        assert_eq!(m.episodes_by_season()[&2].len(), 1);
        cleanup(&dir);
    }

    /// Reading source binding: one per group, upsert replaces, groups are
    /// isolated.
    #[test]
    fn test_reader_source_roundtrip() {
        let (db, dir) = temp_db();
        assert!(db.get_reader_source("manga", 30002).unwrap().is_none());
        db.set_reader_source("manga", 30002, "weebcentral", "01J76XY7EF75DJNQCV04HTPDZK", Some("Berserk"))
            .unwrap();
        let b = db.get_reader_source("manga", 30002).unwrap().unwrap();
        assert_eq!(b.source, "weebcentral");
        assert_eq!(b.series_ref, "01J76XY7EF75DJNQCV04HTPDZK");
        assert_eq!(b.series_title.as_deref(), Some("Berserk"));
        // rebind to another source overwrites
        db.set_reader_source("manga", 30002, "mangadex", "801513ba-a712-498c-8f57-cae55b38cc92", None)
            .unwrap();
        let b = db.get_reader_source("manga", 30002).unwrap().unwrap();
        assert_eq!(b.source, "mangadex");
        assert_eq!(b.series_title, None);
        // other group / media type isolated
        assert!(db.get_reader_source("comic", 30002).unwrap().is_none());
        assert!(db.get_reader_source("manga", 1).unwrap().is_none());
        // survives a reopen (migration creates the table)
        drop(db);
        let db2 = Database::new(dir.join("test.db").to_str().unwrap()).unwrap();
        assert!(db2.get_reader_source("manga", 30002).unwrap().is_some());
        cleanup(&dir);
    }

    /// Chapter progress: upsert, sticky read flag, per-chapter isolation,
    /// group cleanup.
    #[test]
    fn test_chapter_progress_roundtrip() {
        let (db, dir) = temp_db();
        assert!(db.list_chapter_progress("manga", 7).unwrap().is_empty());
        db.set_chapter_progress("manga", 7, "386", Some(true), Some(37)).unwrap();
        db.set_chapter_progress("manga", 7, "385", None, Some(4)).unwrap();
        let rows = db.list_chapter_progress("manga", 7).unwrap();
        assert_eq!(rows.len(), 2);
        let r386 = rows.iter().find(|r| r.chapter == "386").unwrap();
        assert!(r386.read);
        assert_eq!(r386.page, 37);
        let r385 = rows.iter().find(|r| r.chapter == "385").unwrap();
        assert!(!r385.read);
        assert_eq!(r385.page, 4);
        // page upserts; read stays sticky across a later page-only write
        db.set_chapter_progress("manga", 7, "386", None, Some(12)).unwrap();
        let r386 = db.list_chapter_progress("manga", 7)
            .unwrap()
            .into_iter()
            .find(|r| r.chapter == "386")
            .unwrap();
        assert!(r386.read);
        assert_eq!(r386.page, 12);
        // group isolation + clear
        assert!(db.list_chapter_progress("comic", 7).unwrap().is_empty());
        db.clear_reading_state("manga", 7).unwrap();
        assert!(db.list_chapter_progress("manga", 7).unwrap().is_empty());
        assert!(db.get_reader_source("manga", 7).unwrap().is_none());
        cleanup(&dir);
    }
}
