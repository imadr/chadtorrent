//! Reading (manga & comics) metadata search + chapter lists.
//!
//! One source per catalogue, plus one deliberate helper:
//!   - **MangaDex** (manga): search, synopsis and the real per-chapter list
//!     (numbers + titles). Keyless. Its cover uploads are NOT used for
//!     display — hotlinked browsers get a placeholder logo — so covers come
//!     from **AniList**, title-matched (keyless). Misses fall back to the
//!     MangaDex cover URL.
//!   - **Comic Vine** (western comics): volume search + issue lists. Needs a
//!     free API key in `chadtorrent.json` (`comicvine_api_key`).
//!
//! Both map onto the library's movie-placeholder row shape (`ReadingResult`):
//! `poster_path` carries a full absolute image URL (unlike TMDB's path
//! fragments), `media_type` is `"manga"` / `"comic"`, and the id becomes the
//! row's `tmdb_id` — the grouping key is `media_type:id`, so catalogues never
//! collide. MangaDex ids are UUIDs, so rows store the UUID in `ext_key` and a
//! stable 64-bit hash of it as the numeric `tmdb_id`.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

use crate::error::{Error, Result};

const MANGADEX_API: &str = "https://api.mangadex.org";
const MANGADEX_COVERS: &str = "https://uploads.mangadex.org/covers";
const ANILIST_URL: &str = "https://graphql.anilist.co";
/// Text search lives on `/search/` (with `resources=volume`); the `/volumes/`
/// browse endpoint silently ignores `query` and returns the whole catalogue.
const COMICVINE_URL: &str = "https://comicvine.gamespot.com/api/search/";

/// How to get a Comic Vine key, surfaced verbatim by the comics search
/// endpoint when none is configured.
pub const COMICVINE_HELP: &str = "Comics search needs a free Comic Vine API key: \
register at https://comicvine.gamespot.com/api/, generate a key, and put it in \
chadtorrent.json as \"comicvine_api_key\" (then restart).";

/// One search hit, shaped like the TMDB `SearchResult` so the frontend's add
/// flow treats every catalogue identically.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReadingResult {
    pub id: i64,
    pub title: String,
    pub year: Option<i32>,
    /// Absolute image URL (TMDB stores path fragments; reading catalogues
    /// hand out full URLs — the frontend passes both through).
    pub poster_path: Option<String>,
    pub media_type: String,
    pub overview: Option<String>,
    /// The catalogue's native key when it isn't an integer (MangaDex UUID),
    /// stored on the library row so the chapter sweep can reach the API.
    pub ext_key: Option<String>,
}

pub struct ReadingClient {
    http: Client,
    comicvine_api_key: Option<String>,
}

/// Chapter/issue list of one reading entry, ready for the `tmdb_meta` cache.
/// Manga chapters come from the MangaDex feed (real titles); comic issues
/// from Comic Vine with names and cover dates.
#[derive(Debug, Clone)]
pub struct ReadingDetail {
    pub overview: Option<String>,
    pub episodes: Vec<crate::metadata::EpisodeInfo>,
    /// AniList cover (manga) — the sweep stores it on the library row so
    /// existing entries heal without a re-add.
    pub poster: Option<String>,
}

/// One chapter/issue row in the detail page's list.
pub fn chapter_info(
    number: i64,
    name: Option<String>,
    air_date: Option<String>,
    still_path: Option<String>,
) -> crate::metadata::EpisodeInfo {
    crate::metadata::EpisodeInfo {
        episode_number: number,
        name,
        overview: None,
        still_path,
        air_date,
    }
}

/// MangaDex ids are UUIDs; the library groups rows by a numeric id, so the
/// UUID is folded to a stable 64-bit value (first 16 hex digits).
pub fn mangadex_id_hash(uuid: &str) -> i64 {
    let hex: String = uuid
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(16)
        .collect();
    u64::from_str_radix(&hex, 16)
        .map(|v| v as i64)
        .unwrap_or(0)
}

/// One feed row → display row. Integer chapters keep their number and title;
/// sub-chapters ("1.5", "0.01" specials) get a "Ch. <raw> · <title>" label
/// so nothing is lost or silently renumbered.
pub fn chapter_rows(
    chapters: Vec<(Option<String>, Option<String>)>,
) -> Vec<crate::metadata::EpisodeInfo> {
    let mut parsed: Vec<(f64, String, Option<String>)> = chapters
        .into_iter()
        .filter_map(|(number, title)| {
            let raw = number?;
            let value = raw.trim().parse::<f64>().ok()?;
            Some((value, raw, title))
        })
        .collect();
    parsed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    parsed
        .into_iter()
        .map(|(value, raw, title)| {
            let whole = value.trunc();
            let is_integer = (value - whole).abs() < f64::EPSILON && value >= 0.0;
            let name = if is_integer {
                title
            } else {
                Some(match title {
                    Some(t) => format!("Ch. {raw} · {t}"),
                    None => format!("Ch. {raw}"),
                })
            };
            chapter_info(whole.max(0.0) as i64, name, None, None)
        })
        .collect()
}

/// ComicVine issue numbers are strings: "3", "2.5" (→ 2), "Annual 1" (no
/// leading digits → skipped — those aren't chapters).
pub fn issue_number(s: &str) -> Option<i64> {
    let digits: String = s
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    digits.split('.').next()?.parse::<i64>().ok()
}

/// Title key for cross-catalogue cover matching: lowercase alphanumerics
/// only ("Berserk: The Black Swordsman" == "berserk the black swordsman").
fn norm_title(s: &str) -> String {
    s.to_lowercase().chars().filter(|c| c.is_alphanumeric()).collect()
}

// ---- AniList cover types (covers only — nothing else comes from AniList) ----

#[derive(Debug, Deserialize)]
struct AniListCoverResp {
    #[serde(default)]
    data: Option<AniListCoverData>,
}

#[derive(Debug, Deserialize)]
struct AniListCoverData {
    #[serde(default, rename = "Page")]
    page: Option<AniListCoverPage>,
}

#[derive(Debug, Deserialize)]
struct AniListCoverPage {
    #[serde(default)]
    media: Vec<AniListCoverMedia>,
}

#[derive(Debug, Deserialize)]
struct AniListCoverMedia {
    #[serde(default)]
    title: AniListCoverTitle,
    #[serde(rename = "coverImage", default)]
    cover_image: Option<AniListCoverImage>,
}

#[derive(Debug, Deserialize, Default)]
struct AniListCoverTitle {
    #[serde(default)]
    romaji: Option<String>,
    #[serde(default)]
    english: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AniListCoverImage {
    #[serde(rename = "extraLarge", default)]
    extra_large: Option<String>,
    #[serde(default)]
    large: Option<String>,
}

/// "2005" / "2005-00-00" / "2005ish" → `Some(2005)`; anything else `None`.
fn year_prefix(s: &str) -> Option<i32> {
    let digits: String = s
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits
        .parse::<i32>()
        .ok()
        .filter(|y| (1800..=2100).contains(y))
}

impl ReadingClient {
    pub fn new(comicvine_api_key: Option<String>) -> Self {
        Self {
            http: Client::builder()
                .timeout(Duration::from_secs(10))
                // Comic Vine (and other Cloudflare-fronted APIs) block
                // requests without a User-Agent outright, serving an HTML
                // "scraper blocked" page that fails JSON decoding.
                .user_agent("chadtorrent/0.1")
                .build()
                .expect("reqwest client"),
            comicvine_api_key: comicvine_api_key.filter(|k| !k.trim().is_empty()),
        }
    }

    pub fn comics_configured(&self) -> bool {
        self.comicvine_api_key.is_some()
    }

    /// Cover images from AniList for a search query: normalized title →
    /// extra-large cover URL. MangaDex uploads render as a placeholder logo
    /// in browsers, so AniList art always wins where a title matches; misses
    /// keep the MangaDex URL. Best effort — a failed call just leaves the
    /// map empty.
    async fn anilist_cover_map(&self, query: &str) -> HashMap<String, String> {
        let body = serde_json::json!({
            "query": "query ($s: String) { \
                Page(page: 1, perPage: 24) { \
                    media(type: MANGA, search: $s, sort: SEARCH_MATCH, isAdult: false) { \
                        title { romaji english } \
                        coverImage { extraLarge large } \
                    } \
                } \
            }",
            "variables": { "s": query },
        });
        let Ok(resp) = self.http.post(ANILIST_URL).json(&body).send().await else {
            return HashMap::new();
        };
        let Ok(parsed) = resp.json::<AniListCoverResp>().await else {
            return HashMap::new();
        };
        let media = parsed
            .data
            .and_then(|d| d.page)
            .map(|p| p.media)
            .unwrap_or_default();
        let mut map = HashMap::new();
        for m in media {
            let title = m.title.english.or(m.title.romaji);
            let url = m
                .cover_image
                .and_then(|c| c.extra_large.or(c.large));
            if let (Some(t), Some(u)) = (title, url) {
                map.insert(norm_title(&t), u);
            }
        }
        map
    }

    /// The AniList cover for one title: exact normalized match, else the
    /// search's first hit (AniList ranks the same query the same way).
    pub async fn anilist_cover_for(&self, title: &str) -> Option<String> {
        let map = self.anilist_cover_map(title).await;
        if map.is_empty() {
            return None;
        }
        map.get(&norm_title(title)).cloned().or_else(|| map.values().next().cloned())
    }

    /// Manga search via MangaDex (keyless). Covers ride along via the
    /// `cover_art` include; english title/description when available.
    pub async fn search_manga(&self, query: &str) -> Result<Vec<ReadingResult>> {
        let resp = self
            .http
            .get(format!("{MANGADEX_API}/manga"))
            .query(&[
                ("title", query),
                ("limit", "24"),
                ("includes[]", "cover_art"),
                ("contentRating[]", "safe"),
                ("contentRating[]", "suggestive"),
                ("contentRating[]", "erotica"),
            ])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("mangadex search failed: {e}")))?
            .json::<MdCollection<MdManga>>()
            .await
            .map_err(|e| Error::BadRequest(format!("mangadex response invalid: {e}")))?;
        // AniList art wins for every hit whose title matches; the rest keep
        // their MangaDex cover URL as fallback.
        let covers = self.anilist_cover_map(query).await;
        Ok(resp
            .data
            .into_iter()
            .map(|m| {
                let cover = m
                    .relationships
                    .iter()
                    .find(|r| r.r#type == "cover_art")
                    .and_then(|r| r.attributes.as_ref())
                    .and_then(|c| c.file_name.clone());
                // fileName already carries its extension ("uuid.jpg").
                let md_poster = cover.map(|f| format!("{MANGADEX_COVERS}/{}/{f}", m.id));
                let title = m.attributes.pick_title();
                let poster = covers
                    .get(&norm_title(&title))
                    .cloned()
                    .or(md_poster);
                ReadingResult {
                    id: mangadex_id_hash(&m.id),
                    title,
                    year: m.attributes.year,
                    poster_path: poster,
                    media_type: "manga".into(),
                    overview: m.attributes.description.get("en").map(|d| strip_html(d)),
                    ext_key: Some(m.id),
                }
            })
            .collect())
    }

    /// Manga detail: fresh description + the real chapter feed, plus the
    /// AniList cover for the title (MangaDex uploads show a placeholder logo
    /// when hotlinked by browsers, so they are never displayed).
    pub async fn manga_detail(&self, uuid: &str) -> Result<ReadingDetail> {
        let manga: MdSingle<MdManga> = self
            .http
            .get(format!("{MANGADEX_API}/manga/{uuid}"))
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("mangadex detail failed: {e}")))?
            .json()
            .await
            .map_err(|e| Error::BadRequest(format!("mangadex response invalid: {e}")))?;
        let title = manga.data.attributes.pick_title();
        let overview = manga
            .data
            .attributes
            .description
            .get("en")
            .map(|d| strip_html(d));
        let episodes = self.mangadex_chapters(uuid).await.unwrap_or_default();
        let poster = self.anilist_cover_for(&title).await;
        Ok(ReadingDetail {
            overview,
            episodes,
            poster,
        })
    }

    /// For rows added before MangaDex became the manga source (no `ext_key`):
    /// resolve the title to a MangaDex entry, then fetch its details.
    pub async fn manga_detail_by_title(&self, title: &str) -> Result<ReadingDetail> {
        let hits = self.search_manga(title).await?;
        let Some(hit) = hits
            .iter()
            .find(|h| h.title.to_lowercase() == title.to_lowercase())
            .or_else(|| hits.first())
        else {
            return Ok(ReadingDetail {
                overview: None,
                episodes: Vec::new(),
                poster: None,
            });
        };
        let Some(ext) = hit.ext_key.as_deref() else {
            return Ok(ReadingDetail {
                overview: None,
                episodes: Vec::new(),
                poster: None,
            });
        };
        self.manga_detail(ext).await
    }

    /// The chapter feed, english first, paginated (500/page). Falls back to
    /// any language when the english feed is empty.
    async fn mangadex_chapters(&self, uuid: &str) -> Result<Vec<crate::metadata::EpisodeInfo>> {
        let mut rows = self.mangadex_feed(uuid, true).await?;
        if rows.is_empty() {
            rows = self.mangadex_feed(uuid, false).await?;
        }
        Ok(rows)
    }

    async fn mangadex_feed(
        &self,
        uuid: &str,
        english_only: bool,
    ) -> Result<Vec<crate::metadata::EpisodeInfo>> {
        let mut chapters: Vec<(Option<String>, Option<String>)> = Vec::new();
        let mut total = i64::MAX; // refined after the first page
        for offset in (0..2500i64).step_by(500) {
            if offset >= total {
                break;
            }
            let mut req = self
                .http
                .get(format!("{MANGADEX_API}/manga/{uuid}/feed"))
                .query(&[
                    ("limit", "500"),
                    ("offset", offset.to_string().as_str()),
                    ("order[chapter]", "asc"),
                    ("contentRating[]", "safe"),
                    ("contentRating[]", "suggestive"),
                    ("contentRating[]", "erotica"),
                    ("includeEmptyPages", "0"),
                ]);
            if english_only {
                req = req.query(&[("translatedLanguage[]", "en")]);
            }
            let resp: MdCollection<MdChapter> = req
                .send()
                .await
                .map_err(|e| Error::BadRequest(format!("mangadex feed failed: {e}")))?
                .json()
                .await
                .map_err(|e| Error::BadRequest(format!("mangadex response invalid: {e}")))?;
            total = resp.total.unwrap_or(0);
            let got = resp.data.len() as i64;
            chapters.extend(
                resp.data
                    .into_iter()
                    .map(|c| (c.attributes.chapter, c.attributes.title)),
            );
            if got < 500 {
                break;
            }
        }
        Ok(chapter_rows(chapters))
    }

    /// Comics (volume) search via Comic Vine. Errors with setup instructions
    /// when no key is configured.
    pub async fn search_comics(&self, query: &str) -> Result<Vec<ReadingResult>> {
        let Some(key) = self.comicvine_api_key.as_ref() else {
            return Err(Error::BadRequest(COMICVINE_HELP.to_string()));
        };
        let resp = self
            .http
            .get(COMICVINE_URL)
            .query(&[
                ("api_key", key.as_str()),
                ("format", "json"),
                ("limit", "24"),
                ("query", query),
                ("resources", "volume"),
            ])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("comicvine search failed: {e}")))?
            .json::<ComicVineResp>()
            .await
            .map_err(|e| Error::BadRequest(format!("comicvine response invalid: {e}")))?;
        let err = resp.error.unwrap_or_default();
        if !err.is_empty() && err != "OK" {
            // Comic Vine reports bad keys / throttling through this field
            // with HTTP 200.
            let hint = if err.to_lowercase().contains("invalid") {
                format!("{err} — check comicvine_api_key in chadtorrent.json")
            } else {
                err
            };
            return Err(Error::BadRequest(hint));
        }
        Ok(resp
            .results
            .into_iter()
            .map(|v| {
                let title = v.name.unwrap_or_else(|| format!("Comic {}", v.id));
                let poster = v.image.and_then(|i| i.medium_url.or(i.small_url));
                ReadingResult {
                    id: v.id,
                    title,
                    year: v.start_year.as_deref().and_then(year_prefix),
                    poster_path: poster,
                    media_type: "comic".into(),
                    overview: v.description.or(v.deck).map(|d| strip_html(&d)),
                    ext_key: None,
                }
            })
            .collect())
    }

    /// Comic detail: the volume's issue list, paginated (100/page max).
    /// `sort` is lexicographic upstream ("1", "10", "2"…), so ordering is
    /// fixed client-side after parsing the numbers.
    pub async fn comic_detail(&self, id: i64) -> Result<ReadingDetail> {
        let Some(key) = self.comicvine_api_key.as_ref() else {
            return Err(Error::BadRequest(COMICVINE_HELP.to_string()));
        };
        let mut issues: Vec<ComicVineIssue> = Vec::new();
        let mut total = i64::MAX; // refined after the first page
        for offset in (0..2000).step_by(100) {
            if offset >= total {
                break;
            }
            let resp = self
                .http
                .get("https://comicvine.gamespot.com/api/issues/")
                .query(&[
                    ("api_key", key.as_str()),
                    ("format", "json"),
                    ("limit", "100"),
                    ("offset", offset.to_string().as_str()),
                    ("filter", format!("volume:{id}").as_str()),
                    ("field_list", "id,issue_number,name,cover_date,image"),
                ])
                .send()
                .await
                .map_err(|e| Error::BadRequest(format!("comicvine issues failed: {e}")))?
                .json::<ComicVineIssuesResp>()
                .await
                .map_err(|e| Error::BadRequest(format!("comicvine response invalid: {e}")))?;
            let err = resp.error.unwrap_or_default();
            if !err.is_empty() && err != "OK" {
                return Err(Error::BadRequest(err));
            }
            total = resp.number_of_total_results.unwrap_or(0);
            let got = resp.results.len() as i64;
            issues.extend(resp.results);
            if got < 100 {
                break;
            }
        }
        let mut episodes: Vec<crate::metadata::EpisodeInfo> = issues
            .into_iter()
            .filter_map(|i| {
                let number = issue_number(i.issue_number.as_deref()?)?;
                Some(chapter_info(
                    number,
                    i.name,
                    i.cover_date,
                    i.image.and_then(|img| img.medium_url.or(img.small_url)),
                ))
            })
            .collect();
        episodes.sort_by_key(|e| e.episode_number);
        Ok(ReadingDetail {
            overview: None, // the volume description is already on the row
            episodes,
            poster: None,
        })
    }
}

// ---- MangaDex response types ----

#[derive(Debug, Deserialize)]
struct MdCollection<T> {
    data: Vec<T>,
    #[serde(default)]
    total: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MdSingle<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct MdManga {
    id: String,
    #[serde(default)]
    attributes: MdMangaAttrs,
    #[serde(default)]
    relationships: Vec<MdRelationship>,
}

#[derive(Debug, Deserialize, Default)]
struct MdMangaAttrs {
    #[serde(default)]
    title: HashMap<String, String>,
    #[serde(rename = "altTitles", default)]
    alt_titles: Vec<HashMap<String, String>>,
    #[serde(default)]
    description: HashMap<String, String>,
    #[serde(default)]
    year: Option<i32>,
}

impl MdMangaAttrs {
    /// Display title: english map entry, else english alt title, else the
    /// first title/alt the entry carries (romaji, native…).
    fn pick_title(&self) -> String {
        let from_map = |m: &HashMap<String, String>| {
            m.get("en")
                .cloned()
                .or_else(|| m.values().next().cloned())
        };
        self.title
            .get("en")
            .cloned()
            .or_else(|| self.alt_titles.iter().find_map(|t| t.get("en").cloned()))
            .or_else(|| from_map(&self.title))
            .or_else(|| self.alt_titles.iter().find_map(from_map))
            .unwrap_or_else(|| "Manga".into())
    }
}

#[derive(Debug, Deserialize)]
struct MdRelationship {
    #[serde(rename = "type", default)]
    r#type: String,
    #[serde(default)]
    attributes: Option<MdCoverAttrs>,
}

#[derive(Debug, Deserialize)]
struct MdCoverAttrs {
    #[serde(rename = "fileName", default)]
    file_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MdChapter {
    #[serde(default)]
    attributes: MdChapterAttrs,
}

#[derive(Debug, Deserialize, Default)]
struct MdChapterAttrs {
    #[serde(default)]
    chapter: Option<String>,
    #[serde(default)]
    title: Option<String>,
}

// ---- Comic Vine response types ----

#[derive(Debug, Deserialize)]
struct ComicVineResp {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    results: Vec<ComicVineVolume>,
}

#[derive(Debug, Deserialize)]
struct ComicVineVolume {
    id: i64,
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "start_year", default)]
    start_year: Option<String>,
    #[serde(default)]
    image: Option<ComicVineImage>,
    #[serde(default)]
    description: Option<String>,
    /// Short summary — the /search/ endpoint often carries only this.
    #[serde(default)]
    deck: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComicVineImage {
    #[serde(rename = "medium_url", default)]
    medium_url: Option<String>,
    #[serde(rename = "small_url", default)]
    small_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ComicVineIssuesResp {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    results: Vec<ComicVineIssue>,
    #[serde(rename = "number_of_total_results", default)]
    number_of_total_results: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ComicVineIssue {
    #[serde(rename = "issue_number", default)]
    issue_number: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(rename = "cover_date", default)]
    cover_date: Option<String>,
    #[serde(default)]
    image: Option<ComicVineImage>,
}

/// HTML tag stripper + the entities both catalogues actually emit
/// (descriptions arrive as small HTML documents: `<p>`, `<br>`, `<i>…`).
/// `<br>` becomes a newline; every other tag is dropped; runs of blank lines
/// collapse to one.
pub fn strip_html(input: &str) -> String {
    let mut s = strip_html_utf8(input);
    for (from, to) in [
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", "\""),
        ("&#39;", "'"),
        ("&#039;", "'"),
        ("&rsquo;", "\u{2019}"),
        ("&lsquo;", "\u{2018}"),
        ("&ldquo;", "\u{201c}"),
        ("&rdquo;", "\u{201d}"),
        ("&ndash;", "\u{2013}"),
        ("&mdash;", "\u{2014}"),
        ("&hellip;", "\u{2026}"),
        ("&nbsp;", " "),
    ] {
        s = s.replace(from, to);
    }
    // Collapse 3+ newlines (tag soup around <br> and </p>) into a blank line.
    while s.contains("\n\n\n") {
        s = s.replace("\n\n\n", "\n\n");
    }
    s.trim().to_string()
}

/// Tag-stripping pass: copies only non-tag spans from the original `&str`, so
/// multi-byte characters survive intact. `<br…>` becomes a newline.
fn strip_html_utf8(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(lt) = rest.find('<') {
        out.push_str(&rest[..lt]);
        match rest[lt..].find('>') {
            Some(gt) => {
                let tag = &rest[lt + 1..lt + gt];
                let name = tag
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if name.starts_with("br") {
                    out.push('\n');
                }
                rest = &rest[lt + gt + 1..];
            }
            None => {
                // Unterminated tag: keep the text, drop the marker.
                rest = &rest[lt + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_html_basic() {
        assert_eq!(strip_html("<p>Guts, a lone <b>mercenary</b>.</p>"), "Guts, a lone mercenary.");
    }

    #[test]
    fn test_strip_html_br_is_newline() {
        assert_eq!(
            strip_html("line one<br>line two<br/>line three"),
            "line one\nline two\nline three"
        );
    }

    #[test]
    fn test_strip_html_entities() {
        assert_eq!(strip_html("A &amp; B &lt;3 &quot;X&quot;"), "A & B <3 \"X\"");
        assert_eq!(strip_html("it&#039;s &hellip;fine&nbsp;"), "it's \u{2026}fine");
    }

    #[test]
    fn test_strip_html_utf8_survives() {
        assert_eq!(strip_html("キャラ<b>クター</b> — naïve"), "キャラクター — naïve");
        assert_eq!(strip_html(" Attack on Titan"), "Attack on Titan");
    }

    #[test]
    fn test_strip_html_unterminated_tag() {
        // Unterminated tag: the marker is dropped, the text stays.
        assert_eq!(strip_html("text <oops"), "text oops");
    }

    #[test]
    fn test_year_prefix() {
        assert_eq!(year_prefix("2005"), Some(2005));
        assert_eq!(year_prefix(" 1989-01-01 "), Some(1989));
        assert_eq!(year_prefix("Ongoing"), None);
        assert_eq!(year_prefix(""), None);
    }

    #[test]
    fn test_issue_number_parsing() {
        assert_eq!(issue_number("1"), Some(1));
        assert_eq!(issue_number("2.5"), Some(2));
        assert_eq!(issue_number(" 372 "), Some(372));
        assert_eq!(issue_number("Annual 1"), None);
        assert_eq!(issue_number(""), None);
    }

    /// UUID → stable numeric id: deterministic and collision-free enough for
    /// a personal library (the UUID's first 16 hex digits, as i64 — sign bit
    /// included, the value is opaque).
    #[test]
    fn test_mangadex_id_hash() {
        let a = mangadex_id_hash("801513ba-a712-498c-8f57-cae55b38cc92");
        let b = mangadex_id_hash("801513ba-a712-498c-8f57-cae55b38cc92");
        let c = mangadex_id_hash("dc332d04-d3b0-413c-a767-70f5e451b031");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    /// MangaDex search payload: en title falls back through altTitles, cover
    /// art rides in the relationships.
    #[test]
    fn test_mangadex_search_payload_parses() {
        let payload = r#"{
            "data": [
                {
                    "id": "801513ba-a712-498c-8f57-cae55b38cc92",
                    "type": "manga",
                    "attributes": {
                        "title": { "ja-ro": "Berserk" },
                        "altTitles": [{ "ja": "ベルセルク" }, { "en": "Berserk" }],
                        "description": { "en": "Guts, known as the <b>Black Swordsman</b>." },
                        "year": 1989
                    },
                    "relationships": [
                        { "id": "rel1", "type": "author" },
                        { "id": "rel2", "type": "cover_art",
                          "attributes": { "fileName": "81e1c82d-6672-400c-8c58-4ff9bfb89031.jpg" } }
                    ]
                }
            ],
            "total": 22
        }"#;
        let parsed: MdCollection<MdManga> = serde_json::from_str(payload).unwrap();
        assert_eq!(parsed.total, Some(22));
        let m = &parsed.data[0];
        assert_eq!(m.attributes.pick_title(), "Berserk");
        assert_eq!(m.attributes.year, Some(1989));
        assert_eq!(
            m.attributes.description.get("en").map(|d| strip_html(d)),
            Some("Guts, known as the Black Swordsman.".into())
        );
        let cover = m
            .relationships
            .iter()
            .find(|r| r.r#type == "cover_art")
            .and_then(|r| r.attributes.as_ref())
            .and_then(|c| c.file_name.clone());
        assert_eq!(
            cover.as_deref(),
            Some("81e1c82d-6672-400c-8c58-4ff9bfb89031.jpg")
        );
    }

    /// Chapter feed mapping: integers keep number+title; specials like
    /// "0.01" get a labelled row; ordering follows the numeric value.
    #[test]
    fn test_chapter_rows() {
        let rows = chapter_rows(vec![
            (Some("1".into()), Some("Roast Goat".into())),
            (Some("0.01".into()), Some("The Black Swordsman".into())),
            (Some("2".into()), None),
            (Some("1.5".into()), None),
            (None, Some("external only".into())),
        ]);
        // external-only chapter is dropped; the rest sort numerically.
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0].episode_number, 0);
        assert_eq!(rows[0].name.as_deref(), Some("Ch. 0.01 · The Black Swordsman"));
        assert_eq!(rows[1].episode_number, 1);
        assert_eq!(rows[1].name.as_deref(), Some("Roast Goat"));
        assert_eq!(rows[2].episode_number, 1); // 1.5 truncates, label keeps truth
        assert_eq!(rows[2].name.as_deref(), Some("Ch. 1.5"));
        assert_eq!(rows[3].episode_number, 2);
        assert_eq!(rows[3].name, None);
    }

    /// AniList cover payload (covers only): english title preferred, the
    /// extraLarge art wins.
    #[test]
    fn test_anilist_cover_payload_parses() {
        let payload = r#"{
            "data": { "Page": { "media": [
                { "title": { "romaji": "Berserk", "english": "Berserk" },
                  "coverImage": { "extraLarge": "https://s4.anilist.co/x.jpg", "large": "https://s4.anilist.co/s.jpg" } },
                { "title": { "romaji": "Romaji Only" }, "coverImage": null }
            ] } }
        }"#;
        let parsed: AniListCoverResp = serde_json::from_str(payload).unwrap();
        let media = parsed.data.unwrap().page.unwrap().media;
        assert_eq!(media.len(), 2);
        assert_eq!(media[0].title.english.as_deref(), Some("Berserk"));
        assert_eq!(
            media[0].cover_image.as_ref().unwrap().extra_large.as_deref(),
            Some("https://s4.anilist.co/x.jpg")
        );
        assert!(media[1].cover_image.is_none());
    }

    #[test]
    fn test_norm_title() {
        assert_eq!(norm_title("Berserk"), norm_title(" berserk "));
        assert_eq!(
            norm_title("Berserk: The Black Swordsman"),
            norm_title("berserk the black swordsman")
        );
        assert_ne!(norm_title("Berserk"), norm_title("Berserk of Gluttony"));
    }

    #[test]
    fn test_comicvine_payload_parses() {
        let payload = r#"{
            "error": "OK",
            "status_code": 200,
            "results": [
                {
                    "id": 4050,
                    "name": "Blake and Mortimer",
                    "start_year": "1946",
                    "image": {
                        "medium_url": "https://comicvine.gamespot.com/api/image/medium/1-4050.jpg",
                        "small_url": "https://comicvine.gamespot.com/api/image/small/1-4050.jpg"
                    },
                    "description": "<p>An espionage <b>series</b>.<br>By Jacobs.</p>"
                }
            ]
        }"#;
        let parsed: ComicVineResp = serde_json::from_str(payload).unwrap();
        assert_eq!(parsed.error.as_deref(), Some("OK"));
        let v = &parsed.results[0];
        assert_eq!(v.name.as_deref(), Some("Blake and Mortimer"));
        assert_eq!(v.start_year.as_deref(), Some("1946"));
        assert_eq!(
            v.image.as_ref().unwrap().medium_url.as_deref(),
            Some("https://comicvine.gamespot.com/api/image/medium/1-4050.jpg")
        );
        assert_eq!(
            strip_html(v.description.as_deref().unwrap()),
            "An espionage series.\nBy Jacobs."
        );
    }

    #[test]
    fn test_comicvine_error_field_carries_failures() {
        let payload = r#"{ "error": "API Key Invalid", "results": [] }"#;
        let parsed: ComicVineResp = serde_json::from_str(payload).unwrap();
        assert_eq!(parsed.error.as_deref(), Some("API Key Invalid"));
        assert!(parsed.results.is_empty());
    }

    #[test]
    fn test_comicvine_issues_payload_parses() {
        let payload = r#"{
            "error": "OK",
            "number_of_total_results": 2,
            "results": [
                {
                    "issue_number": "10",
                    "name": "Vol. 10",
                    "cover_date": "1995-09-30",
                    "image": { "medium_url": "https://x/10.jpg", "small_url": "https://x/10s.jpg" }
                },
                { "issue_number": "1", "name": null, "cover_date": "1990-12-01", "image": null }
            ]
        }"#;
        let parsed: ComicVineIssuesResp = serde_json::from_str(payload).unwrap();
        assert_eq!(parsed.number_of_total_results, Some(2));
        assert_eq!(parsed.results.len(), 2);
        assert_eq!(parsed.results[0].issue_number.as_deref(), Some("10"));
        assert_eq!(
            parsed.results[0]
                .image
                .as_ref()
                .unwrap()
                .medium_url
                .as_deref(),
            Some("https://x/10.jpg")
        );
    }
}
