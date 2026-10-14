//! Reading (manga & comics) metadata: ground-truth catalogues + helpers.
//!
//!   - **Manga** catalogue: **Weeb Central** (`reader.rs` owns the client —
//!     search, series synopsis and the complete chapter list all come from
//!     there; this module only shapes the rows). MangaDex is gone: it
//!     delists licensed chapters (Gantz 0/383, One Piece 4/1100+), so its
//!     feed was never "every chapter that exists". AniList still supplies
//!     cover art, title-matched (keyless) — Weeb Central covers are fine to
//!     hotlink but AniList art is larger.
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

/// Legacy: MangaDex UUIDs folded to 52-bit ids (kept only so the one-time
/// `manga_id_52bit` migration can re-key pre-Weeb-Central rows).
pub fn mangadex_id_hash(uuid: &str) -> i64 {
    let hex: String = uuid
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .take(13)
        .collect();
    u64::from_str_radix(&hex, 16)
        .map(|v| v as i64)
        .unwrap_or(0)
}

/// A Weeb Central series id: 26-char uppercase ULID (no dashes).
pub fn is_weebcentral_ref(s: &str) -> bool {
    s.len() == 26 && s.chars().all(|c| c.is_ascii_alphanumeric())
}

/// Stable 52-bit id for a Weeb Central series id (FNV-1a, masked to 52 bits
/// so the value round-trips JavaScript's exact-integer range untouched).
pub fn ext_id_hash(s: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h & 0x000f_ffff_ffff_ffff) as i64
}

/// Numbered chapter rows for the catalogue sweep (Weeb Central lists carry
/// no titles): dedupe, sort ascending, sub-chapters ("37.5") keep their raw
/// number in the label so the frontend can address them exactly.
pub fn chapter_rows_from_labels(labels: Vec<String>) -> Vec<crate::metadata::EpisodeInfo> {
    let mut seen: Vec<String> = Vec::with_capacity(labels.len());
    let mut parsed: Vec<(f64, String)> = Vec::with_capacity(labels.len());
    for raw in labels {
        let Ok(value) = raw.trim().parse::<f64>() else { continue };
        if seen.iter().any(|l| l == &raw) {
            continue;
        }
        seen.push(raw.clone());
        parsed.push((value, raw));
    }
    parsed.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    parsed
        .into_iter()
        .map(|(value, raw)| {
            let whole = value.trunc();
            let is_integer = (value - whole).abs() < f64::EPSILON && value >= 0.0;
            let name = if is_integer { None } else { Some(format!("Ch. {raw}")) };
            chapter_info(whole.max(0.0) as i64, name, None, None)
        })
        .collect()
}

/// Map key for a chapter label: the value scaled ×1000, so "37.5" and
/// "37.50" collide and "0.01" stays distinct.
pub fn title_map_key(label: &str) -> Option<i64> {
    label.trim().parse::<f64>().ok().map(|v| (v * 1000.0).round() as i64)
}

/// Merge swept chapter titles into catalogue rows. Each row's raw label is
/// recovered from the row itself (integer rows are their number;
/// sub-chapter rows carry "Ch. <raw>" in `name`). Integer chapters take the
/// plain title; sub-chapters keep the "Ch. <raw> · <title>" convention (the
/// frontend extracts the raw label from it). Unnamed chapters stay unnamed —
/// never invent names.
pub fn apply_chapter_titles(rows: &mut [crate::metadata::EpisodeInfo], titles: &HashMap<i64, String>) {
    if titles.is_empty() {
        return;
    }
    for row in rows.iter_mut() {
        let raw_label = match row.name.as_deref().and_then(|n| n.strip_prefix("Ch. ")) {
            Some(rest) => rest.split(" · ").next().unwrap_or(rest).to_string(),
            None => row.episode_number.to_string(),
        };
        let Some(key) = title_map_key(&raw_label) else { continue };
        let Some(t) = titles.get(&key) else { continue };
        if t.trim().is_empty() {
            continue;
        }
        row.name = Some(if raw_label.contains('.') {
            format!("Ch. {raw_label} · {t}")
        } else {
            t.clone()
        });
    }
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

    /// Chapter titles from MangaDex, keyed by `title_map_key(label)`.
    /// Best-effort enrichment only — MangaDex is NOT the catalogue (it
    /// delists licensed chapters), but where its feed survives it is the
    /// only source that still carries real chapter names. English titles
    /// win; gaps fall back to any language (first by lang code), so a
    /// chapter gets at most one name.
    pub async fn chapter_titles(&self, title: &str) -> HashMap<i64, String> {
        let norm = norm_title(title);
        if norm.is_empty() {
            return HashMap::new();
        }
        // Resolve the MangaDex entry by exact normalized title; a mismatch
        // means "not this series" — never glue another series' names on.
        let Ok(resp) = self
            .http
            .get(format!("{MANGADEX_API}/manga"))
            .query(&[
                ("title", title),
                ("limit", "8"),
                ("contentRating[]", "safe"),
                ("contentRating[]", "suggestive"),
                ("contentRating[]", "erotica"),
            ])
            .send()
            .await
        else {
            return HashMap::new();
        };
        let Ok(parsed) = resp.json::<MdCollection<MdTitleManga>>().await else {
            return HashMap::new();
        };
        let Some(entry) = parsed
            .data
            .iter()
            .find(|m| norm_title(&m.attributes.pick_title()) == norm)
        else {
            return HashMap::new();
        };
        let uuid = entry.id.clone();
        let mut titles: HashMap<i64, String> = HashMap::new();
        let mut fallbacks: Vec<(i64, String, String)> = Vec::new();
        for english_only in [true, false] {
            let mut total = i64::MAX;
            for offset in (0..1500i64).step_by(500) {
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
                let Ok(resp) = req.send().await else { break };
                let Ok(feed) = resp.json::<MdCollection<MdTitleChapter>>().await else { break };
                total = feed.total.unwrap_or(0);
                for c in &feed.data {
                    let Some(raw) = c.attributes.chapter.as_deref() else { continue };
                    let Some(key) = title_map_key(raw) else { continue };
                    let name = c.attributes.title.clone().unwrap_or_default();
                    if name.trim().is_empty() {
                        continue;
                    }
                    if english_only {
                        titles.entry(key).or_insert(name);
                    } else if !titles.contains_key(&key) {
                        fallbacks.push((
                            key,
                            c.attributes.translated_language.clone().unwrap_or_default(),
                            name,
                        ));
                    }
                }
                if feed.data.len() < 500 {
                    break;
                }
            }
        }
        // deterministic fill: lang code, then title, per key
        fallbacks.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        for (key, _, name) in fallbacks {
            titles.entry(key).or_insert(name);
        }
        titles
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

// ---- MangaDex response types (title enrichment only) ----

#[derive(Debug, Deserialize)]
struct MdCollection<T> {
    #[serde(default)]
    data: Vec<T>,
    #[serde(default)]
    total: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
struct MdTitleManga {
    id: String,
    #[serde(default)]
    attributes: MdTitleAttrs,
}

#[derive(Debug, Deserialize, Default)]
struct MdTitleAttrs {
    #[serde(default)]
    title: HashMap<String, String>,
    #[serde(rename = "altTitles", default)]
    alt_titles: Vec<HashMap<String, String>>,
}

impl MdTitleAttrs {
    fn pick_title(&self) -> String {
        self.title
            .get("en")
            .cloned()
            .or_else(|| self.title.values().next().cloned())
            .or_else(|| self.alt_titles.iter().find_map(|t| t.get("en").cloned()))
            .or_else(|| self.alt_titles.iter().find_map(|t| t.values().next().cloned()))
            .unwrap_or_default()
    }
}

#[derive(Debug, Deserialize, Default)]
struct MdTitleChapter {
    #[serde(default)]
    attributes: MdTitleChapterAttrs,
}

#[derive(Debug, Deserialize, Default)]
struct MdTitleChapterAttrs {
    #[serde(default)]
    chapter: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(rename = "translatedLanguage", default)]
    translated_language: Option<String>,
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

    /// Numbered rows keep their raw label derivable: integers are their
    /// number, sub-chapters carry "Ch. <raw>" — that's what the title merge
    /// keys off.
    #[test]
    fn test_apply_chapter_titles() {
        let mut rows = chapter_rows_from_labels(vec![
            "0.01".into(), "1".into(), "2".into(), "37.5".into(), "386".into(),
        ]);
        let titles: HashMap<i64, String> = HashMap::from([
            (title_map_key("386").unwrap(), "The City of Humans".into()),
            (title_map_key("37.5").unwrap(), "My Festival".into()),
            (title_map_key("1").unwrap(), "Roast Goat".into()),
            (title_map_key("2").unwrap(), "   ".into()), // blank: ignored
        ]);
        apply_chapter_titles(&mut rows, &titles);
        assert_eq!(rows[0].name.as_deref(), Some("Ch. 0.01")); // no title anywhere: label kept
        assert_eq!(rows[1].name.as_deref(), Some("Roast Goat"));
        assert_eq!(rows[2].name.as_deref(), None); // blank title never applies
        assert_eq!(rows[3].name.as_deref(), Some("Ch. 37.5 · My Festival"));
        assert_eq!(rows[4].name.as_deref(), Some("The City of Humans"));
        // raw labels survive the merge — the frontend needs them to resolve
        let raw = rows[3].name.as_deref().unwrap().strip_prefix("Ch. ").unwrap();
        assert_eq!(raw.split(" · ").next(), Some("37.5"));
    }

    /// MangaDex title-enrichment payload: search hit (title maps/altTitles)
    /// and feed rows (chapter + title + language).
    #[test]
    fn test_mangadex_title_payload_parses() {
        let search = r#"{
            "data": [
                { "id": "801513ba-0000",
                  "attributes": { "title": { "ja-ro": "Berserk" },
                                  "altTitles": [{ "en": "Berserk" }] } }
            ]
        }"#;
        let parsed: MdCollection<MdTitleManga> = serde_json::from_str(search).unwrap();
        assert_eq!(parsed.data[0].attributes.pick_title(), "Berserk");
        let feed = r#"{
            "total": 2,
            "data": [
                { "attributes": { "chapter": "386", "title": "The City of Humans", "translatedLanguage": "en" } },
                { "attributes": { "chapter": "37.50", "title": "My Festival", "translatedLanguage": "fr" } }
            ]
        }"#;
        let feed: MdCollection<MdTitleChapter> = serde_json::from_str(feed).unwrap();
        assert_eq!(feed.total, Some(2));
        assert_eq!(feed.data[0].attributes.chapter.as_deref(), Some("386"));
        // "37.50" and "37.5" share a map key
        assert_eq!(
            title_map_key(feed.data[1].attributes.chapter.as_deref().unwrap()),
            title_map_key("37.5")
        );
    }

    #[test]
    fn test_issue_number_parsing() {
        assert_eq!(issue_number("1"), Some(1));
        assert_eq!(issue_number("2.5"), Some(2));
        assert_eq!(issue_number(" 372 "), Some(372));
        assert_eq!(issue_number("Annual 1"), None);
        assert_eq!(issue_number(""), None);
    }

    /// UUID → stable numeric id: deterministic, distinct per UUID, and
    /// always inside JavaScript's exact-integer range (±2^53) so the value
    /// survives the browser round-trip.
    #[test]
    fn test_mangadex_id_hash() {
        let a = mangadex_id_hash("801513ba-a712-498c-8f57-cae55b38cc92");
        let b = mangadex_id_hash("801513ba-a712-498c-8f57-cae55b38cc92");
        let c = mangadex_id_hash("dc332d04-d3b0-413c-a767-70f5e451b031");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.unsigned_abs() < (1u64 << 52), "hash must stay under 2^53 (JS-safe)");
        assert!(c.unsigned_abs() < (1u64 << 52), "hash must stay under 2^53 (JS-safe)");
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
