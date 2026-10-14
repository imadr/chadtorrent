//! Reading sources — where manga chapter *pages* come from, and (since the
//! catalogue merged into it) the ground-truth sweep: search, series metadata
//! and the full chapter list all come from Weeb Central, keyless.
//!
//! Why Weeb Central and not MangaDex: MangaDex delists licensed chapters
//! (Gantz: 0/383 english, One Piece: 4/1100+, Berserk: shrunken) — its feed
//! is not "every chapter that exists". Weeb Central keeps complete lists
//! (verified 383/383 Gantz, 386/386 Berserk) with plain HTML fragments:
//!   - search: `POST /search/simple?location=main` (htmx fragment)
//!   - series page: `GET /series/{id}` (title, synopsis)
//!   - chapters: `GET /series/{id}/full-chapter-list` (numbers + ids)
//!   - pages: `GET /chapters/{hid}/images?style=list` (image strip)
//!
//! Chapter TITLES don't exist anywhere reliable (every keyless complete
//! source strips them), so rows are numbered chapters — the frontend labels
//! them "Chapter N".
//!
//! The catalogue id (library row `ext_key`) IS the Weeb Central series id,
//! so a group's reading-source binding is exact by construction. Page
//! images are always proxied through the server (`http.rs`), so the browser
//! never talks to the source: no CORS, no referer games.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use reqwest::Client;

use crate::error::{Error, Result};
use crate::reading::{chapter_rows_from_labels, ext_id_hash, ReadingResult};

const WEEBCENTRAL: &str = "https://weebcentral.com";
/// Cover CDN, constructible straight from the series id (verified stable).
fn cover_url(series_id: &str) -> String {
    format!("https://temp.compsci88.com/cover/fallback/{series_id}.jpg")
}
/// Resolved page lists are cached so the per-image proxy route doesn't
/// re-resolve (chapter list + strip) for every page of a chapter.
const PAGES_TTL: Duration = Duration::from_secs(10 * 60);
const PAGES_CACHE_MAX: usize = 128;

/// Every source id the API accepts, with its display label.
pub fn sources() -> &'static [(&'static str, &'static str)] {
    &[("weebcentral", "Weeb Central")]
}

pub fn source_label(id: &str) -> Result<&'static str> {
    sources()
        .iter()
        .find(|(sid, _)| *sid == id)
        .map(|(_, label)| *label)
        .ok_or_else(|| Error::BadRequest(format!("unknown reading source '{id}'")))
}

/// One series a manga can be bound to on a source.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceMatch {
    /// Source-specific series key (Weeb Central series id).
    pub key: String,
    pub title: String,
}

/// Series-page metadata (the catalogue sweep's title + synopsis).
#[derive(Debug, Clone, Default)]
pub struct SeriesMeta {
    pub title: String,
    pub description: Option<String>,
}

pub struct ReaderClient {
    http: Client,
    /// (source, series, chapter label) → (page URLs, resolved at).
    pages_cache: Mutex<HashMap<PagesCacheKey, CachedPages>>,
}

/// Resolved page URLs for one chapter, plus when they were resolved.
type CachedPages = (Vec<String>, Instant);
/// Cache key: (source, series ref, chapter label).
type PagesCacheKey = (String, String, String);

impl Default for ReaderClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ReaderClient {
    pub fn new() -> Self {
        Self {
            http: Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 \
                     (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                )
                .build()
                .expect("reqwest client"),
            pages_cache: Mutex::new(HashMap::new()),
        }
    }

    // ---- catalogue (ground truth) ----

    /// Manga catalogue search. Same result shape as the TMDB search, so the
    /// frontend's add flow treats every catalogue identically. The Weeb
    /// Central series id is short ULID — it becomes the library row's
    /// `ext_key` verbatim, and its 52-bit FNV hash the numeric `tmdb_id`.
    pub async fn search_catalogue(&self, query: &str) -> Result<Vec<ReadingResult>> {
        let hits = self.weebcentral_search(query).await?;
        Ok(hits
            .into_iter()
            .map(|m| ReadingResult {
                id: ext_id_hash(&m.key),
                title: m.title,
                year: None,
                poster_path: Some(cover_url(&m.key)),
                media_type: "manga".into(),
                // The quick-search fragment carries no description; the
                // first detail open sweeps it into the cache.
                overview: None,
                ext_key: Some(m.key),
            })
            .collect())
    }

    /// Series metadata (title + synopsis) for the catalogue sweep.
    pub async fn series_meta(&self, series_id: &str) -> Result<SeriesMeta> {
        let page = self
            .http
            .get(format!("{WEEBCENTRAL}/series/{series_id}"))
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central series failed: {e}")))?
            .error_for_status()
            .map_err(|e| Error::BadRequest(format!("weeb central series failed: {e}")))?
            .text()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central response invalid: {e}")))?;
        Ok(parse_weebcentral_series(&page))
    }

    /// The full chapter list (raw labels, ascending, deduped) — the
    /// ground-truth rows for the catalogue sweep.
    pub async fn chapter_labels(&self, series_id: &str) -> Result<Vec<String>> {
        let fragment = self
            .http
            .get(format!("{WEEBCENTRAL}/series/{series_id}/full-chapter-list"))
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central chapter list failed: {e}")))?
            .error_for_status()
            .map_err(|e| Error::BadRequest(format!("weeb central chapter list failed: {e}")))?
            .text()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central response invalid: {e}")))?;
        let rows = parse_weebcentral_chapters(&fragment);
        let mut labels: Vec<String> = Vec::with_capacity(rows.len());
        for (label, _) in rows {
            if !labels.iter().any(|l| l == &label) {
                labels.push(label);
            }
        }
        Ok(labels)
    }

    /// Ground-truth rows for one series: chapter numbers ascending, ready
    /// for the `tmdb_meta` cache.
    pub async fn catalogue_detail(
        &self,
        series_id: &str,
    ) -> Result<(SeriesMeta, Vec<crate::metadata::EpisodeInfo>)> {
        let meta = self.series_meta(series_id).await?;
        let labels = self.chapter_labels(series_id).await?;
        Ok((meta, chapter_rows_from_labels(labels)))
    }

    // ---- reading source ----

    /// Series search on one source (manual match picker).
    pub async fn search(&self, source: &str, query: &str) -> Result<Vec<SourceMatch>> {
        match source {
            "weebcentral" => self.weebcentral_search(query).await,
            _ => Err(Error::BadRequest(format!(
                "unknown reading source '{source}'"
            ))),
        }
    }

    /// Best match for a title: exact normalized title when the search has
    /// one, else the first hit, else nothing.
    pub async fn auto_match(&self, source: &str, title: &str) -> Option<SourceMatch> {
        let hits = self.search(source, title).await.ok()?;
        hits.iter()
            .find(|h| norm_key(&h.title) == norm_key(title))
            .cloned()
            .or_else(|| hits.first().cloned())
    }

    /// The page image URLs of one chapter, in order. `label` is the raw
    /// chapter number ("386", "37.5"); `series` is the source's series key.
    pub async fn chapter_pages(
        &self,
        source: &str,
        series: &str,
        label: &str,
    ) -> Result<Vec<String>> {
        let cache_key = (source.to_string(), series.to_string(), label.to_string());
        if let Some((pages, at)) = self.pages_cache.lock().unwrap().get(&cache_key) {
            if at.elapsed() < PAGES_TTL {
                return Ok(pages.clone());
            }
        }
        let pages = match source {
            "weebcentral" => self.weebcentral_pages(series, label).await?,
            _ => return Err(Error::BadRequest(format!("unknown reading source '{source}'"))),
        };
        if pages.is_empty() {
            return Err(Error::BadRequest(format!(
                "chapter {label} not found on {}",
                source_label(source).unwrap_or(source)
            )));
        }
        let mut cache = self.pages_cache.lock().unwrap();
        if cache.len() >= PAGES_CACHE_MAX {
            cache.retain(|_, (_, at)| at.elapsed() < PAGES_TTL);
        }
        cache.insert(cache_key, (pages.clone(), Instant::now()));
        Ok(pages)
    }

    /// Fetch one proxied page image. Returns (bytes, content-type).
    pub async fn fetch_page(
        &self,
        source: &str,
        series: &str,
        label: &str,
        index: usize,
    ) -> Result<(Vec<u8>, String)> {
        let pages = self.chapter_pages(source, series, label).await?;
        let url = pages
            .get(index)
            .ok_or_else(|| Error::NotFound(format!("page {index} out of range")))?;
        let resp = self
            .http
            .get(url)
            .header("Accept", "image/*,*/*")
            .header("Referer", "https://weebcentral.com/")
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("page fetch failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(Error::BadRequest(format!(
                "page fetch failed: HTTP {}",
                resp.status()
            )));
        }
        let ctype = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("image/jpeg")
            .to_string();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| Error::BadRequest(format!("page read failed: {e}")))?;
        Ok((bytes.to_vec(), ctype))
    }

    // ---- Weeb Central ----

    async fn weebcentral_search(&self, query: &str) -> Result<Vec<SourceMatch>> {
        let fragment = self
            .http
            .post(format!("{WEEBCENTRAL}/search/simple?location=main"))
            .form(&[("text", query)])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central search failed: {e}")))?
            .error_for_status()
            .map_err(|e| Error::BadRequest(format!("weeb central search failed: {e}")))?
            .text()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central response invalid: {e}")))?;
        Ok(parse_weebcentral_search(&fragment))
    }

    async fn weebcentral_pages(&self, series_id: &str, label: &str) -> Result<Vec<String>> {
        let list = self.weebcentral_chapter_list(series_id).await?;
        let hid = match_chapter(&list, label).ok_or_else(|| {
            Error::BadRequest(format!(
                "chapter {label} not found on Weeb Central (series has {} chapters listed)",
                list.len()
            ))
        })?;
        let strip = self
            .http
            .get(format!("{WEEBCENTRAL}/chapters/{hid}/images"))
            .query(&[("style", "list")])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central chapter fetch failed: {e}")))?
            .error_for_status()
            .map_err(|e| Error::BadRequest(format!("weeb central chapter fetch failed: {e}")))?
            .text()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central response invalid: {e}")))?;
        Ok(parse_weebcentral_images(&strip))
    }

    async fn weebcentral_chapter_list(&self, series_id: &str) -> Result<Vec<(String, String)>> {
        let fragment = self
            .http
            .get(format!("{WEEBCENTRAL}/series/{series_id}/full-chapter-list"))
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central chapter list failed: {e}")))?
            .error_for_status()
            .map_err(|e| Error::BadRequest(format!("weeb central chapter list failed: {e}")))?
            .text()
            .await
            .map_err(|e| Error::BadRequest(format!("weeb central response invalid: {e}")))?;
        Ok(parse_weebcentral_chapters(&fragment))
    }
}

/// Lowercase-alphanumerics title key for exact-match comparisons.
fn norm_key(s: &str) -> String {
    s.to_lowercase().chars().filter(|c| c.is_alphanumeric()).collect()
}

/// Chapter labels compare numerically ("37.5" == "37.50"), falling back to
/// exact string equality for odd labels.
fn label_matches(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => (x - y).abs() < 1e-6,
        _ => false,
    }
}

fn match_chapter(list: &[(String, String)], label: &str) -> Option<String> {
    // The fragment lists newest first; duplicates (re-uploads) resolve to
    // the first occurrence, which is the current one.
    list.iter()
        .find(|(l, _)| label_matches(l, label))
        .map(|(_, hid)| hid.clone())
}

// ---- Weeb Central fragment parsers (plain regex-free scanners) ----

/// The series page: `<h1>{title}</h1>` and the synopsis
/// `<strong>Description</strong><p class="whitespace-pre-wrap …">{text}</p>`.
pub fn parse_weebcentral_series(page: &str) -> SeriesMeta {
    let title = page
        .find("<h1")
        .and_then(|i| {
            let rest = &page[i..];
            let gt = rest.find('>')?;
            let end = rest[gt + 1..].find("</h1>")?;
            Some(rest[gt + 1..gt + 1 + end].trim().to_string())
        })
        .unwrap_or_default();
    let description = page.find("<strong>Description</strong>").and_then(|i| {
        let rest = &page[i..];
        let p = rest.find("<p")?;
        let gt = rest[p..].find('>')? + p;
        let end = rest[gt..].find("</p>")?;
        Some(crate::reading::strip_html(&rest[gt + 1..gt + end]))
    });
    SeriesMeta {
        title,
        description,
    }
}

/// Extract the `src` of every `<img …>` in the pages strip, keeping document
/// order. Only `<img>` tags count — the strip also carries `<script src=…>`
/// and similar, which must not end up as pages.
pub fn parse_weebcentral_images(fragment: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = fragment;
    while let Some(i) = rest.find("<img") {
        rest = &rest[i + 4..];
        let tag_end = rest.find('>').unwrap_or(rest.len());
        let tag = &rest[..tag_end];
        if let Some(s) = tag.find("src=\"") {
            let t = &tag[s + 5..];
            if let Some(e) = t.find('"') {
                let url = &t[..e];
                if url.starts_with("http") && !url.contains("/static/") {
                    out.push(url.to_string());
                }
            }
        }
        rest = &rest[tag_end..];
    }
    out
}

/// The quick-search fragment: `<a href="…/series/{id}/{slug}">` cards whose
/// cover `<img alt="{title} cover">` carries the display title.
pub fn parse_weebcentral_search(fragment: &str) -> Vec<SourceMatch> {
    let mut out = Vec::new();
    let mut rest = fragment;
    while let Some(i) = rest.find("/series/") {
        rest = &rest[i + 8..];
        let Some(id_end) = rest.find(['"', '/']) else {
            break;
        };
        let id = &rest[..id_end];
        // the anchor's inner HTML carries the cover alt text
        let Some(close) = rest.find("</a>") else { break };
        let inner = &rest[..close];
        let title = inner
            .find("alt=\"")
            .map(|a| {
                let t = &inner[a + 5..];
                let e = t.find('"').unwrap_or(t.len());
                t[..e].trim_end_matches(" cover").to_string()
            })
            .unwrap_or_default();
        if !id.is_empty() && !title.is_empty() {
            out.push(SourceMatch {
                key: id.to_string(),
                title,
            });
        }
        rest = &rest[close..];
    }
    out
}

/// The full chapter list fragment: rows of
/// `<a href="…/chapters/{hid}">…<span…>Chapter 386</span></a>`. Returned in
/// document order (newest first). The label lookup is bounded to the anchor
/// itself, so season-separator rows (no "Chapter N" text inside) are skipped
/// instead of stealing the next row's label.
pub fn parse_weebcentral_chapters(fragment: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = fragment;
    while let Some(h) = rest.find("href=\"") {
        rest = &rest[h + 6..];
        let Some(end) = rest.find('"') else { break };
        let url = &rest[..end];
        let Some(cs) = url.find("/chapters/") else { continue };
        let hid = url[cs + 10..].to_string();
        if hid.is_empty() {
            continue;
        }
        let Some(close) = rest.find("</a>") else { break };
        let Some(label) = extract_chapter_label(&rest[..close]) else {
            rest = &rest[close..];
            continue;
        };
        out.push((label, hid));
    }
    out
}

/// "Chapter 386" / "Chapter 37.5" → "386" / "37.5" from a fragment window.
fn extract_chapter_label(window: &str) -> Option<String> {
    let i = window.find(">Chapter ")?;
    let t = &window[i + 9..];
    let end = t.find('<')?;
    let label = t[..end].trim();
    if label.is_empty() { None } else { Some(label.to_string()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_weebcentral_search() {
        let frag = r#"
        <section id="quick-search-result" x-data="{ showResult: true }">
          <a href="https://weebcentral.com/series/01J76XY7EF75DJNQCV04HTPDZK/Berserk" class="btn join-item h-20">
            <div class="w-12 h-12 overflow-hidden">
              <picture>
                <source srcset="https://temp.compsci88.com/cover/small/01J76XY7EF75DJNQCV04HTPDZK.webp" width="200" height="300" type="image/webp">
                <img src="https://temp.compsci88.com/cover/fallback/01J76XY7EF75DJNQCV04HTPDZK.jpg" alt="Berserk cover" width="200" height="300" decoding="async">
              </picture>
            </div>
            <div class="flex-1">Berserk</div>
          </a>
          <a href="https://weebcentral.com/series/01J76XYC3W73Q3BJCAB493FPPR/Tales-Of-Berseria" class="btn join-item h-20">
            <img src="https://temp.compsci88.com/cover/fallback/01J76XYC3W73Q3BJCAB493FPPR.jpg" alt="Tales Of Berseria cover">
            <div class="flex-1">Tales Of Berseria</div>
          </a>
        </section>
        "#;
        let hits = parse_weebcentral_search(frag);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].key, "01J76XY7EF75DJNQCV04HTPDZK");
        assert_eq!(hits[0].title, "Berserk");
        assert_eq!(hits[1].key, "01J76XYC3W73Q3BJCAB493FPPR");
        assert_eq!(hits[1].title, "Tales Of Berseria");
    }

    #[test]
    fn test_parse_weebcentral_search_drops_junk() {
        // season separators / nav links never carry a cover alt
        let frag = r#"
        <a href="https://weebcentral.com/series/ABC123/One-Piece">
          <img alt="One Piece cover">
        </a>
        <a href="https://weebcentral.com/browse">browse</a>
        "#;
        let hits = parse_weebcentral_search(frag);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key, "ABC123");
    }

    #[test]
    fn test_parse_weebcentral_series() {
        let page = r#"<html><head><title>Berserk | Weeb Central</title></head><body>
        <h1 class="text-3xl font-bold">Berserk</h1>
        <section><ul><li><strong>Description</strong>
        <p class="whitespace-pre-wrap break-words">His name is Guts, the <b>Black Swordsman</b>.<br>A feared warrior.</p>
        </li></ul></section></body></html>"#;
        let meta = parse_weebcentral_series(page);
        assert_eq!(meta.title, "Berserk");
        assert_eq!(
            meta.description.as_deref(),
            Some("His name is Guts, the Black Swordsman.\nA feared warrior.")
        );
    }

    #[test]
    fn test_parse_weebcentral_chapters() {
        let frag = r#"
        <li x-show="mark_chapters == 'null'">
          <a href="/chapters/01KX4BFP5E3FEZ50TRS752CT9V" class="hover:bg-base-300 flex-1 flex items-center p-2">
            <span class="me-2"><img src="/static/images/chapter-badge.svg" alt="" width="16" height="16"></span>
            <span class="grow flex gap-2 items-center">
              <span class="">Chapter 386</span>
            </span>
          </a>
        </li>
        <li>
          <a href="/chapters/01KVZQCN6YH5S98R99YMEA5X29" class="hover:bg-base-300 flex-1 flex items-center p-2">
            <span class="">Chapter 37.5</span>
          </a>
        </li>
        <li>
          <a href="/chapters/01KTZDP62QA7K0AMAD3KAPXZD0" class="...">
            <span class="">Season 2</span>
          </a>
        </li>
        "#;
        let list = parse_weebcentral_chapters(frag);
        assert_eq!(list.len(), 2, "season separator row is skipped");
        assert_eq!(list[0], ("386".into(), "01KX4BFP5E3FEZ50TRS752CT9V".into()));
        assert_eq!(list[1], ("37.5".into(), "01KVZQCN6YH5S98R99YMEA5X29".into()));
    }

    #[test]
    fn test_parse_weebcentral_images() {
        let frag = r#"
        <img src="https://scans-hot.planeptune.us/manga/Berserk/0386-001.png" class="max-w-full h-auto mx-auto" alt="Page 1" loading="lazy">
        <img src="https://scans-hot.planeptune.us/manga/Berserk/0386-002.png" alt="Page 2" loading="lazy">
        <img src="/static/images/broken_img.svg" alt="...">
        <script src="https://cdn.jsdelivr.net/npm/x"></script>
        "#;
        let urls = parse_weebcentral_images(frag);
        assert_eq!(urls.len(), 2);
        assert_eq!(
            urls[0],
            "https://scans-hot.planeptune.us/manga/Berserk/0386-001.png"
        );
        assert_eq!(
            urls[1],
            "https://scans-hot.planeptune.us/manga/Berserk/0386-002.png"
        );
    }

    #[test]
    fn test_label_matches() {
        assert!(label_matches("386", "386"));
        assert!(label_matches("37.5", "37.50"));
        assert!(label_matches("1", "1.0"));
        assert!(!label_matches("37.5", "38"));
        assert!(label_matches("0.01", "0.01"));
    }

    #[test]
    fn test_match_chapter_first_wins() {
        let list = vec![
            ("386".to_string(), "new".to_string()),
            ("386".to_string(), "old".to_string()),
            ("385".to_string(), "x".to_string()),
        ];
        assert_eq!(match_chapter(&list, "386").as_deref(), Some("new"));
        assert_eq!(match_chapter(&list, "385").as_deref(), Some("x"));
        assert_eq!(match_chapter(&list, "999"), None);
    }

    #[test]
    fn test_norm_key() {
        assert_eq!(norm_key("Berserk"), norm_key(" berserk "));
        assert_ne!(norm_key("Berserk"), norm_key("Berserk of Gluttony"));
    }

    #[test]
    fn test_cover_url() {
        assert_eq!(
            cover_url("01J76XY7EF75DJNQCV04HTPDZK"),
            "https://temp.compsci88.com/cover/fallback/01J76XY7EF75DJNQCV04HTPDZK.jpg"
        );
    }
}
