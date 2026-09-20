//! Subtitle support: local subtitle discovery, language guessing, SRT/ASS →
//! WebVTT conversion, and the OpenSubtitles.com API client for online search
//! and download.
//!
//! Online subs require an API key (`chadtorrent.json` →
//! `opensubtitles_api_keys`). Everything local works without one.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{anyhow, Context};

use crate::config::{ApiKeys, Usage};

/// Subtitle file extensions we recognize on disk.
const SUB_EXTS: &[&str] = &["srt", "vtt", "ass", "ssa"];

/// A subtitle file found locally (torrent folder or downloaded).
#[derive(Debug, Clone, serde::Serialize)]
pub struct LocalSub {
    /// File name (unique within the item's subtitle set).
    pub name: String,
    /// ISO 639 code when known (e.g. "en"), otherwise null.
    pub lang: Option<String>,
    /// Flag emoji for the language.
    pub flag: String,
    /// Where it came from: found in the torrent, or downloaded by us.
    pub source: String,
}

// ---------- language tables ----------

/// Common filename language tags → ISO 639-1 code.
const FILENAME_TAGS: &[(&str, &str)] = &[
    ("en", "en"), ("eng", "en"), ("english", "en"),
    ("sv", "sv"), ("se", "sv"), ("swe", "sv"), ("swedish", "sv"),
    ("da", "da"), ("dan", "da"), ("danish", "da"),
    ("no", "no"), ("nor", "no"), ("norwegian", "no"), ("nb", "no"), ("nob", "no"),
    ("fi", "fi"), ("fin", "fi"), ("finnish", "fi"),
    ("de", "de"), ("ger", "de"), ("deu", "de"), ("german", "de"),
    ("fr", "fr"), ("fre", "fr"), ("fra", "fr"), ("french", "fr"),
    ("es", "es"), ("spa", "es"), ("spanish", "es"),
    ("it", "it"), ("ita", "it"), ("italian", "it"),
    ("pt", "pt"), ("por", "pt"), ("portuguese", "pt"),
    ("pb", "pt"), ("pob", "pt"), ("brazilian", "pt"),
    ("nl", "nl"), ("dut", "nl"), ("nld", "nl"), ("dutch", "nl"),
    ("pl", "pl"), ("pol", "pl"), ("polish", "pl"),
    ("ru", "ru"), ("rus", "ru"), ("russian", "ru"),
    ("uk", "uk"), ("ukr", "uk"), ("ukrainian", "uk"),
    ("ar", "ar"), ("ara", "ar"), ("arabic", "ar"),
    ("he", "he"), ("heb", "he"), ("hebrew", "he"),
    ("tr", "tr"), ("tur", "tr"), ("turkish", "tr"),
    ("el", "el"), ("ell", "el"), ("greek", "el"),
    ("zh", "zh"), ("chi", "zh"), ("zho", "zh"), ("chinese", "zh"),
    ("ja", "ja"), ("jpn", "ja"), ("japanese", "ja"),
    ("ko", "ko"), ("kor", "ko"), ("korean", "ko"),
    ("hi", "hi"), ("hin", "hi"), ("hindi", "hi"),
    ("th", "th"), ("tha", "th"), ("thai", "th"),
    ("vi", "vi"), ("vie", "vi"), ("vietnamese", "vi"),
    ("id", "id"), ("ind", "id"), ("indonesian", "id"),
    ("fa", "fa"), ("per", "fa"), ("fas", "fa"), ("persian", "fa"),
    ("cs", "cs"), ("ces", "cs"), ("czech", "cs"),
    ("hu", "hu"), ("hun", "hu"), ("hungarian", "hu"),
    ("ro", "ro"), ("ron", "ro"), ("rum", "ro"), ("romanian", "ro"),
    ("bg", "bg"), ("bul", "bg"), ("bulgarian", "bg"),
    ("hr", "hr"), ("hrv", "hr"), ("croatian", "hr"),
    ("sr", "sr"), ("srp", "sr"), ("serbian", "sr"),
];

/// whatlang 3-letter code → ISO 639-1.
fn whatlang_to_alpha2(code: &str) -> Option<&'static str> {
    Some(match code {
        "eng" => "en", "swe" => "sv", "dan" => "da", "nob" => "no", "nno" => "no",
        "fin" => "fi", "deu" => "de", "fra" => "fr", "spa" => "es", "ita" => "it",
        "por" => "pt", "nld" => "nl", "pol" => "pl", "rus" => "ru", "ukr" => "uk",
        "ara" => "ar", "heb" => "he", "tur" => "tr", "ell" => "el", "zho" => "zh",
        "jpn" => "ja", "kor" => "ko", "hin" => "hi", "tha" => "th", "vie" => "vi",
        "ind" => "id", "fas" => "fa", "ces" => "cs", "hun" => "hu", "ron" => "ro",
        "bul" => "bg", "hrv" => "hr", "srp" => "sr",
        _ => return None,
    })
}

/// Language code → country flag (language codes aren't country codes; "en"
/// has no EN country).
const LANG_FLAGS: &[(&str, &str)] = &[
    ("en", "🇬🇧"), ("sv", "🇸🇪"), ("da", "🇩🇰"), ("no", "🇳🇴"), ("fi", "🇫🇮"),
    ("de", "🇩🇪"), ("fr", "🇫🇷"), ("es", "🇪🇸"), ("it", "🇮🇹"), ("pt", "🇵🇹"),
    ("nl", "🇳🇱"), ("pl", "🇵🇱"), ("ru", "🇷🇺"), ("uk", "🇺🇦"), ("ar", "🇸🇦"),
    ("he", "🇮🇱"), ("tr", "🇹🇷"), ("el", "🇬🇷"), ("zh", "🇨🇳"), ("ja", "🇯🇵"),
    ("ko", "🇰🇷"), ("hi", "🇮🇳"), ("th", "🇹🇭"), ("vi", "🇻🇳"), ("id", "🇮🇩"),
    ("fa", "🇮🇷"), ("cs", "🇨🇿"), ("hu", "🇭🇺"), ("ro", "🇷🇴"), ("bg", "🇧🇬"),
    ("hr", "🇭🇷"), ("sr", "🇷🇸"), ("pt-br", "🇧🇷"),
];

/// ISO 639-1 → regional-indicator flag emoji.
pub fn flag_for(lang: Option<&str>) -> String {
    let Some(lang) = lang else { return "🏳".to_string() };
    let lower = lang.to_lowercase();
    for candidate in [lower.as_str(), lower.split('-').next().unwrap_or("")] {
        if let Some((_, flag)) = LANG_FLAGS.iter().find(|(code, _)| *code == candidate) {
            return flag.to_string();
        }
    }
    let alpha2: String = if lower.len() == 2 {
        lower
    } else {
        FILENAME_TAGS
            .iter()
            .find(|(tag, code)| code.len() == 2 && (*tag == lower || *code == lower))
            .map(|(_, code)| code.to_string())
            .unwrap_or(lower)
    };
    if let Some((_, flag)) = LANG_FLAGS.iter().find(|(code, _)| *code == alpha2) {
        return flag.to_string();
    }
    let letters: Vec<char> = alpha2.chars().filter(|c| c.is_ascii_lowercase()).collect();
    if letters.len() != 2 {
        return "🏳".to_string();
    }
    letters.iter()
        .map(|c| char::from_u32(0x1F1E6 + (*c as u32 - 'a' as u32)).unwrap_or('🏳'))
        .collect()
}

/// Guess the language of a subtitle file from its name, then (as a fallback)
/// from a sample of its decoded text.
pub fn detect_lang(name: &str, text: Option<&str>) -> Option<String> {
    let lower = name.to_lowercase();
    for token in lower.split(|c: char| !c.is_ascii_alphanumeric()) {
        for (tag, code) in FILENAME_TAGS {
            if token == *tag {
                return Some(code.to_string());
            }
        }
    }
    if let Some(text) = text {
        // timestamps and cue numbers poison language detection — sample only
        // the actual subtitle lines
        let mut sample = String::new();
        for line in text.lines() {
            let t = line.trim();
            if t.is_empty()
                || t.contains("-->")
                || t.starts_with(|c: char| c.is_ascii_digit())
                || t.starts_with('[')
            {
                continue;
            }
            sample.push_str(t);
            sample.push(' ');
            if sample.len() > 4000 {
                break;
            }
        }
        if let Some(info) = whatlang::detect(&sample) {
            return whatlang_to_alpha2(info.lang().code()).map(|s| s.to_string());
        }
    }
    None
}

// ---------- format conversion ----------

fn is_srt_time(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() >= 10 && b[2] == b':' && b[5] == b':' && (b[8] == b',' || b[8] == b'.')
}

/// Normalize a timestamp to VTT form: `00:01:02,345` / `0:01:02,5` →
/// `00:01:02.345` / `00:01:02.500`.
fn vtt_time(s: &str) -> String {
    let s = s.trim().replace(',', ".");
    match s.split_once('.') {
        Some((hms, frac)) => {
            if hms.split(':').count() == 2 {
                return vtt_time(&format!("00:{hms}.{frac}"));
            }
            let mut frac = frac.to_string();
            while frac.len() < 3 {
                frac.push('0');
            }
            frac.truncate(3);
            format!("{hms}.{frac}")
        }
        None => s,
    }
}

/// Pad `H:MM:SS.xx` (ASS) to `HH:MM:SS.mmm` (VTT).
fn ass_time_to_vtt(s: &str) -> Option<String> {
    let s = s.trim();
    let (hms, frac) = s.split_once('.')?;
    let parts: Vec<&str> = hms.split(':').collect();
    if parts.len() != 3 {
        return None;
    }
    let h: u32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let sec: u32 = parts[2].parse().ok()?;
    let mut ms = frac.to_string();
    while ms.len() < 3 {
        ms.push('0');
    }
    ms.truncate(3);
    Some(format!("{h:02}:{m:02}:{sec:02}.{ms}"))
}

/// Convert SRT text to WebVTT.
pub fn srt_to_vtt(srt: &str) -> String {
    let srt = srt.trim_start_matches('\u{feff}');
    let normalized = srt.replace("\r\n", "\n").replace('\r', "\n");
    let mut out = String::from("WEBVTT\n\n");
    for block in normalized.split("\n\n") {
        let block = block.trim();
        if block.is_empty() {
            continue;
        }
        let mut lines: Vec<&str> = block.lines().collect();
        // drop numeric cue identifiers
        if lines.len() > 1 && lines[0].trim().parse::<u32>().is_ok() {
            lines.remove(0);
        }
        if lines.is_empty() {
            continue;
        }
        if let Some((a, b)) = lines[0].split_once("-->") {
            let (ta, tb) = (a.trim(), b.trim());
            if !is_srt_time(ta) {
                continue;
            }
            out.push_str(&format!("{} --> {}\n", vtt_time(ta), vtt_time(tb)));
            for l in &lines[1..] {
                out.push_str(l.trim_end());
                out.push('\n');
            }
            out.push('\n');
        }
    }
    out
}

/// Convert ASS/SSA subtitle text to WebVTT (dialogue lines, style overrides
/// stripped). Good enough for playback; positioning is lost.
pub fn ass_to_vtt(ass: &str) -> String {
    let ass = ass.trim_start_matches('\u{feff}');
    let mut out = String::from("WEBVTT\n\n");
    for line in ass.lines() {
        let Some(rest) = line.strip_prefix("Dialogue:") else {
            continue;
        };
        let fields: Vec<&str> = rest.splitn(10, ',').collect();
        if fields.len() < 10 {
            continue;
        }
        let (Some(start), Some(end)) = (ass_time_to_vtt(fields[1]), ass_time_to_vtt(fields[2]))
        else {
            continue;
        };
        // strip {\...} override tags and \N line breaks
        let mut text = String::new();
        let mut depth = 0usize;
        for c in fields[9].chars() {
            match c {
                '{' => depth += 1,
                '}' => depth = depth.saturating_sub(1),
                _ if depth == 0 => text.push(c),
                _ => {}
            }
        }
        let text = text.replace("\\N", "\n").replace("\\n", "\n");
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        out.push_str(&format!("{start} --> {end}\n{text}\n\n"));
    }
    out
}

/// Convert subtitle bytes (any supported format/container) to WebVTT text.
pub fn to_vtt(name: &str, bytes: &[u8]) -> anyhow::Result<String> {
    let bytes = decompress(name, bytes)?;
    let text = String::from_utf8_lossy(&bytes);
    let lower_name = name.to_lowercase();
    if lower_name.ends_with(".ass") || lower_name.ends_with(".ssa") || text.contains("[Events]") {
        Ok(ass_to_vtt(&text))
    } else if text.trim_start().starts_with("WEBVTT") {
        Ok(text.to_string())
    } else {
        Ok(srt_to_vtt(&text))
    }
}

/// Transparently unwrap gzip/zip containers that OpenSubtitles hands out.
fn decompress(_name: &str, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    if bytes.starts_with(&[0x1f, 0x8b]) {
        use std::io::Read;
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(bytes)
            .read_to_end(&mut out)
            .context("bad gzip subtitle")?;
        return Ok(out);
    }
    if bytes.starts_with(b"PK") {
        let reader = std::io::Cursor::new(bytes);
        let mut archive = zip::ZipArchive::new(reader).context("bad zip subtitle")?;
        // prefer the largest entry that looks like a subtitle
        let mut best: Option<(usize, usize)> = None;
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            let looks_sub = SUB_EXTS.iter().any(|e| file.name().to_lowercase().ends_with(e));
            let is_best = match best {
                Some((_, sz)) => file.size() as usize > sz,
                None => true,
            };
            if looks_sub && is_best {
                best = Some((i, file.size() as usize));
            }
        }
        if let Some((i, _)) = best {
            let mut file = archive.by_index(i)?;
            let mut out = Vec::new();
            std::io::Read::read_to_end(&mut file, &mut out)?;
            return Ok(out);
        }
        anyhow::bail!("zip contains no subtitle file");
    }
    Ok(bytes.to_vec())
}

// ---------- local discovery ----------

/// Find subtitle files under `dir` (recursive). Returns entries with unique
/// names suitable for the `/sub/{id}/{name}` endpoint.
pub fn scan_local_subs(dir: &Path) -> Vec<LocalSub> {
    let mut out: Vec<LocalSub> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(cur) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&cur) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // .subs/ holds downloaded subs and is scanned separately
                if path.file_name().map(|n| n == ".subs").unwrap_or(false) {
                    continue;
                }
                stack.push(path);
                continue;
            }
            let ext_ok = path.extension()
                .and_then(|e| e.to_str())
                .map(|e| SUB_EXTS.contains(&e.to_lowercase().as_str()))
                .unwrap_or(false);
            if !ext_ok {
                continue;
            }
            let Some(name) = path.file_name().map(|n| n.to_string_lossy().to_string()) else {
                continue;
            };
            // many subtitle files are Latin-1/CP1252 — lossy decode is fine
            // for language detection
            let text = std::fs::read(&path)
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .ok();
            let lang = detect_lang(&name, text.as_deref());
            out.push(LocalSub {
                name,
                lang: lang.clone(),
                flag: flag_for(lang.as_deref()),
                source: "torrent".to_string(),
            });
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out.dedup_by(|a, b| a.name == b.name);
    out
}

/// Resolve `name` (from the API) to a subtitle file inside `dir`, refusing
/// path tricks.
pub fn resolve_sub_path(dir: &Path, name: &str) -> Option<PathBuf> {
    if name.is_empty() || name.contains("..") || name.contains('/') || name.contains('\\') {
        return None;
    }
    let path = dir.join(name);
    let canonical_dir = dir.canonicalize().ok()?;
    let canonical = path.canonicalize().ok()?;
    if canonical.starts_with(&canonical_dir) {
        Some(path)
    } else {
        None
    }
}

// ---------- OpenSubtitles.com client ----------

/// Per-key usage state, mirrored into `chadtorrent.json` so it survives
/// restarts. Quotas reset each UTC day, so stale days are cleared on pick.
struct PoolState {
    keys: Vec<String>,
    usage: HashMap<String, Usage>,
}

pub struct SubtitlesClient {
    http: reqwest::Client,
    config_path: PathBuf,
    pool: Mutex<PoolState>,
}

/// Today's UTC date (quota reset boundary) without a date dependency.
fn utc_today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let z = (secs / 86400) as i64 + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Search,
    Download,
}

impl PoolState {
    fn fresh_usage(&self, key: &str) -> Usage {
        let mut u = self.usage.get(key).cloned().unwrap_or_default();
        if u.day != utc_today() {
            u = Usage::default();
        }
        u.day = utc_today();
        u
    }

    /// Key with the lowest usage of `kind` today, skipping exhausted ones.
    fn pick(&mut self, kind: Kind) -> Option<String> {
        let mut best: Option<(u32, String)> = None;
        for key in &self.keys {
            let u = self.fresh_usage(key);
            let (used, exhausted) = match kind {
                Kind::Search => (u.searches, u.search_exhausted),
                Kind::Download => (u.downloads, u.download_exhausted),
            };
            if exhausted {
                continue;
            }
            if best.as_ref().map(|(n, _)| used < *n).unwrap_or(true) {
                best = Some((used, key.clone()));
            }
        }
        best.map(|(_, key)| {
            let u = self.fresh_usage(&key);
            self.usage.insert(key.clone(), u);
            key
        })
    }

    fn bump(&mut self, key: &str, kind: Kind) {
        let mut u = self.fresh_usage(key);
        match kind {
            Kind::Search => u.searches += 1,
            Kind::Download => u.downloads += 1,
        }
        self.usage.insert(key.to_string(), u);
    }

    /// Flag a key as out of quota for today (error mentions the limit).
    fn exhaust(&mut self, key: &str, kind: Kind) {
        let mut u = self.fresh_usage(key);
        match kind {
            Kind::Search => u.search_exhausted = true,
            Kind::Download => u.download_exhausted = true,
        }
        self.usage.insert(key.to_string(), u);
    }

    fn len(&self) -> usize {
        self.keys.len()
    }
}

fn is_quota_error(e: &anyhow::Error) -> bool {
    let msg = format!("{e:#}").to_lowercase();
    ["limit", "429", "406", "exceed", "quota"].iter().any(|w| msg.contains(w))
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OnlineSub {
    pub os_id: String,
    /// The concrete file inside the subtitle entry — /download needs this,
    /// not the subtitle id.
    pub file_id: String,
    pub name: String,
    pub lang: Option<String>,
    pub flag: String,
    pub downloads: u64,
}

impl SubtitlesClient {
    /// `config_path` is where the key pool and per-key usage are persisted
    /// (the same `chadtorrent.json` the keys live in).
    pub fn new(config_path: PathBuf, keys: ApiKeys) -> Self {
        let http = reqwest::Client::builder()
            .user_agent("chadtorrent v1.0")
            .timeout(std::time::Duration::from_secs(20))
            .build()
            .expect("reqwest client");
        let pool = Mutex::new(PoolState {
            keys: keys.opensubtitles_api_keys.clone(),
            usage: keys.opensubtitles_usage,
        });
        Self { http, config_path, pool }
    }

    fn persist(&self, pool: &PoolState) {
        let mut cfg: ApiKeys = std::fs::read_to_string(&self.config_path)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default();
        cfg.opensubtitles_api_keys = pool.keys.clone();
        cfg.opensubtitles_usage = pool.usage.clone();
        if let Err(e) = cfg.save(&self.config_path) {
            eprintln!("failed to persist opensubtitles usage: {e}");
        }
    }

    /// Run `f` with each key in the pool (least-used first) until it succeeds.
    /// Quota-related failures exhaust the key for today and try the next one.
    async fn with_key<T>(
        &self,
        kind: Kind,
        mut f: impl FnMut(String, reqwest::Client) -> futures_util::future::BoxFuture<'static, anyhow::Result<T>>,
    ) -> anyhow::Result<T> {
        let order: Vec<String> = {
            let mut pool = self.pool.lock().unwrap();
            (0..pool.len()).filter_map(|_| pool.pick(kind)).collect()
        };
        let mut last: Option<anyhow::Error> = None;
        for key in &order {
            let key = key.clone();
            match f(key.clone(), self.http.clone()).await {
                Ok(v) => {
                    {
                        let mut pool = self.pool.lock().unwrap();
                        pool.bump(&key, kind);
                        self.persist(&pool);
                    }
                    return Ok(v);
                }
                Err(e) if is_quota_error(&e) => {
                    {
                        let mut pool = self.pool.lock().unwrap();
                        pool.exhaust(&key, kind);
                        self.persist(&pool);
                    }
                    last = Some(e);
                }
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| anyhow!("no opensubtitles API keys configured")))
    }

    /// Search OpenSubtitles by release name and (when known) content hash.
    /// Rotates through the API key pool.
    pub async fn search(
        &self,
        query: &str,
        moviehash: Option<(&str, u64)>,
    ) -> anyhow::Result<Vec<OnlineSub>> {
        let mut url = reqwest::Url::parse("https://api.opensubtitles.com/api/v1/subtitles/")?;
        url.query_pairs_mut().append_pair("query", query);
        if let Some((hash, size)) = moviehash {
            url.query_pairs_mut().append_pair("moviehash", hash);
            url.query_pairs_mut()
                .append_pair("moviebytesize", &size.to_string());
        }
        let body: serde_json::Value = self
            .with_key(Kind::Search, |api_key, http| {
                let url = url.clone();
                Box::pin(async move {
                    let resp = http
                        .get(url)
                        .header("Api-Key", api_key)
                        .send()
                        .await
                        .context("opensubtitles request failed")?;
                    let status = resp.status();
                    let body: serde_json::Value =
                        resp.json().await.context("opensubtitles bad json")?;
                    if !status.is_success() {
                        let err = body["error"].as_str().unwrap_or("unknown error");
                        anyhow::bail!("opensubtitles search failed ({status}): {err}");
                    }
                    Ok(body)
                })
            })
            .await?;
        let mut out = Vec::new();
        for entry in body["data"].as_array().cloned().unwrap_or_default() {
            let attrs = &entry["attributes"];
            let id = entry["id"].as_str().or_else(|| attrs["id"].as_str()).unwrap_or("").to_string();
            if id.is_empty() {
                continue;
            }
            let release = attrs["release"].as_str().unwrap_or("");
            let file_name = attrs["files"][0]["file_name"].as_str().unwrap_or("");
            let name = if release.is_empty() { file_name } else { release };
            let lang = attrs["language"].as_str().map(|s| s.to_string());
            let file_id = match attrs["files"][0]["file_id"].as_str() {
                Some(v) => v.to_string(),
                None => attrs["files"][0]["file_id"]
                    .as_u64()
                    .map(|v| v.to_string())
                    .unwrap_or_default(),
            };
            if file_id.is_empty() {
                continue;
            }
            out.push(OnlineSub {
                os_id: id,
                file_id,
                name: name.to_string(),
                lang: lang.clone(),
                flag: flag_for(lang.as_deref()),
                downloads: attrs["download_count"].as_u64().unwrap_or(0),
            });
        }
        out.sort_by_key(|s| std::cmp::Reverse(s.downloads));
        out.truncate(30);
        Ok(out)
    }

    /// Download a subtitle file by OpenSubtitles file id. Rotates through the
    /// API key pool when a key hits its daily download limit. Returns raw
    /// bytes (possibly compressed) plus a suggested file name.
    pub async fn download(&self, file_id: &str) -> anyhow::Result<(String, Vec<u8>)> {
        let body: serde_json::Value = self
            .with_key(Kind::Download, |api_key, http| {
                let file_id = file_id.to_string();
                Box::pin(async move {
                    let resp = http
                        .post("https://api.opensubtitles.com/api/v1/download")
                        .header("Api-Key", api_key)
                        .json(&serde_json::json!({ "file_id": file_id }))
                        .send()
                        .await
                        .context("opensubtitles download request failed")?;
                    let status = resp.status();
                    let body: serde_json::Value =
                        resp.json().await.context("opensubtitles bad json")?;
                    if !status.is_success() {
                        let err = body["error"].as_str().unwrap_or("unknown error");
                        anyhow::bail!("opensubtitles download failed ({status}): {err}");
                    }
                    Ok(body)
                })
            })
            .await?;
        let link = body["link"]
            .as_str()
            .ok_or_else(|| anyhow!("opensubtitles returned no link"))?
            .to_string();
        let saved_name = body["file_name"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "subtitle.srt".to_string());
        let bytes = self
            .http
            .get(&link)
            .send()
            .await
            .context("subtitle file download failed")?
            .error_for_status()
            .context("subtitle file download failed")?
            .bytes()
            .await
            .context("subtitle file download failed")?;
        Ok((saved_name, bytes.to_vec()))
    }
}

/// The OpenSubtitles movie-hash: size + sum of little-endian u64s of the first
/// and last 64 KiB. Blocking I/O — call via spawn_blocking.
pub fn movie_hash(path: &Path, file_size: u64) -> anyhow::Result<(String, u64)> {
    use std::io::{Read, Seek, SeekFrom};
    const CHUNK: u64 = 64 * 1024;
    let mut f = std::fs::File::open(path)?;
    let mut hash = file_size;
    let mut buf = vec![0u8; CHUNK as usize];
    let read_chunk = |f: &mut std::fs::File, buf: &mut [u8]| -> std::io::Result<()> {
        let mut filled = 0;
        while filled < buf.len() {
            match f.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    };
    read_chunk(&mut f, &mut buf)?;
    for chunk in buf.chunks_exact(8) {
        hash = hash.wrapping_add(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    f.seek(SeekFrom::Start(file_size.saturating_sub(CHUNK)))?;
    read_chunk(&mut f, &mut buf)?;
    for chunk in buf.chunks_exact(8) {
        hash = hash.wrapping_add(u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    Ok((format!("{hash:016x}"), file_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn srt_becomes_vtt() {
        let srt = "\u{feff}1\r\n00:00:01,000 --> 00:00:02,500\r\nHello there\r\n\r\n2\r\n00:00:03,5 --> 00:00:04,5\r\nGeneral Kenobi.\r\n";
        let vtt = srt_to_vtt(srt);
        assert!(vtt.starts_with("WEBVTT\n\n"));
        assert!(vtt.contains("00:00:01.000 --> 00:00:02.500"));
        assert!(vtt.contains("Hello there"));
        assert!(vtt.contains("00:00:03.500 --> 00:00:04.500"));
        assert!(!vtt.contains(','));
    }

    #[test]
    fn srt_ignores_garbage_blocks() {
        let vtt = srt_to_vtt("not a subtitle at all");
        assert_eq!(vtt, "WEBVTT\n\n");
    }

    #[test]
    fn ass_becomes_vtt() {
        let ass = "[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\nDialogue: 0,0:00:05.50,0:00:07.00,Default,,0,0,0,,{\\i1}Hi{\\i} there\\Nfriend\n";
        let vtt = ass_to_vtt(ass);
        assert!(vtt.contains("00:00:05.500 --> 00:00:07.000"));
        assert!(vtt.contains("Hi there\nfriend"));
        assert!(!vtt.contains('{'));
    }

    #[test]
    fn lang_from_filename() {
        assert_eq!(detect_lang("Movie.2020.ENG.srt", None).as_deref(), Some("en"));
        assert_eq!(detect_lang("Movie.2020.swe.srt", None).as_deref(), Some("sv"));
        assert_eq!(detect_lang("Movie.2020.srt", None), None);
        assert_eq!(
            detect_lang(
                "Movie.2020.srt",
                Some("Hello there. General Kenobi, you are a bold one. Please take a seat, we have much to discuss about the fleet and the plans for the next attack on the outpost."),
            ).as_deref(),
            Some("en")
        );
    }

    #[test]
    fn flags_render() {
        assert_eq!(flag_for(Some("en")), "🇬🇧");
        assert_eq!(flag_for(Some("sv")), "🇸🇪");
        assert_eq!(flag_for(None), "🏳");
        assert_eq!(flag_for(Some("eng")), "🇬🇧");
    }

    #[test]
    fn resolve_blocks_traversal() {
        let dir = std::env::temp_dir().join(format!("subtest{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.srt"), b"x").unwrap();
        assert!(resolve_sub_path(&dir, "a.srt").is_some());
        assert!(resolve_sub_path(&dir, "../a.srt").is_none());
        assert!(resolve_sub_path(&dir, "sub/dir/a.srt").is_none());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}


#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::config::ApiKeys;

    fn client_with(path: &Path, keys: &[&str]) -> SubtitlesClient {
        let cfg = ApiKeys {
            opensubtitles_api_keys: keys.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        };
        SubtitlesClient::new(path.to_path_buf(), cfg)
    }

    fn patch_of(path: &Path) -> ApiKeys {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    #[test]
    fn rotates_round_robin_and_persists() {
        let path = std::env::temp_dir().join(format!("pool_rot_{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let c = client_with(&path, &["A", "B", "C"]);
        let pick3 = || {
            let k = c.pool.lock().unwrap().pick(Kind::Download);
            if let Some(k) = &k {
                let mut p = c.pool.lock().unwrap();
                p.bump(k, Kind::Download);
                c.persist(&p);
            }
            k
        };
        assert_eq!(pick3().as_deref(), Some("A"));
        assert_eq!(pick3().as_deref(), Some("B"));
        assert_eq!(pick3().as_deref(), Some("C"));
        assert_eq!(pick3().as_deref(), Some("A")); // wraps
        // usage persisted to the config file
        let saved = patch_of(&path);
        assert_eq!(saved.opensubtitles_usage["A"].downloads, 2);
        assert_eq!(saved.opensubtitles_usage["B"].downloads, 1);
        assert_eq!(saved.opensubtitles_usage["C"].downloads, 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn exhausted_keys_are_skipped() {
        let path = std::env::temp_dir().join(format!("pool_exh_{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let c = client_with(&path, &["A", "B"]);
        {
            let mut p = c.pool.lock().unwrap();
            p.bump("A", Kind::Download);
            p.exhaust("A", Kind::Download);
        }
        assert_eq!(c.pool.lock().unwrap().pick(Kind::Download).as_deref(), Some("B"));
        // exhausted only affects its kind
        assert_eq!(c.pool.lock().unwrap().pick(Kind::Search).as_deref(), Some("A"));
        let _ = std::fs::remove_file(&path);
        let _ = &path;
    }

    #[test]
    fn stale_day_resets_counters() {
        let path = std::env::temp_dir().join(format!("pool_day_{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let c = client_with(&path, &["A", "B"]);
        {
            let mut p = c.pool.lock().unwrap();
            p.usage.insert("A".into(), Usage {
                day: "1999-12-31".into(), searches: 99, downloads: 99,
                search_exhausted: true, download_exhausted: true,
            });
        }
        // yesterday's exhausted flag must not survive the day change
        assert_eq!(c.pool.lock().unwrap().pick(Kind::Download).as_deref(), Some("A"));
        let _ = std::fs::remove_file(&path);
        let _ = &path;
    }


    #[test]
    fn utc_today_shape() {
        let t = utc_today();
        assert_eq!(t.len(), 10);
        assert_eq!(t.as_bytes()[4], b'-');
        assert_eq!(t.as_bytes()[7], b'-');
    }
}
