//! Keyless torrent search providers. Every adapter returns the same playable
//! magnet/result shape; provider failures are distinct from a successful empty search.
use std::{collections::HashSet, sync::OnceLock, time::Duration};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    ApiBay,
    Knaben,
    Nyaa,
    Yts,
}

impl Provider {
    pub fn parse(id: &str) -> Result<Self> {
        match id {
            "apibay" => Ok(Self::ApiBay),
            "knaben" => Ok(Self::Knaben),
            "nyaa" => Ok(Self::Nyaa),
            "yts" => Ok(Self::Yts),
            _ => Err(Error::BadRequest(format!("unknown torrent search provider '{id}'"))),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::ApiBay => "ApiBay",
            Self::Knaben => "Knaben",
            Self::Nyaa => "Nyaa",
            Self::Yts => "YTS",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TorrentResult {
    pub name: String,
    pub size_bytes: Option<u64>,
    pub seeders: u64,
    pub leechers: u64,
    pub magnet: String,
}

impl TorrentResult {
    fn new(name: &str, hash: &str, size_bytes: Option<u64>, seeders: u64, leechers: u64) -> Option<Self> {
        let name = name.trim();
        let hash = hash.trim().to_ascii_lowercase();
        if name.is_empty() || hash.len() != 40 || !hash.bytes().all(|b| b.is_ascii_hexdigit())
            || hash.bytes().all(|b| b == b'0') {
            return None;
        }
        let mut magnet = reqwest::Url::parse("magnet:?").ok()?;
        magnet.query_pairs_mut().append_pair("xt", &format!("urn:btih:{hash}")).append_pair("dn", name);
        // Preserve the conventional btih prefix used by existing callers. No
        // trackers are imported: this application's engine uses DHT discovery.
        let dn = magnet.query().and_then(|q| q.split_once("&dn=")).map(|(_, dn)| dn)?;
        Some(Self { name: name.into(), size_bytes, seeders, leechers,
            magnet: format!("magnet:?xt=urn:btih:{hash}&dn={dn}") })
    }
}

pub async fn search(provider: Provider, query: &str) -> Result<Vec<TorrentResult>> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    let client = CLIENT.get_or_init(|| reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .user_agent(concat!("chadtorrent/", env!("CARGO_PKG_VERSION")))
        .build().expect("indexer HTTP client"));
    for attempt in 0..2 {
        match fetch(client, provider, query).await {
            Ok(mut rows) => {
                rows.sort_by(|a, b| b.seeders.cmp(&a.seeders).then_with(|| a.name.cmp(&b.name)));
                let mut seen = HashSet::new();
                rows.retain(|row| seen.insert(row.magnet.split('&').next().unwrap_or("").to_owned()));
                return Ok(rows);
            }
            Err(error) => {
                let http = error.downcast_ref::<reqwest::Error>();
                let retry = http.is_some_and(|e| e.is_timeout() || e.is_connect()
                    || e.status().is_some_and(|s| s.is_server_error() || s.as_u16() == 429));
                if attempt == 0 && retry {
                    tokio::time::sleep(Duration::from_millis(350)).await;
                    continue;
                }
                tracing::warn!("{} torrent search failed for '{query}': {error:#}", provider.label());
                let reason = http.and_then(|e| e.status()).map(|s| format!(" (HTTP {})", s.as_u16())).unwrap_or_default();
                return Err(Error::Torrent(format!("{} search is unavailable{reason}. Try another search provider.", provider.label())));
            }
        }
    }
    unreachable!()
}

async fn fetch(client: &reqwest::Client, provider: Provider, query: &str) -> anyhow::Result<Vec<TorrentResult>> {
    let request = match provider {
        Provider::ApiBay => client.get("https://apibay.org/q.php").query(&[("q", query), ("cat", "0")]),
        Provider::Knaben => client.get("https://api.knaben.org/v2/search")
            .query(&[("q", query), ("o", "seeders"), ("s", "100"), ("c", "2000000,3000000,6000000")]),
        Provider::Nyaa => client.get("https://nyaa.si/")
            .query(&[("page", "rss"), ("q", query), ("c", "1_0"), ("f", "0")]),
        Provider::Yts => client.get("https://movies-api.accel.li/api/v2/list_movies.json")
            .query(&[("query_term", query), ("limit", "50"), ("sort_by", "seeds")]),
    };
    let body = request.send().await?.error_for_status()?.text().await?;
    match provider {
        Provider::ApiBay => parse_apibay(&serde_json::from_str(&body)?),
        Provider::Knaben => parse_knaben(&serde_json::from_str(&body)?),
        Provider::Nyaa => parse_nyaa(&body),
        Provider::Yts => parse_yts(&serde_json::from_str(&body)?),
    }
}

fn number(value: &Value) -> Option<u64> {
    value.as_u64().or_else(|| value.as_str()?.parse().ok())
}

fn parse_apibay(json: &Value) -> anyhow::Result<Vec<TorrentResult>> {
    let rows = json.as_array().ok_or_else(|| anyhow::anyhow!("invalid ApiBay response"))?;
    Ok(rows.iter().filter_map(|row| TorrentResult::new(row["name"].as_str()?, row["info_hash"].as_str()?,
        number(&row["size"]), number(&row["seeders"]).unwrap_or(0), number(&row["leechers"]).unwrap_or(0))).collect())
}

fn parse_knaben(json: &Value) -> anyhow::Result<Vec<TorrentResult>> {
    let hits = json["hits"].as_array().ok_or_else(|| anyhow::anyhow!("invalid Knaben response"))?;
    Ok(hits.iter().filter_map(|row| TorrentResult::new(row["title"].as_str()?, row["hash"].as_str()?,
        number(&row["bytes"]), number(&row["liveSeeders"]).or_else(|| number(&row["seeders"])).unwrap_or(0),
        number(&row["livePeers"]).or_else(|| number(&row["peers"])).unwrap_or(0))).collect())
}

fn parse_yts(json: &Value) -> anyhow::Result<Vec<TorrentResult>> {
    anyhow::ensure!(json["status"] == "ok" && json["data"].is_object(), "invalid YTS response");
    if number(&json["data"]["movie_count"]) == Some(0) { return Ok(Vec::new()); }
    let movies = json["data"]["movies"].as_array().ok_or_else(|| anyhow::anyhow!("missing YTS movies"))?;
    let mut rows = Vec::new();
    for movie in movies {
        let title = movie["title_long"].as_str().or_else(|| movie["title"].as_str()).unwrap_or("");
        if title.is_empty() { continue; }
        for torrent in movie["torrents"].as_array().into_iter().flatten() {
            let name = [title, torrent["quality"].as_str().unwrap_or(""), torrent["type"].as_str().unwrap_or(""),
                torrent["video_codec"].as_str().unwrap_or("")].into_iter().filter(|s| !s.is_empty()).collect::<Vec<_>>().join(" ");
            if let Some(row) = torrent["hash"].as_str().and_then(|hash| TorrentResult::new(&name, hash,
                number(&torrent["size_bytes"]), number(&torrent["seeds"]).unwrap_or(0), number(&torrent["peers"]).unwrap_or(0))) {
                rows.push(row);
            }
        }
    }
    Ok(rows)
}

#[derive(Deserialize)]
struct Rss { channel: Channel }
#[derive(Deserialize)]
struct Channel { #[serde(rename = "item", default)] items: Vec<NyaaItem> }
#[derive(Default, Deserialize)]
#[serde(default)]
struct NyaaItem {
    title: String,
    #[serde(rename = "infoHash")]
    hash: String,
    seeders: String,
    leechers: String,
    size: String,
}

fn parse_nyaa(xml: &str) -> anyhow::Result<Vec<TorrentResult>> {
    let rss: Rss = quick_xml::de::from_str(xml)?;
    Ok(rss.channel.items.into_iter().filter_map(|row| TorrentResult::new(&row.title, &row.hash, parse_size(&row.size),
        row.seeders.parse().unwrap_or(0), row.leechers.parse().unwrap_or(0))).collect())
}

fn parse_size(text: &str) -> Option<u64> {
    let mut parts = text.split_whitespace();
    let value: f64 = parts.next()?.parse().ok()?;
    let scale = match parts.next()?.to_ascii_lowercase().as_str() {
        "b" | "bytes" => 1.0, "kib" => 1024.0, "mib" => 1024.0f64.powi(2), "gib" => 1024.0f64.powi(3),
        "tib" => 1024.0f64.powi(4), "kb" => 1000.0, "mb" => 1e6, "gb" => 1e9, "tb" => 1e12, _ => return None,
    };
    (value.is_finite() && value >= 0.0 && value * scale <= u64::MAX as f64).then_some((value * scale).round() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn apibay_filters_placeholder_and_invalid_hashes() {
        let rows = parse_apibay(&json!([
            {"name":"JoJo's & Friends","info_hash": HASH.to_uppercase(),"size":"1024","seeders":"7","leechers":"2"},
            {"name":"No results","info_hash":"0000000000000000000000000000000000000000"},
            {"name":"Invalid","info_hash":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}
        ])).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].size_bytes, Some(1024));
        let magnet = reqwest::Url::parse(&rows[0].magnet).unwrap();
        assert_eq!(magnet.query_pairs().find(|(k, _)| k == "dn").unwrap().1, "JoJo's & Friends");
        assert!(rows[0].magnet.contains(HASH));
    }

    #[test]
    fn knaben_uses_live_counts_including_zero_and_ignores_upstream_trackers() {
        let rows = parse_knaben(&json!({"hits":[{"title":"Show","hash":HASH,"bytes":1234,
            "seeders":99,"liveSeeders":0,"peers":12,"livePeers":3,"magnetUrl":"magnet:?xt=fake&tr=tracker"}]})).unwrap();
        assert_eq!((rows[0].seeders, rows[0].leechers), (0, 3));
        assert!(!rows[0].magnet.contains("&tr="));
    }

    #[test]
    fn yts_returns_each_quality_variant_and_distinguishes_empty_from_failure() {
        let rows = parse_yts(&json!({"status":"ok","data":{"movie_count":1,"movies":[{
            "title_long":"Primer (2004)","torrents":[{"hash":HASH,"quality":"1080p","type":"bluray","video_codec":"x264","size_bytes":2048,"seeds":4,"peers":1}]
        }]}})).unwrap();
        assert_eq!(rows[0].name, "Primer (2004) 1080p bluray x264");
        assert!(parse_yts(&json!({"status":"ok","data":{"movie_count":0}})).unwrap().is_empty());
        assert!(parse_yts(&json!({"status":"error","status_message":"unavailable"})).is_err());
        assert!(parse_knaben(&json!({"error":"rate limit"})).is_err());
    }

    #[test]
    fn nyaa_rss_decodes_namespaces_entities_and_cdata() {
        let xml = format!(r#"<rss xmlns:nyaa="https://nyaa.si/xmlns/nyaa"><channel>
            <item><title>JoJo&#39;s &amp; Friends 日本語</title><nyaa:infoHash>{HASH}</nyaa:infoHash>
            <nyaa:seeders>42</nyaa:seeders><nyaa:leechers>3</nyaa:leechers><nyaa:size>1.5 GiB</nyaa:size></item>
            <item><title><![CDATA[Other <title>]]></title><nyaa:infoHash>{HASH}</nyaa:infoHash></item>
            </channel></rss>"#);
        let rows = parse_nyaa(&xml).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "JoJo's & Friends 日本語");
        assert_eq!(rows[0].seeders, 42);
        assert_eq!(rows[0].size_bytes, Some(1610612736));
        assert_eq!(rows[1].name, "Other <title>");
        assert!(parse_nyaa("<html>challenge</html>").is_err());
    }
}
