use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug, Clone)]
#[command(name = "chadtorrent", version, about = "Self-hosted torrent streaming service")]
pub struct Config {
    /// Port to listen on
    #[arg(short, long, default_value_t = 3000)]
    pub port: u16,

    /// Directory for permanent downloads
    #[arg(short, long, default_value = "./library")]
    pub library_dir: PathBuf,

    /// Legacy location of old temporary downloads; only used to clean up
    /// leftover files from before "keep files" was removed.
    #[arg(short, long, default_value = "./temp")]
    pub temp_dir: PathBuf,

    /// SQLite database path
    #[arg(short, long, default_value = "./chadtorrent.db")]
    pub db_path: PathBuf,

    /// Base URL of the watch-party relay used when creating parties (e.g.
    /// wss://relay.example.com). When unset, invites point at this
    /// instance's LAN address instead (same-network parties only).
    #[arg(long)]
    pub party_relay: Option<String>,
}

/// Best-effort LAN address of this machine as a ws:// base URL, used as
/// the invite relay when no external relay is configured. The UDP-connect
/// trick picks the default-route interface without sending any packet.
pub fn lan_ws_base_url(port: u16) -> String {
    use std::net::{IpAddr, UdpSocket};
    let ip = (|| {
        let s = UdpSocket::bind("0.0.0.0:0").ok()?;
        s.connect("8.8.8.8:80").ok()?;
        s.local_addr().ok().map(|a| a.ip())
    })()
    .unwrap_or(IpAddr::from([127, 0, 0, 1]));
    format!("ws://{ip}:{port}")
}

/// API keys live in `chadtorrent.json` next to the working directory — no env
/// vars, no CLI arguments. The file is created with defaults on first start.
///
/// OpenSubtitles keys form a pool: usage (per UTC day) is tracked here too and
/// the client rotates through the pool to spread the per-key quota.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApiKeys {
    #[serde(default)]
    pub tmdb_api_key: String,
    #[serde(default)]
    pub opensubtitles_api_keys: Vec<String>,
    /// Comic Vine API key for the comics search. Free: register at
    /// https://comicvine.gamespot.com/api/ and paste the generated key here.
    #[serde(default)]
    pub comicvine_api_key: String,
    #[serde(default)]
    pub opensubtitles_usage: HashMap<String, Usage>,
}

/// Per-key usage counters, reset when the UTC day changes.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Usage {
    #[serde(default)]
    pub day: String,
    #[serde(default)]
    pub searches: u32,
    #[serde(default)]
    pub downloads: u32,
    #[serde(default)]
    pub search_exhausted: bool,
    #[serde(default)]
    pub download_exhausted: bool,
}

impl ApiKeys {
    pub fn load_or_create(path: &Path) -> std::io::Result<Self> {
        let mut keys: ApiKeys = match std::fs::read_to_string(path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_default(),
            Err(_) => ApiKeys::default(),
        };
        if keys.tmdb_api_key.is_empty() {
            keys.tmdb_api_key = ApiKeys::default().tmdb_api_key;
        }
        keys.opensubtitles_api_keys.retain(|k| !k.is_empty());
        keys.save(path)?;
        Ok(keys)
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        std::fs::write(path, serde_json::to_string_pretty(self)?)
    }
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
