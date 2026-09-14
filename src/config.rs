use clap::Parser;
use std::path::PathBuf;

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

    /// TMDB API key for movie metadata (or set TMDB_API_KEY env var)
    #[arg(
        long,
        env = "TMDB_API_KEY",
        default_value = "e66a2fd4790b22ae4d1a60997f8a47b2"
    )]
    pub tmdb_api_key: Option<String>,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
