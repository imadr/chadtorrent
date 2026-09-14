use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "chadtorrent", version, about = "Self-hosted torrent streaming service")]
pub struct Config {
    /// Port to listen on
    #[arg(short, long, default_value_t = 3000)]
    pub port: u16,

    /// Directory for permanent downloads (keep=true)
    #[arg(short, long, default_value = "./library")]
    pub library_dir: PathBuf,

    /// Directory for temporary downloads (keep=false)
    #[arg(short, long, default_value = "./temp")]
    pub temp_dir: PathBuf,

    /// SQLite database path
    #[arg(short, long, default_value = "./chadtorrent.db")]
    pub db_path: PathBuf,
}

impl Config {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
