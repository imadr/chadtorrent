use reqwest::Client;
use serde::Deserialize;

use crate::error::{Error, Result};

const TMDB_BASE: &str = "https://api.themoviedb.org/3";

#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchResult {
    pub id: i64,
    pub title: String,
    pub year: Option<i32>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
    pub media_type: String,
    pub overview: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TmdbSearchResponse {
    results: Vec<TmdbResult>,
}

#[derive(Debug, Deserialize)]
struct TmdbResult {
    id: i64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    release_date: Option<String>,
    #[serde(default)]
    first_air_date: Option<String>,
    #[serde(default)]
    poster_path: Option<String>,
    #[serde(default)]
    backdrop_path: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    overview: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TmdbMovieDetails {
    id: i64,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    release_date: Option<String>,
    #[serde(default)]
    first_air_date: Option<String>,
    #[serde(default)]
    poster_path: Option<String>,
    #[serde(default)]
    backdrop_path: Option<String>,
    #[serde(default)]
    overview: Option<String>,
    #[serde(default)]
    seasons: Vec<TmdbSeason>,
}

#[derive(Debug, Deserialize)]
struct TmdbSeason {
    #[serde(default)]
    season_number: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    episode_count: Option<i64>,
    #[serde(default)]
    air_date: Option<String>,
}

/// One canonical season of a TV show (TMDB), used by the season chips in the
/// library's TV-show detail view. Chips list the show's real seasons; torrents
/// are then filtered by the selected season.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SeasonInfo {
    pub season_number: i64,
    pub name: Option<String>,
    pub episode_count: Option<i64>,
    pub air_date: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SeasonInfoList {
    pub seasons: Vec<SeasonInfo>,
}

pub struct TmdbClient {
    client: Client,
    api_key: String,
}

impl TmdbClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
        }
    }

    pub async fn search(&self, query: &str) -> Result<Vec<SearchResult>> {
        let resp = self
            .client
            .get(format!("{}/search/multi", TMDB_BASE))
            .query(&[
                ("api_key", self.api_key.as_str()),
                ("query", query),
                ("include_adult", "false"),
            ])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("search request failed: {e}")))?
            .json::<TmdbSearchResponse>()
            .await
            .map_err(|e| Error::BadRequest(format!("search response invalid: {e}")))?;

        let results = resp
            .results
            .into_iter()
            .filter(|r| {
                matches!(r.media_type.as_deref(), Some("movie") | Some("tv"))
            })
            .map(|r| {
                let (title, date) = match r.media_type.as_deref() {
                    Some("tv") => (r.name.unwrap_or_default(), r.first_air_date),
                    _ => (r.title.unwrap_or_default(), r.release_date),
                };
                let year = date
                    .as_ref()
                    .and_then(|d| d.split('-').next())
                    .and_then(|y| y.parse::<i32>().ok());
                SearchResult {
                    id: r.id,
                    title,
                    year,
                    poster_path: r.poster_path,
                    backdrop_path: r.backdrop_path,
                    media_type: r.media_type.unwrap_or_else(|| "movie".into()),
                    overview: r.overview,
                }
            })
            .collect();
        Ok(results)
    }

    /// The canonical season list of a TV show (specials skipped). This feeds
    /// the season chips in the library — the app only knows seasons from
    /// torrent file names otherwise, and a show "has" seasons 1..N regardless
    /// of which torrents the user added.
    pub async fn tv_seasons(&self, tmdb_id: i64) -> Result<Vec<SeasonInfo>> {
        let resp = self
            .client
            .get(format!("{}/tv/{}", TMDB_BASE, tmdb_id))
            .query(&[("api_key", self.api_key.as_str())])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("search request failed: {e}")))?
            .json::<TmdbMovieDetails>()
            .await
            .map_err(|e| Error::BadRequest(format!("search response invalid: {e}")))?;

        Ok(resp
            .seasons
            .into_iter()
            .filter(|s| s.season_number.unwrap_or(0) > 0)
            .map(|s| SeasonInfo {
                season_number: s.season_number.unwrap_or(0),
                name: s.name,
                episode_count: s.episode_count,
                air_date: s.air_date,
            })
            .collect())
    }

    #[allow(dead_code)]
    pub async fn get_details(&self, tmdb_id: i64, media_type: &str) -> Result<SearchResult> {
        let endpoint = match media_type {
            "tv" => format!("{}/tv/{}", TMDB_BASE, tmdb_id),
            _ => format!("{}/movie/{}", TMDB_BASE, tmdb_id),
        };
        let resp = self
            .client
            .get(&endpoint)
            .query(&[("api_key", self.api_key.as_str())])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("search request failed: {e}")))?
            .json::<TmdbMovieDetails>()
            .await
            .map_err(|e| Error::BadRequest(format!("search response invalid: {e}")))?;

        let title = match media_type {
            "tv" => resp.name.unwrap_or_default(),
            _ => resp.title.unwrap_or_default(),
        };
        let date = match media_type {
            "tv" => resp.first_air_date,
            _ => resp.release_date,
        };
        let year = date
            .as_ref()
            .and_then(|d| d.split('-').next())
            .and_then(|y| y.parse::<i32>().ok());

        Ok(SearchResult {
            id: resp.id,
            title,
            year,
            poster_path: resp.poster_path,
            backdrop_path: resp.backdrop_path,
            media_type: media_type.to_string(),
            overview: resp.overview,
        })
    }
}
