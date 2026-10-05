use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing::warn;

use crate::db::Database;
use crate::error::{Error, Result};

const TMDB_BASE: &str = "https://api.themoviedb.org/3";

/// How long a cached TMDB payload is trusted before the library re-checks for
/// new seasons/episodes. Fresh-looking shows cost two SQLite reads and no
/// upstream request.
const META_TTL_SECS: i64 = 12 * 3600;

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeasonInfo {
    pub season_number: i64,
    pub name: Option<String>,
    pub episode_count: Option<i64>,
    pub air_date: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EpisodeInfo {
    pub episode_number: i64,
    pub name: Option<String>,
    pub overview: Option<String>,
    pub still_path: Option<String>,
    pub air_date: Option<String>,
}

/// Every episode of one season, as stored in the `tmdb_meta` cache. The whole
/// show's episode lists are cached in one row so a season click never has to
/// wait on TMDB.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeasonEpisodes {
    pub season_number: i64,
    pub episodes: Vec<EpisodeInfo>,
}

/// The cached TMDB payload for one `media_type` + `tmdb_id` pair: the overview
/// paragraph, the canonical season list and every season's episode list.
#[derive(Debug, Clone, Default)]
pub struct TmdbMeta {
    pub overview: Option<String>,
    pub seasons: Vec<SeasonInfo>,
    pub episodes: Vec<SeasonEpisodes>,
    /// Unix seconds of the last *complete* TMDB read. Zero (or old) means the
    /// cache should be re-checked.
    pub fetched_at: i64,
}

impl TmdbMeta {
    pub fn is_stale(&self) -> bool {
        now_secs().saturating_sub(self.fetched_at) >= META_TTL_SECS
    }

    /// Season number -> episode list, the shape the library's episode buttons
    /// read straight out of.
    pub fn episodes_by_season(&self) -> BTreeMap<i64, Vec<EpisodeInfo>> {
        self.episodes
            .iter()
            .map(|s| (s.season_number, s.episodes.clone()))
            .collect()
    }

    /// Identity of the payload ignoring its age — what `refresh` compares to
    /// decide whether anything actually changed upstream.
    fn fingerprint(&self) -> String {
        format!(
            "{:?}|{:?}|{:?}",
            self.overview, self.seasons, self.episodes
        )
    }
}

#[derive(Debug, Deserialize)]
struct TmdbSeasonDetail {
    #[serde(default)]
    episodes: Vec<TmdbEpisode>,
}

#[derive(Debug, Deserialize)]
struct TmdbEpisode {
    #[serde(default)]
    episode_number: Option<i64>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    overview: Option<String>,
    #[serde(default)]
    still_path: Option<String>,
    #[serde(default)]
    air_date: Option<String>,
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

    /// Every season's episode list in one bounded-concurrency sweep. The
    /// library's detail view caches the result, so this only ever runs when a
    /// show is opened cold or its cache has aged out. A season that fails is
    /// dropped rather than failing the sweep — partial data still beats none.
    pub async fn tv_all_episodes(
        &self,
        tmdb_id: i64,
        seasons: &[SeasonInfo],
    ) -> Result<Vec<SeasonEpisodes>> {
        let stream = futures_util::stream::iter(seasons.iter().cloned())
            .map(|s| async move {
                let episodes = self.tv_episodes(tmdb_id, s.season_number).await?;
                Ok::<_, Error>(SeasonEpisodes {
                    season_number: s.season_number,
                    episodes,
                })
            })
            .buffered(6);
        let mut all: Vec<SeasonEpisodes> = stream.filter_map(|r| async move { r.ok() }).collect().await;
        all.sort_by_key(|s| s.season_number);
        Ok(all)
    }

    /// The ordered episode list of one TV season (TMDB), for the show's episode
    /// buttons — the show "has" episodes 1..N regardless of which torrents the
    /// user added.
    pub async fn tv_episodes(&self, tmdb_id: i64, season_number: i64) -> Result<Vec<EpisodeInfo>> {
        let resp = self
            .client
            .get(format!(
                "{}/tv/{}/season/{}",
                TMDB_BASE, tmdb_id, season_number
            ))
            .query(&[("api_key", self.api_key.as_str())])
            .send()
            .await
            .map_err(|e| Error::BadRequest(format!("search request failed: {e}")))?
            .json::<TmdbSeasonDetail>()
            .await
            .map_err(|e| Error::BadRequest(format!("search response invalid: {e}")))?;

        Ok(resp
            .episodes
            .into_iter()
            .filter_map(|e| {
                let episode_number = e.episode_number?;
                Some(EpisodeInfo {
                    episode_number,
                    name: e.name,
                    overview: e.overview,
                    still_path: e.still_path,
                    air_date: e.air_date,
                })
            })
            .collect())
    }

    /// Detail for a movie or show: the display fields plus, for TV, the
    /// canonical season list (specials skipped) — both come out of the one
    /// `/tv/{id}` or `/movie/{id}` response, so a show never needs two requests
    /// to describe itself.
    pub async fn get_details(
        &self,
        tmdb_id: i64,
        media_type: &str,
    ) -> Result<(SearchResult, Vec<SeasonInfo>)> {
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
        let seasons = if media_type == "tv" {
            resp.seasons
                .into_iter()
                .filter(|s| s.season_number.unwrap_or(0) > 0)
                .map(|s| SeasonInfo {
                    season_number: s.season_number.unwrap_or(0),
                    name: s.name,
                    episode_count: s.episode_count,
                    air_date: s.air_date,
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok((
            SearchResult {
                id: resp.id,
                title,
                year,
                poster_path: resp.poster_path,
                backdrop_path: resp.backdrop_path,
                media_type: media_type.to_string(),
                overview: resp.overview,
            },
            seasons,
        ))
    }
}

/// The `tmdb_meta`-backed cache in front of [`TmdbClient`].
///
/// Read path: [`MetaCache::get`] is a single SQLite row and never touches the
/// network, so opening a show paints its season and episode buttons
/// immediately — no per-season TMDB round-trip, no flicker.
///
/// Write path: [`MetaCache::refresh`] re-reads TMDB, but only once the stored
/// payload has aged past [`META_TTL_SECS`], so it doubles as the background
/// check for newly released seasons/episodes. Concurrent refreshes of the same
/// show collapse into one upstream sweep; later callers wait for it and report
/// a change so their caller re-reads the row.
pub struct MetaCache {
    db: Arc<Database>,
    tmdb: Arc<TmdbClient>,
    inflight: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

enum Slot {
    Mine(Arc<tokio::sync::Mutex<()>>),
    Theirs(Arc<tokio::sync::Mutex<()>>),
}

impl MetaCache {
    pub fn new(db: Arc<Database>, tmdb: Arc<TmdbClient>) -> Self {
        Self {
            db,
            tmdb,
            inflight: Mutex::new(HashMap::new()),
        }
    }

    pub fn tmdb(&self) -> &TmdbClient {
        &self.tmdb
    }

    /// The stored payload, or `None` if this show was never fetched.
    pub fn get(&self, media_type: &str, tmdb_id: i64) -> Result<Option<TmdbMeta>> {
        self.db.get_tmdb_meta(media_type, tmdb_id)
    }

    /// Re-read TMDB unless the cache is younger than the TTL. `Ok(true)` means
    /// the stored payload may have changed, so an open detail view should
    /// repaint.
    ///
    /// Concurrent callers for the same show collapse into a single upstream
    /// sweep: the loser waits on the winner's async mutex and then reports a
    /// change (without re-reading TMDB itself). A mutex rather than a notify,
    /// so a waiter can never miss the wakeup.
    pub async fn refresh(&self, media_type: &str, tmdb_id: i64) -> Result<bool> {
        let key = format!("{}:{}", media_type, tmdb_id);
        let slot = {
            let mut g = self.inflight.lock().unwrap();
            match g.entry(key.clone()) {
                Entry::Occupied(e) => Slot::Theirs(e.get().clone()),
                Entry::Vacant(e) => {
                    let m = Arc::new(tokio::sync::Mutex::new(()));
                    e.insert(m.clone());
                    Slot::Mine(m)
                }
            }
        };
        let mine = match slot {
            Slot::Theirs(m) => {
                let _held = m.lock().await;
                return Ok(true);
            }
            Slot::Mine(m) => m,
        };
        let guard = mine.lock().await;
        let res = self.sweep(media_type, tmdb_id).await;
        // Drop the table entry before releasing, so a caller arriving now
        // re-checks the (now fresh) cache instead of riding this sweep.
        self.inflight.lock().unwrap().remove(&key);
        drop(guard);
        res
    }

    /// Fire-and-forget refresh (used when a show is added to the library, or
    /// when a stale cache is served) — never blocks the request that spawned it.
    pub fn spawn_refresh(self: &Arc<Self>, media_type: String, tmdb_id: i64) {
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = me.refresh(&media_type, tmdb_id).await {
                warn!("tmdb refresh failed for {media_type}:{tmdb_id}: {e}");
            }
        });
    }

    async fn sweep(&self, media_type: &str, tmdb_id: i64) -> Result<bool> {
        let cached = self.db.get_tmdb_meta(media_type, tmdb_id).ok().flatten();
        if let Some(c) = cached.as_ref() {
            if !c.is_stale() {
                return Ok(false);
            }
        }
        let (details, seasons) = self.tmdb.get_details(tmdb_id, media_type).await?;
        // A failed episode sweep is stored (the overview and season list are
        // still good) but leaves `fetched_at` alone so the next view retries
        // instead of trusting a show with empty episode lists for a day.
        let (episodes, complete) = if media_type == "tv" {
            match self.tmdb.tv_all_episodes(tmdb_id, &seasons).await {
                Ok(e) => (e, true),
                Err(e) => {
                    warn!("tmdb episode sweep failed for tv:{tmdb_id}: {e}");
                    (Vec::new(), false)
                }
            }
        } else {
            (Vec::new(), true)
        };
        let fetched_at = if complete {
            now_secs()
        } else {
            cached.as_ref().map(|c| c.fetched_at).unwrap_or(0)
        };
        let changed = match cached.as_ref() {
            Some(c) => {
                c.fingerprint()
                    != TmdbMeta {
                        overview: details.overview.clone(),
                        seasons: seasons.clone(),
                        episodes: episodes.clone(),
                        fetched_at,
                    }
                    .fingerprint()
            }
            None => true,
        };
        self.db.set_tmdb_meta(
            media_type,
            tmdb_id,
            details.overview.as_deref(),
            &seasons,
            &episodes,
            fetched_at,
        )?;
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(n: i64) -> EpisodeInfo {
        EpisodeInfo {
            episode_number: n,
            name: Some(format!("Ep {n}")),
            overview: None,
            still_path: None,
            air_date: None,
        }
    }

    fn sample() -> TmdbMeta {
        TmdbMeta {
            overview: Some("A cool show.".into()),
            seasons: vec![SeasonInfo {
                season_number: 2,
                name: None,
                episode_count: Some(2),
                air_date: None,
            }],
            episodes: vec![
                SeasonEpisodes {
                    season_number: 1,
                    episodes: vec![ep(1), ep(2)],
                },
                SeasonEpisodes {
                    season_number: 2,
                    episodes: vec![ep(1)],
                },
            ],
            fetched_at: now_secs(),
        }
    }

    /// The client indexes the episode map by *string* season number
    /// (`m.episodes[String(tvSeason)]`), so integer keys must serialize as
    /// strings — otherwise every season click falls back to the live endpoint.
    #[test]
    fn test_episodes_by_season_serializes_string_keys() {
        let v = serde_json::to_value(sample().episodes_by_season()).unwrap();
        let obj = v.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj["1"].as_array().unwrap().len(), 2);
        assert_eq!(obj["2"].as_array().unwrap().len(), 1);
        assert_eq!(obj["1"][1]["episode_number"], 2);
    }

    #[test]
    fn test_stale_depends_on_fetched_at() {
        let fresh = now_secs();
        assert!(!sample().is_stale());
        let mut old = sample();
        old.fetched_at = fresh - META_TTL_SECS - 1;
        assert!(old.is_stale());
        // A failed sweep leaves fetched_at at 0, so the next view retries
        // instead of trusting a half-filled cache for a day.
        let mut never = sample();
        never.fetched_at = 0;
        assert!(never.is_stale());
    }

    /// The cache's change detection ignores the row's age — otherwise every
    /// successful re-read would look like a change and repaint for nothing.
    #[test]
    fn test_fingerprint_ignores_age() {
        let a = sample();
        let mut b = sample();
        b.fetched_at += 60;
        assert_eq!(a.fingerprint(), b.fingerprint());
        let mut c = sample();
        c.episodes[0].episodes.push(ep(3));
        assert_ne!(a.fingerprint(), c.fingerprint());
    }

    /// Everything in `TmdbMeta` must round-trip through the `tmdb_meta` row —
    /// the fields are serialized by hand into JSON columns.
    #[test]
    fn test_tmdb_meta_roundtrips_through_json() {
        let s = sample();
        let seasons_json = serde_json::to_string(&s.seasons).unwrap();
        let episodes_json = serde_json::to_string(&s.episodes).unwrap();
        let back_seasons: Vec<SeasonInfo> = serde_json::from_str(&seasons_json).unwrap();
        let back_episodes: Vec<SeasonEpisodes> = serde_json::from_str(&episodes_json).unwrap();
        assert_eq!(back_seasons[0].season_number, 2);
        assert_eq!(back_seasons[0].episode_count, Some(2));
        assert_eq!(back_episodes.len(), 2);
        assert_eq!(back_episodes[0].episodes[0].name.as_deref(), Some("Ep 1"));
    }

    /// A corrupt/absent JSON column must degrade to "no data", never panic a
    /// detail request.
    #[test]
    fn test_bad_json_degrades_to_empty() {
        let seasons: Vec<SeasonInfo> = serde_json::from_str("not json").unwrap_or_default();
        let episodes: Vec<SeasonEpisodes> = serde_json::from_str("not json").unwrap_or_default();
        assert!(seasons.is_empty());
        assert!(episodes.is_empty());
    }
}
