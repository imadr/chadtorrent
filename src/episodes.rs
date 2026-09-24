//! Episode / season detection for TV-show torrents.
//!
//! TV torrents come in all naming shapes: `Show.S01E01.720p.mkv`,
//! `Show.1x02.webrip`, `Season 1/Episode 03.mp4`, `Show S1E01E02.mkv` for
//! multi-episode files, etc. This module squeezes a `(season, episode)` out of
//! a path — used by the library's file listing to bucket torrent files into
//! seasons and episode rows. Files that match nothing are left unclassified
//! and surface under an "Other" bucket in the UI.

use std::sync::OnceLock;

fn episode_rules() -> &'static [regex::Regex] {
    static RE: OnceLock<Box<[regex::Regex]>> = OnceLock::new();
    RE.get_or_init(|| {
        [
            // S01E01, S1E1, S01.E01, S01 E01, S01_E01, plus multi-episode
            // files S01E01E02 / S01E01-E02 (reported by their first episode).
            r"(?i)\bS(?P<s>\d{1,2})\s*[ _.-]?\s*E(?P<e>\d{1,3})(?:[ _.-]?[E-]\d{1,3})*\b",
            // "Season 1 Episode 2" / "Season.1.Episode.2" / "Season 1, Episode 2"
            r"(?i)\bseason\s*[.]?[ ]?(?P<s>\d{1,2})\s*[,:. -]+\s*ep(?:isode)?\s*[.]?[ ]?(?P<e>\d{1,3})\b",
            // 1x01, 12x03 — a compact token all by itself
            r"(?i)\b(?P<s>\d{1,2})x(?P<e>\d{1,3})\b",
        ]
        .into_iter()
        .map(|p| regex::Regex::new(p).expect("valid episode regex"))
        .collect::<Box<[_]>>()
    })
}

/// Parse `(season, episode)` out of a single path component (a file name or a
/// directory name). The first rule to match wins.
pub fn parse_episode(part: &str) -> Option<(u32, u32)> {
    let part = part.trim();
    if part.is_empty() {
        return None;
    }
    for re in episode_rules().iter() {
        if let Some(c) = re.captures(part) {
            let s = c.name("s")?.as_str().parse().ok()?;
            let e = c.name("e")?.as_str().parse().ok()?;
            return Some((s, e));
        }
    }
    None
}

/// Normalize a torrent-relative file path for use as a stable key:
/// forward slashes, no leading `./` or `/`, no trailing stuff.
pub fn normalize_rel_path(path: &str) -> String {
    let p = path.replace('\\', "/");
    let p = p.trim_start_matches("./");
    p.trim_matches('/').to_string()
}

/// Parse `(season, episode)` from a relative torrent path. Prefers the file
/// name itself, then walks the parent folders (many season packs nest videos
/// in `Season 1/Episode 04.mp4` where only the folder carries the number).
/// Finally, when a folder only names the season and the file only names an
/// episode ("Season 1/Episode 03.mp4"), the two are combined.
pub fn parse_episode_path(rel_path: &str) -> Option<(u32, u32)> {
    let mut comps: Vec<&str> = rel_path.split('/').collect();
    let name = comps.pop().unwrap_or(rel_path);
    if let Some(ep) = parse_episode(name) {
        return Some(ep);
    }
    for part in comps.iter().rev() {
        if let Some(ep) = parse_episode(part) {
            return Some(ep);
        }
    }
    match (season_only(&comps), episode_only(name)) {
        (Some(s), Some(e)) => Some((s, e)),
        _ => None,
    }
}

/// A season number that stands alone in a directory name: `Season 1`,
/// `Season1`, or a scene-style `Show.S02.720p` folder. Walks folders from
/// the innermost out.
fn season_only(comps: &[&str]) -> Option<u32> {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(
            r"(?i)\b(?:S(?P<s1>\d{1,2})\b|season\s*[.]?[ ]?(?P<s2>\d{1,2})\b)",
        )
        .expect("valid season regex")
    });
    comps.iter().rev().find_map(|part| {
        let c = re.captures(part.trim())?;
        c.name("s1")
            .or_else(|| c.name("s2"))?
            .as_str()
            .parse()
            .ok()
    })
}

/// An episode number in a bare "Episode 03.mp4" / "ep5.mkv" file name. Only
/// used to complete a season from the parent folder — never in isolation
/// (a stray "ep 5" elsewhere in the library must not turn a file into an
/// episode).
fn episode_only(part: &str) -> Option<u32> {
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(r"(?i)\bep(?:isode)?\s*[.]?[ ]?(?P<e>\d{1,3})\b")
            .expect("valid episode-only regex")
    });
    let c = re.captures(part.trim())?;
    c.name("e")?.as_str().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sxxeyy_forms() {
        assert_eq!(
            parse_episode("True.Detective.S01E01.720p.BluRay.mkv"),
            Some((1, 1))
        );
        assert_eq!(
            parse_episode("Show.S03E12.1080p.WEB-DL.mkv"),
            Some((3, 12))
        );
        assert_eq!(parse_episode("s1e1.mp4"), Some((1, 1)));
        assert_eq!(parse_episode("S01.E02.mp4"), Some((1, 2)));
        assert_eq!(parse_episode("S01 E03.avi"), Some((1, 3)));
        assert_eq!(parse_episode("S1E04-web.webm"), Some((1, 4)));
        assert_eq!(parse_episode("Some.S12E345.mkv"), Some((12, 345)));
    }

    #[test]
    fn multi_episode_takes_first() {
        assert_eq!(parse_episode("Show.S01E01E02.mkv"), Some((1, 1)));
        assert_eq!(parse_episode("Show.S01E01-E02.mkv"), Some((1, 1)));
        assert_eq!(parse_episode("Show.S01E01E02E03.mkv"), Some((1, 1)));
    }

    #[test]
    fn season_episode_words() {
        assert_eq!(parse_episode("Show - Season 1 Episode 2.mkv"), Some((1, 2)));
        assert_eq!(parse_episode("Show.Season.2.Episode.3.mkv"), Some((2, 3)));
        assert_eq!(parse_episode("Season 3, Episode 4.mp4"), Some((3, 4)));
        assert_eq!(parse_episode("Season 5 - Episode 06.mp4"), Some((5, 6)));
    }

    #[test]
    fn x_separator() {
        assert_eq!(parse_episode("Show.1x01.mkv"), Some((1, 1)));
        assert_eq!(parse_episode("Show.12x03.720p.mkv"), Some((12, 3)));
        assert_eq!(parse_episode("Show 1x02 webrip.mp4"), Some((1, 2)));
    }

    #[test]
    fn no_episode() {
        assert_eq!(parse_episode("Show.S01.720p.mkv"), None);
        assert_eq!(parse_episode("movie.2024.720p.mkv"), None);
        assert_eq!(parse_episode("subs.en.srt"), None);
        assert_eq!(parse_episode(""), None);
        assert_eq!(parse_episode("Show.720p.WEB-DL.mkv"), None);
        assert_eq!(parse_episode("Season 2/random.avi"), None);
    }

    #[test]
    fn bare_episode_not_confused() {
        assert_eq!(parse_episode("Episode 4.mp4"), None);
        assert_eq!(parse_episode("ep.07.mkv"), None);
        assert_eq!(parse_episode("ep4.mkv"), None);
    }

    #[test]
    fn normalize_relative_paths() {
        assert_eq!(normalize_rel_path("a/b/c.mkv"), "a/b/c.mkv");
        assert_eq!(normalize_rel_path("./a/b.mkv"), "a/b.mkv");
        assert_eq!(normalize_rel_path("/a/b.mkv"), "a/b.mkv");
        assert_eq!(normalize_rel_path("a\\b.mkv"), "a/b.mkv");
        assert_eq!(normalize_rel_path("a/b.mkv/"), "a/b.mkv");
        assert_eq!(normalize_rel_path(""), "");
    }

    #[test]
    fn paths_prefer_filename_then_folder() {
        assert_eq!(
            parse_episode_path("True.Detective.S01.720p/True.Detective.S01E02.720p.mkv"),
            Some((1, 2))
        );
        assert_eq!(parse_episode_path("Season 1/Episode 03.mp4"), Some((1, 3)));
        assert_eq!(
            parse_episode_path("Show.S02.720p/episode5.mkv"),
            Some((2, 5))
        );
        assert_eq!(parse_episode_path("Show.S02.720p/special.mkv"), None);
        assert_eq!(parse_episode_path("Show/season 2/01.mkv"), None);
        assert_eq!(parse_episode_path("Breaking.Bad.S03/03x01.mp4"), Some((3, 1)));
    }
}