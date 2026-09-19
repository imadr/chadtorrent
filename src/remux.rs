use std::process::Stdio;
use tokio::process::Command;

pub struct Remuxer;

impl Remuxer {
    /// Build the `SourceBuffer` MIME type for a file, e.g.
    /// `video/mp4; codecs="avc1.640028,mp4a.40.5"`. Returns `None` when the
    /// file can't be probed yet (e.g. a partial download without its moov
    /// atom) — the caller retries later instead of caching a guess. Guessing
    /// is dangerous: Firefox wedges on an AAC profile mismatch, so a wrong
    /// fallback is worse than no answer.
    pub async fn probe_mse_mime(path: &str) -> Option<String> {
        let output = Command::new("ffprobe")
            .args([
                "-v", "error",
                "-print_format", "json",
                "-show_entries", "stream=codec_type,codec_name,profile,level",
                path,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;

        let mut video: Option<String> = None;
        let mut audio: Option<String> = None;
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&output.stdout) {
            if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
                for st in streams {
                    let ctype = st.get("codec_type").and_then(|v| v.as_str()).unwrap_or("");
                    let codec = st.get("codec_name").and_then(|v| v.as_str()).unwrap_or("");
                    match ctype {
                        "video" if video.is_none() => {
                            video = Some(mse_video_codec(codec, st));
                        }
                        "audio" if audio.is_none() => {
                            audio = Some(mse_audio_codec(codec, st));
                        }
                        _ => {}
                    }
                }
            }
        }

        let v = video.as_deref()?;
        let mut codecs: Vec<&str> = Vec::new();
        codecs.push(v);
        if let Some(a) = audio.as_deref() {
            codecs.push(a);
        }
        Some(format!("video/mp4; codecs=\"{}\"", codecs.join(",")))
    }

    pub async fn probe_mse_duration(path: &str) -> Option<f64> {
        let output = Command::new("ffprobe")
            .args([
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "csv=p=0",
                path,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await
            .ok()?;
        let s = String::from_utf8_lossy(&output.stdout);
        s.trim().parse::<f64>().ok()
    }

}

fn mse_video_codec(codec: &str, st: &serde_json::Value) -> String {
    match codec {
        "h264" => {
            // avc1.<profile><constraint><level> · profile/level from ffprobe.
            let profile = st.get("profile").and_then(|v| v.as_str()).unwrap_or("High");
            let level = st.get("level").and_then(|v| v.as_i64()).unwrap_or(40).clamp(10, 52);
            let level_hex = format!("{:02X}", level as u32);
            let (profile_hex, constraint) = match profile {
                "Baseline" => ("42", "00"),
                "Constrained Baseline" => ("42", "C0"),
                "Main" => ("4D", "00"),
                "Extended" => ("58", "00"),
                "High" => ("64", "00"),
                "High 10" => ("6E", "00"),
                "High 4:2:2" => ("7A", "00"),
                "High 4:4:4 Predictive" => ("F4", "00"),
                _ => ("64", "00"), // High
            };
            format!("avc1.{}{}{}", profile_hex, constraint, level_hex)
        }
        "hevc" | "h265" => {
            // hvc1.<profile_space+profile_idc>.<compat>.<tier_level>.<constraints>
            // Common defaults (Main / Main 10 at level 4.1, Main tier).
            let profile = st.get("profile").and_then(|v| v.as_str()).unwrap_or("Main");
            let level = st.get("level").and_then(|v| v.as_i64()).unwrap_or(120).clamp(30, 186);
            let level_code = format!("L{:03X}.B0", level as u32);
            let profile_code = match profile {
                "Main 10" => "1.6",
                "Main Still Picture" => "3",
                "Main 4:2:2 10" => "4.4",
                "Main 4:4:4" => "3.4",
                "Main 4:4:4 10" => "5.6",
                _ => "1.6", // Main
            };
            format!("hvc1.{}.{}", profile_code, level_code)
        }
        "vp9" => "vp09.00.10.08".to_string(),
        "av1" => "av01.0.04M.08".to_string(),
        "mpeg4" => "mp4v.20.240".to_string(),
        _ => "avc1.640028".to_string(),
    }
}

fn mse_audio_codec(codec: &str, st: &serde_json::Value) -> String {
    match codec {
        "aac" => {
            // The declared codec string must match the AudioSpecificConfig
            // actually carried in the stream. Firefox wedges or degrades
            // playback when the MIME says AAC-LC (40.2) but the stream is
            // HE-AAC (40.5) — verified with a headless A/B test.
            let profile = st.get("profile").and_then(|v| v.as_str()).unwrap_or("");
            match profile {
                "HE-AAC" | "HE-AAC v2" => "mp4a.40.5".to_string(),
                _ => "mp4a.40.2".to_string(),
            }
        }
        "mp3" => "mp4a.6B".to_string(),
        "opus" => "opus".to_string(),
        "flac" => "fLaC".to_string(),
        "vorbis" | "vorb" => "mp4a.40.6".to_string(),
        "ac3" => "ac-3".to_string(),
        "eac3" => "ec-3".to_string(),
        _ => "mp4a.40.2".to_string(),
    }
}