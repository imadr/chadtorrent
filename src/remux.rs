use std::process::Stdio;
use tokio::process::Command;

pub struct Remuxer;

#[derive(Debug, Clone)]
pub struct SourceTiming {
    pub video_start: f64,
    pub audio_start: Option<f64>,
    pub audio_skip_seconds: f64,
}

impl Remuxer {
    /// Original packet timestamps and explicit priming metadata. These survive
    /// a container rewrap through an offset mapping; codec/sample-rate guesses
    /// cannot establish the source's A/V alignment.
    pub async fn probe_source_timing(path: &str) -> Option<SourceTiming> {
        let output = Command::new("ffprobe").args([
            "-v", "error", "-read_intervals", "%+#256", "-show_packets", "-show_streams",
            "-show_entries", "stream=index,codec_type,start_time,sample_rate:packet=stream_index,pts_time:packet_side_data=skip_samples",
            "-of", "json", path,
        ]).kill_on_drop(true).output().await.ok()?;
        parse_source_timing(&serde_json::from_slice::<serde_json::Value>(&output.stdout).ok()?)
    }
    /// Build the stream MIME type for a file, e.g.
    /// `video/mp4; codecs="avc1.640028,mp4a.40.5"`. Returns `None` when the
    /// file can't be probed yet (e.g. a partial download without its moov
    /// atom) or when its video codec can't be honestly named — the caller
    /// retries later / fails honestly instead of caching a guess. Guessing
    /// is dangerous: the declared codec string must match the stream's
    /// actual codecs, so a wrong answer is worse than none.
    pub async fn probe_mse_mime(path: &str) -> Option<String> {
        let output = Command::new("ffprobe")
            .args([
                "-v", "error",
                "-print_format", "json",
                "-show_entries", "stream=codec_type,codec_name,profile,level,pix_fmt",
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
                            video = mse_video_codec(codec, st);
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

fn parse_source_timing(json: &serde_json::Value) -> Option<SourceTiming> {
    let streams = json["streams"].as_array()?;
    let packets = json["packets"].as_array()?;
    let first = |stream: &serde_json::Value| packets.iter().find(|p| p["stream_index"] == stream["index"]);
    let timestamp = |stream: &serde_json::Value| {
        first(stream).and_then(|p| p["pts_time"].as_str())
            .or_else(|| stream["start_time"].as_str())?.parse::<f64>().ok().filter(|t| t.is_finite())
    };
    let video = streams.iter().find(|s| s["codec_type"] == "video")?;
    let audio = streams.iter().find(|s| s["codec_type"] == "audio");
    let skip = audio.and_then(first).and_then(|p| p["side_data_list"].as_array())
        .and_then(|data| data.iter().find_map(|d| d["skip_samples"].as_u64())).unwrap_or(0);
    let rate = audio.and_then(|s| s["sample_rate"].as_str()).and_then(|r| r.parse::<f64>().ok()).unwrap_or(0.0);
    Some(SourceTiming {
        video_start: timestamp(video)?,
        audio_start: audio.and_then(timestamp),
        audio_skip_seconds: if rate > 0.0 { skip as f64 / rate } else { 0.0 },
    })
}

/// Bit depth from the ffprobe pixel format ("yuv420p10le" → 10).
fn pix_depth(st: &serde_json::Value) -> u32 {
    let pf = st.get("pix_fmt").and_then(|v| v.as_str()).unwrap_or("");
    if pf.contains("10") {
        10
    } else if pf.contains("12") {
        12
    } else if pf.contains("16") {
        16
    } else {
        8
    }
}

/// Codec string for the video stream, or `None` when the codec can't be
/// honestly named (exotic container codecs with no browser decoder). A
/// guaranteed-mismatch string is worse than none: the browser would reject
/// a stream by its NAME, not by its content.
fn mse_video_codec(codec: &str, st: &serde_json::Value) -> Option<String> {
    match codec {
        "h264" => {
            // avc1.<profile><constraint><level> · profile/level from ffprobe.
            // NOTE: the AVC level byte is the decimal level written as 2 HEX
            // digits (40 → "28") — the opposite convention of HEVC. Don't
            // "fix" this one to decimal.
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
            Some(format!("avc1.{}{}{}", profile_hex, constraint, level_hex))
        }
        "hevc" | "h265" => {
            // hvc1.<profile_idc>.<compat>.L<general_level_idc>.<constraints>
            // e.g. `hvc1.1.6.L93.B0` (Main@3.1), `hvc1.2.4.L153.B0` (Main10).
            // The level MUST be DECIMAL: ffprobe reports general_level_idc as
            // a decimal integer, and Chrome's strict codec-string parser
            // rejects anything else before ever consulting the hardware
            // decoder (a {:03X} here once produced "L05D" for level 93 and
            // wedged every Chrome machine with a perfectly good HEVC
            // decoder, while Safari — which ignores the string — played
            // the same file fine).
            let profile = st.get("profile").and_then(|v| v.as_str()).unwrap_or("Main");
            let level = st.get("level").and_then(|v| v.as_i64()).unwrap_or(120).clamp(30, 186);
            let level_code = format!("L{}.B0", level);
            let profile_code = match profile {
                // The declared profile must match the hvcC the stream
                // actually carries: Main → "1.6", Main10 → "2.4" (the
                // canonical Apple/ffmpeg form).
                "Main 10" => "2.4",
                "Main Still Picture" => "3.1",
                // ffprobe lumps every range-extension profile (4:2:2, 4:4:4,
                // 12-bit) under "Rext" — profile_idc 4. No browser decodes
                // these, but the name still has to tell the truth (the
                // decoder ladder rejects it and the UI says so honestly).
                "Rext" => "4.13",
                _ => "1.6", // Main
            };
            Some(format!("hvc1.{}.{}", profile_code, level_code))
        }
        "vp9" => {
            // vp09.<profile>.<level>.<bit-depth> — declare the REAL depth; a
            // 10-bit stream declared as 08 would configure an 8-bit decoder.
            Some(format!("vp09.00.10.{:02}", pix_depth(st)))
        }
        "av1" => {
            // av01.<profile>.<level><tier>.<bit-depth>
            Some(format!("av01.0.04M.{:02}", pix_depth(st)))
        }
        // Anything else (mpeg2video, mpeg4-ASP, vp6, theora, ...) either
        // can't live in fMP4 or has no browser decoder: name nothing.
        _ => None,
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
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn st(profile: &str, level: i64, pix_fmt: &str) -> serde_json::Value {
        json!({ "profile": profile, "level": level, "pix_fmt": pix_fmt })
    }

    #[test]
    fn source_timing_uses_packet_priming_and_keeps_track_offsets() {
        let timing = parse_source_timing(&json!({
            "streams": [{"index":0,"codec_type":"video","start_time":"0"},
                {"index":1,"codec_type":"audio","sample_rate":"48000","start_time":"0"}],
            "packets": [{"stream_index":0,"pts_time":"0"},
                {"stream_index":1,"pts_time":"-0.021333333333","side_data_list":[{"skip_samples":1024}]}]
        })).unwrap();
        assert_eq!(timing.video_start, 0.0);
        assert!((timing.audio_skip_seconds - 1024.0 / 48000.0).abs() < 1e-9);
        assert!(timing.audio_start.unwrap() < 0.0);
    }

    #[test]
    fn hevc_level_is_decimal() {
        // ffprobe reports general_level_idc as a DECIMAL integer (93 = 3.1).
        // It must land in the codec string verbatim: a hex formatting here
        // produced "L05D" for level 93, and Chrome's strict codec-string
        // parser rejected the whole string before ever consulting the
        // hardware HEVC decoder.
        assert_eq!(
            mse_video_codec("hevc", &st("Main", 93, "yuv420p")).as_deref(),
            Some("hvc1.1.6.L93.B0")
        );
        assert_eq!(
            mse_video_codec("hevc", &st("Main", 120, "yuv420p")).as_deref(),
            Some("hvc1.1.6.L120.B0")
        );
        assert_eq!(
            mse_video_codec("hevc", &st("Main", 150, "yuv420p")).as_deref(),
            Some("hvc1.1.6.L150.B0")
        );
    }

    #[test]
    fn hevc_profiles_match_the_stream() {
        // Main10 is profile_idc 2 compat 4 (the canonical "2.4") — declaring
        // Main "1.6" for a Main10 hvcC is a lie that strict parsers punish.
        assert_eq!(
            mse_video_codec("hevc", &st("Main 10", 153, "yuv420p10le")).as_deref(),
            Some("hvc1.2.4.L153.B0")
        );
        // ffprobe's "Rext" covers all range-extension profiles (idc 4).
        assert_eq!(
            mse_video_codec("hevc", &st("Rext", 120, "yuv422p10le")).as_deref(),
            Some("hvc1.4.13.L120.B0")
        );
        // Still Picture (idc 3) must keep all four segments.
        assert_eq!(
            mse_video_codec("hevc", &st("Main Still Picture", 93, "yuv420p")).as_deref(),
            Some("hvc1.3.1.L93.B0")
        );
    }

    #[test]
    fn h264_keeps_the_hex_level_byte() {
        // AVC is the OPPOSITE convention of HEVC: the level_idc byte is the
        // decimal level written as 2 hex digits (40 → "28", 51 → "33").
        assert_eq!(
            mse_video_codec("h264", &st("High", 40, "yuv420p")).as_deref(),
            Some("avc1.640028")
        );
        assert_eq!(
            mse_video_codec("h264", &st("High", 51, "yuv420p")).as_deref(),
            Some("avc1.640033")
        );
    }

    #[test]
    fn vp9_av1_declare_real_bit_depth() {
        // 10-bit streams declared as 08 would configure an 8-bit decoder.
        assert_eq!(
            mse_video_codec("vp9", &st("", 0, "yuv420p10le")).as_deref(),
            Some("vp09.00.10.10")
        );
        assert_eq!(
            mse_video_codec("vp9", &st("", 0, "yuv420p")).as_deref(),
            Some("vp09.00.10.08")
        );
        assert_eq!(
            mse_video_codec("av1", &st("", 0, "yuv420p10le")).as_deref(),
            Some("av01.0.04M.10")
        );
    }

    #[test]
    fn unknown_video_is_not_named() {
        // Exotic container codecs get NO string rather than a guaranteed
        // mismatch — the manifest stays unset and the player fails honestly.
        assert_eq!(mse_video_codec("mpeg2video", &st("", 0, "")), None);
        assert_eq!(mse_video_codec("mpeg4", &st("", 0, "")), None);
        assert_eq!(mse_video_codec("vp6", &st("", 0, "")), None);
    }
}
