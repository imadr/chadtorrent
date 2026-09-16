use std::process::Stdio;
use tokio::process::{Child, Command};
use tracing::info;

pub struct Remuxer;

impl Remuxer {
    /// Build the `SourceBuffer` MIME type for a file, e.g.
    /// `video/mp4; codecs="avc1.640028,mp4a.40.2"`. The codec/base string is
    /// discovered with ffprobe; anything unknown falls back to generic
    /// H.264 + AAC. The player always ends up feeding fMP4 (we remux to that
    /// container), so this is `video/mp4` plus codec hints.
    pub async fn probe_mse_mime(path: &str) -> String {
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
            .await;

        let mut video: Option<String> = None;
        let mut audio: Option<String> = None;
        if let Ok(out) = output {
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                if let Some(streams) = json.get("streams").and_then(|s| s.as_array()) {
                    for st in streams {
                        let ctype = st.get("codec_type").and_then(|v| v.as_str()).unwrap_or("");
                        let codec = st.get("codec_name").and_then(|v| v.as_str()).unwrap_or("");
                        match ctype {
                            "video" if video.is_none() => {
                                video = Some(mse_video_codec(codec, st));
                            }
                            "audio" if audio.is_none() => {
                                audio = Some(mse_audio_codec(codec));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        let mut codecs: Vec<&str> = Vec::new();
        if let Some(v) = video.as_deref() {
            codecs.push(v);
        } else {
            codecs.push("avc1.640028");
        }
        if let Some(a) = audio.as_deref() {
            codecs.push(a);
        }
        format!("video/mp4; codecs=\"{}\"", codecs.join(","))
    }

    /// Start ffmpeg remuxing an *input path* (already downloaded file) to
    /// fragmented MP4 on stdout. Container-only (`-c copy`): the codec bytes
    /// pass through untouched; the browser does all the decoding.
    ///
    /// `seek_sec` (if given) seeks into the input before remuxing, so a
    /// reconnect can resume the stream from an arbitrary time instead of
    /// starting at byte 0.
    pub async fn start_remux(
        input_path: &str,
        seek_sec: Option<f64>,
    ) -> Result<Child, std::io::Error> {
        info!("Starting remux: {} -> fragmented mp4", input_path);
        let mut cmd = Command::new("ffmpeg");
        cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error"]);
        if let Some(t) = seek_sec {
            if t > 0.0 {
                cmd.args(["-ss", &t.to_string()]);
            }
        }
        cmd.args(["-i", input_path])
            .args([
                "-map", "0:v:0?",
                "-map", "0:a:0?",
                "-c", "copy",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof",
                "-f", "mp4",
                "pipe:1",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
    }

    /// Like `start_remux` but reads the source from a progressive byte
    /// stream (stdin). This is how a still-downloading librqbit `FileStream`
    /// is fed: ffmpeg consumes exactly the bytes that exist, nothing more —
    /// if a piece isn't there yet nothing flows, there's no hang, no error.
    ///
    /// `seek_sec` does an input-side seek: on a non-seekable stdin ffmpeg
    /// reads-and-discards packets until the requested time, which costs
    /// bandwidth but is exact and keeps `-c copy` intact.
    pub async fn start_remux_from_stream(
        input_name: &str,
        seek_sec: Option<f64>,
    ) -> Result<Child, std::io::Error> {
        info!("Starting stream remux: {} -> fragmented mp4", input_name);
        let mut cmd = Command::new("ffmpeg");
        cmd.args(["-nostdin", "-hide_banner", "-loglevel", "error"]);
        if let Some(t) = seek_sec {
            if t > 0.0 {
                cmd.args(["-ss", &t.to_string()]);
            }
        }
        cmd.arg("-i").arg("pipe:0")
            .args([
                "-map", "0:v:0?",
                "-map", "0:a:0?",
                "-c", "copy",
                "-movflags", "frag_keyframe+empty_moov+default_base_moof",
                "-f", "mp4",
                "pipe:1",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
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

fn mse_audio_codec(codec: &str) -> String {
    match codec {
        "aac" => "mp4a.40.2".to_string(),
        "mp3" => "mp4a.6B".to_string(),
        "opus" => "opus".to_string(),
        "flac" => "fLaC".to_string(),
        "vorbis" | "vorb" => "mp4a.40.6".to_string(),
        "ac3" => "ac-3".to_string(),
        "eac3" => "ec-3".to_string(),
        _ => "mp4a.40.2".to_string(),
    }
}