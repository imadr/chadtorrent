use std::process::Stdio;
use tokio::process::{Child, Command};
use tracing::info;

pub struct Remuxer;

impl Remuxer {
    pub fn needs_remux(path: &str) -> bool {
        let lower = path.to_lowercase();
        lower.ends_with(".mkv")
            || lower.ends_with(".avi")
            || lower.ends_with(".wmv")
            || lower.ends_with(".flv")
            || lower.ends_with(".webm")
            || lower.ends_with(".mpg")
            || lower.ends_with(".mpeg")
            || lower.ends_with(".m4v")
            || lower.ends_with(".ts")
            || lower.ends_with(".vob")
    }

    pub async fn probe_needs_remux(path: &str) -> bool {
        let output = Command::new("ffprobe")
            .args([
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,codec_tag_string",
                "-of", "csv=p=0",
                path,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .await;

        match output {
            Ok(out) => {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let line = stdout.lines().next().unwrap_or("");
                let parts: Vec<&str> = line.split(',').collect();
                if parts.len() >= 2 {
                    let codec = parts[0].trim();
                    let tag = parts[1].trim();
                    // Check if the codec is H.264/HEVC but not in a container-tagged format
                    // Browser can play h264/avc1 natively via MSE, but needs container info
                    let is_h264 = codec == "h264";
                    let is_hevc = codec == "hevc" || codec == "h265";
                    let is_mpeg4 = codec == "mpeg4";

                    if is_h264 || is_hevc {
                        // If the tag is not an ISO base media file format tag,
                        // we likely need remux
                        return tag != "avc1" && tag != "hev1" && tag != "hvc1";
                    }
                    if is_mpeg4 {
                        return true;
                    }
                    false
                } else {
                    Self::needs_remux(path)
                }
            }
            Err(_) => Self::needs_remux(path),
        }
    }

    pub async fn start_remux(input_path: &str) -> Result<Child, std::io::Error> {
        info!("Starting remux: {} -> mp4", input_path);
        Command::new("ffmpeg")
            .args([
                "-i", input_path,
                "-c", "copy",
                "-movflags", "frag_keyframe+empty_moov+faststart",
                "-f", "mp4",
                "pipe:1",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
    }
}
