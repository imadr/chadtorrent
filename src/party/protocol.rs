//! Watch-party wire protocol: message types, the end-to-end crypto
//! envelope, and the invite codec.
//!
//! Every message that leaves an instance for the relay/LAN host is
//! encrypted with the party key (XChaCha20-Poly1305). The relay sees only
//! the room id (needed for routing) and opaque ciphertext — it can neither
//! read nor forge party traffic. Room id + key both live only in the
//! invite, so possessing the invite is the entire authorization.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::aead::{Aead, KeyInit, OsRng};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};

/// Prefix identifying a chadtorrent watch-party invite.
pub const INVITE_PREFIX: &str = "CTPARTY1:";

/// Max accepted envelope size (plaintext JSON is well under 1 KiB).
const MAX_ENVELOPE: usize = 16 * 1024;

// ---------------------------------------------------------------------------
// Messages

/// Per-member state piggybacked on Hello/Presence messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerState {
    pub id: String,
    pub name: String,
    /// Fully downloaded, or >= ~60s of playable media ahead of the party
    /// position (computed by the member's own player).
    #[serde(default)]
    pub ready: bool,
    /// Last known playback position (seconds).
    #[serde(default)]
    pub pos: f64,
    #[serde(default)]
    pub playing: bool,
    /// True while the member's player is stalled and the party should wait.
    #[serde(default)]
    pub buffering: bool,
}

/// A watch-party control message. Actions are last-writer-wins: whoever
/// acts, everyone follows. There is no host authority. Action messages
/// carry the acting member's peer id (`from`) because the relay forwards
/// opaque frames — receivers resolve names and roster state from it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// Sent once on (re)connect. Everyone who sees a Hello replies with a
    /// Presence so the newcomer learns the roster and current position.
    Hello { peer: PeerState },
    /// Heartbeat + roster update (every few seconds, and immediately on
    /// local state changes such as becoming ready or stalling).
    Presence { peer: PeerState },
    Play { from: String, t: f64 },
    Pause { from: String, t: f64 },
    Seek { from: String, t: f64 },
}

impl Message {
    pub fn peer_state(&self) -> Option<&PeerState> {
        match self {
            Message::Hello { peer } | Message::Presence { peer } => Some(peer),
            _ => None,
        }
    }

    pub fn action(&self) -> Option<(&'static str, &str, f64)> {
        match self {
            Message::Play { from, t } => Some(("play", from, *t)),
            Message::Pause { from, t } => Some(("pause", from, *t)),
            Message::Seek { from, t } => Some(("seek", from, *t)),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Crypto envelope

/// Frame layout: `[24-byte nonce][ciphertext + 16-byte Poly1305 tag]`.
pub fn seal(key: &[u8; 32], plaintext: &[u8]) -> Vec<u8> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
    let mut nonce_bytes = [0u8; 24];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = XNonce::from_slice(&nonce_bytes);
    let mut out = Vec::with_capacity(24 + plaintext.len() + 16);
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&cipher.encrypt(nonce, plaintext).expect("encrypt cannot fail"));
    out
}

/// Verify + decrypt a frame produced by [`seal`]. Returns `None` on any
/// malformation or authentication failure (wrong key, tampering).
pub fn open(key: &[u8; 32], frame: &[u8]) -> Option<Vec<u8>> {
    if frame.len() < 24 + 16 || frame.len() > MAX_ENVELOPE {
        return None;
    }
    let nonce = XNonce::from_slice(&frame[..24]);
    let cipher = XChaCha20Poly1305::new(Key::from_slice(key));
    cipher.decrypt(nonce, &frame[24..]).ok()
}

/// Serialize + encrypt a message for the wire.
pub fn encode_message(key: &[u8; 32], msg: &Message) -> Vec<u8> {
    let json = serde_json::to_vec(msg).expect("party message serializes");
    seal(key, &json)
}

/// Decrypt + parse a wire frame into a message.
pub fn decode_message(key: &[u8; 32], frame: &[u8]) -> Option<Message> {
    let plain = open(key, frame)?;
    serde_json::from_slice(&plain).ok()
}

pub fn generate_room_id() -> [u8; 16] {
    let mut b = [0u8; 16];
    OsRng.fill_bytes(&mut b);
    b
}

pub fn generate_key() -> [u8; 32] {
    let mut b = [0u8; 32];
    OsRng.fill_bytes(&mut b);
    b
}

pub fn random_peer_id() -> String {
    let mut b = [0u8; 8];
    OsRng.fill_bytes(&mut b);
    URL_SAFE_NO_PAD.encode(b)
}

/// Friendly random roster name for this instance ("swift-otter-42"),
/// generated once per process start.
pub fn random_member_name() -> String {
    const ADJ: &[&str] = &[
        "swift", "brave", "calm", "bright", "clever", "eager", "gentle", "happy",
        "jolly", "lucky", "mellow", "noble", "quiet", "rapid", "sunny", "tidy",
        "witty", "cosmic", "bold", "merry",
    ];
    const ANIMAL: &[&str] = &[
        "otter", "falcon", "panda", "tiger", "raven", "moose", "lynx", "corgi",
        "koala", "lemur", "gecko", "heron", "ibex", "jaguar", "manta", "ocelot",
        "puffin", "quokka", "badger", "ferret",
    ];
    let mut b = [0u8; 2];
    OsRng.fill_bytes(&mut b);
    let num = 10 + (b[0] as u16) % 90;
    format!(
        "{}-{}-{}",
        ADJ[b[0] as usize % ADJ.len()],
        ANIMAL[b[1] as usize % ANIMAL.len()],
        num
    )
}

// ---------------------------------------------------------------------------
// Invite

/// Everything a joiner needs: where to connect (relay), how to
/// authenticate (room + key), what to watch (magnet + metadata for nice
/// grouping in the joiner's library).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invite {
    /// Base ws(s):// URL of the relay (or another chadtorrent instance).
    pub relay: String,
    /// Room id, base64url (16 random bytes — unguessable).
    pub room: String,
    /// Party key, base64url (32 random bytes — E2E encryption).
    pub key: String,
    pub magnet: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tmdb_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub year: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poster_path: Option<String>,
    /// Display name of the party creator (roster convenience).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
}

impl Invite {
    pub fn room_bytes(&self) -> Option<[u8; 16]> {
        let b = URL_SAFE_NO_PAD.decode(&self.room).ok()?;
        b.try_into().ok()
    }

    pub fn key_bytes(&self) -> Option<[u8; 32]> {
        let b = URL_SAFE_NO_PAD.decode(&self.key).ok()?;
        b.try_into().ok()
    }
}

/// Render an invite into the paste-able `CTPARTY1:<base64url>` string.
pub fn encode_invite(invite: &Invite) -> String {
    let json = serde_json::to_vec(invite).expect("invite serializes");
    format!("{INVITE_PREFIX}{}", URL_SAFE_NO_PAD.encode(json))
}

/// Parse a paste-able invite string (whitespace tolerated).
pub fn parse_invite(s: &str) -> Option<Invite> {
    let s = s.trim();
    let b64 = s.strip_prefix(INVITE_PREFIX)?;
    let json = URL_SAFE_NO_PAD.decode(b64.trim()).ok()?;
    let invite: Invite = serde_json::from_slice(&json).ok()?;
    // Sanity: must have a usable relay, room and key, plus something to watch.
    if invite.relay.is_empty() || invite.magnet.is_empty() {
        return None;
    }
    invite.room_bytes()?;
    invite.key_bytes()?;
    Some(invite)
}

/// Extract the `xt=urn:btih:<infohash>` from a magnet link, lowercased —
/// used to dedupe on join so the same torrent is never added twice.
pub fn magnet_infohash(magnet: &str) -> Option<String> {
    let body = magnet.trim().strip_prefix("magnet:?").unwrap_or(magnet);
    for part in body.split('&') {
        if let Some(v) = part.strip_prefix("xt=") {
            let hash = v
                .trim_start_matches("urn:btih:")
                .trim_start_matches("urn:sha1:");
            if !hash.is_empty() {
                return Some(hash.to_lowercase());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_invite() -> Invite {
        Invite {
            relay: "ws://192.168.1.10:3000".into(),
            room: URL_SAFE_NO_PAD.encode(generate_room_id()),
            key: URL_SAFE_NO_PAD.encode(generate_key()),
            magnet: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Movie".into(),
            tmdb_id: Some(42),
            media_type: Some("movie".into()),
            title: Some("A Movie".into()),
            year: Some(1999),
            poster_path: Some("/abc.jpg".into()),
            host_name: Some("alice".into()),
        }
    }

    #[test]
    fn invite_roundtrip() {
        let invite = sample_invite();
        let s = encode_invite(&invite);
        assert!(s.starts_with("CTPARTY1:"));
        let back = parse_invite(&s).unwrap();
        assert_eq!(back.relay, invite.relay);
        assert_eq!(back.room, invite.room);
        assert_eq!(back.key, invite.key);
        assert_eq!(back.magnet, invite.magnet);
        assert_eq!(back.tmdb_id, Some(42));
        assert_eq!(back.title.as_deref(), Some("A Movie"));
    }

    #[test]
    fn invite_tolerates_whitespace() {
        let s = encode_invite(&sample_invite());
        let padded = format!("  {}\n", s);
        assert!(parse_invite(&padded).is_some());
    }

    #[test]
    fn invite_rejects_garbage() {
        assert!(parse_invite("hello").is_none());
        assert!(parse_invite("CTPARTY1:!!!notbase64!!!").is_none());
        // structurally valid base64 of invalid JSON
        let bad = URL_SAFE_NO_PAD.encode(br#"{"relay":"ws://x"}"#);
        assert!(parse_invite(&format!("CTPARTY1:{bad}")).is_none());
    }

    #[test]
    fn seal_open_roundtrip() {
        let key = generate_key();
        let msg = Message::Play {
            from: "abcd".into(),
            t: 1234.5,
        };
        let frame = encode_message(&key, &msg);
        let back = decode_message(&key, &frame).unwrap();
        assert!(matches!(back, Message::Play { ref from, t } if from == "abcd" && (t - 1234.5).abs() < 1e-9));
    }

    #[test]
    fn seal_rejects_wrong_key_and_tampering() {
        let key = generate_key();
        let frame = encode_message(&key, &Message::Pause { from: "x".into(), t: 1.0 });
        assert!(decode_message(&generate_key(), &frame).is_none());
        let mut tampered = frame.clone();
        let last = tampered.len() - 1;
        tampered[last] ^= 0x01;
        assert!(decode_message(&key, &tampered).is_none());
    }

    #[test]
    fn open_rejects_short_frames() {
        let key = generate_key();
        assert!(open(&key, &[]).is_none());
        assert!(open(&key, &[0u8; 10]).is_none());
    }

    #[test]
    fn message_json_tags() {
        let m = Message::Seek {
            from: "p1".into(),
            t: 9.0,
        };
        let j = serde_json::to_string(&m).unwrap();
        assert!(j.contains(r#""type":"seek""#));
        let back: Message = serde_json::from_str(&j).unwrap();
        assert!(matches!(back, Message::Seek { ref from, t } if from == "p1" && t == 9.0));
    }

    #[test]
    fn member_name_shape() {
        let n = random_member_name();
        let parts: Vec<&str> = n.split('-').collect();
        assert_eq!(parts.len(), 3, "adjective-animal-number: {n}");
        assert!(parts[2].parse::<u16>().is_ok(), "numeric suffix: {n}");
    }

    #[test]
    fn infohash_extraction() {
        assert_eq!(
            magnet_infohash("magnet:?xt=urn:btih:ABCDEF0123&dn=x"),
            Some("abcdef0123".to_string())
        );
        assert_eq!(
            magnet_infohash("magnet:?dn=x&xt=urn:btih:0123456789abcdef0123456789abcdef01234567"),
            Some("0123456789abcdef0123456789abcdef01234567".to_string())
        );
        assert_eq!(magnet_infohash("magnet:?dn=only-name"), None);
        assert_eq!(magnet_infohash(""), None);
    }
}
