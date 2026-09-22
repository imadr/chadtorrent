//! Watch-party client: the member-facing half of the feature.
//!
//! An instance is in at most one party. `PartyManager` owns the outbound
//! connection to the party's relay/LAN host, maintains the roster from
//! presence heartbeats, persists the party in SQLite (auto-rejoin on
//! restart) and exposes a small REST/WS surface for the browser. The
//! browser owns the actual `<video>` element: it reports its local state
//! (position, playing, ready, buffering) and applies remote actions —
//! the server is only a ferry.

pub mod punch;
pub mod protocol;
pub mod relay;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

use crate::config;
use crate::db::PartyRow;
use crate::error::{Error, Result};
use crate::library::LibraryManager;

use protocol::{Invite, Message, PeerState};

/// Presence heartbeat cadence — also the relay's keep-alive traffic.
const HEARTBEAT: Duration = Duration::from_secs(4);
/// Drop remote peers we haven't heard from for this long.
const PEER_TIMEOUT: Duration = Duration::from_secs(15);

/// Command from the manager to the connection task.
enum ClientCmd {
    Send(Message),
    Stop,
}

struct Peer {
    state: PeerState,
    last_seen: Instant,
}

struct ActiveParty {
    invite: Invite,
    item_id: i64,
    /// True when the invite's relay is this instance's own LAN address
    /// (no external relay configured) — the UI warns "same network only".
    lan_only: bool,
    me: PeerState,
    peers: HashMap<String, Peer>,
    out: UnboundedSender<ClientCmd>,
    conn: String,
    /// NAT-punch transport state, set by the connection task once the
    /// punch probe against the relay completes (or not).
    punch: std::sync::Mutex<Option<Arc<punch::PunchActive>>>,
}

#[derive(Default)]
struct Inner {
    party: Option<ActiveParty>,
}

pub struct PartyManager {
    library: Arc<LibraryManager>,
    events: broadcast::Sender<String>,
    inner: Arc<Mutex<Inner>>,
    /// Bumped on every activate/leave; background tasks exit when their
    /// generation goes stale.
    gen: Arc<AtomicU64>,
    /// Roster name for this instance, generated once at startup.
    default_name: String,
}

impl PartyManager {
    pub fn new(library: Arc<LibraryManager>) -> Arc<Self> {
        let (events, _) = broadcast::channel(256);
        let mgr = Arc::new(Self {
            library,
            events,
            inner: Arc::new(Mutex::new(Inner::default())),
            gen: Arc::new(AtomicU64::new(0)),
            default_name: protocol::random_member_name(),
        });
        mgr.recover_persisted_party();
        mgr
    }

    /// This instance's auto-generated roster name.
    pub fn default_name(&self) -> &str {
        &self.default_name
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.events.subscribe()
    }

    /// Rejoin the party persisted in SQLite after a restart. The item must
    /// still exist; if it was deleted while we were down, drop the row.
    fn recover_persisted_party(self: &Arc<Self>) {
        let Ok(Some(row)) = self.library.db.get_party() else {
            return;
        };
        match self.library.db.get_item(row.item_id) {
            Ok(Some(item)) if !item.magnet.trim().is_empty() => {
                let invite = Invite {
                    relay: row.relay,
                    room: row.room,
                    key: row.room_key,
                    magnet: item.magnet,
                    tmdb_id: item.tmdb_id,
                    media_type: item.media_type,
                    title: item.title,
                    year: item.year,
                    poster_path: item.poster_path,
                    host_name: None,
                };
                info!("Rejoining persisted watch party for item {}", row.item_id);
                self.activate(invite, row.item_id, row.name, false);
            }
            _ => {
                let _ = self.library.db.clear_party();
            }
        }
    }

    /// Create a party around a library item. Returns the invite string and
    /// whether it points at this instance's LAN address (no external
    /// relay configured — parties work on the local network only).
    pub fn create(
        self: &Arc<Self>,
        item_id: i64,
        name: &str,
        relay_override: Option<String>,
    ) -> Result<(String, bool)> {
        let item = self
            .library
            .db
            .get_item(item_id)?
            .ok_or_else(|| Error::NotFound(format!("item {item_id} not found")))?;
        if item.magnet.trim().is_empty() {
            return Err(Error::BadRequest(
                "item has no torrent attached".to_string(),
            ));
        }
        let (relay, lan_only) = if let Some(r) = relay_override.filter(|s| !s.trim().is_empty()) {
            (r.trim().to_string(), false)
        } else if let Some(r) = self.library.config.party_relay.as_deref() {
            (r.trim().to_string(), false)
        } else {
            (config::lan_ws_base_url(self.library.config.port), true)
        };
        let invite = Invite {
            relay,
            room: URL_SAFE_NO_PAD.encode(protocol::generate_room_id()),
            key: URL_SAFE_NO_PAD.encode(protocol::generate_key()),
            magnet: item.magnet,
            tmdb_id: item.tmdb_id,
            media_type: item.media_type,
            title: item.title,
            year: item.year,
            poster_path: item.poster_path,
            host_name: Some(name.to_string()),
        };
        let invite_str = protocol::encode_invite(&invite);
        self.activate(invite, item_id, name.to_string(), lan_only);
        Ok((invite_str, lan_only))
    }

    /// Join an existing party (invite already parsed and the torrent
    /// ensured to exist in the library by the caller).
    pub fn join(self: &Arc<Self>, invite: Invite, name: &str, item_id: i64) -> Result<()> {
        if invite.key_bytes().is_none() || invite.room_bytes().is_none() {
            return Err(Error::BadRequest("invalid invite".to_string()));
        }
        self.activate(invite, item_id, name.to_string(), false);
        Ok(())
    }

    pub fn leave(&self) -> Result<()> {
        self.gen.fetch_add(1, Ordering::SeqCst);
        if let Some(p) = self.inner.lock().unwrap().party.take() {
            let _ = p.out.send(ClientCmd::Stop);
        }
        self.library.db.clear_party()?;
        self.emit_snapshot();
        Ok(())
    }

    fn activate(self: &Arc<Self>, invite: Invite, item_id: i64, name: String, lan_only: bool) {
        let gen = self.gen.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(p) = self.inner.lock().unwrap().party.as_ref() {
            let _ = p.out.send(ClientCmd::Stop);
        }

        let me = PeerState {
            id: protocol::random_peer_id(),
            name: name.clone(),
            ready: false,
            pos: 0.0,
            playing: false,
            buffering: false,
        };
        let (tx, rx) = mpsc::unbounded_channel::<ClientCmd>();
        let (in_tx, in_rx) = mpsc::unbounded_channel::<Message>();
        self.inner.lock().unwrap().party = Some(ActiveParty {
            invite: invite.clone(),
            item_id,
            lan_only,
            me: me.clone(),
            peers: HashMap::new(),
            out: tx,
            conn: "connecting".to_string(),
            punch: std::sync::Mutex::new(None),
        });

        let row = PartyRow {
            relay: invite.relay.clone(),
            room: invite.room.clone(),
            room_key: invite.key.clone(),
            magnet: invite.magnet.clone(),
            tmdb_id: invite.tmdb_id,
            media_type: invite.media_type.clone(),
            title: invite.title.clone(),
            year: invite.year,
            poster_path: invite.poster_path.clone(),
            item_id,
            name,
        };
        if let Err(e) = self.library.db.set_party(&row) {
            warn!("Failed to persist party: {e}");
        }

        info!("Watch party activated for item {item_id} (gen {gen})");
        tokio::spawn(run_client(
            self.clone(),
            gen,
            self.gen.clone(),
            invite,
            me.id,
            rx,
            in_tx,
        ));        tokio::spawn(run_inbound(self.clone(), gen, in_rx));
        tokio::spawn(run_housekeeping(self.clone(), gen));
        self.emit_snapshot();
    }

    /// Run `f` on the active party if `my_gen` is still current.
    fn with_party<T>(&self, my_gen: u64, f: impl FnOnce(&mut ActiveParty) -> T) -> Option<T> {
        if self.gen.load(Ordering::SeqCst) != my_gen {
            return None;
        }
        self.inner.lock().unwrap().party.as_mut().map(f)
    }

    fn me_snapshot(&self, my_gen: u64) -> Option<PeerState> {
        self.with_party(my_gen, |p| p.me.clone())
    }

    fn send_raw(&self, my_gen: u64, msg: Message) {
        if let Some(out) = self.with_party(my_gen, |p| p.out.clone()) {
            let _ = out.send(ClientCmd::Send(msg));
        }
    }

    fn set_conn(&self, my_gen: u64, status: &str) {
        let changed = self
            .with_party(my_gen, |p| {
                let changed = p.conn != status;
                p.conn = status.to_string();
                changed
            })
            .unwrap_or(false);
        if changed {
            self.emit_snapshot();
        }
    }

    /// Forget all remote peers (on reconnect they re-announce via Hello).
    fn clear_peers(&self, my_gen: u64) {
        let cleared = self
            .with_party(my_gen, |p| {
                let n = p.peers.len();
                p.peers.clear();
                n
            })
            .unwrap_or(0);
        if cleared > 0 {
            self.emit_snapshot();
        }
    }

    /// Local browser sent an action (it has already applied it locally).
    pub fn send_action(&self, kind: &str, t: f64) {
        let mut inner = self.inner.lock().unwrap();
        let Some(p) = inner.party.as_mut() else { return };
        p.me.pos = t;
        p.me.playing = kind == "play";
        p.me.buffering = false;
        let msg = match kind {
            "play" => Message::Play {
                from: p.me.id.clone(),
                t,
            },
            "pause" => Message::Pause {
                from: p.me.id.clone(),
                t,
            },
            "seek" => Message::Seek {
                from: p.me.id.clone(),
                t,
            },
            _ => return,
        };
        let out = p.out.clone();
        drop(inner);
        let _ = out.send(ClientCmd::Send(msg));
    }

    /// Local browser reported its player state. Changes worth showing in
    /// the roster (ready/buffering/playing) trigger an immediate presence
    /// broadcast so peers react without waiting for the heartbeat.
    pub fn set_local_state(&self, ready: bool, pos: f64, playing: bool, buffering: bool) {
        let notify = {
            let mut inner = self.inner.lock().unwrap();
            let Some(p) = inner.party.as_mut() else { return };
            let changed =
                p.me.ready != ready || p.me.buffering != buffering || p.me.playing != playing;
            p.me.ready = ready;
            p.me.pos = pos;
            p.me.playing = playing;
            p.me.buffering = buffering;
            if changed {
                let msg = Message::Presence {
                    peer: p.me.clone(),
                };
                let out = p.out.clone();
                drop(inner);
                let _ = out.send(ClientCmd::Send(msg));
            }
            changed
        };
        if notify {
            self.emit_snapshot();
        }
    }

    pub fn snapshot_json(&self) -> serde_json::Value {
        let inner = self.inner.lock().unwrap();
        let Some(p) = inner.party.as_ref() else {
            return serde_json::json!({ "type": "party", "party": null });
        };
        let peers: Vec<serde_json::Value> = p
            .peers
            .values()
            .map(|peer| peer_json(&peer.state))
            .collect();
        let udp_punched = p
            .punch
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|a| a.active.load(Ordering::SeqCst));
        serde_json::json!({
            "type": "party",
            "party": {
                "item_id": p.item_id,
                "invite": protocol::encode_invite(&p.invite),
                "relay": p.invite.relay,
                "lan_only": p.lan_only,
                "conn": p.conn,
                "transport": if udp_punched { "udp" } else { "relay" },
                "title": p.invite.title,
                "me": peer_json(&p.me),
                "peers": peers,
            }
        })
    }

    fn emit_snapshot(&self) {
        let _ = self.events.send(self.snapshot_json().to_string());
    }

    /// Update roster from a remote Presence/Hello. Returns whether
    /// anything visible changed (drives snapshot emission).
    fn update_peer(&self, peer: PeerState) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(p) = inner.party.as_mut() else { return false };
        match p.peers.get_mut(&peer.id) {
            Some(existing) => {
                let changed = existing.state.ready != peer.ready
                    || existing.state.buffering != peer.buffering
                    || existing.state.playing != peer.playing
                    || existing.state.name != peer.name
                    || (existing.state.pos - peer.pos).abs() > 0.25;
                existing.state = peer;
                existing.last_seen = Instant::now();
                changed
            }
            None => {
                p.peers.insert(
                    peer.id.clone(),
                    Peer {
                        state: peer,
                        last_seen: Instant::now(),
                    },
                );
                true
            }
        }
    }

    fn peer_name(&self, id: &str) -> String {
        let inner = self.inner.lock().unwrap();
        if let Some(p) = inner.party.as_ref() {
            if p.me.id == id {
                return p.me.name.clone();
            }
            if let Some(peer) = p.peers.get(id) {
                return peer.state.name.clone();
            }
        }
        id.chars().take(6).collect()
    }

    /// Fold a remote action into the roster so positions stay fresh.
    fn apply_remote_action(&self, from: &str, kind: &str, t: f64) {
        let mut inner = self.inner.lock().unwrap();
        let Some(p) = inner.party.as_mut() else { return };
        if let Some(peer) = p.peers.get_mut(from) {
            peer.state.pos = t;
            match kind {
                "play" => peer.state.playing = true,
                "pause" => peer.state.playing = false,
                _ => {}
            }
        }
    }
}

fn peer_json(p: &PeerState) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "name": p.name,
        "ready": p.ready,
        "pos": p.pos,
        "playing": p.playing,
        "buffering": p.buffering,
    })
}

/// The relay connection: one outbound WebSocket, auto-reconnecting with
/// backoff. Encrypts outgoing frames, decrypts incoming ones, and shuttles
/// both ways between the manager and the wire.
async fn run_client(
    mgr: Arc<PartyManager>,
    my_gen: u64,
    gen: Arc<AtomicU64>,
    invite: Invite,
    peer_id: String,
    mut cmd_rx: UnboundedReceiver<ClientCmd>,
    inbound: UnboundedSender<Message>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message as WsMessage;

    let url = protocol::room_ws_url(&invite.relay, &invite.room);
    let key = match invite.key_bytes() {
        Some(k) => k,
        None => return,
    };
    // NAT punch probe against the same host, in parallel with the WS
    // relay connection. Pure optimization: while it hasn't succeeded,
    // all traffic keeps flowing through the WS.
    let punch_task = punch::PunchTask::spawn(
        &invite.relay,
        &invite.room,
        key,
        peer_id.clone(),
        inbound.clone(),
    )
    .await;
    if let Some(t) = &punch_task {
        let active = t.active_handle();
        mgr.with_party(my_gen, |p| {
            *p.punch.lock().unwrap() = Some(active);
        });
    }
    let mut backoff: u64 = 1;

    loop {
        if gen.load(Ordering::SeqCst) != my_gen {
            return;
        }
        mgr.set_conn(my_gen, if backoff == 1 { "connecting" } else { "reconnecting" });
        let connected = tokio_tungstenite::connect_async(url.as_str()).await;
        let ws = match connected {
            Ok((ws, _)) => ws,
            Err(e) => {
                warn!("Party relay connect failed ({url}): {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                backoff = (backoff * 2).min(30);
                continue;
            }
        };
        backoff = 1;
        mgr.set_conn(my_gen, "online");
        mgr.clear_peers(my_gen);

        let (mut sink, mut stream) = ws.split();
        // Announce ourselves; members reply with Presence so the roster
        // populates immediately.
        if let Some(me) = mgr.me_snapshot(my_gen) {
            let hello = Message::Hello { peer: me };
            let frame = protocol::encode_message(&key, &hello);
            if sink.send(WsMessage::Binary(frame)).await.is_err() {
                continue;
            }
        }
        let _ = peer_id;

        loop {
            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(ClientCmd::Send(m)) => {
                            if gen.load(Ordering::SeqCst) != my_gen {
                                let _ = sink.close().await;
                                return;
                            }
                            let frame = protocol::encode_message(&key, &m);
                            // Punched peers get frames peer-to-peer; the WS
                            // relay is the fallback (and the pre-punch path).
                            if punch_task
                                .as_ref()
                                .is_some_and(|t| t.is_active())
                            {
                                punch_task.as_ref().unwrap().broadcast(frame);
                            } else if sink.send(WsMessage::Binary(frame)).await.is_err() {
                                break;
                            }
                        }
                        Some(ClientCmd::Stop) | None => {
                            let _ = sink.close().await;
                            return;
                        }
                    }
                }
                msg = stream.next() => {
                    match msg {
                        Some(Ok(WsMessage::Binary(b))) => {
                            if let Some(m) = protocol::decode_message(&key, &b) {
                                if inbound.send(m).is_err() {
                                    return;
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) | None => break,
                    }
                }
            }
        }
        // Connection lost — the outer loop reconnects.
        mgr.set_conn(my_gen, "reconnecting");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Process decrypted messages from the party: maintain the roster, reply
/// to Hello, forward actions to the browser.
async fn run_inbound(mgr: Arc<PartyManager>, my_gen: u64, mut rx: UnboundedReceiver<Message>) {
    while let Some(msg) = rx.recv().await {
        if mgr.gen.load(Ordering::SeqCst) != my_gen {
            return;
        }
        if let Some(peer) = msg.peer_state().cloned() {
            let is_hello = matches!(msg, Message::Hello { .. });
            let changed = mgr.update_peer(peer.clone());
            if is_hello {
                // New member: answer with our presence right away so their
                // roster (and the party position) fills in instantly.
                if let Some(me) = mgr.me_snapshot(my_gen) {
                    mgr.send_raw(my_gen, Message::Presence { peer: me });
                }
            }
            if changed || is_hello {
                mgr.emit_snapshot();
            }
        } else if let Some((kind, from, t)) = msg.action() {
            let by = mgr.peer_name(from);
            mgr.apply_remote_action(from, kind, t);
            mgr.emit_snapshot();
            let ev = serde_json::json!({ "type": "action", "kind": kind, "t": t, "by": by });
            let _ = mgr.events.send(ev.to_string());
        }
    }
}

/// Heartbeat presence + stale-peer garbage collection.
async fn run_housekeeping(mgr: Arc<PartyManager>, my_gen: u64) {
    let mut tick = tokio::time::interval(HEARTBEAT);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if mgr.gen.load(Ordering::SeqCst) != my_gen {
            return;
        }
        if let Some(me) = mgr.me_snapshot(my_gen) {
            mgr.send_raw(my_gen, Message::Presence { peer: me });
        }
        let removed = {
            let mut inner = mgr.inner.lock().unwrap();
            match inner.party.as_mut() {
                Some(p) => {
                    let before = p.peers.len();
                    p.peers.retain(|_, peer| peer.last_seen.elapsed() < PEER_TIMEOUT);
                    before != p.peers.len()
                }
                None => return,
            }
        };
        if removed {
            mgr.emit_snapshot();
        }
    }
}

// Re-exported for the HTTP layer's join handler.
pub use protocol::{magnet_infohash, parse_invite};
