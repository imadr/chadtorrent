//! The watch-party relay: a dumb, stateless room broadcaster.
//!
//! Members of a party each hold ONE outbound WebSocket to the relay and
//! join a room identified by the (unguessable) room id. The relay
//! broadcasts every member frame to the other members of the same room —
//! nothing else. It never persists anything, never inspects payloads (they
//! are end-to-end encrypted; the relay could not read them even if it
//! wanted to), and rooms vanish the moment they are empty.
//!
//! The same router is mounted twice: as routes on every chadtorrent
//! instance (so an instance on a reachable/LAN host can act as the relay
//! for its own invites) and in the tiny standalone `chadtorrent-relay`
//! binary for internet-wide parties.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::{self, UnboundedSender};

/// Drop a connection that produced no traffic for this long. Members
/// heartbeat every few seconds, so this only fires for dead peers.
const IDLE_TIMEOUT: Duration = Duration::from_secs(75);
/// Watch parties are small; anything bigger is abuse of one room.
const MAX_ROOM_MEMBERS: usize = 32;
/// Matches the protocol envelope cap; oversized frames are dropped.
const MAX_FRAME: usize = 16 * 1024;

#[derive(Default)]
struct Relay {
    /// room id -> outbound senders of connected members.
    rooms: Mutex<HashMap<String, Vec<UnboundedSender<Message>>>>,
}

impl Relay {
    fn join(&self, room: &str, tx: UnboundedSender<Message>) -> bool {
        let mut rooms = self.rooms.lock().unwrap();
        let members = rooms.entry(room.to_string()).or_default();
        if members.len() >= MAX_ROOM_MEMBERS {
            return false;
        }
        members.push(tx);
        true
    }

    fn leave(&self, room: &str, tx: &UnboundedSender<Message>) {
        let mut rooms = self.rooms.lock().unwrap();
        if let Some(members) = rooms.get_mut(room) {
            members.retain(|m| !m.same_channel(tx));
            if members.is_empty() {
                rooms.remove(room);
            }
        }
    }

    fn broadcast(&self, room: &str, from: &UnboundedSender<Message>, msg: Message) {
        let rooms = self.rooms.lock().unwrap();
        if let Some(members) = rooms.get(room) {
            for m in members {
                if m.same_channel(from) {
                    continue;
                }
                let _ = m.send(msg.clone());
            }
        }
    }

    fn stats(&self) -> (usize, usize) {
        let rooms = self.rooms.lock().unwrap();
        let conns: usize = rooms.values().map(|v| v.len()).sum();
        (rooms.len(), conns)
    }
}

/// Router serving the relay. Mountable on any host (main app or the
/// standalone binary); stateless, so it can be merged anywhere.
pub fn relay_router() -> Router {
    let relay = Arc::new(Relay::default());
    Router::new()
        .route("/ws/party/relay/{room}", get(ws_relay))
        .route("/health", get(health))
        .with_state(relay)
}

async fn health(State(relay): State<Arc<Relay>>) -> impl IntoResponse {
    let (rooms, conns) = relay.stats();
    axum::Json(serde_json::json!({ "ok": true, "rooms": rooms, "connections": conns }))
}

async fn ws_relay(
    ws: WebSocketUpgrade,
    State(relay): State<Arc<Relay>>,
    Path(room): Path<String>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| relay_session(relay, room, socket))
}

async fn relay_session(relay: Arc<Relay>, room: String, socket: WebSocket) {
    // Guard the room id a little: it routes state in memory only, but no
    // reason to accept arbitrary junk.
    if room.is_empty() || room.len() > 64 {
        return;
    }
    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    if !relay.join(&room, tx.clone()) {
        let _ = sender.send(Message::Close(None)).await;
        return;
    }

    loop {
        tokio::select! {
            // Outgoing frame (from another member via broadcast).
            Some(msg) = rx.recv() => {
                if sender.send(msg).await.is_err() {
                    break;
                }
            }
            // Incoming frame from this member.
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        if t.len() <= MAX_FRAME {
                            relay.broadcast(&room, &tx, Message::Binary(t.as_bytes().to_vec().into()));
                        }
                    }
                    Some(Ok(Message::Binary(b))) => {
                        if b.len() <= MAX_FRAME {
                            relay.broadcast(&room, &tx, Message::Binary(b));
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = sender.send(Message::Pong(p)).await;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(Message::Pong(_))) => {}
                }
            }
            // No traffic in either direction for a long time.
            _ = tokio::time::sleep(IDLE_TIMEOUT) => break,
        }
    }

    relay.leave(&room, &tx);
}
