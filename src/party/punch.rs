//! NAT punch: UDP mailroom server + peer-to-peer client transport.
//!
//! The punch server (the standalone `chadtorrent-relay` binary, on a
//! reachable host) does nothing but introductions over one UDP port:
//! members send a `bind`, the server replies with their observed public
//! address plus the other members' addresses, then it steps out. Members
//! punch directly to each other (simultaneous-open over UDP) and sync
//! messages flow peer-to-peer.
//!
//! Punching fails for symmetric-NAT/CGNAT pairs, so this is strictly an
//! optimization on top of the WebSocket relay: the party always works
//! through the relay, and switches to UDP only when every member punched
//! successfully. All payloads stay end-to-end encrypted in both modes —
//! punch frames are authenticated with the party key, so an attacker who
//! finds the port still cannot inject anything.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

use super::protocol::{self, Message};

/// UDP frame magic.
const MAGIC: &[u8; 4] = b"CTP1";

// Frame types.
const F_REQ: u8 = 1; // punch request, payload = sealed PunchWho
const F_OK: u8 = 2; // punch confirmed, payload = sealed PunchWho
const F_DATA: u8 = 3; // payload = sealed party-message envelope
const F_ACK: u8 = 4; // payload = u32 BE cumulative ack
const F_PING: u8 = 5;
const F_PONG: u8 = 6;

const TICK: Duration = Duration::from_millis(100);
const PUNCH_EVERY: Duration = Duration::from_millis(250);
const PUNCH_GIVE_UP: Duration = Duration::from_secs(12);
const RETRANSMIT_EVERY: Duration = Duration::from_millis(400);
const SEND_WINDOW: usize = 64;
const PING_EVERY: Duration = Duration::from_secs(8);
/// No frames from a punched peer for this long => dead => relay fallback.
const DEAD_AFTER: Duration = Duration::from_secs(15);
const REBIND_EVERY: Duration = Duration::from_secs(15);

// ---------------------------------------------------------------------------
// Wire structs (JSON; tiny and rare)

/// Client -> server: register my UDP mapping for a room.
#[derive(Serialize, Deserialize)]
struct Bind {
    room: String,
    peer_id: String,
}

/// Server -> client: your observed address + the other members'.
#[derive(Serialize, Deserialize)]
struct Bound {
    #[allow(dead_code)]
    addr: String,
    peers: Vec<BoundPeer>,
}

#[derive(Serialize, Deserialize, Clone)]
struct BoundPeer {
    id: String,
    addr: String,
}

/// Sealed punch request/ok payload.
#[derive(Serialize, Deserialize)]
struct PunchWho {
    peer_id: String,
}

// ---------------------------------------------------------------------------
// Server: the UDP mailroom

/// Run the punch hub on an already-bound UDP socket. Room membership here
/// is independent of the WebSocket relay rooms (same ids, separate tables).
/// Used by the standalone `chadtorrent-relay` binary (and tests); the main
/// app deliberately does not host a punch hub (LAN parties don't need one).
#[allow(dead_code)]
pub async fn run_punch_server(socket: UdpSocket) {
    let mut buf = [0u8; 1024];
    let mut rooms: HashMap<String, HashMap<String, (SocketAddr, Instant)>> = HashMap::new();
    info!(
        "Punch hub on udp://{}",
        socket
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_default()
    );

    loop {
        let Ok((n, from)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let Ok(bind) = serde_json::from_slice::<Bind>(&buf[..n]) else {
            continue;
        };
        if bind.room.is_empty() || bind.room.len() > 64 || bind.peer_id.is_empty() {
            continue;
        }
        let members = rooms.entry(bind.room).or_default();
        members.retain(|_, (addr, seen)| seen.elapsed() < Duration::from_secs(45) || *addr != from);
        members.insert(bind.peer_id.clone(), (from, Instant::now()));

        // Reply to the binder: their mapping + everyone else.
        let others: Vec<BoundPeer> = members
            .iter()
            .filter(|(id, _)| *id != &bind.peer_id)
            .map(|(id, (addr, _))| BoundPeer {
                id: id.clone(),
                addr: addr.to_string(),
            })
            .collect();
        let reply = serde_json::to_vec(&Bound {
            addr: from.to_string(),
            peers: others,
        })
        .unwrap_or_default();
        let _ = socket.send_to(&reply, from).await;

        // Push the updated list to the other members.
        let all: Vec<BoundPeer> = members
            .iter()
            .map(|(id, (addr, _))| BoundPeer {
                id: id.clone(),
                addr: addr.to_string(),
            })
            .collect();
        for (id, (addr, _)) in members.iter() {
            if *id == bind.peer_id {
                continue;
            }
            let peers: Vec<BoundPeer> = all.iter().filter(|p| p.id != *id).cloned().collect();
            let push = serde_json::to_vec(&Bound {
                addr: addr.to_string(),
                peers,
            })
            .unwrap_or_default();
            let _ = socket.send_to(&push, *addr).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Client: punch task + UDP transport with reliability

/// Shared transport state the PartyManager reads.
#[derive(Debug)]
pub struct PunchActive {
    /// True when every known peer is punched and UDP is the live transport.
    pub active: AtomicBool,
}

/// Handle held by the party connection. Cheap to create: the bind probe
/// happens in the background task, so a relay without punch support costs
/// nothing (the task exits after `BIND_TIMEOUT`).
pub struct PunchTask {
    active: Arc<PunchActive>,
    /// Encrypted envelopes to broadcast to all punched peers.
    send: UnboundedSender<Vec<u8>>,
    cancel: Arc<AtomicBool>,
}

impl PunchTask {
    /// Returns immediately; all network probing happens in the spawned
    /// task so a punch-less relay never delays the WS connection.
    pub async fn spawn(
        relay_base: &str,
        room: &str,
        key: [u8; 32],
        my_peer_id: String,
        inbound: UnboundedSender<Message>,
    ) -> Option<Self> {
        let host_port = relay_base
            .trim()
            .trim_start_matches("wss://")
            .trim_start_matches("ws://")
            .trim_end_matches('/');
        let server: SocketAddr = tokio::net::lookup_host(host_port).await.ok()?.next()?;
        let socket = UdpSocket::bind("0.0.0.0:0").await.ok()?;

        let active = Arc::new(PunchActive {
            active: AtomicBool::new(false),
        });
        let (send, recv) = mpsc::unbounded_channel::<Vec<u8>>();
        let cancel = Arc::new(AtomicBool::new(false));
        tokio::spawn(run_client(
            socket,
            server,
            room.to_string(),
            key,
            my_peer_id,
            inbound,
            recv,
            active.clone(),
            cancel.clone(),
        ));
        Some(Self {
            active,
            send,
            cancel,
        })
    }

    pub fn is_active(&self) -> bool {
        self.active.active.load(Ordering::SeqCst)
    }

    pub fn active_handle(&self) -> Arc<PunchActive> {
        self.active.clone()
    }

    pub fn broadcast(&self, envelope: Vec<u8>) {
        let _ = self.send.send(envelope);
    }
}

impl Drop for PunchTask {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::SeqCst);
    }
}

struct PeerConn {
    peer_id: String,
    addr: SocketAddr,
    punched: bool,
    my_seq: u32,
    /// (seq, frame, first_sent, last_sent)
    unacked: VecDeque<(u32, Vec<u8>, Instant, Instant)>,
    /// Next in-order seq we expect (seqs start at 1).
    recv_next: u32,
    /// Out-of-order frames awaiting their predecessor.
    hold: HashMap<u32, Vec<u8>>,
    last_seen: Instant,
    last_ping: Instant,
    last_req: Instant,
}

#[allow(clippy::too_many_arguments)]
async fn run_client(
    socket: UdpSocket,
    server: SocketAddr,
    room: String,
    key: [u8; 32],
    my_peer_id: String,
    inbound: UnboundedSender<Message>,
    mut outbox: UnboundedReceiver<Vec<u8>>,
    active: Arc<PunchActive>,
    cancel: Arc<AtomicBool>,
) {
    let mut targets: Vec<BoundPeer> = Vec::new();
    let mut conns: HashMap<SocketAddr, PeerConn> = HashMap::new();
    let mut last_bind = Instant::now() - REBIND_EVERY;
    let punch_started = Instant::now();
    let mut punch_warned = false;
    let mut known_targets = 0usize;
    let mut buf = [0u8; 2048];
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if cancel.load(Ordering::SeqCst) {
            return;
        }
        tokio::select! {
            r = socket.recv_from(&mut buf) => {
                let Ok((n, from)) = r else { continue };
                let frame = &buf[..n];
                if frame.first() == Some(&b'{') {
                    // Server push (JSON Bound).
                    if let Ok(b) = serde_json::from_slice::<Bound>(frame) {
                        apply_bound(&mut targets, &mut conns, b);
                    }
                    continue;
                }
                handle_frame(
                    &socket, &key, &my_peer_id, &inbound, &mut conns,
                    from, frame, Instant::now(),
                )
                .await;
            }
            Some(env) = outbox.recv() => {
                if active.active.load(Ordering::SeqCst) {
                    broadcast(&socket, &mut conns, env).await;
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if cancel.load(Ordering::SeqCst) {
                    return;
                }
                if last_bind.elapsed() >= REBIND_EVERY {
                    last_bind = now;
                    let bind = serde_json::to_vec(&Bind {
                        room: room.clone(),
                        peer_id: my_peer_id.clone(),
                    })
                    .unwrap_or_default();
                    let _ = socket.send_to(&bind, server).await;
                }

                for peer in &targets {
                    let Ok(addr) = peer.addr.parse::<SocketAddr>() else { continue };
                    let conn = conns
                        .entry(addr)
                        .or_insert_with(|| new_conn(peer.id.clone(), addr, now));
                    if !conn.punched {
                        if now - conn.last_req >= PUNCH_EVERY {
                            conn.last_req = now;
                            send_frame(&socket, addr, &frame_req(&key, &my_peer_id));
                        }
                    } else {
                        if now - conn.last_ping >= PING_EVERY {
                            conn.last_ping = now;
                            conn.my_seq = conn.my_seq.wrapping_add(1);
                            send_frame(&socket, addr, &build_frame(F_PING, conn.my_seq, &[]));
                        }
                        retransmit(&socket, conn, now).await;
                    }
                }

                // New member mid-party: back to the relay for everyone
                // until the newcomer has punched too (the completion check
                // below re-activates UDP).
                if targets.len() != known_targets {
                    known_targets = targets.len();
                    if active.active.load(Ordering::SeqCst) {
                        warn!("new party member — re-punching, on the relay meanwhile");
                        active.active.store(false, Ordering::SeqCst);
                    }
                }

                // Punch outcome.
                if !targets.is_empty() && !active.active.load(Ordering::SeqCst) {
                    let punched = conns.values().filter(|c| c.punched).count();
                    if punched == targets.len() {
                        info!("punched {punched} peer(s) — peer-to-peer sync");
                        active.active.store(true, Ordering::SeqCst);
                    } else if punch_started.elapsed() > PUNCH_GIVE_UP && !punch_warned {
                        punch_warned = true;
                        warn!(
                            "punch incomplete ({punched}/{} peers) — staying on the relay",
                            targets.len()
                        );
                    }
                }

                // Liveness: a dead punched peer drops everyone back to the relay.
                if active.active.load(Ordering::SeqCst) {
                    let dead = conns
                        .values()
                        .any(|c| c.punched && now - c.last_seen > DEAD_AFTER);
                    if dead {
                        warn!("punch peer lost — falling back to the relay");
                        active.active.store(false, Ordering::SeqCst);
                    }
                }
            }
        }
    }
}

fn new_conn(peer_id: String, addr: SocketAddr, now: Instant) -> PeerConn {
    PeerConn {
        peer_id,
        addr,
        punched: false,
        my_seq: 0,
        unacked: VecDeque::new(),
        recv_next: 1,
        hold: HashMap::new(),
        last_seen: now,
        last_ping: now,
        last_req: now - PUNCH_EVERY, // fire immediately
    }
}

fn apply_bound(
    targets: &mut Vec<BoundPeer>,
    conns: &mut HashMap<SocketAddr, PeerConn>,
    b: Bound,
) {
    *targets = b.peers;
    let listed: HashSet<String> = targets.iter().map(|p| p.id.clone()).collect();
    conns.retain(|_, c| listed.contains(&c.peer_id));
}

#[allow(clippy::too_many_arguments)]
async fn handle_frame(
    socket: &UdpSocket,
    key: &[u8; 32],
    my_peer_id: &str,
    inbound: &UnboundedSender<Message>,
    conns: &mut HashMap<SocketAddr, PeerConn>,
    from: SocketAddr,
    frame: &[u8],
    now: Instant,
) {
    if frame.len() < 9 || &frame[..4] != MAGIC {
        return;
    }
    let ftype = frame[4];
    let seq = u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]);
    let payload = &frame[9..];

    match ftype {
        F_REQ | F_OK => {
            // Authenticated introduction: only party members can produce it.
            let Some(plain) = protocol::open(key, payload) else {
                return;
            };
            let Ok(who) = serde_json::from_slice::<PunchWho>(&plain) else {
                return;
            };
            if who.peer_id == my_peer_id {
                return;
            }
            // One conn per peer id; follow it if its address changed.
            if let Some(old) = conns
                .iter()
                .find(|(_, c)| c.peer_id == who.peer_id)
                .map(|(a, _)| *a)
            {
                if old != from {
                    if let Some(mut c) = conns.remove(&old) {
                        c.addr = from;
                        c.last_seen = now;
                        conns.insert(from, c);
                    }
                }
            }
            let conn = conns
                .entry(from)
                .or_insert_with(|| new_conn(who.peer_id.clone(), from, now));
            conn.last_seen = now;
            if ftype == F_REQ {
                // Their packets reach us; confirm so both sides can switch.
                send_frame(socket, from, &frame_ok(key, my_peer_id));
            } else {
                // OK received: us->them proven (they answered our REQ) and
                // them->us proven (this frame). Fully punched.
                conn.punched = true;
            }
        }
        F_DATA => {
            let Some(conn) = conns.get_mut(&from) else { return };
            if !conn.punched {
                return;
            }
            // Authenticate before touching reliability state.
            let Some(plain) = protocol::open(key, payload) else {
                return;
            };
            conn.last_seen = now;
            if seq >= conn.recv_next {
                if seq > conn.recv_next {
                    conn.hold.insert(seq, plain);
                } else {
                    deliver(inbound, plain);
                    conn.recv_next = conn.recv_next.wrapping_add(1);
                    while let Some(next) = conn.hold.remove(&conn.recv_next) {
                        deliver(inbound, next);
                        conn.recv_next = conn.recv_next.wrapping_add(1);
                    }
                }
            } // seq < recv_next => duplicate, just re-ack below
            send_frame(socket, from, &build_frame(F_ACK, conn.recv_next.wrapping_sub(1), &[]));
        }
        F_ACK => {
            let Some(conn) = conns.get_mut(&from) else { return };
            if !conn.punched {
                return;
            }
            conn.last_seen = now;
            conn.unacked.retain(|(s, _, _, _)| *s > seq);
        }
        F_PING => {
            let Some(conn) = conns.get_mut(&from) else { return };
            if !conn.punched {
                return;
            }
            conn.last_seen = now;
            send_frame(socket, from, &build_frame(F_PONG, seq, &[]));
        }
        F_PONG => {
            if let Some(conn) = conns.get_mut(&from) {
                if conn.punched {
                    conn.last_seen = now;
                }
            }
        }
        _ => {}
    }
}

fn deliver(inbound: &UnboundedSender<Message>, plain: Vec<u8>) {
    // `plain` is the AEAD-verified JSON of a party Message.
    if let Ok(msg) = serde_json::from_slice::<Message>(&plain) {
        let _ = inbound.send(msg);
    }
}

async fn broadcast(socket: &UdpSocket, conns: &mut HashMap<SocketAddr, PeerConn>, env: Vec<u8>) {
    for conn in conns.values_mut() {
        if !conn.punched {
            continue;
        }
        conn.my_seq = conn.my_seq.wrapping_add(1);
        let seq = conn.my_seq;
        let f = build_frame(F_DATA, seq, &env);
        if conn.unacked.len() >= SEND_WINDOW {
            conn.unacked.pop_front(); // bound memory; acks still work
        }
        conn.unacked
            .push_back((seq, f.clone(), Instant::now(), Instant::now()));
        let _ = socket.send_to(&f, conn.addr).await;
    }
}

async fn retransmit(socket: &UdpSocket, conn: &mut PeerConn, now: Instant) {
    let mut resend: Vec<Vec<u8>> = Vec::new();
    for (_, f, _, last) in conn.unacked.iter_mut() {
        if now - *last >= RETRANSMIT_EVERY {
            *last = now;
            resend.push(f.clone());
        }
    }
    for f in resend {
        let _ = socket.send_to(&f, conn.addr).await;
    }
}

fn send_frame(socket: &UdpSocket, addr: SocketAddr, f: &[u8]) {
    let _ = socket.try_send_to(f, addr);
}

fn build_frame(ftype: u8, seq: u32, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(9 + payload.len());
    v.extend_from_slice(MAGIC);
    v.push(ftype);
    v.extend_from_slice(&seq.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

fn sealed_who(key: &[u8; 32], ftype: u8, peer_id: &str) -> Vec<u8> {
    let who = serde_json::to_vec(&PunchWho {
        peer_id: peer_id.to_string(),
    })
    .unwrap_or_default();
    build_frame(ftype, 0, &protocol::seal(key, &who))
}

fn frame_req(key: &[u8; 32], peer_id: &str) -> Vec<u8> {
    sealed_who(key, F_REQ, peer_id)
}

fn frame_ok(key: &[u8; 32], peer_id: &str) -> Vec<u8> {
    sealed_who(key, F_OK, peer_id)
}
