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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, info, warn};

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
/// Resync request: both sides drop their sequence state (send counter,
/// receive frontier, held/unacked frames) and keep the punch. Old peers
/// ignore the unknown type — same behaviour as before this existed.
const F_RESET: u8 = 7;

const TICK: Duration = Duration::from_millis(100);
const PUNCH_EVERY: Duration = Duration::from_millis(250);
const PUNCH_GIVE_UP: Duration = Duration::from_secs(12);
const RETRANSMIT_EVERY: Duration = Duration::from_millis(400);
const SEND_WINDOW: usize = 64;
const PING_EVERY: Duration = Duration::from_secs(8);
/// No frames from a punched peer for this long => dead => re-punch. Generous:
/// a still-downloading peer hammers CPU (ffmpeg segmenting, FFmpeg reads),
/// which can stall the async runtime for a while. Pings fly every 8s, so a
/// live peer is never hit by this.
const DEAD_AFTER: Duration = Duration::from_secs(35);
const REBIND_EVERY: Duration = Duration::from_secs(15);
/// A hold map (out-of-order frames awaiting a gap) that stays unmoved this
/// long means the two sides' seq counters have diverged — the gap will never
/// close, so ask the peer to resync (F_RESET) and reset ours too.
const STALL_RECOVER: Duration = Duration::from_secs(10);
/// The hub drops a room member after this much silence. Live members rebind
/// every ~15s, so 45s means the member is gone for good.
const HUB_MEMBER_EXPIRE: Duration = Duration::from_secs(45);

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
        let members = rooms.entry(bind.room.clone()).or_default();
        let expired = expire_room_members(members, from, Instant::now());
        if expired > 0 {
            warn!(
                "hub: expired {expired} silent member(s) of room {} ({} remain)",
                bind.room.as_str(),
                members.len()
            );
        }
        members.insert(bind.peer_id.clone(), (from, Instant::now()));
        debug!(
            "hub: {} bound as {} ({} member(s) in room)",
            from, bind.peer_id, members.len()
        );

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
// Client: punched peer-to-peer transport (the only transport)

struct PeerConn {
    peer_id: String,
    addr: SocketAddr,
    punched: bool,
    my_seq: u32,
    /// (seq, frame, first_sent, last_sent)
    unacked: VecDeque<(u32, Vec<u8>, Instant, Instant)>,
    /// Next in-order seq we expect (seqs start at 1).
    recv_next: u32,
    /// Out-of-order messages awaiting their predecessor.
    hold: HashMap<u32, Message>,
    last_seen: Instant,
    last_ping: Instant,
    last_req: Instant,
    last_hold_warn: Instant,
    /// Oldest seq sitting in `hold`, and when it last changed. Lets the tick
    /// tell a closing gap (retransmit lands within ~400ms) from a permanently
    /// desynced stream.
    stall_oldest: u32,
    stall_since: Option<Instant>,
    /// Last F_RESET sent/received — cooldown guards the reply loop.
    last_reset: Instant,
}

/// The party's transport task. Punches every member of the room, then
/// carries ALL party traffic peer-to-peer. There is deliberately no
/// server relay: until every member has punched, messages don't flow
/// (the manager reports connecting/reconnecting during that time), and a
/// silent peer drops the party back into re-punching.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_transport(
    mgr: Arc<super::PartyManager>,
    my_gen: u64,
    gen: Arc<AtomicU64>,
    relay_base: String,
    room: String,
    key: [u8; 32],
    my_peer_id: String,
    mut cmd_rx: UnboundedReceiver<super::ClientCmd>,
    inbound: UnboundedSender<Message>,
) {
    mgr.set_conn(my_gen, "connecting");
    // Resolve the coordination hub (the invite's relay URL, host:port).
    let server: SocketAddr = loop {
        let host_port = relay_base
            .trim()
            .trim_start_matches("wss://")
            .trim_start_matches("ws://")
            .trim_end_matches('/');
        if let Ok(mut addrs) = tokio::net::lookup_host(host_port).await {
            if let Some(addr) = addrs.next() {
                break addr;
            }
        }
        if gen.load(Ordering::SeqCst) != my_gen {
            return;
        }
        warn!("Punch hub {host_port} unresolved — retrying");
        mgr.set_conn(my_gen, "reconnecting");
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await else {
        return;
    };

    let mut targets: Vec<BoundPeer> = Vec::new();
    let mut conns: HashMap<SocketAddr, PeerConn> = HashMap::new();
    let mut last_bind = Instant::now() - REBIND_EVERY;
    let mut punch_warned = false;
    let mut hub_alive = false;
    let punch_started = Instant::now();
    let mut known_targets = 0usize;
    // Actions/state are NOT dropped while unpunched: they queue here and
    // flush in order once (re)punched, so a seek during a re-punch moment
    // still lands on the other side.
    let mut pending: VecDeque<Vec<u8>> = VecDeque::new();
    let mut active = false;
    let mut buf = [0u8; 2048];
    let mut tick = tokio::time::interval(TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if gen.load(Ordering::SeqCst) != my_gen {
            return;
        }
        tokio::select! {
            r = socket.recv_from(&mut buf) => {
                let Ok((n, from)) = r else { continue };
                let frame = &buf[..n];
                if frame.first() == Some(&b'{') {
                    // Hub push (JSON Bound) — proof the hub is reachable.
                    if let Ok(b) = serde_json::from_slice::<Bound>(frame) {
                        hub_alive = true;
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
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(super::ClientCmd::Send(m)) => {
                        let env = protocol::encode_message(&key, &m);
                        if active {
                            broadcast(&socket, &mut conns, env).await;
                        } else {
                            // Queue rather than drop: a seek made during the
                            // brief unpunched/re-punch window must still land.
                            if pending.len() < 256 {
                                pending.push_back(env);
                            }
                        }
                    }
                    Some(super::ClientCmd::Stop) | None => return,
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if gen.load(Ordering::SeqCst) != my_gen {
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
                            // PING eschews the DATA seq namespace: the
                            // receiver's in-order recv_next only advances on
                            // F_DATA, so a PING taking a data seq would leave
                            // a permanent hole in the stream and stall every
                            // subsequent message in `hold`. Its seq field is
                            // only echoed by PONG and is never interpreted.
                            send_frame(&socket, addr, &build_frame(F_PING, 0, &[]));
                        }
                        retransmit(&socket, conn, now).await;
                    }
                }

                // Stalled in-order stream: a non-empty hold map that stops
                // moving means sequenced frames are stuck waiting for a gap
                // that will never close — the two sides' seq counters have
                // diverged (e.g. only one side went through the peer-lost
                // reset). Presences stop, the roster prunes, yet PINGs keep
                // the conn alive — the exact "online but roster=0" symptom.
                // Log while it builds; after STALL_RECOVER ask the peer to
                // resync (F_RESET) and reset our own stream state too.
                for (addr, c) in conns.iter_mut() {
                    if !c.punched {
                        continue;
                    }
                    let Some(since) = c.stall_since else { continue };
                    if now - c.last_seen > Duration::from_secs(5) {
                        continue; // silent peer — the dead-check below owns this conn
                    }
                    if now - since < STALL_RECOVER {
                        if now - c.last_hold_warn > Duration::from_secs(5) {
                            c.last_hold_warn = now;
                            let oldest = c.hold.keys().min().copied().unwrap_or(0);
                            warn!(
                                "seq stall: {} unreachable (expected {} got-hold {} oldest {})",
                                c.peer_id, c.recv_next, c.hold.len(), oldest
                            );
                        }
                        continue;
                    }
                    warn!(
                        "seq stall with {} unresolved for {}s — resetting stream state",
                        c.peer_id,
                        (now - since).as_secs()
                    );
                    send_frame(&socket, *addr, &build_frame(F_RESET, 0, &[]));
                    c.last_reset = now;
                    reset_stream(c);
                }

                // Membership change: re-punch before anything flows again.
                if targets.len() != known_targets {
                    warn!(
                        "membership changed: {} -> {} peer(s)",
                        known_targets, targets.len()
                    );
                    known_targets = targets.len();
                    if active {
                        active = false;
                        mgr.set_conn(my_gen, "reconnecting");
                        mgr.clear_peers(my_gen);
                    }
                }

                // Fully punched (or alone in the room) => live. Announce
                // and rebuild the roster.
                let alone = hub_alive && targets.is_empty();
                let targets_len = targets.len();
                let all_punched = targets_len > 0
                    && conns.values().filter(|c| c.punched).count() == targets_len;
                if !active && (alone || all_punched) {
                    if alone {
                        info!("party hub reached — waiting for other members");
                    } else {
                        info!("punched all {targets_len} peer(s) — peer-to-peer sync");
                    }
                    active = true;
                    mgr.clear_peers(my_gen);
                    mgr.set_conn(my_gen, "online");
                    if let Some(me) = mgr.me_snapshot(my_gen) {
                        broadcast(
                            &socket,
                            &mut conns,
                            protocol::encode_message(&key, &Message::Hello { peer: me }),
                        )
                        .await;
                    }
                    // Deliver everything that was queued while unpunched.
                    while let Some(env) = pending.pop_front() {
                        broadcast(&socket, &mut conns, env).await;
                    }
                } else if !targets.is_empty()
                    && !active
                    && !punch_warned
                    && punch_started.elapsed() > PUNCH_GIVE_UP
                {
                    punch_warned = true;
                    warn!(
                        "punch incomplete ({}/{} peers) — some members may be unreachable",
                        conns.values().filter(|c| c.punched).count(),
                        targets.len()
                    );
                }

                // Liveness: a silent peer stops the party until re-punched.
                if active {
                    let dead: Vec<SocketAddr> = conns
                        .iter()
                        .filter(|(_, c)| c.punched && now - c.last_seen > DEAD_AFTER)
                        .map(|(a, _)| *a)
                        .collect();
                    if !dead.is_empty() {
                        warn!("party peer lost — re-punching");
                        active = false;
                        mgr.set_conn(my_gen, "reconnecting");
                        mgr.clear_peers(my_gen);
                        // Re-question the hub RIGHT NOW: if the peer's address
                        // changed (they re-joined), the next Bound push tells
                        // us instead of punching a stale addr for REBIND_EVERY.
                        last_bind = now;
                        let bind = serde_json::to_vec(&Bind {
                            room: room.clone(),
                            peer_id: my_peer_id.clone(),
                        })
                        .unwrap_or_default();
                        let _ = socket.send_to(&bind, server).await;
                        for addr in dead {
                            // If the peer is actually alive (we were the only
                            // one who saw silence), tell it to drop its seq
                            // state too — otherwise its sender counter would
                            // sit ahead of our fresh recv frontier and every
                            // frame would be misfiled as a duplicate.
                            send_frame(&socket, addr, &build_frame(F_RESET, 0, &[]));
                            if let Some(c) = conns.get_mut(&addr) {
                                let peer_id = c.peer_id.clone();
                                *c = new_conn(peer_id, addr, now);
                            }
                        }
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
        last_hold_warn: now - Duration::from_secs(10),
        stall_oldest: 0,
        stall_since: None,
        last_reset: now - Duration::from_secs(10), // cooldown already expired
    }
}

/// Drop all sequence-numbered stream state, keeping the punch. Sent (F_RESET)
/// when the two sides' seq counters have diverged — e.g. only one side went
/// through the peer-lost reset, so one side's sender counter sits ahead of the
/// other's fresh receive frontier and every frame is misfiled (held forever or
/// dropped as a "duplicate"). After both sides reset, numbering starts from 1
/// on each and stays consistent. In-flight frames are dropped — party traffic
/// is idempotent snapshots (presence, absolute-position actions), so losing a
/// beat beats stalling forever.
fn reset_stream(conn: &mut PeerConn) {
    conn.my_seq = 0;
    conn.unacked.clear();
    conn.recv_next = 1;
    conn.hold.clear();
    conn.stall_since = None;
}

/// Expire room members on the hub. A member that stopped rebinding is gone no
/// matter which address it sat at — the old `|| addr != from` escape hatch
/// kept ghosts forever, and since the hub keeps pushing them as punch targets
/// while `all_punched` requires every target, one ghost could wedge new
/// joiners into eternal "connecting". The binder's own address is always
/// reset: a restart under the same address carries a new peer id, and the old
/// identity must not linger as an unpunchable duplicate target. Returns the
/// number of dropped entries.
fn expire_room_members(
    members: &mut HashMap<String, (SocketAddr, Instant)>,
    from: SocketAddr,
    now: Instant,
) -> usize {
    let before = members.len();
    members.retain(|_, (addr, seen)| {
        addr != &from && now.duration_since(*seen) < HUB_MEMBER_EXPIRE
    });
    before - members.len()
}

fn apply_bound(
    targets: &mut Vec<BoundPeer>,
    conns: &mut HashMap<SocketAddr, PeerConn>,
    b: Bound,
) {
    let prev = targets.len();
    *targets = b.peers;
    let n = targets.len();
    if n != prev {
        warn!("hub push: {} -> {} peer(s)", prev, n);
    }
    let listed: HashSet<String> = targets.iter().map(|p| p.id.clone()).collect();
    let addrs: HashSet<SocketAddr> = targets
        .iter()
        .filter_map(|p| p.addr.parse::<SocketAddr>().ok())
        .collect();
    conns.retain(|addr, c| addrs.contains(addr) && listed.contains(&c.peer_id));
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
            let Some(msg) = protocol::decode_message(key, payload) else {
                return;
            };
            conn.last_seen = now;
            if seq >= conn.recv_next {
                if seq > conn.recv_next {
                    conn.hold.insert(seq, msg);
                } else {
                    let _ = inbound.send(msg);
                    conn.recv_next = conn.recv_next.wrapping_add(1);
                    while let Some(next) = conn.hold.remove(&conn.recv_next) {
                        let _ = inbound.send(next);
                        conn.recv_next = conn.recv_next.wrapping_add(1);
                    }
                }
            } // seq < recv_next => duplicate, just re-ack below
            // Stall bookkeeping: track when the oldest held seq last changed.
            match conn.hold.keys().min().copied() {
                Some(oldest) => {
                    if conn.stall_since.is_none() || oldest != conn.stall_oldest {
                        conn.stall_oldest = oldest;
                        conn.stall_since = Some(now);
                    }
                }
                None => conn.stall_since = None,
            }
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
        F_RESET => {
            let Some(conn) = conns.get_mut(&from) else { return };
            if !conn.punched {
                return;
            }
            conn.last_seen = now;
            // Cooldown guards the reply loop: both sides reset and answer,
            // and each must ignore the other's echo of its own request.
            if now - conn.last_reset < Duration::from_secs(2) {
                return;
            }
            conn.last_reset = now;
            info!("stream reset from {} — resyncing sequence state", conn.peer_id);
            reset_stream(conn);
            send_frame(socket, from, &build_frame(F_RESET, 0, &[]));
        }
        _ => {}
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_expires_silent_members() {
        let a: SocketAddr = "1.2.3.4:1000".parse().unwrap();
        let b: SocketAddr = "5.6.7.8:2000".parse().unwrap();
        let now = Instant::now();
        let mut members = HashMap::new();
        // Live member at another address: kept.
        members.insert("live".to_string(), (b, now - Duration::from_secs(10)));
        // Ghost that went silent at another address: dropped (was kept
        // forever by the old `|| addr != from` escape hatch).
        members.insert(
            "ghost".to_string(),
            (a, now - Duration::from_secs(90)),
        );
        // Stale identity at the binder's own address (restart under the same
        // address carries a new peer id): dropped immediately.
        members.insert(
            "restarted".to_string(),
            (a, now - Duration::from_secs(2)),
        );
        let expired = expire_room_members(&mut members, a, now);
        assert_eq!(expired, 2);
        assert_eq!(members.len(), 1);
        assert!(members.contains_key("live"));
    }

    #[test]
    fn reset_stream_clears_sequence_state() {
        let now = Instant::now();
        let mut c = new_conn("peer".into(), "1.2.3.4:5".parse().unwrap(), now);
        c.punched = true;
        c.my_seq = 42;
        c.recv_next = 500;
        c.hold.insert(501, Message::Seek { from: "p".into(), t: 1.0, file: None });
        c.unacked.push_back((42, vec![0u8], now, now));
        c.stall_since = Some(now);
        reset_stream(&mut c);
        assert_eq!(c.my_seq, 0);
        assert_eq!(c.recv_next, 1);
        assert!(c.hold.is_empty());
        assert!(c.unacked.is_empty());
        assert!(c.stall_since.is_none());
        assert!(c.punched, "the punch itself must survive a stream reset");
    }

    /// F_RESET from a punched peer resyncs the conn and answers with its own
    /// F_RESET, so both sides converge regardless of who initiated.
    #[tokio::test]
    async fn f_reset_resyncs_conn_and_replies() {
        let sock_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sock_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_addr = sock_a.local_addr().unwrap();
        let key = [7u8; 32];
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

        let mut conns = HashMap::new();
        let mut c = new_conn("peer-a".into(), a_addr, Instant::now());
        c.punched = true;
        c.recv_next = 400;
        c.my_seq = 10;
        conns.insert(a_addr, c);

        sock_a
            .send_to(&build_frame(F_RESET, 0, &[]), sock_b.local_addr().unwrap())
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        let (n, from) = sock_b.recv_from(&mut buf).await.unwrap();
        assert_eq!(from, a_addr);
        handle_frame(
            &sock_b, &key, "peer-b", &tx, &mut conns, from, &buf[..n], Instant::now(),
        )
        .await;

        let c = conns.get(&a_addr).unwrap();
        assert_eq!(c.recv_next, 1);
        assert_eq!(c.my_seq, 0);
        assert!(c.punched);

        // The reply is a bare F_RESET frame.
        let (n2, rfrom) = sock_a.recv_from(&mut buf).await.unwrap();
        assert_eq!(rfrom, sock_b.local_addr().unwrap());
        assert_eq!(n2, 9);
        assert_eq!(&buf[4], &F_RESET);
    }

    /// Out-of-order DATA arms the stall clock; in-order delivery disarms it.
    #[tokio::test]
    async fn data_stall_bookkeeping_tracks_hold() {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer: SocketAddr = "127.0.0.1:65000".parse().unwrap();
        let key = [9u8; 32];
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

        let mut conns = HashMap::new();
        let mut c = new_conn("peer".into(), peer, Instant::now());
        c.punched = true;
        conns.insert(peer, c);

        let env = protocol::encode_message(&key, &Message::Seek { from: "p".into(), t: 3.0, file: None });
        // seq 2 while recv_next=1 → held, stall armed with oldest=2.
        let frame = build_frame(F_DATA, 2, &env);
        handle_frame(&sock, &key, "me", &tx, &mut conns, peer, &frame, Instant::now()).await;
        {
            let c = conns.get(&peer).unwrap();
            assert_eq!(c.recv_next, 1);
            assert!(c.hold.contains_key(&2));
            assert_eq!(c.stall_oldest, 2);
            assert!(c.stall_since.is_some());
        }
        // seq 1 closes the gap → hold drains, stall disarmed.
        let frame = build_frame(F_DATA, 1, &env);
        handle_frame(&sock, &key, "me", &tx, &mut conns, peer, &frame, Instant::now()).await;
        let c = conns.get(&peer).unwrap();
        assert_eq!(c.recv_next, 3);
        assert!(c.hold.is_empty());
        assert!(c.stall_since.is_none());
    }
}
