//! chadtorrent-relay: the standalone watch-party relay.
//!
//! Deploy once on any host with a public address (free-tier VPS, a
//! port-forwarded box, ...) and every chadtorrent instance can then run
//! internet-wide watch parties through it — invites carry this relay's
//! URL, joiners configure nothing. The process is stateless: it only
//! broadcasts opaque encrypted frames between members of a room and keeps
//! no data on disk.
//!
//!     chadtorrent-relay [--port 3001] [--addr 0.0.0.0]

use clap::Parser;

#[derive(Parser)]
#[command(name = "chadtorrent-relay", version, about = "Stateless watch-party message relay")]
struct Args {
    /// Listen port
    #[arg(long, short, default_value_t = 3001)]
    port: u16,
    /// Listen address
    #[arg(long, short, default_value = "0.0.0.0")]
    addr: String,
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("init tracing");

    let args = Args::parse();
    let router = chadtorrent::party::relay::relay_router();
    let addr = format!("{}:{}", args.addr, args.port);
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind {addr}: {e}");
            std::process::exit(1);
        }
    };
    // NAT punch hub on the same port over UDP (forward both TCP and UDP
    // for this port when running behind a router).
    match tokio::net::UdpSocket::bind(&addr).await {
        Ok(udp) => {
            tokio::spawn(chadtorrent::party::punch::run_punch_server(udp));
        }
        Err(e) => tracing::warn!("UDP bind failed ({e}) — running without the punch hub"),
    };
    tracing::info!("chadtorrent-relay listening on ws://{addr}/ws/party/relay/<room>");
    if let Err(e) = axum::serve(listener, router).await {
        tracing::error!("server error: {e}");
    }
}
