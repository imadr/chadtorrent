//! chadtorrent-relay: the watch-party coordination hub.
//!
//! Deploy once on any host with a public address (free-tier VPS, your own
//! home server with one UDP port forwarded, ...) and chadtorrent
//! instances can run watch parties through it — invites carry this hub's
//! address, joiners configure nothing.
//!
//! The hub is a pure UDP "mailroom": members bind, learn each other's
//! public endpoints, punch directly, and the hub steps out. It never
//! carries party traffic, stores nothing, and speaks no WebSocket.
//!
//!     chadtorrent-relay [--port 3001] [--addr 0.0.0.0]

use clap::Parser;

#[derive(Parser)]
#[command(name = "chadtorrent-relay", version, about = "Watch-party NAT-punch hub (UDP introducer)")]
struct Args {
    /// UDP listen port
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
    let addr = format!("{}:{}", args.addr, args.port);
    let udp = match tokio::net::UdpSocket::bind(&addr).await {
        Ok(u) => u,
        Err(e) => {
            eprintln!("failed to bind udp://{addr}: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!("chadtorrent-relay punch hub on udp://{addr}");
    chadtorrent::party::punch::run_punch_server(udp).await;
}
