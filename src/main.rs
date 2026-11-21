#![deny(clippy::all, clippy::unwrap_used, clippy::expect_used)]

mod config;
mod coordinator;
mod engine;
mod metrics;
mod server;
mod stats;

use anyhow::{bail, Context, Result};
use config::Config;
use coordinator::TestCoordinator;
use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Serve,
    Version,
    Help,
    Healthcheck,
    Once { json: bool },
}

fn parse_args() -> Result<Command> {
    let mut args = env::args().skip(1);
    let Some(flag) = args.next() else {
        return Ok(Command::Serve);
    };

    if let Some(extra) = args.next() {
        bail!("unexpected argument '{extra}'");
    }

    match flag.as_str() {
        "-V" | "--version" => Ok(Command::Version),
        "-h" | "--help" => Ok(Command::Help),
        "--healthcheck" => Ok(Command::Healthcheck),
        "--once" => Ok(Command::Once { json: false }),
        "--once-json" => Ok(Command::Once { json: true }),
        other => bail!("unknown flag '{other}' (try --help)"),
    }
}

fn print_help() {
    println!(
        "cloudflare-speedtest-exporter {}\n\n\
         USAGE:\n  \
           cloudflare-speedtest-exporter [FLAG]\n\n\
         FLAGS:\n  \
           (none)         Start the Prometheus HTTP exporter server\n  \
           --once         Run a single speedtest, print summary, and exit\n  \
           --once-json    Run a single speedtest, print JSON, and exit\n  \
           --healthcheck  Probe http://127.0.0.1:$SPEEDTEST_PORT/health\n  \
           -V, --version  Print version and exit\n  \
           -h, --help     Print help information",
        env!("CARGO_PKG_VERSION")
    );
}

fn run_healthcheck() -> Result<()> {
    let port: u16 = match env::var("SPEEDTEST_PORT") {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<u16>()
            .context("invalid SPEEDTEST_PORT")?,
        _ => 9798,
    };

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let timeout = Duration::from_secs(3);
    let mut stream = TcpStream::connect_timeout(&addr, timeout)
        .with_context(|| format!("failed to connect to {addr}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    write!(
        stream,
        "GET /health HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf)?;
    let head = std::str::from_utf8(&buf[..n]).unwrap_or("");
    if head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.0 200") {
        Ok(())
    } else {
        bail!("unhealthy response from /health: {}", head.lines().next().unwrap_or(""))
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

async fn run_once_cli(cfg: &Config, json: bool) -> Result<()> {
    let cancel = CancellationToken::new();
    let result = tokio::time::timeout(cfg.timeout, engine::run_once(cfg, &cancel))
        .await
        .with_context(|| format!("speedtest timed out after {}s", cfg.timeout.as_secs()))??;

    if json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    println!("Cloudflare Speedtest Result");
    println!(
        "  Edge PoP   : {} ({}, {})",
        result.meta.colo.as_deref().unwrap_or("unknown"),
        result.meta.colo_city.as_deref().unwrap_or("-"),
        result.meta.country.as_deref().unwrap_or("-"),
    );
    println!(
        "  ISP / ASN  : {} (AS{})",
        result.meta.isp.as_deref().unwrap_or("unknown"),
        result.meta.asn.as_deref().unwrap_or("-"),
    );
    println!(
        "  DNS / TLS  : {:.2} ms / {} ms",
        result.dns_ms,
        result
            .tls_ms
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "n/a".to_string())
    );
    println!(
        "  Idle Ping  : {:.2} ms (min {:.2} / p95 {:.2} / max {:.2}, jitter {:.2} ms)",
        result.idle_latency.median_ms.unwrap_or(0.0),
        result.idle_latency.min_ms.unwrap_or(0.0),
        result.idle_latency.p95_ms.unwrap_or(0.0),
        result.idle_latency.max_ms.unwrap_or(0.0),
        result.idle_latency.jitter_ms.unwrap_or(0.0),
    );
    println!(
        "  Download   : {:.2} Mbps (p95 {:.2} Mbps, CV {}, loaded ping {:.2} ms)",
        result.download.steady_bps / 1_000_000.0,
        result.download.p95_bps / 1_000_000.0,
        result
            .download
            .stability_cv_percent
            .map(|v| format!("{v:.1}%"))
            .unwrap_or_else(|| "n/a".to_string()),
        result.loaded_download_latency.median_ms.unwrap_or(0.0),
    );
    println!(
        "  Upload     : {:.2} Mbps (p95 {:.2} Mbps, CV {}, loaded ping {:.2} ms)",
        result.upload.steady_bps / 1_000_000.0,
        result.upload.p95_bps / 1_000_000.0,
        result
            .upload
            .stability_cv_percent
            .map(|v| format!("{v:.1}%"))
            .unwrap_or_else(|| "n/a".to_string()),
        result.loaded_upload_latency.median_ms.unwrap_or(0.0),
    );
    if let Some(udp) = &result.udp {
        if udp.reachable {
            println!(
                "  UDP STUN   : {:.2}% loss ({}/{}), median {:.2} ms, jitter {:.2} ms",
                udp.loss_ratio.unwrap_or(0.0) * 100.0,
                udp.received,
                udp.sent,
                udp.median_ms.unwrap_or(0.0),
                udp.jitter_ms.unwrap_or(0.0),
            );
        } else {
            println!("  UDP STUN   : unreachable ({} probes sent)", udp.sent);
        }
    }
    println!("  Duration   : {:.2}s", result.duration_secs);

    Ok(())
}

async fn run_server(cfg: Config) -> Result<()> {
    let bind_socket = SocketAddr::new(cfg.bind_addr, cfg.port);
    let interval_secs = cfg.interval.as_secs();
    let cache_secs = cfg.cache_duration.as_secs();
    let cfg = Arc::new(cfg);

    let coordinator = Arc::new(TestCoordinator::new(Arc::clone(&cfg)));
    let shutdown = CancellationToken::new();

    let bg_handle = if interval_secs > 0 {
        Some(coordinator.spawn_background_loop(shutdown.clone()))
    } else {
        None
    };

    let router = server::build_router(coordinator, shutdown.clone());
    let listener = TcpListener::bind(bind_socket)
        .await
        .with_context(|| format!("failed to bind HTTP server on {bind_socket}"))?;

    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %bind_socket,
        mode = if interval_secs > 0 { "background" } else { "on-scrape" },
        interval_secs,
        cache_secs,
        "Cloudflare Speedtest Exporter started"
    );

    let signal_shutdown = shutdown.clone();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            wait_for_shutdown_signal().await;
            info!("Shutdown signal received, stopping exporter");
            signal_shutdown.cancel();
        })
        .await
        .context("HTTP server error")?;

    if let Some(handle) = bg_handle {
        let _ = handle.await;
    }

    Ok(())
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn main() -> ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cmd = match parse_args() {
        Ok(cmd) => cmd,
        Err(err) => {
            eprintln!("error: {err:#}");
            return ExitCode::FAILURE;
        }
    };

    match cmd {
        Command::Version => {
            println!(
                "cloudflare-speedtest-exporter {}",
                env!("CARGO_PKG_VERSION")
            );
            ExitCode::SUCCESS
        }
        Command::Help => {
            print_help();
            ExitCode::SUCCESS
        }
        Command::Healthcheck => match run_healthcheck() {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("healthcheck failed: {err:#}");
                ExitCode::FAILURE
            }
        },
        Command::Once { json } => {
            init_tracing();
            let cfg = match Config::from_env() {
                Ok(c) => c,
                Err(err) => {
                    error!("Configuration error: {err:#}");
                    return ExitCode::FAILURE;
                }
            };
            let rt = match tokio::runtime::Runtime::new() {
                Ok(r) => r,
                Err(err) => {
                    error!("Failed to initialize async runtime: {err}");
                    return ExitCode::FAILURE;
                }
            };
            match rt.block_on(run_once_cli(&cfg, json)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    error!("Speedtest failed: {err:#}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Serve => {
            init_tracing();
            let cfg = match Config::from_env() {
                Ok(c) => c,
                Err(err) => {
                    error!("Configuration error: {err:#}");
                    return ExitCode::FAILURE;
                }
            };
            let rt = match tokio::runtime::Runtime::new() {
                Ok(r) => r,
                Err(err) => {
                    error!("Failed to initialize async runtime: {err}");
                    return ExitCode::FAILURE;
                }
            };
            match rt.block_on(run_server(cfg)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    error!("Fatal server error: {err:#}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}
