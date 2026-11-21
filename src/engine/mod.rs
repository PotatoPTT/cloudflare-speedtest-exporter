pub mod cloudflare;
pub mod diagnostics;
pub mod latency;
pub mod throughput;
pub mod udp_stun;

use crate::config::Config;
use anyhow::{bail, Context, Result};
use cloudflare::{CloudflareClient, CloudflareMeta};
use latency::LatencySummary;
use rand::RngCore;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;
use throughput::ThroughputSummary;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use udp_stun::UdpSummary;

/// Complete result produced by a single speedtest execution.
#[derive(Debug, Serialize)]
pub struct SpeedtestResult {
    pub meta: CloudflareMeta,
    pub dns_ms: f64,
    pub tls_ms: Option<f64>,
    pub idle_latency: LatencySummary,
    pub download: ThroughputSummary,
    pub loaded_download_latency: LatencySummary,
    pub upload: ThroughputSummary,
    pub loaded_upload_latency: LatencySummary,
    pub udp: Option<UdpSummary>,
    pub duration_secs: f64,
}

/// Executes a full Cloudflare speedtest run across DNS, TLS, metadata, idle latency,
/// download, upload, and optional STUN UDP diagnostics.
pub async fn run_once(cfg: &Config, cancel: &CancellationToken) -> Result<SpeedtestResult> {
    let total_start = Instant::now();
    let meas_id = random_meas_id();

    let host = cfg
        .base_url
        .host_str()
        .context("SPEEDTEST_BASE_URL missing host")?;
    let port = cfg.base_url.port_or_known_default().unwrap_or(443);

    let (dns_ms, resolved_addrs) = diagnostics::measure_dns(host, port, cfg.ip_version).await?;

    let tls_ms = if cfg.base_url.scheme() == "https" {
        Some(diagnostics::measure_tls(host, &resolved_addrs).await?)
    } else {
        None
    };

    let client = Arc::new(CloudflareClient::new(
        &cfg.base_url,
        meas_id,
        &resolved_addrs,
        cfg.ip_version,
    )?);

    let meta = client.fetch_metadata().await;

    let idle_latency = latency::run_latency_probes(
        &client,
        None,
        cfg.idle_latency_duration,
        cfg.probe_interval,
        cfg.probe_timeout,
        cancel,
    )
    .await;
    if idle_latency.received == 0 {
        bail!("idle latency phase received 0 responses");
    }

    let (download, loaded_download_latency) =
        throughput::run_download(&client, cfg, cancel).await?;
    let (upload, loaded_upload_latency) = throughput::run_upload(&client, cfg, cancel).await?;

    let udp = if cfg.udp_enabled && !cancel.is_cancelled() {
        match udp_stun::run_udp_stun_probe(
            &cfg.stun_host,
            cfg.stun_port,
            cfg.udp_probes,
            cfg.ip_version,
            cancel,
        )
        .await
        {
            Ok(summary) => Some(summary),
            Err(err) => {
                warn!("UDP STUN probe skipped: {err:#}");
                Some(UdpSummary::unreachable(0))
            }
        }
    } else {
        None
    };

    let duration_secs = total_start.elapsed().as_secs_f64();

    info!(
        colo = meta.colo.as_deref().unwrap_or("unknown"),
        download_mbps = format_args!("{:.2}", download.steady_bps / 1_000_000.0),
        upload_mbps = format_args!("{:.2}", upload.steady_bps / 1_000_000.0),
        idle_ping_ms = format_args!("{:.2}", idle_latency.median_ms.unwrap_or(0.0)),
        duration_secs = format_args!("{duration_secs:.2}"),
        "Cloudflare speedtest completed"
    );

    Ok(SpeedtestResult {
        meta,
        dns_ms,
        tls_ms,
        idle_latency,
        download,
        loaded_download_latency,
        upload,
        loaded_upload_latency,
        udp,
        duration_secs,
    })
}

fn random_meas_id() -> String {
    let mut bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut bytes);
    let mut id = String::with_capacity(16);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(id, "{b:02x}");
    }
    id
}
