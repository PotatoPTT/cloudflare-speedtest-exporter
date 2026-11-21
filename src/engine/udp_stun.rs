use crate::config::IpVersion;
use crate::stats;
use anyhow::{anyhow, bail, Context, Result};
use rand::RngCore;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::{lookup_host, UdpSocket};
use tokio_util::sync::CancellationToken;

const STUN_MAGIC_COOKIE: [u8; 4] = [0x21, 0x12, 0xA4, 0x42];
const STUN_PROBE_INTERVAL: Duration = Duration::from_millis(80);
const STUN_PROBE_TIMEOUT: Duration = Duration::from_millis(600);
const UNREACHABLE_CUTOFF: u64 = 10;

/// Summary of UDP packet loss and RTT measured via STUN Binding probes.
#[derive(Debug, Serialize)]
pub struct UdpSummary {
    pub sent: u64,
    pub received: u64,
    pub reachable: bool,
    pub loss_ratio: Option<f64>,
    pub median_ms: Option<f64>,
    pub jitter_ms: Option<f64>,
}

impl UdpSummary {
    /// Returns an unreachable summary when UDP resolution, bind, or probes fail.
    pub fn unreachable(sent: u64) -> Self {
        Self {
            sent,
            received: 0,
            reachable: false,
            loss_ratio: None,
            median_ms: None,
            jitter_ms: None,
        }
    }
}

/// Runs RFC 5389 STUN Binding probes against `host:port` to measure UDP packet loss and jitter.
pub async fn run_udp_stun_probe(
    host: &str,
    port: u16,
    probe_count: u64,
    ip_version: IpVersion,
    cancel: &CancellationToken,
) -> Result<UdpSummary> {
    let candidates: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .with_context(|| format!("DNS lookup failed for STUN server {host}:{port}"))?
        .filter(|addr| ip_version.matches(addr.ip()))
        .collect();

    if candidates.is_empty() {
        bail!("no matching addresses resolved for STUN server {host}:{port}");
    }

    let socket = connect_udp_candidate(&candidates).await?;
    run_stun_loop(&socket, probe_count, cancel).await
}

async fn connect_udp_candidate(candidates: &[SocketAddr]) -> Result<UdpSocket> {
    let mut last_err: Option<anyhow::Error> = None;

    for &addr in candidates {
        let bind_addr = if addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };

        let socket = match UdpSocket::bind(bind_addr).await {
            Ok(s) => s,
            Err(err) => {
                last_err = Some(anyhow!(err).context(format!("UDP bind {bind_addr} failed")));
                continue;
            }
        };

        match socket.connect(addr).await {
            Ok(()) => return Ok(socket),
            Err(err) => {
                last_err = Some(anyhow!(err).context(format!("UDP connect to {addr} failed")));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("no reachable UDP STUN candidate")))
}

async fn run_stun_loop(
    socket: &UdpSocket,
    probe_count: u64,
    cancel: &CancellationToken,
) -> Result<UdpSummary> {
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut rtts_ms = Vec::with_capacity(probe_count as usize);
    let mut in_flight: HashMap<[u8; 12], (u64, Instant)> =
        HashMap::with_capacity(probe_count as usize);
    let mut seen_seqs: HashSet<u64> = HashSet::with_capacity(probe_count as usize);
    let mut recv_buf = [0u8; 512];
    let mut next_send_at = tokio::time::Instant::now();
    let mut drain_deadline: Option<tokio::time::Instant> = None;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        if sent >= probe_count && received >= sent {
            break;
        }

        let timer_deadline = if sent < probe_count {
            next_send_at
        } else {
            drain_deadline.unwrap_or(next_send_at)
        };

        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep_until(timer_deadline) => {
                if sent < probe_count {
                    if received == 0 && sent >= UNREACHABLE_CUTOFF {
                        return Ok(UdpSummary::unreachable(sent));
                    }

                    let mut txid = [0u8; 12];
                    rand::thread_rng().fill_bytes(&mut txid);
                    let req = build_binding_request(txid);
                    if socket.send(&req).await.is_ok() {
                        in_flight.insert(txid, (sent, Instant::now()));
                    }
                    sent += 1;

                    let now = tokio::time::Instant::now();
                    if sent >= probe_count {
                        drain_deadline = Some(now + STUN_PROBE_TIMEOUT);
                    } else {
                        next_send_at = now + STUN_PROBE_INTERVAL;
                    }
                } else {
                    break;
                }
            }
            recv_res = socket.recv(&mut recv_buf) => {
                if let Ok(len) = recv_res {
                    if let Some((seq, sent_at)) =
                        match_binding_response(&recv_buf[..len], &in_flight)
                    {
                        if seen_seqs.insert(seq) {
                            received += 1;
                            rtts_ms.push(sent_at.elapsed().as_secs_f64() * 1000.0);
                        }
                    }
                }
            }
        }
    }

    if received == 0 {
        return Ok(UdpSummary::unreachable(sent));
    }

    let loss_ratio = if sent == 0 {
        0.0
    } else {
        ((sent.saturating_sub(received)) as f64) / (sent as f64)
    };
    let median_ms = stats::compute_sample_stats(&rtts_ms).map(|s| s.median);
    let jitter_ms = stats::compute_jitter(&rtts_ms);

    Ok(UdpSummary {
        sent,
        received,
        reachable: true,
        loss_ratio: Some(loss_ratio),
        median_ms,
        jitter_ms,
    })
}

fn build_binding_request(txid: [u8; 12]) -> [u8; 20] {
    let mut packet = [0u8; 20];
    packet[0] = 0x00;
    packet[1] = 0x01;
    packet[4..8].copy_from_slice(&STUN_MAGIC_COOKIE);
    packet[8..20].copy_from_slice(&txid);
    packet
}

fn match_binding_response(
    buf: &[u8],
    in_flight: &HashMap<[u8; 12], (u64, Instant)>,
) -> Option<(u64, Instant)> {
    if buf.len() < 20 {
        return None;
    }
    if buf[0] != 0x01 || buf[1] != 0x01 || buf[4..8] != STUN_MAGIC_COOKIE {
        return None;
    }

    let mut txid = [0u8; 12];
    txid.copy_from_slice(&buf[8..20]);
    in_flight.get(&txid).copied()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn build_binding_success(txid: &[u8]) -> [u8; 20] {
        let mut resp = [0u8; 20];
        resp[0] = 0x01;
        resp[1] = 0x01;
        resp[4..8].copy_from_slice(&STUN_MAGIC_COOKIE);
        resp[8..20].copy_from_slice(txid);
        resp
    }

    #[tokio::test]
    async fn stun_probe_calculates_loss_and_deduplicates_responses() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let stop = CancellationToken::new();
        let server_stop = stop.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 64];
            let mut count = 0u64;
            loop {
                tokio::select! {
                    _ = server_stop.cancelled() => break,
                    res = server.recv_from(&mut buf) => {
                        let Ok((len, peer)) = res else { break };
                        if len >= 20 {
                            count += 1;
                            if count == 5 {
                                // Intentionally drop every 5th packet to verify loss accounting.
                                continue;
                            }
                            let reply = build_binding_success(&buf[8..20]);
                            let _ = server.send_to(&reply, peer).await;
                            if count == 1 {
                                // Send a duplicate reply to verify deduplication.
                                let _ = server.send_to(&reply, peer).await;
                            }
                        }
                    }
                }
            }
        });

        let cancel = CancellationToken::new();
        let summary = run_udp_stun_probe("127.0.0.1", addr.port(), 10, IpVersion::Ipv4, &cancel)
            .await
            .unwrap();
        stop.cancel();

        assert!(summary.reachable);
        assert_eq!(summary.sent, 10);
        assert_eq!(summary.received, 9);
        assert!((summary.loss_ratio.unwrap() - 0.1).abs() < 1e-9);
        assert!(summary.median_ms.is_some());
        assert!(summary.jitter_ms.is_some());
    }

    #[tokio::test]
    async fn stun_probe_marks_silent_server_unreachable_instead_of_full_loss() {
        let blackhole = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = blackhole.local_addr().unwrap();
        let cancel = CancellationToken::new();

        let summary = run_udp_stun_probe("127.0.0.1", addr.port(), 25, IpVersion::Ipv4, &cancel)
            .await
            .unwrap();

        assert!(!summary.reachable);
        assert_eq!(summary.sent, UNREACHABLE_CUTOFF);
        assert_eq!(summary.received, 0);
        assert!(summary.loss_ratio.is_none());
    }
}
