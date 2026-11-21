use crate::engine::cloudflare::CloudflareClient;
use crate::stats;
use serde::Serialize;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Latency statistics collected across HTTP probes.
#[derive(Debug, Default, Serialize)]
pub struct LatencySummary {
    pub sent: u64,
    pub received: u64,
    pub min_ms: Option<f64>,
    pub median_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub max_ms: Option<f64>,
    pub jitter_ms: Option<f64>,
}

/// Runs periodic zero-byte `/__down` latency probes for `duration` or until `cancel` fires.
pub async fn run_latency_probes(
    client: &CloudflareClient,
    during: Option<&str>,
    duration: Duration,
    interval: Duration,
    probe_timeout: Duration,
    cancel: &CancellationToken,
) -> LatencySummary {
    let phase_start = Instant::now();
    let mut sent = 0u64;
    let mut received = 0u64;
    let mut samples = Vec::new();

    loop {
        if cancel.is_cancelled() || phase_start.elapsed() >= duration {
            break;
        }

        sent += 1;
        let url = client.build_down_url(0, during);
        let probe_start = Instant::now();

        let probe_res = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = async {
                let resp = client
                    .http
                    .get(url)
                    .timeout(probe_timeout)
                    .send()
                    .await?;
                let ok = resp.status().is_success();
                let _ = resp.bytes().await;
                Ok::<bool, reqwest::Error>(ok)
            } => res,
        };

        if matches!(probe_res, Ok(true)) {
            received += 1;
            samples.push(probe_start.elapsed().as_secs_f64() * 1000.0);
        }

        let Some(remaining) = duration.checked_sub(phase_start.elapsed()) else {
            break;
        };
        if remaining.is_zero() {
            break;
        }

        let sleep_for = interval.min(remaining);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(sleep_for) => {}
        }
    }

    summarize_latency(sent, received, &samples)
}

fn summarize_latency(sent: u64, received: u64, samples: &[f64]) -> LatencySummary {
    let stats_opt = stats::compute_sample_stats(samples);
    let jitter_ms = stats::compute_jitter(samples);

    LatencySummary {
        sent,
        received,
        min_ms: stats_opt.map(|s| s.min),
        median_ms: stats_opt.map(|s| s.median),
        p95_ms: stats_opt.map(|s| s.p95),
        p99_ms: stats_opt.map(|s| s.p99),
        max_ms: stats_opt.map(|s| s.max),
        jitter_ms,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn summarize_latency_handles_empty_and_populated_samples() {
        let empty = summarize_latency(5, 0, &[]);
        assert_eq!(empty.sent, 5);
        assert_eq!(empty.received, 0);
        assert!(empty.median_ms.is_none());

        let filled = summarize_latency(4, 3, &[10.0, 12.0, 14.0]);
        assert_eq!(filled.sent, 4);
        assert_eq!(filled.received, 3);
        assert!((filled.min_ms.unwrap() - 10.0).abs() < 1e-9);
        assert!((filled.median_ms.unwrap() - 12.0).abs() < 1e-9);
        assert!((filled.max_ms.unwrap() - 14.0).abs() < 1e-9);
        assert!((filled.jitter_ms.unwrap() - 2.0).abs() < 1e-9);
    }
}
