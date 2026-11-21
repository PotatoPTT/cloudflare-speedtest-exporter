use crate::config::Config;
use crate::engine::cloudflare::CloudflareClient;
use crate::engine::latency::{run_latency_probes, LatencySummary};
use crate::stats;
use anyhow::{bail, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

const UPLOAD_CHUNK_SIZE: usize = 65_536;
const MIN_DOWNLOAD_BYTES_PER_REQ: u64 = 100_000;
const WORKER_ERROR_BACKOFF: Duration = Duration::from_millis(100);

static UPLOAD_CHUNK: [u8; UPLOAD_CHUNK_SIZE] = [0u8; UPLOAD_CHUNK_SIZE];

/// Throughput measurement summary in bits per second and stability percentage.
#[derive(Debug, Serialize)]
pub struct ThroughputSummary {
    pub total_bytes: u64,
    pub steady_bps: f64,
    pub p95_bps: f64,
    pub p99_bps: f64,
    pub stability_cv_percent: Option<f64>,
}

/// Runs concurrent HTTP download workers alongside loaded latency probes.
pub async fn run_download(
    client: &Arc<CloudflareClient>,
    cfg: &Config,
    cancel: &CancellationToken,
) -> Result<(ThroughputSummary, LatencySummary)> {
    let phase_cancel = cancel.child_token();
    let total_bytes = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(cfg.concurrency);

    for _ in 0..cfg.concurrency {
        let worker_client = Arc::clone(client);
        let worker_total = Arc::clone(&total_bytes);
        let worker_cancel = phase_cancel.clone();
        let initial_bytes = cfg.download_bytes_per_req;

        handles.push(tokio::spawn(async move {
            download_worker(worker_client, worker_total, initial_bytes, worker_cancel).await;
        }));
    }

    let lat_handle = spawn_loaded_latency(
        client,
        "download",
        cfg.download_duration,
        cfg,
        &phase_cancel,
    );

    let (samples, rates_bps, active_duration) =
        sample_throughput(&total_bytes, cfg.download_duration, cancel).await;

    phase_cancel.cancel();
    for handle in handles {
        let _ = handle.await;
    }
    let loaded_latency = lat_handle.await.unwrap_or_default();

    let final_bytes = total_bytes.load(Ordering::Relaxed);
    if final_bytes == 0 {
        bail!("download phase transferred 0 bytes");
    }

    let summary = summarize_throughput(final_bytes, &samples, &rates_bps, active_duration);
    Ok((summary, loaded_latency))
}

/// Runs concurrent HTTP upload workers alongside loaded latency probes.
pub async fn run_upload(
    client: &Arc<CloudflareClient>,
    cfg: &Config,
    cancel: &CancellationToken,
) -> Result<(ThroughputSummary, LatencySummary)> {
    let phase_cancel = cancel.child_token();
    let total_bytes = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(cfg.concurrency);

    for _ in 0..cfg.concurrency {
        let worker_client = Arc::clone(client);
        let worker_total = Arc::clone(&total_bytes);
        let worker_cancel = phase_cancel.clone();
        let bytes_per_req = cfg.upload_bytes_per_req;

        handles.push(tokio::spawn(async move {
            upload_worker(worker_client, worker_total, bytes_per_req, worker_cancel).await;
        }));
    }

    let lat_handle =
        spawn_loaded_latency(client, "upload", cfg.upload_duration, cfg, &phase_cancel);

    let (samples, rates_bps, active_duration) =
        sample_throughput(&total_bytes, cfg.upload_duration, cancel).await;

    phase_cancel.cancel();
    for handle in handles {
        let _ = handle.await;
    }
    let loaded_latency = lat_handle.await.unwrap_or_default();

    let final_bytes = total_bytes.load(Ordering::Relaxed);
    if final_bytes == 0 {
        bail!("upload phase transferred 0 bytes");
    }

    let summary = summarize_throughput(final_bytes, &samples, &rates_bps, active_duration);
    Ok((summary, loaded_latency))
}

fn spawn_loaded_latency(
    client: &Arc<CloudflareClient>,
    phase_name: &'static str,
    duration: Duration,
    cfg: &Config,
    phase_cancel: &CancellationToken,
) -> JoinHandle<LatencySummary> {
    let lat_client = Arc::clone(client);
    let lat_cancel = phase_cancel.clone();
    let probe_interval = cfg.probe_interval;
    let probe_timeout = cfg.probe_timeout;

    tokio::spawn(async move {
        run_latency_probes(
            &lat_client,
            Some(phase_name),
            duration,
            probe_interval,
            probe_timeout,
            &lat_cancel,
        )
        .await
    })
}

async fn download_worker(
    client: Arc<CloudflareClient>,
    total_bytes: Arc<AtomicU64>,
    mut bytes_per_req: u64,
    cancel: CancellationToken,
) {
    while !cancel.is_cancelled() {
        let url = client.build_down_url(bytes_per_req, None);
        let resp_result = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = client.http.get(url).send() => res,
        };

        let resp = match resp_result {
            Ok(resp) => resp,
            Err(err) => {
                debug!("Download worker request failed: {err}");
                sleep_or_cancel(WORKER_ERROR_BACKOFF, &cancel).await;
                continue;
            }
        };

        if !resp.status().is_success() {
            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                let reduced = (bytes_per_req / 2).max(MIN_DOWNLOAD_BYTES_PER_REQ);
                if reduced < bytes_per_req {
                    debug!("Download 429 received; reducing bytes per request to {reduced}");
                    bytes_per_req = reduced;
                }
            }
            sleep_or_cancel(WORKER_ERROR_BACKOFF, &cancel).await;
            continue;
        }

        let mut stream = resp.bytes_stream();
        loop {
            let next_chunk = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                item = stream.next() => item,
            };

            match next_chunk {
                Some(Ok(chunk)) => {
                    total_bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                }
                Some(Err(_)) | None => break,
            }
        }
    }
}

async fn upload_worker(
    client: Arc<CloudflareClient>,
    total_bytes: Arc<AtomicU64>,
    bytes_per_req: u64,
    cancel: CancellationToken,
) {
    let up_url = client.build_up_url();

    while !cancel.is_cancelled() {
        let req_counted = Arc::new(AtomicU64::new(0));
        let body_stream = futures_util::stream::unfold(bytes_per_req, {
            let total_ref = Arc::clone(&total_bytes);
            let counted_ref = Arc::clone(&req_counted);
            move |remaining| {
                let total_inner = Arc::clone(&total_ref);
                let counted_inner = Arc::clone(&counted_ref);
                async move {
                    if remaining == 0 {
                        return None;
                    }
                    let n = remaining.min(UPLOAD_CHUNK_SIZE as u64);
                    total_inner.fetch_add(n, Ordering::Relaxed);
                    counted_inner.fetch_add(n, Ordering::Relaxed);
                    let chunk = Bytes::from_static(&UPLOAD_CHUNK[..n as usize]);
                    Some((Ok::<Bytes, std::io::Error>(chunk), remaining - n))
                }
            }
        });
        let body = reqwest::Body::wrap_stream(body_stream);

        let req_fut = async {
            let resp = client.http.post(up_url.clone()).body(body).send().await?;
            let ok = resp.status().is_success();
            // Consume full body so the underlying HTTP keep-alive connection returns to the pool.
            let _ = resp.bytes().await;
            Ok::<bool, reqwest::Error>(ok)
        };

        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            res = req_fut => {
                if !matches!(res, Ok(true)) {
                    let rolled_back = req_counted.load(Ordering::Relaxed);
                    subtract_saturating(&total_bytes, rolled_back);
                    debug!(
                        rolled_back,
                        "Upload worker request failed or returned non-2xx; rolled back bytes"
                    );
                    sleep_or_cancel(WORKER_ERROR_BACKOFF, &cancel).await;
                }
            }
        }
    }
}

async fn sample_throughput(
    total_bytes: &AtomicU64,
    duration: Duration,
    cancel: &CancellationToken,
) -> (Vec<(Duration, u64)>, Vec<f64>, Duration) {
    let start = Instant::now();
    let mut last_bytes = 0u64;
    let mut samples = Vec::with_capacity(64);
    let mut rates_bps = Vec::with_capacity(64);

    loop {
        let Some(remaining) = duration.checked_sub(start.elapsed()) else {
            break;
        };
        if remaining.is_zero() || cancel.is_cancelled() {
            break;
        }

        let tick_len = stats::THROUGHPUT_SAMPLE_INTERVAL.min(remaining);
        let tick_start = Instant::now();
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(tick_len) => {}
        }

        let actual_dt = tick_start.elapsed().as_secs_f64().max(1e-9);
        let now_total = total_bytes.load(Ordering::Relaxed);
        let delta_bytes = now_total.saturating_sub(last_bytes);
        let inst_bps = (delta_bytes as f64 * 8.0) / actual_dt;

        last_bytes = now_total;
        samples.push((start.elapsed(), now_total));
        rates_bps.push(inst_bps);
    }

    (samples, rates_bps, start.elapsed())
}

fn summarize_throughput(
    total_bytes: u64,
    samples: &[(Duration, u64)],
    rates_bps: &[f64],
    active_duration: Duration,
) -> ThroughputSummary {
    let (steady_bytes, steady_window, steady_rates) = stats::steady_state_window(
        samples,
        rates_bps,
        active_duration,
    )
    .unwrap_or((total_bytes, active_duration, rates_bps));

    let secs = steady_window.as_secs_f64().max(1e-9);
    let steady_bps = (steady_bytes as f64 * 8.0) / secs;

    let (p95_bps, p99_bps) = stats::compute_sample_stats(steady_rates)
        .map(|s| (s.p95, s.p99))
        .unwrap_or((steady_bps, steady_bps));

    let stability_cv_percent = stats::compute_cv_percent(steady_rates);

    ThroughputSummary {
        total_bytes,
        steady_bps,
        p95_bps,
        p99_bps,
        stability_cv_percent,
    }
}

fn subtract_saturating(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        Some(cur.saturating_sub(amount))
    });
}

async fn sleep_or_cancel(delay: Duration, cancel: &CancellationToken) {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {}
        _ = tokio::time::sleep(delay) => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::IpVersion;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    fn test_config(base_url: &str, overrides: &[(&str, &str)]) -> Config {
        let mut map: HashMap<&str, &str> = overrides.iter().copied().collect();
        map.entry("SPEEDTEST_BASE_URL").or_insert(base_url);
        map.entry("SPEEDTEST_CONCURRENCY").or_insert("1");
        map.entry("SPEEDTEST_DOWNLOAD_SECONDS").or_insert("1");
        map.entry("SPEEDTEST_UPLOAD_SECONDS").or_insert("1");
        map.entry("SPEEDTEST_PROBE_INTERVAL_MS").or_insert("500");
        Config::from_lookup(|k| map.get(k).map(|v| (*v).to_string())).unwrap()
    }

    async fn handle_mock_http_conn(
        stream: TcpStream,
        peer: std::net::SocketAddr,
        down_sizes: Arc<Mutex<Vec<String>>>,
        upload_peers: Arc<Mutex<Vec<std::net::SocketAddr>>>,
        fail_uploads: bool,
    ) {
        let (reader, mut writer) = stream.into_split();
        let mut buf_reader = BufReader::new(reader);

        loop {
            let mut request_line = String::new();
            match buf_reader.read_line(&mut request_line).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
            if request_line.trim().is_empty() {
                continue;
            }

            let mut content_length = 0usize;
            let mut chunked = false;
            loop {
                let mut header_line = String::new();
                if buf_reader.read_line(&mut header_line).await.unwrap_or(0) == 0 {
                    return;
                }
                let trimmed = header_line.trim();
                if trimmed.is_empty() {
                    break;
                }
                if let Some((name, val)) = trimmed.split_once(':') {
                    if name.eq_ignore_ascii_case("content-length") {
                        content_length = val.trim().parse::<usize>().unwrap_or(0);
                    } else if name.eq_ignore_ascii_case("transfer-encoding")
                        && val.to_ascii_lowercase().contains("chunked")
                    {
                        chunked = true;
                    }
                }
            }

            if chunked {
                loop {
                    let mut size_line = String::new();
                    if buf_reader.read_line(&mut size_line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let hex_size = size_line.split(';').next().unwrap_or("").trim();
                    let size = usize::from_str_radix(hex_size, 16).unwrap_or(0);
                    if size == 0 {
                        let mut crlf = [0u8; 2];
                        let _ = buf_reader.read_exact(&mut crlf).await;
                        break;
                    }
                    let mut chunk = vec![0u8; size + 2];
                    if buf_reader.read_exact(&mut chunk).await.is_err() {
                        return;
                    }
                }
            } else if content_length > 0 {
                let mut body = vec![0u8; content_length];
                if buf_reader.read_exact(&mut body).await.is_err() {
                    return;
                }
            }

            let path = request_line.split_whitespace().nth(1).unwrap_or("/");
            if path.starts_with("/__down") {
                let is_probe = path.contains("bytes=0");
                if is_probe {
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                    if writer.write_all(resp.as_bytes()).await.is_err() {
                        break;
                    }
                } else {
                    let mut sizes = down_sizes.lock().unwrap();
                    sizes.push(path.to_string());
                    if sizes.len() == 1 {
                        let resp = "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n";
                        if writer.write_all(resp.as_bytes()).await.is_err() {
                            break;
                        }
                    } else {
                        let payload = vec![b'x'; 1024];
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                            payload.len()
                        );
                        if writer.write_all(header.as_bytes()).await.is_err()
                            || writer.write_all(&payload).await.is_err()
                        {
                            break;
                        }
                    }
                }
            } else if path.starts_with("/__up") {
                upload_peers.lock().unwrap().push(peer);
                let resp = if fail_uploads {
                    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 2\r\n\r\nno"
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
                };
                if writer.write_all(resp.as_bytes()).await.is_err() {
                    break;
                }
            } else {
                let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
                if writer.write_all(resp.as_bytes()).await.is_err() {
                    break;
                }
            }
        }
    }

    async fn start_mock_server(
        fail_uploads: bool,
    ) -> (
        String,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Vec<std::net::SocketAddr>>>,
        CancellationToken,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let down_sizes = Arc::new(Mutex::new(Vec::new()));
        let upload_peers = Arc::new(Mutex::new(Vec::new()));
        let server_cancel = CancellationToken::new();

        let d = Arc::clone(&down_sizes);
        let u = Arc::clone(&upload_peers);
        let c = server_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = c.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, peer)) = accepted else { break };
                        let d2 = Arc::clone(&d);
                        let u2 = Arc::clone(&u);
                        tokio::spawn(handle_mock_http_conn(stream, peer, d2, u2, fail_uploads));
                    }
                }
            }
        });

        (
            format!("http://{addr}"),
            down_sizes,
            upload_peers,
            server_cancel,
        )
    }

    #[tokio::test]
    async fn download_reduces_request_size_on_429() {
        let (base_url, down_paths, _uploads, server_cancel) = start_mock_server(false).await;
        let cfg = test_config(&base_url, &[("SPEEDTEST_DOWNLOAD_BYTES_PER_REQ", "400000")]);
        let client = Arc::new(
            CloudflareClient::new(&cfg.base_url, "test".to_string(), &[], IpVersion::Auto).unwrap(),
        );
        let cancel = CancellationToken::new();

        let (dl, _lat) = run_download(&client, &cfg, &cancel).await.unwrap();
        server_cancel.cancel();

        assert!(dl.total_bytes > 0);
        let paths = down_paths.lock().unwrap();
        assert!(paths.iter().any(|p| p.contains("bytes=400000")));
        assert!(paths.iter().any(|p| p.contains("bytes=200000")));
    }

    #[tokio::test]
    async fn upload_reuses_keep_alive_connection() {
        let (base_url, _downs, upload_peers, server_cancel) = start_mock_server(false).await;
        let cfg = test_config(&base_url, &[("SPEEDTEST_UPLOAD_BYTES_PER_REQ", "128")]);
        let client = Arc::new(
            CloudflareClient::new(&cfg.base_url, "test".to_string(), &[], IpVersion::Auto).unwrap(),
        );
        let cancel = CancellationToken::new();

        let (ul, _lat) = run_upload(&client, &cfg, &cancel).await.unwrap();
        server_cancel.cancel();

        assert!(ul.total_bytes >= 256);
        let peers = upload_peers.lock().unwrap();
        assert!(
            peers.len() >= 2,
            "expected >= 2 upload requests, got {peers:?}"
        );
        let unique: HashSet<_> = peers.iter().copied().collect();
        assert_eq!(unique.len(), 1, "expected HTTP keep-alive reuse: {peers:?}");
    }

    #[tokio::test]
    async fn upload_rolls_back_bytes_when_server_returns_500() {
        let (base_url, _downs, _upload_peers, server_cancel) = start_mock_server(true).await;
        let cfg = test_config(&base_url, &[("SPEEDTEST_UPLOAD_BYTES_PER_REQ", "128")]);
        let client = Arc::new(
            CloudflareClient::new(&cfg.base_url, "test".to_string(), &[], IpVersion::Auto).unwrap(),
        );
        let cancel = CancellationToken::new();

        let res = run_upload(&client, &cfg, &cancel).await;
        server_cancel.cancel();

        assert!(
            res.is_err(),
            "expected error when all uploads fail with 500"
        );
    }
}
