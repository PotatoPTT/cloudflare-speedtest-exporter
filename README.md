# Cloudflare Speedtest Exporter

[![CI](https://github.com/PotatoPTT/cloudflare-speedtest-exporter/actions/workflows/ci.yml/badge.svg)](https://github.com/PotatoPTT/cloudflare-speedtest-exporter/actions/workflows/ci.yml)
[![Release](https://github.com/PotatoPTT/cloudflare-speedtest-exporter/actions/workflows/release.yml/badge.svg)](https://github.com/PotatoPTT/cloudflare-speedtest-exporter/actions/workflows/release.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85+-orange.svg)](https://www.rust-lang.org/)
[![Docker Multi-Arch](https://img.shields.io/badge/arch-amd64%20%7C%20arm64%20%7C%20arm%2Fv7-2496ED.svg)](https://github.com/PotatoPTT/cloudflare-speedtest-exporter/pkgs/container/cloudflare-speedtest-exporter)

A fast, lightweight, multi-architecture **Prometheus exporter** written in **Rust** that measures internet performance directly against **Cloudflare Speedtest** (`speed.cloudflare.com`) — complete with **loaded latency (bufferbloat)**, **DNS/TLS timing**, **throughput stability**, and **UDP packet loss via STUN**.

Built as a reliable, zero-CLI-dependency alternative to Ookla-based exporters: no flaky regional ISP test servers, no proprietary binary rate limits (`429`), and clean failure handling that never pollutes your Grafana charts with fake `0 Mbps` drops.

---

## Why Cloudflare Speedtest + Rust?

- **Anycast reliability:** Tests hit Cloudflare's global edge (`speed.cloudflare.com`) instead of rotating through third-party ISP servers that periodically hang or reject sockets.
- **No external CLI subprocesses:** Single self-contained Rust binary using `tokio`, `reqwest` (`rustls`), and `axum` — tiny memory footprint and fast cold starts.
- **First-class `linux/arm/v7` support:** Cross-compiled cleanly for `amd64`, `arm64`, and `armv7` (great for Raspberry Pi and ARM SBC homelabs).
- **Drop-in Grafana compatibility:** Exposes core `speedtest_*` metrics (`speedtest_download_bits_per_second`, `speedtest_upload_bits_per_second`, `speedtest_ping_latency_milliseconds`, `speedtest_jitter_latency_milliseconds`, `speedtest_up`) compatible with popular dashboards.
- **Rich network diagnostics:**
  - **Steady-state throughput:** Excludes TCP slow-start ramp-up (`max(20%, 1s)`), plus `p95`, `p99`, and Coefficient of Variation (`CV%`).
  - **Bufferbloat detection:** Concurrent HTTP latency probes running *during* download and upload phases.
  - **DNS & TLS timing:** Isolated DNS lookup and TLS handshake measurements.
  - **UDP quality via STUN:** Lightweight RFC 5389 Binding probes against `stun.cloudflare.com:3478` with automatic unreachable detection so blocked UDP never reports fake `100%` loss.
- **Dual execution modes:**
  - **On-scrape (default):** Runs on `/metrics` scrape with TTL caching and **singleflight** protection (concurrent scrapes never trigger duplicate tests).
  - **Background scheduler:** Set `SPEEDTEST_INTERVAL` (e.g. `1800`) to run tests on a timer and serve `/metrics` in `<1ms`.
- **Clean failure semantics:** Retries transient network errors with backoff; if a run still fails, sets `speedtest_up 0`, increments `speedtest_errors_total`, and omits measurement gauges for that scrape instead of writing `0 Mbps`.
- **Low cardinality & privacy-safe:** No client IP addresses are attached to Prometheus labels.

---

## Quick Start

### Run a one-off speedtest via Docker

```bash
docker run --rm ghcr.io/potatoptt/cloudflare-speedtest-exporter:latest --once
```

### Run as a Prometheus exporter

```bash
docker run -d \
  --name cloudflare-speedtest-exporter \
  --restart unless-stopped \
  -p 9798:9798 \
  -e SPEEDTEST_CACHE_DURATION=300 \
  ghcr.io/potatoptt/cloudflare-speedtest-exporter:latest
```

### Docker Compose

#### Option A: On-scrape mode (default, compatible with `scrape_interval: 30m`)

```yaml
services:
  speedtest:
    image: ghcr.io/potatoptt/cloudflare-speedtest-exporter:latest
    container_name: speedtest
    restart: unless-stopped
    ports:
      - "9798:9798"
    environment:
      SPEEDTEST_CACHE_DURATION: 300
      SPEEDTEST_TIMEOUT: 90
      SPEEDTEST_RETRIES: 2
```

#### Option B: Background scheduler mode (instant `/metrics` responses)

```yaml
services:
  speedtest:
    image: ghcr.io/potatoptt/cloudflare-speedtest-exporter:latest
    container_name: speedtest
    restart: unless-stopped
    ports:
      - "9798:9798"
    environment:
      SPEEDTEST_INTERVAL: 1800 # Run every 30 minutes in the background
      SPEEDTEST_TIMEOUT: 90
      SPEEDTEST_RETRIES: 2
```

### Prometheus Configuration

In **on-scrape** mode, match `scrape_interval` to your desired test cadence (e.g. `30m`) with a generous `scrape_timeout`:

```yaml
scrape_configs:
  - job_name: "speedtest"
    scrape_interval: 30m
    scrape_timeout: 120s
    static_configs:
      - targets: ["speedtest:9798"]
```

In **background scheduler** mode (`SPEEDTEST_INTERVAL=1800`), Prometheus can scrape at your normal interval (e.g. `60s`) with minimal timeout since `/metrics` responds instantaneously from memory.

---

## Configuration

All settings are configured via environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `SPEEDTEST_PORT` | `9798` | HTTP listen port |
| `SPEEDTEST_BIND_ADDR` | `0.0.0.0` | HTTP bind address |
| `SPEEDTEST_CACHE_DURATION` | `300` | Cache TTL in seconds for on-scrape mode (`0` disables caching) |
| `SPEEDTEST_INTERVAL` | `0` | `0` = on-scrape mode; `> 0` = background test interval in seconds |
| `SPEEDTEST_TIMEOUT` | `90` | Global timeout in seconds per test run (including retries) |
| `SPEEDTEST_RETRIES` | `2` | Retry count on transient failures (`2s`, `5s`, `10s` backoff) |
| `SPEEDTEST_CONCURRENCY` | `6` | Parallel connections during download and upload phases |
| `SPEEDTEST_IDLE_LATENCY_SECONDS` | `2` | Duration of the idle latency probe phase |
| `SPEEDTEST_DOWNLOAD_SECONDS` | `10` | Duration of the download phase |
| `SPEEDTEST_UPLOAD_SECONDS` | `10` | Duration of the upload phase |
| `SPEEDTEST_DOWNLOAD_BYTES_PER_REQ` | `10000000` | Initial request size in bytes for `/__down` (auto-reduces on HTTP `429`) |
| `SPEEDTEST_UPLOAD_BYTES_PER_REQ` | `5000000` | Request payload size in bytes for `/__up` |
| `SPEEDTEST_PROBE_INTERVAL_MS` | `250` | Interval between HTTP latency probes in milliseconds |
| `SPEEDTEST_PROBE_TIMEOUT_MS` | `2000` | Per-probe HTTP latency timeout in milliseconds |
| `SPEEDTEST_UDP_ENABLED` | `true` | Enable UDP packet loss and RTT probing via STUN |
| `SPEEDTEST_UDP_PROBES` | `25` | Number of STUN Binding requests to send |
| `SPEEDTEST_STUN_HOST` | `stun.cloudflare.com` | STUN server hostname for UDP probes |
| `SPEEDTEST_STUN_PORT` | `3478` | STUN server UDP port |
| `SPEEDTEST_IP_VERSION` | `auto` | IP address family restriction (`auto`, `ipv4`, `ipv6`) |
| `SPEEDTEST_BASE_URL` | `https://speed.cloudflare.com` | Base URL for Cloudflare Speedtest endpoints |
| `RUST_LOG` | `info` | Log verbosity (`error`, `warn`, `info`, `debug`, `trace`) |

---

## CLI Flags & HTTP Endpoints

### CLI Flags

- *(no flags)* — Starts the Prometheus HTTP server.
- `--once` — Runs one speedtest, prints a human-readable summary, and exits.
- `--once-json` — Runs one speedtest, prints pretty JSON, and exits.
- `--healthcheck` — Probes `http://127.0.0.1:$SPEEDTEST_PORT/health` (used by `Dockerfile` `HEALTHCHECK`).
- `-V`, `--version` — Prints version and exits.
- `-h`, `--help` — Prints usage information.

### HTTP Endpoints

- `GET /` — Status overview with links to `/metrics` and `/health`.
- `GET /health` — Lightweight liveness check (`200 OK`).
- `GET /metrics` — Prometheus exposition endpoint.

---

## Prometheus Metrics

### Always Emitted

| Metric | Type | Description |
| --- | --- | --- |
| `speedtest_up` | Gauge | `1` if the last test succeeded, `0` otherwise |
| `speedtest_errors_total` | Counter | Total failed test cycles after exhausting retries |
| `speedtest_retries_total` | Counter | Total retry attempts executed |
| `speedtest_last_run_timestamp_seconds` | Gauge | Unix timestamp of the last test attempt *(after 1st run)* |
| `speedtest_last_success_timestamp_seconds` | Gauge | Unix timestamp of the last successful test *(after 1st success)* |

### Emitted on Successful Runs

| Metric | Type | Description |
| --- | --- | --- |
| `speedtest_download_bits_per_second` | Gauge | Steady-state download throughput (bps) |
| `speedtest_download_p95_bits_per_second` | Gauge | 95th percentile steady-state download throughput (bps) |
| `speedtest_download_p99_bits_per_second` | Gauge | 99th percentile steady-state download throughput (bps) |
| `speedtest_download_stability_cv_percent` | Gauge | Download throughput coefficient of variation (%) |
| `speedtest_download_bytes` | Gauge | Total bytes transferred during download phase |
| `speedtest_upload_bits_per_second` | Gauge | Steady-state upload throughput (bps) |
| `speedtest_upload_p95_bits_per_second` | Gauge | 95th percentile steady-state upload throughput (bps) |
| `speedtest_upload_p99_bits_per_second` | Gauge | 99th percentile steady-state upload throughput (bps) |
| `speedtest_upload_stability_cv_percent` | Gauge | Upload throughput coefficient of variation (%) |
| `speedtest_upload_bytes` | Gauge | Total bytes transferred during upload phase |
| `speedtest_ping_latency_milliseconds` | Gauge | Idle ping latency median (ms) |
| `speedtest_jitter_latency_milliseconds` | Gauge | Idle ping jitter / sample stddev (ms) |
| `speedtest_ping_latency_min_milliseconds` | Gauge | Minimum idle ping latency (ms) |
| `speedtest_ping_latency_p95_milliseconds` | Gauge | 95th percentile idle ping latency (ms) |
| `speedtest_ping_latency_p99_milliseconds` | Gauge | 99th percentile idle ping latency (ms) |
| `speedtest_ping_latency_max_milliseconds` | Gauge | Maximum idle ping latency (ms) |
| `speedtest_loaded_download_latency_milliseconds` | Gauge | Median ping latency during download load (ms) |
| `speedtest_loaded_download_jitter_milliseconds` | Gauge | Ping jitter during download load (ms) |
| `speedtest_loaded_download_latency_p95_milliseconds` | Gauge | 95th percentile ping latency during download load (ms) |
| `speedtest_loaded_download_latency_p99_milliseconds` | Gauge | 99th percentile ping latency during download load (ms) |
| `speedtest_loaded_upload_latency_milliseconds` | Gauge | Median ping latency during upload load (ms) |
| `speedtest_loaded_upload_jitter_milliseconds` | Gauge | Ping jitter during upload load (ms) |
| `speedtest_loaded_upload_latency_p95_milliseconds` | Gauge | 95th percentile ping latency during upload load (ms) |
| `speedtest_loaded_upload_latency_p99_milliseconds` | Gauge | 99th percentile ping latency during upload load (ms) |
| `speedtest_dns_resolution_milliseconds` | Gauge | DNS resolution latency for `speed.cloudflare.com` (ms) |
| `speedtest_tls_handshake_milliseconds` | Gauge | TLS handshake latency excluding TCP connect (ms) |
| `speedtest_udp_reachable` | Gauge | `1` if STUN UDP probing succeeded, `0` otherwise |
| `speedtest_packet_loss_ratio` | Gauge | UDP packet loss ratio (`0.0` to `1.0`), when reachable |
| `speedtest_udp_latency_milliseconds` | Gauge | Median UDP STUN RTT (ms), when reachable |
| `speedtest_udp_jitter_milliseconds` | Gauge | UDP STUN jitter (ms), when reachable |
| `speedtest_duration_seconds` | Gauge | Total duration of the test run (s) |
| `speedtest_info` | Gauge | Labels: `colo`, `colo_city`, `country`, `asn`, `isp` (value `1`) |

---

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

---

## Acknowledgments & Transparency

- **Developed with AI assistance:** This repository was designed and implemented with AI assistance, keeping clean-room Rust code under the MIT license.
- **Engine & protocol inspiration:** Measurement techniques and Cloudflare Speedtest edge-case mitigations (required `Referer` header, metadata fallback chain, steady-state ramp-up exclusion, backpressure-aware upload streaming with HTTP keep-alive reuse, adaptive `429` chunk sizing, and STUN UDP packet loss probing) were inspired by **[kavehtehrani/cloudflare-speed-cli](https://github.com/kavehtehrani/cloudflare-speed-cli)**.
- **Prometheus metric conventions:** Core metric names were kept compatible with **[tzockt/speedtest-exporter](https://github.com/tzockt/speedtest-exporter)** and **[miguelndecarvalho/speedtest-exporter](https://github.com/miguelndecarvalho/speedtest-exporter)** for smooth migration of existing Grafana dashboards.

## License

Licensed under the [MIT License](LICENSE).
