use crate::coordinator::Snapshot;
use crate::engine::SpeedtestResult;
use std::fmt::Write;

/// Renders exporter state and latest measurement result in Prometheus exposition format.
pub fn render_prometheus(snapshot: &Snapshot) -> String {
    let mut out = String::with_capacity(4096);

    let up_val = u64::from(snapshot.up == Some(true));
    write_gauge_u64(
        &mut out,
        "speedtest_up",
        "Speedtest status (1 if the last run succeeded, 0 otherwise)",
        up_val,
    );
    write_counter_u64(
        &mut out,
        "speedtest_errors_total",
        "Total number of failed speedtest runs after retries",
        snapshot.errors_total,
    );
    write_counter_u64(
        &mut out,
        "speedtest_retries_total",
        "Total number of speedtest retry attempts",
        snapshot.retries_total,
    );

    if let Some(ts) = snapshot.last_run_unix {
        write_gauge_u64(
            &mut out,
            "speedtest_last_run_timestamp_seconds",
            "Unix timestamp of the last speedtest attempt",
            ts,
        );
    }
    if let Some(ts) = snapshot.last_success_unix {
        write_gauge_u64(
            &mut out,
            "speedtest_last_success_timestamp_seconds",
            "Unix timestamp of the last successful speedtest",
            ts,
        );
    }

    if snapshot.up == Some(true) {
        if let Some(res) = &snapshot.last_result {
            render_result_metrics(&mut out, res);
        }
    }

    out
}

fn render_result_metrics(out: &mut String, res: &SpeedtestResult) {
    // Drop-in core metrics matching standard Prometheus speedtest dashboards
    write_gauge_f64(
        out,
        "speedtest_download_bits_per_second",
        "Measured steady-state download speed in bits per second",
        res.download.steady_bps,
    );
    write_gauge_f64(
        out,
        "speedtest_upload_bits_per_second",
        "Measured steady-state upload speed in bits per second",
        res.upload.steady_bps,
    );
    if let Some(ping) = res.idle_latency.median_ms {
        write_gauge_f64(
            out,
            "speedtest_ping_latency_milliseconds",
            "Idle ping latency median in milliseconds",
            ping,
        );
    }
    if let Some(jitter) = res.idle_latency.jitter_ms {
        write_gauge_f64(
            out,
            "speedtest_jitter_latency_milliseconds",
            "Idle ping latency jitter (sample standard deviation) in milliseconds",
            jitter,
        );
    }

    // Extended throughput percentiles and stability
    write_gauge_f64(
        out,
        "speedtest_download_p95_bits_per_second",
        "95th percentile steady-state download speed in bits per second",
        res.download.p95_bps,
    );
    write_gauge_f64(
        out,
        "speedtest_download_p99_bits_per_second",
        "99th percentile steady-state download speed in bits per second",
        res.download.p99_bps,
    );
    if let Some(cv) = res.download.stability_cv_percent {
        write_gauge_f64(
            out,
            "speedtest_download_stability_cv_percent",
            "Download throughput coefficient of variation in percent",
            cv,
        );
    }
    write_gauge_u64(
        out,
        "speedtest_download_bytes",
        "Total bytes transferred during the download phase",
        res.download.total_bytes,
    );

    write_gauge_f64(
        out,
        "speedtest_upload_p95_bits_per_second",
        "95th percentile steady-state upload speed in bits per second",
        res.upload.p95_bps,
    );
    write_gauge_f64(
        out,
        "speedtest_upload_p99_bits_per_second",
        "99th percentile steady-state upload speed in bits per second",
        res.upload.p99_bps,
    );
    if let Some(cv) = res.upload.stability_cv_percent {
        write_gauge_f64(
            out,
            "speedtest_upload_stability_cv_percent",
            "Upload throughput coefficient of variation in percent",
            cv,
        );
    }
    write_gauge_u64(
        out,
        "speedtest_upload_bytes",
        "Total bytes transferred during the upload phase",
        res.upload.total_bytes,
    );

    // Detailed idle latency quantiles
    if let Some(v) = res.idle_latency.min_ms {
        write_gauge_f64(
            out,
            "speedtest_ping_latency_min_milliseconds",
            "Minimum idle ping latency in milliseconds",
            v,
        );
    }
    if let Some(v) = res.idle_latency.p95_ms {
        write_gauge_f64(
            out,
            "speedtest_ping_latency_p95_milliseconds",
            "95th percentile idle ping latency in milliseconds",
            v,
        );
    }
    if let Some(v) = res.idle_latency.p99_ms {
        write_gauge_f64(
            out,
            "speedtest_ping_latency_p99_milliseconds",
            "99th percentile idle ping latency in milliseconds",
            v,
        );
    }
    if let Some(v) = res.idle_latency.max_ms {
        write_gauge_f64(
            out,
            "speedtest_ping_latency_max_milliseconds",
            "Maximum idle ping latency in milliseconds",
            v,
        );
    }

    // Loaded latency during download and upload (bufferbloat)
    if let Some(v) = res.loaded_download_latency.median_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_download_latency_milliseconds",
            "Median ping latency under download load in milliseconds",
            v,
        );
    }
    if let Some(v) = res.loaded_download_latency.jitter_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_download_jitter_milliseconds",
            "Ping jitter under download load in milliseconds",
            v,
        );
    }
    if let Some(v) = res.loaded_download_latency.p95_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_download_latency_p95_milliseconds",
            "95th percentile ping latency under download load in milliseconds",
            v,
        );
    }
    if let Some(v) = res.loaded_download_latency.p99_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_download_latency_p99_milliseconds",
            "99th percentile ping latency under download load in milliseconds",
            v,
        );
    }

    if let Some(v) = res.loaded_upload_latency.median_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_upload_latency_milliseconds",
            "Median ping latency under upload load in milliseconds",
            v,
        );
    }
    if let Some(v) = res.loaded_upload_latency.jitter_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_upload_jitter_milliseconds",
            "Ping jitter under upload load in milliseconds",
            v,
        );
    }
    if let Some(v) = res.loaded_upload_latency.p95_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_upload_latency_p95_milliseconds",
            "95th percentile ping latency under upload load in milliseconds",
            v,
        );
    }
    if let Some(v) = res.loaded_upload_latency.p99_ms {
        write_gauge_f64(
            out,
            "speedtest_loaded_upload_latency_p99_milliseconds",
            "99th percentile ping latency under upload load in milliseconds",
            v,
        );
    }

    // DNS, TLS, UDP STUN diagnostics, and overall duration
    write_gauge_f64(
        out,
        "speedtest_dns_resolution_milliseconds",
        "DNS resolution duration for speed.cloudflare.com in milliseconds",
        res.dns_ms,
    );
    if let Some(tls_ms) = res.tls_ms {
        write_gauge_f64(
            out,
            "speedtest_tls_handshake_milliseconds",
            "TLS handshake duration in milliseconds",
            tls_ms,
        );
    }

    if let Some(udp) = &res.udp {
        write_gauge_u64(
            out,
            "speedtest_udp_reachable",
            "Whether Cloudflare UDP STUN probing succeeded (1 if reachable, 0 otherwise)",
            u64::from(udp.reachable),
        );
        if udp.reachable {
            if let Some(loss) = udp.loss_ratio {
                write_gauge_f64(
                    out,
                    "speedtest_packet_loss_ratio",
                    "UDP packet loss ratio (0.0 to 1.0) measured via STUN Binding probes",
                    loss,
                );
            }
            if let Some(rtt) = udp.median_ms {
                write_gauge_f64(
                    out,
                    "speedtest_udp_latency_milliseconds",
                    "Median UDP STUN RTT in milliseconds",
                    rtt,
                );
            }
            if let Some(jitter) = udp.jitter_ms {
                write_gauge_f64(
                    out,
                    "speedtest_udp_jitter_milliseconds",
                    "UDP STUN jitter in milliseconds",
                    jitter,
                );
            }
        }
    }

    write_gauge_f64(
        out,
        "speedtest_duration_seconds",
        "Total execution duration of the speedtest run in seconds",
        res.duration_secs,
    );

    // Edge & ISP metadata info gauge (low cardinality, no client IP)
    let _ = writeln!(
        out,
        "# HELP speedtest_info Metadata about the Cloudflare edge PoP and client ISP"
    );
    let _ = writeln!(out, "# TYPE speedtest_info gauge");
    let _ = writeln!(
        out,
        "speedtest_info{{colo=\"{}\",colo_city=\"{}\",country=\"{}\",asn=\"{}\",isp=\"{}\"}} 1",
        escape_label(res.meta.colo.as_deref().unwrap_or("")),
        escape_label(res.meta.colo_city.as_deref().unwrap_or("")),
        escape_label(res.meta.country.as_deref().unwrap_or("")),
        escape_label(res.meta.asn.as_deref().unwrap_or("")),
        escape_label(res.meta.isp.as_deref().unwrap_or("")),
    );
}

fn write_gauge_f64(out: &mut String, name: &str, help: &str, value: f64) {
    if !value.is_finite() {
        return;
    }
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn write_gauge_u64(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn write_counter_u64(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

fn escape_label(raw: &str) -> String {
    let mut escaped = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::engine::cloudflare::CloudflareMeta;
    use crate::engine::latency::LatencySummary;
    use crate::engine::throughput::ThroughputSummary;
    use crate::engine::udp_stun::UdpSummary;

    fn sample_snapshot() -> Snapshot {
        Snapshot {
            up: Some(true),
            errors_total: 1,
            retries_total: 2,
            last_run_unix: Some(1_700_000_000),
            last_success_unix: Some(1_700_000_000),
            last_success_at: None,
            last_result: Some(SpeedtestResult {
                meta: CloudflareMeta {
                    colo: Some("GRU".to_string()),
                    colo_city: Some("Sao Paulo".to_string()),
                    country: Some("BR".to_string()),
                    asn: Some("28220".to_string()),
                    isp: Some("ISP \"Test\"\\Net\n".to_string()),
                },
                dns_ms: 3.5,
                tls_ms: Some(11.2),
                idle_latency: LatencySummary {
                    sent: 8,
                    received: 8,
                    min_ms: Some(9.5),
                    median_ms: Some(11.0),
                    p95_ms: Some(12.5),
                    p99_ms: Some(13.0),
                    max_ms: Some(13.2),
                    jitter_ms: Some(0.9),
                },
                download: ThroughputSummary {
                    total_bytes: 120_000_000,
                    steady_bps: 108_000_000.0,
                    p95_bps: 112_000_000.0,
                    p99_bps: 115_000_000.0,
                    stability_cv_percent: Some(2.5),
                },
                loaded_download_latency: LatencySummary {
                    sent: 20,
                    received: 20,
                    min_ms: Some(15.0),
                    median_ms: Some(32.0),
                    p95_ms: Some(50.0),
                    p99_ms: Some(65.0),
                    max_ms: Some(70.0),
                    jitter_ms: Some(8.4),
                },
                upload: ThroughputSummary {
                    total_bytes: 60_000_000,
                    steady_bps: 59_000_000.0,
                    p95_bps: 61_000_000.0,
                    p99_bps: 62_000_000.0,
                    stability_cv_percent: Some(3.1),
                },
                loaded_upload_latency: LatencySummary {
                    sent: 20,
                    received: 20,
                    min_ms: Some(14.0),
                    median_ms: Some(22.0),
                    p95_ms: Some(35.0),
                    p99_ms: Some(40.0),
                    max_ms: Some(45.0),
                    jitter_ms: Some(4.2),
                },
                udp: Some(UdpSummary {
                    sent: 25,
                    received: 25,
                    reachable: true,
                    loss_ratio: Some(0.0),
                    median_ms: Some(11.5),
                    jitter_ms: Some(0.4),
                }),
                duration_secs: 24.3,
            }),
        }
    }

    #[test]
    fn renders_all_expected_prometheus_metrics_and_escapes_labels() {
        let output = render_prometheus(&sample_snapshot());

        for expected in [
            "speedtest_up 1",
            "speedtest_errors_total 1",
            "speedtest_retries_total 2",
            "speedtest_download_bits_per_second 108000000",
            "speedtest_upload_bits_per_second 59000000",
            "speedtest_ping_latency_milliseconds 11",
            "speedtest_jitter_latency_milliseconds 0.9",
            "speedtest_loaded_download_latency_milliseconds 32",
            "speedtest_loaded_upload_latency_milliseconds 22",
            "speedtest_dns_resolution_milliseconds 3.5",
            "speedtest_tls_handshake_milliseconds 11.2",
            "speedtest_udp_reachable 1",
            "speedtest_packet_loss_ratio 0",
            "speedtest_duration_seconds 24.3",
            r#"speedtest_info{colo="GRU",colo_city="Sao Paulo",country="BR",asn="28220",isp="ISP \"Test\"\\Net\n"} 1"#,
        ] {
            assert!(output.contains(expected), "missing line: {expected}");
        }

        // Every sample line must start with `speedtest_`.
        for line in output.lines() {
            if !line.is_empty() && !line.starts_with('#') {
                assert!(
                    line.starts_with("speedtest_"),
                    "unexpected metric prefix: {line}"
                );
            }
        }
    }

    #[test]
    fn unreachable_udp_omits_loss_and_rtt_metrics() {
        let mut snap = sample_snapshot();
        snap.last_result.as_mut().unwrap().udp = Some(UdpSummary::unreachable(10));
        let output = render_prometheus(&snap);

        assert!(output.contains("speedtest_udp_reachable 0"));
        assert!(!output.contains("speedtest_packet_loss_ratio"));
        assert!(!output.contains("speedtest_udp_latency_milliseconds"));
    }
}
