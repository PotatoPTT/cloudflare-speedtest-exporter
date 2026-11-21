use crate::config::Config;
use crate::engine::{self, SpeedtestResult};
use crate::metrics;
use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const DEFAULT_RETRY_BACKOFFS: [Duration; 3] = [
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

/// Abstraction over speedtest execution to allow deterministic testing of retries and singleflight.
pub trait SpeedtestRunner: Send + Sync {
    fn run<'a>(
        &'a self,
        cfg: &'a Config,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<SpeedtestResult>> + Send + 'a>>;
}

/// Production speedtest runner calling the Cloudflare engine.
#[derive(Default)]
pub struct CloudflareRunner;

impl SpeedtestRunner for CloudflareRunner {
    fn run<'a>(
        &'a self,
        cfg: &'a Config,
        cancel: &'a CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<SpeedtestResult>> + Send + 'a>> {
        Box::pin(engine::run_once(cfg, cancel))
    }
}

/// Shared measurement state rendered into Prometheus metrics.
#[derive(Debug, Default)]
pub struct Snapshot {
    pub up: Option<bool>,
    pub errors_total: u64,
    pub retries_total: u64,
    pub last_run_unix: Option<u64>,
    pub last_success_unix: Option<u64>,
    pub last_success_at: Option<Instant>,
    pub last_result: Option<SpeedtestResult>,
}

impl Snapshot {
    /// Returns true if a successful measurement is cached within `cache_duration`.
    pub fn is_cache_fresh(&self, cache_duration: Duration) -> bool {
        if cache_duration.is_zero() || self.up != Some(true) || self.last_result.is_none() {
            return false;
        }
        self.last_success_at
            .is_some_and(|at| at.elapsed() < cache_duration)
    }
}

/// Coordinates speedtest execution across on-scrape caching, singleflight locking,
/// scheduled background runs, and bounded retries.
pub struct TestCoordinator<R: SpeedtestRunner = CloudflareRunner> {
    cfg: Arc<Config>,
    runner: R,
    state: RwLock<Snapshot>,
    flight_lock: Mutex<()>,
    retry_backoffs: [Duration; 3],
}

impl TestCoordinator<CloudflareRunner> {
    /// Creates a coordinator backed by the production Cloudflare engine.
    pub fn new(cfg: Arc<Config>) -> Self {
        Self::with_runner(cfg, CloudflareRunner)
    }
}

impl<R: SpeedtestRunner + 'static> TestCoordinator<R> {
    /// Creates a coordinator with a custom runner implementation.
    pub fn with_runner(cfg: Arc<Config>, runner: R) -> Self {
        Self {
            cfg,
            runner,
            state: RwLock::new(Snapshot::default()),
            flight_lock: Mutex::new(()),
            retry_backoffs: DEFAULT_RETRY_BACKOFFS,
        }
    }

    /// Returns a reference to the active configuration.
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Spawns the periodic background collection loop when `SPEEDTEST_INTERVAL > 0`.
    pub fn spawn_background_loop(self: &Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        let coord = Arc::clone(self);
        tokio::spawn(async move {
            info!(
                interval_secs = coord.cfg.interval.as_secs(),
                "Starting background speedtest scheduler"
            );
            loop {
                coord.run_scheduled_once(&shutdown).await;
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(coord.cfg.interval) => {}
                }
            }
        })
    }

    /// Serves Prometheus metrics according to the active mode (`interval == 0` on-scrape vs
    /// `interval > 0` background cached).
    pub async fn scrape_and_render(&self, shutdown: &CancellationToken) -> String {
        if !self.cfg.interval.is_zero() {
            let guard = self.state.read().await;
            return metrics::render_prometheus(&guard);
        }

        {
            let guard = self.state.read().await;
            if guard.is_cache_fresh(self.cfg.cache_duration) {
                return metrics::render_prometheus(&guard);
            }
        }

        let _flight = self.flight_lock.lock().await;

        {
            let guard = self.state.read().await;
            if guard.is_cache_fresh(self.cfg.cache_duration) {
                return metrics::render_prometheus(&guard);
            }
        }

        self.execute_with_retries(shutdown).await;

        let guard = self.state.read().await;
        metrics::render_prometheus(&guard)
    }

    async fn run_scheduled_once(&self, shutdown: &CancellationToken) {
        let _flight = self.flight_lock.lock().await;
        if shutdown.is_cancelled() {
            return;
        }
        self.execute_with_retries(shutdown).await;
    }

    async fn execute_with_retries(&self, shutdown: &CancellationToken) {
        let run_cancel = shutdown.child_token();
        let max_attempts = self.cfg.retries.saturating_add(1);
        let mut retries_used = 0u64;

        let outcome = tokio::time::timeout(self.cfg.timeout, async {
            let mut last_err: Option<anyhow::Error> = None;

            for attempt in 0..max_attempts {
                if run_cancel.is_cancelled() {
                    break;
                }

                if attempt > 0 {
                    retries_used += 1;
                    let idx = ((attempt - 1) as usize).min(self.retry_backoffs.len() - 1);
                    let delay = self.retry_backoffs[idx];
                    warn!(
                        attempt = attempt + 1,
                        max_attempts,
                        delay_ms = delay.as_millis(),
                        "Retrying Cloudflare speedtest after failure"
                    );
                    tokio::select! {
                        biased;
                        _ = run_cancel.cancelled() => break,
                        _ = tokio::time::sleep(delay) => {}
                    }
                }

                match self.runner.run(&self.cfg, &run_cancel).await {
                    Ok(result) => return Ok(result),
                    Err(err) => {
                        warn!(
                            attempt = attempt + 1,
                            max_attempts, "Speedtest attempt failed: {err:#}"
                        );
                        last_err = Some(err);
                    }
                }
            }

            Err(last_err.unwrap_or_else(|| anyhow::anyhow!("speedtest cancelled")))
        })
        .await;

        let now_unix = unix_timestamp_secs();
        let mut guard = self.state.write().await;
        guard.last_run_unix = Some(now_unix);
        guard.retries_total = guard.retries_total.saturating_add(retries_used);

        match outcome {
            Ok(Ok(result)) => {
                guard.up = Some(true);
                guard.last_success_unix = Some(now_unix);
                guard.last_success_at = Some(Instant::now());
                guard.last_result = Some(result);
            }
            Ok(Err(err)) => {
                error!("Speedtest failed after {max_attempts} attempt(s): {err:#}");
                guard.up = Some(false);
                guard.last_result = None;
                guard.errors_total = guard.errors_total.saturating_add(1);
            }
            Err(_) => {
                run_cancel.cancel();
                error!(
                    timeout_secs = self.cfg.timeout.as_secs(),
                    "Speedtest timed out across attempt(s)"
                );
                guard.up = Some(false);
                guard.last_result = None;
                guard.errors_total = guard.errors_total.saturating_add(1);
            }
        }
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::engine::cloudflare::CloudflareMeta;
    use crate::engine::latency::LatencySummary;
    use crate::engine::throughput::ThroughputSummary;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn dummy_result() -> SpeedtestResult {
        SpeedtestResult {
            meta: CloudflareMeta {
                colo: Some("GRU".to_string()),
                colo_city: Some("Sao Paulo".to_string()),
                country: Some("BR".to_string()),
                asn: Some("28220".to_string()),
                isp: Some("Example ISP".to_string()),
            },
            dns_ms: 4.2,
            tls_ms: Some(12.5),
            idle_latency: LatencySummary {
                sent: 5,
                received: 5,
                min_ms: Some(10.0),
                median_ms: Some(11.0),
                p95_ms: Some(12.0),
                p99_ms: Some(12.5),
                max_ms: Some(13.0),
                jitter_ms: Some(0.8),
            },
            download: ThroughputSummary {
                total_bytes: 100_000_000,
                steady_bps: 100_000_000.0,
                p95_bps: 105_000_000.0,
                p99_bps: 108_000_000.0,
                stability_cv_percent: Some(3.2),
            },
            loaded_download_latency: LatencySummary::default(),
            upload: ThroughputSummary {
                total_bytes: 50_000_000,
                steady_bps: 50_000_000.0,
                p95_bps: 54_000_000.0,
                p99_bps: 56_000_000.0,
                stability_cv_percent: Some(4.1),
            },
            loaded_upload_latency: LatencySummary::default(),
            udp: None,
            duration_secs: 24.0,
        }
    }

    fn test_cfg(overrides: &[(&str, &str)]) -> Arc<Config> {
        let map: HashMap<&str, &str> = overrides.iter().copied().collect();
        Arc::new(Config::from_lookup(|k| map.get(k).map(|v| (*v).to_string())).unwrap())
    }

    struct ScriptedRunner {
        calls: AtomicUsize,
        should_fail: fn(usize) -> bool,
        delay: Duration,
    }

    impl SpeedtestRunner for ScriptedRunner {
        fn run<'a>(
            &'a self,
            _cfg: &'a Config,
            _cancel: &'a CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<SpeedtestResult>> + Send + 'a>> {
            Box::pin(async move {
                let idx = self.calls.fetch_add(1, Ordering::SeqCst);
                if !self.delay.is_zero() {
                    tokio::time::sleep(self.delay).await;
                }
                if (self.should_fail)(idx) {
                    anyhow::bail!("simulated transient failure #{idx}");
                }
                Ok(dummy_result())
            })
        }
    }

    fn fast_coord(cfg: Arc<Config>, runner: ScriptedRunner) -> TestCoordinator<ScriptedRunner> {
        let mut coord = TestCoordinator::with_runner(cfg, runner);
        coord.retry_backoffs = [
            Duration::from_millis(5),
            Duration::from_millis(5),
            Duration::from_millis(5),
        ];
        coord
    }

    #[tokio::test]
    async fn singleflight_shares_one_execution_and_caches_followups() {
        let cfg = test_cfg(&[
            ("SPEEDTEST_CACHE_DURATION", "60"),
            ("SPEEDTEST_RETRIES", "0"),
        ]);
        let coord = Arc::new(fast_coord(
            cfg,
            ScriptedRunner {
                calls: AtomicUsize::new(0),
                should_fail: |_| false,
                delay: Duration::from_millis(50),
            },
        ));
        let shutdown = CancellationToken::new();

        let (m1, m2) = tokio::join!(
            coord.scrape_and_render(&shutdown),
            coord.scrape_and_render(&shutdown)
        );
        assert_eq!(coord.runner.calls.load(Ordering::SeqCst), 1);
        assert!(m1.contains("speedtest_up 1"));
        assert!(m2.contains("speedtest_up 1"));

        let m3 = coord.scrape_and_render(&shutdown).await;
        assert_eq!(coord.runner.calls.load(Ordering::SeqCst), 1);
        assert!(m3.contains("speedtest_download_bits_per_second 100000000"));
    }

    #[tokio::test]
    async fn retries_transient_errors_and_tracks_retry_counter() {
        let cfg = test_cfg(&[("SPEEDTEST_RETRIES", "2")]);
        let coord = fast_coord(
            cfg,
            ScriptedRunner {
                calls: AtomicUsize::new(0),
                should_fail: |i| i == 0,
                delay: Duration::ZERO,
            },
        );
        let shutdown = CancellationToken::new();

        let body = coord.scrape_and_render(&shutdown).await;
        assert_eq!(coord.runner.calls.load(Ordering::SeqCst), 2);
        assert!(body.contains("speedtest_up 1"));
        assert!(body.contains("speedtest_retries_total 1"));
        assert!(body.contains("speedtest_errors_total 0"));
    }

    #[tokio::test]
    async fn failure_clears_measurement_metrics_and_sets_up_zero() {
        let cfg = test_cfg(&[
            ("SPEEDTEST_CACHE_DURATION", "0"),
            ("SPEEDTEST_RETRIES", "1"),
        ]);
        let coord = fast_coord(
            cfg,
            ScriptedRunner {
                calls: AtomicUsize::new(0),
                should_fail: |i| i >= 1,
                delay: Duration::ZERO,
            },
        );
        let shutdown = CancellationToken::new();

        // First run succeeds; second run fails after retries and must clear stale measurements.
        let ok_body = coord.scrape_and_render(&shutdown).await;
        assert!(ok_body.contains("speedtest_download_bits_per_second"));

        let fail_body = coord.scrape_and_render(&shutdown).await;
        assert!(fail_body.contains("speedtest_up 0"));
        assert!(fail_body.contains("speedtest_errors_total 1"));
        assert!(fail_body.contains("speedtest_retries_total 1"));
        assert!(!fail_body.contains("speedtest_download_bits_per_second"));
        assert!(!fail_body.contains("speedtest_upload_bits_per_second"));
        assert!(!fail_body.contains("speedtest_ping_latency_milliseconds"));
    }
}
