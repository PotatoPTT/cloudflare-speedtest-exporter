use std::time::Duration;

/// Sampling interval used for throughput rate ticks.
pub const THROUGHPUT_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

/// Initial fraction of a throughput phase treated as TCP slow-start ramp-up.
const STEADY_STATE_RAMP_FRACTION: f64 = 0.20;

/// Minimum initial ramp-up exclusion window.
const STEADY_STATE_MIN_RAMP: Duration = Duration::from_secs(1);

/// Summary percentiles computed from a slice of finite numeric samples.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SampleStats {
    pub mean: f64,
    pub median: f64,
    pub min: f64,
    pub max: f64,
    pub p95: f64,
    pub p99: f64,
}

/// Computes linear-interpolated summary statistics from finite samples.
pub fn compute_sample_stats(samples: &[f64]) -> Option<SampleStats> {
    let mut sorted: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if sorted.is_empty() {
        return None;
    }

    sorted.sort_by(f64::total_cmp);
    let mean = sorted.iter().sum::<f64>() / (sorted.len() as f64);

    Some(SampleStats {
        mean,
        median: percentile_sorted(&sorted, 0.50),
        min: sorted[0],
        max: sorted[sorted.len() - 1],
        p95: percentile_sorted(&sorted, 0.95),
        p99: percentile_sorted(&sorted, 0.99),
    })
}

/// Computes sample standard deviation (jitter) when at least two finite samples exist.
pub fn compute_jitter(samples: &[f64]) -> Option<f64> {
    let finite: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if finite.len() < 2 {
        return None;
    }

    let n = finite.len() as f64;
    let mean = finite.iter().sum::<f64>() / n;
    let variance = finite.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    Some(variance.sqrt())
}

/// Computes coefficient of variation (`stddev / mean * 100`) when at least 3 finite
/// samples exist and `mean > 0`.
pub fn compute_cv_percent(samples: &[f64]) -> Option<f64> {
    let finite: Vec<f64> = samples.iter().copied().filter(|x| x.is_finite()).collect();
    if finite.len() < 3 {
        return None;
    }

    let n = finite.len() as f64;
    let mean = finite.iter().sum::<f64>() / n;
    if mean <= 0.0 {
        return None;
    }

    let variance = finite.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    Some((variance.sqrt() / mean) * 100.0)
}

/// Excludes initial TCP slow-start ramp-up (`max(20% of duration, 1s)`) from throughput
/// samples and cumulative byte counters, returning `(bytes, elapsed_window, steady_rates)`.
pub fn steady_state_window<'a>(
    samples: &[(Duration, u64)],
    rates_bps: &'a [f64],
    total_duration: Duration,
) -> Option<(u64, Duration, &'a [f64])> {
    if samples.len() < 2 {
        return None;
    }

    let ramp = total_duration
        .mul_f64(STEADY_STATE_RAMP_FRACTION)
        .max(STEADY_STATE_MIN_RAMP);
    let start_idx = samples.iter().position(|(t, _)| *t >= ramp)?;
    let (t_start, b_start) = samples[start_idx];
    let (t_end, b_end) = *samples.last()?;

    let window = t_end.saturating_sub(t_start);
    if window < THROUGHPUT_SAMPLE_INTERVAL {
        return None;
    }

    let steady_start = (start_idx + 1).min(rates_bps.len());
    let steady_rates = rates_bps.get(steady_start..)?;
    Some((b_end.saturating_sub(b_start), window, steady_rates))
}

fn percentile_sorted(sorted: &[f64], p: f64) -> f64 {
    if sorted.len() == 1 {
        return sorted[0];
    }

    let p = p.clamp(0.0, 1.0);
    let pos = p * ((sorted.len() - 1) as f64);
    let lower = pos.floor() as usize;
    let upper = pos.ceil() as usize;

    if lower == upper {
        sorted[lower]
    } else {
        let frac = pos - (lower as f64);
        sorted[lower] + frac * (sorted[upper] - sorted[lower])
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn sample_stats_handles_percentiles_and_non_finite() {
        let stats = compute_sample_stats(&[10.0, f64::NAN, 20.0, 30.0, 40.0, 50.0]).unwrap();
        assert!((stats.min - 10.0).abs() < 1e-9);
        assert!((stats.max - 50.0).abs() < 1e-9);
        assert!((stats.mean - 30.0).abs() < 1e-9);
        assert!((stats.median - 30.0).abs() < 1e-9);
        assert!((stats.p95 - 48.0).abs() < 1e-9);
        assert!((stats.p99 - 49.6).abs() < 1e-9);
    }

    #[test]
    fn jitter_and_cv_require_minimum_samples() {
        assert!(compute_jitter(&[10.0]).is_none());
        let jitter = compute_jitter(&[10.0, 12.0, 14.0]).unwrap();
        assert!((jitter - 2.0).abs() < 1e-9);

        assert!(compute_cv_percent(&[100.0, 100.0]).is_none());
        assert!(compute_cv_percent(&[0.0, 0.0, 0.0]).is_none());
        let cv = compute_cv_percent(&[90.0, 100.0, 110.0]).unwrap();
        assert!((cv - 10.0).abs() < 1e-9);
    }

    #[test]
    fn steady_state_window_excludes_ramp_up() {
        let samples: Vec<(Duration, u64)> = [5u64, 30, 130, 230, 330, 430, 530, 630, 730, 830]
            .iter()
            .enumerate()
            .map(|(i, b)| (Duration::from_secs(i as u64 + 1), *b))
            .collect();
        let rates = vec![40.0, 200.0, 800.0, 800.0, 800.0, 800.0, 800.0, 800.0, 800.0, 800.0];

        let (bytes, window, steady) =
            steady_state_window(&samples, &rates, Duration::from_secs(10)).unwrap();
        assert_eq!(bytes, 800);
        assert_eq!(window, Duration::from_secs(8));
        assert_eq!(steady, &rates[2..]);
    }

    #[test]
    fn steady_state_window_returns_none_when_too_short() {
        let short = vec![
            (Duration::from_millis(200), 10),
            (Duration::from_millis(400), 20),
        ];
        assert!(steady_state_window(&short, &[400.0, 400.0], Duration::from_secs(10)).is_none());
    }
}
