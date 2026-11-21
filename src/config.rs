use anyhow::{bail, Context, Result};
use reqwest::Url;
use std::env;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

/// IP address family restriction applied to DNS, TLS, HTTP, and UDP probes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpVersion {
    Auto,
    Ipv4,
    Ipv6,
}

impl IpVersion {
    /// Parses an IP version preference string (`auto`, `ipv4`, `ipv6`).
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" | "" => Ok(Self::Auto),
            "ipv4" | "v4" | "4" => Ok(Self::Ipv4),
            "ipv6" | "v6" | "6" => Ok(Self::Ipv6),
            other => bail!("invalid SPEEDTEST_IP_VERSION '{other}' (expected auto, ipv4, or ipv6)"),
        }
    }

    /// Returns true if `ip` belongs to the allowed address family.
    pub fn matches(self, ip: IpAddr) -> bool {
        match self {
            Self::Auto => true,
            Self::Ipv4 => ip.is_ipv4(),
            Self::Ipv6 => ip.is_ipv6(),
        }
    }
}

/// Exporter runtime and measurement configuration loaded from environment variables.
#[derive(Debug)]
pub struct Config {
    pub bind_addr: IpAddr,
    pub port: u16,
    pub cache_duration: Duration,
    pub interval: Duration,
    pub timeout: Duration,
    pub retries: u32,
    pub concurrency: usize,
    pub idle_latency_duration: Duration,
    pub download_duration: Duration,
    pub upload_duration: Duration,
    pub download_bytes_per_req: u64,
    pub upload_bytes_per_req: u64,
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub udp_enabled: bool,
    pub udp_probes: u64,
    pub stun_host: String,
    pub stun_port: u16,
    pub ip_version: IpVersion,
    pub base_url: Url,
}

impl Config {
    /// Loads configuration from process environment variables.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    /// Loads configuration using an arbitrary key lookup function.
    pub fn from_lookup<F>(lookup: F) -> Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let bind_addr: IpAddr = parse_or_default(
            &lookup,
            "SPEEDTEST_BIND_ADDR",
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        )?;
        let port: u16 = parse_or_default(&lookup, "SPEEDTEST_PORT", 9798)?;

        let cache_secs: u64 = parse_or_default(&lookup, "SPEEDTEST_CACHE_DURATION", 300)?;
        let interval_secs: u64 = parse_or_default(&lookup, "SPEEDTEST_INTERVAL", 0)?;
        let timeout_secs: u64 = parse_or_default(&lookup, "SPEEDTEST_TIMEOUT", 90)?;
        let retries: u32 = parse_or_default(&lookup, "SPEEDTEST_RETRIES", 2)?;

        let concurrency: usize = parse_or_default(&lookup, "SPEEDTEST_CONCURRENCY", 6)?;
        let idle_secs: u64 = parse_or_default(&lookup, "SPEEDTEST_IDLE_LATENCY_SECONDS", 2)?;
        let download_secs: u64 = parse_or_default(&lookup, "SPEEDTEST_DOWNLOAD_SECONDS", 10)?;
        let upload_secs: u64 = parse_or_default(&lookup, "SPEEDTEST_UPLOAD_SECONDS", 10)?;

        let download_bytes_per_req: u64 =
            parse_or_default(&lookup, "SPEEDTEST_DOWNLOAD_BYTES_PER_REQ", 10_000_000)?;
        let upload_bytes_per_req: u64 =
            parse_or_default(&lookup, "SPEEDTEST_UPLOAD_BYTES_PER_REQ", 5_000_000)?;

        let probe_interval_ms: u64 = parse_or_default(&lookup, "SPEEDTEST_PROBE_INTERVAL_MS", 250)?;
        let probe_timeout_ms: u64 = parse_or_default(&lookup, "SPEEDTEST_PROBE_TIMEOUT_MS", 2000)?;

        let udp_enabled = parse_bool(&lookup, "SPEEDTEST_UDP_ENABLED", true)?;
        let udp_probes: u64 = parse_or_default(&lookup, "SPEEDTEST_UDP_PROBES", 25)?;
        let stun_host = lookup("SPEEDTEST_STUN_HOST")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "stun.cloudflare.com".to_string());
        let stun_port: u16 = parse_or_default(&lookup, "SPEEDTEST_STUN_PORT", 3478)?;

        let ip_version = match lookup("SPEEDTEST_IP_VERSION") {
            Some(raw) => IpVersion::parse(&raw)?,
            None => IpVersion::Auto,
        };

        let raw_base_url = lookup("SPEEDTEST_BASE_URL")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "https://speed.cloudflare.com".to_string());
        let base_url = Url::parse(&raw_base_url)
            .with_context(|| format!("invalid SPEEDTEST_BASE_URL '{raw_base_url}'"))?;
        if !matches!(base_url.scheme(), "http" | "https") || base_url.host_str().is_none() {
            bail!("SPEEDTEST_BASE_URL must be an HTTP or HTTPS URL with a host");
        }

        if timeout_secs == 0 {
            bail!("SPEEDTEST_TIMEOUT must be greater than 0");
        }
        if concurrency == 0 {
            bail!("SPEEDTEST_CONCURRENCY must be at least 1");
        }
        if idle_secs == 0 || download_secs == 0 || upload_secs == 0 {
            bail!("test phase durations must be greater than 0 seconds");
        }
        if download_bytes_per_req == 0 || upload_bytes_per_req == 0 {
            bail!("bytes per request must be greater than 0");
        }
        if probe_interval_ms == 0 || probe_timeout_ms == 0 {
            bail!("probe interval and timeout must be greater than 0 ms");
        }
        if udp_enabled && udp_probes == 0 {
            bail!("SPEEDTEST_UDP_PROBES must be at least 1 when UDP is enabled");
        }
        if stun_port == 0 {
            bail!("SPEEDTEST_STUN_PORT must be greater than 0");
        }

        Ok(Self {
            bind_addr,
            port,
            cache_duration: Duration::from_secs(cache_secs),
            interval: Duration::from_secs(interval_secs),
            timeout: Duration::from_secs(timeout_secs),
            retries,
            concurrency,
            idle_latency_duration: Duration::from_secs(idle_secs),
            download_duration: Duration::from_secs(download_secs),
            upload_duration: Duration::from_secs(upload_secs),
            download_bytes_per_req,
            upload_bytes_per_req,
            probe_interval: Duration::from_millis(probe_interval_ms),
            probe_timeout: Duration::from_millis(probe_timeout_ms),
            udp_enabled,
            udp_probes,
            stun_host,
            stun_port,
            ip_version,
            base_url,
        })
    }
}

fn parse_or_default<T, F>(lookup: &F, key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
    F: Fn(&str) -> Option<String>,
{
    match lookup(key) {
        Some(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("invalid {key}='{raw}': {e}")),
        _ => Ok(default),
    }
}

fn parse_bool<F>(lookup: &F, key: &str, default: bool) -> Result<bool>
where
    F: Fn(&str) -> Option<String>,
{
    match lookup(key) {
        Some(raw) if !raw.trim().is_empty() => match raw.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            other => bail!("invalid {key}='{other}' (expected boolean)"),
        },
        _ => Ok(default),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::net::{Ipv4Addr, Ipv6Addr};

    fn lookup_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn defaults_are_valid() {
        let cfg = Config::from_lookup(lookup_from(&[])).unwrap();
        assert_eq!(cfg.port, 9798);
        assert_eq!(cfg.cache_duration, Duration::from_secs(300));
        assert_eq!(cfg.interval, Duration::ZERO);
        assert_eq!(cfg.timeout, Duration::from_secs(90));
        assert_eq!(cfg.retries, 2);
        assert_eq!(cfg.concurrency, 6);
        assert!(cfg.udp_enabled);
        assert_eq!(cfg.udp_probes, 25);
        assert_eq!(cfg.ip_version, IpVersion::Auto);
        assert_eq!(cfg.base_url.as_str(), "https://speed.cloudflare.com/");
    }

    #[test]
    fn custom_overrides_parse_correctly() {
        let cfg = Config::from_lookup(lookup_from(&[
            ("SPEEDTEST_PORT", "19798"),
            ("SPEEDTEST_INTERVAL", "1800"),
            ("SPEEDTEST_RETRIES", "0"),
            ("SPEEDTEST_UDP_ENABLED", "false"),
            ("SPEEDTEST_IP_VERSION", "ipv4"),
            ("SPEEDTEST_BASE_URL", "http://127.0.0.1:8080"),
        ]))
        .unwrap();
        assert_eq!(cfg.port, 19798);
        assert_eq!(cfg.interval, Duration::from_secs(1800));
        assert_eq!(cfg.retries, 0);
        assert!(!cfg.udp_enabled);
        assert_eq!(cfg.ip_version, IpVersion::Ipv4);
        assert_eq!(cfg.base_url.port(), Some(8080));
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(Config::from_lookup(lookup_from(&[("SPEEDTEST_CONCURRENCY", "0")])).is_err());
        assert!(Config::from_lookup(lookup_from(&[("SPEEDTEST_TIMEOUT", "0")])).is_err());
        assert!(Config::from_lookup(lookup_from(&[("SPEEDTEST_IP_VERSION", "ipx")])).is_err());
        assert!(Config::from_lookup(lookup_from(&[("SPEEDTEST_BASE_URL", "not a url")])).is_err());
    }

    #[test]
    fn ip_version_matches_expected_families() {
        let v4 = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);
        assert!(IpVersion::Auto.matches(v4) && IpVersion::Auto.matches(v6));
        assert!(IpVersion::Ipv4.matches(v4) && !IpVersion::Ipv4.matches(v6));
        assert!(!IpVersion::Ipv6.matches(v4) && IpVersion::Ipv6.matches(v6));
    }
}
