use crate::config::IpVersion;
use anyhow::{Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, REFERER};
use reqwest::{Response, Url};
use serde::Serialize;
use serde_json::Value;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, warn};

const META_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const TCP_KEEPALIVE: Duration = Duration::from_secs(15);

/// Cloudflare edge and network metadata collected during a test run.
///
/// Client IP addresses are intentionally omitted so high-cardinality or sensitive
/// values are never emitted as Prometheus labels.
#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct CloudflareMeta {
    pub colo: Option<String>,
    pub colo_city: Option<String>,
    pub country: Option<String>,
    pub asn: Option<String>,
    pub isp: Option<String>,
}

impl CloudflareMeta {
    fn merge_missing_from(&mut self, other: CloudflareMeta) {
        if self.colo.is_none() {
            self.colo = other.colo;
        }
        if self.colo_city.is_none() {
            self.colo_city = other.colo_city;
        }
        if self.country.is_none() {
            self.country = other.country;
        }
        if self.asn.is_none() {
            self.asn = other.asn;
        }
        if self.isp.is_none() {
            self.isp = other.isp;
        }
    }
}

/// Shared HTTP client configured for Cloudflare Speedtest endpoints.
pub struct CloudflareClient {
    pub base_url: Url,
    pub down_url: Url,
    pub up_url: Url,
    pub meas_id: String,
    pub http: reqwest::Client,
}

impl CloudflareClient {
    /// Builds a new HTTP client with Cloudflare's required `Referer` header and
    /// optional address-family resolution pinning.
    pub fn new(
        base_url: &Url,
        meas_id: String,
        resolved_addrs: &[SocketAddr],
        ip_version: IpVersion,
    ) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            REFERER,
            HeaderValue::from_static("https://speed.cloudflare.com/"),
        );

        let user_agent = concat!("cloudflare-speedtest-exporter/", env!("CARGO_PKG_VERSION"));
        let mut builder = reqwest::Client::builder()
            .user_agent(user_agent)
            .default_headers(headers)
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .tcp_keepalive(TCP_KEEPALIVE);

        if ip_version != IpVersion::Auto && !resolved_addrs.is_empty() {
            if let Some(host) = base_url.host_str() {
                builder = builder.resolve_to_addrs(host, resolved_addrs);
            }
        }

        let http = builder.build().context("failed to build HTTP client")?;
        let down_url = base_url.join("/__down").context("failed to join /__down")?;
        let up_url = base_url.join("/__up").context("failed to join /__up")?;

        Ok(Self {
            base_url: base_url.clone(),
            down_url,
            up_url,
            meas_id,
            http,
        })
    }

    /// Builds a `/__down` URL with `bytes` and optional `during` or `measId`.
    pub fn build_down_url(&self, bytes: u64, during: Option<&str>) -> Url {
        let mut url = self.down_url.clone();
        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("bytes", &bytes.to_string());
            if let Some(phase) = during {
                qp.append_pair("during", phase);
            } else {
                qp.append_pair("measId", &self.meas_id);
            }
        }
        url
    }

    /// Builds an `/__up` URL tagged with `measId`.
    pub fn build_up_url(&self) -> Url {
        let mut url = self.up_url.clone();
        url.query_pairs_mut().append_pair("measId", &self.meas_id);
        url
    }

    /// Fetches Cloudflare PoP/ISP metadata using a best-effort fallback chain:
    /// `/meta` -> `/cdn-cgi/trace` -> `/__down?bytes=0` response headers.
    pub async fn fetch_metadata(&self) -> CloudflareMeta {
        let mut meta = match timeout(META_TIMEOUT, self.fetch_meta_endpoint()).await {
            Ok(Ok(parsed)) => parsed,
            Ok(Err(err)) => {
                debug!("Cloudflare /meta request failed: {err:#}");
                CloudflareMeta::default()
            }
            Err(_) => {
                debug!("Cloudflare /meta request timed out");
                CloudflareMeta::default()
            }
        };

        if meta.colo.is_none() || meta.country.is_none() {
            match timeout(META_TIMEOUT, self.fetch_trace_endpoint()).await {
                Ok(Ok(trace)) => meta.merge_missing_from(trace),
                Ok(Err(err)) => debug!("Cloudflare /cdn-cgi/trace request failed: {err:#}"),
                Err(_) => debug!("Cloudflare /cdn-cgi/trace request timed out"),
            }
        }

        if meta.colo.is_none() {
            match timeout(META_TIMEOUT, self.fetch_header_metadata()).await {
                Ok(Ok(hdr_meta)) => meta.merge_missing_from(hdr_meta),
                Ok(Err(err)) => warn!("Cloudflare header metadata fallback failed: {err:#}"),
                Err(_) => warn!("Cloudflare header metadata fallback timed out"),
            }
        }

        meta
    }

    async fn fetch_meta_endpoint(&self) -> Result<CloudflareMeta> {
        let mut url = self
            .base_url
            .join("/meta")
            .context("failed to join /meta")?;
        url.query_pairs_mut().append_pair("measId", &self.meas_id);
        let value: Value = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(parse_meta_json(&value))
    }

    async fn fetch_trace_endpoint(&self) -> Result<CloudflareMeta> {
        let url = self
            .base_url
            .join("/cdn-cgi/trace")
            .context("failed to join /cdn-cgi/trace")?;
        let text = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        Ok(parse_trace_text(&text))
    }

    async fn fetch_header_metadata(&self) -> Result<CloudflareMeta> {
        let url = self.build_down_url(0, None);
        let resp = self.http.get(url).send().await?;
        let parsed = parse_meta_headers(&resp);
        let _ = resp.bytes().await;
        Ok(parsed)
    }
}

/// Parses JSON from `/meta`, handling both object and string forms of `colo`.
pub fn parse_meta_json(value: &Value) -> CloudflareMeta {
    let Some(obj) = value.as_object() else {
        return CloudflareMeta::default();
    };

    let (colo, colo_city, colo_country) = match obj.get("colo") {
        Some(Value::String(code)) => (non_empty_str(code), None, None),
        Some(Value::Object(colo_obj)) => {
            let code = colo_obj
                .get("iata")
                .or_else(|| colo_obj.get("code"))
                .and_then(Value::as_str)
                .and_then(non_empty_str);
            let city = colo_obj
                .get("city")
                .and_then(Value::as_str)
                .and_then(non_empty_str);
            let cca2 = colo_obj
                .get("cca2")
                .and_then(Value::as_str)
                .and_then(non_empty_str);
            (code, city, cca2)
        }
        _ => (None, None, None),
    };

    let country = obj
        .get("country")
        .and_then(Value::as_str)
        .and_then(non_empty_str)
        .or(colo_country);

    let asn = obj.get("asn").and_then(|v| match v {
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => non_empty_str(s),
        _ => None,
    });

    let isp = obj
        .get("asOrganization")
        .or_else(|| obj.get("isp"))
        .or_else(|| obj.get("org"))
        .and_then(Value::as_str)
        .and_then(non_empty_str);

    CloudflareMeta {
        colo,
        colo_city,
        country,
        asn,
        isp,
    }
}

/// Parses `key=value` lines from `/cdn-cgi/trace`.
pub fn parse_trace_text(text: &str) -> CloudflareMeta {
    let mut meta = CloudflareMeta::default();
    for line in text.lines() {
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let val = val.trim();
        match key.trim() {
            "colo" => meta.colo = non_empty_str(val),
            "loc" => meta.country = non_empty_str(val),
            _ => {}
        }
    }
    meta
}

/// Extracts metadata from Cloudflare response headers (`cf-meta-*` and `cf-ray`).
pub fn parse_meta_headers(resp: &Response) -> CloudflareMeta {
    let headers = resp.headers();

    let colo = header_str(headers, "cf-meta-colo").or_else(|| {
        header_str(headers, "cf-ray").and_then(|ray| ray.split('-').nth(1).and_then(non_empty_str))
    });

    CloudflareMeta {
        colo,
        colo_city: header_str(headers, "cf-meta-city"),
        country: header_str(headers, "cf-meta-country"),
        asn: header_str(headers, "cf-meta-asn"),
        isp: None,
    }
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(non_empty_str)
}

fn non_empty_str(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_real_meta_json_with_colo_object() {
        let payload = json!({
            "hostname": "speed.cloudflare.com",
            "clientIp": "203.0.113.10",
            "asn": 28220,
            "asOrganization": "Alares Cabo Servicos de Telecomunicacoes S.A.",
            "country": "BR",
            "city": "Limeira",
            "colo": {
                "iata": "GRU",
                "cca2": "BR",
                "city": "Sao Paulo"
            }
        });
        let meta = parse_meta_json(&payload);
        assert_eq!(meta.colo.as_deref(), Some("GRU"));
        assert_eq!(meta.colo_city.as_deref(), Some("Sao Paulo"));
        assert_eq!(meta.country.as_deref(), Some("BR"));
        assert_eq!(meta.asn.as_deref(), Some("28220"));
        assert_eq!(
            meta.isp.as_deref(),
            Some("Alares Cabo Servicos de Telecomunicacoes S.A.")
        );
    }

    #[test]
    fn parses_meta_json_with_string_colo_and_asn() {
        let payload = json!({
            "colo": "EZE",
            "country": "AR",
            "asn": "12345",
            "isp": "Example ISP"
        });
        let meta = parse_meta_json(&payload);
        assert_eq!(meta.colo.as_deref(), Some("EZE"));
        assert_eq!(meta.colo_city, None);
        assert_eq!(meta.country.as_deref(), Some("AR"));
        assert_eq!(meta.asn.as_deref(), Some("12345"));
        assert_eq!(meta.isp.as_deref(), Some("Example ISP"));
    }

    #[test]
    fn parses_trace_output() {
        let trace =
            "fl=123f45\nh=speed.cloudflare.com\nip=192.0.2.1\ncolos=ignored\ncolo=GRU\nloc=BR\n";
        let meta = parse_trace_text(trace);
        assert_eq!(meta.colo.as_deref(), Some("GRU"));
        assert_eq!(meta.country.as_deref(), Some("BR"));
    }
}
