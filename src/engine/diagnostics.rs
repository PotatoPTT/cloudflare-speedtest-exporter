use crate::config::IpVersion;
use anyhow::{anyhow, bail, Context, Result};
use rustls::pki_types::ServerName;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{lookup_host, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Measures DNS resolution latency for `host:port` and returns addresses matching `ip_version`.
pub async fn measure_dns(
    host: &str,
    port: u16,
    ip_version: IpVersion,
) -> Result<(f64, Vec<SocketAddr>)> {
    let start = Instant::now();
    let resolved: Vec<SocketAddr> = lookup_host((host, port))
        .await
        .with_context(|| format!("DNS lookup failed for {host}"))?
        .filter(|addr| ip_version.matches(addr.ip()))
        .collect();
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    if resolved.is_empty() {
        bail!("DNS returned no matching addresses for {host}");
    }

    Ok((elapsed_ms, resolved))
}

/// Measures TLS handshake latency (excluding TCP connection time) against the first
/// reachable address in `addrs`.
pub async fn measure_tls(host: &str, addrs: &[SocketAddr]) -> Result<f64> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));

    let tcp_stream = connect_first_reachable(host, addrs).await?;
    let server_name: ServerName<'static> = host
        .to_owned()
        .try_into()
        .map_err(|_| anyhow!("invalid TLS server name '{host}'"))?;

    let start = Instant::now();
    let _tls_stream = timeout(HANDSHAKE_TIMEOUT, connector.connect(server_name, tcp_stream))
        .await
        .with_context(|| format!("TLS handshake timed out after {HANDSHAKE_TIMEOUT:?}"))?
        .with_context(|| format!("TLS handshake failed with {host}"))?;

    Ok(start.elapsed().as_secs_f64() * 1000.0)
}

async fn connect_first_reachable(host: &str, addrs: &[SocketAddr]) -> Result<TcpStream> {
    let mut last_err: Option<anyhow::Error> = None;

    for &addr in addrs {
        match timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(err)) => {
                last_err = Some(anyhow!(err).context(format!("TCP connect to {addr} failed")));
            }
            Err(_) => {
                last_err = Some(anyhow!(
                    "TCP connect to {addr} timed out after {CONNECT_TIMEOUT:?}"
                ));
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("no reachable TCP addresses for {host}")))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn measure_dns_resolves_localhost() {
        let (ms, addrs) = measure_dns("127.0.0.1", 443, IpVersion::Ipv4)
            .await
            .unwrap();
        assert!(ms >= 0.0);
        assert_eq!(addrs.len(), 1);
        assert!(addrs[0].is_ipv4());
    }
}
