use crate::coordinator::{CloudflareRunner, TestCoordinator};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::Router;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct AppState {
    coordinator: Arc<TestCoordinator<CloudflareRunner>>,
    shutdown: CancellationToken,
}

/// Builds the Axum HTTP router exposing `/`, `/health`, and `/metrics`.
pub fn build_router(
    coordinator: Arc<TestCoordinator<CloudflareRunner>>,
    shutdown: CancellationToken,
) -> Router {
    let state = Arc::new(AppState {
        coordinator,
        shutdown,
    });

    Router::new()
        .route("/", get(index_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

async fn index_handler(State(state): State<Arc<AppState>>) -> Html<String> {
    let cfg = state.coordinator.config();
    let mode = if cfg.interval.is_zero() {
        format!("on-scrape (cache TTL: {}s)", cfg.cache_duration.as_secs())
    } else {
        format!("background scheduler (every {}s)", cfg.interval.as_secs())
    };
    let version = env!("CARGO_PKG_VERSION");

    Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Cloudflare Speedtest Exporter</title>
</head>
<body>
  <h1>Cloudflare Speedtest Exporter v{version}</h1>
  <p>Prometheus exporter powered by Cloudflare Speedtest endpoints in Rust.</p>
  <p>Mode: <code>{mode}</code></p>
  <ul>
    <li><a href="/metrics">Metrics (/metrics)</a></li>
    <li><a href="/health">Health Check (/health)</a></li>
  </ul>
</body>
</html>
"#
    ))
}

async fn health_handler() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK\n")
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let body = state.coordinator.scrape_and_render(&state.shutdown).await;
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}
