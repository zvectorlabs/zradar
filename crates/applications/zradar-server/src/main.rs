//! zradar standalone OSS binary.
//!
//! Loads `config.toml`, builds a [`ConfigAuthenticator`] from `[[api_keys]]`,
//! and delegates all server startup to [`ZradarRuntimeBuilder`].
//!
//! This binary contains no platform-specific code. External platform wrapper binaries
//! follow the same pattern using their own auth implementations.

use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use zradar_auth_config::ConfigAuthenticator;
use zradar_models::Config;
use zradar_runtime::{RuntimeAuth, ZradarRuntimeBuilder, api_key_authorizers_from_config};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,zradar=debug")),
        )
        .init();

    // `zradar migrate` — run pending DB migrations and exit.
    //
    // Reads DATABASE_URL from the environment, applies all pending migrations,
    // and exits 0. Intended for Kubernetes Jobs, init containers, or any
    // deployment that needs a migrate-then-serve pattern rather than
    // auto-migrate on startup. The full server is NOT started.
    if std::env::args().nth(1).as_deref() == Some("migrate") {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|_| anyhow::anyhow!("DATABASE_URL must be set for the migrate subcommand"))?;
        zradar_runtime::migrate(&database_url).await?;
        println!("zradar migrations applied");
        return Ok(());
    }

    // `zradar healthcheck` — probe this instance's liveness endpoint and exit
    // 0 if it answers 2xx, non-zero otherwise. Lets container health checks
    // run without an HTTP client in the image. Reads the same config as the
    // server to find the admin port.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        let config = Config::load()?;
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], config.effective_admin_port()));
        zradar_runtime::health_probe(addr, "/health/live", std::time::Duration::from_secs(3))
            .await?;
        return Ok(());
    }

    info!("Starting zradar server...");

    let config = Config::load()?;
    info!("Configuration loaded");

    let api_keys = &config.api_keys;

    let otlp_auth = if config.auth.otlp_require_api_key {
        info!(
            "Config-based authenticator initialized ({} API keys)",
            api_keys.len()
        );
        Some(Arc::new(ConfigAuthenticator::from_config(api_keys))
            as Arc<dyn zradar_traits::Authenticator>)
    } else {
        info!("OTLP open ingest: no API key required");
        None
    };

    let (query, admin) =
        api_key_authorizers_from_config(api_keys, config.auth.allow_test_header_context);

    let runtime_auth = RuntimeAuth {
        otlp: otlp_auth,
        query,
        admin,
    };

    ZradarRuntimeBuilder::new(config, runtime_auth).run().await
}
