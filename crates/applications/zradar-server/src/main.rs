//! zradar standalone OSS binary.
//!
//! Loads `config.toml`, builds a [`ConfigAuthenticator`] from `[[api_keys]]`,
//! and delegates all server startup to [`ZradarRuntimeBuilder`].
//!
//! This binary contains no platform-specific code. External platform wrapper binaries
//! follow the same pattern using their own `Authenticator` and `AdminAuthorizer` implementations.

use anyhow::Result;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

use zradar_auth_config::ConfigAuthenticator;
use zradar_models::Config;
use zradar_runtime::{ApiKeyAdminAuthorizer, RuntimeAuth, ZradarRuntimeBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,zradar=debug")),
        )
        .init();

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

    let admin = Arc::new(ApiKeyAdminAuthorizer::from_config(api_keys));

    let runtime_auth = RuntimeAuth {
        otlp: otlp_auth,
        admin,
    };

    ZradarRuntimeBuilder::new(config, runtime_auth).run().await
}
