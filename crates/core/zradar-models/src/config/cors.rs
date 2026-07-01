//! CORS configuration for OTLP/HTTP ingest router.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CorsConfig {
    /// List of allowed origins. If empty or not set, CORS will be permissive (*).
    #[serde(default)]
    pub allowed_origins: Vec<String>,
}
