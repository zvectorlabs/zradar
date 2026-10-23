//! Admission control and protocol limits for the OTLP ingest listeners.
//!
//! These limits keep the public ingest endpoints (gRPC :4317, HTTP :4318)
//! responsive under request floods: excess requests are rejected immediately
//! with a retryable status (gRPC `UNAVAILABLE`, HTTP `503`) instead of queueing
//! until clients time out.
//!
//! ```toml
//! [otlp_limits]
//! max_concurrent_requests = 256
//! request_timeout_secs    = 30
//! ```

use serde::Deserialize;

/// OTLP ingest limits. Every field has a serde default, and `Default` matches
/// those defaults exactly.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct OtlpLimitsConfig {
    /// Maximum in-flight export requests per listener, shared across all
    /// connections (gRPC and HTTP each get their own budget). Requests beyond
    /// the limit are rejected immediately. `0` disables the limit.
    /// Default: 256
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,

    /// Maximum time a single export request may take, including reading the
    /// request body. `0` disables the timeout. Default: 30
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,

    /// Maximum size of an OTLP gRPC message in bytes, enforced both on the
    /// wire (`OUT_OF_RANGE` above it) and after decompression
    /// (`RESOURCE_EXHAUSTED` once decompressed output passes it, which stops
    /// decompression bombs). Default: 4 MiB, matching the common gRPC and
    /// OpenTelemetry Collector receiver default.
    #[serde(default = "default_grpc_max_decoding_message_bytes")]
    pub grpc_max_decoding_message_bytes: usize,

    /// Maximum concurrent HTTP/2 streams per gRPC connection. Default: 100
    #[serde(default = "default_grpc_max_concurrent_streams")]
    pub grpc_max_concurrent_streams: u32,

    /// Interval between HTTP/2 keepalive pings on gRPC connections, in
    /// seconds. `0` disables keepalive pings. Default: 60
    #[serde(default = "default_grpc_http2_keepalive_interval_secs")]
    pub grpc_http2_keepalive_interval_secs: u64,

    /// How long to wait for a keepalive ping acknowledgement before closing
    /// the connection, in seconds. Default: 20
    #[serde(default = "default_grpc_http2_keepalive_timeout_secs")]
    pub grpc_http2_keepalive_timeout_secs: u64,

    /// TCP keepalive for accepted gRPC connections, in seconds. `0` disables
    /// it. Default: 60
    #[serde(default = "default_grpc_tcp_keepalive_secs")]
    pub grpc_tcp_keepalive_secs: u64,

    /// Maximum OTLP/HTTP request body size in bytes. Default: 8 MiB
    #[serde(default = "default_http_max_body_bytes")]
    pub http_max_body_bytes: usize,

    /// Maximum time for an OTLP/HTTP client to start a request on a new
    /// connection and to send a complete HTTP/1 request head, in seconds.
    /// Bounds slow-header ("slowloris") clients. It is also the HTTP/1
    /// keep-alive idle limit: an idle connection is closed after this delay,
    /// and exporters reconnect on their next export. `0` disables it.
    /// Default: 30
    #[serde(default = "default_http_header_read_timeout_secs")]
    pub http_header_read_timeout_secs: u64,

    /// Maximum time for an OTLP/HTTP client to send the whole request body,
    /// in seconds. Slower bodies get `408 Request Timeout`. Keep it below
    /// `request_timeout_secs`, which also covers the body upload (`504`).
    /// `0` disables it. Default: 15
    #[serde(default = "default_http_body_read_timeout_secs")]
    pub http_body_read_timeout_secs: u64,

    /// Maximum size of an OTLP/HTTP request body after `Content-Encoding`
    /// (`gzip`, `zstd`) is removed, in bytes. Decompression stops as soon as
    /// the output passes it and the request gets `413 Payload Too Large`, so a
    /// small compressed body cannot expand into an unbounded allocation
    /// ("decompression bomb"). Uncompressed bodies are bounded by the smaller
    /// of this and `http_max_body_bytes`. Default (unset): `http_max_body_bytes`,
    /// so a decoded body may never exceed what an uncompressed one may be.
    /// See [`Self::effective_http_max_decoded_body_bytes`].
    #[serde(default)]
    pub http_max_decoded_body_bytes: Option<usize>,

    /// Maximum time to wait for the configured authenticator to verify an
    /// ingest API key, in seconds. A verification that takes longer is
    /// treated as the key backend being unavailable: gRPC `UNAVAILABLE` /
    /// HTTP `503` with a retry delay, so exporters retry rather than drop
    /// data. `0` disables the limit (the request timeout still applies).
    /// Default: 10
    #[serde(default = "default_auth_timeout_secs")]
    pub auth_timeout_secs: u64,
}

impl Default for OtlpLimitsConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: default_max_concurrent_requests(),
            request_timeout_secs: default_request_timeout_secs(),
            grpc_max_decoding_message_bytes: default_grpc_max_decoding_message_bytes(),
            grpc_max_concurrent_streams: default_grpc_max_concurrent_streams(),
            grpc_http2_keepalive_interval_secs: default_grpc_http2_keepalive_interval_secs(),
            grpc_http2_keepalive_timeout_secs: default_grpc_http2_keepalive_timeout_secs(),
            grpc_tcp_keepalive_secs: default_grpc_tcp_keepalive_secs(),
            http_max_body_bytes: default_http_max_body_bytes(),
            http_header_read_timeout_secs: default_http_header_read_timeout_secs(),
            http_body_read_timeout_secs: default_http_body_read_timeout_secs(),
            http_max_decoded_body_bytes: None,
            auth_timeout_secs: default_auth_timeout_secs(),
        }
    }
}

impl OtlpLimitsConfig {
    /// The OTLP/HTTP decoded body cap: `http_max_decoded_body_bytes`, or
    /// `http_max_body_bytes` when unset.
    pub fn effective_http_max_decoded_body_bytes(&self) -> usize {
        self.http_max_decoded_body_bytes
            .unwrap_or(self.http_max_body_bytes)
    }
}

fn default_max_concurrent_requests() -> usize {
    256
}
fn default_request_timeout_secs() -> u64 {
    30
}
fn default_grpc_max_decoding_message_bytes() -> usize {
    4 * 1024 * 1024
}
fn default_grpc_max_concurrent_streams() -> u32 {
    100
}
fn default_grpc_http2_keepalive_interval_secs() -> u64 {
    60
}
fn default_grpc_http2_keepalive_timeout_secs() -> u64 {
    20
}
fn default_grpc_tcp_keepalive_secs() -> u64 {
    60
}
fn default_http_max_body_bytes() -> usize {
    8 * 1024 * 1024
}
fn default_http_header_read_timeout_secs() -> u64 {
    30
}
fn default_http_body_read_timeout_secs() -> u64 {
    15
}
fn default_auth_timeout_secs() -> u64 {
    10
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_serde_defaults() {
        let from_toml: OtlpLimitsConfig = toml::from_str("").expect("empty table parses");
        assert_eq!(OtlpLimitsConfig::default(), from_toml);
    }

    #[test]
    fn overrides_parse() {
        let cfg: OtlpLimitsConfig =
            toml::from_str("max_concurrent_requests = 1\nhttp_max_body_bytes = 1024\n")
                .expect("parses");
        assert_eq!(cfg.max_concurrent_requests, 1);
        assert_eq!(cfg.http_max_body_bytes, 1024);
        assert_eq!(cfg.request_timeout_secs, 30);
        assert_eq!(cfg.http_header_read_timeout_secs, 30);
        assert_eq!(cfg.http_body_read_timeout_secs, 15);
        assert!(cfg.http_body_read_timeout_secs < cfg.request_timeout_secs);
        assert_eq!(cfg.http_max_decoded_body_bytes, None);
        // Unset, the decoded cap follows the wire cap.
        assert_eq!(cfg.effective_http_max_decoded_body_bytes(), 1024);
        assert_eq!(cfg.auth_timeout_secs, 10);
        assert!(cfg.auth_timeout_secs < cfg.request_timeout_secs);
    }

    #[test]
    fn decoded_body_cap_and_auth_timeout_parse() {
        let cfg: OtlpLimitsConfig =
            toml::from_str("http_max_decoded_body_bytes = 2048\nauth_timeout_secs = 0\n")
                .expect("parses");
        assert_eq!(cfg.http_max_decoded_body_bytes, Some(2048));
        assert_eq!(cfg.effective_http_max_decoded_body_bytes(), 2048);
        assert_eq!(
            OtlpLimitsConfig::default().effective_http_max_decoded_body_bytes(),
            8 * 1024 * 1024
        );
        assert_eq!(cfg.auth_timeout_secs, 0);
    }

    #[test]
    fn http_read_timeouts_can_be_disabled() {
        let cfg: OtlpLimitsConfig =
            toml::from_str("http_header_read_timeout_secs = 0\nhttp_body_read_timeout_secs = 0\n")
                .expect("parses");
        assert_eq!(cfg.http_header_read_timeout_secs, 0);
        assert_eq!(cfg.http_body_read_timeout_secs, 0);
    }
}
