//! Transport-agnostic domain errors for zradar service layer.
//!
//! `ServiceError` is used by all service traits. Each transport layer (HTTP, gRPC)
//! maps it to its own wire format — axum `IntoResponse` or tonic `Status`.

use std::fmt;
use std::time::Duration;

/// Domain error — used by service traits. Transport-agnostic.
#[derive(Debug)]
pub enum ServiceError {
    /// Resource not found (HTTP 404, gRPC NOT_FOUND)
    NotFound(String),
    /// Authentication failed (HTTP 401, gRPC UNAUTHENTICATED)
    Unauthorized(String),
    /// Insufficient permissions (HTTP 403, gRPC PERMISSION_DENIED)
    Forbidden(String),
    /// Bad request / validation failure (HTTP 400, gRPC INVALID_ARGUMENT)
    InvalidInput(String),
    /// Internal server error (HTTP 500, gRPC INTERNAL)
    Internal(String),
    /// Rate limit / quota exceeded (HTTP 429, gRPC RESOURCE_EXHAUSTED)
    ResourceExhausted(String),
    /// Feature not implemented (HTTP 501, gRPC UNIMPLEMENTED)
    Unimplemented(String),
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(m) => write!(f, "not found: {m}"),
            Self::Unauthorized(m) => write!(f, "unauthorized: {m}"),
            Self::Forbidden(m) => write!(f, "forbidden: {m}"),
            Self::InvalidInput(m) => write!(f, "invalid input: {m}"),
            Self::Internal(m) => write!(f, "internal: {m}"),
            Self::ResourceExhausted(m) => write!(f, "resource exhausted: {m}"),
            Self::Unimplemented(m) => write!(f, "unimplemented: {m}"),
        }
    }
}

impl std::error::Error for ServiceError {}

// Convenience constructors
impl ServiceError {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }
    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::Unauthorized(msg.into())
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::Forbidden(msg.into())
    }
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
    pub fn resource_exhausted(msg: impl Into<String>) -> Self {
        Self::ResourceExhausted(msg.into())
    }
    pub fn unimplemented(msg: impl Into<String>) -> Self {
        Self::Unimplemented(msg.into())
    }
}

/// A telemetry write refused for a transient reason, such as the write-ahead
/// log's filesystem running low on free space.
///
/// `TelemetryWriter` implementations return it (inside `anyhow::Error`) when
/// the same write is expected to succeed later. Ingest transports map it to a
/// retryable response (gRPC `UNAVAILABLE` with `RetryInfo`, HTTP `503` with
/// `Retry-After`) so exporters back off and retry instead of dropping data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteUnavailable {
    /// Human-readable cause.
    pub reason: String,
    /// Suggested delay before the client retries.
    pub retry_after: Duration,
}

impl WriteUnavailable {
    pub fn new(reason: impl Into<String>, retry_after: Duration) -> Self {
        Self {
            reason: reason.into(),
            retry_after,
        }
    }

    /// Find a `WriteUnavailable` anywhere in an `anyhow` error chain.
    pub fn find(error: &anyhow::Error) -> Option<&Self> {
        error.chain().find_map(|cause| cause.downcast_ref::<Self>())
    }
}

impl fmt::Display for WriteUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "write temporarily unavailable: {}", self.reason)
    }
}

impl std::error::Error for WriteUnavailable {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_unavailable_is_found_through_context() {
        let err = anyhow::Error::new(WriteUnavailable::new("disk low", Duration::from_secs(5)))
            .context("inserting spans");
        let found = WriteUnavailable::find(&err).expect("found in chain");
        assert_eq!(found.reason, "disk low");
        assert_eq!(found.retry_after, Duration::from_secs(5));

        let other = anyhow::anyhow!("boom");
        assert!(WriteUnavailable::find(&other).is_none());
    }
}
