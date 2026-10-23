//! Retryable rejections for the OTLP ingest paths.
//!
//! The OTLP specification defines which failures an exporter retries:
//!
//! - gRPC: `UNAVAILABLE` is retryable; `RESOURCE_EXHAUSTED` is retryable only
//!   when the status carries `RetryInfo`. Servers signalling backpressure
//!   SHOULD use `UNAVAILABLE` and MAY attach `RetryInfo`
//!   (<https://opentelemetry.io/docs/specs/otlp/#failures>,
//!   <https://opentelemetry.io/docs/specs/otlp/#otlpgrpc-throttling>).
//! - HTTP: only `429`, `502`, `503` and `504` are retried; an overloaded server
//!   SHOULD answer `429` or `503` and MAY send `Retry-After`
//!   (<https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes>).
//!
//! Transient server-side conditions (tripped circuit breaker, WAL disk below
//! its free-space floor) therefore map to gRPC `UNAVAILABLE` + `RetryInfo` and
//! HTTP `503` + `Retry-After`, so well-behaved exporters back off and retry
//! instead of dropping data.

use std::time::Duration;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};
use zradar_traits::WriteUnavailable;

/// gRPC `UNAVAILABLE` carrying a `google.rpc.RetryInfo` detail.
pub(crate) fn unavailable(message: impl Into<String>, retry_after: Duration) -> Status {
    Status::with_error_details(
        Code::Unavailable,
        message,
        ErrorDetails::with_retry_info(Some(retry_after)),
    )
}

/// Map a `TelemetryWriter` failure to a gRPC status: retryable
/// `UNAVAILABLE` for [`WriteUnavailable`], `INTERNAL` otherwise.
pub(crate) fn write_error_status(what: &str, error: &anyhow::Error) -> Status {
    match WriteUnavailable::find(error) {
        Some(transient) => unavailable(
            format!("Failed to insert {what}: {}", transient.reason),
            transient.retry_after,
        ),
        None => Status::internal(format!("Failed to insert {what}: {error}")),
    }
}

/// Map a `TelemetryWriter` failure to an OTLP/HTTP response: `503` with
/// `Retry-After` for [`WriteUnavailable`], `500` otherwise.
pub(crate) fn write_error_response(error: &anyhow::Error) -> Response {
    match WriteUnavailable::find(error) {
        Some(transient) => http_unavailable(transient.retry_after),
        None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// `503 Service Unavailable` with a `Retry-After` header (whole seconds,
/// rounded up, at least 1).
pub(crate) fn http_unavailable(retry_after: Duration) -> Response {
    with_retry_after(StatusCode::SERVICE_UNAVAILABLE, retry_after)
}

/// The retry delay a status asks for, if it carries `RetryInfo`.
pub(crate) fn retry_delay(status: &Status) -> Option<Duration> {
    status
        .get_details_retry_info()
        .and_then(|info| info.retry_delay)
}

pub(crate) fn with_retry_after(code: StatusCode, retry_after: Duration) -> Response {
    let mut response = code.into_response();
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, retry_after_header(retry_after));
    response
}

fn retry_after_header(retry_after: Duration) -> HeaderValue {
    let secs = retry_after
        .as_secs()
        .saturating_add(u64::from(retry_after.subsec_nanos() > 0))
        .max(1);
    HeaderValue::from(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_carries_retry_info() {
        let status = unavailable("busy", Duration::from_secs(2));
        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "busy");
        assert_eq!(retry_delay(&status), Some(Duration::from_secs(2)));
    }

    #[test]
    fn plain_status_has_no_retry_delay() {
        assert_eq!(retry_delay(&Status::internal("x")), None);
    }

    #[test]
    fn transient_write_errors_are_retryable() {
        let err = anyhow::Error::new(WriteUnavailable::new(
            "WAL disk low",
            Duration::from_secs(5),
        ));
        let status = write_error_status("spans", &err);
        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("WAL disk low"));
        assert_eq!(retry_delay(&status), Some(Duration::from_secs(5)));

        let response = write_error_response(&err);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "5");
    }

    #[test]
    fn other_write_errors_are_internal() {
        let err = anyhow::anyhow!("parquet exploded");
        let status = write_error_status("spans", &err);
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(retry_delay(&status), None);

        let response = write_error_response(&err);
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
    }

    #[test]
    fn retry_after_rounds_up_to_whole_seconds() {
        assert_eq!(retry_after_header(Duration::ZERO), "1");
        assert_eq!(retry_after_header(Duration::from_millis(1)), "1");
        assert_eq!(retry_after_header(Duration::from_millis(1500)), "2");
        assert_eq!(retry_after_header(Duration::from_secs(30)), "30");
    }
}
