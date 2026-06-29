//! Maps `ServiceError` to `tonic::Status` for gRPC transport.
//!
//! Because neither `ServiceError` nor `tonic::Status` are defined in this crate,
//! Rust's orphan rule prevents a blanket `From` impl. Instead we provide a
//! free function that handlers use via `.map_err(into_status)?`.

use tonic::Status;
use zradar_traits::ServiceError;

/// Convert a [`ServiceError`] into a [`tonic::Status`] with the appropriate
/// gRPC status code.
///
/// Usage in gRPC handlers:
/// ```ignore
/// let result = service.query_traces(&ctx, &params)
///     .await
///     .map_err(into_status)?;
/// ```
pub fn into_status(e: ServiceError) -> Status {
    match e {
        ServiceError::NotFound(m) => Status::not_found(m),
        ServiceError::Unauthorized(m) => Status::unauthenticated(m),
        ServiceError::Forbidden(m) => Status::permission_denied(m),
        ServiceError::InvalidInput(m) => Status::invalid_argument(m),
        ServiceError::Internal(m) => Status::internal(m),
        ServiceError::ResourceExhausted(m) => Status::resource_exhausted(m),
        ServiceError::Unimplemented(m) => Status::unimplemented(m),
    }
}
