//! gRPC authorization helpers — mirror HTTP auth using query/admin authorizers.

use std::sync::Arc;

use axum::http::HeaderMap;
use tonic::{Code, Request, Status};
use tonic_types::{ErrorDetails, StatusExt};
use zradar_traits::{
    AdminAuthorizer, AuthContext, AuthResolution, AuthUnavailable, Capability, QueryAuthorizer,
};

use super::errors::into_status;

/// All request metadata as HTTP headers, so authorizers see exactly what they
/// see on the HTTP routes (`authorization` plus any context headers a gateway
/// adds). `-bin` entries keep their wire (base64) form.
fn metadata_to_headers(metadata: &tonic::metadata::MetadataMap) -> HeaderMap {
    metadata.clone().into_headers()
}

/// Map an authorizer failure: retryable `UNAVAILABLE` (with `RetryInfo`) when
/// the verification backend could not be reached, `UNAUTHENTICATED` otherwise.
fn rejection_status(error: &anyhow::Error) -> Status {
    match AuthUnavailable::find(error) {
        Some(transient) => {
            // Per request, so debug only: the authorizer owns backend error logging.
            tracing::debug!(reason = %transient.reason, "credential verification unavailable");
            Status::with_error_details(
                Code::Unavailable,
                "credential verification temporarily unavailable",
                ErrorDetails::with_retry_info(Some(transient.retry_after)),
            )
        }
        None => Status::unauthenticated("Invalid credentials"),
    }
}

fn auth_context_from_resolution(resolution: AuthResolution) -> AuthContext {
    let capabilities = resolution
        .capability_keys
        .iter()
        .filter_map(|key| Capability::from_key(key))
        .collect();
    AuthContext::new(resolution.context, capabilities)
}

async fn authorize_request<T>(
    resolution: anyhow::Result<AuthResolution>,
    cap: Capability,
    request: Request<T>,
) -> Result<(T, AuthContext), Status> {
    let resolution = resolution.map_err(|error| rejection_status(&error))?;
    let auth = auth_context_from_resolution(resolution);
    auth.require(cap).map_err(into_status)?;
    Ok((request.into_inner(), auth))
}

/// Authorize a Query gRPC request and enforce the required capability.
pub async fn authorize_query<T>(
    authorizer: &Arc<dyn QueryAuthorizer>,
    request: Request<T>,
    cap: Capability,
) -> Result<(T, AuthContext), Status> {
    let headers = metadata_to_headers(request.metadata());
    authorize_request(authorizer.authorize(&headers).await, cap, request).await
}

/// Authorize an Admin gRPC request and enforce the required capability.
pub async fn authorize_admin<T>(
    authorizer: &Arc<dyn AdminAuthorizer>,
    request: Request<T>,
    cap: Capability,
) -> Result<(T, AuthContext), Status> {
    let headers = metadata_to_headers(request.metadata());
    authorize_request(authorizer.authorize(&headers).await, cap, request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::http::header::AUTHORIZATION;
    use tonic::metadata::MetadataValue;
    use zradar_models::RequestContext;

    struct MockQueryAuthorizer;

    #[async_trait]
    impl QueryAuthorizer for MockQueryAuthorizer {
        async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AuthResolution> {
            let bearer = headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .ok_or_else(|| anyhow::anyhow!("missing auth"))?;
            if bearer != "valid" {
                anyhow::bail!("invalid");
            }
            Ok(AuthResolution {
                context: RequestContext::new(uuid::Uuid::nil().into()),
                capability_keys: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn authorize_query_reads_bearer_metadata() {
        let auth: Arc<dyn QueryAuthorizer> = Arc::new(MockQueryAuthorizer);
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_static("Bearer valid"));

        let (_, ctx) = authorize_query(&auth, request, Capability::ReadTraces)
            .await
            .unwrap();
        assert_eq!(ctx.workspace_id(), uuid::Uuid::nil().into());
    }

    /// Records the headers it is given and reports its backend down for
    /// `Bearer down`.
    #[derive(Default)]
    struct RecordingAuthorizer {
        seen: std::sync::Mutex<Option<HeaderMap>>,
    }

    #[async_trait]
    impl AdminAuthorizer for RecordingAuthorizer {
        async fn authorize(&self, headers: &HeaderMap) -> anyhow::Result<AuthResolution> {
            *self.seen.lock().unwrap() = Some(headers.clone());
            if headers.get(AUTHORIZATION).map(|v| v.as_bytes()) == Some(b"Bearer down") {
                return Err(anyhow::Error::new(AuthUnavailable::new(
                    "identity backend timed out",
                    std::time::Duration::from_secs(4),
                )));
            }
            Ok(AuthResolution {
                context: RequestContext::new(uuid::Uuid::nil().into()),
                capability_keys: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn authorize_admin_forwards_all_metadata() {
        let recorder = Arc::new(RecordingAuthorizer::default());
        let auth: Arc<dyn AdminAuthorizer> = recorder.clone();
        let mut request = Request::new(());
        let metadata = request.metadata_mut();
        metadata.insert("authorization", MetadataValue::from_static("Bearer valid"));
        metadata.insert("x-workspace-id", MetadataValue::from_static("ws-1"));
        metadata.insert("x-project-id", MetadataValue::from_static("proj-7"));
        metadata.insert("x-custom-context", MetadataValue::from_static("abc"));
        metadata.insert_bin(
            "x-trace-context-bin",
            tonic::metadata::MetadataValue::from_bytes(b"\x01\x02"),
        );

        authorize_admin(&auth, request, Capability::Admin)
            .await
            .unwrap();

        let seen = recorder.seen.lock().unwrap().clone().unwrap();
        assert_eq!(seen[AUTHORIZATION], "Bearer valid");
        assert_eq!(seen["x-workspace-id"], "ws-1");
        assert_eq!(seen["x-project-id"], "proj-7");
        assert_eq!(seen["x-custom-context"], "abc");
        assert!(seen.contains_key("x-trace-context-bin"));
    }

    #[tokio::test]
    async fn unavailable_authorizer_backend_is_retryable() {
        let auth: Arc<dyn AdminAuthorizer> = Arc::new(RecordingAuthorizer::default());
        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", MetadataValue::from_static("Bearer down"));
        let status = authorize_admin(&auth, request, Capability::Admin)
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(!status.message().contains("identity backend"));
        assert_eq!(
            status
                .get_details_retry_info()
                .and_then(|info| info.retry_delay),
            Some(std::time::Duration::from_secs(4))
        );
    }

    #[tokio::test]
    async fn authorize_query_rejects_missing_bearer() {
        let auth: Arc<dyn QueryAuthorizer> = Arc::new(MockQueryAuthorizer);
        let request = Request::new(());
        let err = authorize_query(&auth, request, Capability::ReadTraces)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
