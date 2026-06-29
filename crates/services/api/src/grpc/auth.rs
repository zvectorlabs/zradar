//! gRPC authorization helpers — mirror HTTP auth using query/admin authorizers.

use std::sync::Arc;

use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
use tonic::{Request, Status};
use zradar_traits::{AdminAuthorizer, AuthContext, AuthResolution, Capability, QueryAuthorizer};

use super::errors::into_status;

fn metadata_to_headers(metadata: &tonic::metadata::MetadataMap) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(value) = metadata.get("authorization")
        && let Ok(header_value) = HeaderValue::try_from(value.as_bytes())
    {
        headers.insert(AUTHORIZATION, header_value);
    }

    for name in ["x-workspace-id", "x-zradar-capabilities"] {
        if let Some(value) = metadata.get(name)
            && let Ok(header_value) = HeaderValue::try_from(value.as_bytes())
            && let Ok(header_name) = axum::http::HeaderName::try_from(name)
        {
            headers.insert(header_name, header_value);
        }
    }

    headers
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
    let resolution = resolution.map_err(|_| Status::unauthenticated("Invalid credentials"))?;
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
