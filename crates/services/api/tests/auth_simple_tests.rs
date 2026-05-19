use api::http::AuthContext;
use async_trait::async_trait;
use axum::extract::FromRequestParts;
use axum::http::Request;
use std::sync::Arc;
use uuid::Uuid;
use zradar_models::RequestContext;
use zradar_traits::Authenticator;

struct TestAuthenticator;

#[async_trait]
impl Authenticator for TestAuthenticator {
    async fn authenticate(&self, token: &str) -> anyhow::Result<RequestContext> {
        if token != "valid-token" {
            anyhow::bail!("invalid token");
        }

        Ok(RequestContext {
            tenant_id: Uuid::nil().to_string(),
            project_id: Uuid::nil().to_string(),
        })
    }
}

#[tokio::test]
async fn test_auth_context_accepts_valid_bearer_token() {
    let request = Request::builder()
        .header("authorization", "Bearer valid-token")
        .body(())
        .unwrap();
    let (mut parts, _) = request.into_parts();
    parts
        .extensions
        .insert(Arc::new(TestAuthenticator) as Arc<dyn Authenticator>);

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let AuthContext(context) = result.unwrap_or_else(|_| unreachable!());

    assert_eq!(context.tenant_id, Uuid::nil().to_string());
    assert_eq!(context.project_id, Uuid::nil().to_string());
}

#[tokio::test]
async fn test_auth_context_applies_tenant_and_project_overrides() {
    let tenant_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let request = Request::builder()
        .header("authorization", "Bearer valid-token")
        .header("x-tenant-id", &tenant_id)
        .header("x-project-id", &project_id)
        .body(())
        .unwrap();
    let (mut parts, _) = request.into_parts();
    parts
        .extensions
        .insert(Arc::new(TestAuthenticator) as Arc<dyn Authenticator>);

    let result = AuthContext::from_request_parts(&mut parts, &()).await;
    assert!(result.is_ok());
    let AuthContext(context) = result.unwrap_or_else(|_| unreachable!());

    assert_eq!(context.tenant_id, tenant_id);
    assert_eq!(context.project_id, project_id);
}

#[tokio::test]
async fn test_auth_context_rejects_invalid_token() {
    let request = Request::builder()
        .header("authorization", "Bearer invalid-token")
        .body(())
        .unwrap();
    let (mut parts, _) = request.into_parts();
    parts
        .extensions
        .insert(Arc::new(TestAuthenticator) as Arc<dyn Authenticator>);

    let result = AuthContext::from_request_parts(&mut parts, &()).await;

    assert!(result.is_err());
}
