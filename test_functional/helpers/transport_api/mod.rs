//! HTTP-path-compatible API client for dual HTTP + gRPC functional tests.
//!
//! Tests keep using REST-style paths (`/api/v1/...`); the gRPC backend maps those
//! paths to the corresponding Query/Analytics/Admin RPCs and returns HTTP-shaped JSON.

mod grpc;
mod response;

use anyhow::{Result, bail};
use reqwest::StatusCode;
use serde::Serialize;
use serde_json::Value;

pub use response::TransportResponse;

use crate::TestContext;
use crate::helpers::transport::Transport;
use crate::helpers::{ApiClient, ZradarAdminClient, ZradarQueryClient};

/// API client that speaks REST paths over either HTTP or gRPC.
#[derive(Clone)]
pub struct TransportApiClient {
    transport: Transport,
    http: ApiClient,
    query_grpc: ZradarQueryClient,
    admin_grpc: ZradarAdminClient,
}

impl TransportApiClient {
    pub fn transport(&self) -> Transport {
        self.transport
    }

    pub fn from_test_context(
        ctx: &TestContext,
        transport: Transport,
        api_key: &str,
        workspace_id: &str,
    ) -> Self {
        let mut http = ApiClient::new(ctx.config.api_url.clone());
        http.set_token(api_key.to_string());
        http.set_workspace_id(workspace_id.to_string());

        let query_grpc = ZradarQueryClient::new(ctx.config.query_grpc_url.clone())
            .with_api_key(api_key.to_string())
            .with_workspace_id(workspace_id.to_string());

        let admin_grpc = ZradarAdminClient::new(ctx.config.admin_grpc_url.clone())
            .with_api_key(api_key.to_string())
            .with_workspace_id(workspace_id.to_string());

        Self {
            transport,
            http,
            query_grpc,
            admin_grpc,
        }
    }

    pub fn set_token(&mut self, token: String) {
        self.http.set_token(token.clone());
        self.query_grpc = self.query_grpc.clone().with_api_key(token.clone());
        self.admin_grpc = self.admin_grpc.clone().with_api_key(token);
    }

    pub fn set_workspace_id(&mut self, workspace_id: String) {
        self.http.set_workspace_id(workspace_id.clone());
        self.query_grpc = self
            .query_grpc
            .clone()
            .with_workspace_id(workspace_id.clone());
        self.admin_grpc = self.admin_grpc.clone().with_workspace_id(workspace_id);
    }

    pub fn get_token(&self) -> Option<&str> {
        self.http.get_token()
    }

    pub fn workspace_id(&self) -> &str {
        self.http.workspace_id()
    }

    pub async fn get(&self, path: &str) -> Result<TransportResponse> {
        match self.transport {
            Transport::Http => self.http_get(path).await,
            Transport::Grpc => grpc::dispatch_get(self, path).await,
        }
    }

    pub async fn post<T: Serialize>(&self, path: &str, body: &T) -> Result<TransportResponse> {
        match self.transport {
            Transport::Http => self.http_post(path, body).await,
            Transport::Grpc => grpc::dispatch_post(self, path, body).await,
        }
    }

    pub async fn put<T: Serialize>(&self, path: &str, body: &T) -> Result<TransportResponse> {
        match self.transport {
            Transport::Http => self.http_put(path, body).await,
            Transport::Grpc => grpc::dispatch_put(self, path, body).await,
        }
    }

    pub async fn delete(&self, path: &str) -> Result<TransportResponse> {
        match self.transport {
            Transport::Http => self.http_delete(path).await,
            Transport::Grpc => grpc::dispatch_delete(self, path).await,
        }
    }

    pub async fn health(&self) -> Result<Value> {
        if self.transport != Transport::Http {
            bail!("health checks are HTTP-only");
        }
        self.http.health().await
    }

    pub async fn health_ready(&self) -> Result<Value> {
        if self.transport != Transport::Http {
            bail!("health checks are HTTP-only");
        }
        self.http.health_ready().await
    }

    pub async fn health_live(&self) -> Result<()> {
        if self.transport != Transport::Http {
            bail!("health checks are HTTP-only");
        }
        self.http.health_live().await
    }

    pub(crate) fn query_grpc(&self) -> &ZradarQueryClient {
        &self.query_grpc
    }

    pub(crate) fn admin_grpc(&self) -> &ZradarAdminClient {
        &self.admin_grpc
    }

    async fn http_get(&self, path: &str) -> Result<TransportResponse> {
        let response = self.http.get(path).await?;
        TransportResponse::from_http(response).await
    }

    async fn http_post<T: Serialize>(&self, path: &str, body: &T) -> Result<TransportResponse> {
        let response = self.http.post(path, body).await?;
        TransportResponse::from_http(response).await
    }

    async fn http_put<T: Serialize>(&self, path: &str, body: &T) -> Result<TransportResponse> {
        let response = self.http.put(path, body).await?;
        TransportResponse::from_http(response).await
    }

    async fn http_delete(&self, path: &str) -> Result<TransportResponse> {
        let response = self.http.delete(path).await?;
        TransportResponse::from_http(response).await
    }
}

impl ApiClient {
    pub fn into_transport(self) -> TransportApiClient {
        TransportApiClient {
            transport: Transport::Http,
            http: self,
            query_grpc: ZradarQueryClient::new(String::new()),
            admin_grpc: ZradarAdminClient::new(String::new()),
        }
    }
}

impl TransportApiClient {
    pub async fn get_json(response: TransportResponse) -> Result<Value> {
        response.json().await
    }
}

impl TransportResponse {
    pub fn is_success(&self) -> bool {
        self.status().is_success()
    }
}

impl PartialEq<StatusCode> for TransportResponse {
    fn eq(&self, other: &StatusCode) -> bool {
        self.status() == *other
    }
}
