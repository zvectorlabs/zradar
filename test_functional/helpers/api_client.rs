//! HTTP API client for testing REST endpoints

use anyhow::{Context, Result};
use reqwest::StatusCode;
use reqwest::{Client, Response};
use serde::Serialize;
use serde_json::Value;
use std::time::Duration;

/// HTTP client for API testing with API key authentication.
pub struct ApiClient {
    client: Client,
    base_url: String,
    token: Option<String>,
    workspace_id: Option<String>,
}

impl ApiClient {
    /// Create a new API client.
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        Self {
            client,
            base_url,
            token: None,
            workspace_id: None,
        }
    }

    /// Set the API key (used as Bearer token).
    pub fn set_token(&mut self, token: String) {
        self.token = Some(token);
    }

    /// Get current token.
    pub fn get_token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    /// Set workspace ID header override.
    pub fn set_workspace_id(&mut self, workspace_id: String) {
        self.workspace_id = Some(workspace_id);
    }

    /// Return the configured workspace ID header.
    pub fn workspace_id(&self) -> &str {
        self.workspace_id.as_deref().expect("missing")
    }

    /// Apply common headers (auth + workspace/workspace overrides) to a request builder.
    fn apply_headers(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        if let Some(tid) = &self.workspace_id {
            req = req.header("x-workspace-id", tid.as_str());
        }
        req
    }

    fn build_url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    // =========================================================================
    // Generic HTTP Methods
    // =========================================================================

    pub async fn get(&self, path: &str) -> Result<Response> {
        let req = self.client.get(self.build_url(path));
        self.apply_headers(req)
            .send()
            .await
            .context("GET request failed")
    }

    pub async fn get_with_header(
        &self,
        path: &str,
        header_name: &str,
        header_value: &str,
    ) -> Result<Response> {
        let req = self
            .client
            .get(self.build_url(path))
            .header(header_name, header_value);
        self.apply_headers(req)
            .send()
            .await
            .context("GET request with header failed")
    }

    pub async fn post<T: Serialize>(&self, path: &str, body: &T) -> Result<Response> {
        let req = self.client.post(self.build_url(path)).json(body);
        self.apply_headers(req)
            .send()
            .await
            .context("POST request failed")
    }

    pub async fn put<T: Serialize>(&self, path: &str, body: &T) -> Result<Response> {
        let req = self.client.put(self.build_url(path)).json(body);
        self.apply_headers(req)
            .send()
            .await
            .context("PUT request failed")
    }

    pub async fn delete(&self, path: &str) -> Result<Response> {
        let req = self.client.delete(self.build_url(path));
        self.apply_headers(req)
            .send()
            .await
            .context("DELETE request failed")
    }

    // =========================================================================
    // Response helpers
    // =========================================================================

    pub fn assert_status(response: &Response, expected: StatusCode) -> Result<()> {
        let actual = response.status();
        if actual != expected {
            anyhow::bail!("Expected status {}, got {}", expected, actual);
        }
        Ok(())
    }

    pub async fn get_json(response: Response) -> Result<Value> {
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Request failed with status {}. Body: {}", status, body);
        }
        response
            .json()
            .await
            .context("Failed to parse JSON response")
    }

    // =========================================================================
    // Health Endpoints
    // =========================================================================

    pub async fn health(&self) -> Result<Value> {
        let response = self.get("/health").await?;
        Self::get_json(response).await
    }

    pub async fn health_ready(&self) -> Result<Value> {
        let response = self.get("/health/ready").await?;
        Self::get_json(response).await
    }

    pub async fn health_live(&self) -> Result<()> {
        let response = self.get("/health/live").await?;
        Self::assert_status(&response, StatusCode::OK)
    }

    // =========================================================================
    // Telemetry Query Endpoints
    // =========================================================================

    /// Query traces.
    pub async fn query_traces(&self) -> Result<Value> {
        let response = self.get("/api/v1/traces").await?;
        response.json().await.map_err(Into::into)
    }

    /// Get a single trace by ID.
    pub async fn get_trace(&self, trace_id: &str) -> Result<Value> {
        let response = self.get(&format!("/api/v1/traces/{}", trace_id)).await?;
        response.json().await.map_err(Into::into)
    }

    /// Query spans.
    pub async fn query_spans(&self) -> Result<Value> {
        let response = self.get("/api/v1/spans").await?;
        response.json().await.map_err(Into::into)
    }

    /// Get analytics.
    pub async fn get_analytics(&self) -> Result<Value> {
        let response = self.get("/api/v1/analytics").await?;
        response.json().await.map_err(Into::into)
    }

    /// Query logs.
    pub async fn query_logs(&self) -> Result<Value> {
        let response = self.get("/api/v1/logs").await?;
        response.json().await.map_err(Into::into)
    }

    /// Query metrics.
    pub async fn query_metrics(&self) -> Result<Value> {
        let response = self.get("/api/v1/metrics").await?;
        response.json().await.map_err(Into::into)
    }
}
