use anyhow::{Context, Result};
use reqwest::{Response, StatusCode};
use serde_json::Value;

/// Unified HTTP/gRPC response for functional tests.
pub struct TransportResponse {
    status: StatusCode,
    body: Option<Value>,
    text: Option<String>,
}

impl TransportResponse {
    pub fn from_grpc(status: u16, body: Option<Value>, text: Option<String>) -> Self {
        Self {
            status: StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            body,
            text,
        }
    }

    pub async fn from_http(response: Response) -> Result<Self> {
        let status = response.status();
        let text = response.text().await.context("read HTTP response body")?;
        let body = if text.is_empty() {
            None
        } else {
            serde_json::from_str(&text).ok()
        };
        Ok(Self {
            status,
            body,
            text: Some(text),
        })
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub async fn json(self) -> Result<Value> {
        if let Some(body) = self.body {
            return Ok(body);
        }
        if let Some(text) = self.text {
            if text.is_empty() {
                return Ok(Value::Null);
            }
            return serde_json::from_str(&text).context("parse response JSON");
        }
        Ok(Value::Null)
    }

    pub async fn text(self) -> Result<String> {
        if let Some(text) = self.text {
            return Ok(text);
        }
        if let Some(body) = self.body {
            return Ok(body.to_string());
        }
        Ok(String::new())
    }
}

// Tests compare `response.status()` to numeric literals via `== 200`.
impl PartialEq<u16> for TransportResponse {
    fn eq(&self, other: &u16) -> bool {
        self.status.as_u16() == *other
    }
}

impl PartialEq<i32> for TransportResponse {
    fn eq(&self, other: &i32) -> bool {
        self.status.as_u16() == *other as u16
    }
}
