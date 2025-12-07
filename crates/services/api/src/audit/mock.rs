//! Mock audit logger for testing

use async_trait::async_trait;
use std::sync::Mutex;
use uuid::Uuid;

use zradar_traits::{AuditEvent, AuditLog, AuditLogger};

/// Mock audit logger for testing
#[derive(Default)]
pub struct MockAuditLogger {
    pub logs: Mutex<Vec<AuditEvent>>,
}

impl MockAuditLogger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_logged_events(&self) -> Vec<AuditEvent> {
        self.logs.lock().unwrap().clone()
    }
}

#[async_trait]
impl AuditLogger for MockAuditLogger {
    async fn log(&self, event: AuditEvent) -> anyhow::Result<()> {
        self.logs.lock().unwrap().push(event);
        Ok(())
    }

    async fn get_logs(
        &self,
        _org_id: Option<Uuid>,
        _limit: Option<i64>,
    ) -> anyhow::Result<Vec<AuditLog>> {
        Ok(vec![])
    }
}
