use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RetentionPolicy {
    pub id: i64,
    pub org_id: Uuid,
    pub default_days: i32,
    pub project_overrides: serde_json::Value,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewRetentionPolicy {
    pub org_id: Uuid,
    pub default_days: i32,
    pub project_overrides: HashMap<Uuid, u32>,
}

impl RetentionPolicy {
    pub fn project_overrides_map(&self) -> anyhow::Result<HashMap<Uuid, u32>> {
        let overrides = serde_json::from_value(self.project_overrides.clone())?;
        Ok(overrides)
    }
}
