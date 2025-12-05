//! Metric data model

use serde::{Deserialize, Serialize};
use clickhouse::Row;

/// Metric represents aggregated time-series data
#[derive(Debug, Clone, Serialize, Deserialize, Row, sqlx::FromRow)]
pub struct Metric {
    // Identity
    pub metric_name: String,
    pub metric_type: String,      // Stored as string in DB
    
    // Timing
    pub timestamp: i64,
    
    // Hierarchy
    pub tenant_id: String,
    pub project_id: String,
    
    // Values
    pub value: f64,
    pub count: i64,               // i64 for PostgreSQL compat
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    
    // Common labels
    pub service_name: String,
    pub agent_name: String,
    pub user_id: String,
    pub session_id: String,
    
    // All other labels as JSON
    pub labels: String,
}

/// MetricType enum for type-safe metric types
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

impl From<MetricType> for String {
    fn from(mt: MetricType) -> String {
        match mt {
            MetricType::Counter => "COUNTER".to_string(),
            MetricType::Gauge => "GAUGE".to_string(),
            MetricType::Histogram => "HISTOGRAM".to_string(),
            MetricType::Summary => "SUMMARY".to_string(),
        }
    }
}

