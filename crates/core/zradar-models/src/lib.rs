//! # zradar-models
//!
//! Data models for zradar telemetry ingestion.
//!
//! This crate contains all shared types used across zradar components:
//! - `Span`: Distributed tracing spans with LLM-specific fields
//! - `Metric`: Time-series metrics
//! - `Config`: Configuration loading from TOML/env
//! - `RequestContext`: Authentication context

pub mod config;
mod context;
mod evaluation_score;
pub mod file_list;
mod log_record;
mod metric;
mod span;

pub use config::Config;
pub use context::RequestContext;
pub use evaluation_score::{EvalDataType, EvalSource, EvaluationScore};
pub use file_list::{
    FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
};
pub use log_record::LogRecord;
pub use metric::{Metric, MetricType};
pub use span::Span;

// Re-export commonly used config types for convenience
pub use config::{ApiKeyConfig, AuthConfig, ClickHouseConfig};
