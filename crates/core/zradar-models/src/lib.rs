//! # zradar-models
//! 
//! Data models for zradar telemetry ingestion.
//! 
//! This crate contains all shared types used across zradar components:
//! - `Span`: Distributed tracing spans with LLM-specific fields
//! - `Metric`: Time-series metrics
//! - `Config`: Configuration loading from TOML/env
//! - `RequestContext`: Authentication context

mod span;
mod metric;
pub mod config;
mod context;
mod evaluation_score;

pub use span::Span;
pub use metric::{Metric, MetricType};
pub use config::Config;
pub use context::RequestContext;
pub use evaluation_score::{EvaluationScore, EvalDataType, EvalSource};

// Re-export commonly used config types for convenience
pub use config::{ClickHouseConfig, AuthConfig, ApiKeyConfig};

