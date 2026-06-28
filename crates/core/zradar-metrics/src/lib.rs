//! # zradar-metrics
//!
//! End-to-end ingestion metrics for zradar, covering **correctness**
//! (conservation/drop/anomaly counters) and **performance** (ack latency,
//! throughput, visibility lag, write/storage, saturation).
//!
//! Design:
//! - **No heavy deps.** Metrics are plain atomics ([`Counter`], [`Gauge`],
//!   [`Histogram`]); the `/metrics` handler renders them via [`PromEncoder`].
//!   This matches the convention in `zradar_wal::metrics`.
//! - **System-level by default.** [`IngestMetrics`] is always recorded and has
//!   fixed, low cardinality (bounded labels: signal, reason, cause).
//! - **Per-workspace by policy.** [`WorkspaceMetrics`] is recorded only for
//!   workspaces a [`MetricsPolicy`] marks observed, so "key" tenants can be
//!   tracked individually without unbounded label cardinality.
//! - **Extensible.** Add a field to a metric group, init it in `new`/`Default`,
//!   emit it in `collect`, and add a `record_*` method on [`MetricsHub`].
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//! use zradar_metrics::{MetricsHub, ObserveNone, Signal};
//!
//! let hub = MetricsHub::new(Arc::new(ObserveNone));
//! hub.request(Signal::Traces);
//! // ...after a durable, accepted batch:
//! hub.accepted(
//!     Default::default(), // WorkspaceId
//!     Signal::Traces,
//!     /* rows */ 10,
//!     /* bytes */ 4096,
//!     /* ack */ Duration::from_millis(2),
//!     /* convert */ Duration::from_micros(90),
//! );
//! let prometheus_text = hub.render();
//! assert!(prometheus_text.contains("zradar_ingest_rows_accepted_total"));
//! ```

mod hub;
mod ingest;
mod primitive;

pub use hub::{MetricsHub, MetricsPolicy, ObserveAll, ObserveNone};
pub use ingest::{BreakerCause, IngestMetrics, RejectReason, Signal, WorkspaceMetrics};
pub use primitive::{Counter, Gauge, Histogram, LATENCY_SECONDS, PromEncoder, SIZE_BYTES};
