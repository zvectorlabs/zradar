//! [`MetricsHub`] — the single entry point the pipeline records through.
//!
//! System metrics are always recorded. Per-workspace metrics are recorded only
//! when [`MetricsPolicy::observed`] returns `true` for that workspace, so the
//! default cardinality is just the system set; turning on a few "key" tenants
//! through policy adds only their series.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use zradar_models::WorkspaceId;

use crate::ingest::{BreakerCause, IngestMetrics, RejectReason, Signal, WorkspaceMetrics};
use crate::primitive::PromEncoder;

/// Decides which workspaces get per-workspace metric series.
///
/// Back this with the workspace settings (e.g. a `metrics_observed` flag read
/// through the cached settings repository) so only flagged tenants are tracked.
pub trait MetricsPolicy: Send + Sync {
    fn observed(&self, workspace: WorkspaceId) -> bool;
}

/// Default policy: system metrics only, no per-workspace series.
#[derive(Debug, Default)]
pub struct ObserveNone;
impl MetricsPolicy for ObserveNone {
    fn observed(&self, _: WorkspaceId) -> bool {
        false
    }
}

/// Observe every workspace — intended for tests/small single-tenant setups.
#[derive(Debug, Default)]
pub struct ObserveAll;
impl MetricsPolicy for ObserveAll {
    fn observed(&self, _: WorkspaceId) -> bool {
        true
    }
}

/// Central metrics registry: always-on system metrics plus a policy-gated map of
/// per-workspace metrics.
pub struct MetricsHub {
    system: IngestMetrics,
    workspaces: DashMap<WorkspaceId, Arc<WorkspaceMetrics>>,
    policy: Arc<dyn MetricsPolicy>,
}

impl std::fmt::Debug for MetricsHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsHub")
            .field("observed_workspaces", &self.workspaces.len())
            .finish_non_exhaustive()
    }
}

impl MetricsHub {
    #[must_use]
    pub fn new(policy: Arc<dyn MetricsPolicy>) -> Self {
        Self {
            system: IngestMetrics::default(),
            workspaces: DashMap::new(),
            policy,
        }
    }

    /// Direct access to the system metric group (for callers that record their
    /// own structured events).
    #[must_use]
    pub fn system(&self) -> &IngestMetrics {
        &self.system
    }

    /// Returns the per-workspace metrics handle iff policy observes `id`,
    /// creating it lazily on first observed use.
    fn ws(&self, id: WorkspaceId) -> Option<Arc<WorkspaceMetrics>> {
        if !self.policy.observed(id) {
            return None;
        }
        Some(
            self.workspaces
                .entry(id)
                .or_insert_with(|| Arc::new(WorkspaceMetrics::default()))
                .value()
                .clone(),
        )
    }

    /// Drop the cached series for a workspace that is no longer observed. A
    /// periodic sweep can call this so policy "un-observe" frees the series.
    pub fn forget_workspace(&self, id: WorkspaceId) {
        self.workspaces.remove(&id);
    }

    // ---- ack path ----

    pub fn request(&self, signal: Signal) {
        self.system.request(signal);
    }

    /// Record an accepted batch: rows + bytes, plus ack and conversion latency.
    pub fn accepted(
        &self,
        workspace: WorkspaceId,
        signal: Signal,
        rows: u64,
        bytes: u64,
        ack: Duration,
        convert: Duration,
    ) {
        self.system.accepted(signal, rows, bytes);
        self.system.observe_ack(signal, ack.as_secs_f64());
        self.system.observe_convert(signal, convert.as_secs_f64());
        if let Some(w) = self.ws(workspace) {
            w.accepted(signal, rows, bytes);
            w.observe_ack(signal, ack.as_secs_f64());
        }
    }

    pub fn rejected(&self, workspace: WorkspaceId, reason: RejectReason) {
        self.system.rejected(reason);
        if let Some(w) = self.ws(workspace) {
            w.rejected(reason);
        }
    }

    /// Record ack→queryable lag for a workspace's signal.
    pub fn visibility(&self, workspace: WorkspaceId, signal: Signal, lag: Duration) {
        self.system.observe_visibility(signal, lag.as_secs_f64());
        if let Some(w) = self.ws(workspace) {
            w.observe_visibility(signal, lag.as_secs_f64());
        }
    }

    // ---- write / storage (system-wide) ----

    pub fn flushed(&self, signal: Signal, rows: u64) {
        self.system.flushed(signal, rows);
    }
    pub fn parquet_written(&self, signal: Signal, bytes: u64) {
        self.system.parquet_written(signal, bytes);
    }
    pub fn set_buffer(&self, rows: u64, bytes: u64) {
        self.system.set_buffer(rows, bytes);
    }

    // ---- saturation (system-wide) ----

    pub fn set_queue_depth(&self, n: u64) {
        self.system.set_queue_depth(n);
    }
    pub fn set_breaker(&self, open: bool) {
        self.system.set_breaker(open);
    }
    pub fn breaker_trip(&self, cause: BreakerCause) {
        self.system.breaker_trip(cause);
    }
    pub fn set_disk_ratio(&self, ratio: f64) {
        self.system.set_disk_ratio(ratio);
    }

    // ---- correctness (system-wide) ----

    pub fn convert_skipped(&self) {
        self.system.convert_skipped();
    }
    pub fn timestamp_anomaly(&self) {
        self.system.timestamp_anomaly();
    }
    pub fn dropped(&self, n: u64) {
        self.system.dropped(n);
    }

    /// Render the full registry as Prometheus exposition text.
    #[must_use]
    pub fn render(&self) -> String {
        let mut enc = PromEncoder::new();
        self.system.collect(&mut enc);
        for entry in self.workspaces.iter() {
            let ws = entry.key().to_string();
            entry.value().collect(&mut enc, &ws);
        }
        enc.into_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws_id() -> WorkspaceId {
        WorkspaceId::from(uuid::Uuid::nil())
    }

    #[test]
    fn system_metrics_always_recorded() {
        let hub = MetricsHub::new(Arc::new(ObserveNone));
        hub.accepted(
            ws_id(),
            Signal::Traces,
            3,
            1500,
            Duration::from_millis(2),
            Duration::from_micros(80),
        );
        hub.set_queue_depth(42);
        let out = hub.render();
        assert!(out.contains("zradar_ingest_rows_accepted_total{signal=\"traces\"} 3"));
        assert!(out.contains("zradar_saturation_queue_depth 42"));
        // ObserveNone → no per-workspace series and no cached entry.
        assert!(!out.contains("_ws_"));
        assert_eq!(hub.workspaces.len(), 0);
    }

    #[test]
    fn workspace_metrics_only_when_observed() {
        let hub = MetricsHub::new(Arc::new(ObserveAll));
        hub.accepted(
            ws_id(),
            Signal::Traces,
            5,
            10,
            Duration::from_millis(1),
            Duration::from_micros(50),
        );
        let out = hub.render();
        // System aggregate present...
        assert!(out.contains("zradar_ingest_rows_accepted_total{signal=\"traces\"} 5"));
        // ...and a per-workspace series labelled with the workspace id.
        assert!(out.contains(
            "zradar_ingest_ws_rows_accepted_total{workspace=\"00000000-0000-0000-0000-000000000000\",signal=\"traces\"} 5"
        ));
        assert_eq!(hub.workspaces.len(), 1);
    }

    #[test]
    fn rejections_carry_reason_label() {
        let hub = MetricsHub::new(Arc::new(ObserveNone));
        hub.rejected(ws_id(), RejectReason::RateLimit);
        hub.rejected(ws_id(), RejectReason::RateLimit);
        let out = hub.render();
        assert!(out.contains("zradar_ingest_rejections_total{reason=\"rate_limit\"} 2"));
    }

    #[test]
    fn forget_workspace_drops_series() {
        let hub = MetricsHub::new(Arc::new(ObserveAll));
        hub.rejected(ws_id(), RejectReason::Auth);
        assert_eq!(hub.workspaces.len(), 1);
        hub.forget_workspace(ws_id());
        assert_eq!(hub.workspaces.len(), 0);
        assert!(!hub.render().contains("_ws_"));
    }
}
