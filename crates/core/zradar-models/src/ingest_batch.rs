//! Versioned ingest-batch boundary (re-architecture Phase B).
//!
//! [`IngestBatch`] is the internal contract between the ingest/conversion
//! layer and the storage layer. Today every [`IngestPayload`] variant simply
//! owns the same row structs the row-oriented `TelemetryWriter::insert_*`
//! methods already take, so this is a **zero-behavior-change** shim. The
//! point is the *seam*: `IngestPayload` can later grow `Arrow(RecordBatch)` /
//! `OtlpBytes` / `OtapArrow` variants — and a storage backend can override
//! the writer trait's `insert_batch` to consume those directly — without
//! touching any service code or the existing row APIs.
//!
//! See
//! `zradar-plans/re-architecture/ARCHITECTURE-REVIEW-HIGH-PERFORMANCE-INGEST-QUERY.md`
//! §6.1 / Phase B.

use std::collections::HashMap;
use std::str::FromStr;

use crate::{EvaluationScore, LogRecord, Metric, Span, WorkspaceId};

/// Schema version stamped on every [`IngestBatch`].
///
/// Bump when the promoted-column set / domain-struct layout changes in a way
/// a stored-data reader must be able to distinguish. Carrying it on the batch
/// lets future block metadata (re-arch Phase E) record the schema a block was
/// written with, instead of inferring it positionally.
pub const INGEST_SCHEMA_VERSION: u16 = 1;

/// Telemetry signal kind carried by an [`IngestBatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignalType {
    Spans,
    Metrics,
    Logs,
    Scores,
}

impl SignalType {
    /// Stable storage tag, matching `file_list.signal_type` and the
    /// `WriteBuffer` keys. Note spans are stored under `"traces"`, not
    /// `"spans"`.
    pub fn as_str(self) -> &'static str {
        match self {
            SignalType::Spans => "traces",
            SignalType::Metrics => "metrics",
            SignalType::Logs => "logs",
            SignalType::Scores => "scores",
        }
    }
}

/// Columnar-ready payload. Each variant currently owns row structs; new
/// variants (Arrow / OTLP bytes / OTAP) can be added without changing the
/// boundary type or the writer trait.
#[derive(Debug, Clone)]
pub enum IngestPayload {
    Spans(Vec<Span>),
    Metrics(Vec<Metric>),
    Logs(Vec<LogRecord>),
    Scores(Vec<EvaluationScore>),
}

/// A versioned, self-describing unit of telemetry crossing the
/// ingest → storage boundary.
#[derive(Debug, Clone)]
pub struct IngestBatch {
    /// Layout version of the carried rows (see [`INGEST_SCHEMA_VERSION`]).
    pub schema_version: u16,
    /// Which signal the payload holds.
    pub signal_type: SignalType,
    /// `Some` when every row shares one workspace **and** that id parses as a
    /// [`WorkspaceId`]; `None` for an empty batch, a mixed-workspace batch, or
    /// an unparseable workspace string. Use [`IngestBatch::split_by_workspace`]
    /// to obtain homogeneous, single-workspace batches.
    pub workspace_id: Option<WorkspaceId>,
    /// Minimum row timestamp (Unix ns); `0` for an empty batch.
    pub min_timestamp: i64,
    /// Maximum row timestamp (Unix ns); `0` for an empty batch.
    pub max_timestamp: i64,
    /// Number of rows in the payload.
    pub record_count: usize,
    /// The carried rows.
    pub payload: IngestPayload,
}

impl IngestBatch {
    /// Wrap spans, deriving batch metadata (time bounds, count, homogeneous
    /// workspace) from the rows.
    pub fn spans(spans: Vec<Span>) -> Self {
        let (min_timestamp, max_timestamp) = time_bounds(&spans, |s| s.timestamp);
        let workspace_id = homogeneous_workspace(&spans, |s| s.workspace_id.as_str());
        Self {
            schema_version: INGEST_SCHEMA_VERSION,
            signal_type: SignalType::Spans,
            workspace_id,
            min_timestamp,
            max_timestamp,
            record_count: spans.len(),
            payload: IngestPayload::Spans(spans),
        }
    }

    /// Wrap metrics, deriving batch metadata from the rows.
    pub fn metrics(metrics: Vec<Metric>) -> Self {
        let (min_timestamp, max_timestamp) = time_bounds(&metrics, |m| m.timestamp);
        let workspace_id = homogeneous_workspace(&metrics, |m| m.workspace_id.as_str());
        Self {
            schema_version: INGEST_SCHEMA_VERSION,
            signal_type: SignalType::Metrics,
            workspace_id,
            min_timestamp,
            max_timestamp,
            record_count: metrics.len(),
            payload: IngestPayload::Metrics(metrics),
        }
    }

    /// Wrap logs, deriving batch metadata from the rows.
    pub fn logs(logs: Vec<LogRecord>) -> Self {
        let (min_timestamp, max_timestamp) = time_bounds(&logs, |l| l.timestamp);
        let workspace_id = homogeneous_workspace(&logs, |l| l.workspace_id.as_str());
        Self {
            schema_version: INGEST_SCHEMA_VERSION,
            signal_type: SignalType::Logs,
            workspace_id,
            min_timestamp,
            max_timestamp,
            record_count: logs.len(),
            payload: IngestPayload::Logs(logs),
        }
    }

    /// Wrap evaluation scores, deriving batch metadata from the rows.
    pub fn scores(scores: Vec<EvaluationScore>) -> Self {
        let (min_timestamp, max_timestamp) = time_bounds(&scores, |s| s.timestamp);
        let workspace_id = homogeneous_workspace(&scores, |s| s.workspace_id.as_str());
        Self {
            schema_version: INGEST_SCHEMA_VERSION,
            signal_type: SignalType::Scores,
            workspace_id,
            min_timestamp,
            max_timestamp,
            record_count: scores.len(),
            payload: IngestPayload::Scores(scores),
        }
    }

    /// Which signal the payload holds.
    pub fn signal_type(&self) -> SignalType {
        self.signal_type
    }

    /// Number of rows in the payload.
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// `true` when the payload carries no rows.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// `(min_timestamp, max_timestamp)` in Unix ns; `(0, 0)` when empty.
    pub fn time_range(&self) -> (i64, i64) {
        (self.min_timestamp, self.max_timestamp)
    }

    /// Split a possibly mixed-workspace batch into per-workspace batches, each
    /// homogeneous (so `workspace_id` is `Some` whenever the id parses).
    ///
    /// Mirrors the grouping `WalTelemetryWriter::append_batches` performs
    /// today, and is what per-block metadata (Phase E) will need. First-seen
    /// workspace order and per-workspace row order are both preserved. An
    /// empty batch yields an empty `Vec`.
    pub fn split_by_workspace(self) -> Vec<IngestBatch> {
        match self.payload {
            IngestPayload::Spans(rows) => group_by_workspace(rows, |s| s.workspace_id.as_str())
                .into_iter()
                .map(IngestBatch::spans)
                .collect(),
            IngestPayload::Metrics(rows) => group_by_workspace(rows, |m| m.workspace_id.as_str())
                .into_iter()
                .map(IngestBatch::metrics)
                .collect(),
            IngestPayload::Logs(rows) => group_by_workspace(rows, |l| l.workspace_id.as_str())
                .into_iter()
                .map(IngestBatch::logs)
                .collect(),
            IngestPayload::Scores(rows) => group_by_workspace(rows, |s| s.workspace_id.as_str())
                .into_iter()
                .map(IngestBatch::scores)
                .collect(),
        }
    }
}

impl From<Vec<Span>> for IngestBatch {
    fn from(spans: Vec<Span>) -> Self {
        Self::spans(spans)
    }
}

impl From<Vec<Metric>> for IngestBatch {
    fn from(metrics: Vec<Metric>) -> Self {
        Self::metrics(metrics)
    }
}

impl From<Vec<LogRecord>> for IngestBatch {
    fn from(logs: Vec<LogRecord>) -> Self {
        Self::logs(logs)
    }
}

impl From<Vec<EvaluationScore>> for IngestBatch {
    fn from(scores: Vec<EvaluationScore>) -> Self {
        Self::scores(scores)
    }
}

/// Min/max of a timestamp accessor over `rows`; `(0, 0)` when empty.
fn time_bounds<T>(rows: &[T], ts: impl Fn(&T) -> i64) -> (i64, i64) {
    let mut iter = rows.iter().map(ts);
    match iter.next() {
        None => (0, 0),
        Some(first) => iter.fold((first, first), |(lo, hi), t| (lo.min(t), hi.max(t))),
    }
}

/// Returns the single shared workspace as a [`WorkspaceId`] when every row
/// carries the same (parseable) workspace string, otherwise `None`.
fn homogeneous_workspace<T>(rows: &[T], ws: impl Fn(&T) -> &str) -> Option<WorkspaceId> {
    let first = ws(rows.first()?);
    if rows.iter().all(|r| ws(r) == first) {
        WorkspaceId::from_str(first).ok()
    } else {
        None
    }
}

/// Group rows by their workspace string, preserving first-seen group order and
/// per-group row order. Backs [`IngestBatch::split_by_workspace`].
fn group_by_workspace<T>(rows: Vec<T>, ws: impl Fn(&T) -> &str) -> Vec<Vec<T>> {
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<Vec<T>> = Vec::new();
    for row in rows {
        let key = ws(&row).to_string();
        let idx = *index.entry(key).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[idx].push(row);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    const WS_A: &str = "018f1a2b-0000-7000-8000-000000000001";
    const WS_B: &str = "018f1a2b-0000-7000-8000-000000000002";

    fn span(ws: &str, ts: i64) -> Span {
        Span {
            workspace_id: ws.to_string(),
            timestamp: ts,
            ..Default::default()
        }
    }

    fn metric(ws: &str, ts: i64) -> Metric {
        Metric {
            workspace_id: ws.to_string(),
            timestamp: ts,
            ..Default::default()
        }
    }

    fn log(ws: &str, ts: i64) -> LogRecord {
        LogRecord {
            workspace_id: ws.to_string(),
            timestamp: ts,
            ..Default::default()
        }
    }

    fn score(ws: &str, ts: i64) -> EvaluationScore {
        EvaluationScore {
            workspace_id: ws.to_string(),
            timestamp: ts,
            ..Default::default()
        }
    }

    #[test]
    fn empty_batch_has_zeroed_metadata() {
        let b = IngestBatch::spans(vec![]);
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
        assert_eq!(b.time_range(), (0, 0));
        assert!(b.workspace_id.is_none());
        assert_eq!(b.signal_type(), SignalType::Spans);
        assert_eq!(b.schema_version, INGEST_SCHEMA_VERSION);
    }

    #[test]
    fn derives_time_bounds_and_count() {
        // Rows out of timestamp order to prove min/max scan, not first/last.
        let b = IngestBatch::spans(vec![span(WS_A, 30), span(WS_A, 10), span(WS_A, 20)]);
        assert_eq!(b.len(), 3);
        assert_eq!(b.time_range(), (10, 30));
    }

    #[test]
    fn homogeneous_workspace_is_parsed() {
        let b = IngestBatch::metrics(vec![metric(WS_A, 1), metric(WS_A, 2)]);
        assert_eq!(b.workspace_id, Some(WorkspaceId::from_str(WS_A).unwrap()));
    }

    #[test]
    fn mixed_workspace_is_none() {
        let b = IngestBatch::spans(vec![span(WS_A, 1), span(WS_B, 2)]);
        assert!(b.workspace_id.is_none());
    }

    #[test]
    fn non_uuid_workspace_is_none_but_rows_kept() {
        let b = IngestBatch::logs(vec![log("not-a-uuid", 1)]);
        assert!(b.workspace_id.is_none());
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn split_groups_by_workspace_preserving_order() {
        let b = IngestBatch::spans(vec![span(WS_A, 1), span(WS_B, 2), span(WS_A, 3)]);
        let parts = b.split_by_workspace();
        assert_eq!(parts.len(), 2);
        // First-seen order: A then B.
        assert_eq!(
            parts[0].workspace_id,
            Some(WorkspaceId::from_str(WS_A).unwrap())
        );
        assert_eq!(parts[0].len(), 2);
        assert_eq!(parts[0].time_range(), (1, 3));
        assert_eq!(
            parts[1].workspace_id,
            Some(WorkspaceId::from_str(WS_B).unwrap())
        );
        assert_eq!(parts[1].len(), 1);
        // Every split is homogeneous.
        assert!(parts.iter().all(|p| p.workspace_id.is_some()));
    }

    #[test]
    fn split_single_workspace_returns_one() {
        let b = IngestBatch::scores(vec![score(WS_A, 1), score(WS_A, 2)]);
        let parts = b.split_by_workspace();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].len(), 2);
    }

    #[test]
    fn split_empty_returns_empty() {
        assert!(IngestBatch::metrics(vec![]).split_by_workspace().is_empty());
    }

    #[test]
    fn from_vec_constructs_typed_batch() {
        let b: IngestBatch = vec![metric(WS_A, 5)].into();
        assert_eq!(b.signal_type(), SignalType::Metrics);
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn signal_type_storage_tags() {
        assert_eq!(SignalType::Spans.as_str(), "traces");
        assert_eq!(SignalType::Metrics.as_str(), "metrics");
        assert_eq!(SignalType::Logs.as_str(), "logs");
        assert_eq!(SignalType::Scores.as_str(), "scores");
    }
}
