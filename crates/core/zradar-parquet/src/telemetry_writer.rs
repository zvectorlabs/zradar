//! `TelemetryWriter` implementation backed by Parquet files.
//!
//! `ParquetTelemetryWriter` supports two write modes:
//!
//! * **Direct** (default) — each `insert_*` call immediately writes a Parquet
//!   file via `ParquetFileWriter`.
//! * **Buffered** (M07-04) — `insert_*` calls accumulate records in a
//!   `WriteBuffer`; a background `FlushWorker` batches them into fewer files.
//!   Enable by passing a `WriteBuffer` to `ParquetTelemetryWriter::with_buffer`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use zradar_models::{EvaluationScore, LogRecord, Metric, Span};
use zradar_traits::TelemetryWriter;

use crate::write_buffer::{BufferKey, WriteBuffer};
use crate::writer::{ParquetFileWriter, ts_ns_to_date_path};

/// Implements `TelemetryWriter` backed by `ParquetFileWriter`.
///
/// When a `WriteBuffer` is present, data is buffered and flushed in batches
/// by the `FlushWorker`.  Without a buffer, each call writes directly.
pub struct ParquetTelemetryWriter {
    writer: Arc<ParquetFileWriter>,
    buffer: Option<Arc<WriteBuffer>>,
}

impl ParquetTelemetryWriter {
    /// Create a writer using the direct (unbuffered) write path.
    pub fn new(writer: Arc<ParquetFileWriter>) -> Self {
        Self {
            writer,
            buffer: None,
        }
    }

    /// Create a writer that accumulates records in `buffer` before writing.
    ///
    /// The caller must also spawn a `FlushWorker` that drains the same buffer.
    pub fn with_buffer(writer: Arc<ParquetFileWriter>, buffer: Arc<WriteBuffer>) -> Self {
        Self {
            writer,
            buffer: Some(buffer),
        }
    }
}

#[async_trait]
impl TelemetryWriter for ParquetTelemetryWriter {
    async fn insert_spans(&self, spans: &[Span]) -> anyhow::Result<()> {
        if spans.is_empty() {
            return Ok(());
        }
        match partition_by_storage_key(
            spans,
            |s| s.workspace_id.as_str(),
            |s| s.service_name.as_str(),
            |s| s.timestamp,
        ) {
            Partitioned::Homogeneous(key, slice) => self.write_span_partition(&key, slice).await,
            Partitioned::Grouped(groups) => {
                for (key, group) in &groups {
                    self.write_span_partition(key, group).await?;
                }
                Ok(())
            }
        }
    }

    async fn insert_metrics(&self, metrics: &[Metric]) -> anyhow::Result<()> {
        if metrics.is_empty() {
            return Ok(());
        }
        match partition_by_storage_key(
            metrics,
            |m| m.workspace_id.as_str(),
            |m| m.service_name.as_str(),
            |m| m.timestamp,
        ) {
            Partitioned::Homogeneous(key, slice) => self.write_metric_partition(&key, slice).await,
            Partitioned::Grouped(groups) => {
                for (key, group) in &groups {
                    self.write_metric_partition(key, group).await?;
                }
                Ok(())
            }
        }
    }

    async fn insert_logs(&self, logs: &[LogRecord]) -> anyhow::Result<()> {
        if logs.is_empty() {
            return Ok(());
        }
        match partition_by_storage_key(
            logs,
            |l| l.workspace_id.as_str(),
            |l| l.service_name.as_str(),
            |l| l.timestamp,
        ) {
            Partitioned::Homogeneous(key, slice) => self.write_log_partition(&key, slice).await,
            Partitioned::Grouped(groups) => {
                for (key, group) in &groups {
                    self.write_log_partition(key, group).await?;
                }
                Ok(())
            }
        }
    }

    async fn insert_scores(&self, scores: &[EvaluationScore]) -> anyhow::Result<()> {
        if scores.is_empty() {
            return Ok(());
        }
        // Scores have no service_name of their own; they are binned under the
        // parent trace's service so per-stream queries co-locate scores with the
        // traces they evaluate.
        match partition_by_storage_key(
            scores,
            |s| s.workspace_id.as_str(),
            |s| s.service_name.as_str(),
            |s| s.timestamp,
        ) {
            Partitioned::Homogeneous(key, slice) => self.write_score_partition(&key, slice).await,
            Partitioned::Grouped(groups) => {
                for (key, group) in &groups {
                    self.write_score_partition(key, group).await?;
                }
                Ok(())
            }
        }
    }
}

impl ParquetTelemetryWriter {
    async fn write_span_partition(&self, key: &PartitionKey, spans: &[Span]) -> anyhow::Result<()> {
        if let Some(buf) = &self.buffer {
            buf.push_spans(key.buffer_key("traces"), spans);
        } else {
            self.writer
                .write_spans(&key.workspace_id, &key.stream_name, spans)
                .await?;
        }
        Ok(())
    }

    async fn write_metric_partition(
        &self,
        key: &PartitionKey,
        metrics: &[Metric],
    ) -> anyhow::Result<()> {
        if let Some(buf) = &self.buffer {
            buf.push_metrics(key.buffer_key("metrics"), metrics);
        } else {
            self.writer
                .write_metrics(&key.workspace_id, &key.stream_name, metrics)
                .await?;
        }
        Ok(())
    }

    async fn write_log_partition(
        &self,
        key: &PartitionKey,
        logs: &[LogRecord],
    ) -> anyhow::Result<()> {
        if let Some(buf) = &self.buffer {
            buf.push_logs(key.buffer_key("logs"), logs);
        } else {
            self.writer
                .write_logs(&key.workspace_id, &key.stream_name, logs)
                .await?;
        }
        Ok(())
    }

    async fn write_score_partition(
        &self,
        key: &PartitionKey,
        scores: &[EvaluationScore],
    ) -> anyhow::Result<()> {
        if let Some(buf) = &self.buffer {
            buf.push_scores(key.buffer_key("scores"), scores);
        } else {
            self.writer
                .write_scores(&key.workspace_id, &key.stream_name, scores)
                .await?;
        }
        Ok(())
    }
}

/// Nanoseconds in one UTC hour — the granularity Parquet files are partitioned
/// at. Two timestamps share an hour partition iff they share this bucket.
const NANOS_PER_HOUR: i64 = 3_600_000_000_000;

/// The storage partition a single telemetry row belongs to: `(tenant, project,
/// stream, hour)` (the signal type is fixed per `insert_*` call).
///
/// A batch handed to an `insert_*` method can span many partitions — most
/// notably, the WAL flusher merges records from many tenants into one slice —
/// so each row must be keyed from its own fields, never from `rows[0]`. Keying
/// the whole batch off the first row caused a cross-tenant leak plus hour/stream
/// mis-binning.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    workspace_id: String,
    stream_name: String,
    hour: String,
}

impl PartitionKey {
    fn of(workspace_id: &str, service_name: &str, ts_ns: i64) -> Self {
        Self {
            workspace_id: workspace_id.to_string(),
            stream_name: stream_of(service_name).to_string(),
            hour: ts_ns_to_date_path(ts_ns),
        }
    }

    fn buffer_key(&self, signal_type: &str) -> BufferKey {
        BufferKey {
            workspace_id: self.workspace_id.clone(),
            signal_type: signal_type.to_string(),
            stream_name: self.stream_name.clone(),
            hour: self.hour.clone(),
        }
    }
}

/// Outcome of partitioning a batch by its storage key.
enum Partitioned<'a, T> {
    /// Every row maps to the same partition — the common case. Carries the
    /// original slice, so the hot path does no per-row grouping or cloning.
    Homogeneous(PartitionKey, &'a [T]),
    /// Rows span multiple partitions; split into per-partition owned groups in
    /// first-seen order, preserving arrival order within each group.
    Grouped(Vec<(PartitionKey, Vec<T>)>),
}

/// Partition `rows` (which must be non-empty) by `(tenant, project, stream,
/// hour)`.
///
/// Homogeneity is checked with an allocation-free discriminator
/// `(&tenant, &project, &stream, hour_bucket)` so the common single-partition
/// batch never allocates; only a genuinely mixed batch pays for grouping. The
/// `hour_bucket` integer is a faithful stand-in for the formatted hour string:
/// UTC hours are exactly `NANOS_PER_HOUR` wide and epoch-aligned, so equal
/// buckets ⇔ equal `"%Y/%m/%d/%H"`.
fn partition_by_storage_key<T: Clone>(
    rows: &[T],
    workspace_of: impl Fn(&T) -> &str,
    service_of: impl Fn(&T) -> &str,
    ts_of: impl Fn(&T) -> i64,
) -> Partitioned<'_, T> {
    let first = &rows[0];
    let d0 = (
        workspace_of(first),
        stream_of(service_of(first)),
        ts_of(first).div_euclid(NANOS_PER_HOUR),
    );
    let homogeneous = rows.iter().all(|r| {
        (
            workspace_of(r),
            stream_of(service_of(r)),
            ts_of(r).div_euclid(NANOS_PER_HOUR),
        ) == d0
    });
    if homogeneous {
        let key = PartitionKey::of(workspace_of(first), service_of(first), ts_of(first));
        return Partitioned::Homogeneous(key, rows);
    }

    let mut index: HashMap<PartitionKey, usize> = HashMap::new();
    let mut groups: Vec<(PartitionKey, Vec<T>)> = Vec::new();
    for r in rows {
        let key = PartitionKey::of(workspace_of(r), service_of(r), ts_of(r));
        let idx = *index.entry(key.clone()).or_insert_with(|| {
            groups.push((key.clone(), Vec::new()));
            groups.len() - 1
        });
        groups[idx].1.push(r.clone());
    }
    Partitioned::Grouped(groups)
}

/// Map an OTLP `service.name` to its stream name, defaulting empty to "default".
fn stream_of(service_name: &str) -> &str {
    if service_name.is_empty() {
        "default"
    } else {
        service_name
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use zradar_models::WorkspaceId;

    use super::*;
    use std::sync::Mutex;
    use uuid::Uuid;
    use zradar_models::{
        FileListEntry, FileListFilter, NewFileListEntry, StreamStats, StreamStatsUpdate,
    };

    // ---------------------------------------------------------------------------
    // Stub repo that captures the most recent registered entry
    // ---------------------------------------------------------------------------

    #[derive(Default)]
    struct CapturingRepo {
        last_entry: Mutex<Option<NewFileListEntry>>,
    }

    #[async_trait::async_trait]
    impl zradar_traits::FileListRepository for CapturingRepo {
        async fn register_file(&self, entry: NewFileListEntry) -> anyhow::Result<i64> {
            *self.last_entry.lock().unwrap() = Some(entry);
            Ok(1)
        }

        async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(vec![])
        }

        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }

        async fn get_stream_stats(
            &self,
            _: zradar_models::WorkspaceId,
        ) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }

        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn make_writer_with_repo(
        dir: &std::path::Path,
        repo: Arc<dyn zradar_traits::FileListRepository>,
    ) -> ParquetTelemetryWriter {
        let fw = Arc::new(ParquetFileWriter::new(dir.to_path_buf(), repo));
        ParquetTelemetryWriter::new(fw)
    }

    // ---------------------------------------------------------------------------
    // Unit tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn test_insert_spans_empty_is_noop() {
        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo,
        ));
        let writer = ParquetTelemetryWriter::new(fw);
        // Must not error on empty slice.
        writer.insert_spans(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_metrics_always_succeeds() {
        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo,
        ));
        let writer = ParquetTelemetryWriter::new(fw);
        writer.insert_metrics(&[]).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_name_derived_from_service_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let writer = make_writer_with_repo(dir.path(), repo.clone());

        let span = Span {
            service_name: "my-agent".to_string(),
            workspace_id: WorkspaceId::new().to_string(),
            ..Span::default()
        };

        writer.insert_spans(&[span]).await.unwrap();

        let entry = repo.last_entry.lock().unwrap();
        assert_eq!(entry.as_ref().unwrap().stream_name, "my-agent");
    }

    #[tokio::test]
    async fn test_stream_name_defaults_when_service_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(CapturingRepo::default());
        let writer = make_writer_with_repo(dir.path(), repo.clone());

        // service_name is empty string (the Span default)
        let span = Span {
            workspace_id: WorkspaceId::new().to_string(),
            ..Span::default()
        };

        writer.insert_spans(&[span]).await.unwrap();

        let entry = repo.last_entry.lock().unwrap();
        assert_eq!(entry.as_ref().unwrap().stream_name, "default");
    }

    // ---- M07-04: buffered path ----

    #[tokio::test]
    async fn test_buffered_insert_spans_goes_to_buffer_not_file() {
        use crate::write_buffer::WriteBuffer;

        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo.clone(),
        ));
        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let writer = ParquetTelemetryWriter::with_buffer(fw, buffer.clone());

        let span = Span {
            service_name: "my-svc".to_string(),
            workspace_id: WorkspaceId::new().to_string(),
            timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            ..Span::default()
        };

        writer.insert_spans(&[span]).await.unwrap();

        // Data must be in the buffer, NOT yet written to Parquet.
        assert_eq!(buffer.len(), 1, "one slot should be in the buffer");
        assert!(
            repo.last_entry.lock().unwrap().is_none(),
            "no Parquet file should be written yet"
        );
    }

    #[tokio::test]
    async fn test_stream_name_helper_empty() {
        assert_eq!(stream_of(""), "default");
    }

    #[tokio::test]
    async fn test_stream_name_helper_non_empty() {
        assert_eq!(stream_of("my-svc"), "my-svc");
    }

    // ---------------------------------------------------------------------------
    // Per-row partitioning — regression for the cross-tenant leak / hour & stream
    // mis-binning where a mixed batch was keyed entirely off `rows[0]`.
    // ---------------------------------------------------------------------------

    /// Repo that records *every* registered entry (not just the last) so the
    /// direct write path can be asserted per partition.
    #[derive(Default)]
    struct MultiCapturingRepo {
        entries: Mutex<Vec<NewFileListEntry>>,
    }

    #[async_trait::async_trait]
    impl zradar_traits::FileListRepository for MultiCapturingRepo {
        async fn register_file(&self, entry: NewFileListEntry) -> anyhow::Result<i64> {
            let mut e = self.entries.lock().unwrap();
            e.push(entry);
            Ok(e.len() as i64)
        }
        async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
            Ok(vec![])
        }
        async fn update_location(&self, _: i64, _: &str, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn mark_deleted(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_entries(&self, _: &[i64]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn get_stream_stats(
            &self,
            _: zradar_models::WorkspaceId,
        ) -> anyhow::Result<Vec<StreamStats>> {
            Ok(vec![])
        }
        async fn upsert_stream_stats(&self, _: StreamStatsUpdate) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn buffered_writer(buffer: Arc<WriteBuffer>) -> ParquetTelemetryWriter {
        let repo = Arc::new(CapturingRepo::default());
        let fw = Arc::new(ParquetFileWriter::new(
            std::path::PathBuf::from("/tmp"),
            repo,
        ));
        ParquetTelemetryWriter::with_buffer(fw, buffer)
    }

    fn span_at(workspace: &str, svc: &str, ts: i64) -> Span {
        Span {
            workspace_id: workspace.to_string(),
            service_name: svc.to_string(),
            timestamp: ts,
            ..Span::default()
        }
    }

    /// The bot's exact scenario: one slice mixing two tenants, two streams, and
    /// two hours must fan out into three partitions — tenant B never merged into
    /// tenant A, and tenant A's two hours kept apart.
    #[tokio::test]
    async fn test_insert_spans_mixed_tenants_and_hours_partitioned() {
        use crate::write_buffer::SignalBatch;

        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let writer = buffered_writer(buffer.clone());

        let wa = Uuid::new_v4().to_string();
        let wb = Uuid::new_v4().to_string();
        let h10 = 10 * NANOS_PER_HOUR;
        let h11 = 11 * NANOS_PER_HOUR;

        writer
            .insert_spans(&[
                span_at(&wa, "api", h10),
                span_at(&wb, "web", h10),
                span_at(&wa, "api", h11),
            ])
            .await
            .unwrap();

        let slots = buffer.drain_all();
        assert_eq!(slots.len(), 3, "three distinct partitions expected");

        let (b_key, b_slot) = slots
            .iter()
            .find(|(k, _)| k.workspace_id == wb)
            .expect("workspace B partition must exist");
        assert_eq!(b_key.stream_name, "web");
        match &b_slot.data {
            SignalBatch::Spans(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(
                    v[0].workspace_id, wb,
                    "no cross-workspace leak into workspace B's slot"
                );
            }
            _ => panic!("expected Spans batch"),
        }

        // Workspace A has two slots, same workspace/stream but different hour.
        let a_slots: Vec<_> = slots.iter().filter(|(k, _)| k.workspace_id == wa).collect();
        assert_eq!(a_slots.len(), 2, "workspace A split across two hours");
        let hours: std::collections::HashSet<_> =
            a_slots.iter().map(|(k, _)| k.hour.clone()).collect();
        assert_eq!(
            hours.len(),
            2,
            "two distinct hour partitions for workspace A"
        );
    }

    /// Scores were the originally flagged signal — verify they bin per tenant.
    #[tokio::test]
    async fn test_insert_scores_mixed_tenants_no_leak() {
        use crate::write_buffer::SignalBatch;

        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let writer = buffered_writer(buffer.clone());

        let wa = Uuid::new_v4().to_string();
        let wb = Uuid::new_v4().to_string();
        let score = |workspace: &str| EvaluationScore {
            workspace_id: workspace.to_string(),
            service_name: "api".to_string(),
            timestamp: 10 * NANOS_PER_HOUR,
            ..EvaluationScore::default()
        };

        writer
            .insert_scores(&[score(&wa), score(&wb)])
            .await
            .unwrap();

        let slots = buffer.drain_all();
        assert_eq!(slots.len(), 2, "one partition per tenant");
        for (k, slot) in &slots {
            match &slot.data {
                SignalBatch::Scores(v) => {
                    assert_eq!(v.len(), 1);
                    assert_eq!(
                        v[0].workspace_id, k.workspace_id,
                        "each score binned under its own workspace"
                    );
                }
                _ => panic!("expected Scores batch"),
            }
        }
        let workspaces: std::collections::HashSet<_> =
            slots.iter().map(|(k, _)| k.workspace_id.clone()).collect();
        assert!(workspaces.contains(&wa) && workspaces.contains(&wb));
    }

    /// Fast path: a homogeneous batch stays a single partition with all rows.
    #[tokio::test]
    async fn test_insert_spans_homogeneous_single_partition() {
        use crate::write_buffer::SignalBatch;

        let buffer = Arc::new(WriteBuffer::new(8 * 1024 * 1024));
        let writer = buffered_writer(buffer.clone());

        let w = Uuid::new_v4().to_string();
        let h10 = 10 * NANOS_PER_HOUR;

        // Same workspace/stream, both within the same hour.
        writer
            .insert_spans(&[span_at(&w, "api", h10), span_at(&w, "api", h10 + 5)])
            .await
            .unwrap();

        let slots = buffer.drain_all();
        assert_eq!(slots.len(), 1, "one partition for a homogeneous batch");
        match &slots[0].1.data {
            SignalBatch::Spans(v) => assert_eq!(v.len(), 2),
            _ => panic!("expected Spans batch"),
        }
    }

    /// Direct (unbuffered) path: a mixed batch must register one Parquet file per
    /// partition, each carrying only its own tenant's rows.
    #[tokio::test]
    async fn test_direct_insert_spans_mixed_tenants_registers_per_partition() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = Arc::new(MultiCapturingRepo::default());
        let writer = {
            let fw = Arc::new(ParquetFileWriter::new(
                dir.path().to_path_buf(),
                repo.clone(),
            ));
            ParquetTelemetryWriter::new(fw)
        };

        let wa = Uuid::new_v4();
        let wb = Uuid::new_v4();
        let h10 = 10 * NANOS_PER_HOUR;

        writer
            .insert_spans(&[
                span_at(&wa.to_string(), "api", h10),
                span_at(&wb.to_string(), "web", h10),
                span_at(&wa.to_string(), "api", h10),
            ])
            .await
            .unwrap();

        let entries = repo.entries.lock().unwrap();
        assert_eq!(entries.len(), 2, "one file per partition");

        let b = entries
            .iter()
            .find(|e| e.workspace_id == wb.into())
            .expect("workspace B file must exist");
        assert_eq!(b.records, 1, "workspace B file holds only B's span");
        assert_eq!(b.stream_name, "web");

        let a = entries
            .iter()
            .find(|e| e.workspace_id == wa.into())
            .expect("workspace A file must exist");
        assert_eq!(
            a.records, 2,
            "workspace A's two same-partition spans coalesced"
        );
        assert_eq!(a.stream_name, "api");
    }
}
