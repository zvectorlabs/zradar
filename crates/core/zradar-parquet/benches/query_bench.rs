//! Query benchmarks for the read path (Phase 5 §6.2).
//!
//! Measures `ParquetTelemetryReader::query_traces` latency at three dataset
//! sizes the spec calls out: 10k / 100k / 1M spans. We hold the
//! rows-per-file at 1_000 and scale the file count linearly, since file count
//! is what makes file_list pruning and ListingTable planning expensive.
//!
//! Run locally:
//!
//! ```sh
//! cargo bench -p zradar-parquet --bench query_bench
//! cargo bench -p zradar-parquet --bench query_bench -- --save-baseline release-1.0
//! ```
//!
//! 1M-span (1000-file) run takes the longest on this box but is the most
//! useful number for capacity planning. If you only need quick smoke
//! measurements, use `--quick`.

use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use zradar_models::{
    FileListEntry, FileListFilter, NewFileListEntry, Span, StreamStats, StreamStatsUpdate,
};
use zradar_parquet::{
    ParquetFileReader, ParquetTelemetryReader, schema::spans::spans_to_record_batch,
};
use zradar_traits::{FileListRepository, Pagination, TelemetryReader, TraceQueryFilters};

const ROWS_PER_FILE: usize = 1_000;

/// In-memory file_list backing the bench. Returns the canned list verbatim;
/// other operations are no-ops since the bench never writes.
///
/// Note: `query_files` returns an owned `Vec<FileListEntry>` per the trait,
/// so we pay one clone per `query_traces` call. That's once per bench
/// iteration — at 100 entries (the 100k case) the clone is small compared to
/// the 100+ ms DataFusion scan it gates, so we accept it. If the trait ever
/// gains a `Cow` or borrowed-slice variant, switch to that here.
struct InMemoryFileList {
    entries: Vec<FileListEntry>,
}

#[async_trait::async_trait]
impl FileListRepository for InMemoryFileList {
    async fn register_file(&self, _: NewFileListEntry) -> anyhow::Result<i64> {
        Ok(0)
    }
    async fn query_files(&self, _: FileListFilter) -> anyhow::Result<Vec<FileListEntry>> {
        Ok(self.entries.clone())
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

/// Build `rows_per_file` synthetic spans for trace `trace_index`. Spans are
/// spread across timestamps so realistic time-range queries land in the
/// middle of the dataset.
fn synth_spans(file_index: usize, rows_per_file: usize) -> Vec<Span> {
    let mut spans = Vec::with_capacity(rows_per_file);
    let base_ts = 1_700_000_000_000_000_000i64 + (file_index as i64 * 1_000_000_000);
    for i in 0..rows_per_file {
        // Each span belongs to a trace identified by file_index + i — every
        // row is its own trace so the GROUP BY in query_traces sees the full
        // working set.
        let trace = format!("t{:08x}{:024x}", file_index, i);
        let span_id = format!("s{:015x}", i);
        spans.push(Span {
            trace_id: trace,
            span_id,
            parent_span_id: String::new(),
            timestamp: base_ts + (i as i64 * 1_000),
            duration_ns: 1_000_000,
            workspace_id: uuid::Uuid::nil().to_string(),
            service_name: "bench-svc".to_string(),
            span_name: "bench.op".to_string(),
            span_type: "LLM".to_string(),
            span_kind: "INTERNAL".to_string(),
            status_code: "OK".to_string(),
            ..Default::default()
        });
    }
    spans
}

/// Write a Parquet file from a span batch and return its file_list entry.
fn write_parquet_file(dir: &std::path::Path, file_index: usize, spans: &[Span]) -> FileListEntry {
    let path = dir.join(format!("file_{file_index:06}.parquet"));
    let batch: RecordBatch = spans_to_record_batch(spans).unwrap();
    let schema = batch.schema();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let min_ts = spans.iter().map(|s| s.timestamp).min().unwrap_or(0);
    let max_ts = spans.iter().map(|s| s.timestamp).max().unwrap_or(0);

    FileListEntry {
        id: file_index as i64,
        workspace_id: uuid::Uuid::nil().into(),
        signal_type: "traces".to_string(),
        stream_name: "default".to_string(),
        date: "2026/06/26/00".to_string(),
        file_path: path.to_string_lossy().into_owned(),
        location: "local".to_string(),
        min_ts,
        max_ts,
        records: spans.len() as i64,
        original_size: 0,
        compressed_size: 0,
        deleted: false,
        created_at: 0,
        updated_at: 0,
    }
}

/// Seed `file_count` Parquet files (each ROWS_PER_FILE rows) into a temp dir
/// and return a `(reader, _tempdir)` ready for benching.
fn seed(file_count: usize) -> (ParquetTelemetryReader, TempDir) {
    let tmp = TempDir::new().unwrap();
    let mut entries = Vec::with_capacity(file_count);
    for i in 0..file_count {
        let spans = synth_spans(i, ROWS_PER_FILE);
        entries.push(write_parquet_file(tmp.path(), i, &spans));
    }
    let repo: Arc<dyn FileListRepository> = Arc::new(InMemoryFileList { entries });
    let file_reader = Arc::new(ParquetFileReader::new(PathBuf::from(tmp.path()), repo));
    let telemetry_reader = ParquetTelemetryReader::new(file_reader);
    (telemetry_reader, tmp)
}

fn bench_query_traces(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query");

    // Per spec: 10k / 100k / 1M. 1M (1000 files * 1000 rows) is slow to seed
    // and slow to scan — opt in via env so a default `cargo bench` run stays
    // fast. Set `Z_QUERY_BENCH_INCLUDE_1M=1` to enable.
    let include_1m = std::env::var("Z_QUERY_BENCH_INCLUDE_1M").is_ok();
    let sizes: &[(&str, usize)] = if include_1m {
        &[("10k", 10), ("100k", 100), ("1M", 1_000)]
    } else {
        &[("10k", 10), ("100k", 100)]
    };

    for (label, file_count) in sizes {
        let (reader, _tmp) = seed(*file_count);
        let row_count = (*file_count * ROWS_PER_FILE) as u64;
        group.throughput(Throughput::Elements(row_count));

        group.bench_with_input(
            BenchmarkId::new("query_traces", label),
            file_count,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let filters = TraceQueryFilters {
                        workspace_id: Some(uuid::Uuid::nil()),
                        pagination: Pagination {
                            limit: Some(50),
                            offset: Some(0),
                        },
                        ..Default::default()
                    };
                    let resp = reader.query_traces(filters).await.unwrap();
                    black_box(resp);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_query_traces);
criterion_main!(benches);
