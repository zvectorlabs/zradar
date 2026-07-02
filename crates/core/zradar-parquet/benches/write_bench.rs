//! Write-path benchmarks (re-architecture Phase A).
//!
//! Covers the two hot-path stages the read-side `query_bench` does not:
//!   1. `spans_to_record_batch` — the row→Arrow materialization (59 columns +
//!      JSON catch-all). This is the dominant per-batch CPU cost on the
//!      ingest flush and the headline target of re-arch §5.1 / Phase G.
//!   2. Full Parquet file write (build batch + `ArrowWriter`→file) — the
//!      end-to-end flush cost including encoding + compression.
//!
//! Run locally:
//!
//! ```sh
//! cargo bench -p zradar-parquet --bench write_bench
//! cargo bench -p zradar-parquet --bench write_bench -- --save-baseline pre-opt
//! ```

use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::arrow::ArrowWriter;
use std::hint::black_box;
use tempfile::TempDir;

use zradar_models::Span;
use zradar_parquet::schema::spans::spans_to_record_batch;

const SIZES: &[usize] = &[1_000, 10_000, 100_000];

/// Build `n` representative agent spans: realistic string fields plus a
/// non-trivial JSON `attributes` blob, so the row→Arrow cost reflects the real
/// promoted-column + catch-all shape rather than empty strings.
fn synth_spans(n: usize) -> Vec<Span> {
    let base_ts = 1_700_000_000_000_000_000i64;
    let attributes = r#"{"gen_ai.request.model":"gpt-4o","gen_ai.request.temperature":0.7,"http.route":"/v1/chat","session.turn":3,"user.tier":"pro"}"#;
    (0..n)
        .map(|i| Span {
            trace_id: format!("t{:024x}", i),
            span_id: format!("s{:015x}", i),
            parent_span_id: format!("p{:015x}", i / 2),
            timestamp: base_ts + (i as i64 * 1_000),
            duration_ns: 1_250_000,
            workspace_id: uuid::Uuid::nil().to_string(),
            service_name: "agent-orchestrator".to_string(),
            span_name: "llm.chat.completion".to_string(),
            span_type: "GENERATION".to_string(),
            span_kind: "CLIENT".to_string(),
            status_code: "OK".to_string(),
            llm_model: "gpt-4o".to_string(),
            llm_provider: "openai".to_string(),
            prompt_tokens: 1_200,
            completion_tokens: 350,
            total_tokens: 1_550,
            total_cost_usd: 0.0123,
            agent_name: "planner".to_string(),
            session_id: format!("sess-{}", i % 64),
            attributes: attributes.to_string(),
            ..Default::default()
        })
        .collect()
}

/// Stage 1: row structs → Arrow `RecordBatch`.
fn bench_spans_to_record_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write/spans_to_record_batch");
    for &n in SIZES {
        let spans = synth_spans(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &spans, |b, spans| {
            b.iter(|| {
                let batch: RecordBatch = spans_to_record_batch(black_box(spans)).unwrap();
                black_box(batch);
            });
        });
    }
    group.finish();
}

/// Stage 2: build batch once, then measure the full Parquet file encode+write.
fn bench_parquet_file_write(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let mut group = c.benchmark_group("write/parquet_file");
    for &n in SIZES {
        let spans = synth_spans(n);
        let batch = spans_to_record_batch(&spans).unwrap();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &batch, |b, batch| {
            b.iter(|| {
                let path = tmp.path().join("bench.parquet");
                let file = std::fs::File::create(&path).unwrap();
                let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
                writer.write(black_box(batch)).unwrap();
                writer.close().unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_spans_to_record_batch,
    bench_parquet_file_write
);
criterion_main!(benches);
