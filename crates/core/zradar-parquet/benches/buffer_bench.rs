//! Buffer-push benchmarks (re-architecture Phase B follow-up).
//!
//! Quantifies the per-row clone the move-based ingest path removes in buffered
//! mode. Compares:
//!   * `WriteBuffer::push_spans` — borrow path, `extend_from_slice` deep-clones
//!     every row's `String` fields into the slot.
//!   * `WriteBuffer::push_spans_owned` — move path, transfers the allocation
//!     into an empty slot or `Vec::append`s into a warm one (no deep clone).
//!
//! Measured in both slot states: `empty_slot` (first push of a hour/stream) and
//! `warm_slot` (accumulating into a slot that already holds rows). Each routine
//! returns the buffer so criterion drops it *outside* the timed region (drop of
//! the accumulated rows would otherwise dwarf the push being measured).
//!
//! ```sh
//! cargo bench -p zradar-parquet --bench buffer_bench
//! ```

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use zradar_models::Span;
use zradar_parquet::WriteBuffer;
use zradar_parquet::write_buffer::BufferKey;

const SIZES: &[usize] = &[1_000, 10_000];

fn key() -> BufferKey {
    BufferKey {
        workspace_id: uuid::Uuid::nil().to_string(),
        signal_type: "traces".to_string(),
        stream_name: "agent-orchestrator".to_string(),
        hour: "2026/06/28/00".to_string(),
    }
}

fn synth_spans(n: usize) -> Vec<Span> {
    let attributes =
        r#"{"gen_ai.request.model":"gpt-4o","http.route":"/v1/chat","user.tier":"pro"}"#;
    (0..n)
        .map(|i| Span {
            trace_id: format!("t{:024x}", i),
            span_id: format!("s{:015x}", i),
            workspace_id: uuid::Uuid::nil().to_string(),
            service_name: "agent-orchestrator".to_string(),
            span_name: "llm.chat.completion".to_string(),
            span_type: "GENERATION".to_string(),
            llm_model: "gpt-4o".to_string(),
            attributes: attributes.to_string(),
            timestamp: 1_700_000_000_000_000_000 + i as i64,
            ..Default::default()
        })
        .collect()
}

/// A fresh buffer whose single slot already holds `seed` cloned spans, used to
/// model a warm slot. `usize::MAX` byte cap so pushes never trigger a flush.
fn warm_buffer(seed: &[Span]) -> WriteBuffer {
    let buf = WriteBuffer::new(usize::MAX);
    buf.push_spans_owned(key(), seed.to_vec());
    buf
}

fn bench_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_push");
    for &n in SIZES {
        let spans = synth_spans(n);
        group.throughput(Throughput::Elements(n as u64));

        // --- Empty slot: first push into a fresh buffer. ---
        group.bench_with_input(
            BenchmarkId::new("clone_empty_slot", n),
            &spans,
            |b, spans| {
                b.iter_batched(
                    || WriteBuffer::new(usize::MAX),
                    |buf| {
                        buf.push_spans(key(), spans);
                        buf // returned → dropped outside the timed region
                    },
                    BatchSize::PerIteration,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("move_empty_slot", n),
            &spans,
            |b, spans| {
                b.iter_batched(
                    || (WriteBuffer::new(usize::MAX), spans.clone()),
                    |(buf, owned)| {
                        buf.push_spans_owned(key(), owned);
                        buf
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        // --- Warm slot: push into a slot already holding `n` rows. ---
        group.bench_with_input(
            BenchmarkId::new("clone_warm_slot", n),
            &spans,
            |b, spans| {
                b.iter_batched(
                    || warm_buffer(spans),
                    |buf| {
                        buf.push_spans(key(), spans);
                        buf
                    },
                    BatchSize::PerIteration,
                );
            },
        );
        group.bench_with_input(BenchmarkId::new("move_warm_slot", n), &spans, |b, spans| {
            b.iter_batched(
                || (warm_buffer(spans), spans.clone()),
                |(buf, owned)| {
                    buf.push_spans_owned(key(), owned);
                    buf
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_push);
criterion_main!(benches);
