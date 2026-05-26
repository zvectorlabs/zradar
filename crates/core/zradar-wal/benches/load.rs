/// WAL load benchmarks (M08-04 §2.5 scenarios A–D).
///
/// Scenario A: Sequential appends (single writer, measure throughput)
/// Scenario B: Concurrent appends (N writers, measure throughput + fsync coalescing)
/// Scenario C: Append under segment rotation (small segment_max_bytes)
/// Scenario D: Append + durable() round-trip latency
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

use bytes::Bytes;
use zradar_wal::Wal;
use zradar_wal::config::WalConfig;
use zradar_wal::record::{SignalType, WalRecord};

fn make_record(payload_size: usize) -> WalRecord {
    WalRecord {
        signal_type: SignalType::Trace,
        tenant_id: uuid::Uuid::new_v4(),
        project_id: uuid::Uuid::new_v4(),
        arrival_timestamp_ns: 1_700_000_000_000_000_000,
        assigned_offset: 0,
        payload: Bytes::from(vec![0xAA; payload_size]),
    }
}

fn bench_sequential_appends(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("wal_append_sequential_1kb", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let cancel = CancellationToken::new();
                let config = WalConfig {
                    segment_max_bytes: 256 * 1024 * 1024,
                    group_commit_window_ms: 1,
                    ..Default::default()
                };
                let wal = Wal::open(tmp.path(), config, cancel.clone()).await.unwrap();

                for _ in 0..1000 {
                    let h = wal.append(make_record(1024)).await.unwrap();
                    black_box(h);
                }
                cancel.cancel();
            });
        });
    });
}

fn bench_concurrent_appends(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("wal_append_concurrent_100_writers", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let cancel = CancellationToken::new();
                let config = WalConfig {
                    segment_max_bytes: 256 * 1024 * 1024,
                    group_commit_window_ms: 2,
                    ..Default::default()
                };
                let wal = Arc::new(Wal::open(tmp.path(), config, cancel.clone()).await.unwrap());

                let mut handles = Vec::new();
                for _ in 0..100 {
                    let w = wal.clone();
                    handles.push(tokio::spawn(async move {
                        for _ in 0..10 {
                            let h = w.append(make_record(512)).await.unwrap();
                            h.durable().await.unwrap();
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                cancel.cancel();
            });
        });
    });
}

fn bench_segment_rotation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("wal_append_with_rotation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let cancel = CancellationToken::new();
                let config = WalConfig {
                    segment_max_bytes: 4096, // force frequent rotation
                    group_commit_window_ms: 1,
                    ..Default::default()
                };
                let wal = Wal::open(tmp.path(), config, cancel.clone()).await.unwrap();

                for _ in 0..100 {
                    let h = wal.append(make_record(256)).await.unwrap();
                    black_box(h);
                }
                cancel.cancel();
            });
        });
    });
}

fn bench_durable_roundtrip(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("wal_append_durable_roundtrip", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tmp = tempfile::TempDir::new().unwrap();
                let cancel = CancellationToken::new();
                let config = WalConfig {
                    segment_max_bytes: 64 * 1024 * 1024,
                    group_commit_window_ms: 1,
                    ..Default::default()
                };
                let wal = Wal::open(tmp.path(), config, cancel.clone()).await.unwrap();

                for _ in 0..100 {
                    let h = wal.append(make_record(512)).await.unwrap();
                    h.durable().await.unwrap();
                }
                cancel.cancel();
            });
        });
    });
}

criterion_group!(
    benches,
    bench_sequential_appends,
    bench_concurrent_appends,
    bench_segment_rotation,
    bench_durable_roundtrip,
);
criterion_main!(benches);
