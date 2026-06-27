//! End-to-end tests for the batch-WAL contract: one envelope per
//! `(signal_type, tenant, project)` group, decoded on replay back into the
//! original row payloads.
//!
//! These tests exercise the on-disk shape rather than the [`WalTelemetryWriter`]
//! struct (which is defined inline in `zradar-runtime::builder` and not
//! reachable from outside that module), so they pin the contract that
//! `zradar-runtime` must keep producing.

use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use zradar_wal::Wal;
use zradar_wal::batch::{BatchEncoding, decode as decode_batch, encode_json_rows};
use zradar_wal::config::WalConfig;
use zradar_wal::record::{SignalType, WalRecord};
use zradar_wal::segment::{SegmentReader, list_segments};

fn open_wal_default(
    tmp: &TempDir,
    cancel: CancellationToken,
) -> impl std::future::Future<Output = Wal> + use<'_> {
    let cfg = WalConfig {
        segment_max_bytes: 10 * 1024 * 1024,
        group_commit_window_ms: 1,
        ..Default::default()
    };
    async move { Wal::open(tmp.path(), cfg, cancel).await.expect("open WAL") }
}

#[tokio::test]
async fn one_batch_append_carries_many_rows_in_one_record() {
    let tmp = TempDir::new().unwrap();
    let cancel = CancellationToken::new();
    let wal = open_wal_default(&tmp, cancel.clone()).await;

    // Three row payloads framed in one batch envelope.
    let rows: Vec<&[u8]> = vec![
        br#"{"trace_id":"a"}"#,
        br#"{"trace_id":"b"}"#,
        br#"{"trace_id":"c"}"#,
    ];
    let envelope = encode_json_rows(rows.iter().copied());

    let tenant = uuid::Uuid::new_v4();
    let project = uuid::Uuid::new_v4();
    let rec = WalRecord {
        signal_type: SignalType::Trace,
        tenant_id: tenant,
        project_id: project,
        arrival_timestamp_ns: 1_700_000_000_000_000_000,
        assigned_offset: 0,
        payload: envelope,
    };

    let h = wal.append(rec).await.unwrap();
    h.durable().await.unwrap();

    // Exactly one record was written.
    let mut reader = SegmentReader::open(tmp.path(), 0).unwrap();
    let stored = reader.next_record().unwrap().expect("one record");
    assert!(
        reader.next_record().unwrap().is_none(),
        "exactly one record"
    );

    // And it decodes back to the original three rows.
    let decoded = decode_batch(&stored.payload)
        .unwrap()
        .expect("batch envelope");
    assert_eq!(decoded.encoding, BatchEncoding::JsonRowsV1);
    assert_eq!(decoded.rows.len(), 3);
    assert_eq!(&decoded.rows[0][..], rows[0]);
    assert_eq!(&decoded.rows[1][..], rows[1]);
    assert_eq!(&decoded.rows[2][..], rows[2]);

    // Tenant/project/signal carried on the outer WalRecord (so flushed rows
    // can be attributed without re-parsing the JSON payloads).
    assert_eq!(stored.tenant_id, tenant);
    assert_eq!(stored.project_id, project);
    assert_eq!(stored.signal_type, SignalType::Trace);

    cancel.cancel();
}

#[tokio::test]
async fn batch_envelope_replays_correctly_through_segment_reader() {
    // Simulates a process restart: write batched records, drop the WAL,
    // re-open via SegmentReader, decode every record and confirm row count
    // matches what we wrote.
    let tmp = TempDir::new().unwrap();
    let cancel = CancellationToken::new();
    let wal = Arc::new(open_wal_default(&tmp, cancel.clone()).await);

    // 5 envelopes, each carrying 4 rows = 20 rows total over 5 records.
    let tenant = uuid::Uuid::new_v4();
    let project = uuid::Uuid::new_v4();
    let mut expected_rows: Vec<String> = Vec::new();

    for envelope_idx in 0..5 {
        let group: Vec<Vec<u8>> = (0..4)
            .map(|row_idx| {
                let body = format!(r#"{{"env":{envelope_idx},"row":{row_idx}}}"#);
                expected_rows.push(body.clone());
                body.into_bytes()
            })
            .collect();
        let envelope = encode_json_rows(group.iter().map(|v| v.as_slice()));
        let rec = WalRecord {
            signal_type: SignalType::Trace,
            tenant_id: tenant,
            project_id: project,
            arrival_timestamp_ns: envelope_idx,
            assigned_offset: 0,
            payload: envelope,
        };
        let h = wal.append(rec).await.unwrap();
        h.durable().await.unwrap();
    }

    cancel.cancel();
    drop(wal);

    // Reopen segment and replay every record.
    let mut reader = SegmentReader::open(tmp.path(), 0).unwrap();
    let mut decoded_rows: Vec<String> = Vec::new();
    while let Some(rec) = reader.next_record().unwrap() {
        let batch = decode_batch(&rec.payload).unwrap().expect("batch");
        for row in &batch.rows {
            decoded_rows.push(String::from_utf8(row.to_vec()).unwrap());
        }
    }

    assert_eq!(decoded_rows.len(), 20);
    assert_eq!(decoded_rows, expected_rows);
}

#[tokio::test]
async fn flush_sink_handles_legacy_single_row_and_batch_records_in_same_segment() {
    // Mixed-encoding replay: a segment can contain both pre-upgrade single-row
    // JSON payloads and post-upgrade batch envelopes. The flusher's decoder
    // must accept both.
    let tmp = TempDir::new().unwrap();
    let cancel = CancellationToken::new();
    let wal = Arc::new(open_wal_default(&tmp, cancel.clone()).await);

    let tenant = uuid::Uuid::new_v4();
    let project = uuid::Uuid::new_v4();

    // Two legacy single-row records (each one JSON document, no magic prefix).
    for legacy_idx in 0..2 {
        let legacy_payload = format!(r#"{{"legacy":true,"idx":{legacy_idx}}}"#);
        let rec = WalRecord {
            signal_type: SignalType::Trace,
            tenant_id: tenant,
            project_id: project,
            arrival_timestamp_ns: 0,
            assigned_offset: 0,
            payload: Bytes::from(legacy_payload),
        };
        wal.append(rec).await.unwrap().durable().await.unwrap();
    }

    // One batch record carrying three new rows.
    let new_rows: Vec<&[u8]> = vec![
        br#"{"batch":true,"idx":0}"#,
        br#"{"batch":true,"idx":1}"#,
        br#"{"batch":true,"idx":2}"#,
    ];
    let envelope = encode_json_rows(new_rows.iter().copied());
    let rec = WalRecord {
        signal_type: SignalType::Trace,
        tenant_id: tenant,
        project_id: project,
        arrival_timestamp_ns: 0,
        assigned_offset: 0,
        payload: envelope,
    };
    wal.append(rec).await.unwrap().durable().await.unwrap();

    cancel.cancel();
    drop(wal);

    // Walk all records, applying the same fallback logic the runtime flush
    // sink uses: try batch decode; if it returns Ok(None), treat as one
    // legacy row. We should see 5 total rows.
    let mut reader = SegmentReader::open(tmp.path(), 0).unwrap();
    let mut all_rows: Vec<Vec<u8>> = Vec::new();
    while let Some(rec) = reader.next_record().unwrap() {
        match decode_batch(&rec.payload).unwrap() {
            Some(batch) => {
                for row in &batch.rows {
                    all_rows.push(row.to_vec());
                }
            }
            None => {
                all_rows.push(rec.payload.to_vec());
            }
        }
    }

    assert_eq!(all_rows.len(), 5, "2 legacy rows + 3 batched rows");
    // Sanity check on content
    let strings: Vec<String> = all_rows
        .into_iter()
        .map(|b| String::from_utf8(b).unwrap())
        .collect();
    assert!(strings.iter().any(|s| s.contains(r#""legacy":true"#)));
    assert!(strings.iter().any(|s| s.contains(r#""batch":true"#)));
}

#[tokio::test]
async fn empty_batch_envelope_is_valid_and_decodes_to_zero_rows() {
    // The runtime writer skips empty input early, but defense in depth: if an
    // empty envelope is ever written, replay must not panic.
    let tmp = TempDir::new().unwrap();
    let cancel = CancellationToken::new();
    let wal = open_wal_default(&tmp, cancel.clone()).await;

    let empty_envelope = encode_json_rows::<_, &[u8]>(Vec::<&[u8]>::new());
    let rec = WalRecord {
        signal_type: SignalType::Score,
        tenant_id: uuid::Uuid::nil(),
        project_id: uuid::Uuid::nil(),
        arrival_timestamp_ns: 0,
        assigned_offset: 0,
        payload: empty_envelope,
    };
    wal.append(rec).await.unwrap().durable().await.unwrap();

    let mut reader = SegmentReader::open(tmp.path(), 0).unwrap();
    let stored = reader.next_record().unwrap().unwrap();
    let decoded = decode_batch(&stored.payload).unwrap().unwrap();
    assert!(decoded.rows.is_empty());

    cancel.cancel();
}

#[tokio::test]
async fn batched_appends_coalesce_into_few_fsyncs() {
    // Pin the headline benefit: many appends in a tight loop are coalesced by
    // the group-commit task. We append 50 single-row batches and assert that
    // the number of underlying fsyncs is well under 50.
    let tmp = TempDir::new().unwrap();
    let cancel = CancellationToken::new();
    let cfg = WalConfig {
        segment_max_bytes: 10 * 1024 * 1024,
        group_commit_window_ms: 5,
        ..Default::default()
    };
    let wal = Arc::new(Wal::open(tmp.path(), cfg, cancel.clone()).await.unwrap());

    let mut handles = Vec::new();
    for i in 0..50 {
        let envelope = encode_json_rows(std::iter::once(
            format!(r#"{{"i":{i}}}"#).into_bytes().as_slice(),
        ));
        let rec = WalRecord {
            signal_type: SignalType::Trace,
            tenant_id: uuid::Uuid::nil(),
            project_id: uuid::Uuid::nil(),
            arrival_timestamp_ns: 0,
            assigned_offset: 0,
            payload: envelope,
        };
        handles.push(wal.append(rec).await.unwrap());
    }
    for h in handles {
        h.durable().await.unwrap();
    }

    let fsyncs = wal.fsync_count();
    assert!(
        fsyncs <= 10,
        "expected <=10 fsyncs for 50 batched appends, got {fsyncs}"
    );

    cancel.cancel();
}

#[tokio::test]
async fn batch_envelope_groups_separate_tenants_into_separate_records() {
    // The runtime writer groups rows by (tenant, project) before batching. We
    // simulate that contract: two tenants → two records, each with its own
    // tenant_id on the outer WalRecord.
    let tmp = TempDir::new().unwrap();
    let cancel = CancellationToken::new();
    let wal = Arc::new(open_wal_default(&tmp, cancel.clone()).await);

    let tenant_a = uuid::Uuid::new_v4();
    let tenant_b = uuid::Uuid::new_v4();
    let project = uuid::Uuid::new_v4();

    let envelope_a = encode_json_rows(vec![
        br#"{"t":"a","i":0}"#.as_slice(),
        br#"{"t":"a","i":1}"#.as_slice(),
    ]);
    let envelope_b = encode_json_rows(std::iter::once(br#"{"t":"b","i":0}"#.as_slice()));

    wal.append(WalRecord {
        signal_type: SignalType::Trace,
        tenant_id: tenant_a,
        project_id: project,
        arrival_timestamp_ns: 0,
        assigned_offset: 0,
        payload: envelope_a,
    })
    .await
    .unwrap()
    .durable()
    .await
    .unwrap();

    wal.append(WalRecord {
        signal_type: SignalType::Trace,
        tenant_id: tenant_b,
        project_id: project,
        arrival_timestamp_ns: 0,
        assigned_offset: 0,
        payload: envelope_b,
    })
    .await
    .unwrap()
    .durable()
    .await
    .unwrap();

    cancel.cancel();
    drop(wal);

    let segments = list_segments(tmp.path()).unwrap();
    let mut reader = SegmentReader::open(tmp.path(), segments[0]).unwrap();

    let r0 = reader.next_record().unwrap().unwrap();
    assert_eq!(r0.tenant_id, tenant_a);
    assert_eq!(decode_batch(&r0.payload).unwrap().unwrap().rows.len(), 2);

    let r1 = reader.next_record().unwrap().unwrap();
    assert_eq!(r1.tenant_id, tenant_b);
    assert_eq!(decode_batch(&r1.payload).unwrap().unwrap().rows.len(), 1);

    assert!(reader.next_record().unwrap().is_none());
}
