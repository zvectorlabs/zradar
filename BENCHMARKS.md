# zradar Benchmarks — Re-architecture Phase A baselines

Baselines for the high-performance ingest/query re-architecture
(`zradar-plans/re-architecture/ARCHITECTURE-REVIEW-HIGH-PERFORMANCE-INGEST-QUERY.md`,
Phase A: *measure before refactor*).

> **Numbers below were captured on a dev sandbox with `--quick` (criterion) and
> are indicative, not canonical.** Re-capture on a quiet reference host before
> using them to gate a refactor. The point of Phase A is the *harness* and the
> *shape* of the cost, both of which are stable across machines.

Environment: `rustc 1.96.0`, release profile, single box.

## Benches

| Bench | Crate | Covers | Status |
|-------|-------|--------|--------|
| `converter_bench` | `api-optel` | OTLP decode + convention mapping (incl. `detect_type`) | pre-existing |
| `query_bench` | `zradar-parquet` | `query_traces` read path at 10k/100k/1M | pre-existing |
| `load` | `zradar-wal` | WAL append / fsync coalescing / replay | pre-existing |
| **`write_bench`** | `zradar-parquet` | **row→Arrow `spans_to_record_batch` + full Parquet file write** | **added (Phase A)** |

Run:

```sh
just bench "-p zradar-parquet --bench write_bench"
just bench "-p api-optel --bench converter_bench"
just bench "-p zradar-parquet --bench query_bench"     # set Z_QUERY_BENCH_INCLUDE_1M=1 for the 1M case
just bench "-p zradar-wal --bench load"
# compare a change:
just bench "-p api-optel --bench converter_bench -- --save-baseline before"
#   …make change…
just bench "-p api-optel --bench converter_bench -- --baseline before"
```

## write_bench (new) — write hot path

| Batch | `spans_to_record_batch` | throughput | full Parquet file write | throughput |
|-------|------------------------:|-----------:|------------------------:|-----------:|
| 1,000   | ~316 µs  | ~3.17 M elem/s | ~1.9 ms  | ~0.52 M elem/s |
| 10,000  | ~8.5 ms  | ~1.18 M elem/s | ~10.9 ms | ~0.92 M elem/s |
| 100,000 | ~149 ms  | ~0.67 M elem/s | ~117 ms  | ~0.86 M elem/s |

**Headline finding (grounds re-arch §5.1 / Phase G):** `spans_to_record_batch`
throughput **falls** as the batch grows (3.17M → 0.67M elem/s). The row→Arrow
builder scans the span slice once per column (~59 columns), so per-element cost
rises with cache pressure at larger batches. This is the strongest single
argument for a single-pass / columnar-builder conversion (Phase G), and is now
measurable on every change.

## converter_bench — OTLP→Span (post opt: borrowed attribute map)

| Span shape | time |
|------------|-----:|
| `tiny_1_attr` | ~3.2 µs |
| `med_20_attr_nat_llm` | ~9.4 µs |
| `fat_100_attr_30_events` | ~71.8 µs |
| `wide_512_attr_200_events` | ~307 µs |

### Optimization landed: `detect_type` borrows instead of cloning

`converter.rs` previously cloned **every** attribute (key + value) into a fresh
`HashMap` per span just to call `SpanTypeMapper::detect_type`. `detect_type`
only reads the map (`.get` / `.contains_key` / `.keys`), so it now takes
`&serde_json::Map` and the converter passes the existing map by reference — the
per-span clone is gone. Behavior is unchanged (covered by the ~30 existing
`detect_type` unit tests).

Controlled before/after (full run, PRE = with clone, vs POST baseline):

| Span shape | PRE vs POST | significance |
|------------|------------:|--------------|
| `fat_100_attr_30_events` | PRE **+22%** slower | p < 0.05 |
| `wide_512_attr_200_events` | PRE **+26%** slower | p < 0.05 |
| `med_20_attr_nat_llm` | PRE ~+15% (central) | p = 0.10 (noisy) |
| `tiny_1_attr` | within noise | — |

The win scales with attribute count, as expected for an eliminated
O(attributes) clone.

### Optimization landed: eliminate the `serde_json::Map` entirely

Building on the above, `convert_span` no longer builds an intermediate
`serde_json::Map` at all. `detect_type` now reads the borrowed `AttrView`
(via an `AttrSource` trait), and the catch-all `attributes` column is produced
by a streaming `Serialize` impl (`attrs_to_json_filtered`) that serializes the
borrowed OTLP `KeyValue`s straight to the JSON string — no key `String` clones,
no `Value` tree. Content-capture scrubbing is a skip-predicate during
serialization (no parse→filter→reserialize). Output is byte-identical
(BTreeMap sort + last-wins, non-finite floats → `null`), pinned by a golden
equivalence test.

Controlled before/after (`converter_bench`, vs the borrowed-map baseline):

| Span shape | this change | significance |
|------------|------------:|--------------|
| `wide_512_attr_200_events` | **−36%** (307→198 µs) | p < 0.05 |
| `fat_100_attr_30_events` | **−20%** (72→56 µs) | p < 0.05 |
| `med_20_attr_nat_llm` | ~−25% (9.4→7.7 µs) | p = 0.07 (noisy) |
| `tiny_1_attr` | within noise | — |

Heap allocations per span (OQ15 `count-allocations` invariant test,
`nat_simple_workflow` shape): **44 → 27** (−39%). The OQ15 cap was retightened
from 55 to 35 to lock in the reduction.

## buffer_bench — move-based ingest path (Phase B follow-up)

In **buffered** mode (the durable/realistic config) `WriteBuffer::push_*`
deep-cloned **every** row into the slot via `extend_from_slice`, on every push,
regardless of partitioning. The move-based path (`insert_batch` → owned
partition → `push_*_owned`) instead **moves** rows: it transfers the allocation
into an empty slot, or `Vec::append`s into a warm one — no `String` deep-clone.

`buffer_bench` isolates `push_spans` (clone) vs `push_spans_owned` (move), with
drop excluded from the timed region:

| slot state | n | clone (`extend_from_slice`) | move (owned) | speedup |
|------------|--:|----------------------------:|-------------:|--------:|
| empty (first push) | 1,000  | 414 µs  | **1.2 µs** | ~350× (alloc handoff) |
| empty (first push) | 10,000 | 4.53 ms | **3.2 µs** | ~1400× |
| warm (append)      | 10,000 | 5.77 ms | **2.32 ms** | ~2.5× |
| warm (append)      | 1,000  | 587 µs  | 716 µs | within `--quick` noise |

**Reading it:** the first push into a `(workspace, hour, stream)` slot is now
~O(1) (the incoming `Vec`'s allocation becomes the slot). Subsequent pushes into
that slot during the flush window `Vec::append`-move (~2.5× faster at 10k, no
per-row `String` clone). Net: the per-row deep clone is removed from **all**
buffered ingest; the 1k-warm inversion is `--quick` jitter (re-run without
`--quick` for a clean steady-state number). The `&[Span]` `insert_*` methods are
unchanged, so direct-mode and borrow callers keep today's behavior.

## Still to capture (Phase A remainder)

`query_bench`, `wal load`, and write-path sub-stages (compaction,
overload/backpressure) baselines should be captured on a reference host and, if
the team wants them version-controlled, saved via `--save-baseline` (the repo
already commits criterion baselines under `crates/*/target/criterion`).
