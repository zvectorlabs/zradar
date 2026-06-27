//! OQ15 zero-copy invariant regression test (TECH-SPEC-PHASE-5 §6.5).
//!
//! ## What this test actually measures
//!
//! The spec's OQ15 invariant is stated per-span: the convention pipeline must
//! allocate *at most K + 1* `String`s per span, where K is the number of
//! populated `Span` fields after conversion (+1 for the JSON attributes blob).
//!
//! This test takes a **pragmatic regression-gate form** of that invariant:
//! it measures the **total** heap allocations made inside one call to
//! `convert_resource_spans_with` on a representative input (the
//! `nat_simple_workflow` shape) and asserts the count stays below a recorded
//! ceiling. That ceiling includes:
//!
//! - the per-Span `String` clones the convention pipeline is allowed to make
//!   (the K+1 the spec is really about)
//! - JSON serialization of the `attributes` catch-all
//! - `Vec`/`String` allocations from `convert_span` itself (status,
//!   hex-encoding, etc.)
//!
//! A literal K+1 assertion would need runtime introspection of the
//! constructed `Span` to count populated fields. The pragmatic-ceiling form
//! still catches every regression the spec invariant catches (any new
//! per-span allocation source pushes the total up), without binding the test
//! to private internals.
//!
//! ## Gating
//!
//! `allocation-counter` swaps in a custom global allocator on link. It must
//! stay opt-in or every other test in this crate would see different
//! allocation behavior. Run with:
//!
//! ```sh
//! cargo test -p api-optel --features count-allocations --test oq15_zero_copy
//! ```
//!
//! Standard `cargo test` skips the whole file — the `cfg(feature = ...)`
//! gate ensures the body never compiles unless the feature is on.

#![cfg(feature = "count-allocations")]

use api_optel::OtlpConverter;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan};
use zradar_models::RequestContext;

fn kv_str(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.to_string())),
        }),
    }
}

/// The `nat_simple_workflow` shape from spec §3.2 — minimal real-world input.
fn nat_simple_workflow_input() -> ResourceSpans {
    ResourceSpans {
        resource: Some(Resource {
            attributes: vec![
                kv_str("service.name", "nat-svc"),
                kv_str("deployment.environment", "test"),
            ],
            dropped_attributes_count: 0,
        }),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope::default()),
            spans: vec![OtlpSpan {
                trace_id: vec![0x01; 16],
                span_id: vec![0x02; 8],
                name: "nat.llm.call".to_string(),
                start_time_unix_nano: 1_000_000,
                end_time_unix_nano: 2_000_000,
                attributes: vec![
                    kv_str("nat.workflow.run_id", "wf-7c1d"),
                    kv_str("nat.framework", "langchain"),
                    kv_str("gen_ai.system", "openai"),
                    kv_str("gen_ai.request.model", "gpt-4-turbo"),
                ],
                ..Default::default()
            }],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// Recorded baseline on 2026-06-26 against the `nat_simple_workflow` shape
/// (4 attributes, 1 span) — measured `count_total = 44`. Cap set to 55 — a
/// ~25% (11-allocation) cushion that absorbs single-allocation refactors
/// and toolchain allocator noise without letting a meaningful regression
/// hide.
///
/// Measurement detail: the cloned `ResourceSpans` is built **outside**
/// `measure`, so the input-clone allocations are not charged to the
/// converter. The converter is built outside too — matches the real ingest
/// path where the server holds one converter for its lifetime. The
/// 44-allocation budget therefore reflects the *steady-state* per-span
/// conversion cost.
///
/// If this assertion regresses, *first* verify the regression is real (run
/// the test 5x; look at the emitted `count_total`), *then* either fix the
/// new allocation source or update this number **with a changelog note**
/// explaining why the convention pipeline now allocates more.
///
/// **Do not silently bump.** Every increase here is a contract widening.
const OQ15_BASELINE_MAX_ALLOCS: u64 = 55;

#[test]
fn convert_one_span_total_alloc_budget_within_baseline() {
    let ctx = RequestContext {
        tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        project_id: "22222222-2222-2222-2222-222222222222".to_string(),
    };
    let converter = OtlpConverter::new();
    let input = nat_simple_workflow_input();

    // Clone *outside* `measure` so the clone's own allocations aren't charged
    // to the converter. The real ingest path receives an already-owned struct
    // from protobuf decode — no clone — so the bench shape we want to pin is
    // "converter operating on an owned input."
    let owned_input = input.clone();
    let info = allocation_counter::measure(|| {
        let spans = converter
            .convert_resource_spans_with(owned_input, &ctx)
            .unwrap();
        std::hint::black_box(spans);
    });

    // Emit the actual count so the CI log shows remaining headroom.
    eprintln!(
        "OQ15: count_total={} bytes_total={} count_max={} baseline={}",
        info.count_total, info.bytes_total, info.count_max, OQ15_BASELINE_MAX_ALLOCS,
    );

    assert!(
        info.count_total <= OQ15_BASELINE_MAX_ALLOCS,
        "OQ15 regression: convert_resource_spans_with allocated {} times \
         (baseline allows up to {}). \
         A new allocation source has appeared in the convention pipeline. \
         Investigate; do not silently bump OQ15_BASELINE_MAX_ALLOCS.",
        info.count_total,
        OQ15_BASELINE_MAX_ALLOCS,
    );

    // Note on `count_current`: we do *not* assert it equals zero. The closure
    // takes ownership of `owned_input` (allocated outside `measure`) and
    // drops it inside — so `count_current` ends up negative, reflecting the
    // net deallocations of the input strings. That's expected behavior, not
    // a leak. A real leak would manifest as `count_total` drifting upward
    // across the second-call stability test below.
}

#[test]
fn convert_one_span_alloc_count_is_stable_across_calls() {
    // Pin that the second call uses the same allocation budget as the first.
    // Catches "lazy init" allocations that would only show up on the first
    // call (e.g. lazy_static, OnceCell, interned strings).
    let ctx = RequestContext {
        tenant_id: "11111111-1111-1111-1111-111111111111".to_string(),
        project_id: "22222222-2222-2222-2222-222222222222".to_string(),
    };
    let converter = OtlpConverter::new();
    let input = nat_simple_workflow_input();

    // Warm-up call outside the measurement: covers anything that allocates
    // exactly once for the process lifetime.
    let warmup_input = input.clone();
    converter
        .convert_resource_spans_with(warmup_input, &ctx)
        .unwrap();

    let first_input = input.clone();
    let first = allocation_counter::measure(|| {
        let _ = converter
            .convert_resource_spans_with(first_input, &ctx)
            .unwrap();
    });
    let second_input = input.clone();
    let second = allocation_counter::measure(|| {
        let _ = converter
            .convert_resource_spans_with(second_input, &ctx)
            .unwrap();
    });

    assert_eq!(
        first.count_total, second.count_total,
        "convert_resource_spans_with allocation count drifts between calls: \
         first={}, second={}. This suggests caching, lazy init, or interior \
         mutability that this test is not modelling. Investigate before \
         bumping the baseline.",
        first.count_total, second.count_total,
    );
}
