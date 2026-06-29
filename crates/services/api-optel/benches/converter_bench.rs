//! Converter benchmarks for the OTLP write path (Phase 5 §6.1).
//!
//! Measures `OtlpConverter::convert_resource_spans_with` latency across four
//! input shapes drawn from the spec:
//!
//! - `tiny` — 1-attribute span (the generic SPAN baseline)
//! - `med` — 20-attribute span modelling a NAT workflow LLM call
//! - `fat` — 100-attribute span + 30 events (NAT Guardrails action with chat
//!   history) — the upper end of realistic instrumentation density
//! - `wide` — 512 attributes + 200 events; pushes the OQ18 per-span **event**
//!   cap (`MAX_EVENTS_PER_SPAN = 128`) so the event-truncation branch fires,
//!   and exercises a wide attribute slice. There is no attribute *count* cap
//!   on the OTLP side — this input measures convention-pipeline cost on a
//!   wide slice, not an attribute-truncation path.
//!
//! Measurement methodology:
//! - **One `OtlpConverter` is built outside the bench loop** (the real
//!   ingest path holds a single converter for the server's lifetime).
//!   Using the static `convert_resource_spans` rebuilds the pipeline on
//!   every call, which would charge the tiny case with fixed-cost setup.
//! - **`iter_batched` clones the input outside the timed window** (the
//!   real ingest path decodes protobuf once and moves the owned struct in
//!   — it never clones). Including the clone would over-count the tiny
//!   shape by 8–12 small allocations.
//!
//! Run locally: `cargo bench -p api-optel --bench converter_bench`
//! Save a baseline: `cargo bench -p api-optel --bench converter_bench -- --save-baseline release-1.0`

use api_optel::OtlpConverter;
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, span::Event,
};
use zradar_models::RequestContext;

fn kv_str(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.to_string())),
        }),
        ..Default::default()
    }
}

fn kv_int(k: &str, v: i64) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(v)),
        }),
        ..Default::default()
    }
}

fn kv_bool(k: &str, v: bool) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(v)),
        }),
        ..Default::default()
    }
}

fn bench_context() -> RequestContext {
    RequestContext {
        workspace_id: uuid::Uuid::nil().into(),
    }
}

fn resource_with_service(name: &str) -> Resource {
    Resource {
        attributes: vec![
            kv_str("service.name", name),
            kv_str("deployment.environment", "bench"),
        ],
        dropped_attributes_count: 0,
        ..Default::default()
    }
}

/// 1-attribute span — the generic SPAN baseline.
fn input_tiny() -> ResourceSpans {
    let span = OtlpSpan {
        trace_id: vec![0x11; 16],
        span_id: vec![0x22; 8],
        name: "tiny.op".to_string(),
        start_time_unix_nano: 1_000,
        end_time_unix_nano: 2_000,
        attributes: vec![kv_str("custom.key", "value")],
        ..Default::default()
    };
    ResourceSpans {
        resource: Some(resource_with_service("tiny-svc")),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope::default()),
            spans: vec![span],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// 20-attribute span modelling a NAT workflow LLM call — the realistic mid
/// case for NAT instrumentation.
fn input_med() -> ResourceSpans {
    let attrs = vec![
        kv_str("nat.workflow.run_id", "wf-7c1d-bench"),
        kv_str("nat.conversation.id", "conv-bench"),
        kv_str("nat.framework", "langchain"),
        kv_str("nat.function.name", "summarize"),
        kv_str("nat.event_type", "TOOL_END"),
        kv_str("gen_ai.system", "openai"),
        kv_str("gen_ai.request.model", "gpt-4-turbo"),
        kv_str("gen_ai.response.model", "gpt-4-turbo-2024-04-09"),
        kv_str("gen_ai.response.id", "chatcmpl-bench"),
        kv_int("gen_ai.usage.prompt_tokens", 487),
        kv_int("gen_ai.usage.completion_tokens", 132),
        kv_int("gen_ai.usage.total_tokens", 619),
        kv_str("gen_ai.request.temperature", "0.2"),
        kv_str("gen_ai.request.max_tokens", "1024"),
        kv_bool("llm.cache.hit", false),
        kv_str("session.id", "sess-abc"),
        kv_str("user.id", "user-bench"),
        kv_str("tool.name", "websearch"),
        kv_str("invocation.id", "inv-bench"),
        kv_str("http.route", "/agents/summarize"),
    ];
    let span = OtlpSpan {
        trace_id: vec![0x33; 16],
        span_id: vec![0x44; 8],
        parent_span_id: vec![0x55; 8],
        name: "nat.llm.call".to_string(),
        start_time_unix_nano: 10_000_000,
        end_time_unix_nano: 12_500_000,
        attributes: attrs,
        ..Default::default()
    };
    ResourceSpans {
        resource: Some(resource_with_service("nat-svc")),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope::default()),
            spans: vec![span],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// 100-attribute span + 30 events — a NAT Guardrails action with a long chat
/// history. Upper end of realistic instrumentation density.
fn input_fat() -> ResourceSpans {
    let mut attrs = Vec::with_capacity(100);
    // First 10 are first-class mapped fields — exercises every convention.
    attrs.push(kv_str("nat.workflow.run_id", "wf-fat-bench"));
    attrs.push(kv_str("nat.framework", "langgraph"));
    attrs.push(kv_str("nat.function.name", "self_check_input"));
    attrs.push(kv_str("rail.type", "input"));
    attrs.push(kv_str("rail.name", "self_check"));
    attrs.push(kv_bool("rail.stop", true));
    attrs.push(kv_str("action.name", "self_check_input"));
    attrs.push(kv_bool("action.has_llm_calls", true));
    attrs.push(kv_str("gen_ai.system", "openai"));
    attrs.push(kv_str("gen_ai.request.model", "gpt-4"));
    // Remaining 90 are catch-all attributes — these go into the JSON column.
    for i in 0..90 {
        attrs.push(kv_str(&format!("custom.attr.{i}"), &format!("value-{i}")));
    }

    let mut events = Vec::with_capacity(30);
    for i in 0..30 {
        events.push(Event {
            time_unix_nano: 1_000 + i,
            name: format!("chat.turn.{i}"),
            attributes: vec![
                kv_str("role", if i % 2 == 0 { "user" } else { "assistant" }),
                kv_str("content", &format!("turn-{i}-content")),
            ],
            dropped_attributes_count: 0,
        });
    }

    let span = OtlpSpan {
        trace_id: vec![0x66; 16],
        span_id: vec![0x77; 8],
        name: "guardrails.action.self_check_input".to_string(),
        start_time_unix_nano: 0,
        end_time_unix_nano: 50_000_000,
        attributes: attrs,
        events,
        ..Default::default()
    };
    ResourceSpans {
        resource: Some(resource_with_service("guardrails-svc")),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope::default()),
            spans: vec![span],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

/// Wide-input span: 512 attributes + 200 events. The 200-event count crosses
/// the OQ18 per-span event cap (`MAX_EVENTS_PER_SPAN = 128`) so the event
/// truncation branch is exercised. Attributes have no count cap in
/// `api-optel`, so the wide attribute slice just measures convention-pipeline
/// cost at scale.
fn input_wide() -> ResourceSpans {
    let mut attrs = Vec::with_capacity(512);
    for i in 0..512 {
        attrs.push(kv_str(
            &format!("attr.{i}"),
            "x".repeat(64).as_str(), // 64-byte values
        ));
    }
    let mut events = Vec::with_capacity(200);
    for i in 0..200 {
        events.push(Event {
            time_unix_nano: i,
            name: format!("e.{i}"),
            attributes: vec![kv_str("k", "v")],
            dropped_attributes_count: 0,
        });
    }
    let span = OtlpSpan {
        trace_id: vec![0x88; 16],
        span_id: vec![0x99; 8],
        name: "wide.test".to_string(),
        start_time_unix_nano: 1,
        end_time_unix_nano: 2,
        attributes: attrs,
        events,
        ..Default::default()
    };
    ResourceSpans {
        resource: Some(resource_with_service("wide-svc")),
        scope_spans: vec![ScopeSpans {
            scope: Some(InstrumentationScope::default()),
            spans: vec![span],
            schema_url: String::new(),
        }],
        schema_url: String::new(),
    }
}

fn bench_convert(c: &mut Criterion) {
    let ctx = bench_context();
    // Build one converter and reuse it across iterations — matches the real
    // ingest path (the server holds one converter for its lifetime). The
    // static `convert_resource_spans` rebuilds the conventions pipeline every
    // call; charging the tiny case with that fixed setup would mislead.
    let converter = OtlpConverter::new();
    let mut group = c.benchmark_group("converter");

    let tiny = input_tiny();
    group.bench_function("convert_resource_spans/tiny_1_attr", |b| {
        b.iter_batched(
            || tiny.clone(),
            |input| {
                let spans = converter
                    .convert_resource_spans_with(black_box(input), &ctx)
                    .unwrap();
                black_box(spans);
            },
            BatchSize::SmallInput,
        );
    });

    let med = input_med();
    group.bench_function("convert_resource_spans/med_20_attr_nat_llm", |b| {
        b.iter_batched(
            || med.clone(),
            |input| {
                let spans = converter
                    .convert_resource_spans_with(black_box(input), &ctx)
                    .unwrap();
                black_box(spans);
            },
            BatchSize::SmallInput,
        );
    });

    let fat = input_fat();
    group.bench_function("convert_resource_spans/fat_100_attr_30_events", |b| {
        b.iter_batched(
            || fat.clone(),
            |input| {
                let spans = converter
                    .convert_resource_spans_with(black_box(input), &ctx)
                    .unwrap();
                black_box(spans);
            },
            BatchSize::SmallInput,
        );
    });

    let wide = input_wide();
    group.bench_function("convert_resource_spans/wide_512_attr_200_events", |b| {
        // Wide inputs (~40KB once cloned) — `LargeInput` keeps Criterion from
        // batching ~10 of them in memory between iterations, which inflated
        // the measurement vs. `SmallInput`.
        b.iter_batched(
            || wide.clone(),
            |input| {
                let spans = converter
                    .convert_resource_spans_with(black_box(input), &ctx)
                    .unwrap();
                black_box(spans);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_convert);
criterion_main!(benches);
