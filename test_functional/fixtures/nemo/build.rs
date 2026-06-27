//! Generate synthetic NeMo OTLP fixtures (Phase 5 §3 captured-fixture corpus,
//! authored from spec until real-capture infra exists per OQ29).
//!
//! Each fixture is a `(spec.binpb, spec.meta.json)` pair.
//!
//! - `spec.binpb` — wire-format `ExportTraceServiceRequest` (or
//!   `ExportLogsServiceRequest` for `evaluator_score`) bytes. This is what
//!   a real client would POST to `:4318/v1/traces` (or `/v1/logs`).
//! - `spec.meta.json` — assertions the e2e runner makes after the
//!   ingest+query roundtrip. The schema is defined inline in
//!   `scenarios/test_e2e_fixtures.rs::FixtureMeta`.
//!
//! Run: `cargo run -p zradar-functional-tests --bin gen_nemo_fixtures`
//!
//! Spec drift policy: when the spec changes (new field, retire a field),
//! update this builder, regenerate, and commit the updated bytes. The OQ20
//! cron will eventually replace these with real captured fixtures.
//!
//! Out of scope: `nat_dangling_span` lives with the reaper (Phase 5.5 / 6)
//! since it needs the libSQL Hot Store. That fixture stays unpublished
//! until the reaper code lands.

use std::path::Path;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, span::Event,
};
use prost::Message;

fn kv_str(k: &str, v: &str) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(v.to_string())),
        }),
    }
}

fn kv_int(k: &str, v: i64) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(v)),
        }),
    }
}

fn kv_bool(k: &str, v: bool) -> KeyValue {
    KeyValue {
        key: k.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(v)),
        }),
    }
}

fn resource_with_service(name: &str) -> Resource {
    Resource {
        attributes: vec![
            kv_str("service.name", name),
            kv_str("deployment.environment", "fixture"),
        ],
        dropped_attributes_count: 0,
    }
}

fn write_fixture(name: &str, dir: &Path, payload: &[u8], meta: &serde_json::Value) {
    let binpb_path = dir.join(format!("{name}.binpb"));
    let meta_path = dir.join(format!("{name}.meta.json"));
    std::fs::write(&binpb_path, payload).expect("write binpb");
    let mut meta_pretty = serde_json::to_string_pretty(meta).expect("encode meta");
    meta_pretty.push('\n');
    std::fs::write(&meta_path, meta_pretty).expect("write meta");
    println!(
        "wrote {} ({} bytes) + {}",
        binpb_path.file_name().unwrap().to_string_lossy(),
        payload.len(),
        meta_path.file_name().unwrap().to_string_lossy(),
    );
}

// ---------------------------------------------------------------------------
// 1. guardrails_input_halt
// ---------------------------------------------------------------------------

fn guardrails_input_halt() -> (ExportTraceServiceRequest, serde_json::Value) {
    let trace_id = (0x10u8..0x20).collect::<Vec<u8>>();
    let root_span_id = vec![0xA1u8; 8];
    let rail_span_id = vec![0xA2u8; 8];
    let action_span_id = vec![0xA3u8; 8];
    let gen_span_id = vec![0xA4u8; 8];

    let base_ts = 1_700_000_000_000_000_000u64;
    let root = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: root_span_id.clone(),
        name: "guardrails.request".to_string(),
        start_time_unix_nano: base_ts,
        end_time_unix_nano: base_ts + 50_000_000,
        attributes: vec![kv_str("guardrails.flow", "input_check")],
        ..Default::default()
    };
    let rail = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: rail_span_id,
        parent_span_id: root_span_id.clone(),
        name: "guardrails.rail.input".to_string(),
        start_time_unix_nano: base_ts + 1_000,
        end_time_unix_nano: base_ts + 30_000_000,
        attributes: vec![
            kv_str("rail.type", "input"),
            kv_str("rail.name", "self_check_input"),
            kv_bool("rail.stop", true),
        ],
        ..Default::default()
    };
    let action = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: action_span_id,
        parent_span_id: root_span_id.clone(),
        name: "guardrails.action.self_check_input".to_string(),
        start_time_unix_nano: base_ts + 2_000,
        end_time_unix_nano: base_ts + 25_000_000,
        attributes: vec![
            kv_str("action.name", "self_check_input"),
            kv_bool("action.has_llm_calls", true),
        ],
        ..Default::default()
    };
    let gen_span = OtlpSpan {
        trace_id,
        span_id: gen_span_id,
        parent_span_id: root_span_id,
        name: "gen_ai.chat.completion".to_string(),
        start_time_unix_nano: base_ts + 3_000,
        end_time_unix_nano: base_ts + 20_000_000,
        attributes: vec![
            kv_str("gen_ai.provider.name", "openai"),
            kv_str("gen_ai.request.model", "gpt-4"),
            kv_str("gen_ai.response.model", "gpt-4-0613"),
            kv_int("gen_ai.usage.input_tokens", 42),
            kv_int("gen_ai.usage.output_tokens", 0),
        ],
        ..Default::default()
    };

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource_with_service("guardrails-svc")),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![root, rail, action, gen_span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let meta = serde_json::json!({
        "fixture": "guardrails_input_halt",
        "kind": "traces",
        "description": "NeMo Guardrails input rail halts the request before the LLM call completes",
        "expected_total_spans": 4,
        "expected_attributes": [
            { "span_name": "guardrails.rail.input",
              "fields": { "rail_type": "input", "rail_stop": true } },
            { "span_name": "guardrails.action.self_check_input",
              "fields": { "action_name": "self_check_input", "action_has_llm_calls": true } },
            { "span_name": "gen_ai.chat.completion",
              "fields": { "llm_model": "gpt-4", "llm_response_model": "gpt-4-0613" } }
        ]
    });
    (req, meta)
}

// ---------------------------------------------------------------------------
// 2. guardrails_action_passthrough
// ---------------------------------------------------------------------------

fn guardrails_action_passthrough() -> (ExportTraceServiceRequest, serde_json::Value) {
    let trace_id = (0x20u8..0x30).collect::<Vec<u8>>();
    let span_id = vec![0xB1u8; 8];
    let base_ts = 1_700_000_100_000_000_000u64;

    let action = OtlpSpan {
        trace_id,
        span_id,
        name: "guardrails.action.summarize".to_string(),
        start_time_unix_nano: base_ts,
        end_time_unix_nano: base_ts + 12_000_000,
        attributes: vec![
            kv_str("action.name", "summarize"),
            kv_bool("action.has_llm_calls", false),
            kv_str("rail.type", "output"),
            kv_bool("rail.stop", false),
        ],
        ..Default::default()
    };

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource_with_service("guardrails-svc")),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![action],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    // Note: the public API treats `rail_stop=0` (i.e. false) as "absent" and
    // returns null for it (see service.rs's `if s.rail_stop != 0` guard).
    // The fixture sets `rail.stop=false` so we exercise the OTLP path, but
    // the meta only asserts the positively-surfaced fields. Same for
    // `action_has_llm_calls=false` — not exposed when false.
    let meta = serde_json::json!({
        "fixture": "guardrails_action_passthrough",
        "kind": "traces",
        "description": "Non-halting Guardrails action passes through to the next rail",
        "expected_total_spans": 1,
        "expected_attributes": [
            { "span_name": "guardrails.action.summarize",
              "fields": {
                  "action_name": "summarize",
                  "rail_type": "output"
              } }
        ]
    });
    (req, meta)
}

// ---------------------------------------------------------------------------
// 3. nat_simple_workflow
// ---------------------------------------------------------------------------

fn nat_simple_workflow() -> (ExportTraceServiceRequest, serde_json::Value) {
    let trace_id = (0x30u8..0x40).collect::<Vec<u8>>();
    let root = vec![0xC1u8; 8];
    let llm = vec![0xC2u8; 8];
    let tool = vec![0xC3u8; 8];
    let base_ts = 1_700_000_200_000_000_000u64;

    let workflow_attrs = vec![
        kv_str("nat.workflow.run_id", "wf-fixture-001"),
        kv_str("nat.framework", "langchain"),
    ];

    let root_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: root.clone(),
        name: "nat.workflow.run".to_string(),
        start_time_unix_nano: base_ts,
        end_time_unix_nano: base_ts + 100_000_000,
        attributes: workflow_attrs.clone(),
        ..Default::default()
    };
    let llm_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: llm,
        parent_span_id: root.clone(),
        name: "nat.llm.call".to_string(),
        start_time_unix_nano: base_ts + 1_000,
        end_time_unix_nano: base_ts + 60_000_000,
        attributes: {
            let mut a = workflow_attrs.clone();
            a.push(kv_str("nat.function.name", "summarize"));
            // GenAI 1.29 names: gen_ai.provider.name maps to llm_provider.
            // gen_ai.usage.input_tokens / .output_tokens map to prompt_tokens
            // / completion_tokens (the legacy gen_ai.usage.prompt_tokens names
            // aren't picked up by the v1.29 convention).
            a.push(kv_str("gen_ai.provider.name", "openai"));
            a.push(kv_str("gen_ai.request.model", "gpt-4-turbo"));
            a.push(kv_int("gen_ai.usage.input_tokens", 487));
            a.push(kv_int("gen_ai.usage.output_tokens", 132));
            a
        },
        ..Default::default()
    };
    let tool_span = OtlpSpan {
        trace_id,
        span_id: tool,
        parent_span_id: root,
        name: "nat.tool.call".to_string(),
        start_time_unix_nano: base_ts + 2_000,
        end_time_unix_nano: base_ts + 80_000_000,
        attributes: {
            let mut a = workflow_attrs.clone();
            a.push(kv_str("tool.name", "websearch"));
            a
        },
        ..Default::default()
    };

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource_with_service("nat-svc")),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![root_span, llm_span, tool_span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let meta = serde_json::json!({
        "fixture": "nat_simple_workflow",
        "kind": "traces",
        "description": "NAT minimal workflow — one LLM call + one tool call under one workflow_run_id",
        "expected_total_spans": 3,
        "expected_attributes": [
            { "span_name": "nat.llm.call",
              "fields": {
                  "workflow_run_id": "wf-fixture-001",
                  "framework": "langchain",
                  "llm_provider": "openai",
                  "llm_model": "gpt-4-turbo"
              } },
            { "span_name": "nat.tool.call",
              "fields": { "tool_name": "websearch", "workflow_run_id": "wf-fixture-001" } }
        ]
    });
    (req, meta)
}

// ---------------------------------------------------------------------------
// 4. nat_multi_function
// ---------------------------------------------------------------------------

fn nat_multi_function() -> (ExportTraceServiceRequest, serde_json::Value) {
    let trace_id = (0x40u8..0x50).collect::<Vec<u8>>();
    let base_ts = 1_700_000_300_000_000_000u64;
    let root = vec![0xD1u8; 8];
    let f1 = vec![0xD2u8; 8];
    let f2 = vec![0xD3u8; 8];
    let f3 = vec![0xD4u8; 8];

    let common = vec![
        kv_str("nat.workflow.run_id", "wf-multi-002"),
        kv_str("nat.framework", "langgraph"),
    ];

    let root_span = OtlpSpan {
        trace_id: trace_id.clone(),
        span_id: root.clone(),
        name: "nat.workflow.run".to_string(),
        start_time_unix_nano: base_ts,
        end_time_unix_nano: base_ts + 200_000_000,
        attributes: common.clone(),
        ..Default::default()
    };

    let make_function = |span_id: Vec<u8>, fn_name: &str, offset: u64| OtlpSpan {
        trace_id: trace_id.clone(),
        span_id,
        parent_span_id: root.clone(),
        name: format!("nat.function.{fn_name}"),
        start_time_unix_nano: base_ts + offset,
        end_time_unix_nano: base_ts + offset + 50_000_000,
        attributes: {
            let mut a = common.clone();
            a.push(kv_str("nat.function.name", fn_name));
            a
        },
        ..Default::default()
    };

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource_with_service("nat-svc")),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![
                    root_span,
                    make_function(f1, "router", 1_000),
                    make_function(f2, "retriever", 60_000_000),
                    make_function(f3, "responder", 120_000_000),
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let meta = serde_json::json!({
        "fixture": "nat_multi_function",
        "kind": "traces",
        "description": "NAT 3-function workflow; every span shares workflow_run_id",
        "expected_total_spans": 4,
        "expected_attributes": [
            { "span_name": "nat.function.router",
              "fields": { "workflow_run_id": "wf-multi-002", "framework": "langgraph" } },
            { "span_name": "nat.function.retriever",
              "fields": { "workflow_run_id": "wf-multi-002" } },
            { "span_name": "nat.function.responder",
              "fields": { "workflow_run_id": "wf-multi-002" } }
        ]
    });
    (req, meta)
}

// ---------------------------------------------------------------------------
// 5. openinference_langchain
// ---------------------------------------------------------------------------
// Vendor-neutral cover: a LangChain span carrying OpenInference attributes
// (a third-party convention) plus gen_ai.* — proves the convention pipeline
// isn't NeMo-only.

fn openinference_langchain() -> (ExportTraceServiceRequest, serde_json::Value) {
    let trace_id = (0x50u8..0x60).collect::<Vec<u8>>();
    let span_id = vec![0xE1u8; 8];
    let base_ts = 1_700_000_400_000_000_000u64;

    let span = OtlpSpan {
        trace_id,
        span_id,
        name: "ChatOpenAI.invoke".to_string(),
        start_time_unix_nano: base_ts,
        end_time_unix_nano: base_ts + 75_000_000,
        attributes: vec![
            // OpenInference-style
            kv_str("openinference.span.kind", "LLM"),
            kv_str("llm.model_name", "gpt-4-turbo-preview"),
            kv_str("llm.provider", "openai"),
            kv_int("llm.token_count.prompt", 612),
            kv_int("llm.token_count.completion", 198),
            kv_int("llm.token_count.total", 810),
            // GenAI 1.29 — gen_ai.provider.name maps to llm_provider.
            kv_str("gen_ai.provider.name", "openai"),
            kv_str("gen_ai.request.model", "gpt-4-turbo-preview"),
            kv_str("gen_ai.response.model", "gpt-4-turbo-2024-04-09"),
            kv_int("gen_ai.usage.input_tokens", 612),
            kv_int("gen_ai.usage.output_tokens", 198),
        ],
        events: vec![Event {
            time_unix_nano: base_ts + 10_000,
            name: "gen_ai.content.prompt".to_string(),
            attributes: vec![kv_str(
                "gen_ai.prompt",
                "[{\"role\":\"user\",\"content\":\"Hello\"}]",
            )],
            dropped_attributes_count: 0,
        }],
        ..Default::default()
    };

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource_with_service("openinference-langchain")),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![span],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let meta = serde_json::json!({
        "fixture": "openinference_langchain",
        "kind": "traces",
        "description": "LangChain instrumented via OpenInference convention — proves vendor-neutral coverage",
        "expected_total_spans": 1,
        "expected_attributes": [
            { "span_name": "ChatOpenAI.invoke",
              "fields": {
                  "llm_provider": "openai",
                  "llm_model": "gpt-4-turbo-preview",
                  "llm_response_model": "gpt-4-turbo-2024-04-09"
              } }
        ]
    });
    (req, meta)
}

// ---------------------------------------------------------------------------
// 6. evaluator_score (logs, not traces)
// ---------------------------------------------------------------------------

fn evaluator_score() -> (ExportLogsServiceRequest, serde_json::Value) {
    let trace_id = (0x60u8..0x70).collect::<Vec<u8>>();
    let base_ts = 1_700_000_500_000_000_000u64;

    let log_record = LogRecord {
        time_unix_nano: base_ts,
        observed_time_unix_nano: base_ts + 100,
        severity_number: 9, // INFO
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(
                "evaluation completed".to_string(),
            )),
        }),
        attributes: vec![
            kv_str("nemo.evaluator.name", "answer_relevance"),
            kv_str("nemo.evaluator.version", "1.0.0"),
            kv_str("score.type", "numeric"),
            KeyValue {
                key: "score.value".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::DoubleValue(0.92)),
                }),
            },
        ],
        trace_id,
        span_id: vec![0xF1u8; 8],
        ..Default::default()
    };

    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(resource_with_service("nemo-evaluator")),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                log_records: vec![log_record],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let meta = serde_json::json!({
        "fixture": "evaluator_score",
        "kind": "logs",
        "description": "NeMo Evaluator emits a numeric score as an OTLP log under the trace it scored",
        "expected_log_count": 1,
        "expected_attributes": [
            { "log_body_contains": "evaluation completed",
              "fields": {
                  "score_type": "numeric",
                  "score_value": 0.92,
                  "evaluator_name": "answer_relevance"
              } }
        ]
    });
    (req, meta)
}

fn main() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("nemo");
    std::fs::create_dir_all(&dir).expect("create fixtures dir");

    let traces: Vec<(&str, ExportTraceServiceRequest, serde_json::Value)> = vec![
        {
            let (r, m) = guardrails_input_halt();
            ("guardrails_input_halt", r, m)
        },
        {
            let (r, m) = guardrails_action_passthrough();
            ("guardrails_action_passthrough", r, m)
        },
        {
            let (r, m) = nat_simple_workflow();
            ("nat_simple_workflow", r, m)
        },
        {
            let (r, m) = nat_multi_function();
            ("nat_multi_function", r, m)
        },
        {
            let (r, m) = openinference_langchain();
            ("openinference_langchain", r, m)
        },
    ];

    for (name, req, meta) in traces {
        let bytes = req.encode_to_vec();
        write_fixture(name, &dir, &bytes, &meta);
    }

    let (logs_req, logs_meta) = evaluator_score();
    write_fixture(
        "evaluator_score",
        &dir,
        &logs_req.encode_to_vec(),
        &logs_meta,
    );

    println!("\nWrote 6 fixtures under {}", dir.display());
}
