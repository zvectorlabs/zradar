//! OTLP span-event processing (Phase 1 R1.6).
//!
//! Implements the event allowlist defined in TECH-SPEC-PHASE-1.md §6:
//!
//! | Event name                                    | Source          | Action                                      |
//! |-----------------------------------------------|-----------------|---------------------------------------------|
//! | `gen_ai.content.prompt`                       | OTel GenAI ≤1.28 | populate `llm_input` (if empty)            |
//! | `gen_ai.content.completion`                   | OTel GenAI ≤1.28 | populate `llm_output` (if empty)           |
//! | `gen_ai.client.inference.operation.details` + `gen_ai.input.messages` | OTel GenAI ≥1.29 | populate `llm_input` (serialized JSON, if empty) |
//! | `gen_ai.client.inference.operation.details` + `gen_ai.output.messages` | OTel GenAI ≥1.29 | populate `llm_output` (serialized JSON, if empty) |
//! | `exception`                                   | OTel             | preserve in `events` JSON                  |
//! | `nat.event_type = *_NEW_TOKEN`                | NAT internals    | **dropped** (token-stream noise)           |
//! | Everything else                               | —                | preserved in `events` JSON                 |
//!
//! **Priority:** attribute-level `llm.input` / `gen_ai.prompt` wins — events
//! only populate `llm_input` / `llm_output` when they are still empty after
//! the attribute convention pipeline has run.
//!
//! **Caps (OQ18):**
//! - `MAX_EVENTS_PER_SPAN = 128` — trailing events are dropped.
//! - `MAX_BYTES_PER_SPAN = 65_536` — JSON serialization is truncated when the
//!   running byte total exceeds this limit, and a sentinel object
//!   `{"events.truncated":true}` is appended.

use opentelemetry_proto::tonic::common::v1::{AnyValue, any_value::Value};
use opentelemetry_proto::tonic::trace::v1::span::Event;
use zradar_models::Span;

/// Maximum number of events preserved in the `events` JSON column per span.
pub const MAX_EVENTS_PER_SPAN: usize = 128;
/// Maximum byte budget for the serialized `events` JSON array per span.
pub const MAX_BYTES_PER_SPAN: usize = 65_536;

/// Process OTLP span events into `Span` fields per the R1.6 allowlist.
///
/// Mutates `span` in place: may populate `llm_input`, `llm_output`, and
/// `events` (JSON array). Called from `OtlpConverter::convert_span` after
/// the attribute convention pipeline has run.
///
/// Cap order (OQ18): NAT `*_NEW_TOKEN` noise is dropped **before** the
/// `MAX_EVENTS_PER_SPAN` count cap is applied so that high-volume token
/// streams never crowd out real events (exception, custom, etc.).
pub fn apply_span_events(events: &[Event], span: &mut Span) {
    let mut preserved: Vec<serde_json::Value> = Vec::new();
    let mut running_bytes: usize = 2; // account for "[]"
    let mut truncated = false;

    // OQ18: drop noise first, then cap the remainder.
    let meaningful_events = events.iter().filter(|e| !is_new_token_event(e));

    for event in meaningful_events.take(MAX_EVENTS_PER_SPAN) {
        let name = event.name.as_str();

        // gen_ai.content.prompt → llm_input (OTel GenAI ≤1.28)
        if name == "gen_ai.content.prompt" {
            // Promoted; not preserved in events JSON.
            if span.llm_input.is_empty()
                && let Some(content) = get_event_body_string(event)
            {
                span.llm_input = content;
            }
            continue;
        }

        // gen_ai.content.completion → llm_output (OTel GenAI ≤1.28)
        if name == "gen_ai.content.completion" {
            // Promoted; not preserved in events JSON.
            if span.llm_output.is_empty()
                && let Some(content) = get_event_body_string(event)
            {
                span.llm_output = content;
            }
            continue;
        }

        // gen_ai.client.inference.operation.details (OTel GenAI ≥1.29)
        if name == "gen_ai.client.inference.operation.details" {
            apply_inference_detail_event(event, span);
            // These are also promoted; do not preserve in events JSON.
            continue;
        }

        // All other events: serialize and accumulate in the events JSON column.
        let event_json = event_to_json(event);
        let serialized = serde_json::to_string(&event_json).unwrap_or_default();
        let byte_cost = serialized.len() + 1; // +1 for comma separator

        if running_bytes + byte_cost > MAX_BYTES_PER_SPAN {
            truncated = true;
            break;
        }

        running_bytes += byte_cost;
        preserved.push(event_json);
    }

    if truncated {
        preserved.push(serde_json::json!({"events.truncated": true}));
    }

    if !preserved.is_empty() {
        span.events = serde_json::to_string(&preserved).unwrap_or_else(|_| "[]".to_string());
    }
    // If preserved is empty we leave span.events at its default "[]".
}

/// Returns true if this event represents a NAT token-stream event that must
/// be dropped (any event whose `nat.event_type` attribute ends with
/// `_NEW_TOKEN`).
fn is_new_token_event(event: &Event) -> bool {
    event.attributes.iter().any(|kv| {
        kv.key == "nat.event_type"
            && kv
                .value
                .as_ref()
                .and_then(|v| v.value.as_ref())
                .map(|v| match v {
                    Value::StringValue(s) => s.ends_with("_NEW_TOKEN"),
                    _ => false,
                })
                .unwrap_or(false)
    })
}

/// Extract the string body from a simple event (either as `event.body` string
/// attribute or from the first `StringValue` attribute named `content` /
/// `gen_ai.content`).
fn get_event_body_string(event: &Event) -> Option<String> {
    // OTel GenAI ≤1.28 uses a `content` attribute on the event.
    for attr in &event.attributes {
        if matches!(attr.key.as_str(), "content" | "gen_ai.content")
            && let Some(Value::StringValue(s)) = attr.value.as_ref().and_then(|a| a.value.as_ref())
        {
            return Some(s.clone());
        }
    }
    // Fallback: first string attribute value.
    for attr in &event.attributes {
        if let Some(Value::StringValue(s)) = attr.value.as_ref().and_then(|a| a.value.as_ref()) {
            return Some(s.clone());
        }
    }
    None
}

/// Handle `gen_ai.client.inference.operation.details` event (OTel GenAI ≥1.29).
/// Maps `gen_ai.input.messages` → `llm_input` (JSON) and
/// `gen_ai.output.messages` → `llm_output` (JSON).
fn apply_inference_detail_event(event: &Event, span: &mut Span) {
    for attr in &event.attributes {
        match attr.key.as_str() {
            "gen_ai.input.messages" if span.llm_input.is_empty() => {
                if let Some(json_val) = attr_to_json_value(attr.value.as_ref()) {
                    span.llm_input =
                        serde_json::to_string(&json_val).unwrap_or_else(|_| String::new());
                }
            }
            "gen_ai.output.messages" if span.llm_output.is_empty() => {
                if let Some(json_val) = attr_to_json_value(attr.value.as_ref()) {
                    span.llm_output =
                        serde_json::to_string(&json_val).unwrap_or_else(|_| String::new());
                }
            }
            _ => {}
        }
    }
}

/// Serialize an OTLP `AnyValue` to `serde_json::Value`.
fn any_value_to_json(v: &AnyValue) -> serde_json::Value {
    match &v.value {
        Some(Value::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(Value::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(Value::IntValue(i)) => serde_json::Value::Number((*i).into()),
        Some(Value::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Some(Value::ArrayValue(arr)) => {
            serde_json::Value::Array(arr.values.iter().map(any_value_to_json).collect())
        }
        Some(Value::KvlistValue(kv)) => {
            let mut map = serde_json::Map::new();
            for item in &kv.values {
                if let Some(val) = &item.value {
                    map.insert(item.key.clone(), any_value_to_json(val));
                }
            }
            serde_json::Value::Object(map)
        }
        Some(Value::BytesValue(b)) => serde_json::Value::String(hex::encode(b)),
        Some(Value::StringValueStrindex(_)) => serde_json::Value::Null,
        None => serde_json::Value::Null,
    }
}

fn attr_to_json_value(value: Option<&AnyValue>) -> Option<serde_json::Value> {
    value.map(any_value_to_json)
}

/// Convert an OTLP `Event` to a `serde_json::Value` for the events JSON column.
fn event_to_json(event: &Event) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert(
        "name".to_string(),
        serde_json::Value::String(event.name.clone()),
    );
    if event.time_unix_nano != 0 {
        map.insert(
            "timestamp_ns".to_string(),
            serde_json::Value::Number((event.time_unix_nano as i64).into()),
        );
    }
    if !event.attributes.is_empty() {
        let mut attrs = serde_json::Map::new();
        for kv in &event.attributes {
            if let Some(v) = &kv.value {
                attrs.insert(kv.key.clone(), any_value_to_json(v));
            }
        }
        map.insert("attributes".to_string(), serde_json::Value::Object(attrs));
    }
    serde_json::Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::trace::v1::span::Event;

    fn str_kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.to_string())),
            }),
            ..Default::default()
        }
    }

    fn make_event(name: &str, attrs: Vec<KeyValue>) -> Event {
        Event {
            name: name.to_string(),
            attributes: attrs,
            ..Default::default()
        }
    }

    #[test]
    fn test_gen_ai_content_prompt_populates_llm_input() {
        let events = vec![make_event(
            "gen_ai.content.prompt",
            vec![str_kv("content", "Is this safe?")],
        )];
        let mut span = Span::default();
        apply_span_events(&events, &mut span);
        assert_eq!(span.llm_input, "Is this safe?");
        // Promoted events do not appear in events JSON.
        assert_eq!(span.events, "[]");
    }

    #[test]
    fn test_gen_ai_content_completion_populates_llm_output() {
        let events = vec![make_event(
            "gen_ai.content.completion",
            vec![str_kv("content", "Yes, it is safe.")],
        )];
        let mut span = Span::default();
        apply_span_events(&events, &mut span);
        assert_eq!(span.llm_output, "Yes, it is safe.");
        assert_eq!(span.events, "[]");
    }

    #[test]
    fn test_llm_input_already_set_is_not_overwritten_by_event() {
        let events = vec![make_event(
            "gen_ai.content.prompt",
            vec![str_kv("content", "from event")],
        )];
        let mut span = Span {
            llm_input: "from attribute".to_string(),
            ..Span::default()
        };
        apply_span_events(&events, &mut span);
        // Attribute wins — event must not overwrite.
        assert_eq!(span.llm_input, "from attribute");
    }

    #[test]
    fn test_nat_new_token_event_is_dropped() {
        let events = vec![
            make_event(
                "some_event",
                vec![str_kv("nat.event_type", "LLM_NEW_TOKEN")],
            ),
            make_event(
                "other_event",
                vec![str_kv("nat.event_type", "CHUNK_NEW_TOKEN")],
            ),
            make_event("exception", vec![str_kv("exception.message", "boom")]),
        ];
        let mut span = Span::default();
        apply_span_events(&events, &mut span);
        // Only "exception" survives.
        let parsed: serde_json::Value = serde_json::from_str(&span.events).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "exception");
    }

    #[test]
    fn test_unrecognized_events_preserved_in_events_json() {
        let events = vec![make_event("my.custom.event", vec![str_kv("key", "value")])];
        let mut span = Span::default();
        apply_span_events(&events, &mut span);
        let parsed: serde_json::Value = serde_json::from_str(&span.events).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "my.custom.event");
        assert_eq!(arr[0]["attributes"]["key"], "value");
    }

    #[test]
    fn test_inference_detail_event_populates_llm_input_and_output() {
        let events = vec![make_event(
            "gen_ai.client.inference.operation.details",
            vec![
                str_kv("gen_ai.input.messages", r#"[{"role":"user"}]"#),
                str_kv("gen_ai.output.messages", r#"[{"role":"assistant"}]"#),
            ],
        )];
        let mut span = Span::default();
        apply_span_events(&events, &mut span);
        // Promoted — not in events JSON.
        assert_eq!(span.events, "[]");
        // llm_input/output populated from the event attributes.
        assert!(!span.llm_input.is_empty());
        assert!(!span.llm_output.is_empty());
    }

    #[test]
    fn test_empty_events_leaves_default() {
        let mut span = Span::default();
        apply_span_events(&[], &mut span);
        assert_eq!(span.events, "[]");
        assert!(span.llm_input.is_empty());
        assert!(span.llm_output.is_empty());
    }

    #[test]
    fn test_max_events_per_span_cap() {
        // Generate 200 events — only MAX_EVENTS_PER_SPAN (128) should be processed.
        let events: Vec<Event> = (0..200)
            .map(|i| make_event("custom.event", vec![str_kv("idx", &i.to_string())]))
            .collect();
        let mut span = Span::default();
        apply_span_events(&events, &mut span);
        let parsed: serde_json::Value = serde_json::from_str(&span.events).unwrap();
        // At most 128 events processed, possibly fewer due to byte budget.
        assert!(parsed.as_array().unwrap().len() <= MAX_EVENTS_PER_SPAN);
    }

    /// OQ18 regression: NAT NEW_TOKEN events must be dropped BEFORE the
    /// MAX_EVENTS_PER_SPAN cap is applied. If 200 LLM_NEW_TOKEN events precede
    /// a real exception event, the exception must still survive.
    #[test]
    fn test_oq18_noise_dropped_before_count_cap() {
        let mut events: Vec<Event> = (0..200)
            .map(|_| {
                make_event(
                    "noise_event",
                    vec![str_kv("nat.event_type", "LLM_NEW_TOKEN")],
                )
            })
            .collect();
        events.push(make_event(
            "exception",
            vec![str_kv("exception.message", "real error")],
        ));

        let mut span = Span::default();
        apply_span_events(&events, &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.events).unwrap();
        let arr = parsed.as_array().unwrap();
        // The exception must be present despite all NEW_TOKEN events preceding it.
        assert!(
            arr.iter().any(|e| e["name"] == "exception"),
            "exception must survive after 200 LLM_NEW_TOKEN events are dropped"
        );
        // NEW_TOKEN events must not appear in the preserved events.
        assert!(
            arr.iter().all(|e| e["name"] != "noise_event"),
            "NEW_TOKEN events must be dropped"
        );
    }

    /// byte-budget truncation marker is appended when events JSON exceeds limit.
    #[test]
    fn test_byte_budget_truncation_appends_marker() {
        // Create events large enough to exceed MAX_BYTES_PER_SPAN.
        let big_value = "x".repeat(10_000);
        let events: Vec<Event> = (0..20)
            .map(|i| {
                make_event(
                    "big.event",
                    vec![str_kv("data", &format!("{i}{}", big_value))],
                )
            })
            .collect();
        let mut span = Span::default();
        apply_span_events(&events, &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.events).unwrap();
        let arr = parsed.as_array().unwrap();
        // Last element must be the truncation marker.
        let last = arr.last().expect("events array must not be empty");
        assert_eq!(
            last["events.truncated"],
            serde_json::Value::Bool(true),
            "truncation marker must be present when byte budget exceeded"
        );
    }
}
