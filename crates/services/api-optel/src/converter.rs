//! OTLP protobuf to internal model converter
//!
//! This module converts OpenTelemetry Protocol (OTLP) protobuf messages
//! to zradar's internal Span model.
//!
//! Attribute mapping is dispatched through the [`AttributeConvention`] trait
//! pipeline in [`crate::conventions`] — see TECH-SPEC-PHASE-0.md §4.2b and
//! TECH-SPEC-PHASE-1.md §3.6 for the zero-copy ingest contract. Each OTel /
//! vendor namespace lives in its own module behind the trait; this file owns
//! only the OTLP-to-`Span` framing (identity, timing, status, JSON catch-all).

use anyhow::Result;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, Span as OtlpSpan};
use std::sync::Arc;
use zradar_models::WorkspaceId;
use zradar_models::{RequestContext, Span};
use zradar_traits::{ContentCapturePolicy, NoopContentCapturePolicy};

use crate::conventions::{AttrView, AttributeConvention, default_conventions};
use crate::otlp_util::any_value_to_json;
use crate::span_events::apply_span_events;
use crate::span_links::apply_span_links;

/// Converter for OTLP protobuf to internal models.
///
/// Holds a fixed pipeline of [`AttributeConvention`] implementations applied
/// in priority order to each span's borrowed attributes (zero-copy via
/// [`AttrView`]). Construct with [`OtlpConverter::new`] for the default
/// Phase 0 pipeline, or [`OtlpConverter::with_conventions`] for custom
/// orderings (e.g., tests).
pub struct OtlpConverter {
    conventions: Vec<Box<dyn AttributeConvention>>,
    content_capture_policy: Arc<dyn ContentCapturePolicy>,
}

struct FixedContentCapturePolicy(bool);

impl ContentCapturePolicy for FixedContentCapturePolicy {
    fn capture_enabled(&self, _workspace_id: WorkspaceId) -> bool {
        self.0
    }
}

impl Default for OtlpConverter {
    fn default() -> Self {
        Self::new()
    }
}

impl OtlpConverter {
    /// Construct a converter with the Phase 0 default convention pipeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            conventions: default_conventions(),
            content_capture_policy: Arc::new(NoopContentCapturePolicy),
        }
    }

    /// Construct a converter with a custom convention pipeline. Conventions
    /// run in iteration order; later conventions overwrite earlier ones on
    /// field conflicts.
    #[must_use]
    pub fn with_conventions(conventions: Vec<Box<dyn AttributeConvention>>) -> Self {
        Self {
            conventions,
            content_capture_policy: Arc::new(NoopContentCapturePolicy),
        }
    }

    /// Attach a content capture policy. When the policy returns `false` for a
    /// project, `llm_input` and `llm_output` are cleared before persisting.
    #[must_use]
    pub fn with_content_capture_policy(mut self, policy: Arc<dyn ContentCapturePolicy>) -> Self {
        self.content_capture_policy = policy;
        self
    }

    /// Attach a fixed content capture decision for the current request.
    #[must_use]
    pub fn with_capture_enabled(mut self, capture_enabled: bool) -> Self {
        self.content_capture_policy = Arc::new(FixedContentCapturePolicy(capture_enabled));
        self
    }

    /// Convert OTLP ResourceSpans to internal Span format.
    pub fn convert_resource_spans(
        resource_spans: ResourceSpans,
        context: &RequestContext,
    ) -> Result<Vec<Span>> {
        let converter = Self::new();
        converter.convert_resource_spans_with(resource_spans, context)
    }

    /// Convert OTLP ResourceSpans using this converter's convention pipeline.
    pub fn convert_resource_spans_with(
        &self,
        resource_spans: ResourceSpans,
        context: &RequestContext,
    ) -> Result<Vec<Span>> {
        let mut spans = Vec::new();

        // Extract resource attributes that apply to every span in the request.
        let resource_attrs = resource_spans.resource.as_ref();
        let service_name = extract_resource_string(resource_attrs, "service.name")
            .unwrap_or_else(|| "unknown".to_string());
        // R4.5: deployment.environment is a resource attribute and propagates
        // to every span in the request. Empty/missing leaves Span.environment
        // at its default empty string.
        let environment =
            extract_resource_string(resource_attrs, "deployment.environment").unwrap_or_default();

        // Process each scope (instrumentation library)
        for scope_spans in resource_spans.scope_spans {
            // Process each span
            for otlp_span in scope_spans.spans {
                let span = self.convert_span(otlp_span, &service_name, &environment, context)?;
                spans.push(span);
            }
        }

        Ok(spans)
    }

    /// Convert a single OTLP Span to internal format.
    fn convert_span(
        &self,
        otlp_span: OtlpSpan,
        service_name: &str,
        environment: &str,
        context: &RequestContext,
    ) -> Result<Span> {
        use chrono::Utc;

        // Convert trace_id and span_id from bytes to hex string
        let trace_id = hex::encode(&otlp_span.trace_id);
        let span_id = hex::encode(&otlp_span.span_id);
        // R4.7 (audit G37): parent_span_id of empty bytes OR all-zero bytes
        // must both normalize to the empty string so root spans are
        // recognized regardless of which convention the client used.
        let parent_span_id = normalize_parent_span_id(&otlp_span.parent_span_id);

        // Extract timestamps (nanoseconds since epoch)
        let timestamp = otlp_span.start_time_unix_nano as i64;
        // A not-yet-closed span (`end_time_unix_nano == 0`, the dangling-span
        // shape the Phase 5 reaper keys off) or any `end < start` (clock skew /
        // malformed) must yield `duration_ns == 0` rather than a wrapped u64.
        // `checked_sub` collapses both underflow cases to `None`; we saturate
        // the well-formed case to `i64::MAX` to avoid a lossy cast on huge
        // durations.
        let duration_ns = otlp_span
            .end_time_unix_nano
            .checked_sub(otlp_span.start_time_unix_nano)
            .map_or(0, |d| i64::try_from(d).unwrap_or(i64::MAX));

        // Initialize span with basic fields
        let mut span_data = Span {
            trace_id,
            span_id,
            parent_span_id,
            timestamp,
            duration_ns,
            workspace_id: context.workspace_id.to_string(),
            service_name: service_name.to_string(),
            span_name: otlp_span.name.clone(),
            environment: environment.to_string(),
            ..Default::default()
        };

        // Zero-copy view over the borrowed attribute slice (TECH-SPEC-PHASE-0
        // §4.2b, TECH-SPEC-PHASE-1 §3.6). No HashMap rebuild, no per-attribute
        // string clones during dispatch.
        let view = AttrView::new(&otlp_span.attributes);

        // Dispatch through the convention pipeline. Later conventions
        // overwrite earlier ones on field conflicts — see priority order in
        // `conventions::default_conventions`.
        for convention in &self.conventions {
            convention.apply(&view, &mut span_data);
        }

        // Process span events per the R1.6 allowlist:
        //   - gen_ai.content.prompt / .completion → llm_input / llm_output
        //   - gen_ai.client.inference.operation.details → llm_input / llm_output (1.29)
        //   - nat.event_type=*_NEW_TOKEN → dropped
        //   - everything else → preserved in events JSON column
        // Events are processed AFTER attribute conventions so attribute-level
        // values (higher priority) already set llm_input/llm_output first.
        apply_span_events(&otlp_span.events, &mut span_data);

        // Phase 4 R4.6: preserve OTLP span links as JSON. Capped at 128 per
        // OQ27 with a truncation marker on overflow.
        apply_span_links(&otlp_span.links, &mut span_data);

        // Build the JSON attribute catch-all from the borrowed slice.
        //
        // NOTE (Phase 0 PR3): we currently mirror EVERY attribute into the
        // JSON column, matching the pre-refactor behavior at
        // `converter.rs:86-96`. The `AttrView` tracks consumed keys via
        // `mark_consumed`, but Phase 1 will be the one to actually drop
        // already-mapped keys from the JSON blob — that's a semantic change
        // out of scope for this pure-refactor PR.
        let mut attributes_map = serde_json::Map::new();
        for attr in &otlp_span.attributes {
            if let Some(value) = &attr.value {
                attributes_map.insert(attr.key.clone(), any_value_to_json(value));
            }
        }

        // Detect span type from attributes
        use std::collections::HashMap;
        let attributes_hashmap: HashMap<String, serde_json::Value> = attributes_map
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        span_data.span_type = crate::SpanTypeMapper::detect_type(
            &attributes_hashmap,
            duration_ns,
            &span_data.span_name,
            &span_data.service_name,
        );

        // Determine span kind
        span_data.span_kind = match otlp_span.kind {
            1 => "INTERNAL",
            2 => "SERVER",
            3 => "CLIENT",
            4 => "PRODUCER",
            5 => "CONSUMER",
            _ => "UNSPECIFIED",
        }
        .to_string();

        // Extract status
        if let Some(status) = otlp_span.status {
            span_data.status_code = match status.code {
                0 => "UNSET",
                1 => "OK",
                2 => "ERROR",
                _ => "UNSET",
            }
            .to_string();
            span_data.status_message = status.message;
        }

        // Store all attributes as JSON
        span_data.attributes = serde_json::to_string(&attributes_map)?;

        // Apply content capture policy: when disabled, strip all prompt/completion
        // content from every storage location so it cannot leak via any column.
        if let Ok(workspace_id) = span_data.workspace_id.parse::<uuid::Uuid>()
            && !self
                .content_capture_policy
                .capture_enabled(workspace_id.into())
        {
            // Clear first-class columns.
            span_data.llm_input = String::new();
            span_data.llm_output = String::new();

            // Strip gen_ai.content.* / llm.input / llm.output from attributes JSON.
            if let Ok(mut attrs) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
                &span_data.attributes,
            ) {
                attrs.retain(|k, _| {
                    !k.starts_with("gen_ai.content.")
                        && k != "llm.input"
                        && k != "llm.output"
                        && k != "gen_ai.prompt"
                        && k != "gen_ai.completion"
                });
                span_data.attributes =
                    serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
            }

            // Strip gen_ai.content.prompt / gen_ai.content.completion events
            // from the events JSON so they cannot leak via the events column.
            if let Ok(mut events) =
                serde_json::from_str::<Vec<serde_json::Value>>(&span_data.events)
            {
                events.retain(|e| {
                    e["name"]
                        .as_str()
                        .map(|n| {
                            n != "gen_ai.content.prompt"
                                && n != "gen_ai.content.completion"
                                && n != "gen_ai.client.inference.operation.details"
                        })
                        .unwrap_or(true)
                });
                span_data.events =
                    serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string());
            }
        }

        // Set timestamps
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        span_data.created_at = now;
        span_data.updated_at = now;

        Ok(span_data)
    }
}

/// Extract a `StringValue` resource attribute by key.
///
/// Returns `None` when the resource is absent, the key isn't present, the
/// value is non-string, or the string is empty.
pub(crate) fn extract_resource_string(
    resource: Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
    key: &str,
) -> Option<String> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyVal;
    resource
        .and_then(|r| r.attributes.iter().find(|attr| attr.key == key))
        .and_then(|attr| attr.value.as_ref())
        .and_then(|v| match &v.value {
            Some(AnyVal::StringValue(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        })
}

/// Normalize an OTLP `parent_span_id` byte slice to a canonical hex string.
///
/// Per R4.7 / audit G37, two encodings of "no parent" exist in the wild:
///   * empty bytes (`[]`) — the OTel SDK convention for root spans
///   * eight zero bytes (`[0u8; 8]`) — sent by some collectors / forwarders
///
/// Both must collapse to the empty string so the SQL root-span predicate
/// (`WHERE parent_span_id = ''`) catches every root regardless of which
/// client convention produced the wire payload.
///
/// Any other byte pattern is preserved as a lowercase hex string.
pub(crate) fn normalize_parent_span_id(parent: &[u8]) -> String {
    if parent.is_empty() || parent.iter().all(|b| *b == 0) {
        String::new()
    } else {
        hex::encode(parent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use zradar_traits::ContentCapturePolicy;

    struct AlwaysDisableCapture;
    impl ContentCapturePolicy for AlwaysDisableCapture {
        fn capture_enabled(&self, _workspace_id: WorkspaceId) -> bool {
            false
        }
    }

    #[test]
    fn test_content_capture_disabled_clears_llm_fields() {
        let mut span = Span {
            workspace_id: WorkspaceId::new().to_string(),
            llm_input: "secret prompt".to_string(),
            llm_output: "secret completion".to_string(),
            attributes: r#"{"gen_ai.content.prompt":"secret","other":"keep"}"#.to_string(),
            events: r#"[{"name":"gen_ai.content.prompt"},{"name":"exception"}]"#.to_string(),
            ..Span::default()
        };

        let converter =
            OtlpConverter::new().with_content_capture_policy(Arc::new(AlwaysDisableCapture));

        // Simulate the content-capture block from convert_span.
        if let Ok(workspace_id) = span.workspace_id.parse::<Uuid>()
            && !converter
                .content_capture_policy
                .capture_enabled(workspace_id.into())
        {
            span.llm_input = String::new();
            span.llm_output = String::new();

            if let Ok(mut attrs) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&span.attributes)
            {
                attrs.retain(|k, _| {
                    !k.starts_with("gen_ai.content.")
                        && k != "llm.input"
                        && k != "llm.output"
                        && k != "gen_ai.prompt"
                        && k != "gen_ai.completion"
                });
                span.attributes =
                    serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
            }

            if let Ok(mut events) = serde_json::from_str::<Vec<serde_json::Value>>(&span.events) {
                events.retain(|e| {
                    e["name"]
                        .as_str()
                        .map(|n| {
                            n != "gen_ai.content.prompt"
                                && n != "gen_ai.content.completion"
                                && n != "gen_ai.client.inference.operation.details"
                        })
                        .unwrap_or(true)
                });
                span.events = serde_json::to_string(&events).unwrap_or_else(|_| "[]".to_string());
            }
        }

        assert!(span.llm_input.is_empty(), "llm_input must be cleared");
        assert!(span.llm_output.is_empty(), "llm_output must be cleared");

        let attrs: serde_json::Value = serde_json::from_str(&span.attributes).unwrap();
        assert!(
            attrs.get("gen_ai.content.prompt").is_none(),
            "gen_ai.content.prompt must be stripped from attributes"
        );
        assert_eq!(
            attrs.get("other").and_then(|v| v.as_str()),
            Some("keep"),
            "non-content attributes must survive"
        );

        let events: Vec<serde_json::Value> = serde_json::from_str(&span.events).unwrap();
        assert!(
            events.iter().all(|e| e["name"] != "gen_ai.content.prompt"),
            "gen_ai.content.prompt event must be stripped from events JSON"
        );
        assert!(
            events.iter().any(|e| e["name"] == "exception"),
            "non-content events must survive"
        );
    }

    // -----------------------------------------------------------------------
    // R4.7 / G37: parent_span_id normalization
    // -----------------------------------------------------------------------

    #[test]
    fn test_normalize_parent_span_id_empty_bytes_returns_empty_string() {
        assert_eq!(normalize_parent_span_id(&[]), "");
    }

    #[test]
    fn test_normalize_parent_span_id_eight_zero_bytes_returns_empty_string() {
        // Some collectors emit eight zero bytes instead of an empty slice when
        // there is no parent. Both must collapse to "" so the root-span SQL
        // predicate matches.
        assert_eq!(normalize_parent_span_id(&[0u8; 8]), "");
    }

    #[test]
    fn test_normalize_parent_span_id_arbitrary_length_all_zero_returns_empty() {
        // Be tolerant of forwarders that pad to 16 bytes — still "no parent".
        assert_eq!(normalize_parent_span_id(&[0u8; 16]), "");
        assert_eq!(normalize_parent_span_id(&[0u8; 1]), "");
    }

    #[test]
    fn test_normalize_parent_span_id_real_parent_hex_encoded() {
        // A non-zero parent must encode to lowercase hex.
        let bytes = [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(normalize_parent_span_id(&bytes), "deadbeef01020304");
    }

    #[test]
    fn test_normalize_parent_span_id_one_nonzero_byte_is_not_root() {
        // Even with seven zeros plus one nonzero byte, this is a real id.
        let bytes = [0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(normalize_parent_span_id(&bytes), "0000000000000001");
    }

    // -----------------------------------------------------------------------
    // duration_ns guard — dangling / clock-skewed spans must not wrap
    // -----------------------------------------------------------------------

    fn ctx() -> RequestContext {
        RequestContext {
            workspace_id: WorkspaceId::new(),
        }
    }

    fn span_with_times(start: u64, end: u64) -> OtlpSpan {
        OtlpSpan {
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            ..Default::default()
        }
    }

    #[test]
    fn test_duration_ns_normal_span_is_positive() {
        let converter = OtlpConverter::new();
        let span = converter
            .convert_span(span_with_times(1_000, 5_000), "svc", "", &ctx())
            .unwrap();
        assert_eq!(span.duration_ns, 4_000);
    }

    #[test]
    fn test_duration_ns_dangling_span_end_zero_is_zero() {
        // The exact Issue #1030 / Phase 5 reaper shape: start set, end never
        // arrived (0). Must be 0, not a wrapped u64.
        let converter = OtlpConverter::new();
        let span = converter
            .convert_span(span_with_times(1_000, 0), "svc", "", &ctx())
            .unwrap();
        assert_eq!(
            span.duration_ns, 0,
            "an unclosed span (end=0) must report duration 0, not a wrapped value"
        );
    }

    #[test]
    fn test_duration_ns_end_before_start_is_zero() {
        // Clock skew / malformed: end < start must clamp to 0, not underflow.
        let converter = OtlpConverter::new();
        let span = converter
            .convert_span(span_with_times(5_000, 1_000), "svc", "", &ctx())
            .unwrap();
        assert_eq!(span.duration_ns, 0);
    }

    // -----------------------------------------------------------------------
    // R4.5: extract_resource_string for deployment.environment
    // -----------------------------------------------------------------------

    fn make_resource(
        attrs: Vec<(&str, &str)>,
    ) -> opentelemetry_proto::tonic::resource::v1::Resource {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
        opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: attrs
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(v.to_string())),
                    }),
                })
                .collect(),
            dropped_attributes_count: 0,
        }
    }

    #[test]
    fn test_extract_resource_string_present() {
        let r = make_resource(vec![
            ("service.name", "agent"),
            ("deployment.environment", "production"),
        ]);
        assert_eq!(
            extract_resource_string(Some(&r), "deployment.environment"),
            Some("production".to_string())
        );
    }

    #[test]
    fn test_extract_resource_string_absent_returns_none() {
        let r = make_resource(vec![("service.name", "agent")]);
        assert_eq!(
            extract_resource_string(Some(&r), "deployment.environment"),
            None
        );
    }

    #[test]
    fn test_extract_resource_string_no_resource_returns_none() {
        assert_eq!(
            extract_resource_string(None, "deployment.environment"),
            None
        );
    }

    #[test]
    fn test_extract_resource_string_empty_value_returns_none() {
        let r = make_resource(vec![("deployment.environment", "")]);
        assert_eq!(
            extract_resource_string(Some(&r), "deployment.environment"),
            None,
            "empty string must be treated as absent"
        );
    }

    #[test]
    fn test_extract_resource_string_non_string_returns_none() {
        use opentelemetry_proto::tonic::common::v1::any_value::Value;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
        let r = opentelemetry_proto::tonic::resource::v1::Resource {
            attributes: vec![KeyValue {
                key: "deployment.environment".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(42)),
                }),
            }],
            dropped_attributes_count: 0,
        };
        assert_eq!(
            extract_resource_string(Some(&r), "deployment.environment"),
            None,
            "non-string typed value must not be coerced"
        );
    }
}
