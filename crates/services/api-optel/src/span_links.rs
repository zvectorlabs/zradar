//! OTLP span-link processing (Phase 4 R4.6, audit G29).
//!
//! Serializes the OTLP span `links` array into the `Span.links` JSON column
//! so causal relationships between spans (retry chains, cross-conversation
//! references, fan-out parents) survive ingest.
//!
//! Cap order (OQ27): up to `MAX_LINKS_PER_SPAN = 128` links are kept; any
//! excess is dropped and the sentinel object `{"links.truncated": true}` is
//! appended so the UI / downstream consumers know data was elided.

use opentelemetry_proto::tonic::trace::v1::span::Link;
use zradar_models::Span;

use crate::otlp_util::any_value_to_json;

/// Maximum number of links preserved in the `links` JSON column per span
/// (per OQ27 `otlp.links.max_per_span`). Excess is truncated with a marker.
pub const MAX_LINKS_PER_SPAN: usize = 128;

/// Serialize OTLP span links into `span.links` as a JSON array.
///
/// Each element has shape:
/// ```json
/// {
///   "trace_id": "<hex>",
///   "span_id": "<hex>",
///   "trace_state": "<opt>",   // omitted when empty
///   "attributes": { ... }     // omitted when empty
/// }
/// ```
///
/// When the link count exceeds `MAX_LINKS_PER_SPAN`, the trailing links are
/// dropped and `{"links.truncated": true}` is appended.
///
/// If the OTLP span has no links, `span.links` is left at its `Default`
/// value of `"[]"`.
pub fn apply_span_links(links: &[Link], span: &mut Span) {
    if links.is_empty() {
        return;
    }

    let mut out: Vec<serde_json::Value> = Vec::with_capacity(links.len().min(MAX_LINKS_PER_SPAN));
    let truncated = links.len() > MAX_LINKS_PER_SPAN;

    for link in links.iter().take(MAX_LINKS_PER_SPAN) {
        out.push(link_to_json(link));
    }

    if truncated {
        out.push(serde_json::json!({"links.truncated": true}));
    }

    span.links = serde_json::to_string(&out).unwrap_or_else(|_| "[]".to_string());
}

fn link_to_json(link: &Link) -> serde_json::Value {
    let mut map = serde_json::Map::with_capacity(4);
    map.insert(
        "trace_id".to_string(),
        serde_json::Value::String(hex::encode(&link.trace_id)),
    );
    map.insert(
        "span_id".to_string(),
        serde_json::Value::String(hex::encode(&link.span_id)),
    );
    if !link.trace_state.is_empty() {
        map.insert(
            "trace_state".to_string(),
            serde_json::Value::String(link.trace_state.clone()),
        );
    }
    if !link.attributes.is_empty() {
        let mut attrs = serde_json::Map::with_capacity(link.attributes.len());
        for kv in &link.attributes {
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
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyVal;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    fn kv_str(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(AnyVal::StringValue(v.to_string())),
            }),
            ..Default::default()
        }
    }

    fn make_link(trace_id: [u8; 16], span_id: [u8; 8], attrs: Vec<KeyValue>) -> Link {
        Link {
            trace_id: trace_id.to_vec(),
            span_id: span_id.to_vec(),
            trace_state: String::new(),
            attributes: attrs,
            dropped_attributes_count: 0,
            ..Default::default()
        }
    }

    #[test]
    fn test_empty_links_leaves_default() {
        let mut span = Span::default();
        apply_span_links(&[], &mut span);
        assert_eq!(
            span.links, "[]",
            "default must remain when no links present"
        );
    }

    #[test]
    fn test_single_link_serializes_trace_and_span_id_as_hex() {
        let trace_id = [0xAA; 16];
        let span_id = [0xBB; 8];
        let link = make_link(trace_id, span_id, vec![]);
        let mut span = Span::default();
        apply_span_links(&[link], &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.links).unwrap();
        let arr = parsed.as_array().expect("links must serialize as array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["trace_id"], "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(arr[0]["span_id"], "bbbbbbbbbbbbbbbb");
        // Empty trace_state / attributes are omitted, not stored as null.
        assert!(arr[0].get("trace_state").is_none());
        assert!(arr[0].get("attributes").is_none());
    }

    #[test]
    fn test_link_attributes_propagate_into_json() {
        let link = make_link(
            [0x01; 16],
            [0x02; 8],
            vec![
                kv_str("reason", "retry"),
                kv_str("source", "agent_supervisor"),
            ],
        );
        let mut span = Span::default();
        apply_span_links(&[link], &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.links).unwrap();
        let attrs = &parsed[0]["attributes"];
        assert_eq!(attrs["reason"], "retry");
        assert_eq!(attrs["source"], "agent_supervisor");
    }

    #[test]
    fn test_link_trace_state_present_when_non_empty() {
        let mut link = make_link([0x01; 16], [0x02; 8], vec![]);
        link.trace_state = "vendor=opaque".to_string();
        let mut span = Span::default();
        apply_span_links(&[link], &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.links).unwrap();
        assert_eq!(parsed[0]["trace_state"], "vendor=opaque");
    }

    #[test]
    fn test_multiple_links_preserved_in_order() {
        let l1 = make_link([0x11; 16], [0xAA; 8], vec![]);
        let l2 = make_link([0x22; 16], [0xBB; 8], vec![]);
        let l3 = make_link([0x33; 16], [0xCC; 8], vec![]);
        let mut span = Span::default();
        apply_span_links(&[l1, l2, l3], &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.links).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0]["trace_id"], "11111111111111111111111111111111");
        assert_eq!(arr[1]["trace_id"], "22222222222222222222222222222222");
        assert_eq!(arr[2]["trace_id"], "33333333333333333333333333333333");
    }

    /// OQ27: count cap drops trailing links and appends a truncation marker.
    #[test]
    fn test_link_count_cap_appends_truncation_marker() {
        let links: Vec<Link> = (0..(MAX_LINKS_PER_SPAN + 5))
            .map(|i| make_link([i as u8; 16], [i as u8; 8], vec![]))
            .collect();
        assert_eq!(links.len(), MAX_LINKS_PER_SPAN + 5);

        let mut span = Span::default();
        apply_span_links(&links, &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.links).unwrap();
        let arr = parsed.as_array().unwrap();
        // 128 retained + 1 truncation marker = 129 total entries.
        assert_eq!(arr.len(), MAX_LINKS_PER_SPAN + 1);
        let last = arr.last().unwrap();
        assert_eq!(
            last["links.truncated"],
            serde_json::Value::Bool(true),
            "truncation marker must be the final entry when capped"
        );
    }

    /// Exactly `MAX_LINKS_PER_SPAN` links: no truncation marker should be added.
    #[test]
    fn test_exactly_max_links_no_truncation_marker() {
        let links: Vec<Link> = (0..MAX_LINKS_PER_SPAN)
            .map(|i| make_link([i as u8; 16], [i as u8; 8], vec![]))
            .collect();
        let mut span = Span::default();
        apply_span_links(&links, &mut span);

        let parsed: serde_json::Value = serde_json::from_str(&span.links).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), MAX_LINKS_PER_SPAN);
        assert!(
            arr.iter().all(|e| e.get("links.truncated").is_none()),
            "no truncation marker should appear when under the cap"
        );
    }
}
