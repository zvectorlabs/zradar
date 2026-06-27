//! Zero-copy borrowed-attribute view over an OTLP span's `KeyValue` slice.
//!
//! Per TECH-SPEC-PHASE-0.md §4.2b and TECH-SPEC-PHASE-1.md §3.6, `AttrView<'a>`
//! holds a borrow of the OTLP request buffer for the lifetime of conversion.
//! Lookups use a lazily-built `name -> index` map. Conventions call typed
//! accessors (`get_str` / `get_i64` / `get_f64` / `get_u64` / `get_bool`) that
//! return borrowed references into the original buffer — no per-call clones.
//!
//! The view also exposes a `mark_consumed`/`consumed_keys` API. Today the
//! converter still stores every attribute into the JSON column to preserve
//! pre-refactor behavior bit-for-bit; the consumed-key tracking is the
//! Phase 1 slot for future filtering ("only stash un-mapped attrs in JSON").

use std::cell::{OnceCell, RefCell};
use std::collections::{HashMap, HashSet};

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

/// Borrowed alias for an OTLP attribute value. Zero-copy: this is the protobuf
/// type directly, so accessors can return `&'a str` slices into the request
/// buffer without allocating.
pub type AnyValueRef = AnyValue;

/// Zero-copy borrowed view over an OTLP span's attribute list.
///
/// Conventions consume this view to populate `Span` fields. Lookups are lazy:
/// the underlying `name -> index` map is built on first key access via
/// `OnceCell`, so spans whose conventions short-circuit avoid the index cost.
///
/// All accessor methods borrow from the underlying `&'a [KeyValue]` slice and
/// allocate only when a convention itself decides to `.to_string()` a borrowed
/// `&'a str` into an owning `Span` field.
pub struct AttrView<'a> {
    attrs: &'a [KeyValue],
    index: OnceCell<HashMap<&'a str, usize>>,
    consumed: RefCell<HashSet<&'a str>>,
}

impl<'a> AttrView<'a> {
    /// Construct a new view borrowing from the given OTLP attribute slice.
    #[must_use]
    pub fn new(attrs: &'a [KeyValue]) -> Self {
        Self {
            attrs,
            index: OnceCell::new(),
            consumed: RefCell::new(HashSet::new()),
        }
    }

    /// Build (or fetch) the lazy `name -> index` map.
    ///
    /// Borrowed `&'a str` keys point directly into the OTLP request buffer —
    /// no string copies.
    fn index(&self) -> &HashMap<&'a str, usize> {
        self.index.get_or_init(|| {
            let mut map = HashMap::with_capacity(self.attrs.len());
            for (i, kv) in self.attrs.iter().enumerate() {
                map.insert(kv.key.as_str(), i);
            }
            map
        })
    }

    /// Borrow the raw `AnyValue` for `key` if present.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&'a AnyValueRef> {
        let idx = *self.index().get(key)?;
        let kv = self.attrs.get(idx)?;
        kv.value.as_ref()
    }

    /// Borrow `key`'s string value, if present and of `StringValue` kind.
    #[must_use]
    pub fn get_str(&self, key: &str) -> Option<&'a str> {
        match self.get(key)?.value.as_ref()? {
            Value::StringValue(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Borrow `key`'s signed integer value, if present and `IntValue`.
    #[must_use]
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        match self.get(key)?.value.as_ref()? {
            Value::IntValue(i) => Some(*i),
            _ => None,
        }
    }

    /// Borrow `key`'s unsigned integer value (saturating cast of `IntValue`).
    ///
    /// Returns `None` for negative integers so callers can fall back to a
    /// default rather than silently wrapping to a huge `u64`.
    #[must_use]
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        let v = self.get_i64(key)?;
        if v < 0 { None } else { Some(v as u64) }
    }

    /// Borrow `key`'s floating-point value, if present and `DoubleValue`.
    #[must_use]
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        match self.get(key)?.value.as_ref()? {
            Value::DoubleValue(d) => Some(*d),
            _ => None,
        }
    }

    /// Borrow `key`'s boolean value, if present and `BoolValue`.
    #[must_use]
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        match self.get(key)?.value.as_ref()? {
            Value::BoolValue(b) => Some(*b),
            _ => None,
        }
    }

    /// Whether `key` exists in the view (regardless of value kind).
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.index().contains_key(key)
    }

    /// Iterate `(key, value)` pairs in original OTLP order.
    ///
    /// Yielded `&'a str` keys and `&'a AnyValueRef` values borrow from the
    /// underlying OTLP request — no allocations.
    pub fn iter(&self) -> impl Iterator<Item = (&'a str, &'a AnyValueRef)> {
        self.attrs
            .iter()
            .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.as_str(), v)))
    }

    /// Record that `key` was consumed by a convention.
    ///
    /// The stored reference borrows from the OTLP request buffer, so this
    /// allocates only the `HashSet` slot itself (no string copies).
    pub fn mark_consumed(&self, key: &'a str) {
        self.consumed.borrow_mut().insert(key);
    }

    /// Snapshot of keys consumed so far.
    ///
    /// Phase 1 will use this to elide already-mapped attributes from the
    /// JSON catch-all column. Phase 0 keeps the legacy "mirror everything
    /// into JSON" behavior, so this method is currently informational.
    #[must_use]
    pub fn is_consumed(&self, key: &str) -> bool {
        self.consumed.borrow().contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};

    fn kv_str(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(v.to_string())),
            }),
        }
    }

    fn kv_int(k: &str, v: i64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::IntValue(v)),
            }),
        }
    }

    fn kv_bool(k: &str, v: bool) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::BoolValue(v)),
            }),
        }
    }

    fn kv_f64(k: &str, v: f64) -> KeyValue {
        KeyValue {
            key: k.to_string(),
            value: Some(AnyValue {
                value: Some(Value::DoubleValue(v)),
            }),
        }
    }

    #[test]
    fn test_get_str_returns_borrowed_value() {
        let attrs = vec![kv_str("agent.name", "researcher")];
        let view = AttrView::new(&attrs);
        assert_eq!(view.get_str("agent.name"), Some("researcher"));
    }

    #[test]
    fn test_get_str_missing_returns_none() {
        let attrs: Vec<KeyValue> = vec![];
        let view = AttrView::new(&attrs);
        assert_eq!(view.get_str("agent.name"), None);
    }

    #[test]
    fn test_get_i64_and_u64() {
        let attrs = vec![kv_int("tokens", 42), kv_int("neg", -1)];
        let view = AttrView::new(&attrs);
        assert_eq!(view.get_i64("tokens"), Some(42));
        assert_eq!(view.get_u64("tokens"), Some(42));
        assert_eq!(view.get_i64("neg"), Some(-1));
        assert_eq!(view.get_u64("neg"), None);
    }

    #[test]
    fn test_get_f64_and_bool() {
        let attrs = vec![kv_f64("cost", 0.5), kv_bool("flag", true)];
        let view = AttrView::new(&attrs);
        assert_eq!(view.get_f64("cost"), Some(0.5));
        assert_eq!(view.get_bool("flag"), Some(true));
    }

    #[test]
    fn test_wrong_type_returns_none() {
        let attrs = vec![kv_int("x", 1)];
        let view = AttrView::new(&attrs);
        assert_eq!(view.get_str("x"), None);
        assert_eq!(view.get_bool("x"), None);
        assert_eq!(view.get_f64("x"), None);
    }

    #[test]
    fn test_contains_and_iter() {
        let attrs = vec![kv_str("a", "1"), kv_int("b", 2)];
        let view = AttrView::new(&attrs);
        assert!(view.contains("a"));
        assert!(view.contains("b"));
        assert!(!view.contains("c"));
        let collected: Vec<&str> = view.iter().map(|(k, _)| k).collect();
        assert_eq!(collected, vec!["a", "b"]);
    }

    #[test]
    fn test_mark_consumed_tracks_keys() {
        let attrs = vec![kv_str("agent.name", "x"), kv_str("user_id", "u")];
        let view = AttrView::new(&attrs);
        assert!(!view.is_consumed("agent.name"));
        // Need to grab a borrowed str with the right lifetime — fetch via index.
        for (k, _) in view.iter() {
            if k == "agent.name" {
                view.mark_consumed(k);
            }
        }
        assert!(view.is_consumed("agent.name"));
        assert!(!view.is_consumed("user_id"));
    }
}
