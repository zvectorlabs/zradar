//! M07-05: Shared DataFusion session engine.
//!
//! `SharedEngine` holds a pre-configured [`SessionConfig`] that is reused
//! across every Parquet query.  Creating a new [`SessionContext`] from a
//! pre-built config is cheap (no function/optimizer registration overhead) and
//! avoids the table-catalog state collision that would arise if a single
//! long-lived context were shared across concurrent queries.
//!
//! The real query-planning win comes from the companion change in `reader.rs`:
//! replacing the N-file `UNION ALL` view with a single DataFusion
//! `ListingTable` over all file URLs, which reduces optimizer complexity from
//! O(N × rules) to O(rules).

use datafusion::prelude::{SessionConfig, SessionContext};

/// Factory for pre-configured DataFusion `SessionContext` instances.
///
/// Create one `SharedEngine` at server startup and clone the inner `Arc` into
/// every component that needs to run Parquet queries.  Call
/// [`SharedEngine::new_context`] for each query — the call is O(1) and thread-
/// safe.
#[derive(Clone)]
pub struct SharedEngine {
    config: SessionConfig,
}

impl Default for SharedEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedEngine {
    /// Create a new engine with zradar-appropriate DataFusion settings.
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        // DataFusion 44 defaults to Utf8View (StringViewArray) for Parquet
        // string columns.  Disable this so callers get plain StringArray which
        // our record_batch_to_* converters expect.
        config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = false;
        Self { config }
    }

    /// Return a fresh `SessionContext` pre-configured for Parquet queries.
    ///
    /// Each call returns an independent context so concurrent queries never
    /// see each other's registered tables.
    pub fn new_context(&self) -> SessionContext {
        SessionContext::new_with_config(self.config.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_engine_new_context_is_independent() {
        let engine = SharedEngine::new();
        let ctx1 = engine.new_context();
        let ctx2 = engine.new_context();
        // Each context has its own catalog — they don't share registered tables.
        // We can't easily inspect this without registering tables, but we can
        // verify that two contexts are separate objects.
        assert!(!std::ptr::eq(&ctx1 as *const _, &ctx2 as *const _));
    }

    #[test]
    fn test_shared_engine_clone() {
        let engine = SharedEngine::new();
        let _cloned = engine.clone();
    }

    #[tokio::test]
    async fn test_shared_engine_contexts_do_not_share_table_registrations() {
        use std::sync::Arc;

        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;

        let engine = SharedEngine::new();
        let ctx1 = engine.new_context();
        let ctx2 = engine.new_context();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx1.register_table("shared_engine_test", Arc::new(table))
            .unwrap();

        let result = ctx2.sql("SELECT value FROM shared_engine_test").await;
        assert!(result.is_err());
    }
}
