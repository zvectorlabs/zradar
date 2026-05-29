use std::sync::Arc;

use crate::enforcer::DefaultPolicyEnforcer;
use crate::traits::{
    DecisionAuditSink, PolicyEnforcer, PolicyStore, ThresholdSink, UsageReader, UsageTracker,
};

#[derive(Clone)]
pub struct PolicyEngine {
    pub store: Arc<dyn PolicyStore>,
    pub usage_reader: Arc<dyn UsageReader>,
    pub usage_tracker: Arc<dyn UsageTracker>,
    pub enforcer: Arc<dyn PolicyEnforcer>,
    pub threshold_sink: Arc<dyn ThresholdSink>,
}

impl PolicyEngine {
    pub fn new(
        store: Arc<dyn PolicyStore>,
        usage_reader: Arc<dyn UsageReader>,
        usage_tracker: Arc<dyn UsageTracker>,
        threshold_sink: Arc<dyn ThresholdSink>,
    ) -> Self {
        let enforcer: Arc<dyn PolicyEnforcer> = Arc::new(DefaultPolicyEnforcer::new(
            store.clone(),
            usage_reader.clone(),
            threshold_sink.clone(),
        ));

        Self {
            store,
            usage_reader,
            usage_tracker,
            enforcer,
            threshold_sink,
        }
    }

    pub fn with_enforcer(
        store: Arc<dyn PolicyStore>,
        usage_reader: Arc<dyn UsageReader>,
        usage_tracker: Arc<dyn UsageTracker>,
        threshold_sink: Arc<dyn ThresholdSink>,
        enforcer: Arc<dyn PolicyEnforcer>,
    ) -> Self {
        Self {
            store,
            usage_reader,
            usage_tracker,
            enforcer,
            threshold_sink,
        }
    }

    pub fn new_with_decision_audit(
        store: Arc<dyn PolicyStore>,
        usage_reader: Arc<dyn UsageReader>,
        usage_tracker: Arc<dyn UsageTracker>,
        threshold_sink: Arc<dyn ThresholdSink>,
        decision_audit_sink: Arc<dyn DecisionAuditSink>,
    ) -> Self {
        let enforcer: Arc<dyn PolicyEnforcer> = Arc::new(
            DefaultPolicyEnforcer::new(store.clone(), usage_reader.clone(), threshold_sink.clone())
                .with_decision_audit_sink(decision_audit_sink),
        );

        Self {
            store,
            usage_reader,
            usage_tracker,
            enforcer,
            threshold_sink,
        }
    }
}
