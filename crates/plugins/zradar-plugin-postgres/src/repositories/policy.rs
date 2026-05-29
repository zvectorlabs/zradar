use crate::client::PostgresClient;
use async_trait::async_trait;
use parking_lot::RwLock;
use serde_json::Value;
use sqlx::FromRow;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;
use zradar_policy::{
    Operation, Policy, PolicyError, PolicyId, PolicyLimit, PolicySource, PolicyStore,
    ResolvedPolicy, SignalKind,
};

#[derive(Debug, FromRow)]
struct PolicyRow {
    id: i64,
    tenant_id: Uuid,
    project_id: Option<Uuid>,
    signal_kind: String,
    operation: String,
    limit_json: Value,
    grace_pct: i16,
    hard_block_pct: i16,
    effective_from: i64,
    effective_until: Option<i64>,
    source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PolicyCacheKey {
    tenant_id: Uuid,
    project_id: Option<Uuid>,
    signal: SignalKind,
    operation: Operation,
    limit_kind: &'static str,
}

type PolicyCacheMap = HashMap<PolicyCacheKey, Policy>;

#[derive(Debug, Default)]
struct PolicyCache {
    inner: RwLock<Arc<PolicyCacheMap>>,
}

impl PolicyCache {
    fn replace(&self, cache: PolicyCacheMap) {
        *self.inner.write() = Arc::new(cache);
    }

    fn snapshot(&self) -> Arc<PolicyCacheMap> {
        self.inner.read().clone()
    }
}

pub struct PostgresPolicyStore {
    client: Arc<PostgresClient>,
    cache: PolicyCache,
}

impl PostgresPolicyStore {
    pub fn new(client: Arc<PostgresClient>) -> Self {
        Self {
            client,
            cache: PolicyCache::default(),
        }
    }

    pub async fn refresh(&self) -> Result<(), PolicyError> {
        let rows = sqlx::query_as::<_, PolicyRow>(
            r#"
            SELECT
                id, tenant_id, project_id, signal_kind, operation, limit_json,
                grace_pct, hard_block_pct, effective_from, effective_until, source
            FROM policies
            WHERE effective_from <= $1
              AND (effective_until IS NULL OR effective_until > $1)
            ORDER BY updated_at DESC
            "#,
        )
        .bind(chrono::Utc::now().timestamp_micros())
        .fetch_all(self.client.pool())
        .await
        .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;

        self.cache.replace(rows_to_cache(rows)?);

        Ok(())
    }
}

#[async_trait]
impl PolicyStore for PostgresPolicyStore {
    async fn upsert(&self, policy: Policy) -> Result<(), PolicyError> {
        let limit_kind = limit_kind(&policy.limit);
        let limit_json =
            serde_json::to_value(&policy.limit).map_err(|e| PolicyError::Invalid(e.to_string()))?;
        let now = chrono::Utc::now().timestamp_micros();

        if policy.project_id.is_some() {
            sqlx::query(
                r#"
                INSERT INTO policies (
                    tenant_id, project_id, signal_kind, operation, limit_kind,
                    limit_json, grace_pct, hard_block_pct, effective_from,
                    effective_until, source, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (tenant_id, project_id, signal_kind, operation, limit_kind)
                WHERE project_id IS NOT NULL AND effective_until IS NULL
                DO UPDATE SET
                    limit_json = EXCLUDED.limit_json,
                    grace_pct = EXCLUDED.grace_pct,
                    hard_block_pct = EXCLUDED.hard_block_pct,
                    effective_from = EXCLUDED.effective_from,
                    effective_until = EXCLUDED.effective_until,
                    source = EXCLUDED.source,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(policy.tenant_id)
            .bind(policy.project_id)
            .bind(signal_kind(policy.signal))
            .bind(operation(policy.operation))
            .bind(limit_kind)
            .bind(limit_json)
            .bind(i16::from(policy.grace_pct))
            .bind(i16::from(policy.hard_block_pct))
            .bind(policy.effective_from)
            .bind(policy.effective_until)
            .bind(policy_source(policy.source))
            .bind(now)
            .execute(self.client.pool())
            .await
            .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO policies (
                    tenant_id, project_id, signal_kind, operation, limit_kind,
                    limit_json, grace_pct, hard_block_pct, effective_from,
                    effective_until, source, updated_at
                ) VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (tenant_id, signal_kind, operation, limit_kind)
                WHERE project_id IS NULL AND effective_until IS NULL
                DO UPDATE SET
                    limit_json = EXCLUDED.limit_json,
                    grace_pct = EXCLUDED.grace_pct,
                    hard_block_pct = EXCLUDED.hard_block_pct,
                    effective_from = EXCLUDED.effective_from,
                    effective_until = EXCLUDED.effective_until,
                    source = EXCLUDED.source,
                    updated_at = EXCLUDED.updated_at
                "#,
            )
            .bind(policy.tenant_id)
            .bind(signal_kind(policy.signal))
            .bind(operation(policy.operation))
            .bind(limit_kind)
            .bind(limit_json)
            .bind(i16::from(policy.grace_pct))
            .bind(i16::from(policy.hard_block_pct))
            .bind(policy.effective_from)
            .bind(policy.effective_until)
            .bind(policy_source(policy.source))
            .bind(now)
            .execute(self.client.pool())
            .await
            .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;
        }

        self.refresh().await
    }

    async fn upsert_many(&self, policies: Vec<Policy>) -> Result<(), PolicyError> {
        if policies.is_empty() {
            return Ok(());
        }

        let mut prepared = Vec::with_capacity(policies.len());
        for policy in policies {
            let limit_json = serde_json::to_value(&policy.limit)
                .map_err(|e| PolicyError::Invalid(e.to_string()))?;
            prepared.push((policy, limit_json));
        }

        let now = chrono::Utc::now().timestamp_micros();
        let mut tx = self
            .client
            .pool()
            .begin()
            .await
            .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;

        for (policy, limit_json) in prepared {
            let limit_kind = limit_kind(&policy.limit);
            if policy.project_id.is_some() {
                sqlx::query(
                    r#"
                    INSERT INTO policies (
                        tenant_id, project_id, signal_kind, operation, limit_kind,
                        limit_json, grace_pct, hard_block_pct, effective_from,
                        effective_until, source, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (tenant_id, project_id, signal_kind, operation, limit_kind)
                    WHERE project_id IS NOT NULL AND effective_until IS NULL
                    DO UPDATE SET
                        limit_json = EXCLUDED.limit_json,
                        grace_pct = EXCLUDED.grace_pct,
                        hard_block_pct = EXCLUDED.hard_block_pct,
                        effective_from = EXCLUDED.effective_from,
                        effective_until = EXCLUDED.effective_until,
                        source = EXCLUDED.source,
                        updated_at = EXCLUDED.updated_at
                    "#,
                )
                .bind(policy.tenant_id)
                .bind(policy.project_id)
                .bind(signal_kind(policy.signal))
                .bind(operation(policy.operation))
                .bind(limit_kind)
                .bind(limit_json)
                .bind(i16::from(policy.grace_pct))
                .bind(i16::from(policy.hard_block_pct))
                .bind(policy.effective_from)
                .bind(policy.effective_until)
                .bind(policy_source(policy.source))
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;
            } else {
                sqlx::query(
                    r#"
                    INSERT INTO policies (
                        tenant_id, project_id, signal_kind, operation, limit_kind,
                        limit_json, grace_pct, hard_block_pct, effective_from,
                        effective_until, source, updated_at
                    ) VALUES ($1, NULL, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (tenant_id, signal_kind, operation, limit_kind)
                    WHERE project_id IS NULL AND effective_until IS NULL
                    DO UPDATE SET
                        limit_json = EXCLUDED.limit_json,
                        grace_pct = EXCLUDED.grace_pct,
                        hard_block_pct = EXCLUDED.hard_block_pct,
                        effective_from = EXCLUDED.effective_from,
                        effective_until = EXCLUDED.effective_until,
                        source = EXCLUDED.source,
                        updated_at = EXCLUDED.updated_at
                    "#,
                )
                .bind(policy.tenant_id)
                .bind(signal_kind(policy.signal))
                .bind(operation(policy.operation))
                .bind(limit_kind)
                .bind(limit_json)
                .bind(i16::from(policy.grace_pct))
                .bind(i16::from(policy.hard_block_pct))
                .bind(policy.effective_from)
                .bind(policy.effective_until)
                .bind(policy_source(policy.source))
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;

        self.refresh().await
    }

    async fn delete(&self, id: PolicyId) -> Result<(), PolicyError> {
        sqlx::query("DELETE FROM policies WHERE id = $1")
            .bind(id.0)
            .execute(self.client.pool())
            .await
            .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;

        self.refresh().await
    }

    async fn list(&self, tenant_id: Uuid) -> Result<Vec<Policy>, PolicyError> {
        let rows = sqlx::query_as::<_, PolicyRow>(
            r#"
            SELECT
                id, tenant_id, project_id, signal_kind, operation, limit_json,
                grace_pct, hard_block_pct, effective_from, effective_until, source
            FROM policies
            WHERE tenant_id = $1
            ORDER BY updated_at DESC
            "#,
        )
        .bind(tenant_id)
        .fetch_all(self.client.pool())
        .await
        .map_err(|e| PolicyError::StoreUnavailable(e.to_string()))?;

        rows.into_iter().map(row_to_policy).collect()
    }

    fn resolve(
        &self,
        tenant_id: Uuid,
        project_id: Uuid,
        signal: SignalKind,
        operation: Operation,
    ) -> ResolvedPolicy {
        resolve_cached_policy(
            &self.cache.snapshot(),
            tenant_id,
            project_id,
            signal,
            operation,
        )
    }
}

fn rows_to_cache(rows: Vec<PolicyRow>) -> Result<PolicyCacheMap, PolicyError> {
    let mut cache = HashMap::with_capacity(rows.len());
    for row in rows {
        let policy = row_to_policy(row)?;
        cache.entry(policy_key(&policy)).or_insert(policy);
    }
    Ok(cache)
}

fn resolve_cached_policy(
    cache: &PolicyCacheMap,
    tenant_id: Uuid,
    project_id: Uuid,
    signal: SignalKind,
    operation: Operation,
) -> ResolvedPolicy {
    let mut resolved = ResolvedPolicy::default();
    let scopes = [Some(project_id), None];
    let signals = [signal, SignalKind::All];
    let operations = [operation, Operation::All];

    for project_scope in scopes {
        for candidate_signal in signals {
            for candidate_operation in operations {
                apply_cached_policy(
                    cache,
                    &mut resolved,
                    tenant_id,
                    project_scope,
                    candidate_signal,
                    candidate_operation,
                );
            }
        }
    }

    resolved
}

fn apply_cached_policy(
    cache: &PolicyCacheMap,
    resolved: &mut ResolvedPolicy,
    tenant_id: Uuid,
    project_id: Option<Uuid>,
    signal: SignalKind,
    operation: Operation,
) {
    for limit_kind in ["rate", "size", "retention", "window"] {
        let key = PolicyCacheKey {
            tenant_id,
            project_id,
            signal,
            operation,
            limit_kind,
        };
        if let Some(policy) = cache.get(&key) {
            merge_policy(resolved, policy);
        }
    }

    let quota_key = PolicyCacheKey {
        tenant_id,
        project_id,
        signal,
        operation,
        limit_kind: "quota",
    };
    if let Some(policy) = cache.get(&quota_key) {
        merge_policy(resolved, policy);
    }
}

fn merge_policy(resolved: &mut ResolvedPolicy, policy: &Policy) {
    resolved.grace_pct = resolved.grace_pct.min(policy.grace_pct);
    resolved.hard_block_pct = resolved.hard_block_pct.min(policy.hard_block_pct);

    match &policy.limit {
        PolicyLimit::Rate { .. } => {
            resolved.rate = choose_rate(resolved.rate.take(), policy.limit.clone());
        }
        PolicyLimit::Quota { .. } => resolved.quotas.push(policy.limit.clone()),
        PolicyLimit::Size { .. } => {
            resolved.size = choose_lowest_bytes(resolved.size.take(), policy.limit.clone());
        }
        PolicyLimit::Retention { .. } => {
            resolved.retention =
                choose_lowest_days(resolved.retention.take(), policy.limit.clone());
        }
        PolicyLimit::Window { .. } => {
            resolved.query_window =
                choose_lowest_days(resolved.query_window.take(), policy.limit.clone());
        }
    }
}

fn choose_rate(current: Option<PolicyLimit>, candidate: PolicyLimit) -> Option<PolicyLimit> {
    match (current, candidate) {
        (
            Some(PolicyLimit::Rate {
                records_per_sec: current_records,
                bytes_per_sec: current_bytes,
            }),
            PolicyLimit::Rate {
                records_per_sec,
                bytes_per_sec,
            },
        ) => Some(PolicyLimit::Rate {
            records_per_sec: min_option(current_records, records_per_sec),
            bytes_per_sec: min_option(current_bytes, bytes_per_sec),
        }),
        (None, candidate) => Some(candidate),
        (Some(current), _) => Some(current),
    }
}

fn choose_lowest_bytes(
    current: Option<PolicyLimit>,
    candidate: PolicyLimit,
) -> Option<PolicyLimit> {
    match (current, candidate) {
        (
            Some(PolicyLimit::Size {
                max_bytes: current_bytes,
                basis,
            }),
            PolicyLimit::Size {
                max_bytes,
                basis: _,
            },
        ) if max_bytes < current_bytes => Some(PolicyLimit::Size { max_bytes, basis }),
        (None, candidate) => Some(candidate),
        (Some(current), _) => Some(current),
    }
}

fn choose_lowest_days(current: Option<PolicyLimit>, candidate: PolicyLimit) -> Option<PolicyLimit> {
    match (current, candidate) {
        (
            Some(PolicyLimit::Retention {
                max_days: current_days,
            }),
            PolicyLimit::Retention { max_days },
        ) if max_days < current_days => Some(PolicyLimit::Retention { max_days }),
        (
            Some(PolicyLimit::Window {
                max_query_days: current_days,
            }),
            PolicyLimit::Window { max_query_days },
        ) if max_query_days < current_days => Some(PolicyLimit::Window { max_query_days }),
        (None, candidate) => Some(candidate),
        (Some(current), _) => Some(current),
    }
}

fn min_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn row_to_policy(row: PolicyRow) -> Result<Policy, PolicyError> {
    Ok(Policy {
        id: Some(PolicyId(row.id)),
        tenant_id: row.tenant_id,
        project_id: row.project_id,
        signal: parse_signal_kind(&row.signal_kind)?,
        operation: parse_operation(&row.operation)?,
        limit: serde_json::from_value(row.limit_json)
            .map_err(|e| PolicyError::Invalid(e.to_string()))?,
        grace_pct: u8::try_from(row.grace_pct)
            .map_err(|_| PolicyError::Invalid("invalid grace_pct".to_string()))?,
        hard_block_pct: u8::try_from(row.hard_block_pct)
            .map_err(|_| PolicyError::Invalid("invalid hard_block_pct".to_string()))?,
        effective_from: row.effective_from,
        effective_until: row.effective_until,
        source: parse_policy_source(&row.source)?,
    })
}

fn policy_key(policy: &Policy) -> PolicyCacheKey {
    PolicyCacheKey {
        tenant_id: policy.tenant_id,
        project_id: policy.project_id,
        signal: policy.signal,
        operation: policy.operation,
        limit_kind: limit_kind(&policy.limit),
    }
}

fn limit_kind(limit: &PolicyLimit) -> &'static str {
    match limit {
        PolicyLimit::Rate { .. } => "rate",
        PolicyLimit::Quota { .. } => "quota",
        PolicyLimit::Size { .. } => "size",
        PolicyLimit::Retention { .. } => "retention",
        PolicyLimit::Window { .. } => "window",
    }
}

fn signal_kind(signal: SignalKind) -> &'static str {
    match signal {
        SignalKind::Traces => "traces",
        SignalKind::Logs => "logs",
        SignalKind::Metrics => "metrics",
        SignalKind::Rum => "rum",
        SignalKind::SessionReplay => "session_replay",
        SignalKind::ErrorTracking => "error_tracking",
        SignalKind::All => "all",
    }
}

fn operation(operation: Operation) -> &'static str {
    match operation {
        Operation::Ingest => "ingest",
        Operation::Query => "query",
        Operation::Store => "store",
        Operation::All => "all",
    }
}

fn policy_source(source: PolicySource) -> &'static str {
    match source {
        PolicySource::Api => "api",
        PolicySource::File => "file",
        PolicySource::Env => "env",
        PolicySource::Crd => "crd",
        PolicySource::Default => "default",
    }
}

fn parse_signal_kind(value: &str) -> Result<SignalKind, PolicyError> {
    match value {
        "traces" => Ok(SignalKind::Traces),
        "logs" => Ok(SignalKind::Logs),
        "metrics" => Ok(SignalKind::Metrics),
        "rum" => Ok(SignalKind::Rum),
        "session_replay" => Ok(SignalKind::SessionReplay),
        "error_tracking" => Ok(SignalKind::ErrorTracking),
        "all" => Ok(SignalKind::All),
        _ => Err(PolicyError::Invalid(format!("unknown signal_kind {value}"))),
    }
}

fn parse_operation(value: &str) -> Result<Operation, PolicyError> {
    match value {
        "ingest" => Ok(Operation::Ingest),
        "query" => Ok(Operation::Query),
        "store" => Ok(Operation::Store),
        "all" => Ok(Operation::All),
        _ => Err(PolicyError::Invalid(format!("unknown operation {value}"))),
    }
}

fn parse_policy_source(value: &str) -> Result<PolicySource, PolicyError> {
    match value {
        "api" => Ok(PolicySource::Api),
        "file" => Ok(PolicySource::File),
        "env" => Ok(PolicySource::Env),
        "crd" => Ok(PolicySource::Crd),
        "default" => Ok(PolicySource::Default),
        _ => Err(PolicyError::Invalid(format!(
            "unknown policy source {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_row(
        tenant_id: Uuid,
        project_id: Option<Uuid>,
        signal_kind: &str,
        operation: &str,
        limit_json: Value,
    ) -> PolicyRow {
        PolicyRow {
            id: 1,
            tenant_id,
            project_id,
            signal_kind: signal_kind.to_string(),
            operation: operation.to_string(),
            limit_json,
            grace_pct: 101,
            hard_block_pct: 103,
            effective_from: 0,
            effective_until: None,
            source: "api".to_string(),
        }
    }

    #[test]
    fn min_option_uses_most_restrictive_present_value() {
        assert_eq!(min_option(Some(10), Some(3)), Some(3));
        assert_eq!(min_option(Some(10), None), Some(10));
        assert_eq!(min_option(None, Some(3)), Some(3));
        assert_eq!(min_option(None, None), None);
    }

    #[test]
    fn parses_round_trip_names() {
        assert_eq!(
            parse_signal_kind(signal_kind(SignalKind::Logs)).unwrap(),
            SignalKind::Logs
        );
        assert_eq!(
            parse_operation(operation(Operation::Query)).unwrap(),
            Operation::Query
        );
        assert_eq!(
            parse_policy_source(policy_source(PolicySource::Api)).unwrap(),
            PolicySource::Api
        );
    }

    #[test]
    fn rows_to_cache_fails_before_replacing_active_snapshot() {
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let cache = PolicyCache::default();
        cache.replace(
            rows_to_cache(vec![policy_row(
                tenant_id,
                Some(project_id),
                "traces",
                "ingest",
                serde_json::json!({
                    "kind": "rate",
                    "records_per_sec": 10,
                    "bytes_per_sec": null
                }),
            )])
            .unwrap(),
        );

        assert!(
            rows_to_cache(vec![policy_row(
                tenant_id,
                Some(project_id),
                "not_a_signal",
                "ingest",
                serde_json::json!({
                    "kind": "rate",
                    "records_per_sec": 1,
                    "bytes_per_sec": null
                }),
            )])
            .is_err()
        );

        let resolved = resolve_cached_policy(
            &cache.snapshot(),
            tenant_id,
            project_id,
            SignalKind::Traces,
            Operation::Ingest,
        );
        assert_eq!(
            resolved.rate,
            Some(PolicyLimit::Rate {
                records_per_sec: Some(10),
                bytes_per_sec: None,
            })
        );
    }

    #[test]
    fn resolve_cached_policy_prefers_most_restrictive_specific_and_default_limits() {
        let tenant_id = Uuid::new_v4();
        let project_id = Uuid::new_v4();
        let cache = rows_to_cache(vec![
            policy_row(
                tenant_id,
                None,
                "all",
                "ingest",
                serde_json::json!({
                    "kind": "rate",
                    "records_per_sec": 100,
                    "bytes_per_sec": null
                }),
            ),
            policy_row(
                tenant_id,
                Some(project_id),
                "traces",
                "ingest",
                serde_json::json!({
                    "kind": "rate",
                    "records_per_sec": 25,
                    "bytes_per_sec": null
                }),
            ),
            policy_row(
                tenant_id,
                Some(project_id),
                "traces",
                "ingest",
                serde_json::json!({
                    "kind": "quota",
                    "max_bytes": 1024,
                    "period_start": 0,
                    "period_end": null,
                    "basis": "compressed_bytes"
                }),
            ),
        ])
        .unwrap();

        let resolved = resolve_cached_policy(
            &cache,
            tenant_id,
            project_id,
            SignalKind::Traces,
            Operation::Ingest,
        );
        assert_eq!(
            resolved.rate,
            Some(PolicyLimit::Rate {
                records_per_sec: Some(25),
                bytes_per_sec: None,
            })
        );
        assert_eq!(resolved.quotas.len(), 1);
    }
}
