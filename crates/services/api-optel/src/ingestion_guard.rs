use crate::rate_limiter::ProjectRateLimiter;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};
use uuid::Uuid;
use zradar_models::{RequestContext, WorkspaceSettings};
use zradar_policy::{BlockCode, Decision, IngestCtx, PolicyEnforcer, SignalKind};
use zradar_traits::SettingsRepository;

pub async fn load_workspace_settings(
    repository: &Option<Arc<dyn SettingsRepository>>,
    context: &RequestContext,
) -> Result<(Uuid, Option<WorkspaceSettings>), Status> {
    let workspace_id = context.workspace_id;

    let Some(repository) = repository else {
        return Ok((workspace_id.into(), None));
    };

    let settings = repository
        .get_settings(workspace_id)
        .await
        .map_err(|e| Status::internal(format!("Failed to load project settings: {}", e)))?;

    Ok((workspace_id.into(), settings))
}

pub async fn enforce_workspace_settings(
    repository: &Option<Arc<dyn SettingsRepository>>,
    rate_limiter: &Option<Arc<ProjectRateLimiter>>,
    context: &RequestContext,
    records: u64,
) -> Result<(), Status> {
    enforce_workspace_settings_and_get(repository, rate_limiter, context, records)
        .await
        .map(|_| ())
}

pub async fn enforce_workspace_settings_and_get(
    repository: &Option<Arc<dyn SettingsRepository>>,
    rate_limiter: &Option<Arc<ProjectRateLimiter>>,
    context: &RequestContext,
    records: u64,
) -> Result<Option<WorkspaceSettings>, Status> {
    let (workspace_id, settings) = load_workspace_settings(repository, context).await?;

    let Some(settings) = settings else {
        return Ok(None);
    };

    if settings.blocked {
        return Err(Status::permission_denied(
            "Project is blocked for ingestion",
        ));
    }

    if let Some(limit) = settings.max_ingestion_rate {
        let limit = u64::try_from(limit)
            .map_err(|_| Status::invalid_argument("Invalid max_ingestion_rate"))?;

        let Some(rate_limiter) = rate_limiter else {
            return Ok(Some(settings));
        };

        if !rate_limiter.check_and_record(workspace_id.into(), limit, records) {
            return Err(rate_limited_status(limit, records));
        }
    }

    Ok(Some(settings))
}

pub async fn enforce_policy_ingest(
    enforcer: &dyn PolicyEnforcer,
    context: &RequestContext,
    signal: SignalKind,
    records: u64,
    estimated_bytes: Option<u64>,
) -> Result<(), Status> {
    let workspace_id = context.workspace_id;

    let decision = enforcer
        .check_ingest(IngestCtx {
            workspace_id,
            signal,
            records,
            estimated_bytes,
            now_micros: chrono::Utc::now().timestamp_micros(),
        })
        .await;

    match decision {
        Decision::Allow | Decision::AllowWithGrace { .. } => Ok(()),
        // Throttling is recoverable: RetryInfo makes RESOURCE_EXHAUSTED
        // retryable for OTLP exporters (and becomes Retry-After on HTTP).
        Decision::Throttle {
            retry_after_ms,
            reason,
        } => Err(retryable_resource_exhausted(
            reason,
            Duration::from_millis(u64::from(retry_after_ms)),
        )),
        Decision::Block { reason, code } => match code {
            BlockCode::ProjectBlocked => Err(Status::permission_denied(reason)),
            BlockCode::RateLimitExceeded | BlockCode::QuotaExceeded | BlockCode::SizeExceeded => {
                Err(Status::resource_exhausted(reason))
            }
            BlockCode::RetentionViolation | BlockCode::QueryWindowViolation => {
                Err(Status::invalid_argument(reason))
            }
        },
    }
}

/// `RESOURCE_EXHAUSTED` with `RetryInfo`: retryable per the OTLP spec
/// (<https://opentelemetry.io/docs/specs/otlp/#failures>).
fn retryable_resource_exhausted(message: impl Into<String>, retry_after: Duration) -> Status {
    Status::with_error_details(
        Code::ResourceExhausted,
        message,
        ErrorDetails::with_retry_info(Some(retry_after)),
    )
}

/// Status for a request over the per-project rate limit. The token bucket
/// holds up to two seconds of budget, so a request that fits it can succeed
/// after a refill and is retryable; a larger one never can and is not.
fn rate_limited_status(limit_per_second: u64, records: u64) -> Status {
    const MESSAGE: &str = "Project ingestion rate limit exceeded";
    if limit_per_second > 0 && records <= limit_per_second.saturating_mul(2) {
        retryable_resource_exhausted(MESSAGE, Duration::from_secs(1))
    } else {
        Status::resource_exhausted(MESSAGE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limit_is_retryable_only_when_the_request_can_fit() {
        let status = rate_limited_status(100, 150);
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            crate::retry::retry_delay(&status),
            Some(Duration::from_secs(1))
        );

        // More records than the bucket can ever hold: retrying cannot help.
        assert_eq!(
            crate::retry::retry_delay(&rate_limited_status(100, 201)),
            None
        );
        assert_eq!(crate::retry::retry_delay(&rate_limited_status(0, 1)), None);
    }

    struct Throttling;
    #[tonic::async_trait]
    impl PolicyEnforcer for Throttling {
        async fn check_ingest(&self, _ctx: IngestCtx) -> Decision {
            Decision::Throttle {
                retry_after_ms: 1500,
                reason: "slow down",
            }
        }
        async fn check_query(&self, _ctx: zradar_policy::QueryCtx) -> Decision {
            Decision::Allow
        }
    }

    #[tokio::test]
    async fn throttle_carries_the_policy_retry_delay() {
        let context = RequestContext::new(Uuid::nil().into());
        let status = enforce_policy_ingest(&Throttling, &context, SignalKind::Traces, 1, None)
            .await
            .expect_err("throttled");
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(
            crate::retry::retry_delay(&status),
            Some(Duration::from_millis(1500))
        );
    }
}
