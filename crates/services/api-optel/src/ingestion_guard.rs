use crate::rate_limiter::ProjectRateLimiter;
use std::sync::Arc;
use tonic::Status;
use uuid::Uuid;
use zradar_models::{ProjectSettings, RequestContext};
use zradar_policy::{BlockCode, Decision, IngestCtx, PolicyEnforcer, SignalKind};
use zradar_traits::SettingsRepository;

pub async fn load_project_settings(
    repository: &Option<Arc<dyn SettingsRepository>>,
    context: &RequestContext,
) -> Result<(Uuid, Option<ProjectSettings>), Status> {
    let project_id = Uuid::parse_str(&context.project_id)
        .map_err(|_| Status::invalid_argument("Invalid project_id in request context"))?;

    let Some(repository) = repository else {
        return Ok((project_id, None));
    };

    let settings = repository
        .get_settings(project_id)
        .await
        .map_err(|e| Status::internal(format!("Failed to load project settings: {}", e)))?;

    Ok((project_id, settings))
}

pub async fn enforce_project_settings(
    repository: &Option<Arc<dyn SettingsRepository>>,
    rate_limiter: &Option<Arc<ProjectRateLimiter>>,
    context: &RequestContext,
    records: u64,
) -> Result<(), Status> {
    let (project_id, settings) = load_project_settings(repository, context).await?;

    let Some(settings) = settings else {
        return Ok(());
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
            return Ok(());
        };

        if !rate_limiter.check_and_record(project_id, limit, records) {
            return Err(Status::resource_exhausted(
                "Project ingestion rate limit exceeded",
            ));
        }
    }

    Ok(())
}

pub async fn enforce_policy_ingest(
    enforcer: &dyn PolicyEnforcer,
    context: &RequestContext,
    signal: SignalKind,
    records: u64,
    estimated_bytes: Option<u64>,
) -> Result<(), Status> {
    let tenant_id = Uuid::parse_str(&context.tenant_id)
        .map_err(|_| Status::invalid_argument("Invalid tenant_id in request context"))?;
    let project_id = Uuid::parse_str(&context.project_id)
        .map_err(|_| Status::invalid_argument("Invalid project_id in request context"))?;

    let decision = enforcer
        .check_ingest(IngestCtx {
            tenant_id,
            project_id,
            signal,
            records,
            estimated_bytes,
            now_micros: chrono::Utc::now().timestamp_micros(),
        })
        .await;

    match decision {
        Decision::Allow | Decision::AllowWithGrace { .. } => Ok(()),
        Decision::Throttle {
            retry_after_ms: _,
            reason,
        } => Err(Status::resource_exhausted(reason)),
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
