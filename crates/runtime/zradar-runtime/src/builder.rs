//! [`ZradarRuntimeBuilder`] — owns server startup, accepts pluggable auth.
//!
//! Call sites:
//! - OSS `zradar-server/src/main.rs` — builds [`ConfigAuthenticator`] +
//!   [`ApiKeyAdminAuthorizer`] and passes them in.
//! - External platform wrapper binaries — supply their own `Authenticator` and
//!   `AdminAuthorizer` implementations and pass them in without modifying this crate.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use api::{
    audit::{AuditState, audit_router},
    http::{AuthMode, create_admin_router},
    policy::{PolicyState, policy_router},
    retention::{handlers::RetentionState, retention_router},
    settings::{SettingsState, settings_router},
    telemetry::QueryService,
};
use api_optel::{
    CircuitBreaker, LogsServiceServer, MetricsServiceServer, OtlpLogsService, OtlpMetricsService,
    OtlpTraceService, ProjectRateLimiter, TraceServiceServer,
};
use axum::Router;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tower_http::compression::CompressionLayer;

use sqlx::postgres::PgPoolOptions;

use zradar_models::Config;
use zradar_parquet::{
    Compactor, DiskCache, FileMover, FlushWorker, MemoryCache, ParquetFileReader,
    ParquetFileWriter, ParquetTelemetryReader, ParquetTelemetryWriter, RetentionJob, WriteBuffer,
    WriterConfig, recover_incomplete_writes,
};
use zradar_plugin_postgres::{
    PostgresAuditLogRepository, PostgresClient, PostgresDecisionAuditSink,
    PostgresFileListRepository, PostgresPolicyStore, PostgresRetentionPolicyRepository,
    PostgresSettingsRepository, PostgresThresholdSink, PostgresUsageReader, PostgresUsageTracker,
    UsageTrackerMetrics, migrations::MIGRATIONS,
};
use zradar_plugin_s3::S3BlockStorage;
use zradar_policy::{
    DecisionAuditSink, FanoutUsageTracker, InMemoryUsageTracker, PolicyEnforcer, PolicyEngine,
    PolicyStore, ThresholdSink, UsageAnalyticsReader, UsageReader, UsageTracker,
};
use zradar_retention::{
    CleanupJob, EnforcementStrategy, OrgRetentionConfig, QueryEnforcer, RetentionConfigStore,
};
use zradar_traits::{AdminAuthorizer, Authenticator};

/// Authentication and admin-context strategies injected by the binary.
///
/// - `otlp` — validates OTLP `Authorization: Bearer <token>`.
/// - `admin` — validates Admin HTTP requests and resolves tenant/project/capabilities.
pub struct RuntimeAuth {
    /// OTLP authenticator. `None` = open ingest (no key required).
    pub otlp: Option<Arc<dyn Authenticator>>,
    /// Admin HTTP authorizer.
    pub admin: Arc<dyn AdminAuthorizer>,
}

/// Builds and runs the zradar server runtime.
///
/// Accepts [`RuntimeAuth`] from the caller so the binary decides which
/// auth implementations to wire in without this crate knowing about them.
pub struct ZradarRuntimeBuilder {
    config: Config,
    auth: RuntimeAuth,
}

impl ZradarRuntimeBuilder {
    /// Create a new builder from a loaded config and caller-provided auth.
    pub fn new(config: Config, auth: RuntimeAuth) -> Self {
        Self { config, auth }
    }

    /// Run the OTLP gRPC and Admin HTTP servers. Blocks until either exits.
    pub async fn run(self) -> Result<()> {
        let config = self.config;
        let RuntimeAuth {
            otlp: otlp_auth,
            admin: admin_authorizer,
        } = self.auth;

        // =====================================================================
        // Database
        // =====================================================================

        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pg_pool = PgPoolOptions::new()
            .max_connections(
                config
                    .postgres
                    .as_ref()
                    .map(|p| p.max_connections)
                    .unwrap_or(20) as u32,
            )
            .connect(&database_url)
            .await
            .expect("Failed to connect to PostgreSQL");

        info!("Connected to PostgreSQL");
        MIGRATIONS.run(&pg_pool).await?;
        info!("Migrations applied");

        let pg_pool = Arc::new(pg_pool);
        let pg_client = Arc::new(PostgresClient::from_pool(pg_pool.clone()));
        let file_list_repo = Arc::new(PostgresFileListRepository::new(pg_client.clone()))
            as Arc<dyn zradar_traits::FileListRepository>;
        let settings_repo = Arc::new(PostgresSettingsRepository::new(pg_client.clone()))
            as Arc<dyn zradar_traits::SettingsRepository>;
        let retention_policy_repo =
            Arc::new(PostgresRetentionPolicyRepository::new(pg_client.clone()))
                as Arc<dyn zradar_traits::RetentionPolicyRepository>;
        let audit_log_repo = Arc::new(PostgresAuditLogRepository::new(pg_client.clone()))
            as Arc<dyn zradar_traits::AuditLogRepository>;
        let policy_store_impl = Arc::new(PostgresPolicyStore::new(pg_client.clone()));
        policy_store_impl.refresh().await?;
        let policy_store = policy_store_impl.clone() as Arc<dyn PolicyStore>;
        let hot_usage = Arc::new(InMemoryUsageTracker::new());
        let usage_reader = hot_usage.clone() as Arc<dyn UsageReader>;
        let reporting_usage_reader =
            Arc::new(PostgresUsageReader::new(pg_client.clone())) as Arc<dyn UsageReader>;
        let usage_analytics_reader =
            Arc::new(PostgresUsageReader::new(pg_client.clone())) as Arc<dyn UsageAnalyticsReader>;
        let usage_tracker_metrics = Arc::new(UsageTrackerMetrics::default());
        let postgres_usage_tracker = Arc::new(PostgresUsageTracker::spawn_with_metrics(
            pg_client.clone(),
            usage_tracker_metrics.clone(),
        )) as Arc<dyn UsageTracker>;
        let usage_tracker = Arc::new(FanoutUsageTracker::new(vec![
            hot_usage.clone() as Arc<dyn UsageTracker>,
            postgres_usage_tracker,
        ])) as Arc<dyn UsageTracker>;
        let threshold_sink =
            Arc::new(PostgresThresholdSink::new(pg_client.clone())) as Arc<dyn ThresholdSink>;
        let decision_audit_sink = Arc::new(PostgresDecisionAuditSink::new(pg_client.clone()))
            as Arc<dyn DecisionAuditSink>;
        let policy_engine = Arc::new(PolicyEngine::new_with_decision_audit(
            policy_store.clone(),
            usage_reader,
            usage_tracker.clone(),
            threshold_sink,
            decision_audit_sink,
        ));
        let policy_enforcer = policy_engine.enforcer.clone() as Arc<dyn PolicyEnforcer>;

        // =====================================================================
        // Parquet storage layer
        // =====================================================================

        let ingestor_storage_config = config
            .ingestor
            .as_ref()
            .map(|i| i.storage.clone())
            .unwrap_or_default();

        let parquet_data_dir = std::path::PathBuf::from(&ingestor_storage_config.parquet_data_dir);
        let parquet_lifecycle_config = ingestor_storage_config.parquet.clone();

        tokio::fs::create_dir_all(&parquet_data_dir).await?;

        if let Err(e) = recover_incomplete_writes(&parquet_data_dir).await {
            error!("Crash recovery failed (non-fatal): {}", e);
        }
        info!("Crash recovery complete");

        let writer_config = WriterConfig::from_storage_config(
            parquet_lifecycle_config.bloom_filter_columns.clone(),
            parquet_lifecycle_config.fsync_before_rename,
        );
        let parquet_file_writer = Arc::new(ParquetFileWriter::with_config_and_usage_tracker(
            parquet_data_dir.clone(),
            file_list_repo.clone(),
            writer_config,
            usage_tracker.clone(),
        ));

        let cancel_token = CancellationToken::new();

        let write_buffer: Option<Arc<WriteBuffer>> =
            if parquet_lifecycle_config.write_buffer_enabled {
                let buf = Arc::new(WriteBuffer::new(
                    parquet_lifecycle_config.write_buffer_size_bytes,
                ));
                let worker = FlushWorker::new(
                    buf.clone(),
                    parquet_file_writer.clone(),
                    parquet_lifecycle_config.write_buffer_flush_interval_secs,
                );
                tokio::spawn(worker.run(cancel_token.clone()));
                info!(
                    max_bytes = parquet_lifecycle_config.write_buffer_size_bytes,
                    "Write buffer + FlushWorker started"
                );
                Some(buf)
            } else {
                info!("Write buffer disabled — using direct Parquet writes");
                None
            };

        let memory_cache: Option<Arc<MemoryCache>> =
            if parquet_lifecycle_config.memory_cache_enabled {
                let mc = Arc::new(MemoryCache::new(
                    parquet_lifecycle_config.memory_cache_max_bytes,
                    parquet_lifecycle_config.memory_cache_shards,
                ));
                info!(
                    max_bytes = parquet_lifecycle_config.memory_cache_max_bytes,
                    "MemoryCache initialized"
                );
                Some(mc)
            } else {
                None
            };

        if parquet_lifecycle_config.compaction_enabled {
            let compactor = Compactor::new(
                file_list_repo.clone(),
                parquet_file_writer.clone(),
                parquet_lifecycle_config.compaction_check_interval_secs,
                parquet_lifecycle_config.compaction_min_files,
                parquet_lifecycle_config.compaction_max_file_size_bytes,
            );
            tokio::spawn(compactor.run(cancel_token.clone()));
            info!("Compactor background task started");
        }

        let s3_block_storage: Option<Arc<dyn zradar_traits::BlockStorage>> =
            if let Some(s3_cfg) = &ingestor_storage_config.s3 {
                match S3BlockStorage::new(
                    s3_cfg.bucket.clone(),
                    s3_cfg.region.clone(),
                    s3_cfg.endpoint.clone(),
                )
                .await
                {
                    Ok(s3) => {
                        let s3: Arc<dyn zradar_traits::BlockStorage> = Arc::new(s3);
                        info!(
                            bucket = %s3_cfg.bucket,
                            region = %s3_cfg.region,
                            "S3 block storage initialized"
                        );

                        let mover = FileMover::new(
                            file_list_repo.clone(),
                            s3.clone(),
                            parquet_lifecycle_config.clone(),
                            parquet_data_dir.clone(),
                        );
                        tokio::spawn(mover.run(cancel_token.clone()));
                        info!("FileMover background task started");

                        let retention = RetentionJob::with_storage(
                            file_list_repo.clone(),
                            s3.clone(),
                            parquet_lifecycle_config.clone(),
                        );
                        tokio::spawn(retention.run(cancel_token.clone()));
                        info!("RetentionJob background task started");

                        Some(s3)
                    }
                    Err(e) => {
                        error!("Failed to initialize S3 block storage: {}", e);
                        return Err(e);
                    }
                }
            } else {
                let retention =
                    RetentionJob::new(file_list_repo.clone(), parquet_lifecycle_config.clone());
                tokio::spawn(retention.run(cancel_token.clone()));
                info!("RetentionJob (local-only) background task started");
                None
            };

        let parquet_file_reader: Arc<ParquetFileReader> = if let Some(s3_storage) = s3_block_storage
        {
            let disk_cache = Arc::new(DiskCache::new(s3_storage, &parquet_lifecycle_config));
            Arc::new(if let Some(mc) = memory_cache.clone() {
                ParquetFileReader::with_cache_and_memory_cache(
                    parquet_data_dir.clone(),
                    file_list_repo.clone(),
                    disk_cache,
                    mc,
                )
            } else {
                ParquetFileReader::with_cache(
                    parquet_data_dir.clone(),
                    file_list_repo.clone(),
                    disk_cache,
                )
            })
        } else {
            Arc::new(if let Some(mc) = memory_cache.clone() {
                ParquetFileReader::with_memory_cache(
                    parquet_data_dir.clone(),
                    file_list_repo.clone(),
                    mc,
                )
            } else {
                ParquetFileReader::new(parquet_data_dir.clone(), file_list_repo.clone())
            })
        };

        let parquet_telemetry_reader =
            Arc::new(ParquetTelemetryReader::new(parquet_file_reader.clone()));

        // =====================================================================
        // Retention system
        // =====================================================================

        let global_retention_days = parquet_lifecycle_config.retention_days;
        let retention_config_store = Arc::new(RetentionConfigStore::new(global_retention_days));
        for policy in retention_policy_repo.list_policies().await? {
            retention_config_store.upsert(OrgRetentionConfig {
                org_id: policy.org_id,
                default_days: policy.default_days as u32,
                project_overrides: policy.project_overrides_map()?,
            });
        }
        info!(
            retention_days = global_retention_days,
            "RetentionConfigStore initialized"
        );

        let cleanup_job = Arc::new(CleanupJob::new(
            file_list_repo.clone(),
            retention_config_store.clone(),
            parquet_lifecycle_config.retention_check_interval_secs,
        ));
        {
            let job = cleanup_job.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move { job.run(cancel).await });
        }
        info!("CleanupJob background task started");

        let retention_state = Arc::new(RetentionState {
            cleanup_job: cleanup_job.clone(),
            config_store: retention_config_store.clone(),
            policy_repo: retention_policy_repo.clone(),
            audit_log_repo: Some(audit_log_repo.clone()),
        });

        let query_enforcer = Arc::new(QueryEnforcer::new(
            retention_config_store.clone(),
            EnforcementStrategy::Clamp,
        ));

        let query_service = Arc::new(
            QueryService::with_enforcer(
                parquet_telemetry_reader as Arc<dyn zradar_traits::TelemetryReader>,
                query_enforcer,
            )
            .with_file_list_repo(file_list_repo.clone())
            .with_policy_enforcer(policy_enforcer.clone())
            .with_usage_tracker(usage_tracker.clone())
            .with_policy_context(
                policy_store.clone(),
                reporting_usage_reader.clone(),
                usage_analytics_reader,
            ),
        );

        info!("Storage layer initialized");

        // =====================================================================
        // Parquet writer + WAL
        // =====================================================================

        let parquet_writer: Arc<dyn zradar_traits::TelemetryWriter> =
            if let Some(buf) = write_buffer.clone() {
                Arc::new(ParquetTelemetryWriter::with_buffer(
                    parquet_file_writer.clone(),
                    buf,
                ))
            } else {
                Arc::new(ParquetTelemetryWriter::new(parquet_file_writer.clone()))
            };

        let wal_config = config
            .ingestor
            .as_ref()
            .map(|i| i.wal.clone())
            .unwrap_or_default();

        let otlp_writer: Arc<dyn zradar_traits::TelemetryWriter> = if wal_config.enabled {
            // WAL path is preserved from the original main.rs.
            // The WAL writer wraps the Parquet writer and applies backpressure.
            // Inline here to avoid re-exporting WAL internals from this crate.
            use zradar_wal::Wal;
            use zradar_wal::checkpoint::CheckpointStore;
            use zradar_wal::config::WalConfig;
            use zradar_wal::flusher::{FlushSink, WalFlusher};
            use zradar_wal::janitor::WalJanitor;
            use zradar_wal::record::{SignalType, WalRecord};
            use zradar_wal::replay::WalReplayer;

            let wal_dir = std::path::PathBuf::from(&wal_config.wal_dir);
            tokio::fs::create_dir_all(&wal_dir).await?;

            let native_config = WalConfig {
                enabled: true,
                wal_dir: wal_config.wal_dir.clone(),
                segment_max_bytes: wal_config.segment_max_bytes,
                flush_interval_ms: wal_config.flush_interval_ms,
                group_commit_window_ms: wal_config.group_commit_window_ms,
                replay_batch_max_bytes: wal_config.replay_batch_max_bytes,
                ..WalConfig::default()
            };

            let checkpoint_store = Arc::new(CheckpointStore::new(&wal_dir));

            let wal = Arc::new(
                Wal::open(&wal_dir, native_config.clone(), cancel_token.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL open: {}", e))?,
            );

            struct TelemetryFlushSink {
                writer: Arc<dyn zradar_traits::TelemetryWriter>,
            }

            #[async_trait::async_trait]
            impl FlushSink for TelemetryFlushSink {
                async fn flush_records(&self, records: &[WalRecord]) -> anyhow::Result<()> {
                    use zradar_models::{LogRecord, Metric, Span};
                    let mut spans: Vec<Span> = Vec::new();
                    let mut metrics: Vec<Metric> = Vec::new();
                    let mut logs: Vec<LogRecord> = Vec::new();
                    for record in records {
                        match record.signal_type {
                            SignalType::Trace => {
                                if let Ok(span) = serde_json::from_slice::<Span>(&record.payload) {
                                    spans.push(span);
                                }
                            }
                            SignalType::Metric => {
                                if let Ok(m) = serde_json::from_slice::<Metric>(&record.payload) {
                                    metrics.push(m);
                                }
                            }
                            SignalType::Log => {
                                if let Ok(log) =
                                    serde_json::from_slice::<LogRecord>(&record.payload)
                                {
                                    logs.push(log);
                                }
                            }
                        }
                    }
                    if !spans.is_empty() {
                        self.writer.insert_spans(&spans).await?;
                    }
                    if !metrics.is_empty() {
                        self.writer.insert_metrics(&metrics).await?;
                    }
                    if !logs.is_empty() {
                        self.writer.insert_logs(&logs).await?;
                    }
                    Ok(())
                }
            }

            let sink = Arc::new(TelemetryFlushSink {
                writer: parquet_writer.clone(),
            });

            // Replay
            let replayer = WalReplayer::new(
                wal_dir.clone(),
                sink.clone(),
                checkpoint_store.clone(),
                native_config.replay_batch_max_bytes,
            );
            replayer.replay().await?;
            info!("WAL replay complete");

            // Flusher
            let flusher = WalFlusher::new(
                wal.clone(),
                sink.clone(),
                checkpoint_store.clone(),
                native_config.flush_interval_ms,
            );
            tokio::spawn(flusher.run(cancel_token.clone()));

            // Janitor
            let janitor = WalJanitor::new(
                wal.clone(),
                checkpoint_store.clone(),
                native_config.flush_interval_ms,
            );
            tokio::spawn(janitor.run(cancel_token.clone()));

            info!("WAL background tasks started");

            struct WalTelemetryWriter {
                wal: Arc<Wal>,
            }

            #[async_trait::async_trait]
            impl zradar_traits::TelemetryWriter for WalTelemetryWriter {
                async fn insert_spans(&self, spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                    for span in spans {
                        let payload = serde_json::to_vec(span)
                            .map_err(|e| anyhow::anyhow!("WAL span serialize: {}", e))?;
                        let tenant_id =
                            uuid::Uuid::parse_str(&span.tenant_id).unwrap_or(uuid::Uuid::nil());
                        let project_id =
                            uuid::Uuid::parse_str(&span.project_id).unwrap_or(uuid::Uuid::nil());
                        let record = WalRecord {
                            signal_type: SignalType::Trace,
                            tenant_id,
                            project_id,
                            arrival_timestamp_ns: chrono::Utc::now()
                                .timestamp_nanos_opt()
                                .unwrap_or(0),
                            assigned_offset: 0,
                            payload: bytes::Bytes::from(payload),
                        };
                        let handle = self
                            .wal
                            .append(record)
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL append: {}", e))?;
                        handle
                            .durable()
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL fsync: {}", e))?;
                    }
                    Ok(())
                }

                async fn insert_metrics(
                    &self,
                    metrics: &[zradar_models::Metric],
                ) -> anyhow::Result<()> {
                    for m in metrics {
                        let payload = serde_json::to_vec(m)
                            .map_err(|e| anyhow::anyhow!("WAL metric serialize: {}", e))?;
                        let tenant_id =
                            uuid::Uuid::parse_str(&m.tenant_id).unwrap_or(uuid::Uuid::nil());
                        let project_id =
                            uuid::Uuid::parse_str(&m.project_id).unwrap_or(uuid::Uuid::nil());
                        let record = WalRecord {
                            signal_type: SignalType::Metric,
                            tenant_id,
                            project_id,
                            arrival_timestamp_ns: chrono::Utc::now()
                                .timestamp_nanos_opt()
                                .unwrap_or(0),
                            assigned_offset: 0,
                            payload: bytes::Bytes::from(payload),
                        };
                        let handle = self
                            .wal
                            .append(record)
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL append: {}", e))?;
                        handle
                            .durable()
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL fsync: {}", e))?;
                    }
                    Ok(())
                }

                async fn insert_logs(
                    &self,
                    logs: &[zradar_models::LogRecord],
                ) -> anyhow::Result<()> {
                    for log in logs {
                        let payload = serde_json::to_vec(log)
                            .map_err(|e| anyhow::anyhow!("WAL log serialize: {}", e))?;
                        let tenant_id =
                            uuid::Uuid::parse_str(&log.tenant_id).unwrap_or(uuid::Uuid::nil());
                        let project_id =
                            uuid::Uuid::parse_str(&log.project_id).unwrap_or(uuid::Uuid::nil());
                        let record = WalRecord {
                            signal_type: SignalType::Log,
                            tenant_id,
                            project_id,
                            arrival_timestamp_ns: chrono::Utc::now()
                                .timestamp_nanos_opt()
                                .unwrap_or(0),
                            assigned_offset: 0,
                            payload: bytes::Bytes::from(payload),
                        };
                        let handle = self
                            .wal
                            .append(record)
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL append: {}", e))?;
                        handle
                            .durable()
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL fsync: {}", e))?;
                    }
                    Ok(())
                }
            }

            Arc::new(WalTelemetryWriter { wal }) as Arc<dyn zradar_traits::TelemetryWriter>
        } else {
            parquet_writer.clone()
        };

        let wal_metrics = Arc::new(zradar_wal::metrics::WalMetrics::new());

        // =====================================================================
        // OTLP services
        // =====================================================================

        let otlp_auth_ext = otlp_auth;
        if otlp_auth_ext.is_none() {
            info!("OTLP gRPC is open: no API key required");
        }

        let rate_limiter = Arc::new(ProjectRateLimiter::new());
        let circuit_breaker = Arc::new(CircuitBreaker::new(
            parquet_data_dir.clone(),
            parquet_lifecycle_config.circuit_breaker_max_disk_usage_percent,
            parquet_lifecycle_config.circuit_breaker_max_memory_usage_percent,
            parquet_lifecycle_config.circuit_breaker_max_queue_depth,
        ));
        if let Some(buffer) = write_buffer.clone() {
            let breaker = circuit_breaker.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            breaker.set_queue_depth(0);
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            breaker.set_queue_depth(buffer.record_count() as u64);
                        }
                    }
                }
            });
        }

        let trace_service = OtlpTraceService::with_settings_and_policy(
            otlp_writer.clone(),
            otlp_auth_ext.clone(),
            settings_repo.clone(),
            rate_limiter.clone(),
            policy_enforcer.clone(),
            circuit_breaker.clone(),
        );
        let metrics_service = OtlpMetricsService::with_settings_and_policy(
            otlp_writer.clone(),
            otlp_auth_ext.clone(),
            settings_repo.clone(),
            rate_limiter.clone(),
            policy_enforcer.clone(),
            circuit_breaker.clone(),
        );
        let logs_service = OtlpLogsService::with_settings_and_policy(
            otlp_writer.clone(),
            otlp_auth_ext.clone(),
            settings_repo.clone(),
            rate_limiter,
            policy_enforcer,
            circuit_breaker.clone(),
        );

        info!("OTLP services initialized");

        // =====================================================================
        // Admin HTTP routers
        // =====================================================================

        let otlp_port = config.otlp_port;
        let admin_port = config.effective_admin_port();

        let health_router = crate::health::create_health_router(crate::health::HealthState {
            pg_pool: Some(pg_pool.clone()),
            storage_path: parquet_data_dir.clone(),
            circuit_breaker: Some(circuit_breaker),
            retention_initialized: true,
            ingestion_initialized: true,
            background_jobs_started: true,
        });

        let auth_mode = AuthMode::Standalone;

        // Pass AdminAuthorizer directly to each router. The `AuthContext` extractor
        // calls `authorizer.authorize(&parts.headers)` with the original request
        // headers, preserving all trusted context headers set by gateway wrappers.
        let admin_api = create_admin_router(query_service, admin_authorizer.clone(), auth_mode);
        let retention_api = retention_router(retention_state, admin_authorizer.clone(), auth_mode);
        let settings_api = settings_router(
            Arc::new(SettingsState {
                repository: settings_repo,
                audit_log_repo: Some(audit_log_repo.clone()),
            }),
            admin_authorizer.clone(),
            auth_mode,
        );
        let policy_api = policy_router(
            Arc::new(PolicyState {
                store: policy_store,
            }),
            admin_authorizer.clone(),
            auth_mode,
        );
        let audit_api = audit_router(
            Arc::new(AuditState {
                repository: audit_log_repo,
            }),
            admin_authorizer,
            auth_mode,
        );

        let metrics_state = wal_metrics.clone();
        let usage_metrics_state = usage_tracker_metrics.clone();
        let metrics_route = axum::routing::get(move || {
            let m = metrics_state.clone();
            let usage_m = usage_metrics_state.clone();
            async move {
                let mut output = m.render_prometheus();
                output.push_str(&usage_m.render_prometheus());
                output
            }
        });

        let app = Router::new()
            .route("/metrics", metrics_route)
            .merge(health_router)
            .merge(admin_api)
            .merge(retention_api)
            .merge(settings_api)
            .merge(policy_api)
            .merge(audit_api)
            .layer(CompressionLayer::new());

        // =====================================================================
        // Run servers
        // =====================================================================

        let otlp_addr = format!("0.0.0.0:{}", otlp_port).parse()?;
        let otlp_server = async move {
            info!("OTLP gRPC server listening on {}", otlp_addr);
            Server::builder()
                .add_service(
                    TraceServiceServer::new(trace_service)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    MetricsServiceServer::new(metrics_service)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .add_service(
                    LogsServiceServer::new(logs_service)
                        .accept_compressed(CompressionEncoding::Gzip)
                        .send_compressed(CompressionEncoding::Gzip),
                )
                .serve(otlp_addr)
                .await
                .map_err(|e| anyhow::anyhow!("OTLP server failed: {}", e))
        };

        let admin_addr = format!("0.0.0.0:{}", admin_port);
        let admin_server = async move {
            info!("Admin API listening on http://{}", admin_addr);
            let listener = tokio::net::TcpListener::bind(&admin_addr).await?;
            axum::serve(listener, app)
                .await
                .map_err(|e| anyhow::anyhow!("Admin API server failed: {}", e))
        };

        info!("zradar is ready!");
        info!("  OTLP gRPC: localhost:{}", otlp_port);
        info!("  Admin API: http://localhost:{}", admin_port);

        tokio::select! {
            result = otlp_server => {
                if let Err(e) = result {
                    error!("OTLP server failed: {}", e);
                }
            }
            result = admin_server => {
                if let Err(e) = result {
                    error!("Admin API server failed: {}", e);
                }
            }
        }

        Ok(())
    }
}
