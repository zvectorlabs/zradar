//! zradar Server — OTLP gRPC + Admin HTTP API

mod auth;
mod health;

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use std::{sync::Arc, time::Duration};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use axum::Router;
use tower_http::compression::CompressionLayer;

use tonic::codec::CompressionEncoding;
use tonic::transport::Server;

use api::{
    audit::{AuditState, audit_router},
    http::{AuthMode, create_admin_router},
    retention::{handlers::RetentionState, retention_router},
    settings::{SettingsState, settings_router},
    telemetry::QueryService,
};
use api_optel::{
    CircuitBreaker, LogsServiceServer, MetricsServiceServer, OtlpLogsService, OtlpMetricsService,
    OtlpTraceService, ProjectRateLimiter, TraceServiceServer,
};
use tokio_util::sync::CancellationToken;
use zradar_models::Config;
use zradar_parquet::{
    Compactor, DiskCache, FileMover, FlushWorker, MemoryCache, ParquetFileReader,
    ParquetFileWriter, ParquetTelemetryReader, ParquetTelemetryWriter, RetentionJob, WriteBuffer,
    WriterConfig, recover_incomplete_writes,
};
use zradar_plugin_postgres::{
    PostgresAuditLogRepository, PostgresClient, PostgresFileListRepository,
    PostgresRetentionPolicyRepository, PostgresSettingsRepository, migrations::MIGRATIONS,
};
use zradar_plugin_s3::S3BlockStorage;
use zradar_retention::{
    CleanupJob, EnforcementStrategy, OrgRetentionConfig, QueryEnforcer, RetentionConfigStore,
};
use zradar_traits::Authenticator;

use auth::{ConfigAuthenticator, PlatformAuthenticator};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,zradar=debug")),
        )
        .init();

    info!("Starting zradar server...");

    let config = Config::load()?;
    info!("Configuration loaded");

    // =========================================================================
    // Authentication
    // =========================================================================

    let authenticator: Arc<dyn Authenticator> = if config.auth.is_platform_mode() {
        let token = &config.auth.platform.gateway_service_token;
        if token.is_empty() {
            anyhow::bail!(
                "auth.mode is 'platform' but auth.platform.gateway_service_token is empty. \
                 Set it in config.toml or via the ZRADAR_GATEWAY_SERVICE_TOKEN env var."
            );
        }
        info!("Platform authenticator initialized (Agnitiv gateway service token)");
        Arc::new(PlatformAuthenticator::new(token.clone()))
    } else {
        info!(
            "Config-based authenticator initialized ({} API keys)",
            config.api_keys.len()
        );
        Arc::new(ConfigAuthenticator::from_config(&config.api_keys))
    };

    // =========================================================================
    // Database
    // =========================================================================

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
    let retention_policy_repo = Arc::new(PostgresRetentionPolicyRepository::new(pg_client.clone()))
        as Arc<dyn zradar_traits::RetentionPolicyRepository>;
    let audit_log_repo = Arc::new(PostgresAuditLogRepository::new(pg_client.clone()))
        as Arc<dyn zradar_traits::AuditLogRepository>;

    // =========================================================================
    // Parquet storage layer
    // =========================================================================

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
    let parquet_file_writer = Arc::new(ParquetFileWriter::with_config(
        parquet_data_dir.clone(),
        file_list_repo.clone(),
        writer_config,
    ));

    let cancel_token = CancellationToken::new();

    // Write buffer
    let write_buffer: Option<Arc<WriteBuffer>> = if parquet_lifecycle_config.write_buffer_enabled {
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
            flush_interval_secs = parquet_lifecycle_config.write_buffer_flush_interval_secs,
            "Write buffer + FlushWorker started"
        );
        Some(buf)
    } else {
        info!("Write buffer disabled — using direct Parquet writes");
        None
    };

    // Memory cache
    let memory_cache: Option<Arc<MemoryCache>> = if parquet_lifecycle_config.memory_cache_enabled {
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

    // Compactor
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

    // S3 block storage + FileMover + RetentionJob
    let s3_block_storage: Option<Arc<dyn zradar_traits::BlockStorage>> = if let Some(s3_cfg) =
        &ingestor_storage_config.s3
    {
        match S3BlockStorage::new(
            s3_cfg.bucket.clone(),
            s3_cfg.region.clone(),
            s3_cfg.endpoint.clone(),
        )
        .await
        {
            Ok(s3) => {
                let s3: Arc<dyn zradar_traits::BlockStorage> = Arc::new(s3);
                info!(bucket = %s3_cfg.bucket, region = %s3_cfg.region, "S3 block storage initialized");

                let mover = FileMover::new(
                    file_list_repo.clone(),
                    s3.clone(),
                    parquet_lifecycle_config.clone(),
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
        let retention = RetentionJob::new(file_list_repo.clone(), parquet_lifecycle_config.clone());
        tokio::spawn(retention.run(cancel_token.clone()));
        info!("RetentionJob (local-only) background task started");
        None
    };

    // Parquet file reader
    let parquet_file_reader: Arc<ParquetFileReader> = if let Some(s3_storage) = s3_block_storage {
        let disk_cache = Arc::new(DiskCache::new(s3_storage, &parquet_lifecycle_config));
        Arc::new(if let Some(memory_cache) = memory_cache.clone() {
            ParquetFileReader::with_cache_and_memory_cache(
                parquet_data_dir.clone(),
                file_list_repo.clone(),
                disk_cache,
                memory_cache,
            )
        } else {
            ParquetFileReader::with_cache(
                parquet_data_dir.clone(),
                file_list_repo.clone(),
                disk_cache,
            )
        })
    } else {
        Arc::new(if let Some(memory_cache) = memory_cache.clone() {
            ParquetFileReader::with_memory_cache(
                parquet_data_dir.clone(),
                file_list_repo.clone(),
                memory_cache,
            )
        } else {
            ParquetFileReader::new(parquet_data_dir.clone(), file_list_repo.clone())
        })
    };

    let parquet_telemetry_reader =
        Arc::new(ParquetTelemetryReader::new(parquet_file_reader.clone()));

    // =========================================================================
    // Retention system
    // =========================================================================

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

    let query_service = Arc::new(QueryService::with_enforcer(
        parquet_telemetry_reader as Arc<dyn zradar_traits::TelemetryReader>,
        query_enforcer,
    ));

    info!("Storage layer initialized");

    // =========================================================================
    // OTLP telemetry writer (direct Parquet path)
    // =========================================================================

    let parquet_writer: Arc<dyn zradar_traits::TelemetryWriter> =
        if let Some(buf) = write_buffer.clone() {
            Arc::new(ParquetTelemetryWriter::with_buffer(
                parquet_file_writer.clone(),
                buf,
            ))
        } else {
            Arc::new(ParquetTelemetryWriter::new(parquet_file_writer.clone()))
        };

    // =========================================================================
    // WAL (Phase 08) — replay + background tasks + WAL-backed writer
    // =========================================================================

    let wal_config = config
        .ingestor
        .as_ref()
        .map(|i| i.wal.clone())
        .unwrap_or_default();

    let otlp_writer: Arc<dyn zradar_traits::TelemetryWriter> = if wal_config.enabled {
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

        // --- FlushSink: deserializes WAL records back into domain objects ---
        struct TelemetryFlushSink {
            writer: Arc<dyn zradar_traits::TelemetryWriter>,
        }

        #[async_trait::async_trait]
        impl FlushSink for TelemetryFlushSink {
            async fn flush_records(
                &self,
                records: &[zradar_wal::record::WalRecord],
            ) -> anyhow::Result<()> {
                use zradar_models::{LogRecord, Metric, Span};

                let mut spans: Vec<Span> = Vec::new();
                let mut metrics: Vec<Metric> = Vec::new();
                let mut logs: Vec<LogRecord> = Vec::new();

                for rec in records {
                    match rec.signal_type {
                        SignalType::Trace => {
                            let batch: Vec<Span> =
                                serde_json::from_slice(&rec.payload).map_err(|e| {
                                    anyhow::anyhow!(
                                        "WAL flush: failed to deserialize spans at offset {}: {}",
                                        rec.assigned_offset,
                                        e
                                    )
                                })?;
                            spans.extend(batch);
                        }
                        SignalType::Metric => {
                            let batch: Vec<Metric> =
                                serde_json::from_slice(&rec.payload).map_err(|e| {
                                    anyhow::anyhow!(
                                        "WAL flush: failed to deserialize metrics at offset {}: {}",
                                        rec.assigned_offset,
                                        e
                                    )
                                })?;
                            metrics.extend(batch);
                        }
                        SignalType::Log => {
                            let batch: Vec<LogRecord> = serde_json::from_slice(&rec.payload)
                                .map_err(|e| {
                                    anyhow::anyhow!(
                                        "WAL flush: failed to deserialize logs at offset {}: {}",
                                        rec.assigned_offset,
                                        e
                                    )
                                })?;
                            logs.extend(batch);
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

        let flush_sink: Arc<dyn FlushSink> = Arc::new(TelemetryFlushSink {
            writer: parquet_writer.clone(),
        });

        // Replay unflushed records before accepting traffic
        let replayer = WalReplayer::new(
            wal_dir.clone(),
            flush_sink.clone(),
            checkpoint_store.clone(),
            native_config.replay_batch_max_bytes,
        );
        let replayed = replayer.replay().await?;
        if replayed > 0 {
            info!(records = replayed, "WAL replay completed");
        }

        // Open WAL for new writes
        let wal = Arc::new(Wal::open(&wal_dir, native_config.clone(), cancel_token.clone()).await?);

        // Start flusher
        let flusher = WalFlusher::new(
            wal.clone(),
            flush_sink,
            checkpoint_store.clone(),
            native_config.flush_interval_ms,
        );
        tokio::spawn(flusher.run(cancel_token.clone()));

        // Start janitor
        let janitor = WalJanitor::new(
            wal.clone(),
            checkpoint_store,
            native_config.flush_interval_ms,
        );
        tokio::spawn(janitor.run(cancel_token.clone()));

        // Start backpressure monitor
        let bp_monitor = zradar_wal::backpressure::BackpressureMonitor::new(native_config.clone());
        tokio::spawn(zradar_wal::backpressure::backpressure_monitor_loop(
            wal.clone(),
            bp_monitor,
            cancel_token.clone(),
        ));

        info!(wal_dir = %wal_config.wal_dir, "WAL flusher + janitor + backpressure started");

        // --- WAL-backed TelemetryWriter for OTLP services ---
        struct WalTelemetryWriter {
            wal: Arc<Wal>,
        }

        #[async_trait::async_trait]
        impl zradar_traits::TelemetryWriter for WalTelemetryWriter {
            async fn insert_spans(&self, spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                if spans.is_empty() {
                    return Ok(());
                }
                let tenant_id = uuid::Uuid::parse_str(&spans[0].tenant_id).unwrap_or_default();
                let project_id = uuid::Uuid::parse_str(&spans[0].project_id).unwrap_or_default();
                let payload = serde_json::to_vec(spans)?;
                let record = WalRecord {
                    signal_type: SignalType::Trace,
                    tenant_id,
                    project_id,
                    arrival_timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    assigned_offset: 0,
                    payload: bytes::Bytes::from(payload),
                };
                let handle = self
                    .wal
                    .append(record)
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL append failed: {}", e))?;
                handle
                    .durable()
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL fsync failed: {}", e))?;
                Ok(())
            }

            async fn insert_metrics(
                &self,
                metrics: &[zradar_models::Metric],
            ) -> anyhow::Result<()> {
                if metrics.is_empty() {
                    return Ok(());
                }
                let tenant_id = uuid::Uuid::parse_str(&metrics[0].tenant_id).unwrap_or_default();
                let project_id = uuid::Uuid::parse_str(&metrics[0].project_id).unwrap_or_default();
                let payload = serde_json::to_vec(metrics)?;
                let record = WalRecord {
                    signal_type: SignalType::Metric,
                    tenant_id,
                    project_id,
                    arrival_timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    assigned_offset: 0,
                    payload: bytes::Bytes::from(payload),
                };
                let handle = self
                    .wal
                    .append(record)
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL append failed: {}", e))?;
                handle
                    .durable()
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL fsync failed: {}", e))?;
                Ok(())
            }

            async fn insert_logs(&self, logs: &[zradar_models::LogRecord]) -> anyhow::Result<()> {
                if logs.is_empty() {
                    return Ok(());
                }
                let tenant_id = uuid::Uuid::parse_str(&logs[0].tenant_id).unwrap_or_default();
                let project_id = uuid::Uuid::parse_str(&logs[0].project_id).unwrap_or_default();
                let payload = serde_json::to_vec(logs)?;
                let record = WalRecord {
                    signal_type: SignalType::Log,
                    tenant_id,
                    project_id,
                    arrival_timestamp_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    assigned_offset: 0,
                    payload: bytes::Bytes::from(payload),
                };
                let handle = self
                    .wal
                    .append(record)
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL append failed: {}", e))?;
                handle
                    .durable()
                    .await
                    .map_err(|e| anyhow::anyhow!("WAL fsync failed: {}", e))?;
                Ok(())
            }
        }

        Arc::new(WalTelemetryWriter { wal }) as Arc<dyn zradar_traits::TelemetryWriter>
    } else {
        parquet_writer.clone()
    };

    // WAL metrics (available even when WAL is disabled — returns zeros)
    let wal_metrics = Arc::new(zradar_wal::metrics::WalMetrics::new());

    // =========================================================================
    // OTLP services
    // =========================================================================

    let otlp_auth = config
        .auth
        .otlp_require_api_key
        .then(|| authenticator.clone());
    if otlp_auth.is_none() {
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
    let trace_service = OtlpTraceService::with_settings_repository(
        otlp_writer.clone(),
        otlp_auth.clone(),
        settings_repo.clone(),
        rate_limiter.clone(),
        circuit_breaker.clone(),
    );
    let metrics_service = OtlpMetricsService::with_settings_repository(
        otlp_writer.clone(),
        otlp_auth.clone(),
        settings_repo.clone(),
        rate_limiter.clone(),
        circuit_breaker.clone(),
    );
    let logs_service = OtlpLogsService::with_settings_repository(
        otlp_writer.clone(),
        otlp_auth.clone(),
        settings_repo.clone(),
        rate_limiter,
        circuit_breaker.clone(),
    );

    info!("OTLP services initialized");

    // =========================================================================
    // Start servers
    // =========================================================================

    let otlp_port = config.otlp_port;
    let admin_port = config.effective_admin_port();

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

    let health_router = health::create_health_router(health::HealthState {
        pg_pool: Some(pg_pool.clone()),
        storage_path: parquet_data_dir.clone(),
        circuit_breaker: Some(circuit_breaker),
        retention_initialized: true,
        ingestion_initialized: true,
        background_jobs_started: true,
    });
    let auth_mode = if config.auth.is_platform_mode() {
        AuthMode::Platform
    } else {
        AuthMode::Standalone
    };
    let admin_api = create_admin_router(query_service, authenticator.clone(), auth_mode);
    let retention_api = retention_router(retention_state, authenticator.clone(), auth_mode);
    let settings_api = settings_router(
        Arc::new(SettingsState {
            repository: settings_repo,
            audit_log_repo: Some(audit_log_repo.clone()),
        }),
        authenticator.clone(),
        auth_mode,
    );
    let audit_api = audit_router(
        Arc::new(AuditState {
            repository: audit_log_repo,
        }),
        authenticator,
        auth_mode,
    );
    let metrics_state = wal_metrics.clone();
    let metrics_route = axum::routing::get(move || {
        let m = metrics_state.clone();
        async move { m.render_prometheus() }
    });

    let app = Router::new()
        .route("/metrics", metrics_route)
        .merge(health_router)
        .merge(admin_api)
        .merge(retention_api)
        .merge(settings_api)
        .merge(audit_api)
        .layer(CompressionLayer::new());

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
