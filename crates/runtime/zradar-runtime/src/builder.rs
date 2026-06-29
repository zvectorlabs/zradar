//! [`ZradarRuntimeBuilder`] — owns server startup, accepts pluggable auth.
//!
//! Call sites:
//! - OSS `zradar-server/src/main.rs` — config-key `Authenticator` + query/admin authorizers.
//! - External platform wrapper binaries — supply their own auth implementations.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use api::{
    audit::{AuditState, audit_router},
    grpc::{
        admin_proto::{
            audit_service_server::AuditServiceServer, policy_service_server::PolicyServiceServer,
            retention_service_server::RetentionServiceServer,
            settings_service_server::SettingsServiceServer,
        },
        analytics_handler::AnalyticsHandler,
        audit_handler::AuditHandler,
        policy_handler::PolicyHandler,
        query_handler::QueryHandler,
        query_proto::{
            analytics_service_server::AnalyticsServiceServer,
            query_service_server::QueryServiceServer,
        },
        retention_handler::RetentionHandler,
        settings_handler::SettingsHandler,
    },
    http::{AuthMode, create_admin_router},
    policy::{PolicyState, policy_router},
    retention::{handlers::RetentionState, retention_router},
    settings::{SettingsState, settings_router},
    telemetry::QueryService,
};
use api_optel::{
    CircuitBreaker, LogsServiceServer, MetricsServiceServer, OtlpLogsService, OtlpMetricsService,
    OtlpTraceService, ProjectRateLimiter, TraceServiceServer, otlp_http_router,
};
use axum::Router;
use sqlx::postgres::PgPoolOptions;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tower_http::compression::CompressionLayer;

use zradar_models::Config;
use zradar_parquet::{
    Compactor, DiskCache, FileLeaseRegistry, FileMover, FlushWorker, MemoryCache,
    ParquetFileReader, ParquetFileWriter, ParquetTelemetryReader, ParquetTelemetryWriter,
    WriteBuffer, WriterConfig, recover_incomplete_writes,
};
use zradar_plugin_postgres::{
    PostgresAuditLogRepository, PostgresClient, PostgresDecisionAuditSink,
    PostgresFileListRepository, PostgresPolicyStore, PostgresSettingsRepository,
    PostgresStorageUsageRepository, PostgresThresholdSink, PostgresUsageReader,
    PostgresUsageTracker, UsageTrackerMetrics, migrations::MIGRATIONS,
};
use zradar_plugin_s3::S3BlockStorage;
use zradar_policy::{
    DecisionAuditSink, FanoutUsageTracker, InMemoryUsageTracker, PolicyEnforcer, PolicyEngine,
    PolicyStore, ThresholdSink, UsageAnalyticsReader, UsageReader, UsageTracker,
};
use zradar_retention::{
    CleanupJob, EnforcementStrategy, FileReclaimer, QueryEnforcer, RetentionConfigStore,
    StorageUsageDailyJob, WorkspaceRetentionConfig,
};
use zradar_traits::{AdminAuthorizer, Authenticator, QueryAuthorizer};

/// Authentication strategies injected by the binary.
///
/// - `otlp` — validates OTLP ingest bearer tokens.
/// - `query` — validates read/query HTTP and gRPC (:8081) requests.
/// - `admin` — validates admin HTTP and gRPC (:8082) requests.
pub struct RuntimeAuth {
    /// OTLP authenticator. `None` = open ingest (no key required).
    pub otlp: Option<Arc<dyn Authenticator>>,
    /// Query/read-path authorizer (HTTP query routes, gRPC :8081).
    pub query: Arc<dyn QueryAuthorizer>,
    /// Admin/mutation-path authorizer (HTTP admin routes, gRPC :8082).
    pub admin: Arc<dyn AdminAuthorizer>,
}

/// Builds and runs the zradar server runtime.
///
/// Accepts [`RuntimeAuth`] from the caller so the binary decides which
/// auth implementations to wire in without this crate knowing about them.
pub struct ZradarRuntimeBuilder {
    config: Config,
    auth: RuntimeAuth,
    policy_overrides: RuntimePolicyOverrides,
}

#[derive(Default)]
struct RuntimePolicyOverrides {
    policy_store: Option<Arc<dyn PolicyStore>>,
    usage_reader: Option<Arc<dyn UsageReader>>,
    reporting_usage_reader: Option<Arc<dyn UsageReader>>,
    usage_analytics_reader: Option<Arc<dyn UsageAnalyticsReader>>,
    usage_tracker: Option<Arc<dyn UsageTracker>>,
    policy_enforcer: Option<Arc<dyn PolicyEnforcer>>,
    threshold_sink: Option<Arc<dyn ThresholdSink>>,
    decision_audit_sink: Option<Arc<dyn DecisionAuditSink>>,
}

struct PolicyRuntimeComponents {
    policy_store: Arc<dyn PolicyStore>,
    reporting_usage_reader: Arc<dyn UsageReader>,
    usage_analytics_reader: Arc<dyn UsageAnalyticsReader>,
    usage_tracker: Arc<dyn UsageTracker>,
    policy_enforcer: Arc<dyn PolicyEnforcer>,
    usage_tracker_metrics: Arc<UsageTrackerMetrics>,
    policy_store_refresh: Option<Arc<PostgresPolicyStore>>,
}

async fn build_policy_runtime_components(
    pg_client: Arc<PostgresClient>,
    overrides: RuntimePolicyOverrides,
) -> Result<PolicyRuntimeComponents> {
    let RuntimePolicyOverrides {
        policy_store,
        usage_reader,
        reporting_usage_reader,
        usage_analytics_reader,
        usage_tracker,
        policy_enforcer,
        threshold_sink,
        decision_audit_sink,
    } = overrides;

    let (policy_store, policy_store_refresh) = if let Some(policy_store) = policy_store {
        (policy_store, None)
    } else {
        let policy_store_impl = Arc::new(PostgresPolicyStore::new(pg_client.clone()));
        policy_store_impl.refresh().await?;
        (
            policy_store_impl.clone() as Arc<dyn PolicyStore>,
            Some(policy_store_impl),
        )
    };

    let hot_usage = Arc::new(InMemoryUsageTracker::new());
    let usage_reader = usage_reader.unwrap_or_else(|| hot_usage.clone() as Arc<dyn UsageReader>);
    let reporting_usage_reader = reporting_usage_reader.unwrap_or_else(|| {
        Arc::new(PostgresUsageReader::new(pg_client.clone())) as Arc<dyn UsageReader>
    });
    let usage_analytics_reader = usage_analytics_reader.unwrap_or_else(|| {
        Arc::new(PostgresUsageReader::new(pg_client.clone())) as Arc<dyn UsageAnalyticsReader>
    });
    let usage_tracker_metrics = Arc::new(UsageTrackerMetrics::default());
    let usage_tracker = usage_tracker.unwrap_or_else(|| {
        let postgres_usage_tracker = Arc::new(PostgresUsageTracker::spawn_with_metrics(
            pg_client.clone(),
            usage_tracker_metrics.clone(),
        )) as Arc<dyn UsageTracker>;
        Arc::new(FanoutUsageTracker::new(vec![
            hot_usage.clone() as Arc<dyn UsageTracker>,
            postgres_usage_tracker,
        ])) as Arc<dyn UsageTracker>
    });
    let threshold_sink = threshold_sink.unwrap_or_else(|| {
        Arc::new(PostgresThresholdSink::new(pg_client.clone())) as Arc<dyn ThresholdSink>
    });
    let decision_audit_sink = decision_audit_sink.unwrap_or_else(|| {
        Arc::new(PostgresDecisionAuditSink::new(pg_client.clone())) as Arc<dyn DecisionAuditSink>
    });
    let policy_enforcer = policy_enforcer.unwrap_or_else(|| {
        let policy_engine = PolicyEngine::new_with_decision_audit(
            policy_store.clone(),
            usage_reader,
            usage_tracker.clone(),
            threshold_sink,
            decision_audit_sink,
        );
        policy_engine.enforcer.clone() as Arc<dyn PolicyEnforcer>
    });

    Ok(PolicyRuntimeComponents {
        policy_store,
        reporting_usage_reader,
        usage_analytics_reader,
        usage_tracker,
        policy_enforcer,
        usage_tracker_metrics,
        policy_store_refresh,
    })
}

impl ZradarRuntimeBuilder {
    /// Create a new builder from a loaded config and caller-provided auth.
    pub fn new(config: Config, auth: RuntimeAuth) -> Self {
        Self {
            config,
            auth,
            policy_overrides: RuntimePolicyOverrides::default(),
        }
    }

    pub fn with_policy_store(mut self, policy_store: Arc<dyn PolicyStore>) -> Self {
        self.policy_overrides.policy_store = Some(policy_store);
        self
    }

    pub fn with_usage_reader(mut self, usage_reader: Arc<dyn UsageReader>) -> Self {
        self.policy_overrides.usage_reader = Some(usage_reader);
        self
    }

    pub fn with_reporting_usage_reader(mut self, usage_reader: Arc<dyn UsageReader>) -> Self {
        self.policy_overrides.reporting_usage_reader = Some(usage_reader);
        self
    }

    pub fn with_usage_analytics_reader(
        mut self,
        usage_analytics_reader: Arc<dyn UsageAnalyticsReader>,
    ) -> Self {
        self.policy_overrides.usage_analytics_reader = Some(usage_analytics_reader);
        self
    }

    pub fn with_usage_tracker(mut self, usage_tracker: Arc<dyn UsageTracker>) -> Self {
        self.policy_overrides.usage_tracker = Some(usage_tracker);
        self
    }

    pub fn with_policy_enforcer(mut self, policy_enforcer: Arc<dyn PolicyEnforcer>) -> Self {
        self.policy_overrides.policy_enforcer = Some(policy_enforcer);
        self
    }

    pub fn with_threshold_sink(mut self, threshold_sink: Arc<dyn ThresholdSink>) -> Self {
        self.policy_overrides.threshold_sink = Some(threshold_sink);
        self
    }

    pub fn with_decision_audit_sink(
        mut self,
        decision_audit_sink: Arc<dyn DecisionAuditSink>,
    ) -> Self {
        self.policy_overrides.decision_audit_sink = Some(decision_audit_sink);
        self
    }

    /// Run the OTLP gRPC and Admin HTTP servers. Blocks until either exits.
    pub async fn run(self) -> Result<()> {
        let config = self.config;
        let policy_overrides = self.policy_overrides;
        let RuntimeAuth {
            otlp: otlp_auth,
            query: query_authorizer,
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
        let raw_settings_repo = Arc::new(PostgresSettingsRepository::new(pg_client.clone()))
            as Arc<dyn zradar_traits::SettingsRepository>;
        let settings_repo = Arc::new(zradar_traits::CachedSettingsRepository::new(
            raw_settings_repo,
            std::time::Duration::from_secs(5),
        )) as Arc<dyn zradar_traits::SettingsRepository>;
        let storage_usage_repo = Arc::new(PostgresStorageUsageRepository::new(pg_client.clone()))
            as Arc<dyn zradar_traits::StorageUsageRepository>;
        let audit_log_repo = Arc::new(PostgresAuditLogRepository::new(pg_client.clone()))
            as Arc<dyn zradar_traits::AuditLogRepository>;
        let policy_components =
            build_policy_runtime_components(pg_client.clone(), policy_overrides).await?;
        let PolicyRuntimeComponents {
            policy_store,
            reporting_usage_reader,
            usage_analytics_reader,
            usage_tracker,
            policy_enforcer,
            usage_tracker_metrics,
            policy_store_refresh,
        } = policy_components;

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

        // Single registry shared between the reader (acquires leases on scan),
        // FileMover (defers local delete while leased), and FileReclaimer
        // (the sole physical-deletion chokepoint for soft-deleted files).
        let file_lease_registry = Arc::new(FileLeaseRegistry::new());

        let cancel_token = CancellationToken::new();
        if let Some(policy_store_refresh) = policy_store_refresh {
            let cancel = cancel_token.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(30));
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            if let Err(e) = policy_store_refresh.refresh().await {
                                error!("Policy store refresh failed: {}", e);
                            }
                        }
                        _ = cancel.cancelled() => break,
                    }
                }
            });
        }

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
                        )
                        .with_lease_registry(file_lease_registry.clone());
                        tokio::spawn(mover.run(cancel_token.clone()));
                        info!("FileMover background task started");

                        Some(s3)
                    }
                    Err(e) => {
                        error!("Failed to initialize S3 block storage: {}", e);
                        return Err(e);
                    }
                }
            } else {
                None
            };

        let parquet_file_reader: Arc<ParquetFileReader> =
            if let Some(s3_storage) = s3_block_storage.clone() {
                let disk_cache = Arc::new(DiskCache::new(s3_storage, &parquet_lifecycle_config));
                let reader = if let Some(mc) = memory_cache.clone() {
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
                };
                Arc::new(reader.with_lease_registry(file_lease_registry.clone()))
            } else {
                let reader = if let Some(mc) = memory_cache.clone() {
                    ParquetFileReader::with_memory_cache(
                        parquet_data_dir.clone(),
                        file_list_repo.clone(),
                        mc,
                    )
                } else {
                    ParquetFileReader::new(parquet_data_dir.clone(), file_list_repo.clone())
                };
                Arc::new(reader.with_lease_registry(file_lease_registry.clone()))
            };

        let parquet_telemetry_reader =
            Arc::new(ParquetTelemetryReader::new(parquet_file_reader.clone()));

        // =====================================================================
        // Retention system
        // =====================================================================

        let global_retention_days = parquet_lifecycle_config.retention_days;
        let retention_config_store = Arc::new(RetentionConfigStore::new(global_retention_days));
        for policy in settings_repo.list_all_settings().await? {
            retention_config_store.upsert(WorkspaceRetentionConfig {
                workspace_id: policy.workspace_id,
                retention_days: policy.traces_retention_days as u32,
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

        let file_reclaimer = Arc::new(
            if let Some(s3_storage) = s3_block_storage.clone() {
                FileReclaimer::with_storage(
                    file_list_repo.clone(),
                    s3_storage,
                    file_lease_registry.clone(),
                    parquet_lifecycle_config.retention_check_interval_secs,
                )
            } else {
                FileReclaimer::new(
                    file_list_repo.clone(),
                    file_lease_registry.clone(),
                    parquet_lifecycle_config.retention_check_interval_secs,
                )
            }
            .with_storage_usage_repository(storage_usage_repo.clone()),
        );
        {
            let job = file_reclaimer.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move { job.run(cancel).await });
        }
        info!("FileReclaimer background task started");

        let storage_usage_daily_job = Arc::new(StorageUsageDailyJob::new(
            file_list_repo.clone(),
            storage_usage_repo.clone(),
            parquet_lifecycle_config.storage_snapshot_interval_secs,
        ));
        {
            let job = storage_usage_daily_job.clone();
            let cancel = cancel_token.clone();
            tokio::spawn(async move { job.run(cancel).await });
        }
        info!("StorageUsageDailyJob background task started");

        let retention_state = Arc::new(RetentionState {
            cleanup_job: cleanup_job.clone(),
            file_reclaimer: file_reclaimer.clone(),
            config_store: retention_config_store.clone(),
            settings_repo: settings_repo.clone(),
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
            .with_storage_usage_repo(storage_usage_repo.clone())
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

        // WAL is mandatory: every accepted OTLP request is durably appended
        // before ack, then drained to Parquet by the background flusher. There
        // is no direct (non-WAL) ingest path.
        let otlp_writer: Arc<dyn zradar_traits::TelemetryWriter> = {
            // The WAL writer wraps the Parquet writer and applies backpressure.
            // Inline here to avoid re-exporting WAL internals from this crate.
            use zradar_wal::Wal;
            use zradar_wal::batch::{decode as decode_batch, encode_json_rows};
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
                    use zradar_models::{EvaluationScore, IngestBatch, LogRecord, Metric, Span};

                    // A `WalRecord` may now carry either a batch envelope (one
                    // append covering many rows) or a legacy single-row JSON
                    // payload from a pre-upgrade segment. The envelope decoder
                    // distinguishes the two by magic prefix.
                    let mut spans: Vec<Span> = Vec::new();
                    let mut metrics: Vec<Metric> = Vec::new();
                    let mut logs: Vec<LogRecord> = Vec::new();
                    let mut scores: Vec<EvaluationScore> = Vec::new();

                    let push_one =
                        |signal: SignalType,
                         row: &[u8],
                         spans: &mut Vec<Span>,
                         metrics: &mut Vec<Metric>,
                         logs: &mut Vec<LogRecord>,
                         scores: &mut Vec<EvaluationScore>| {
                            match signal {
                                SignalType::Trace => {
                                    if let Ok(span) = serde_json::from_slice::<Span>(row) {
                                        spans.push(span);
                                    }
                                }
                                SignalType::Metric => {
                                    if let Ok(m) = serde_json::from_slice::<Metric>(row) {
                                        metrics.push(m);
                                    }
                                }
                                SignalType::Log => {
                                    if let Ok(log) = serde_json::from_slice::<LogRecord>(row) {
                                        logs.push(log);
                                    }
                                }
                                SignalType::Score => {
                                    if let Ok(score) =
                                        serde_json::from_slice::<EvaluationScore>(row)
                                    {
                                        scores.push(score);
                                    }
                                }
                            }
                        };

                    for record in records {
                        match decode_batch(&record.payload) {
                            Ok(Some(batch)) => {
                                for row in &batch.rows {
                                    push_one(
                                        record.signal_type,
                                        row,
                                        &mut spans,
                                        &mut metrics,
                                        &mut logs,
                                        &mut scores,
                                    );
                                }
                            }
                            Ok(None) => {
                                // Legacy single-row payload from a pre-upgrade segment.
                                push_one(
                                    record.signal_type,
                                    &record.payload,
                                    &mut spans,
                                    &mut metrics,
                                    &mut logs,
                                    &mut scores,
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    offset = record.assigned_offset,
                                    error = %e,
                                    "skipping WAL record with malformed batch envelope"
                                );
                            }
                        }
                    }

                    // Hand ownership to the writer's move-based batch path
                    // (re-arch Phase B): the flusher already owns these Vecs,
                    // so `insert_batch` moves rows into the write buffer instead
                    // of `extend_from_slice`-cloning every row.
                    if !spans.is_empty() {
                        self.writer.insert_batch(IngestBatch::spans(spans)).await?;
                    }
                    if !metrics.is_empty() {
                        self.writer
                            .insert_batch(IngestBatch::metrics(metrics))
                            .await?;
                    }
                    if !logs.is_empty() {
                        self.writer.insert_batch(IngestBatch::logs(logs)).await?;
                    }
                    if !scores.is_empty() {
                        self.writer
                            .insert_batch(IngestBatch::scores(scores))
                            .await?;
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

            impl WalTelemetryWriter {
                /// Group `rows` by `(workspace_id)`, serialize each row to
                /// JSON, frame each group into one batch envelope, and append +
                /// fsync once per group. The result is one durable wait per group
                /// rather than per row — the contract widening behind R-arch P0-3.
                async fn append_batches<T>(
                    &self,
                    signal: SignalType,
                    rows: &[T],
                    workspace_of: impl Fn(&T) -> &str,
                ) -> anyhow::Result<()>
                where
                    T: serde::Serialize,
                {
                    if rows.is_empty() {
                        return Ok(());
                    }

                    // Group while preserving the order rows arrived in for each
                    // (workspace) — the WAL replay path relies on offset
                    // ordering to preserve causal order per workspace.
                    let mut groups: std::collections::BTreeMap<uuid::Uuid, Vec<Vec<u8>>> =
                        std::collections::BTreeMap::new();
                    for row in rows {
                        let workspace_id =
                            uuid::Uuid::parse_str(workspace_of(row)).unwrap_or(uuid::Uuid::nil());
                        let bytes = serde_json::to_vec(row)
                            .map_err(|e| anyhow::anyhow!("WAL row serialize: {}", e))?;
                        groups.entry(workspace_id).or_default().push(bytes);
                    }

                    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                    let mut handles = Vec::with_capacity(groups.len());

                    for (workspace_id, payloads) in groups {
                        let envelope = encode_json_rows(payloads.iter().map(|v| v.as_slice()));
                        let record = WalRecord {
                            signal_type: signal,
                            workspace_id: workspace_id.into(),
                            arrival_timestamp_ns: now_ns,
                            assigned_offset: 0,
                            payload: envelope,
                        };
                        let handle = self
                            .wal
                            .append(record)
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL append: {}", e))?;
                        handles.push(handle);
                    }

                    for handle in handles {
                        handle
                            .durable()
                            .await
                            .map_err(|e| anyhow::anyhow!("WAL fsync: {}", e))?;
                    }
                    Ok(())
                }
            }

            #[async_trait::async_trait]
            impl zradar_traits::TelemetryWriter for WalTelemetryWriter {
                async fn insert_spans(&self, spans: &[zradar_models::Span]) -> anyhow::Result<()> {
                    self.append_batches(SignalType::Trace, spans, |s| s.workspace_id.as_str())
                        .await
                }

                async fn insert_metrics(
                    &self,
                    metrics: &[zradar_models::Metric],
                ) -> anyhow::Result<()> {
                    self.append_batches(SignalType::Metric, metrics, |m| m.workspace_id.as_str())
                        .await
                }

                async fn insert_logs(
                    &self,
                    logs: &[zradar_models::LogRecord],
                ) -> anyhow::Result<()> {
                    self.append_batches(SignalType::Log, logs, |l| l.workspace_id.as_str())
                        .await
                }

                async fn insert_scores(
                    &self,
                    scores: &[zradar_models::EvaluationScore],
                ) -> anyhow::Result<()> {
                    self.append_batches(SignalType::Score, scores, |s| s.workspace_id.as_str())
                        .await
                }
            }

            Arc::new(WalTelemetryWriter { wal }) as Arc<dyn zradar_traits::TelemetryWriter>
        };

        let wal_metrics = Arc::new(zradar_wal::metrics::WalMetrics::new());

        // End-to-end ingest metrics hub. System-level by default; per-workspace
        // series are gated by `MetricsPolicy` (swap `ObserveNone` for a
        // settings-backed policy to track key tenants). Rendered on `/metrics`
        // alongside the WAL metrics. Ack-path/storage emit sites are wired into
        // the services/jobs incrementally via this handle.
        let metrics_hub = Arc::new(zradar_metrics::MetricsHub::new(Arc::new(
            zradar_metrics::ObserveNone,
        )));

        // =====================================================================
        // OTLP services
        // =====================================================================

        let otlp_auth_ext = otlp_auth;
        let allow_test_header_context = config.auth.allow_test_header_context;
        if allow_test_header_context {
            info!("TEST ONLY: x-tenant-id/x-project-id header context overrides are enabled");
        }
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
            let hub = metrics_hub.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            breaker.set_queue_depth(0);
                            hub.set_queue_depth(0);
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            let depth = buffer.record_count() as u64;
                            breaker.set_queue_depth(depth);
                            hub.set_queue_depth(depth);
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
        )
        .with_test_header_context(allow_test_header_context);
        let metrics_service = OtlpMetricsService::with_settings_and_policy(
            otlp_writer.clone(),
            otlp_auth_ext.clone(),
            settings_repo.clone(),
            rate_limiter.clone(),
            policy_enforcer.clone(),
            circuit_breaker.clone(),
        )
        .with_test_header_context(allow_test_header_context);
        let logs_service = OtlpLogsService::with_settings_and_policy(
            otlp_writer.clone(),
            otlp_auth_ext.clone(),
            settings_repo.clone(),
            rate_limiter.clone(),
            policy_enforcer.clone(),
            circuit_breaker.clone(),
        )
        .with_test_header_context(allow_test_header_context);

        info!("OTLP services initialized");

        // =====================================================================
        // Admin HTTP routers
        // =====================================================================

        let otlp_port = config.otlp_port;
        let otlp_http_port = config.otlp_http_port;
        let admin_port = config.effective_admin_port();

        let health_router = crate::health::create_health_router(crate::health::HealthState {
            pg_pool: Some(pg_pool.clone()),
            storage_path: parquet_data_dir.clone(),
            circuit_breaker: Some(circuit_breaker.clone()),
            retention_initialized: true,
            ingestion_initialized: true,
            background_jobs_started: true,
        });

        let auth_mode = AuthMode::Standalone;

        let settings_state = Arc::new(SettingsState {
            repository: settings_repo.clone(),
            audit_log_repo: Some(audit_log_repo.clone()),
        });
        let policy_state = Arc::new(PolicyState {
            store: policy_store.clone(),
        });
        let audit_state = Arc::new(AuditState {
            repository: audit_log_repo.clone(),
        });

        // Pass AdminAuthorizer directly to each router. The `AuthContext` extractor
        // calls `authorizer.authorize(&parts.headers)` with the original request
        // headers, preserving all trusted context headers set by gateway wrappers.
        let admin_api =
            create_admin_router(query_service.clone(), query_authorizer.clone(), auth_mode);
        let retention_api =
            retention_router(retention_state.clone(), admin_authorizer.clone(), auth_mode);
        let settings_api =
            settings_router(settings_state.clone(), admin_authorizer.clone(), auth_mode);
        let policy_api = policy_router(policy_state.clone(), admin_authorizer.clone(), auth_mode);
        let audit_api = audit_router(audit_state.clone(), admin_authorizer.clone(), auth_mode);

        let metrics_state = wal_metrics.clone();
        let usage_metrics_state = usage_tracker_metrics.clone();
        let ingest_metrics_state = metrics_hub.clone();
        let metrics_route = axum::routing::get(move || {
            let m = metrics_state.clone();
            let usage_m = usage_metrics_state.clone();
            let ingest_m = ingest_metrics_state.clone();
            async move {
                let mut output = m.render_prometheus();
                output.push_str(&usage_m.render_prometheus());
                output.push_str(&ingest_m.render());
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
        // Query gRPC server (port 8081)
        // =====================================================================

        let query_grpc_port = config.effective_query_grpc_port();
        let query_grpc_addr: std::net::SocketAddr =
            format!("0.0.0.0:{query_grpc_port}").parse().unwrap();

        let query_handler = QueryHandler::new(query_service.clone(), query_authorizer.clone());
        let analytics_handler =
            AnalyticsHandler::new(query_service.clone(), query_authorizer.clone());

        let query_reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(api::grpc::query_proto::QUERY_FILE_DESCRIPTOR_SET)
            .build_v1()
            .expect("failed to build query reflection service");

        let query_grpc_server = async move {
            info!(port = query_grpc_port, "Starting Query gRPC server");
            Server::builder()
                .accept_http1(true)
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(QueryServiceServer::new(query_handler))
                .add_service(AnalyticsServiceServer::new(analytics_handler))
                .add_service(query_reflection)
                .serve(query_grpc_addr)
                .await
                .map_err(|e| anyhow::anyhow!("Query gRPC server failed: {}", e))
        };

        // =====================================================================
        // Admin gRPC server (port 8082)
        // =====================================================================

        let admin_grpc_port = config.effective_admin_grpc_port();
        let admin_grpc_addr: std::net::SocketAddr =
            format!("0.0.0.0:{admin_grpc_port}").parse().unwrap();

        let retention_handler =
            RetentionHandler::new(retention_state.clone(), admin_authorizer.clone());
        let policy_handler = PolicyHandler::new(policy_state.clone(), admin_authorizer.clone());
        let audit_handler = AuditHandler::new(audit_state.clone(), admin_authorizer.clone());
        let settings_handler =
            SettingsHandler::new(settings_state.clone(), admin_authorizer.clone());

        let admin_reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(api::grpc::admin_proto::ADMIN_FILE_DESCRIPTOR_SET)
            .build_v1()
            .expect("failed to build admin reflection service");

        let admin_grpc_server = async move {
            info!(port = admin_grpc_port, "Starting Admin gRPC server");
            Server::builder()
                .accept_http1(true)
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(RetentionServiceServer::new(retention_handler))
                .add_service(PolicyServiceServer::new(policy_handler))
                .add_service(AuditServiceServer::new(audit_handler))
                .add_service(SettingsServiceServer::new(settings_handler))
                .add_service(admin_reflection)
                .serve(admin_grpc_addr)
                .await
                .map_err(|e| anyhow::anyhow!("Admin gRPC server failed: {}", e))
        };

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

        let otlp_http_server = if otlp_http_port > 0 {
            let otlp_http_app = otlp_http_router(
                otlp_writer.clone(),
                otlp_auth_ext.clone(),
                allow_test_header_context,
                settings_repo.clone(),
                rate_limiter.clone(),
                policy_enforcer.clone(),
                circuit_breaker.clone(),
            );
            let otlp_http_addr = format!("0.0.0.0:{}", otlp_http_port);
            Some(async move {
                info!("OTLP/HTTP receiver listening on http://{}", otlp_http_addr);
                let listener = tokio::net::TcpListener::bind(&otlp_http_addr).await?;
                axum::serve(listener, otlp_http_app)
                    .await
                    .map_err(|e| anyhow::anyhow!("OTLP/HTTP server failed: {}", e))
            })
        } else {
            info!("OTLP/HTTP receiver disabled (otlp_http_port = 0)");
            None
        };

        info!("zradar is ready!");
        info!("  OTLP gRPC:      localhost:{}", otlp_port);
        info!("  OTLP/HTTP:      http://localhost:{}", otlp_http_port);
        info!("  Admin API:      http://localhost:{}", admin_port);
        info!("  Query gRPC:     localhost:{}", query_grpc_port);
        info!("  Admin gRPC:     localhost:{}", admin_grpc_port);

        if let Some(otlp_http_server) = otlp_http_server {
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
                result = otlp_http_server => {
                    if let Err(e) = result {
                        error!("OTLP/HTTP server failed: {}", e);
                    }
                }
                result = query_grpc_server => {
                    if let Err(e) = result {
                        error!("Query gRPC server failed: {}", e);
                    }
                }
                result = admin_grpc_server => {
                    if let Err(e) = result {
                        error!("Admin gRPC server failed: {}", e);
                    }
                }
            }
        } else {
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
                result = query_grpc_server => {
                    if let Err(e) = result {
                        error!("Query gRPC server failed: {}", e);
                    }
                }
                result = admin_grpc_server => {
                    if let Err(e) = result {
                        error!("Admin gRPC server failed: {}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
