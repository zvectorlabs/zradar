//! zradar Server - Runs both OTLP gRPC and Admin HTTP API
//!
//! Plugin-based architecture for flexible telemetry storage backends.

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

// OTLP
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;

// Axum
use axum::Router;
use tower_http::compression::CompressionLayer;

// Control plane
use api::{
    api_keys::service::ApiKeyService,
    // Auth
    auth::{ApiKeyAuth as DbApiKeyAuth, JwtAuth},
    // HTTP router
    http::create_admin_router,
    organizations::OrganizationService,
    projects::ProjectService,
    // RBAC
    rbac::RbacService,
    roles::RoleService,
    scores::ScoresService,
    telemetry::QueryService,
    // Services from domain modules
    users::AuthService,
};

// Plugins - individual repositories
use zradar_plugin_postgres::{
    PostgresApiKeyRepository, PostgresAuditLogger, PostgresClient, PostgresOrganizationRepository,
    PostgresProjectRepository, PostgresRoleRepository, PostgresUserRepository,
    migrations::PostgresMigrationProvider,
};
// Note: ClickHouse removed - using only Postgres for now
// TODO: Add plugin-based storage selection via configuration
use zradar_plugins::{PluginLoader, PluginRegistry};

// Migration system
use zradar_migrations::MigrationRegistry;

// Local modules
mod health;

use api_optel::{
    ApiKeyAuth as GrpcApiKeyAuth, DirectLogHandler, DirectSpanHandler, JobQueueSpanHandler,
    NullLogHandler, NullScoreHandler, OtlpLogsService, OtlpMetricsService, OtlpTraceService,
};
use zradar_models::Config;
use zradar_parquet::{
    ParquetFileReader, ParquetFileWriter, ParquetTelemetryReader, ParquetTelemetryWriter,
};
use zradar_plugin_local::LocalBlockStorage;
use zradar_plugin_postgres::{PostgresFileListRepository, PostgresJobQueue};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,zradar=debug")),
        )
        .init();

    info!("🚀 Starting zradar server...");

    // Load configuration
    let config = Config::load()?;
    info!("✅ Configuration loaded");

    // ========================================================================
    // Initialize Plugin System
    // ========================================================================

    let registry = PluginRegistry::new();
    let plugin_loader = PluginLoader::default();

    // Load plugin configuration if it exists
    let plugins_config_path = "config/plugins.toml";
    if std::path::Path::new(plugins_config_path).exists() {
        match zradar_plugins::PluginConfig::from_file(plugins_config_path) {
            Ok(config) => {
                if let Err(e) = plugin_loader.load_from_config(&config, &registry).await {
                    info!("⏭️  Plugin loading warning: {}", e);
                } else {
                    info!("✅ Loaded {} plugins from config", config.enabled.len());
                }
            }
            Err(e) => info!("⏭️  No plugins loaded: {}", e),
        }
    } else {
        info!("⏭️  No plugins config found, using defaults");
    }

    info!(
        "📦 Plugin registry: {} plugins available",
        registry.list_all_plugins().len()
    );

    // ========================================================================
    // Database Connections
    // ========================================================================

    // Connect to PostgreSQL
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

    info!("✅ Connected to PostgreSQL");

    // ========================================================================
    // Migration System - Centralized Registry
    // ========================================================================

    info!("🔄 Initializing migration registry...");

    let pg_pool_arc = Arc::new(pg_pool.clone());
    let migration_registry = MigrationRegistry::new(pg_pool_arc.clone())
        .await
        .expect("Failed to initialize migration registry");

    // Register PostgreSQL migration provider
    let postgres_provider = PostgresMigrationProvider::new(pg_pool_arc.clone());
    migration_registry.register_plugin(Arc::new(postgres_provider));

    // Run migrations if enabled (or on first run)
    let auto_migrate = config
        .migrations
        .as_ref()
        .map(|m| m.auto_migrate)
        .unwrap_or(true);

    info!("🔄 Running migrations (auto_migrate: {})...", auto_migrate);

    match migration_registry.run_all_migrations(auto_migrate).await {
        Ok(summary) => {
            info!(
                "✅ Migration completed: {} successful, {} failed",
                summary.successful, summary.failed
            );

            for result in &summary.plugin_results {
                info!(
                    "  📦 {}: {} migrations applied in {}ms ({})",
                    result.plugin_name,
                    result.migrations_applied,
                    result.duration_ms,
                    result.status
                );
            }

            if summary.failed > 0 {
                error!("❌ Some migrations failed");
                for error in &summary.errors {
                    error!("  - {}", error);
                }
                return Err(anyhow::anyhow!("Migration failed"));
            }
        }
        Err(e) => {
            error!("❌ Migration system failed: {}", e);
            return Err(e);
        }
    }

    // TODO: Add ClickHouse/other storage via plugin configuration later
    info!("📊 Using Postgres for all storage (telemetry, scores, etc.)");

    // ========================================================================
    // Create Repositories (direct, no wrapper)
    // ========================================================================

    let pg_pool = Arc::new(pg_pool);
    let pg_client = Arc::new(PostgresClient::from_pool(pg_pool.clone()));

    // Individual repositories
    let user_repo = Arc::new(PostgresUserRepository::new(pg_client.clone()));
    let org_repo = Arc::new(PostgresOrganizationRepository::new(pg_client.clone()));
    let project_repo = Arc::new(PostgresProjectRepository::new(pg_client.clone()));
    let api_key_repo = Arc::new(PostgresApiKeyRepository::new(pg_client.clone()));
    let role_repo = Arc::new(PostgresRoleRepository::new(pg_client.clone()));
    let audit_logger = Arc::new(PostgresAuditLogger::new(pg_client.clone()));

    // RBAC service (uses role repo for permission definitions)
    let rbac_service = Arc::new(RbacService::new(role_repo.clone()));
    rbac_service.initialize().await?;
    info!("✅ RBAC service initialized");

    // ========================================================================
    // Authentication Services
    // ========================================================================

    let jwt_secret = config
        .admin_api
        .as_ref()
        .and_then(|a| a.jwt_secret.clone())
        .unwrap_or_else(|| "default-secret-change-me".to_string());

    let jwt_expiry_hours = config
        .admin_api
        .as_ref()
        .and_then(|a| a.jwt_expiry_hours)
        .unwrap_or(24);

    let jwt_auth = Arc::new(JwtAuth::new(jwt_secret, jwt_expiry_hours));

    let cache_ttl = config.auth.cache_ttl_seconds.unwrap_or(300);

    let db_api_key_auth = Arc::new(DbApiKeyAuth::new(
        api_key_repo.clone(),
        audit_logger.clone(),
        1000,
        cache_ttl,
    ));

    info!("✅ Authentication services initialized");

    // ========================================================================
    // HTTP Handler Services
    // ========================================================================

    let auth_service = Arc::new(AuthService {
        user_storage: user_repo.clone(),
        jwt_auth: jwt_auth.clone(),
        audit: audit_logger.clone(),
    });

    let org_service = Arc::new(OrganizationService {
        org_storage: org_repo.clone(),
        user_storage: user_repo.clone(),
        rbac: rbac_service.clone(),
        audit: audit_logger.clone(),
    });

    let project_service = Arc::new(ProjectService {
        project_storage: project_repo.clone(),
        user_storage: user_repo.clone(),
        rbac: rbac_service.clone(),
        audit: audit_logger.clone(),
    });

    let api_key_service = Arc::new(ApiKeyService::new(
        api_key_repo.clone(),
        project_repo.clone(),
        rbac_service.clone(),
        audit_logger.clone(),
        db_api_key_auth.clone(),
    ));

    let role_service = Arc::new(RoleService {
        role_storage: role_repo.clone(),
        rbac: rbac_service.clone(),
        audit: audit_logger.clone(),
    });

    // ---------------------------------------------------------------------------
    // Parquet storage layer (shared by write and read paths)
    // ---------------------------------------------------------------------------

    let parquet_data_dir = std::path::PathBuf::from(
        config
            .ingestor
            .as_ref()
            .map(|i| i.storage.parquet_data_dir.clone())
            .unwrap_or_else(|| "./data/parquet-files".to_string()),
    );

    let file_list_repo = Arc::new(PostgresFileListRepository::new(pg_client.clone()))
        as Arc<dyn zradar_traits::FileListRepository>;

    let parquet_file_writer = Arc::new(ParquetFileWriter::new(
        parquet_data_dir.clone(),
        file_list_repo.clone(),
    ));

    let parquet_file_reader = Arc::new(ParquetFileReader::new(
        parquet_data_dir.clone(),
        file_list_repo.clone(),
    ));

    // Telemetry query service — reads from Parquet (Phase 02)
    let parquet_telemetry_reader =
        Arc::new(ParquetTelemetryReader::new(parquet_file_reader.clone()));
    let query_service = Arc::new(QueryService::new(
        parquet_telemetry_reader as Arc<dyn zradar_traits::TelemetryReader>,
    ));

    // Scores service (using Postgres)
    let score_repo = Arc::new(zradar_plugin_postgres::PostgresScoreRepository::new(
        pg_client.clone(),
    ));
    let scores_service = Arc::new(ScoresService::new(
        score_repo as Arc<dyn zradar_traits::ScoreRepository>,
        project_repo.clone(),
        rbac_service.clone() as Arc<dyn api::PermissionChecker>,
        audit_logger.clone() as Arc<dyn api::AuditLogger>,
    ));

    info!("✅ Control plane services initialized");

    // ========================================================================
    // OTLP Data Plane (Job Queue or Direct Write)
    // ========================================================================

    let ingestor_config = config
        .ingestor
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Ingestor configuration required"))?;

    let grpc_api_key_auth = Arc::new(GrpcApiKeyAuth::new(db_api_key_auth.clone()));
    let otlp_auth = config
        .auth
        .otlp_require_api_key
        .unwrap_or(true)
        .then(|| grpc_api_key_auth.clone());
    if otlp_auth.is_none() {
        info!("⚠️  OTLP gRPC (protobuf) is open: no API key required");
    }

    let otlp_port = config.otlp_port;
    let admin_port = config
        .admin_api
        .as_ref()
        .and_then(|a| a.admin_api_port)
        .unwrap_or(config.query_api_port);
    let admin_port = if admin_port != 0 { admin_port } else { 8080 };

    // Check if we should skip job queue and write directly
    if ingestor_config.skip_job {
        // Direct write mode - bypass job queue entirely
        info!("⚠️  SKIP_JOB enabled - spans will be written directly to persistence");
        info!("   This mode skips job queue for immediate consistency");
        info!("   Recommended only for development/testing or low-volume deployments");

        // Create Parquet telemetry writer for direct persistence
        let parquet_writer: Arc<dyn zradar_traits::TelemetryWriter> =
            Arc::new(ParquetTelemetryWriter::new(parquet_file_writer.clone()));
        let log_handler = Arc::new(DirectLogHandler::new(Arc::clone(&parquet_writer)));
        let span_handler = Arc::new(DirectSpanHandler::new(parquet_writer));

        let trace_service = OtlpTraceService::new(span_handler.clone(), otlp_auth.clone());
        let metrics_service = OtlpMetricsService::new(span_handler.clone(), otlp_auth.clone());
        let logs_service = OtlpLogsService::new(
            Arc::new(NullScoreHandler),
            log_handler,
            otlp_auth.clone(),
        );

        info!("✅ OTLP services initialized (direct write mode)");

        // Start servers
        let otlp_addr = format!("0.0.0.0:{}", otlp_port).parse()?;
        let otlp_server = async move {
            info!("🎧 OTLP gRPC server listening on {}", otlp_addr);
            info!("✅ gRPC compression: gzip enabled (send & accept)");
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
                .map_err(|e| {
                    error!("OTLP server error: {}", e);
                    anyhow::anyhow!("OTLP server failed: {}", e)
                })
        };

        let admin_addr = format!("0.0.0.0:{}", admin_port);
        let health_router = health::create_health_router(Some(pg_pool.clone()));

        let admin_api = create_admin_router(
            auth_service,
            org_service,
            project_service,
            api_key_service,
            role_service,
            query_service,
            scores_service,
            jwt_auth.clone(),
            user_repo.clone(),
        );

        let app = Router::new()
            .merge(health_router)
            .merge(admin_api)
            .layer(CompressionLayer::new());

        let admin_server = async move {
            info!("🎧 Admin API listening on http://{}", admin_addr);
            info!("✨ Swagger UI: http://localhost:{}/swagger-ui/", admin_port);

            let listener = tokio::net::TcpListener::bind(&admin_addr).await?;
            axum::serve(listener, app)
                .await
                .map_err(|e| anyhow::anyhow!("Admin API server failed: {}", e))
        };

        info!("✨ zradar is ready!");
        info!("   OTLP gRPC: localhost:{}", otlp_port);
        info!("   Admin API: http://localhost:{}", admin_port);
        info!("   HTTP Compression: gzip enabled");

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
    } else {
        // Job queue mode - enqueue for async processing by workers

        // Initialize block storage (from plugin)
        let storage_path = ingestor_config
            .storage
            .local
            .as_ref()
            .map(|l| l.path.as_str())
            .unwrap_or("./data/trace-batches");

        let block_storage = Arc::new(LocalBlockStorage::new(storage_path));
        info!("✅ Block storage initialized: local ({})", storage_path);

        // Initialize job queue (from plugin)
        let job_queue = Arc::new(PostgresJobQueue::new(
            pg_pool.clone(),
            block_storage.clone(),
        ));
        info!("✅ Job queue initialized: postgres (NO workers - use zradar-worker)");

        let span_handler = Arc::new(JobQueueSpanHandler::new(job_queue.clone()));

        let trace_service = OtlpTraceService::new(span_handler.clone(), otlp_auth.clone());
        let metrics_service = OtlpMetricsService::new(span_handler.clone(), otlp_auth.clone());
        let logs_service = OtlpLogsService::new(
            Arc::new(NullScoreHandler),
            Arc::new(NullLogHandler),
            otlp_auth.clone(),
        );

        info!("✅ OTLP services initialized (job queue mode)");

        // Start servers
        let otlp_addr = format!("0.0.0.0:{}", otlp_port).parse()?;
        let otlp_server = async move {
            info!("🎧 OTLP gRPC server listening on {}", otlp_addr);
            info!("✅ gRPC compression: gzip enabled (send & accept)");
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
                .map_err(|e| {
                    error!("OTLP server error: {}", e);
                    anyhow::anyhow!("OTLP server failed: {}", e)
                })
        };

        let admin_addr = format!("0.0.0.0:{}", admin_port);
        let health_router = health::create_health_router(Some(pg_pool.clone()));

        let admin_api = create_admin_router(
            auth_service,
            org_service,
            project_service,
            api_key_service,
            role_service,
            query_service,
            scores_service,
            jwt_auth.clone(),
            user_repo.clone(),
        );

        let app = Router::new()
            .merge(health_router)
            .merge(admin_api)
            .layer(CompressionLayer::new());

        let admin_server = async move {
            info!("🎧 Admin API listening on http://{}", admin_addr);
            info!("✨ Swagger UI: http://localhost:{}/swagger-ui/", admin_port);

            let listener = tokio::net::TcpListener::bind(&admin_addr).await?;
            axum::serve(listener, app)
                .await
                .map_err(|e| anyhow::anyhow!("Admin API server failed: {}", e))
        };

        info!("✨ zradar is ready!");
        info!("   OTLP gRPC: localhost:{}", otlp_port);
        info!("   Admin API: http://localhost:{}", admin_port);
        info!("   HTTP Compression: gzip enabled");

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
    }

    Ok(())
}
