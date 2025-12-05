//! PostgreSQL repository implementations
//!
//! All repository traits from zradar-traits are implemented here.

pub mod users;
pub mod organizations;
pub mod projects;
pub mod api_keys;
pub mod roles;
pub mod telemetry;
pub mod scores;
pub mod audit;
pub mod job_queue;

pub use users::PostgresUserRepository;
pub use organizations::PostgresOrganizationRepository;
pub use projects::PostgresProjectRepository;
pub use api_keys::PostgresApiKeyRepository;
pub use roles::PostgresRoleRepository;
pub use telemetry::PostgresTelemetryRepository;
pub use scores::PostgresScoreRepository;
pub use audit::PostgresAuditLogger;
pub use job_queue::PostgresJobQueue;

