# ClickHouse Migrations

This directory contains embedded ClickHouse schema migrations for the zradar ClickHouse plugin.

## 📍 Location

**Embedded in plugin**: `crates/plugins/zradar-plugin-clickhouse/migrations/`

These migrations are now part of the plugin itself, making deployment easier and ensuring version compatibility.

## Structure

Migrations follow a timestamp-based naming convention:
```
YYYYMMDDHHMMSS_description.sql
```

## Current Migrations

### `20241123000001_create_telemetry_schema.sql` (273 lines)

Creates the complete telemetry database schema:

#### Tables
1. **`spans`** - Distributed tracing with LLM-specific fields
   - OpenTelemetry-compatible span data
   - LLM model tracking (model, tokens, costs)
   - Agent/tool metadata
   - Resource profiling (CPU, memory)
   - Prompt management
   - **Partitioning**: By day (`toYYYYMMDD(timestamp)`)
   - **TTL**: 90 days
   - **Compression**: ZSTD(1) for text fields

2. **`metrics`** - Time-series metrics
   - Counter, Gauge, Histogram, Summary support
   - Service and agent metadata
   - Flexible label storage
   - **Partitioning**: By day
   - **TTL**: 30 days
   - **Compression**: ZSTD(1) for labels

3. **`evaluation_scores`** - Evaluation and scoring data
   - Trace, observation, and session associations
   - Numeric and string value support
   - Evaluation metadata and context
   - **Partitioning**: By month (`toYYYYMM(timestamp)`)
   - **TTL**: 90 days
   - **Indexes**: Bloom filters for fast lookups

#### Materialized Views
1. **`mv_project_costs`** - Cost summaries by project/model
   - Daily aggregation
   - Token and cost totals
   - Engine: SummingMergeTree

2. **`mv_agent_performance`** - Agent performance metrics
   - Daily aggregation
   - Duration percentiles (p50, p95, p99)
   - Engine: AggregatingMergeTree

3. **`mv_trace_score_summary`** - Trace-level evaluation summaries
   - Score statistics (avg, min, max, count)
   - Daily aggregation

4. **`mv_session_score_summary`** - Session-level evaluation summaries
   - Score statistics by session
   - Daily aggregation

## Migration Tracking System

zradar includes an automatic migration system that tracks applied migrations in a dedicated table.

### Migration Tracking Table

Applied migrations are recorded in the `_zradar_migrations` table:

```sql
CREATE TABLE _zradar_migrations (
    version String,              -- Migration version (timestamp from filename)
    description String,          -- Migration description
    applied_at DateTime64(3),    -- When the migration was applied
    checksum String,             -- SHA256 checksum of migration file
    execution_time_ms UInt32     -- How long the migration took
) ENGINE = MergeTree()
ORDER BY (version, applied_at);
```

### Query Migration Status

To see which migrations have been applied:

```sql
SELECT 
    version,
    description,
    applied_at,
    execution_time_ms
FROM _zradar_migrations 
ORDER BY version;
```

### Verify Migration Integrity

The migration system calculates SHA256 checksums of migration files to detect tampering:

```rust
// In your application code
clickhouse_client.verify_migrations("./crates/plugins/zradar-plugin-clickhouse/migrations").await?;
```

## Usage

### Automatic Migrations (Recommended)

Enable automatic migrations in `config.toml`:

```toml
[migrations]
auto_migrate_clickhouse = true
clickhouse_migrations_path = "./crates/plugins/zradar-plugin-clickhouse/migrations"
```

Or via environment variable:

```bash
export AUTO_MIGRATE_CLICKHOUSE=true
export CLICKHOUSE_MIGRATIONS_PATH="./crates/plugins/zradar-plugin-clickhouse/migrations"
```

The server and worker will automatically apply pending migrations on startup.

### Programmatic Usage

```rust
use zradar_plugin_clickhouse::ClickHouseClient;

let client = ClickHouseClient::connect(&config).await?;

// Apply migrations
client.run_migrations("./crates/plugins/zradar-plugin-clickhouse/migrations").await?;

// Verify checksums
let valid = client.verify_migrations("./crates/plugins/zradar-plugin-clickhouse/migrations").await?;
assert!(valid, "Migration checksums don't match!");
```

### Manual Application

To apply migrations manually using clickhouse-client:

```bash
# Apply the migration
clickhouse-client --multiquery < \
  crates/plugins/zradar-plugin-clickhouse/migrations/20241123000001_create_telemetry_schema.sql

# Or for specific database
clickhouse-client --database telemetry --multiquery < \
  crates/plugins/zradar-plugin-clickhouse/migrations/20241123000001_create_telemetry_schema.sql
```

## Creating New Migrations

1. Create a new file following the naming convention:
   ```
   YYYYMMDDHHMMSS_description.sql
   ```

2. Use `IF NOT EXISTS` clauses for idempotency:
   ```sql
   CREATE TABLE IF NOT EXISTS my_new_table (
       id String,
       ...
   ) ENGINE = MergeTree()
   ORDER BY id;
   ```

3. The migration will be automatically detected and applied on next startup

## Migration Features

- ✅ **Automatic Discovery**: Scans directory for `.sql` files
- ✅ **Version Ordering**: Applies migrations in timestamp order
- ✅ **Idempotent**: Safe to run multiple times (skips already applied)
- ✅ **Checksum Verification**: Detects modified migration files
- ✅ **Execution Tracking**: Records when and how long each migration took
- ✅ **Error Handling**: Fails fast if a migration encounters an error
- ✅ **Multi-statement Support**: Handles complex migrations with multiple statements

## Schema Features

### Performance Optimizations
- **Partitioning**: By day/month for efficient data management
- **TTL Policies**: Automatic data cleanup (30-90 days)
- **Compression**: ZSTD for large text fields
- **Indexes**: Bloom filters and set indexes for fast lookups
- **Materialized Views**: Pre-aggregated analytics

### Multi-tenancy
- `tenant_id` (organization_id from PostgreSQL)
- `project_id` (project_id from PostgreSQL)
- All queries filtered by tenant for data isolation

### LLM-Specific Features
- Token tracking (prompt, completion, total)
- Cost tracking per request
- Model identification
- Prompt versioning
- Tool call tracking
- Time-to-first-token metrics

## Notes

- All CREATE statements use `IF NOT EXISTS` for idempotency
- Migrations are applied in alphabetical/timestamp order
- The `_zradar_migrations` table is created automatically
- Checksum verification prevents silent corruption
- TTL policies handle data retention automatically
- Indexes are embedded in table creation for idempotency

## Data Retention

| Table | TTL | Reason |
|-------|-----|--------|
| `spans` | 90 days | Detailed trace data |
| `evaluation_scores` | 90 days | Evaluation history |
| `metrics` | 30 days | High-volume time-series |
| Materialized Views | Follows source TTL | Derived data |

## Compatibility

- **ClickHouse**: 21.8+ (uses DateTime64)
- **zradar**: 0.1.0+
- **Migration System**: Embedded in plugin

---

**Last Updated**: December 4, 2025  
**Migration Version**: 20241123000001  
**Status**: ✅ Production-ready
