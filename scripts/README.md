# zradar Scripts

## ClickHouse Initialization

### `clickhouse-init.sh`

Minimal ClickHouse initialization script for Docker containers.

**Purpose:** Creates an empty database. Schema migrations are handled by the application.

**Environment Variables:**
- `CLICKHOUSE_USER` - Database user (default: `zradar`)
- `CLICKHOUSE_PASSWORD` - Database password (default: `dev_password`)
- `CLICKHOUSE_DB` - Database name (default: `telemetry`)
- `INIT_TIMEOUT` - Startup timeout in seconds (default: `60`)

**Usage in Docker:**
```yaml
clickhouse:
  image: clickhouse/clickhouse-server:24.1-alpine
  environment:
    CLICKHOUSE_USER: zradar
    CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-dev_password}
    CLICKHOUSE_DB: telemetry
  volumes:
    - ./scripts/clickhouse-init.sh:/docker-entrypoint-initdb.d/init.sh:ro
  entrypoint: ["/bin/bash", "/docker-entrypoint-initdb.d/init.sh"]
```

**What it does:**
1. Starts ClickHouse server
2. Waits for server to be ready
3. Creates database (if not exists)
4. Keeps container running

**What it does NOT do:**
- ❌ Apply schema migrations (handled by application)
- ❌ Create tables (handled by application)
- ❌ Run SQL files (handled by application)

## Migration Testing

### `test-migrations.sh`

Automated test script for the migration system.

**Purpose:** Validates that auto-migrations work correctly for both PostgreSQL and ClickHouse.

**Prerequisites:**
- Docker running
- `postgres` and `clickhouse` containers running
- Application compiled (`cargo build --bin zradar`)

**Usage:**
```bash
./scripts/test-migrations.sh
```

**Tests:**
- ✅ PostgreSQL migration tracking
- ✅ ClickHouse migration tracking
- ✅ Idempotency (won't re-apply)
- ✅ Migration history display
- ✅ Automatic cleanup

## Bootstrap

### `bootstrap.sh`

Development environment setup script.

**Purpose:** Sets up a local development environment with databases and migrations.

**Usage:**
```bash
./scripts/bootstrap.sh
```

**What it does:**
- Checks for required tools (psql, clickhouse-client, sqlx-cli)
- Runs PostgreSQL migrations
- Sets up ClickHouse schema
- Creates config files from examples

## Architecture

```
┌─────────────────────┐
│  Docker Container   │
│    (ClickHouse)     │
├─────────────────────┤
│ clickhouse-init.sh  │  ← Only creates database
└─────────────────────┘
          ↓
┌─────────────────────┐
│    Application      │
│     (zradar)       │
├─────────────────────┤
│  Auto-Migration     │  ← Applies schema migrations
│      System         │     (tracked, verified)
└─────────────────────┘
```

## Best Practices

1. **Keep scripts minimal** - Let the application handle complex logic
2. **Use environment variables** - Make scripts configurable
3. **Clear error messages** - Help debugging
4. **Idempotent operations** - Safe to run multiple times
5. **Single responsibility** - Each script does one thing well

## Migration Philosophy

**Old approach (deprecated):**
```bash
# Docker entrypoint runs migrations
for file in *.sql; do
    clickhouse-client < $file
done
```

**New approach (current):**
```rust
// Application handles migrations
storage.run_migrations("./migrations_ch").await?;
```

**Benefits:**
- ✅ Proper tracking in database
- ✅ Checksum verification
- ✅ Consistent across environments
- ✅ Better error handling
- ✅ Easier to test

