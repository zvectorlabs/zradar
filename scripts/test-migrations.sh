#!/bin/bash
# Test script for the auto-migration system
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}  Testing Auto-Migration System${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""

# Check if databases are running
echo -e "${YELLOW}[1/6]${NC} Checking if databases are running..."
if ! docker ps | grep -q zradar-postgres; then
    echo -e "${RED}  ✗ PostgreSQL is not running${NC}"
    echo -e "  Run: docker-compose up -d postgres"
    exit 1
fi

if ! docker ps | grep -q zradar-clickhouse; then
    echo -e "${RED}  ✗ ClickHouse is not running${NC}"
    echo -e "  Run: docker-compose up -d clickhouse"
    exit 1
fi
echo -e "${GREEN}  ✓ Databases are running${NC}"

# Build the application
echo -e "${YELLOW}[2/6]${NC} Building zradar server..."
SQLX_OFFLINE=true cargo build --bin zradar --quiet
echo -e "${GREEN}  ✓ Build successful${NC}"

# Test PostgreSQL migrations
echo -e "${YELLOW}[3/6]${NC} Testing PostgreSQL migrations..."

# Drop and recreate the database for clean test
docker exec zradar-postgres psql -U zradar -c "DROP DATABASE IF EXISTS zradar_migration_test;" 2>/dev/null || true
docker exec zradar-postgres psql -U zradar -c "CREATE DATABASE zradar_migration_test;"

# Run server with migrations enabled (it will exit immediately, we just want to test migrations)
DATABASE_URL="postgresql://zradar:dev_password@localhost:5432/zradar_migration_test" \
AUTO_MIGRATE_POSTGRES=true \
timeout 5 ./target/debug/zradar 2>&1 | grep -q "PostgreSQL migrations completed" || {
    echo -e "${RED}  ✗ PostgreSQL migrations failed${NC}"
    exit 1
}

# Verify migration tracking table exists
MIGRATION_COUNT=$(docker exec zradar-postgres psql -U zradar -d zradar_migration_test -t -c "SELECT COUNT(*) FROM _sqlx_migrations;" 2>/dev/null | tr -d ' ')
if [ "$MIGRATION_COUNT" -gt 0 ]; then
    echo -e "${GREEN}  ✓ PostgreSQL migrations tracked: $MIGRATION_COUNT migrations applied${NC}"
else
    echo -e "${RED}  ✗ No migrations found in tracking table${NC}"
    exit 1
fi

# Test ClickHouse migrations
echo -e "${YELLOW}[4/6]${NC} Testing ClickHouse migrations..."

# Drop and recreate the database for clean test
docker exec zradar-clickhouse clickhouse-client --user zradar --password dev_password --query "DROP DATABASE IF EXISTS telemetry_migration_test" 2>/dev/null || true
docker exec zradar-clickhouse clickhouse-client --user zradar --password dev_password --query "CREATE DATABASE telemetry_migration_test"

# Create test config
cat > /tmp/zradar-migration-test.toml << EOF
otlp_port = 4317
admin_api_port = 8080

[postgres]
max_connections = 20

[clickhouse]
url = "http://localhost:8123"
user = "zradar"
password = "dev_password"
database = "telemetry_migration_test"
max_connections = 10

[migrations]
auto_migrate_postgres = false
auto_migrate_clickhouse = true
clickhouse_migrations_path = "./crates/plugins/zradar-plugin-clickhouse/migrations"
EOF

# Run server with ClickHouse migrations enabled
DATABASE_URL="postgresql://zradar:dev_password@localhost:5432/zradar_migration_test" \
timeout 5 ./target/debug/zradar 2>&1 | grep -q "ClickHouse migrations completed" || {
    echo -e "${RED}  ✗ ClickHouse migrations failed${NC}"
    exit 1
}

# Verify migration tracking table exists
MIGRATION_COUNT=$(docker exec zradar-clickhouse clickhouse-client --user zradar --password dev_password --database telemetry_migration_test --query "SELECT COUNT(*) FROM _zradar_migrations" 2>/dev/null)
if [ "$MIGRATION_COUNT" -gt 0 ]; then
    echo -e "${GREEN}  ✓ ClickHouse migrations tracked: $MIGRATION_COUNT migrations applied${NC}"
else
    echo -e "${RED}  ✗ No migrations found in tracking table${NC}"
    exit 1
fi

# Test idempotency (running again should not re-apply)
echo -e "${YELLOW}[5/6]${NC} Testing idempotency (running migrations again)..."

DATABASE_URL="postgresql://zradar:dev_password@localhost:5432/zradar_migration_test" \
AUTO_MIGRATE_POSTGRES=true \
AUTO_MIGRATE_CLICKHOUSE=true \
timeout 5 ./target/debug/zradar 2>&1 | grep -q "No pending migrations" || {
    echo -e "${RED}  ✗ Idempotency test failed${NC}"
    exit 1
}

echo -e "${GREEN}  ✓ Idempotency verified - migrations not re-applied${NC}"

# Query migration history
echo -e "${YELLOW}[6/6]${NC} Migration history:"
echo ""
echo "PostgreSQL migrations:"
docker exec zradar-postgres psql -U zradar -d zradar_migration_test -c "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"
echo ""
echo "ClickHouse migrations:"
docker exec zradar-clickhouse clickhouse-client --user zradar --password dev_password --database telemetry_migration_test --query "SELECT version, description, applied_at, execution_time_ms FROM _zradar_migrations ORDER BY version FORMAT PrettyCompact"

# Cleanup
rm -f /tmp/zradar-migration-test.toml
docker exec zradar-postgres psql -U zradar -c "DROP DATABASE IF EXISTS zradar_migration_test;" 2>/dev/null || true
docker exec zradar-clickhouse clickhouse-client --user zradar --password dev_password --query "DROP DATABASE IF EXISTS telemetry_migration_test" 2>/dev/null || true

echo ""
echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}  All Tests Passed! ✓${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""
echo "The auto-migration system is working correctly!"
echo ""
echo "Usage in production:"
echo "  1. Enable in config.toml:"
echo "     [migrations]"
echo "     auto_migrate_postgres = true"
echo "     auto_migrate_clickhouse = true"
echo ""
echo "  2. Or use environment variables:"
echo "     export AUTO_MIGRATE_POSTGRES=true"
echo "     export AUTO_MIGRATE_CLICKHOUSE=true"
echo ""

