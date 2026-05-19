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

# Check if PostgreSQL is running
echo -e "${YELLOW}[1/4]${NC} Checking if PostgreSQL is running..."
if ! docker ps | grep -q zradar-postgres; then
    echo -e "${RED}  ✗ PostgreSQL is not running${NC}"
    echo -e "  Run: docker-compose up -d postgres"
    exit 1
fi

echo -e "${GREEN}  ✓ PostgreSQL is running${NC}"

# Build the application
echo -e "${YELLOW}[2/4]${NC} Building zradar server..."
SQLX_OFFLINE=true cargo build --bin zradar --quiet
echo -e "${GREEN}  ✓ Build successful${NC}"

# Test PostgreSQL migrations
echo -e "${YELLOW}[3/4]${NC} Testing PostgreSQL migrations..."

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

# Test idempotency (running again should not re-apply)
echo -e "${YELLOW}[4/4]${NC} Testing idempotency (running migrations again)..."

DATABASE_URL="postgresql://zradar:dev_password@localhost:5432/zradar_migration_test" \
AUTO_MIGRATE_POSTGRES=true \
timeout 5 ./target/debug/zradar 2>&1 | grep -q "No pending migrations" || {
    echo -e "${RED}  ✗ Idempotency test failed${NC}"
    exit 1
}

echo -e "${GREEN}  ✓ Idempotency verified - migrations not re-applied${NC}"

# Query migration history
echo -e "${YELLOW}Migration history:${NC}"
echo ""
echo "PostgreSQL migrations:"
docker exec zradar-postgres psql -U zradar -d zradar_migration_test -c "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"

# Cleanup
docker exec zradar-postgres psql -U zradar -c "DROP DATABASE IF EXISTS zradar_migration_test;" 2>/dev/null || true

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
echo ""
echo "  2. Or use environment variables:"
echo "     export AUTO_MIGRATE_POSTGRES=true"
echo ""

