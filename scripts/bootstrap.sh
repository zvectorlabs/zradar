#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}  zradar Bootstrap Script${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""

# Check for required environment variables
if [ -z "$DATABASE_URL" ]; then
    echo -e "${YELLOW}Warning: DATABASE_URL not set, using default${NC}"
    export DATABASE_URL="postgresql://zradar:password@localhost:5432/zradar"
fi

# Check for PostgreSQL
echo -e "${GREEN}[1/6]${NC} Checking PostgreSQL..."
if command -v psql &> /dev/null; then
    echo -e "  ✓ PostgreSQL client found"
else
    echo -e "${RED}  ✗ PostgreSQL client not found${NC}"
    echo -e "  Please install PostgreSQL 17+"
    exit 1
fi

# Check for ClickHouse
echo -e "${GREEN}[2/6]${NC} Checking ClickHouse..."
if command -v clickhouse-client &> /dev/null; then
    echo -e "  ✓ ClickHouse client found"
else
    echo -e "${YELLOW}  ⚠ ClickHouse client not found (optional)${NC}"
fi

# Check for sqlx-cli
echo -e "${GREEN}[3/6]${NC} Checking sqlx-cli..."
if command -v sqlx &> /dev/null; then
    echo -e "  ✓ sqlx-cli found"
else
    echo -e "${YELLOW}  ⚠ sqlx-cli not found, installing...${NC}"
    cargo install sqlx-cli --no-default-features --features postgres
fi

# Run PostgreSQL migrations
echo -e "${GREEN}[4/6]${NC} Running PostgreSQL migrations..."
cd "$(dirname "$0")/.."
sqlx migrate run --source migrations
echo -e "  ✓ Migrations completed"

# Run ClickHouse schema
echo -e "${GREEN}[5/6]${NC} Setting up ClickHouse..."
if command -v clickhouse-client &> /dev/null; then
    if [ -f crates/plugins/zradar-plugin-clickhouse/migrations/20241123000001_create_telemetry_schema.sql ]; then
        clickhouse-client < crates/plugins/zradar-plugin-clickhouse/migrations/20241123000001_create_telemetry_schema.sql 2>/dev/null || true
        echo -e "  ✓ ClickHouse schema created"
    else
        echo -e "${YELLOW}  ⚠ ClickHouse schema file not found${NC}"
    fi
else
    echo -e "${YELLOW}  ⚠ Skipping ClickHouse setup${NC}"
fi

# Create example config if it doesn't exist
echo -e "${GREEN}[6/6]${NC} Checking configuration..."
if [ ! -f config.toml ]; then
    if [ -f config.toml.example ]; then
        echo -e "  Creating config.toml from example..."
        cp config.toml.example config.toml
        echo -e "  ✓ Config file created"
        echo -e "${YELLOW}  ⚠ Please review and update config.toml${NC}"
    fi
else
    echo -e "  ✓ Config file exists"
fi

if [ ! -f .env ]; then
    if [ -f env.example ]; then
        echo -e "  Creating .env from example..."
        cp env.example .env
        echo -e "  ✓ .env file created"
        echo -e "${YELLOW}  ⚠ Please review and update .env${NC}"
    fi
else
    echo -e "  ✓ .env file exists"
fi

echo ""
echo -e "${GREEN}=====================================${NC}"
echo -e "${GREEN}  Bootstrap Complete!${NC}"
echo -e "${GREEN}=====================================${NC}"
echo ""
echo "Next steps:"
echo "  1. Review and update config.toml and .env"
echo "  2. Start the server: cargo run --release"
echo "  3. Access Admin API: http://localhost:8080"
echo "  4. View API docs: http://localhost:8080/swagger-ui/"
echo "  5. OTLP endpoint: localhost:4317 (gRPC)"
echo ""
echo "To create an admin user, first register via:"
echo "  POST http://localhost:8080/api/v1/auth/register"
echo ""

