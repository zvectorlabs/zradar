#!/bin/bash
# ClickHouse initialization - creates database only
# Schema migrations are handled by the application's auto-migration system
set -e

# Environment variables (with defaults)
CLICKHOUSE_USER=${CLICKHOUSE_USER:-zradar}
CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:-dev_password}
CLICKHOUSE_DB=${CLICKHOUSE_DB:-telemetry}
INIT_TIMEOUT=${INIT_TIMEOUT:-60}

echo "🚀 Starting ClickHouse server..."
/entrypoint.sh &
CLICKHOUSE_PID=$!

# Wait for ClickHouse to be ready
echo "⏳ Waiting for ClickHouse (user: $CLICKHOUSE_USER, timeout: ${INIT_TIMEOUT}s)..."
for i in $(seq 1 $INIT_TIMEOUT); do
    if clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
        --query "SELECT 1" > /dev/null 2>&1; then
        echo "✅ ClickHouse is ready"
        break
    fi
    if [ $i -eq $INIT_TIMEOUT ]; then
        echo "❌ ClickHouse failed to start within ${INIT_TIMEOUT}s"
        exit 1
    fi
    sleep 1
done

# Create database
echo "📝 Creating database '$CLICKHOUSE_DB'..."
if clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
    --query "CREATE DATABASE IF NOT EXISTS $CLICKHOUSE_DB"; then
    echo "✅ Database '$CLICKHOUSE_DB' ready"
else
    echo "⚠️  Database may already exist (continuing...)"
fi

echo "📋 ClickHouse initialization complete"
echo "   Schema migrations: Handled by application (AUTO_MIGRATE_CLICKHOUSE=true)"

# Keep container running
wait $CLICKHOUSE_PID
