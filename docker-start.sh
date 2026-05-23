#!/bin/bash
# Quick start script for zradar with Docker
# Supports both dev (default) and prod modes

set -e

MODE="${1:-dev}"  # dev or prod
FOLLOW_LOGS="${2:-}"  # optional: "follow" to tail logs

if [ "$MODE" != "dev" ] && [ "$MODE" != "prod" ]; then
    echo "❌ Invalid mode: $MODE"
    echo "Usage: $0 [dev|prod] [follow]"
    exit 1
fi

echo "🐳 zradar Docker Quick Start"
echo "=============================="
echo "Mode: ${MODE}"
echo ""

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Set compose file and ports based on mode
if [ "$MODE" = "prod" ]; then
    COMPOSE_FILE="docker-compose.prod.yml"
    API_PORT="9006"
    OTLP_PORT="9005"
    POSTGRES_PORT="9001"
else
    COMPOSE_FILE="docker-compose.yml"
    API_PORT="8080"
    OTLP_PORT="4317"
    POSTGRES_PORT="5432"
fi

# Check if .env exists, create if not
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    if [ "$MODE" = "prod" ]; then
        cat > .env << EOF
# zradar Production Environment Variables
POSTGRES_PASSWORD=prod_password_$(openssl rand -hex 8)
RUST_LOG=info,zradar=info
QUEUE_TYPE=postgres
STORAGE_TYPE=local
EOF
    else
        cat > .env << EOF
# zradar Development Environment Variables
POSTGRES_PASSWORD=dev_password
RUST_LOG=debug,zradar=trace
QUEUE_TYPE=postgres
STORAGE_TYPE=local
EOF
    fi
    echo "✅ Created .env with default passwords"
else
    echo "✅ Using existing .env file"
fi

echo ""
echo "🚀 Starting zradar services..."
echo ""

# Start services
if [ "$MODE" = "prod" ]; then
    docker-compose -f "$COMPOSE_FILE" up -d --build
else
    docker-compose -f "$COMPOSE_FILE" up -d
fi

echo ""

# If following logs, start tailing immediately so user can see compilation
if [ "$FOLLOW_LOGS" = "follow" ]; then
    echo "📜 Following logs from all services (compilation may take 2-3 minutes on first run)..."
    echo "    Press Ctrl+C to stop watching logs (services will keep running)"
    echo ""
    
    # Start following logs in foreground
    if [ "$MODE" = "dev" ]; then
        docker-compose logs -f
    else
        docker-compose -f "$COMPOSE_FILE" logs -f
    fi
    exit 0
fi

# For non-follow mode, wait for health checks
echo "⏳ Waiting for services to be healthy..."
echo "   (First-time compilation may take 2-3 minutes)"
sleep 5

# Wait for health checks - longer timeout for dev mode (first build)
if [ "$MODE" = "dev" ]; then
    MAX_WAIT=180  # 3 minutes for initial Rust compilation
else
    MAX_WAIT=60
fi

COUNTER=0

while [ $COUNTER -lt $MAX_WAIT ]; do
    if curl -sf http://localhost:${API_PORT}/health > /dev/null 2>&1; then
        echo ""
        echo "✅ zradar is ready!"
        break
    fi
    echo -n "."
    sleep 2
    COUNTER=$((COUNTER + 2))
done

if [ $COUNTER -ge $MAX_WAIT ]; then
    echo ""
    echo "⚠️  Services took longer than expected to start."
    echo "   View build logs with: make logs"
    echo "   Check status with: make status"
    exit 1
fi

echo ""
echo "📊 Service Status:"
docker-compose -f "$COMPOSE_FILE" ps

echo ""
echo "✨ zradar is running in ${MODE} mode!"
echo ""
echo "Available endpoints:"
echo "  🔹 OTLP gRPC:     localhost:${OTLP_PORT}"
echo "  🔹 Health Check:  http://localhost:${API_PORT}/health"
echo "  🔹 Admin API:     http://localhost:${API_PORT}"
echo "  🔹 PostgreSQL:    localhost:${POSTGRES_PORT}"

if [ "$MODE" = "dev" ]; then
    echo "  🔹 Adminer UI:    http://localhost:8081"
    echo ""
    echo "🔧 Development Features:"
    echo "  ✅ Hot reload enabled (code changes auto-rebuild)"
    echo "  ✅ Direct database access on standard ports"
    echo "  ✅ Debug logging enabled"
    echo "  ✅ Database GUI at http://localhost:8081"
fi

echo ""
echo "Next steps:"
if [ "$MODE" = "dev" ]; then
    echo "  1. Edit code in crates/ - changes will auto-reload"
    echo "  2. View logs:"
    echo "     make logs  OR  make dev-logs (follow mode)"
    echo "  3. Run tests:"
    echo "     make test"
    echo "  4. Stop services:"
    echo "     make stop  OR  docker-compose down"
else
    echo "  1. Create an API key"
    echo "  2. Send test traces:"
    echo "     cd examples/python && python send_trace.py"
    echo "  3. View logs:"
    echo "     make logs  OR  docker-compose -f $COMPOSE_FILE logs -f"
    echo "  4. Stop services:"
    echo "     make prod-stop  OR  docker-compose -f $COMPOSE_FILE down"
fi
echo ""
