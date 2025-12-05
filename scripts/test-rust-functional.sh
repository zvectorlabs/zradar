#!/bin/bash
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Usage help
usage() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  zradar Functional Tests (Rust)${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "Usage: $0 [OPTIONS] [TEST_FILTER]"
    echo ""
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo "  -l, --list           List all available tests"
    echo "  -r, --reuse          Reuse Docker if running, else start fresh. Keep running for next iteration."
    echo ""
    echo "Examples:"
    echo "  $0                                    # Fresh Docker, cleanup after"
    echo "  $0 -r                                 # Reuse Docker (or start fresh), keep running"
    echo "  $0 -r test_create_api_key             # Reuse Docker, run specific test"
    echo "  $0 -r test_api_keys                   # Reuse Docker, run filtered tests"
    echo "  $0 test_e2e::test_api_key_lifecycle   # Run specific test in module"
    echo ""
    echo "Tip: Use -r for fast iteration during development"
    echo ""
    exit 0
}

# Parse arguments
TEST_FILTER=""
LIST_TESTS=false
DOCKER_REUSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -l|--list)
            LIST_TESTS=true
            shift
            ;;
        -r|--reuse)
            DOCKER_REUSE=true
            shift
            ;;
        *)
            TEST_FILTER="$1"
            shift
            ;;
    esac
done

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  zradar Functional Tests (Rust)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Show active flags
if [ "$DOCKER_REUSE" = true ]; then
    echo -e "${YELLOW}🔄 Reuse Mode: ON (reuse if available, keep running after)${NC}"
fi
if [ -n "$TEST_FILTER" ]; then
    echo -e "${YELLOW}🔍 Test Filter: ${TEST_FILTER}${NC}"
fi
if [ "$DOCKER_REUSE" = true ] || [ -n "$TEST_FILTER" ]; then
    echo ""
fi

# Configuration - Test ports: 9011-9016
TEST_DATABASE_URL="postgresql://zradar_test:test_pass_123@localhost:9011/zradar_test"
TEST_CLICKHOUSE_URL="http://localhost:9012"
TEST_REDIS_URL="redis://localhost:9014"
TEST_API_URL="http://localhost:9015"
TEST_GRPC_URL="http://localhost:9016"

COMPOSE_FILE="docker-compose.test.yml"

# Cleanup function
cleanup() {
    local exit_code=$?
    echo ""
    echo -e "${YELLOW}🧹 Cleaning up...${NC}"
    
    # Kill worker process if running
    if [ ! -z "$WORKER_PID" ]; then
        kill $WORKER_PID 2>/dev/null || true
        wait $WORKER_PID 2>/dev/null || true
        echo -e "${BLUE}   Stopped worker${NC}"
    fi
    
    # Kill server process if running
    if [ ! -z "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
        echo -e "${BLUE}   Stopped server${NC}"
    fi
    
    # Force kill any processes on test ports
    lsof -ti:9015 2>/dev/null | xargs kill -9 2>/dev/null || true
    lsof -ti:9016 2>/dev/null | xargs kill -9 2>/dev/null || true
    pkill -f "target/release/zradar" 2>/dev/null || true
    pkill -f "target/release/zradar-worker" 2>/dev/null || true
    
    # Conditionally stop and remove containers based on DOCKER_REUSE flag
    if [ "$DOCKER_REUSE" = true ]; then
        echo -e "${YELLOW}   Keeping Docker containers running (reuse mode)${NC}"
        echo -e "${BLUE}   To stop: docker-compose -f $COMPOSE_FILE down -v${NC}"
    else
        echo -e "${BLUE}   Stopping Docker containers with docker-compose...${NC}"
        # Stop and remove containers using docker-compose
        docker-compose -f $COMPOSE_FILE down -v --remove-orphans 2>/dev/null || true
        
        # Remove test data
        rm -rf /tmp/zradar-test-data 2>/dev/null || true
        rm -rf /tmp/zradar-test-batches 2>/dev/null || true
    fi
    
    # Restore original config
    if [ -f "config.toml.backup" ]; then
        mv config.toml.backup config.toml
        echo -e "${BLUE}   Restored config.toml${NC}"
    fi
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ Cleanup complete${NC}"
    else
        echo -e "${RED}❌ Tests failed (exit code: $exit_code)${NC}"
    fi
    
    exit $exit_code
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Step 1: Build server and tests on host (before Docker)
echo -e "${YELLOW}1️⃣  Building server and tests on host...${NC}"
echo -e "${BLUE}   (Building once, reusing for tests)${NC}"

# Build server
echo -e "${BLUE}   → Building zradar server...${NC}"
cargo build --release --bin zradar
if [ ! -f "./target/release/zradar" ]; then
    echo -e "${RED}✗ Server build failed${NC}"
    exit 1
fi

# Build worker
echo -e "${BLUE}   → Building zradar worker...${NC}"
cargo build --release --bin zradar-worker
if [ ! -f "./target/release/zradar-worker" ]; then
    echo -e "${RED}✗ Worker build failed${NC}"
    exit 1
fi

# Build test suite
echo -e "${BLUE}   → Building test suite...${NC}"
cargo build --package zradar-functional-tests --tests
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Test build failed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Server and tests built successfully${NC}"
echo ""

# Step 2: Clean existing test containers (or reuse if -r flag)
if [ "$DOCKER_REUSE" = true ]; then
    echo -e "${YELLOW}2️⃣  Checking existing Docker containers for reuse...${NC}"
    
    # Check if all required containers exist and are healthy
    # Note: We only need 1 container now (Postgres) - simplified setup
    healthy=$(docker ps --filter "name=zradar-test" --format "{{.Status}}" 2>/dev/null | grep -c "(healthy)" 2>/dev/null | tr -d '\n' || echo "0")
    healthy=${healthy:-0}
    
    if [ "$healthy" -ge 1 ] 2>/dev/null; then
        echo -e "${GREEN}✓ Found healthy PostgreSQL container, reusing it${NC}"
        SKIP_DOCKER_SETUP=true
    else
        echo -e "${YELLOW}   PostgreSQL not healthy, will recreate${NC}"
        SKIP_DOCKER_SETUP=false
        # Clean up unhealthy containers using docker-compose
        echo -e "${BLUE}   → Stopping unhealthy containers with docker-compose...${NC}"
        docker-compose -f $COMPOSE_FILE down -v --remove-orphans
        sleep 1 
    fi
else
    echo -e "${YELLOW}2️⃣  Cleaning existing test containers (force fresh start)...${NC}"
    SKIP_DOCKER_SETUP=false
    
    # Stop and remove any existing test containers and volumes using docker-compose
    echo -e "${BLUE}   → Stopping containers with docker-compose...${NC}"
    docker-compose -f $COMPOSE_FILE down -v --remove-orphans 2>/dev/null || true
    
    # Kill any processes still using test ports (in case something leaked)
    echo -e "${BLUE}   → Cleaning up ports...${NC}"
    lsof -ti:9011 2>/dev/null | xargs kill -9 2>/dev/null || true  # PostgreSQL
    # Note: ClickHouse and Redis ports removed (simplified setup)
    lsof -ti:9015 2>/dev/null | xargs kill -9 2>/dev/null || true  # API
    lsof -ti:9016 2>/dev/null | xargs kill -9 2>/dev/null || true  # gRPC
    
    # Short pause to ensure cleanup is complete
    sleep 2
    
    echo -e "${GREEN}✓ Cleanup complete${NC}"
fi
echo ""

# Step 3: Start test databases (skip if reusing)
if [ "$SKIP_DOCKER_SETUP" = true ]; then
    echo -e "${YELLOW}3️⃣  Skipping Docker setup (reusing existing containers)${NC}"
else
    echo -e "${YELLOW}3️⃣  Starting fresh test databases (PostgreSQL, ClickHouse, Redis)...${NC}"
    docker-compose -f $COMPOSE_FILE up -d --force-recreate --remove-orphans
fi

# Wait for PostgreSQL to be healthy
echo -e "${YELLOW}   Waiting for PostgreSQL to be healthy...${NC}"
timeout=30
elapsed=0
while [ $elapsed -lt $timeout ]; do
    # Count healthy containers - ensure we get a clean integer
    healthy=$(docker ps --filter "name=zradar-test-postgres" --format "{{.Status}}" 2>/dev/null | grep -c "(healthy)" 2>/dev/null | tr -d '\n' || echo "0")
    healthy=${healthy:-0}  # Default to 0 if empty
    
    if [ "$healthy" -ge 1 ] 2>/dev/null; then
        echo -e "${GREEN}✓ PostgreSQL healthy${NC}"
        break
    fi
    
    if [ $elapsed -eq $((timeout-1)) ]; then
        echo -e "${RED}✗ Timeout waiting for PostgreSQL${NC}"
        docker-compose -f $COMPOSE_FILE ps
        exit 1
    fi
    
    echo -n "."
    sleep 1
    elapsed=$((elapsed + 1))
done
echo ""

# Step 4: Setup test configuration
echo -e "${YELLOW}4️⃣  Setting up test configuration...${NC}"
if [ -f "config.toml" ]; then
    mv config.toml config.toml.backup
    echo -e "${BLUE}   Backed up config.toml → config.toml.backup${NC}"
fi
cp config.test.toml config.toml
echo -e "${GREEN}✓ Using config.test.toml${NC}"
echo ""

# Step 5: Start zradar server (this will run migrations AND create admin user automatically)
echo -e "${YELLOW}5️⃣  Starting zradar test server...${NC}"
echo -e "${BLUE}   → Server will auto-run migrations via MigrationRegistry${NC}"
echo -e "${BLUE}   → Migrations will create all tables including users${NC}"
DATABASE_URL=$TEST_DATABASE_URL \
ZVRADAR_TEST_MODE=1 \
RUST_LOG=info,zradar=debug \
./target/release/zradar &
SERVER_PID=$!

# Wait for server to be ready (migrations run during startup)
echo -e "${YELLOW}   Waiting for server to be ready (migrations running)...${NC}"
timeout=60
elapsed=0
while [ $elapsed -lt $timeout ]; do
    if curl -sf $TEST_API_URL/health > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Server ready at $TEST_API_URL (migrations completed)${NC}"
        break
    fi
    
    if [ $elapsed -eq $((timeout-1)) ]; then
        echo -e "${RED}✗ Server didn't start in time${NC}"
        echo -e "${YELLOW}   Check server logs above for migration errors${NC}"
        kill $SERVER_PID 2>/dev/null || true
        exit 1
    fi
    
    echo -n "."
    sleep 1
    elapsed=$((elapsed + 1))
done
echo ""

# Step 5.5: Start zradar worker (processes telemetry jobs)
echo -e "${YELLOW}5.5️⃣  Starting zradar worker...${NC}"
echo -e "${BLUE}   → Worker processes telemetry jobs from queue → PostgreSQL${NC}"
DATABASE_URL=$TEST_DATABASE_URL \
STORAGE_PATH=./data/trace-batches \
WORKER_COUNT=2 \
RUST_LOG=info,zradar=debug \
./target/release/zradar-worker &
WORKER_PID=$!
sleep 1  # Give worker a moment to start
echo -e "${GREEN}✓ Worker started (PID: $WORKER_PID)${NC}"
echo ""

# Step 6: Create admin user (after migrations have run)
if [ "$SKIP_DOCKER_SETUP" = true ]; then
    echo -e "${YELLOW}6️⃣  Skipping admin user creation (reusing existing user)${NC}"
else
    echo -e "${YELLOW}6️⃣  Creating test admin user...${NC}"
    if docker exec zradar-test-postgres psql -U zradar_test -d zradar_test -c "
INSERT INTO users (id, email, password_hash, full_name, is_active, email_verified, metadata)
VALUES (
    gen_random_uuid(),
    'admin@example.com',
    '\$2b\$12\$oeeBLCFWxUdHyPstg83KO.nbGCDuciTYdx3YxQU3g2kHTsj89mSVm',
    'Test Admin',
    true,
    true,
    '{\"is_system_admin\": true}'::jsonb
) ON CONFLICT (email) DO NOTHING;
" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Admin user created (email: admin@example.com, password: changeme123)${NC}"
    else
        echo -e "${RED}✗ Failed to create admin user${NC}"
        docker exec zradar-test-postgres psql -U zradar_test -d zradar_test -c "\d users"
        exit 1
    fi
fi
echo ""

# Step 8: Run functional tests
echo -e "${YELLOW}8️⃣  Running Rust functional tests...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Handle --list option
if [ "$LIST_TESTS" = true ]; then
    echo -e "${YELLOW}Available tests:${NC}"
    TEST_DATABASE_URL=$TEST_DATABASE_URL \
    TEST_CLICKHOUSE_URL=$TEST_CLICKHOUSE_URL \
    TEST_API_URL=$TEST_API_URL \
    TEST_GRPC_URL=$TEST_GRPC_URL \
    cargo test --package zradar-functional-tests --test functional_tests -- --ignored --list
    TEST_RESULT=$?
else
    # Build the test command
    if [ -n "$TEST_FILTER" ]; then
        echo -e "${YELLOW}Running filtered test: ${TEST_FILTER}${NC}"
        echo ""
        TEST_DATABASE_URL=$TEST_DATABASE_URL \
        TEST_CLICKHOUSE_URL=$TEST_CLICKHOUSE_URL \
        TEST_API_URL=$TEST_API_URL \
        TEST_GRPC_URL=$TEST_GRPC_URL \
        cargo test --package zradar-functional-tests --test functional_tests "$TEST_FILTER" -- --ignored --nocapture --test-threads=1
    else
        echo -e "${YELLOW}Running all tests...${NC}"
        echo ""
        TEST_DATABASE_URL=$TEST_DATABASE_URL \
        TEST_CLICKHOUSE_URL=$TEST_CLICKHOUSE_URL \
        TEST_API_URL=$TEST_API_URL \
        TEST_GRPC_URL=$TEST_GRPC_URL \
        cargo test --package zradar-functional-tests --test functional_tests -- --ignored --nocapture --test-threads=8
    fi
    TEST_RESULT=$?
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ $TEST_RESULT -eq 0 ]; then
    if [ "$LIST_TESTS" = true ]; then
        echo -e "${GREEN}✅ Test listing complete${NC}"
    elif [ -n "$TEST_FILTER" ]; then
        echo -e "${GREEN}✅ Test '${TEST_FILTER}' passed!${NC}"
    else
        echo -e "${GREEN}✅ All functional tests passed!${NC}"
    fi
    
    # Show Docker status hint if in reuse mode
    if [ "$DOCKER_REUSE" = true ]; then
        echo ""
        echo -e "${YELLOW}🐳 Docker containers are still running (reuse mode):${NC}"
        echo -e "${BLUE}   - PostgreSQL: localhost:9011${NC}"
        echo -e "${BLUE}   - ClickHouse: localhost:9012${NC}"
        echo -e "${BLUE}   - Redis: localhost:9014${NC}"
        echo -e "${BLUE}   To stop: docker-compose -f $COMPOSE_FILE down -v${NC}"
    fi
else
    echo -e "${RED}❌ Some tests failed${NC}"
fi

exit $TEST_RESULT

