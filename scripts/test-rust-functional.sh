#!/usr/bin/env bash
# zradar functional test runner — uses Docker directly (no docker-compose).
#
# Mirrors the identity/ E2E architecture: a single disposable Postgres
# container is started via `docker run`, health-gated, then the zradar server
# and the functional test suite run against it. The whole lifecycle —
# build → infra up → migrate/serve → test → tear down — is orchestrated here.
#
# Usage:
#   ./scripts/test-rust-functional.sh                  # fresh container, tear down after (CI)
#   ./scripts/test-rust-functional.sh -r               # reuse healthy container, keep it running
#   ./scripts/test-rust-functional.sh -r TEST_FILTER   # reuse + run matching tests
#   ./scripts/test-rust-functional.sh TEST_FILTER      # fresh, run matching tests
#   ./scripts/test-rust-functional.sh -l               # list available tests
set -euo pipefail

# ── colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info() { echo -e "${BLUE}  $*${NC}"; }
ok()   { echo -e "${GREEN}✓ $*${NC}"; }
warn() { echo -e "${YELLOW}⚠ $*${NC}"; }
err()  { echo -e "${RED}✗ $*${NC}"; }

usage() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  zradar Functional Tests (Rust)${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "Usage: $0 [OPTIONS] [TEST_FILTER]"
    echo ""
    echo "Options:"
    echo "  -h, --help    Show this help message"
    echo "  -l, --list    List all available tests"
    echo "  -r, --reuse   Reuse a healthy test container if present; keep it running after"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Fresh container, cleanup after"
    echo "  $0 -r                                 # Reuse container, keep running"
    echo "  $0 -r test_create_api_key             # Reuse, run a specific test"
    echo "  $0 test_e2e::test_api_key_lifecycle   # Fresh, run a specific test"
    echo ""
    echo "Tip: use -r for fast iteration during development."
    exit 0
}

# ── args ──────────────────────────────────────────────────────────────────────
TEST_FILTER=""
LIST_TESTS=false
DOCKER_REUSE=false
FUNCTIONAL_TEST_THREADS="${FUNCTIONAL_TEST_THREADS:-4}"

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)  usage ;;
        -l|--list)  LIST_TESTS=true; shift ;;
        -r|--reuse) DOCKER_REUSE=true; shift ;;
        *)          TEST_FILTER="$1"; shift ;;
    esac
done

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}  zradar Functional Tests (Rust)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
[ "$DOCKER_REUSE" = true ] && warn "Reuse mode ON — container stays up after tests"
[ -n "$TEST_FILTER" ]      && warn "Filter: ${TEST_FILTER}"

# ── config ────────────────────────────────────────────────────────────────────
# Container runtime — defaults to docker; override with CTR=podman if preferred.
CTR="${CTR:-docker}"
PG_NAME="zradar-test-postgres"
PG_IMAGE="${ZRADAR_TEST_PG_IMAGE:-postgres:17-alpine}"
PG_PORT=9011

TEST_DATABASE_URL="postgresql://zradar_test:test_pass_123@localhost:${PG_PORT}/zradar_test"
TEST_API_URL="http://localhost:9015"
TEST_GRPC_URL="http://localhost:9016"

SERVER_PID=""
SKIP_DOCKER_SETUP=false
# Tracks what to do with config.toml on teardown: "untouched" (don't touch),
# "backup" (restore the displaced original), or "created" (remove the test
# config we wrote because there was no original).
CONFIG_STATE="untouched"

# ── container helpers ─────────────────────────────────────────────────────────
# Probe Postgres actively rather than reading .State.Health.Status: under
# rootless Podman without a systemd user session the background health-check
# timer never fires, so the status stays stuck at "starting". An explicit
# pg_isready via exec is reliable across both Docker and Podman.
pg_healthy() { $CTR exec "$PG_NAME" pg_isready -U zradar_test -q >/dev/null 2>&1; }

stop_postgres() {
    $CTR rm -f "$PG_NAME" >/dev/null 2>&1 || true
    # Free the DB port in case a previous run left something behind.
    lsof -ti:"$PG_PORT" 2>/dev/null | xargs kill -9 2>/dev/null || true
}

start_postgres() {
    info "Starting Postgres on localhost:${PG_PORT} (container ${PG_NAME})..."
    $CTR run -d --name "$PG_NAME" \
        -e POSTGRES_DB=zradar_test \
        -e POSTGRES_USER=zradar_test \
        -e POSTGRES_PASSWORD=test_pass_123 \
        -p "${PG_PORT}:5432" \
        --tmpfs /var/lib/postgresql/data \
        --health-cmd "pg_isready -U zradar_test" \
        --health-interval 2s \
        --health-timeout 2s \
        --health-retries 10 \
        "$PG_IMAGE" >/dev/null
}

wait_pg_healthy() {
    local timeout=60 elapsed=0
    while [ $elapsed -lt $timeout ]; do
        pg_healthy && { ok "Postgres healthy"; return 0; }
        # Fail fast if the container died during startup.
        $CTR inspect "$PG_NAME" >/dev/null 2>&1 || { err "Postgres container exited"; return 1; }
        printf '.'; sleep 1; elapsed=$((elapsed+1))
    done
    err "Timeout waiting for Postgres"
    $CTR logs --tail 30 "$PG_NAME" 2>/dev/null || true
    return 1
}

# ── cleanup trap — always runs, on success or failure ─────────────────────────
cleanup() {
    local code=$?
    echo ""
    echo -e "${YELLOW}🧹 Cleaning up...${NC}"

    if [ -n "$SERVER_PID" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        info "Stopped zradar server (pid $SERVER_PID)"
    fi
    lsof -ti:9015 2>/dev/null | xargs kill -9 2>/dev/null || true
    lsof -ti:9016 2>/dev/null | xargs kill -9 2>/dev/null || true
    pkill -f "target/release/zradar" 2>/dev/null || true

    if [ "$DOCKER_REUSE" = true ]; then
        warn "Container left running (reuse mode). To destroy: ${CTR} rm -f ${PG_NAME}"
    else
        info "Removing test container..."
        stop_postgres
        rm -rf ./data-test 2>/dev/null || true
        ok "Container removed"
    fi

    case "$CONFIG_STATE" in
        backup)  mv config.toml.backup config.toml; info "Restored config.toml" ;;
        created) rm -f config.toml; info "Removed test config.toml" ;;
    esac

    if [ "$code" -eq 0 ]; then
        ok "Cleanup complete"
    else
        err "Tests failed (exit code: $code)"
    fi
    exit "$code"
}
trap cleanup EXIT INT TERM

# ── preflight ─────────────────────────────────────────────────────────────────
command -v "$CTR" >/dev/null 2>&1 || { err "$CTR not found on PATH"; exit 1; }
$CTR info >/dev/null 2>&1 || { err "$CTR daemon not reachable — is it running?"; exit 1; }

# ── step 1: build server + test suite ─────────────────────────────────────────
echo -e "${YELLOW}1️⃣  Building server and tests on host...${NC}"
info "Building zradar server (release)..."
cargo build --release --bin zradar
[ -f "./target/release/zradar" ] || { err "Server build failed"; exit 1; }
info "Building functional test suite..."
cargo build --package zradar-functional-tests --tests
ok "Server and tests built"
echo ""

# ── step 2: infrastructure (fresh or reused) ──────────────────────────────────
if [ "$DOCKER_REUSE" = true ] && pg_healthy; then
    echo -e "${YELLOW}2️⃣  Reusing healthy Postgres container${NC}"
    SKIP_DOCKER_SETUP=true
else
    echo -e "${YELLOW}2️⃣  Starting a fresh Postgres container${NC}"
    stop_postgres
    sleep 1
    start_postgres
fi
echo ""

# ── step 3: wait for Postgres ─────────────────────────────────────────────────
if [ "$SKIP_DOCKER_SETUP" != true ]; then
    echo -e "${YELLOW}3️⃣  Waiting for Postgres to be healthy...${NC}"
    wait_pg_healthy
    echo ""
fi

# ── step 4: test configuration ────────────────────────────────────────────────
echo -e "${YELLOW}4️⃣  Setting up test configuration...${NC}"
rm -rf ./data-test 2>/dev/null || true
if [ -f "config.toml" ]; then
    mv config.toml config.toml.backup
    CONFIG_STATE="backup"
    info "Backed up config.toml → config.toml.backup"
else
    CONFIG_STATE="created"
fi
cp config.test.toml config.toml
ok "Using config.test.toml"
echo ""

# ── step 5: start zradar server (auto-runs migrations) ────────────────────────
echo -e "${YELLOW}5️⃣  Starting zradar test server (migrations run on startup)...${NC}"
DATABASE_URL="$TEST_DATABASE_URL" \
QUERY_API_PORT=9015 \
ZVRADAR_TEST_MODE=1 \
RUST_LOG=info,zradar=debug \
    ./target/release/zradar &
SERVER_PID=$!

timeout=60; elapsed=0
while [ $elapsed -lt $timeout ]; do
    if curl -sf "$TEST_API_URL/health" >/dev/null 2>&1; then
        ok "Server ready at $TEST_API_URL"
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        err "Server process died during startup"
        exit 1
    fi
    printf '.'; sleep 1; elapsed=$((elapsed+1))
done
[ $elapsed -ge $timeout ] && { err "Server didn't start in time"; exit 1; }
echo ""
info "Auth: static API key (zk_test_default)"
echo ""

# ── step 6: run tests ─────────────────────────────────────────────────────────
echo -e "${YELLOW}6️⃣  Running Rust functional tests...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

set +e
if [ "$LIST_TESTS" = true ]; then
    TEST_DATABASE_URL="$TEST_DATABASE_URL" \
    TEST_API_URL="$TEST_API_URL" \
    TEST_GRPC_URL="$TEST_GRPC_URL" \
        cargo test --package zradar-functional-tests --test functional_tests -- --include-ignored --list
    TEST_RESULT=$?
elif [ -n "$TEST_FILTER" ]; then
    TEST_DATABASE_URL="$TEST_DATABASE_URL" \
    TEST_API_URL="$TEST_API_URL" \
    TEST_GRPC_URL="$TEST_GRPC_URL" \
    TEST_API_KEY=zk_test_default \
        cargo test --package zradar-functional-tests --test functional_tests "$TEST_FILTER" \
            -- --include-ignored --nocapture --test-threads=1
    TEST_RESULT=$?
else
    TEST_DATABASE_URL="$TEST_DATABASE_URL" \
    TEST_API_URL="$TEST_API_URL" \
    TEST_GRPC_URL="$TEST_GRPC_URL" \
    TEST_API_KEY=zk_test_default \
        cargo test --package zradar-functional-tests --test functional_tests \
            -- --include-ignored --nocapture --test-threads="$FUNCTIONAL_TEST_THREADS"
    TEST_RESULT=$?
fi
set -e

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
if [ $TEST_RESULT -eq 0 ]; then
    ok "All functional tests passed"
else
    err "Some tests failed"
fi

# Let the EXIT trap perform teardown with this status.
exit $TEST_RESULT
