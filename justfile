# zradar Justfile — Development Task Runner (Cross-Platform)
#
# Use PowerShell on Windows instead of cmd.exe
set windows-shell := ["powershell.exe", "-c"]

# Export CARGO_TARGET_DIR environment variable
# If CARGO_TARGET_DIR is set in the shell environment, use it. Otherwise, default to "target".
export CARGO_TARGET_DIR := env("CARGO_TARGET_DIR", "target")

# Show available recipes by default
default:
    @just --list

# =============================================================================
# DEVELOPMENT (Default Mode)
# =============================================================================

# Start development environment (hot reload, direct DB access)
dev: hook ensure-sqlx-cache
    @echo "🔧 Starting development environment..."
    python3 scripts/docker_start.py dev

# Start development environment and follow logs
run-dev: hook ensure-sqlx-cache
    @echo "🔧 Starting development environment with log following..."
    python3 scripts/docker_start.py dev follow

# Alias for run-dev
dev-logs: run-dev

# Default target alias to start dev environment
start: dev

# Stop all services
stop:
    @echo "🛑 Stopping services..."
    docker-compose down

# Restart development environment
restart: stop dev

# View all logs
logs:
    docker-compose logs -f

# View zradar server logs only
logs-server:
    docker-compose logs -f zradar

# Show service status and health check
status:
    @echo "📊 Docker Services Status:"
    docker-compose ps
    @echo ""
    @echo "🏥 Health Check:"
    @python3 -c "import urllib.request; \
        try: \
            res=urllib.request.urlopen('http://localhost:8081/health', timeout=2); \
            print('✅ Service healthy' if res.getcode() == 200 else '❌ Service not healthy'); \
        except Exception as e: \
            print('❌ Health check failed:', e)"

# Clean containers and Docker volumes (keeps data/ directory)
clean:
    @echo "🧹 Cleaning up containers..."
    docker-compose down -v
    docker-compose -f docker-compose.prod.yml down -v
    docker-compose -f docker-compose.deploy.yml down
    docker system prune -f
    @python3 -c "import shutil; shutil.rmtree('./data-test', ignore_errors=True)"
    @echo "✅ Containers removed. Database data preserved in ./data/"

# Clean everything including data directory (WARNING: deletes all data!)
clean-all: clean
    #!/usr/bin/env python3
    import shutil
    print("⚠️  WARNING: This will delete ALL data in ./data/ directory!")
    confirm = input("Press Enter to continue, or Ctrl+C to cancel...")
    print("🗑️  Removing data directory...")
    shutil.rmtree('./data', ignore_errors=True)
    print("✅ All data removed!")

# Clean and regenerate SQLx cache
clean-sqlx:
    @echo "🧹 Cleaning SQLx cache..."
    @python3 -c "import shutil; shutil.rmtree('.sqlx', ignore_errors=True)"
    @echo "✅ SQLx cache removed"
    just sqlx-prepare

# =============================================================================
# PRODUCTION BUILD & LOCAL TESTING
# =============================================================================

# Build production Docker images
build-prod: ensure-sqlx-cache
    @echo "🏗️  Building production images..."
    docker-compose -f docker-compose.prod.yml build

# Run production-like environment locally (with local databases)
prod:
    @echo "🚀 Starting production-like environment..."
    python3 scripts/docker_start.py prod

# Stop production environment
prod-stop:
    @echo "🛑 Stopping production environment..."
    docker-compose -f docker-compose.prod.yml down

# =============================================================================
# DEPLOYMENT (External Databases)
# =============================================================================

# Deploy to production (assumes external databases)
deploy:
    #!/usr/bin/env python3
    import os, subprocess, sys
    if not os.path.exists(".env.prod"):
        print("❌ Error: .env.prod not found", file=sys.stderr)
        print("Create .env.prod with DATABASE_URL, etc.", file=sys.stderr)
        sys.exit(1)
    subprocess.run(["docker-compose", "-f", "docker-compose.deploy.yml", "--env-file", ".env.prod", "up", "-d"])

# Stop deployment
deploy-stop:
    docker-compose -f docker-compose.deploy.yml down

# =============================================================================
# TESTING
# =============================================================================

# Run unit tests
test:
    @echo "🧪 Running unit tests..."
    cargo test

# Run unit tests + functional tests (fresh Docker environment)
test-all: test functional-tests
    @echo "✅ All tests passed"

# Run functional tests (fresh Docker — tears down containers after run)
functional-tests:
    @echo "🧪 Running functional tests (fresh Docker)..."
    python3 scripts/test_rust_functional.py

# Run functional tests with Docker/server reuse (preferred for local iteration)
functional-tests-fast test_name="":
    @echo "🧪 Running functional tests (reuse mode — fast iteration)..."
    python3 scripts/test_rust_functional.py -r {{ if test_name == "" { "" } else { test_name } }}

# =============================================================================
# UTILITIES
# =============================================================================

# Check service health
health:
    @python3 -c "import urllib.request; \
        try: \
            res1=urllib.request.urlopen('http://localhost:8081/health', timeout=2); \
            print('✅ Service healthy' if res1.getcode() == 200 else '❌ Service not healthy'); \
        except Exception as e: \
            print('❌ Service not healthy:', e); \
        try: \
            res2=urllib.request.urlopen('http://localhost:8081/health/ready', timeout=2); \
            print('✅ Service ready' if res2.getcode() == 200 else '❌ Service not ready'); \
        except Exception as e: \
            print('❌ Service not ready:', e)"

# Open shell in zradar container
shell:
    docker-compose exec zradar /bin/sh

# Open PostgreSQL shell
db-shell:
    docker-compose exec postgres psql -U zradar -d zradar

# Run database migrations
migrate:
    @echo "🗄️  Running database migrations..."
    sqlx migrate run

# Generate SQLx offline query cache (requires DATABASE_URL)
sqlx-prepare:
    #!/usr/bin/env python3
    import subprocess, time, os, sys
    print("📦 Generating SQLx query cache...")
    print("Starting PostgreSQL...")
    subprocess.run(["docker-compose", "up", "postgres", "-d"], check=True)
    print("Waiting for PostgreSQL to be ready...")
    ready = False
    for i in range(1, 11):
        res = subprocess.run(["docker-compose", "exec", "-T", "postgres", "pg_isready", "-U", "zradar"], capture_output=True)
        if res.returncode == 0:
            print("✅ PostgreSQL is ready")
            ready = True
            break
        print(f"Waiting... ({i}/10)")
        time.sleep(2)
    if not ready:
        print("❌ PostgreSQL failed to start", file=sys.stderr)
        sys.exit(1)
    
    print("Running migrations...")
    env = os.environ.copy()
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    
    postgres_password = env.get("POSTGRES_PASSWORD", "dev_password")
    db_url = f"postgres://zradar:{postgres_password}@localhost:5432/zradar"
    env["DATABASE_URL"] = db_url
    
    subprocess.run(["sqlx", "migrate", "run"], env=env)
    
    print("Generating query cache...")
    subprocess.run(["cargo", "sqlx", "prepare", "--workspace"], env=env, check=True)
    print("✅ SQLx query cache generated in .sqlx/")
    print("💡 Tip: Commit .sqlx/ to git for faster builds")

# Install/Update git hooks
hook:
    #!/usr/bin/env python3
    import os, shutil, filecmp, stat
    print("🪝 Checking git hooks...")
    os.makedirs(".git/hooks", exist_ok=True)
    for hook in ["pre-commit", "commit-msg"]:
        src = f"scripts/hooks/{hook}"
        dst = f".git/hooks/{hook}"
        if not os.path.exists(src):
            continue
        if not os.path.exists(dst) or not filecmp.cmp(src, dst, shallow=False):
            print(f"📦 Installing {hook} hook...")
            shutil.copy(src, dst)
            st = os.stat(dst)
            os.chmod(dst, st.st_mode | stat.S_IEXEC)
        else:
            print(f"✅ {hook} hook is up to date")

# =============================================================================
# RELEASE (version bump, tag, GitHub binary build)
# =============================================================================

# Show current semver (VERSION file)
show-version:
    @python3 -c "with open('VERSION', 'r') as f: print(f.read().strip())"

# Bump VERSION + [workspace.package].version only (does not commit or tag)
version-bump bump_arg="patch":
    python3 scripts/bump_version.py "{{bump_arg}}"

# One-shot: bump → commit → annotated tag → push branch + tag (triggers CI build)
release-publish bump_arg="patch": hook
    python3 scripts/release_publish.py "{{bump_arg}}"

# =============================================================================
# LOCAL RUST DEVELOPMENT (without Docker)
# =============================================================================

# Build release binary locally
build-release:
    @echo "📦 Building release..."
    cargo build --release -p zradar-server --bin zradar

# Alias for local release build
release: build-release

# Run locally (requires external databases)
run:
    @echo "🏃 Running locally..."
    cargo run --bin zradar

# Format code
fmt:
    @cargo fmt

# Check code
check:
    cargo check

# Lint code
lint:
    cargo clippy --all-targets -- -D warnings

# Fix warnings
fix:
    cargo fix --allow-dirty --allow-staged
    cargo clippy --fix --allow-dirty --allow-staged

# Helper to ensure SQLx cache exists
[private]
ensure-sqlx-cache:
    #!/usr/bin/env python3
    import os, subprocess
    if not os.path.isdir(".sqlx"):
        print("📦 SQLx cache not found, generating...")
        subprocess.run(["just", "sqlx-prepare"], check=True)
    else:
        print("✅ SQLx cache exists")
