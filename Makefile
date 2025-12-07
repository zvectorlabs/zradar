.PHONY: help dev start stop restart logs clean test functional_tests build-prod prod deploy

# Default target
help:
	@echo "zradar Makefile - Development Optimized"
	@echo ""
	@echo "📦 Development Commands (default):"
	@echo "  make dev            - Start development environment (hot reload, default)"
	@echo "  make dev-logs       - Start development and follow logs (Ctrl+C to stop)"
	@echo "  make start          - Alias for 'make dev'"
	@echo "  make stop           - Stop all services"
	@echo "  make restart        - Restart development environment"
	@echo "  make status         - Show service status and health"
	@echo "  make logs           - View all logs"
	@echo "  make logs-server    - View zradar server logs"
	@echo "  make clean          - Stop and remove containers (keeps data/)"
	@echo "  make clean-all      - Remove containers AND data/ directory (⚠️ deletes all data)"
	@echo ""
	@echo "🧪 Testing Commands:"
	@echo "  make test           - Run unit tests"
	@echo "  make functional_tests - Run functional tests (fresh Docker)"
	@echo "  make functional_tests_fast - Run functional tests (reuse Docker)"
	@echo "  make functional_tests_fast TEST_NAME=test_name - Run specific test"
	@echo ""
	@echo "🏗️  Production Build Commands:"
	@echo "  make build-prod     - Build production Docker images"
	@echo "  make prod           - Run production-like environment locally"
	@echo "  make prod-stop      - Stop production environment"
	@echo ""
	@echo "🚀 Deployment Commands:"
	@echo "  make deploy         - Deploy with external databases (production)"
	@echo ""
	@echo "🔧 Utility Commands:"
	@echo "  make health         - Check service health"
	@echo "  make shell          - Open shell in zradar container"
	@echo "  make db-shell       - Open PostgreSQL shell"
	@echo "  make ch-shell       - Open ClickHouse shell"
	@echo "  make db-gui         - Open database GUI (Adminer)"
	@echo "  make migrate        - Run database migrations"
	@echo "  make sqlx-prepare   - Generate SQLx offline query cache"
	@echo "  make clean-sqlx     - Regenerate SQLx cache"
	@echo ""
	@echo "💡 Note: SQLx cache is auto-generated on first 'make dev' or 'make build-prod'"
	@echo ""

# =============================================================================
# DEVELOPMENT (Default Mode)
# =============================================================================

# Start development environment (hot reload, direct DB access)
dev: hook ensure-sqlx-cache
	@echo "🔧 Starting development environment..."
	@./docker-start.sh dev

# Start development environment and follow logs
dev-logs: hook ensure-sqlx-cache
	@echo "🔧 Starting development environment with log following..."
	@./docker-start.sh dev follow

# Ensure SQLx cache exists (generate if missing)
ensure-sqlx-cache:
	@if [ ! -d .sqlx ]; then \
		echo "📦 SQLx cache not found, generating..."; \
		$(MAKE) sqlx-prepare; \
	else \
		echo "✅ SQLx cache exists"; \
	fi

# Default: start development
start: dev

# Stop all services
stop:
	@echo "🛑 Stopping services..."
	@docker-compose down

# Restart development environment
restart: stop dev

# View all logs
logs:
	@docker-compose logs -f

# View zradar server logs only
logs-server:
	@docker-compose logs -f zradar

# Show service status and health
status:
	@echo "📊 Docker Services Status:"
	@docker-compose ps
	@echo ""
	@echo "🏥 Health Check:"
	@curl -s http://localhost:8080/health | jq . 2>/dev/null || echo "❌ Health check failed or jq not installed"

# Clean containers and Docker volumes (keeps data/ directory)
clean:
	@echo "🧹 Cleaning up containers..."
	@docker-compose down -v
	@docker-compose -f docker-compose.prod.yml down -v
	@docker-compose -f docker-compose.deploy.yml down
	@docker system prune -f
	@echo "✅ Containers removed. Database data preserved in ./data/"

# Clean everything including data directory (WARNING: deletes all data!)
clean-all: clean
	@echo "⚠️  WARNING: This will delete ALL data in ./data/ directory!"
	@echo "Press Ctrl+C to cancel, or Enter to continue..."
	@read confirm
	@echo "🗑️  Removing data directory..."
	@rm -rf ./data
	@echo "✅ All data removed!"

# Clean and regenerate SQLx cache
clean-sqlx:
	@echo "🧹 Cleaning SQLx cache..."
	@rm -rf .sqlx
	@echo "✅ SQLx cache removed"
	@$(MAKE) sqlx-prepare

# =============================================================================
# PRODUCTION BUILD & LOCAL TESTING
# =============================================================================

# Build production Docker images
build-prod: ensure-sqlx-cache
	@echo "🏗️  Building production images..."
	@docker-compose -f docker-compose.prod.yml build

# Run production-like environment locally (with local databases)
prod:
	@echo "🚀 Starting production-like environment..."
	@./docker-start.sh prod

# Stop production environment
prod-stop:
	@echo "🛑 Stopping production environment..."
	@docker-compose -f docker-compose.prod.yml down

# =============================================================================
# DEPLOYMENT (External Databases)
# =============================================================================

# Deploy to production (assumes external databases)
deploy:
	@echo "🚀 Deploying to production..."
	@if [ ! -f .env.prod ]; then \
		echo "❌ Error: .env.prod not found"; \
		echo "Create .env.prod with DATABASE_URL, CLICKHOUSE_URL, REDIS_URL, etc."; \
		exit 1; \
	fi
	@docker-compose -f docker-compose.deploy.yml --env-file .env.prod up -d

# Stop deployment
deploy-stop:
	@docker-compose -f docker-compose.deploy.yml down

# =============================================================================
# TESTING
# =============================================================================

# Run unit tests
test:
	@echo "🧪 Running unit tests..."
	@cargo test

# Run functional tests (fresh Docker environment)
functional_tests:
	@echo "🧪 Running functional tests (fresh Docker)..."
	@chmod +x scripts/test-rust-functional.sh
	@./scripts/test-rust-functional.sh

# Run functional tests with Docker reuse (faster)
# Usage: make functional_tests_fast TEST_NAME=test_name
functional_tests_fast:
	@echo "🧪 Running functional tests (reuse Docker)..."
	@chmod +x scripts/test-rust-functional.sh
	@./scripts/test-rust-functional.sh -r $(if $(TEST_NAME),$(TEST_NAME),)

# =============================================================================
# UTILITIES
# =============================================================================

# Check service health
health:
	@echo "💓 Checking health..."
	@curl -sf http://localhost:8080/health && echo "✅ Service healthy" || echo "❌ Service not healthy"
	@curl -sf http://localhost:8080/health/ready && echo "✅ Service ready" || echo "❌ Service not ready"

# Open shell in zradar container
shell:
	@docker-compose exec zradar /bin/sh

# Open PostgreSQL shell
db-shell:
	@docker-compose exec postgres psql -U zradar -d zradar

# Open ClickHouse shell
ch-shell:
	@docker-compose exec clickhouse clickhouse-client

# Open database GUI (Adminer)
db-gui:
	@echo "🗄️  Opening Adminer at http://localhost:8081"
	@open http://localhost:8081 2>/dev/null || xdg-open http://localhost:8081 2>/dev/null || echo "Open http://localhost:8081 in your browser"

# Run database migrations
migrate:
	@echo "🗄️  Running database migrations..."
	@sqlx migrate run

# Generate SQLx offline query cache (requires DATABASE_URL)
sqlx-prepare:
	@echo "📦 Generating SQLx query cache..."
	@echo "Starting PostgreSQL..."
	@docker-compose up postgres -d
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if docker-compose exec -T postgres pg_isready -U zradar > /dev/null 2>&1; then \
			echo "✅ PostgreSQL is ready"; \
			break; \
		fi; \
		echo "Waiting... ($$i/10)"; \
		sleep 2; \
	done
	@echo "Running migrations..."
	@bash -c 'set -a; source .env 2>/dev/null || true; set +a; \
		DATABASE_URL="postgres://zradar:$${POSTGRES_PASSWORD:-dev_password}@localhost:5432/zradar" sqlx migrate run || true'
	@echo "Generating query cache..."
	@bash -c 'set -a; source .env 2>/dev/null || true; set +a; \
		DATABASE_URL="postgres://zradar:$${POSTGRES_PASSWORD:-dev_password}@localhost:5432/zradar" cargo sqlx prepare --workspace'
	@echo "✅ SQLx query cache generated in .sqlx/"
	@echo "💡 Tip: Commit .sqlx/ to git for faster builds"

# Install/Update git hooks
hook:
	@echo "🪝 Checking git hooks..."
	@mkdir -p .git/hooks
	@for hook in pre-commit commit-msg; do \
		if [ ! -f .git/hooks/$$hook ] || ! cmp -s scripts/hooks/$$hook .git/hooks/$$hook; then \
			echo "📦 Installing $$hook hook..."; \
			cp scripts/hooks/$$hook .git/hooks/$$hook; \
			chmod +x .git/hooks/$$hook; \
		else \
			echo "✅ $$hook hook is up to date"; \
		fi \
	done

# =============================================================================
# LOCAL RUST DEVELOPMENT (without Docker)
# =============================================================================

# Build release locally
release:
	@echo "📦 Building release..."
	@cargo build --release

# Run locally (requires external databases)
run:
	@echo "🏃 Running locally..."
	@cargo run --bin zradar

# Format code
fmt:
	@cargo fmt

# Check code
check:
	@cargo check

# Lint code
lint:
	@cargo clippy -- -D warnings

# Fix warnings
fix:
	@cargo fix --allow-dirty --allow-staged
	@cargo clippy --fix --allow-dirty --allow-staged
