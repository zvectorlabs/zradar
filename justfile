# zradar Justfile — Development Task Runner (Cross-Platform)
#
# Use PowerShell on Windows instead of cmd.exe
set windows-shell := ["powershell.exe", "-c"]

# Export CARGO_TARGET_DIR environment variable
# If CARGO_TARGET_DIR is set in the shell environment, use it. Otherwise, default to "target".
export CARGO_TARGET_DIR := env("CARGO_TARGET_DIR", "target")

# Opt-in fast builds: `ZRADAR_FAST_BUILD=1 just <recipe>` links with mold and
# caches compiles with sccache (Linux/macOS; both must be installed). Off by
# default so default builds need no extra tooling, and any RUSTFLAGS /
# RUSTC_WRAPPER you already set is honored. Compile/link dominates build time,
# so this is the real speedup for the whole `just test`/`check`/`build` cycle.
fast_build := env("ZRADAR_FAST_BUILD", "")
export RUSTFLAGS := env("RUSTFLAGS", if fast_build == "" { "" } else { "-C link-arg=-fuse-ld=mold" })
export RUSTC_WRAPPER := env("RUSTC_WRAPPER", if fast_build == "" { "" } else { "sccache" })

# Show available recipes by default
default:
    @just --list

# =============================================================================
# BOOTSTRAP (first-time setup)
# =============================================================================

# Install all required tools and git hooks — run once after cloning
bootstrap:
    #!/usr/bin/env python3
    import subprocess, shutil

    def run(cmd):
        subprocess.run(cmd, check=True)

    def check(cmd):
        return shutil.which(cmd) is not None

    print("==> Installing cargo tools")
    tools = [
        ("cargo-nextest", ["cargo", "install", "cargo-nextest", "--locked"]),
        ("sqlx",          ["cargo", "install", "sqlx-cli", "--no-default-features", "--features", "postgres", "--locked"]),
        ("cargo-deny",    ["cargo", "install", "cargo-deny", "--locked"]),
    ]
    for binary, install_cmd in tools:
        if check(binary):
            print(f"  ✓ {binary} already installed")
        else:
            print(f"  → installing {binary}...")
            run(install_cmd)
            print(f"  ✓ {binary} installed")

    print("==> Installing git hooks")
    run(["just", "hook"])

    print()
    print("✓ Bootstrap complete. Run 'just doctor' to verify, then 'just dev' to start.")

# Check environment and auto-fix anything installable via cargo
doctor:
    #!/usr/bin/env python3
    import subprocess, sys, shutil, os

    REQUIRED_RUST = (1, 93, 0)
    ok = True

    def which(cmd):
        return shutil.which(cmd) is not None

    def run(cmd):
        subprocess.run(cmd, check=True)

    def ver(cmd):
        try:
            return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT).strip().split("\n")[0]
        except Exception:
            return "?"

    # ── Rust toolchain ────────────────────────────────────────────────────────
    print("==> Rust toolchain")
    try:
        out = subprocess.check_output(["rustc", "--version"], text=True).strip()
        parts = out.split()[1].split(".")
        version = tuple(int(x.split("-")[0]) for x in parts[:3])
        if version < REQUIRED_RUST:
            print(f"  ✗ {out} — need >= {'.'.join(str(x) for x in REQUIRED_RUST)}")
            print("    Fix: rustup override set 1.93.0")
            ok = False
        else:
            print(f"  ✓ {out}")
    except FileNotFoundError:
        print("  ✗ rustc not found")
        print("    Fix: curl https://sh.rustup.rs -sSf | sh")
        ok = False

    # ── Cargo tools (auto-install if missing) ─────────────────────────────────
    print("==> Cargo tools")
    cargo_tools = [
        ("cargo-nextest", ["cargo", "install", "cargo-nextest", "--locked"]),
        ("sqlx",          ["cargo", "install", "sqlx-cli", "--no-default-features", "--features", "postgres", "--locked"]),
        ("cargo-deny",    ["cargo", "install", "cargo-deny", "--locked"]),
    ]
    for binary, install_cmd in cargo_tools:
        if which(binary):
            print(f"  ✓ {binary} ({ver([binary, '--version'])})")
        else:
            print(f"  ○ {binary} not found — installing...")
            try:
                run(install_cmd)
                print(f"  ✓ {binary} installed")
            except subprocess.CalledProcessError:
                print(f"  ✗ {binary} install failed — check cargo output above")
                ok = False

    # ── System tools (must be installed by user) ──────────────────────────────
    print("==> System tools")
    system_tools = {
        "docker":  {
            "linux": "sudo apt install docker.io  OR  https://docs.docker.com/engine/install/",
            "darwin": "brew install --cask docker  OR  https://docs.docker.com/desktop/mac/",
        },
        "python3": {
            "linux": "sudo apt install python3",
            "darwin": "brew install python3",
        },
    }
    import platform
    plat = "darwin" if platform.system() == "Darwin" else "linux"
    for binary, instructions in system_tools.items():
        if which(binary):
            print(f"  ✓ {binary} ({ver([binary, '--version'])})")
        else:
            print(f"  ✗ {binary} not found")
            print(f"    Install: {instructions[plat]}")
            ok = False

    # ── Optional fast-build tools ─────────────────────────────────────────────
    print("==> Optional fast-build tools")
    for tool in ["mold", "sccache"]:
        if which(tool):
            print(f"  ✓ {tool} — activate with ZRADAR_FAST_BUILD=1")
        else:
            print(f"  ○ {tool} not installed (optional)")
            print(f"    Install: sudo apt install {tool}  OR  brew install {tool}")

    # ── Git hooks ─────────────────────────────────────────────────────────────
    print("==> Git hooks")
    try:
        git_common = subprocess.check_output(
            ["git", "rev-parse", "--git-common-dir"], text=True
        ).strip()
    except Exception:
        git_common = ".git"
    hooks_installed = all(
        os.path.exists(os.path.join(git_common, "hooks", h)) and
        os.access(os.path.join(git_common, "hooks", h), os.X_OK)
        for h in ["pre-commit", "commit-msg"]
    )
    if hooks_installed:
        print("  ✓ pre-commit and commit-msg hooks installed")
    else:
        print("  ○ hooks not installed — running: just hook")
        run(["just", "hook"])
        print("  ✓ hooks installed")

    print()
    if ok:
        print("✓ All checks passed.")
    else:
        print("✗ Some checks failed — see above for fix instructions.")
        sys.exit(1)

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
    #!/usr/bin/env python3
    import os, urllib.request
    print("📊 Docker Services Status:")
    os.system("docker-compose ps")
    print("\n🏥 Health Check:")
    try:
        res = urllib.request.urlopen('http://localhost:8081/health', timeout=2)
        print('✅ Service healthy' if res.getcode() == 200 else '❌ Service not healthy')
    except Exception as e:
        print('❌ Health check failed:', e)

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

# Run benchmarks
bench args="":
    @echo "🚀 Running benchmarks..."
    cargo bench {{args}}

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
    #!/usr/bin/env python3
    import urllib.request
    try:
        res1 = urllib.request.urlopen('http://localhost:8081/health', timeout=2)
        print('✅ Service healthy' if res1.getcode() == 200 else '❌ Service not healthy')
    except Exception as e:
        print('❌ Service not healthy:', e)
    try:
        res2 = urllib.request.urlopen('http://localhost:8081/health/ready', timeout=2)
        print('✅ Service ready' if res2.getcode() == 200 else '❌ Service not ready')
    except Exception as e:
        print('❌ Service not ready:', e)

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
    import os, shutil, filecmp, stat, subprocess
    print("🪝 Checking git hooks...")
    # Use --git-common-dir so this works in both normal clones and git worktrees
    git_common = subprocess.check_output(
        ["git", "rev-parse", "--git-common-dir"], text=True
    ).strip()
    hooks_dir = os.path.join(git_common, "hooks")
    os.makedirs(hooks_dir, exist_ok=True)
    for hook in ["pre-commit", "commit-msg"]:
        src = f"scripts/hooks/{hook}"
        dst = os.path.join(hooks_dir, hook)
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
    cargo check --all-targets

# Lint code
lint:
    cargo clippy --all-targets -- -D warnings

# Fix warnings
fix:
    cargo fix --allow-dirty --allow-staged
    cargo clippy --fix --allow-dirty --allow-staged

# =============================================================================
# EXAMPLES — agent framework E2E tests (requires: just dev running)
# =============================================================================

# Run a single framework example against the running dev stack
# Convention: examples/<provider>/<language>/example.py|ts
# Usage: just example langchain
example framework:
    #!/usr/bin/env python3
    import subprocess, sys, os
    fw = "{{framework}}"
    base = f"examples/{fw}"

    # All examples follow <provider>/<language>/example.* convention
    py_path = f"{base}/python/example.py"
    ts_path = f"{base}/typescript/example.ts"

    if os.path.exists(py_path):
        print(f"==> Running {fw} Python example")
        subprocess.run(["uv", "run", "example.py"],
                       cwd=f"{base}/python", check=True,
                       env={**os.environ, "ZRADAR_API_KEY": os.environ.get("ZRADAR_API_KEY", "zk_dev_example")})
    elif os.path.exists(ts_path):
        ts_dir = f"{base}/typescript"
        print(f"==> Running {fw} TypeScript example")
        subprocess.run(["pnpm", "install", "--silent"], cwd=ts_dir, check=True)
        subprocess.run(["pnpm", "start"], cwd=ts_dir, check=True,
                       env={**os.environ, "ZRADAR_API_KEY": os.environ.get("ZRADAR_API_KEY", "zk_dev_example")})
    else:
        print(f"✗ No python/example.py or typescript/example.ts found for: {fw}")
        sys.exit(1)

# Run a framework example and validate spans arrived in zradar
# Requires: just dev running
# Usage: just example-test langchain
example-test framework:
    just example {{framework}}
    python3 scripts/validate_spans.py --framework {{framework}}

# Run all framework examples and validate spans
# Requires: just dev running
example-test-all:
    #!/usr/bin/env python3
    import subprocess, sys
    frameworks = [
        "langchain", "openai-agents", "openai", "pydantic-ai",
        "crewai", "llamaindex", "anthropic", "google-adk",
        "vercel-ai-sdk", "mastra",
    ]  # all follow <provider>/<language>/example.* convention
    failed = []
    for fw in frameworks:
        print(f"\n{'='*60}")
        print(f"Testing: {fw}")
        print('='*60)
        r = subprocess.run(["just", "example-test", fw])
        if r.returncode != 0:
            failed.append(fw)
    if failed:
        print(f"\n✗ Failed: {', '.join(failed)}")
        sys.exit(1)
    print("\n✓ All framework examples passed.")

# Regenerate expected_spans.json snapshot for a framework (run after intentional format changes)
# Usage: just example-update-snapshot langchain
example-update-snapshot framework:
    python3 scripts/validate_spans.py --framework {{framework}} --update-snapshot

# Check all example SDK dependencies for available updates on PyPI/npm
sdk-check:
    python3 scripts/check_sdk_versions.py

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
