#!/usr/bin/env python3
import sys
import os
import shutil
import subprocess

GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
RED = '\033[0;31m'
NC = '\033[0m'

def main():
    print(f"{GREEN}====================================={NC}")
    print(f"{GREEN}  zradar Bootstrap Script{NC}")
    print(f"{GREEN}====================================={NC}\n")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    os.chdir(root_dir)

    # Required env vars
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print(f"{YELLOW}Warning: DATABASE_URL not set, using default{NC}")
        os.environ["DATABASE_URL"] = "postgresql://zradar:password@localhost:5432/zradar"

    # 1. Check for PostgreSQL
    print(f"{GREEN}[1/5]{NC} Checking PostgreSQL...")
    if shutil.which("psql"):
        print("  ✓ PostgreSQL client found")
    else:
        print(f"{RED}  ✗ PostgreSQL client not found{NC}", file=sys.stderr)
        print("  Please install PostgreSQL 17+", file=sys.stderr)
        sys.exit(1)

    # 2. Check for sqlx-cli
    print(f"{GREEN}[2/5]{NC} Checking sqlx-cli...")
    if shutil.which("sqlx"):
        print("  ✓ sqlx-cli found")
    else:
        print(f"{YELLOW}  ⚠ sqlx-cli not found, installing...{NC}")
        try:
            # We use cargo install. Specify CARGO_TARGET_DIR to separate target folders if needed
            env = os.environ.copy()
            subprocess.run(["cargo", "install", "sqlx-cli", "--no-default-features", "--features", "postgres"], env=env, check=True)
        except subprocess.CalledProcessError:
            print(f"{RED}  ✗ Failed to install sqlx-cli{NC}", file=sys.stderr)
            sys.exit(1)

    # 3. Run PostgreSQL migrations
    print(f"{GREEN}[3/5]{NC} Running PostgreSQL migrations...")
    try:
        env = os.environ.copy()
        subprocess.run(["sqlx", "migrate", "run", "--source", "migrations"], env=env, check=True)
        print("  ✓ Migrations completed")
    except subprocess.CalledProcessError:
        print(f"{RED}  ✗ Migrations failed{NC}", file=sys.stderr)
        sys.exit(1)

    # 4. Create data directories
    print(f"{GREEN}[4/5]{NC} Creating data directories...")
    os.makedirs("data", exist_ok=True)
    print("  ✓ Data directories created")

    # 5. Check configuration
    print(f"{GREEN}[5/5]{NC} Checking configuration...")
    if not os.path.exists("config.toml"):
        if os.path.exists("config.toml.example"):
            print("  Creating config.toml from example...")
            shutil.copy("config.toml.example", "config.toml")
            print("  ✓ Config file created")
            print(f"{YELLOW}  ⚠ Please review and update config.toml{NC}")
    else:
        print("  ✓ Config file exists")

    if not os.path.exists(".env"):
        if os.path.exists("env.example"):
            print("  Creating .env from example...")
            shutil.copy("env.example", ".env")
            print("  ✓ .env file created")
            print(f"{YELLOW}  ⚠ Please review and update .env{NC}")
    else:
        print("  ✓ .env file exists")

    print(f"\n{GREEN}====================================={NC}")
    print(f"{GREEN}  Bootstrap Complete!{NC}")
    print(f"{GREEN}====================================={NC}\n")
    print("Next steps:")
    print("  1. Review and update config.toml and .env")
    print("  2. Start the server: cargo run --release")
    print("  3. Access Admin API: http://localhost:8081")
    print("  4. View API docs: http://localhost:8081/swagger-ui/")
    print("  5. OTLP endpoint: localhost:4317 (gRPC)\n")
    print("To create an admin user, first register via:")
    print("  POST http://localhost:8081/api/v1/auth/register\n")

if __name__ == '__main__':
    main()
