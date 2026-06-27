#!/usr/bin/env python3
import sys
import os
import subprocess
import time
import secrets
import urllib.request

def main():
    if len(sys.argv) < 2:
        mode = "dev"
    else:
        mode = sys.argv[1]

    follow_logs = "follow" in sys.argv[2:] or (len(sys.argv) >= 3 and sys.argv[2] == "follow")

    if mode not in ("dev", "prod"):
        print(f"❌ Invalid mode: {mode}")
        print(f"Usage: {sys.argv[0]} [dev|prod] [follow]")
        sys.exit(1)

    print("🐳 zradar Docker Quick Start")
    print("==============================")
    print(f"Mode: {mode}\n")

    # Check Docker is running
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True)
    except Exception:
        print("❌ Docker is not running. Please start Docker and try again.")
        sys.exit(1)

    print("✅ Docker is running\n")

    # Set compose file and ports based on mode
    if mode == "prod":
        compose_file = "docker-compose.prod.yml"
        api_port = "9006"
        otlp_port = "9005"
        postgres_port = "9001"
    else:
        compose_file = "docker-compose.yml"
        api_port = "8081"
        otlp_port = "4317"
        postgres_port = "5432"

    # Check if .env exists, create if not
    if not os.path.exists(".env"):
        print("📝 Creating .env file...")
        if mode == "prod":
            password = secrets.token_hex(8)
            with open(".env", "w", encoding="utf-8") as f:
                f.write(f"# zradar Production Environment Variables\nPOSTGRES_PASSWORD=prod_password_{password}\nRUST_LOG=info,zradar=info\nQUEUE_TYPE=postgres\nSTORAGE_TYPE=local\n")
        else:
            with open(".env", "w", encoding="utf-8") as f:
                f.write("# zradar Development Environment Variables\nPOSTGRES_PASSWORD=dev_password\nRUST_LOG=debug,zradar=trace\nQUEUE_TYPE=postgres\nSTORAGE_TYPE=local\n")
        print("✅ Created .env with default passwords")
    else:
        print("✅ Using existing .env file")

    print("\n🚀 Starting zradar services...\n")

    # Start services
    up_cmd = ["docker-compose", "-f", compose_file, "up", "-d"]
    if mode == "prod":
        up_cmd.append("--build")
        
    try:
        subprocess.run(up_cmd, check=True)
    except subprocess.CalledProcessError:
        print("❌ Failed to start docker-compose services")
        sys.exit(1)

    print("")

    # If following logs
    if follow_logs:
        print("📜 Following logs from all services (compilation may take 2-3 minutes on first run)...")
        print("    Press Ctrl+C to stop watching logs (services will keep running)\n")
        
        try:
            subprocess.run(["docker-compose", "-f", compose_file, "logs", "-f"])
        except KeyboardInterrupt:
            pass
        sys.exit(0)

    # For non-follow mode, wait for health checks
    print("⏳ Waiting for services to be healthy...")
    print("   (First-time compilation may take 2-3 minutes)")
    time.sleep(5)

    max_wait = 180 if mode == "dev" else 60
    counter = 0
    ready = False

    while counter < max_wait:
        try:
            with urllib.request.urlopen(f"http://localhost:{api_port}/health", timeout=2) as conn:
                if conn.getcode() == 200:
                    print("\n✅ zradar is ready!")
                    ready = True
                    break
        except Exception:
            pass
        print(".", end="", flush=True)
        time.sleep(2)
        counter += 2

    if not ready:
        print("\n⚠️  Services took longer than expected to start.")
        print("   View build logs with: just logs")
        print("   Check status with: just status")
        sys.exit(1)

    print("\n📊 Service Status:")
    subprocess.run(["docker-compose", "-f", compose_file, "ps"])

    print(f"\n✨ zradar is running in {mode} mode!\n")
    print("Available endpoints:")
    print(f"  🔹 OTLP gRPC:     localhost:{otlp_port}")
    print(f"  🔹 Health Check:  http://localhost:{api_port}/health")
    print(f"  🔹 Admin API:     http://localhost:{api_port}")
    print(f"  🔹 PostgreSQL:    localhost:{postgres_port}")

    if mode == "dev":
        print("\n🔧 Development Features:")
        print("  ✅ Hot reload enabled (code changes auto-rebuild)")
        print("  ✅ Direct database access on standard ports")
        print("  ✅ Debug logging enabled")

    print("\nNext steps:")
    if mode == "dev":
        print("  1. Edit code in crates/ - changes will auto-reload")
        print("  2. View logs:")
        print("     just logs  OR  just dev-logs (follow mode)")
        print("  3. Run tests:")
        print("     just test")
        print("  4. Stop services:")
        print("     just stop  OR  docker-compose down")
    else:
        print("  1. Create an API key")
        print("  2. Send test traces:")
        print("     cd examples/python && uv run send_trace.py")
        print("  3. View logs:")
        print("     just logs  OR  docker-compose -f " + compose_file + " logs -f")
        print("  4. Stop services:")
        print("     just prod-stop  OR  docker-compose -f " + compose_file + " down")
    print("")

if __name__ == '__main__':
    main()
