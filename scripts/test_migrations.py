#!/usr/bin/env python3
import sys
import os
import subprocess
import time

GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
RED = '\033[0;31m'
NC = '\033[0m'

def log_header(msg):
    print(f"{GREEN}====================================={NC}")
    print(f"{GREEN}  {msg}{NC}")
    print(f"{GREEN}====================================={NC}\n")

def log_step(step, msg):
    print(f"{YELLOW}[{step}]{NC} {msg}...")

def log_ok(msg):
    print(f"{GREEN}  ✓ {msg}{NC}")

def log_err(msg):
    print(f"{RED}  ✗ {msg}{NC}", file=sys.stderr)

def main():
    log_header("Testing Auto-Migration System")

    # Determine paths and target directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    os.chdir(root_dir)

    cargo_target_dir = os.environ.get("CARGO_TARGET_DIR", "target")
    # Make sure cargo_target_dir is absolute
    cargo_target_dir = os.path.abspath(cargo_target_dir)

    binary_name = "zradar.exe" if os.name == 'nt' else "zradar"
    zradar_bin = os.path.join(cargo_target_dir, "debug", binary_name)

    # 1. Check if PostgreSQL is running
    log_step("1/4", "Checking if PostgreSQL is running")
    try:
        res = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, check=True)
        if "zradar-postgres" not in res.stdout:
            log_err("PostgreSQL is not running")
            print("  Run: docker-compose up -d postgres", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        log_err(f"Failed to check Docker status: {e}")
        sys.exit(1)
    log_ok("PostgreSQL is running")

    # 2. Build the application
    log_step("2/4", "Building zradar server")
    env = os.environ.copy()
    env["SQLX_OFFLINE"] = "true"
    try:
        subprocess.run(["cargo", "build", "--bin", "zradar", "--quiet"], env=env, check=True)
    except subprocess.CalledProcessError:
        log_err("Build failed")
        sys.exit(1)
    log_ok("Build successful")

    if not os.path.exists(zradar_bin):
        log_err(f"zradar binary not found at: {zradar_bin}")
        sys.exit(1)

    # 3. Test PostgreSQL migrations
    log_step("3/4", "Testing PostgreSQL migrations")
    
    # Drop and recreate database for clean test
    drop_cmd = ["docker", "exec", "zradar-postgres", "psql", "-U", "zradar", "-c", "DROP DATABASE IF EXISTS zradar_migration_test;"]
    create_cmd = ["docker", "exec", "zradar-postgres", "psql", "-U", "zradar", "-c", "CREATE DATABASE zradar_migration_test;"]
    
    subprocess.run(drop_cmd, capture_output=True)
    try:
        subprocess.run(create_cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        log_err(f"Failed to create test database: {e.stderr.strip()}")
        sys.exit(1)

    # Run server with migrations enabled and check output
    run_env = os.environ.copy()
    run_env["DATABASE_URL"] = "postgresql://zradar:dev_password@localhost:5432/zradar_migration_test"
    run_env["AUTO_MIGRATE_POSTGRES"] = "true"

    try:
        # Start zradar and read output (with a 5 second timeout)
        proc = subprocess.Popen([zradar_bin], env=run_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Read output line-by-line with timeout
        output = []
        start_time = time.time()
        success = False
        while True:
            # check timeout
            if time.time() - start_time > 5.0:
                break
            
            # non-blocking check
            line = proc.stdout.readline()
            if line:
                output.append(line)
                if "PostgreSQL migrations completed" in line:
                    success = True
                    break
            elif proc.poll() is not None:
                # process exited
                break
            
            time.sleep(0.05)
            
        proc.terminate()
        proc.wait()
        
        if not success:
            log_err("PostgreSQL migrations failed to complete or print success message")
            print("Server output:")
            print("".join(output))
            sys.exit(1)
            
    except Exception as e:
        log_err(f"Failed to run server: {e}")
        sys.exit(1)

    # Verify migration tracking table exists and has rows
    try:
        verify_cmd = ["docker", "exec", "zradar-postgres", "psql", "-U", "zradar", "-d", "zradar_migration_test", "-t", "-c", "SELECT COUNT(*) FROM _sqlx_migrations;"]
        res = subprocess.run(verify_cmd, capture_output=True, text=True, check=True)
        count = int(res.stdout.strip())
        if count > 0:
            log_ok(f"PostgreSQL migrations tracked: {count} migrations applied")
        else:
            log_err("No migrations found in tracking table")
            sys.exit(1)
    except Exception as e:
        log_err(f"Failed to verify migration tracking: {e}")
        sys.exit(1)

    # 4. Test idempotency
    log_step("4/4", "Testing idempotency (running migrations again)")
    try:
        proc = subprocess.Popen([zradar_bin], env=run_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        output = []
        start_time = time.time()
        success = False
        while True:
            if time.time() - start_time > 5.0:
                break
            
            line = proc.stdout.readline()
            if line:
                output.append(line)
                if "No pending migrations" in line or "PostgreSQL migrations completed" in line: # Or no migrations applied
                    # In actual zradar output, it prints "No pending migrations"
                    if "No pending migrations" in line:
                        success = True
                        break
            elif proc.poll() is not None:
                break
            
            time.sleep(0.05)
            
        proc.terminate()
        proc.wait()
        
        if not success:
            log_err("Idempotency test failed (did not report 'No pending migrations')")
            print("Server output:")
            print("".join(output))
            sys.exit(1)
    except Exception as e:
        log_err(f"Failed to run idempotency test: {e}")
        sys.exit(1)
        
    log_ok("Idempotency verified - migrations not re-applied")

    # Query migration history
    print(f"\n{YELLOW}Migration history:{NC}\n")
    print("PostgreSQL migrations:")
    history_cmd = ["docker", "exec", "zradar-postgres", "psql", "-U", "zradar", "-d", "zradar_migration_test", "-c", "SELECT version, description, success FROM _sqlx_migrations ORDER BY version;"]
    subprocess.run(history_cmd)

    # Cleanup
    subprocess.run(drop_cmd, capture_output=True)

    print("")
    log_header("All Tests Passed! ✓")
    print("The auto-migration system is working correctly!\n")

if __name__ == '__main__':
    main()
