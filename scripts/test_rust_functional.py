#!/usr/bin/env python3
import sys
import os
import subprocess
import time
import shutil
import signal
import argparse

RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'

def info(msg):
    print(f"{BLUE}  {msg}{NC}")

def ok(msg):
    print(f"{GREEN}✓ {msg}{NC}")

def warn(msg):
    print(f"{YELLOW}⚠ {msg}{NC}")

def err(msg):
    print(f"{RED}✗ {msg}{NC}", file=sys.stderr)

def print_banner():
    print(f"{BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{NC}")
    print(f"{BLUE}  zradar Functional Tests (Rust/Python){NC}")
    print(f"{BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{NC}\n")

class TestRunner:
    def __init__(self, args):
        self.args = args
        self.ctr = os.environ.get("CTR", "docker")
        self.pg_name = "zradar-test-postgres"
        self.pg_image = os.environ.get("ZRADAR_TEST_PG_IMAGE", "postgres:17-alpine")
        self.pg_port = 9011
        
        self.test_database_url = f"postgresql://zradar_test:test_pass_123@localhost:{self.pg_port}/zradar_test"
        self.test_api_url = "http://localhost:9015"
        self.test_grpc_url = "http://localhost:9016"
        
        self.cargo_target_dir = os.path.abspath(os.environ.get("CARGO_TARGET_DIR", "target"))
        binary_ext = ".exe" if os.name == 'nt' else ""
        self.zradar_bin = os.path.join(self.cargo_target_dir, "release", f"zradar{binary_ext}")
        
        self.server_proc = None
        self.skip_docker_setup = False
        self.config_state = "untouched"

    def pg_healthy(self):
        try:
            res = subprocess.run([self.ctr, "exec", self.pg_name, "psql", "-U", "zradar_test", "-d", "zradar_test", "-c", "SELECT 1;"],
                                 capture_output=True)
            return res.returncode == 0
        except Exception:
            return False

    def stop_postgres(self):
        subprocess.run([self.ctr, "rm", "-f", self.pg_name], capture_output=True)
        # We can also attempt to free the port on unix-like systems
        if os.name != 'nt':
            try:
                lsof_res = subprocess.run(["lsof", "-t", f"-i:{self.pg_port}"], capture_output=True, text=True)
                pids = lsof_res.stdout.strip().split()
                for pid in pids:
                    subprocess.run(["kill", "-9", pid], capture_output=True)
            except Exception:
                pass

    def start_postgres(self):
        info(f"Starting Postgres on localhost:{self.pg_port} (container {self.pg_name})...")
        cmd = [
            self.ctr, "run", "-d", "--name", self.pg_name,
            "-e", "POSTGRES_DB=zradar_test",
            "-e", "POSTGRES_USER=zradar_test",
            "-e", "POSTGRES_PASSWORD=test_pass_123",
            "-p", f"{self.pg_port}:5432",
            "--health-cmd", "pg_isready -U zradar_test",
            "--health-interval", "2s",
            "--health-timeout", "2s",
            "--health-retries", "10",
        ]
        # tmpfs is only supported on Linux/Docker
        if os.name != 'nt':
            cmd.extend(["--tmpfs", "/var/lib/postgresql/data"])
            
        cmd.append(self.pg_image)
        try:
            subprocess.run(cmd, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            err(f"Failed to start Postgres: {e.stderr.decode('utf-8', errors='ignore').strip()}")
            sys.exit(1)

    def wait_pg_healthy(self):
        timeout = 60
        start_time = time.time()
        info("Waiting for Postgres to be healthy...")
        while time.time() - start_time < timeout:
            if self.pg_healthy():
                ok("Postgres healthy")
                return True
            
            # check if container still exists
            inspect_res = subprocess.run([self.ctr, "inspect", self.pg_name], capture_output=True)
            if inspect_res.returncode != 0:
                err("Postgres container exited or does not exist")
                return False
                
            time.sleep(1)
            print(".", end="", flush=True)
        print("")
        err("Timeout waiting for Postgres")
        # print logs
        subprocess.run([self.ctr, "logs", "--tail", "30", self.pg_name])
        return False

    def preflight_checks(self):
        if not shutil.which(self.ctr):
            err(f"'{self.ctr}' container runtime not found on PATH")
            sys.exit(1)
        try:
            subprocess.run([self.ctr, "info"], check=True, capture_output=True)
        except Exception:
            err(f"'{self.ctr}' daemon not reachable — is it running?")
            sys.exit(1)

    def setup_config(self):
        info("Setting up test configuration...")
        shutil.rmtree("./data-test", ignore_errors=True)
        
        if os.path.exists("config.toml"):
            shutil.move("config.toml", "config.toml.backup")
            self.config_state = "backup"
            info("Backed up config.toml → config.toml.backup")
        else:
            self.config_state = "created"
            
        shutil.copy("config.test.toml", "config.toml")
        ok("Using config.test.toml")

    def restore_config(self):
        if self.config_state == "backup":
            if os.path.exists("config.toml.backup"):
                shutil.move("config.toml.backup", "config.toml")
                info("Restored config.toml")
        elif self.config_state == "created":
            if os.path.exists("config.toml"):
                os.remove("config.toml")
                info("Removed test config.toml")

    def run_tests(self):
        self.preflight_checks()
        
        # 1. Build server + tests
        print(f"{YELLOW}1️⃣  Building server and tests on host...{NC}")
        info("Building zradar server (release)...")
        
        env = os.environ.copy()
        
        # Build server
        try:
            subprocess.run(["cargo", "build", "--release", "--bin", "zradar"], env=env, check=True)
        except subprocess.CalledProcessError:
            err("Server build failed")
            sys.exit(1)
            
        if not os.path.exists(self.zradar_bin):
            err(f"Server binary not found at {self.zradar_bin}")
            sys.exit(1)
            
        info("Building functional test suite...")
        try:
            subprocess.run(["cargo", "build", "--package", "zradar-functional-tests", "--tests"], env=env, check=True)
        except subprocess.CalledProcessError:
            err("Functional test suite build failed")
            sys.exit(1)
            
        ok("Server and tests built\n")

        # 2. Infra setup
        if self.args.reuse and self.pg_healthy():
            print(f"{YELLOW}2️⃣  Reusing healthy Postgres container{NC}")
            self.skip_docker_setup = True
        else:
            print(f"{YELLOW}2️⃣  Starting a fresh Postgres container{NC}")
            self.stop_postgres()
            time.sleep(1)
            self.start_postgres()
            
        print("")

        # 3. Wait PG
        if not self.skip_docker_setup:
            print(f"{YELLOW}3️⃣  Waiting for Postgres to be healthy...{NC}")
            if not self.wait_pg_healthy():
                self.teardown(1)
            print("")

        # 4. Config
        self.setup_config()
        print("")

        # 5. Start Server
        print(f"{YELLOW}5️⃣  Starting zradar test server (migrations run on startup)...{NC}")
        server_env = os.environ.copy()
        server_env["DATABASE_URL"] = self.test_database_url
        server_env["QUERY_API_PORT"] = "9015"
        server_env["ZVRADAR_TEST_MODE"] = "1"
        server_env["RUST_LOG"] = "info,zradar=debug"
        
        try:
            # Inherit stdout/stderr to prevent the OS pipe buffer from filling up and hanging the process
            self.server_proc = subprocess.Popen([self.zradar_bin], env=server_env, text=True)
        except Exception as e:
            err(f"Failed to start server process: {e}")
            self.teardown(1)

        # Wait for server ready
        timeout = 60
        start_time = time.time()
        server_ready = False
        import urllib.request
        while time.time() - start_time < timeout:
            # check if server died
            if self.server_proc.poll() is not None:
                err("Server process died during startup")
                self.teardown(1)
            
            try:
                with urllib.request.urlopen(f"{self.test_api_url}/health", timeout=1) as conn:
                    if conn.getcode() == 200:
                        server_ready = True
                        ok(f"Server ready at {self.test_api_url}")
                        break
            except Exception:
                pass
            
            print(".", end="", flush=True)
            time.sleep(1)
        print("")
        if not server_ready:
            err("Server didn't start in time")
            self.teardown(1)
            
        info("Auth: static API key (zk_test_default)\n")

        # 6. Run tests
        print(f"{YELLOW}6️⃣  Running Rust functional tests...{NC}")
        print(f"{BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{NC}\n")

        test_env = os.environ.copy()
        test_env["TEST_DATABASE_URL"] = self.test_database_url
        test_env["TEST_API_URL"] = self.test_api_url
        test_env["TEST_GRPC_URL"] = self.test_grpc_url
        test_env["TEST_API_KEY"] = "zk_test_default"

        threads = os.environ.get("FUNCTIONAL_TEST_THREADS", "4")

        if shutil.which("cargo-nextest"):
            # nextest: the `ci` profile (see .config/nextest.toml) retries
            # load-dependent flakes and reports all failures in one pass;
            # --run-ignored all picks up the #[ignore] functional tests.
            base = [
                "cargo", "nextest", "run",
                "--package", "zradar-functional-tests",
                "--test", "functional_tests",
                "--profile", "ci", "--run-ignored", "all",
            ]
            if self.args.list:
                cargo_test_args = [
                    "cargo", "nextest", "list",
                    "--package", "zradar-functional-tests",
                    "--test", "functional_tests", "--run-ignored", "all",
                ]
            elif self.args.filter:
                # --no-capture streams output and runs serially (one test).
                cargo_test_args = base + [self.args.filter, "--no-capture"]
            else:
                cargo_test_args = base + ["--test-threads", threads]
        else:
            # Fallback: stock cargo test when cargo-nextest is not installed.
            cargo_test_args = ["cargo", "test", "--package", "zradar-functional-tests", "--test", "functional_tests"]
            if self.args.list:
                cargo_test_args.extend(["--", "--include-ignored", "--list"])
            elif self.args.filter:
                cargo_test_args.extend([self.args.filter, "--", "--include-ignored", "--nocapture", "--test-threads=1"])
            else:
                cargo_test_args.extend(["--", "--include-ignored", "--nocapture", f"--test-threads={threads}"])

        # Run test process
        try:
            res = subprocess.run(cargo_test_args, env=test_env)
            test_result = res.returncode
        except Exception as e:
            err(f"Failed to run cargo test: {e}")
            test_result = 1

        print(f"\n{BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━{NC}")
        if test_result == 0:
            ok("All functional tests passed")
        else:
            err("Some tests failed")

        self.teardown(test_result)

    def teardown(self, exit_code):
        print(f"\n{YELLOW}🧹 Cleaning up...{NC}")
        
        if self.server_proc:
            try:
                if os.name == 'nt':
                    self.server_proc.terminate()
                else:
                    os.kill(self.server_proc.pid, signal.SIGTERM)
                self.server_proc.wait(timeout=5)
                info(f"Stopped zradar server (pid {self.server_proc.pid})")
            except Exception:
                try:
                    self.server_proc.kill()
                except Exception:
                    pass
        
        # Kill any stray servers
        if os.name != 'nt':
            try:
                subprocess.run(["pkill", "-f", "target/release/zradar"], capture_output=True)
            except Exception:
                pass
                
        if self.args.reuse:
            warn(f"Container left running (reuse mode). To destroy: {self.ctr} rm -f {self.pg_name}")
        else:
            info("Removing test container...")
            self.stop_postgres()
            shutil.rmtree("./data-test", ignore_errors=True)
            ok("Container removed")

        self.restore_config()
        
        if exit_code == 0:
            ok("Cleanup complete")
        else:
            err(f"Tests failed (exit code: {exit_code})")
            
        sys.exit(exit_code)

def main():
    # Ensure we run from the repository root directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    os.chdir(root_dir)

    parser = argparse.ArgumentParser(description="zradar Functional Test Runner")
    parser.add_argument("-l", "--list", action="store_true", help="List all available tests")
    parser.add_argument("-r", "--reuse", action="store_true", help="Reuse a healthy test container if present; keep it running after")
    parser.add_argument("filter", nargs="?", default="", help="Optional test filter (e.g. test_create_api_key)")
    
    args = parser.parse_args()
    
    print_banner()
    if args.reuse:
        warn("Reuse mode ON — container stays up after tests")
    if args.filter:
        warn(f"Filter: {args.filter}")
        
    runner = TestRunner(args)
    try:
        runner.run_tests()
    except KeyboardInterrupt:
        warn("Interrupted by user")
        runner.teardown(1)
    except Exception as e:
        err(f"Unexpected error: {e}")
        runner.teardown(1)

if __name__ == '__main__':
    main()
