#!/usr/bin/env python3
import sys
import os
import subprocess
import re

def run_cmd(args, dry_run=False, env=None):
    if dry_run:
        print(f"[dry-run] {' '.join(args)}")
        return True
    
    try:
        subprocess.run(args, check=True, env=env)
        return True
    except subprocess.CalledProcessError as e:
        print(f"error: command failed: {e}", file=sys.stderr)
        return False

def check_output(args, env=None):
    try:
        res = subprocess.run(args, capture_output=True, text=True, check=True, env=env)
        return res.stdout.strip()
    except subprocess.CalledProcessError:
        return None

def is_working_tree_clean():
    # check for unstaged changes
    unstaged = subprocess.run(["git", "diff", "--quiet"], capture_output=True)
    # check for staged changes
    staged = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
    return (unstaged.returncode == 0) and (staged.returncode == 0)

def resolve_new_version(current, bump_arg):
    if bump_arg in ("patch", "minor", "major"):
        match = re.match(r'^(\d+)\.(\d+)\.(\d+)', current)
        if not match:
            print(f"error: cannot parse current version '{current}'", file=sys.stderr)
            sys.exit(1)
        major, minor, patch = map(int, match.groups())
        if bump_arg == "patch":
            patch += 1
        elif bump_arg == "minor":
            minor += 1
            patch = 0
        elif bump_arg == "major":
            major += 1
            minor = 0
            patch = 0
        return f"{major}.{minor}.{patch}"
    else:
        return bump_arg

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    os.chdir(root_dir)

    bump = os.environ.get("BUMP", "patch")
    new_version_env = os.environ.get("NEW_VERSION", "")
    dry_run = os.environ.get("DRY_RUN", "0") == "1"
    skip_push = os.environ.get("SKIP_PUSH", "0") == "1"
    skip_check = os.environ.get("SKIP_CHECK", "0") == "1"
    remote = os.environ.get("REMOTE", "origin")

    if len(sys.argv) >= 2:
        bump_arg = sys.argv[1]
    elif new_version_env:
        bump_arg = new_version_env
    else:
        bump_arg = bump

    if not is_working_tree_clean():
        print("error: working tree is not clean; commit or stash changes first", file=sys.stderr)
        sys.exit(1)

    version_file = os.path.join(root_dir, 'VERSION')
    if not os.path.exists(version_file):
        print(f"error: {version_file} not found", file=sys.stderr)
        sys.exit(1)
        
    with open(version_file, 'r', encoding='utf-8') as f:
        current = f.read().strip()

    print(f"Current version: {current}")

    if dry_run:
        new_version = resolve_new_version(current, bump_arg)
    elif bump_arg in ("patch", "minor", "major") or bump_arg != current:
        # Call bump_version.py
        py_executable = sys.executable or "python3"
        try:
            res = subprocess.run([py_executable, "scripts/bump_version.py", bump_arg], 
                                 capture_output=True, text=True, check=True)
            new_version = res.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"error: bump_version.py failed: {e.stderr}", file=sys.stderr)
            sys.exit(1)
    else:
        new_version = current
        print(f"Version already {new_version}; skipping bump (publish current version)")

    tag = f"v{new_version}"
    commit_msg = f"chore(release): {tag}"

    print(f"Release version: {new_version} (tag {tag})")

    # Check if tag already exists locally
    if check_output(["git", "rev-parse", tag]) is not None:
        print(f"error: tag {tag} already exists locally; delete it or pick a new version", file=sys.stderr)
        sys.exit(1)

    # Check if tag exists on remote
    remote_tags = check_output(["git", "ls-remote", "--exit-code", "--tags", remote, f"refs/tags/{tag}"])
    if remote_tags is not None:
        print(f"error: tag {tag} already exists on {remote}", file=sys.stderr)
        sys.exit(1)

    # Run cargo check
    if not skip_check:
        print("Running cargo check...")
        env = os.environ.copy()
        # Ensure we compile to correct target directory if specified
        cargo_args = ["cargo", "check", "--workspace"]
        if dry_run:
            print(f"[dry-run] {' '.join(cargo_args)}")
        else:
            if not run_cmd(cargo_args, env=env):
                print("error: cargo check failed", file=sys.stderr)
                sys.exit(1)

    # Detect if VERSION or Cargo.toml changed
    version_changes = False
    if dry_run:
        version_changes = (new_version != current)
    else:
        diff_status_unstaged = subprocess.run(["git", "diff", "--quiet", "VERSION", "Cargo.toml"])
        diff_status_staged = subprocess.run(["git", "diff", "--cached", "--quiet", "VERSION", "Cargo.toml"])
        if diff_status_unstaged.returncode != 0 or diff_status_staged.returncode != 0:
            version_changes = True

    if version_changes:
        if not run_cmd(["git", "add", "VERSION", "Cargo.toml"], dry_run):
            sys.exit(1)
        if not run_cmd(["git", "commit", "-m", commit_msg], dry_run):
            sys.exit(1)
    else:
        print("No VERSION/Cargo.toml changes; skipping version commit")

    if not run_cmd(["git", "tag", "-a", tag, "-m", f"zradar {tag}"], dry_run):
        sys.exit(1)

    branch = check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    print("\nRelease prepared locally:")
    print(f"  version: {new_version}")
    print(f"  tag:     {tag}")
    print(f"  branch:  {branch}")

    if skip_push:
        print(f"\nSKIP_PUSH=1 — not pushing. When ready:")
        print(f"  git push {remote} {branch} {tag}")
        sys.exit(0)

    if not run_cmd(["git", "push", remote, branch], dry_run):
        sys.exit(1)
    if not run_cmd(["git", "push", remote, tag], dry_run):
        sys.exit(1)

    print(f"\nPushed {tag}. GitHub Actions will build release binaries for this tag.")

if __name__ == '__main__':
    main()
