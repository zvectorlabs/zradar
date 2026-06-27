#!/usr/bin/env python3
import sys
import re
import os

SEMVER_REGEX = re.compile(
    r'^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)'
    r'(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?'
    r'(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$'
)

def is_semver_valid(version):
    return bool(SEMVER_REGEX.match(version))

def read_version(version_file):
    if not os.path.exists(version_file):
        print(f"error: version file not found: {version_file}", file=sys.stderr)
        sys.exit(1)
    with open(version_file, 'r', encoding='utf-8') as f:
        return f.read().strip()

def write_version(version_file, cargo_toml, new_version):
    # Update VERSION file
    with open(version_file, 'w', encoding='utf-8') as f:
        f.write(new_version + '\n')
    
    # Update Cargo.toml workspace version
    if not os.path.exists(cargo_toml):
        print(f"error: Cargo.toml not found: {cargo_toml}", file=sys.stderr)
        sys.exit(1)
        
    with open(cargo_toml, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Search for version key under [workspace.package] or just global version
    pattern = r'^version\s*=\s*".*"'
    updated_content, count = re.subn(pattern, f'version = "{new_version}"', content, flags=re.M)
    if count == 0:
        print(f"error: version = \"...\" not found in {cargo_toml}", file=sys.stderr)
        sys.exit(1)
        
    with open(cargo_toml, 'w', encoding='utf-8') as f:
        f.write(updated_content)

def bump_part(current, part):
    match = SEMVER_REGEX.match(current)
    if not match:
        print(f"error: current version is invalid semver: {current}", file=sys.stderr)
        sys.exit(1)
        
    major = int(match.group('major'))
    minor = int(match.group('minor'))
    patch = int(match.group('patch'))
    
    if part == 'patch':
        patch += 1
    elif part == 'minor':
        minor += 1
        patch = 0
    elif part == 'major':
        major += 1
        minor = 0
        patch = 0
    else:
        print(f"error: invalid bump part '{part}' (use patch, minor, or major)", file=sys.stderr)
        sys.exit(1)
        
    return f"{major}.{minor}.{patch}"

def main():
    if len(sys.argv) != 2:
        print("usage: bump_version.py <patch|minor|major|X.Y.Z>", file=sys.stderr)
        sys.exit(1)
        
    arg = sys.argv[1]
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(script_dir)
    version_file = os.path.join(root_dir, 'VERSION')
    cargo_toml = os.path.join(root_dir, 'Cargo.toml')
    
    current = read_version(version_file)
    if not is_semver_valid(current):
        print(f"error: invalid current version in VERSION: {current}", file=sys.stderr)
        sys.exit(1)
        
    if arg in ('patch', 'minor', 'major'):
        new_version = bump_part(current, arg)
    else:
        new_version = arg
        if not is_semver_valid(new_version):
            print(f"error: '{new_version}' is not valid semver (X.Y.Z)", file=sys.stderr)
            sys.exit(1)
            
    if new_version == current:
        print(f"error: new version {new_version} equals current version", file=sys.stderr)
        sys.exit(1)
        
    write_version(version_file, cargo_toml, new_version)
    print(new_version)

if __name__ == '__main__':
    main()
