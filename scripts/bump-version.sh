#!/usr/bin/env bash
# Bump workspace version (VERSION file + [workspace.package] in Cargo.toml).
#
# Usage:
#   ./scripts/bump-version.sh patch|minor|major
#   ./scripts/bump-version.sh 0.2.0

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="${ROOT}/VERSION"
CARGO_TOML="${ROOT}/Cargo.toml"

semver_valid() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]
}

read_version() {
  tr -d '[:space:]' < "${VERSION_FILE}"
}

write_version() {
  local new_version="$1"
  echo "${new_version}" > "${VERSION_FILE}"
  if grep -q '^version = ' "${CARGO_TOML}"; then
    sed -i.bak "s/^version = \".*\"/version = \"${new_version}\"/" "${CARGO_TOML}"
    rm -f "${CARGO_TOML}.bak"
  else
    echo "error: [workspace.package] version not found in ${CARGO_TOML}" >&2
    exit 1
  fi
}

bump_part() {
  local current="$1"
  local part="$2"
  IFS='.' read -r major minor patch <<< "${current%%-*}"
  patch="${patch%%+*}"
  case "${part}" in
    patch) patch=$((patch + 1)) ;;
    minor) minor=$((minor + 1)); patch=0 ;;
    major) major=$((major + 1)); minor=0; patch=0 ;;
    *)
      echo "error: invalid bump part '${part}' (use patch, minor, or major)" >&2
      exit 1
      ;;
  esac
  echo "${major}.${minor}.${patch}"
}

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <patch|minor|major|X.Y.Z>" >&2
  exit 1
fi

current="$(read_version)"
if ! semver_valid "${current}"; then
  echo "error: invalid current version in VERSION: ${current}" >&2
  exit 1
fi

arg="$1"
case "${arg}" in
  patch|minor|major) new_version="$(bump_part "${current}" "${arg}")" ;;
  *)
    new_version="${arg}"
    if ! semver_valid "${new_version}"; then
      echo "error: '${new_version}' is not valid semver (X.Y.Z)" >&2
      exit 1
    fi
    ;;
esac

if [[ "${new_version}" == "${current}" ]]; then
  echo "error: new version ${new_version} equals current version" >&2
  exit 1
fi

write_version "${new_version}"
echo "${new_version}"
