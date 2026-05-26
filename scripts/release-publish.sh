#!/usr/bin/env bash
# Bump version, commit, tag vX.Y.Z, and push branch + tag (triggers GitHub release build).
#
# Usage:
#   ./scripts/release-publish.sh patch
#   ./scripts/release-publish.sh 0.2.0       # bump to 0.2.0, or tag 0.2.0 if already current
#   DRY_RUN=1 ./scripts/release-publish.sh patch
#   SKIP_PUSH=1 ./scripts/release-publish.sh minor

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

BUMP="${BUMP:-patch}"
NEW_VERSION="${NEW_VERSION:-}"
DRY_RUN="${DRY_RUN:-0}"
SKIP_PUSH="${SKIP_PUSH:-0}"
SKIP_CHECK="${SKIP_CHECK:-0}"
REMOTE="${REMOTE:-origin}"

run() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

if [[ $# -ge 1 ]]; then
  BUMP_ARG="$1"
elif [[ -n "${NEW_VERSION}" ]]; then
  BUMP_ARG="${NEW_VERSION}"
else
  BUMP_ARG="${BUMP}"
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "error: working tree is not clean; commit or stash changes first" >&2
  exit 1
fi

current="$(tr -d '[:space:]' < VERSION)"
echo "Current version: ${current}"

resolve_new_version() {
  if [[ "${BUMP_ARG}" =~ ^(patch|minor|major)$ ]]; then
    case "${BUMP_ARG}" in
      patch) echo "${current}" | awk -F. '{printf "%d.%d.%d\n", $1, $2, $3+1}' ;;
      minor) echo "${current}" | awk -F. '{printf "%d.%d.0\n", $1, $2+1}' ;;
      major) echo "${current}" | awk -F. '{printf "%d.0.0\n", $1+1}' ;;
    esac
  else
    echo "${BUMP_ARG}"
  fi
}

if [[ "${DRY_RUN}" == "1" ]]; then
  new_version="$(resolve_new_version)"
elif [[ "${BUMP_ARG}" =~ ^(patch|minor|major)$ ]] || [[ "${BUMP_ARG}" != "${current}" ]]; then
  new_version="$(./scripts/bump-version.sh "${BUMP_ARG}")"
else
  new_version="${current}"
  echo "Version already ${new_version}; skipping bump (publish current version)"
fi

tag="v${new_version}"
commit_msg="chore(release): ${tag}"

echo "Release version: ${new_version} (tag ${tag})"

if git rev-parse "${tag}" >/dev/null 2>&1; then
  echo "error: tag ${tag} already exists locally; delete it or pick a new version" >&2
  exit 1
fi
if git ls-remote --exit-code --tags "${REMOTE}" "refs/tags/${tag}" >/dev/null 2>&1; then
  echo "error: tag ${tag} already exists on ${REMOTE}" >&2
  exit 1
fi

if [[ "${SKIP_CHECK}" != "1" ]]; then
  echo "Running cargo check..."
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[dry-run] cargo check --workspace"
  else
    cargo check --workspace
  fi
fi

version_changes=0
if ! git diff --quiet VERSION Cargo.toml 2>/dev/null; then
  version_changes=1
fi
if ! git diff --cached --quiet VERSION Cargo.toml 2>/dev/null; then
  version_changes=1
fi

if [[ "${version_changes}" == "1" ]]; then
  run git add VERSION Cargo.toml
  run git commit -m "${commit_msg}"
else
  echo "No VERSION/Cargo.toml changes; skipping version commit"
fi

run git tag -a "${tag}" -m "zradar ${tag}"

branch="$(git rev-parse --abbrev-ref HEAD)"
echo ""
echo "Release prepared locally:"
echo "  version: ${new_version}"
echo "  tag:     ${tag}"
echo "  branch:  ${branch}"

if [[ "${SKIP_PUSH}" == "1" ]]; then
  echo ""
  echo "SKIP_PUSH=1 — not pushing. When ready:"
  echo "  git push ${REMOTE} ${branch} ${tag}"
  exit 0
fi

run git push "${REMOTE}" "${branch}"
run git push "${REMOTE}" "${tag}"

echo ""
echo "Pushed ${tag}. GitHub Actions will build release binaries for this tag."
