#!/usr/bin/env bash
# Bump version, commit, tag vX.Y.Z, and push branch + tag (triggers GitHub release build).
#
# Usage:
#   ./scripts/release-publish.sh patch
#   ./scripts/release-publish.sh 0.2.0
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

if [[ "${DRY_RUN}" == "1" ]]; then
  echo "[dry-run] ./scripts/bump-version.sh ${BUMP_ARG}"
  if [[ "${BUMP_ARG}" =~ ^(patch|minor|major)$ ]]; then
  case "${BUMP_ARG}" in
    patch) new_version="$(echo "${current}" | awk -F. '{printf "%d.%d.%d\n", $1, $2, $3+1}')" ;;
    minor) new_version="$(echo "${current}" | awk -F. '{printf "%d.%d.0\n", $1, $2+1}')" ;;
    major) new_version="$(echo "${current}" | awk -F. '{printf "%d.0.0\n", $1+1}')" ;;
  esac
  else
    new_version="${BUMP_ARG}"
  fi
else
  new_version="$(./scripts/bump-version.sh "${BUMP_ARG}")"
fi

tag="v${new_version}"
commit_msg="chore(release): ${tag}"

echo "New version: ${new_version} (tag ${tag})"

if [[ "${SKIP_CHECK}" != "1" ]]; then
  echo "Running cargo check..."
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "[dry-run] cargo check --workspace"
  else
    cargo check --workspace
  fi
fi

run git add VERSION Cargo.toml
run git commit -m "${commit_msg}"
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
