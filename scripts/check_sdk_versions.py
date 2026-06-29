#!/usr/bin/env python3
"""
check_sdk_versions.py — Check example framework dependencies for available PyPI/npm updates.

Usage:
    python3 scripts/check_sdk_versions.py
    python3 scripts/check_sdk_versions.py --format json
    python3 scripts/check_sdk_versions.py --format table --fail-on-drift
"""

import argparse
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Package manifests
# ---------------------------------------------------------------------------

FRAMEWORKS: dict[str, list[str]] = {
    "langchain": ["langchain", "langchain-community", "langchain-openai"],
    "openai-agents": ["openai-agents", "openai"],
    "openai": ["openai"],                                # openai/python/
    "pydantic-ai": ["pydantic-ai"],
    "crewai": ["crewai"],
    "llamaindex": ["llama-index-core", "llama-index-llms-openai"],
    "anthropic": ["anthropic"],                          # anthropic/python/
    "google-adk": ["google-adk"],                        # google-adk/python/
    "vercel-ai-sdk": [],  # npm packages handled via NPM_PACKAGES
}

# All frameworks use <provider>/python/pyproject.toml convention
_NESTED_PYTHON = {
    "langchain", "openai-agents", "openai", "pydantic-ai",
    "crewai", "llamaindex", "anthropic", "google-adk",
}

NPM_PACKAGES: dict[str, list[str]] = {
    "vercel-ai-sdk": ["ai", "@ai-sdk/openai"],
    "anthropic": ["@anthropic-ai/sdk"],                  # anthropic/typescript/
    "google-adk": ["@google/adk"],                       # google-adk/typescript/
    "openai": ["openai"],                                # openai/typescript/
    "mastra": ["@mastra/core"],                          # mastra/ (TS-only)
}

# ---------------------------------------------------------------------------
# Version resolution helpers
# ---------------------------------------------------------------------------

# Regex to find a pinned lower-bound in pyproject.toml, e.g.:
#   langchain>=0.2.1
#   "langchain>=0.2"
#   langchain = ">=0.2.1"
_PYPROJECT_RE = re.compile(
    r"""(?x)
    (?:^|["\s])          # start of line or quote/whitespace
    {pkg}                # literal package name (injected)
    \s*[><=!~^]+\s*      # version operator
    ([\d]+\.[\d]+        # major.minor
    (?:\.[\d]+)?)        # optional patch
    """,
    re.MULTILINE,
)

_PACKAGE_JSON_RE = re.compile(
    r'"(?P<pkg>[^"]+)"\s*:\s*"[~^]?(?P<ver>[\d]+\.[\d]+(?:\.[\d]+)?)"'
)


def _read_text(path: Path) -> str | None:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return None


def read_pinned_pypi(framework: str, package: str, repo_root: Path) -> str:
    """Read pinned version from examples/<framework>/pyproject.toml (or python/ subdir)."""
    base = repo_root / "examples" / framework
    toml_path = base / "python" / "pyproject.toml" if framework in _NESTED_PYTHON else base / "pyproject.toml"
    content = _read_text(toml_path)
    if content is None:
        return "unpinned"

    # Try tomllib (Python 3.11+) for a structured parse first.
    try:
        import tomllib  # type: ignore[import]

        data = tomllib.loads(content)
        deps: list[str] = (
            data.get("project", {}).get("dependencies", [])
            or data.get("tool", {}).get("poetry", {}).get("dependencies", {})
        )
        if isinstance(deps, list):
            for dep in deps:
                if dep.lower().startswith(package.lower()):
                    m = re.search(r"([\d]+\.[\d]+(?:\.[\d]+)?)", dep)
                    if m:
                        return m.group(1)
        elif isinstance(deps, dict):
            for dep_name, spec in deps.items():
                if dep_name.lower() == package.lower():
                    if isinstance(spec, str):
                        m = re.search(r"([\d]+\.[\d]+(?:\.[\d]+)?)", spec)
                        if m:
                            return m.group(1)
    except (ImportError, Exception):
        pass

    # Fallback: regex scan.
    pattern = re.compile(
        r"""(?:^|["'\s])"""
        + re.escape(package)
        + r"""\s*[><=!~^]+\s*([\d]+\.[\d]+(?:\.[\d]+)?)""",
        re.MULTILINE | re.IGNORECASE,
    )
    m = pattern.search(content)
    if m:
        return m.group(1)
    return "unpinned"


# All TS frameworks use <provider>/typescript/package.json convention
_NESTED_TS = {"anthropic", "google-adk", "openai", "vercel-ai-sdk", "mastra"}


def read_pinned_npm(framework: str, package: str, repo_root: Path) -> str:
    """Read pinned version from examples/<framework>/package.json (or typescript/ subdir)."""
    base = repo_root / "examples" / framework
    pkg_path = base / "typescript" / "package.json" if framework in _NESTED_TS else base / "package.json"
    content = _read_text(pkg_path)
    if content is None:
        return "unpinned"
    try:
        data = json.loads(content)
        for section in ("dependencies", "devDependencies", "peerDependencies"):
            spec = data.get(section, {}).get(package, "")
            if spec:
                m = re.search(r"([\d]+\.[\d]+(?:\.[\d]+)?)", spec)
                if m:
                    return m.group(1)
    except json.JSONDecodeError:
        pass
    return "unpinned"


def fetch_pypi_latest(package: str) -> str:
    """Fetch latest version from PyPI JSON API."""
    url = f"https://pypi.org/pypi/{package}/json"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data["info"]["version"]
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return "not-found"
        return f"error-{exc.code}"
    except Exception as exc:  # noqa: BLE001
        return f"error ({exc})"


def fetch_npm_latest(package: str) -> str:
    """Fetch latest version from the npm registry."""
    # Scoped packages like @ai-sdk/openai need URL encoding of the slash.
    encoded = package.replace("/", "%2F")
    url = f"https://registry.npmjs.org/{encoded}/latest"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return data.get("version", "unknown")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return "not-found"
        return f"error-{exc.code}"
    except Exception as exc:  # noqa: BLE001
        return f"error ({exc})"


def is_drift(pinned: str, latest: str) -> bool:
    """Return True if latest differs from pinned (or pinned is unpinned/error)."""
    if pinned in ("unpinned", "not-found") or pinned.startswith("error"):
        return False  # Can't determine drift without a baseline.
    if latest in ("not-found",) or latest.startswith("error"):
        return False  # Can't determine drift without latest.
    return pinned != latest


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def print_table(rows: list[dict]) -> None:
    col_fw = max(len(r["framework"]) for r in rows) + 2
    col_fw = max(col_fw, len("Framework") + 2)
    col_pkg = max(len(r["package"]) for r in rows) + 2
    col_pkg = max(col_pkg, len("Package") + 2)
    col_pin = max(len(r["pinned"]) for r in rows) + 2
    col_pin = max(col_pin, len("Pinned") + 2)
    col_lat = max(len(r["latest"]) for r in rows) + 2
    col_lat = max(col_lat, len("Latest") + 2)

    header = (
        f"{'Framework':<{col_fw}}"
        f"{'Package':<{col_pkg}}"
        f"{'Pinned':<{col_pin}}"
        f"{'Latest':<{col_lat}}"
        f"{'Status'}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['framework']:<{col_fw}}"
            f"{r['package']:<{col_pkg}}"
            f"{r['pinned']:<{col_pin}}"
            f"{r['latest']:<{col_lat}}"
            f"{r['status']}"
        )


def build_json_output(rows: list[dict]) -> dict:
    """Group rows by framework for JSON output."""
    out: dict[str, list[dict]] = {}
    for r in rows:
        out.setdefault(r["framework"], []).append(
            {
                "package": r["package"],
                "pinned": r["pinned"],
                "latest": r["latest"],
                "status": r["status"],
            }
        )
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check example framework dependencies for available PyPI/npm updates."
    )
    parser.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table)",
    )
    parser.add_argument(
        "--fail-on-drift",
        action="store_true",
        help="Exit 1 if any dependency drift is found",
    )
    args = parser.parse_args()

    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent

    rows: list[dict] = []

    # PyPI packages
    for framework, packages in FRAMEWORKS.items():
        for package in packages:
            pinned = read_pinned_pypi(framework, package, repo_root)
            latest = fetch_pypi_latest(package)
            status = "DRIFT" if is_drift(pinned, latest) else "OK"
            rows.append(
                {
                    "framework": framework,
                    "package": package,
                    "pinned": pinned,
                    "latest": latest,
                    "status": status,
                    "registry": "pypi",
                }
            )

    # npm packages
    for framework, packages in NPM_PACKAGES.items():
        for package in packages:
            pinned = read_pinned_npm(framework, package, repo_root)
            latest = fetch_npm_latest(package)
            status = "DRIFT" if is_drift(pinned, latest) else "OK"
            rows.append(
                {
                    "framework": framework,
                    "package": package,
                    "pinned": pinned,
                    "latest": latest,
                    "status": status,
                    "registry": "npm",
                }
            )

    if args.format == "json":
        print(json.dumps(build_json_output(rows), indent=2))
    else:
        print_table(rows)

    if args.fail_on_drift and any(r["status"] == "DRIFT" for r in rows):
        if args.format == "table":
            drift_count = sum(1 for r in rows if r["status"] == "DRIFT")
            print(f"\n{drift_count} dependency drift(s) found.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
