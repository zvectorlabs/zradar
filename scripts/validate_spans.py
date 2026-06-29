#!/usr/bin/env python3
"""
validate_spans.py — Validate that zradar received spans matching the expected schema.

Usage:
    python3 scripts/validate_spans.py --framework langchain
    python3 scripts/validate_spans.py --framework langchain --endpoint http://localhost:8081 \
        --api-key zk_dev_example --since-seconds 60
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# NOTE: Adjust this path prefix if zradar is deployed under a different API mount.
# The telemetry spans endpoint is assumed to be /api/v1/telemetry/spans.
# If the actual API differs (e.g. /api/v1/spans or /v1/traces), update the constant below.
SPANS_PATH = "/api/v1/telemetry/spans"

TYPE_VALIDATORS = {
    "string": str,
    "int": int,
    "float": float,
    "bool": bool,
}


def load_expected_spans(framework: str) -> dict:
    """Load the expected_spans.json for the given framework."""
    # Resolve relative to the script's repo root (two levels up from scripts/).
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent
    path = repo_root / "examples" / framework / "tests" / "expected_spans.json"
    if not path.exists():
        print(f"ERROR: expected_spans.json not found at {path}", file=sys.stderr)
        sys.exit(1)
    with path.open() as fh:
        return json.load(fh)


def fetch_spans(endpoint: str, api_key: str, limit: int = 100) -> list[dict]:
    """Fetch spans from the zradar telemetry API."""
    url = f"{endpoint.rstrip('/')}{SPANS_PATH}?limit={limit}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
    except urllib.error.HTTPError as exc:
        print(
            f"ERROR: zradar returned HTTP {exc.code} for {url}\n"
            f"  Response: {exc.read().decode(errors='replace')}",
            file=sys.stderr,
        )
        sys.exit(1)
    except urllib.error.URLError as exc:
        print(
            f"ERROR: Could not reach zradar at {url}\n"
            f"  Reason: {exc.reason}\n"
            f"  Make sure zradar is running and --endpoint is correct.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"ERROR: Could not parse JSON response from {url}: {exc}", file=sys.stderr)
        sys.exit(1)

    # Support both {"spans": [...]} and a bare list.
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "spans" in data:
        return data["spans"]

    print(
        f"ERROR: Unexpected response shape from {url}. "
        "Expected {{\"spans\": [...]}} or a JSON array.",
        file=sys.stderr,
    )
    sys.exit(1)


def filter_recent(spans: list[dict], since_seconds: int) -> list[dict]:
    """Keep only spans whose start_time_unix_nano or timestamp is within the window."""
    cutoff_ns = (time.time() - since_seconds) * 1e9
    cutoff_s = time.time() - since_seconds

    recent = []
    for span in spans:
        # Try nanosecond timestamp first, then second-resolution fields.
        ts = span.get("start_time_unix_nano") or span.get("start_time_unixnano")
        if ts is not None:
            if float(ts) >= cutoff_ns:
                recent.append(span)
            continue
        ts = span.get("timestamp") or span.get("start_time") or span.get("created_at")
        if ts is not None:
            try:
                if float(ts) >= cutoff_s:
                    recent.append(span)
            except (TypeError, ValueError):
                # Non-numeric timestamp — include conservatively.
                recent.append(span)
            continue
        # No recognisable timestamp; include conservatively.
        recent.append(span)

    return recent


def validate_span_entry(entry: dict, spans: list[dict]) -> tuple[bool, list[str]]:
    """
    Validate a single expected-span entry against the received spans.

    Returns (passed: bool, failure_messages: list[str]).
    """
    name = entry.get("name", "")
    required_attributes = entry.get("required_attributes", [])

    # Find all spans whose name matches.
    candidates = [s for s in spans if s.get("name") == name]
    if not candidates:
        return False, [f"No span found with name={name!r}"]

    # We only need ONE candidate to satisfy all required attributes.
    for span in candidates:
        attrs = span.get("attributes") or span.get("tags") or {}
        failures = []
        for req in required_attributes:
            attr_name = req.get("name") or req.get("key", "")
            expected_type = req.get("type", "")
            if attr_name not in attrs:
                failures.append(f"  attribute {attr_name!r} missing")
                continue
            validator = TYPE_VALIDATORS.get(expected_type)
            if validator is None:
                # Unknown type spec — skip type check, presence is enough.
                continue
            value = attrs[attr_name]
            if not isinstance(value, validator):
                failures.append(
                    f"  attribute {attr_name!r}: expected type {expected_type}, "
                    f"got {type(value).__name__} (value={value!r})"
                )
        if not failures:
            return True, []

    # All candidates failed; report failures from the last one checked.
    return False, failures


def print_table(results: list[tuple[str, bool, list[str]]]) -> None:
    """Print a PASS/FAIL table."""
    col_name = max(len(name) for name, _, _ in results) + 2
    col_name = max(col_name, len("Span Name") + 2)
    header = f"{'Span Name':<{col_name}}  {'Result':<8}  Details"
    print(header)
    print("-" * len(header))
    for name, passed, msgs in results:
        status = "PASS" if passed else "FAIL"
        detail = "" if passed else "; ".join(msgs)
        print(f"{name:<{col_name}}  {status:<8}  {detail}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate that zradar received spans matching the expected schema."
    )
    parser.add_argument("--framework", required=True, help="Framework name (e.g. langchain)")
    parser.add_argument(
        "--endpoint",
        default="http://localhost:8081",
        help="zradar API base URL (default: http://localhost:8081)",
    )
    parser.add_argument(
        "--api-key",
        default="zk_dev_example",
        help="Bearer API key for zradar (default: zk_dev_example)",
    )
    parser.add_argument(
        "--since-seconds",
        type=int,
        default=60,
        help="Only consider spans created in the last N seconds (default: 60)",
    )
    args = parser.parse_args()

    print(f"Loading expected spans for framework: {args.framework}")
    expected = load_expected_spans(args.framework)
    expected_entries = expected.get("spans", [])
    if not expected_entries:
        print("WARNING: expected_spans.json contains no entries — nothing to validate.")
        return 0

    print(f"Fetching spans from {args.endpoint}{SPANS_PATH} …")
    all_spans = fetch_spans(args.endpoint, args.api_key)
    recent_spans = filter_recent(all_spans, args.since_seconds)
    print(
        f"  Received {len(all_spans)} total span(s); "
        f"{len(recent_spans)} within the last {args.since_seconds}s.\n"
    )

    results: list[tuple[str, bool, list[str]]] = []
    for entry in expected_entries:
        passed, msgs = validate_span_entry(entry, recent_spans)
        results.append((entry.get("name", "<unnamed>"), passed, msgs))

    print_table(results)
    print()

    all_passed = all(passed for _, passed, _ in results)
    if all_passed:
        print("All span validations PASSED.")
        return 0
    else:
        failed = sum(1 for _, passed, _ in results if not passed)
        print(f"{failed}/{len(results)} span validation(s) FAILED.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
