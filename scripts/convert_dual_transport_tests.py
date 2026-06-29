#!/usr/bin/env python3
"""Convert functional tests to dual_transport_test! macro where eligible."""

from __future__ import annotations

import re
from pathlib import Path

SCENARIOS = Path(__file__).resolve().parents[1] / "test_functional" / "scenarios"

SKIP_FILES = {"test_health.rs", "test_grpc_api.rs", "test_query_api.rs"}

SKIP_FUNCTIONS = {
    "test_health_endpoint_returns_ok",
    "test_health_ready_checks_dependencies",
    "test_health_live_probe",
    "test_health_endpoint_no_auth_required",
    "test_grpc_full_api_surface",
    "test_grpc_get_trace_after_ingest",
    "test_grpc_query_requires_auth",
    "test_grpc_admin_requires_auth",
    "test_trace_without_api_key_rejected",
    "test_trace_with_invalid_api_key_rejected",
    "test_logs_export_requires_api_key",
    "test_logs_export_invalid_api_key_rejected",
    "test_metrics_export_requires_api_key",
    "test_metrics_export_invalid_api_key_rejected",
    "test_query_requires_authentication",
    "test_query_rejects_invalid_api_key",
    "test_admin_mutations_create_audit_logs",
    "test_retention_run_requires_auth",
}

TEST_START = re.compile(
    r"(?P<attrs>(?:#\[[^\]]+\]\s*)+)\s*async fn (?P<name>test_[a-zA-Z0-9_]+)\(\) -> Result<\(\)> \{\n",
    re.MULTILINE,
)

SETUP_LINE = re.compile(
    r"^\s*let mut env = TestEnv::setup\(\)\.await\?;\s*\n"
    r"|^\s*let env = TestEnv::setup\(\)\.await\?;\s*\n",
    re.MULTILINE,
)


def find_matching_brace(src: str, open_idx: int) -> int:
    depth = 0
    for i in range(open_idx, len(src)):
        c = src[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i
    raise ValueError("unbalanced braces")


def should_skip_name(name: str) -> bool:
    if name in SKIP_FUNCTIONS:
        return True
    if name.endswith("_body"):
        return True
    if name.endswith("__http") or name.endswith("__grpc"):
        return True
    return False


def normalize_body(body: str) -> str:
    """Drop redundant setup so the body uses the transport-scoped env from the macro."""
    return SETUP_LINE.sub("", body, count=1)


def convert_file(path: Path) -> bool:
    if path.name in SKIP_FILES:
        return False

    text = path.read_text()
    original = text
    matches = list(TEST_START.finditer(text))
    if not matches:
        return False

    out: list[str] = []
    last = 0
    changed = False

    for m in matches:
        name = m.group("name")
        attrs = m.group("attrs")
        brace = m.end() - 2  # opening `{` of `async fn ... {`
        end = find_matching_brace(text, brace) + 1

        out.append(text[last : m.start()])

        if "#[ignore]" not in attrs or should_skip_name(name):
            out.append(text[m.start() : end])
        else:
            body = normalize_body(text[brace + 1 : end - 1])
            out.append(
                f"async fn {name}_body(mut env: TestEnv) -> Result<()> {{{body}\n}}\n\n"
                f"dual_transport_test!({name}, {name}_body);\n"
            )
            changed = True

        last = end

    out.append(text[last:])
    new_text = "".join(out)
    if changed and new_text != original:
        path.write_text(new_text)
        print(f"updated {path.name}")
        return True
    return False


def main() -> None:
    updated = 0
    for path in sorted(SCENARIOS.glob("test_*.rs")):
        if convert_file(path):
            updated += 1
    print(f"done: {updated} files updated")


if __name__ == "__main__":
    main()
