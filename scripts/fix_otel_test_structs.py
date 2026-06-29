#!/usr/bin/env python3
"""Add ..Default::default() to OTLP proto struct literals in test/bench code."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TARGETS = [
    ROOT / "test_functional/scenarios",
    ROOT / "crates/services/api-optel/benches",
    ROOT / "crates/services/api-optel/tests",
]

OTEL_STRUCTS = (
    "KeyValue",
    "Resource",
    "OtlpSpan",
    "Span",
    "Link",
    "Metric",
    "LogRecord",
    "OtlpLogRecord",
    "NumberDataPoint",
)


def fix_file(path: Path) -> bool:
    if "opentelemetry_proto" not in path.read_text():
        return False

    text = path.read_text()
    orig = text

    for struct in OTEL_STRUCTS:
        # Only touch struct literals in files that use OTLP types.
        pattern = rf"({struct}\s*\{{)([^{{}}]*(?:\{{[^{{}}]*\}}[^{{}}]*)*)(\n\s*\}})"
        text = re.sub(
            pattern,
            lambda m: (
                m.group(0)
                if "..Default::default()" in m.group(2) or "..Span::default()" in m.group(2)
                else f"{m.group(1)}{m.group(2)},{re.match(r'\n(\s*)\}', m.group(3)).group(1)}..Default::default(){m.group(3)}"  # type: ignore[union-attr]
            ),
            text,
        )

    text = text.replace("}),,", "}),")
    text = text.replace("))),,", "))),")

    if text != orig:
        path.write_text(text)
        return True
    return False


def main() -> None:
    changed = []
    for base in TARGETS:
        for path in base.rglob("*.rs"):
            if fix_file(path):
                changed.append(str(path.relative_to(ROOT)))
    print(f"updated {len(changed)} files")
    for p in sorted(changed):
        print(p)


if __name__ == "__main__":
    main()
