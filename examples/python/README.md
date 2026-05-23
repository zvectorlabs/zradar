# zradar Python examples

Uses [uv](https://docs.astral.sh/uv/) for dependencies — no manual `venv` or `pip install`.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed
- zradar running (OTLP on `localhost:4317`, Admin API on `localhost:8080`)

## Run

From this directory:

```bash
export ZVRADAR_API_KEY=your-key-here

# OTLP traces
uv run send_trace.py

# Evaluation scores (REST + optional OTLP logs)
uv run send_score.py
```

`uv run` creates `.venv/` automatically on first use and installs locked dependencies.

## Optional

```bash
uv sync          # install deps without running a script
uv lock --upgrade  # refresh uv.lock after editing pyproject.toml
```
