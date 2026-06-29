# OpenAI Agents SDK — zradar Example

Runs a `StockAdvisor` agent with a `check_stock` tool and sends OTel spans to a
local zradar instance.  No API key needed — uses a mock model by default.

## Quickstart

```bash
# 1. Install dependencies with uv
uv sync

# 2. Run (mock LLM, no API key required)
uv run example.py

# 3. Optional: run with a real OpenAI model
OPENAI_API_KEY=sk-... uv run example.py
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `ZRADAR_ENDPOINT` | `localhost:4317` | gRPC OTLP endpoint |
| `ZRADAR_API_KEY` | `zk_dev_example` | Bearer token for zradar auth |
| `OPENAI_API_KEY` | *(unset)* | If set, routes to the real OpenAI API |

## Spans emitted

| Span name | Parent | Required attributes |
|---|---|---|
| `openai_agents.agent.run` | *(root)* | `gen_ai.system` (string), `agent.name` (string), `agent.output` (string) |
| `openai_agents.tool.check_stock` | `openai_agents.agent.run` | `tool.name` (string), `tool.input.ticker` (string) |

## Mock model priority

1. **`agents.testing.FakeModel`** — pre-programmed with one tool-call turn and one
   text-answer turn; requires `openai >= 1.50` for response type imports.
2. **Manual simulation** — emits the identical span structure without invoking the
   SDK runner; used automatically if `FakeModel` or its response types are
   unavailable in the installed SDK version.
