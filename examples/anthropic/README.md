# Anthropic Claude SDK — zradar Example

Shows how to instrument an Anthropic tool-use agent with OpenTelemetry and send traces to zradar.

Both Python and TypeScript versions run the same two-turn agent loop:

1. User asks "What is 2 + 2?"
2. Claude responds with a `tool_use` block calling `calculator("2 + 2")`
3. The agent executes the tool and feeds the result back
4. Claude returns a final text answer

Each step is wrapped in OTel spans that are exported to zradar over OTLP/gRPC.

## Quick start

**Python (requires [uv](https://docs.astral.sh/uv/)):**

```bash
cd python
uv run example.py
```

**TypeScript (requires [pnpm](https://pnpm.io/) or npm):**

```bash
cd typescript
pnpm install && pnpm start
# or: npm install && npm start
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | *(unset)* | Real Anthropic key; if absent a mock client is used |
| `ZRADAR_ENDPOINT` | `localhost:4317` | OTLP gRPC endpoint for zradar |
| `ZRADAR_API_KEY` | `zk_dev_example` | Bearer token sent as `authorization` metadata |

## Spans emitted

| Span | Attributes |
|------|-----------|
| `anthropic.agent.run` | `gen_ai.system`, `gen_ai.request.model`, `agent.tool_calls`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens` |
| `anthropic.tool.calculator` | `tool.name`, `tool.input` |

The `anthropic.tool.*` span is always a child of `anthropic.agent.run`.

## Running against a live zradar instance

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export ZRADAR_ENDPOINT=ingest.your-zradar-host.example.com:4317
export ZRADAR_API_KEY=zk_live_...

# Python
cd python && uv run example.py

# TypeScript
cd typescript && pnpm start
```

## Validating spans

The `tests/expected_spans.json` file describes the expected span names and required attribute types. Feed it to the zradar span validator or use it as a contract for integration tests.
