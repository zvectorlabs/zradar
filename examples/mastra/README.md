# Mastra — zradar Example

A minimal [Mastra](https://mastra.ai) TypeScript agent instrumented with OpenTelemetry, exporting traces to [zradar](https://zradar.dev).

The agent has one tool — `get_forecast` — that returns a hardcoded weather forecast. When `OPENAI_API_KEY` is not set the example runs in **mock mode**: it skips the LLM call and manually emits the expected OTel spans so the trace schema can be validated in CI without an API key.

## Quick start

```bash
pnpm install
pnpm start
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `ZRADAR_ENDPOINT` | `localhost:4317` | OTLP gRPC endpoint (`host:port`, no `http://` prefix) |
| `ZRADAR_API_KEY` | _(unset)_ | API key sent as `x-api-key` gRPC metadata header |
| `OPENAI_API_KEY` | _(unset)_ | When absent the example runs in mock mode |

### With a real OpenAI key

```bash
OPENAI_API_KEY=sk-... ZRADAR_ENDPOINT=ingest.zradar.dev:4317 ZRADAR_API_KEY=zr-... pnpm start
```

## OTel spans emitted

| Span name | Parent | Key attributes |
|---|---|---|
| `mastra.agent.run` | _(root)_ | `gen_ai.system=mastra`, `gen_ai.request.model`, `agent.name` |
| `mastra.tool.get_forecast` | `mastra.agent.run` | `tool.name=get_forecast`, `tool.input` (JSON) |

See [`tests/expected_spans.json`](tests/expected_spans.json) for the machine-readable schema used by zradar's span validator.
