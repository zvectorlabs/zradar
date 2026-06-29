# Google ADK — zradar Example

Shows how to instrument a [Google Agent Development Kit](https://google.github.io/adk-docs/) agent with OpenTelemetry and send traces to zradar.

## Quick start

**Python:**

```bash
cd python
# Optional: export GOOGLE_API_KEY=your-key for real Gemini calls
uv run example.py
```

**TypeScript:**

```bash
cd typescript
pnpm install && pnpm start
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `ZRADAR_ENDPOINT` | `localhost:4317` | OTLP gRPC receiver (insecure) |
| `ZRADAR_API_KEY` | `zk_dev_example` | API key sent as `x-zradar-api-key` header |
| `GOOGLE_API_KEY` | _(unset)_ | Gemini API key; if absent the agent runs in mock/simulation mode |
| `GOOGLE_GENAI_USE_VERTEXAI` | _(unset)_ | Set to `1` to use Vertex AI instead of Gemini Developer API (Python only) |

## Spans emitted

| Span | Attributes |
|---|---|
| `google_adk.agent.run` | `gen_ai.system`, `gen_ai.request.model`, `agent.name` |
| `google_adk.tool.get_exchange_rate` | `tool.name`, `tool.input` |

## Mock / no-credential behaviour

**Python** — uses `google.adk.runners.InMemoryRunner`. When the runner does not
invoke the tool automatically (scripted model behaviour), `get_exchange_rate` is
called directly so the tool span is always present.

**TypeScript** — `@google/adk` does not yet ship a stable `InMemoryRunner`.
When `GOOGLE_API_KEY` is absent the script creates the parent and child spans
manually with hardcoded data. This ensures the same OTel shape that zradar
expects (`expected_spans.json`) is emitted regardless of credentials.

## Directory layout

```
google-adk/
  python/
    agent.py          # ADK agent + manual OTel instrumentation
    pyproject.toml    # uv/hatchling project manifest
  typescript/
    example.ts          # ADK agent + manual OTel instrumentation
    package.json
    tsconfig.json
  tests/
    expected_spans.json   # span contract used by zradar integration tests
  README.md
```
