# Vercel AI SDK — zradar Example

A minimal agent built with the [Vercel AI SDK](https://sdk.vercel.ai/) that sends
OpenTelemetry spans to a zradar-compatible OTLP/gRPC endpoint.

By default the example uses `MockLanguageModelV1` so it works offline with no API key.
Set `OPENAI_API_KEY` to switch to a real `gpt-4o-mini` model.

## Quickstart

```bash
pnpm install
pnpm start
```

### Environment variables

| Variable           | Default            | Description                                  |
|--------------------|--------------------|----------------------------------------------|
| `ZRADAR_ENDPOINT`  | `localhost:4317`   | OTLP/gRPC endpoint (no `http://` prefix)     |
| `ZRADAR_API_KEY`   | `zk_dev_example`   | Bearer token sent in the `authorization` header |
| `OPENAI_API_KEY`   | _(unset)_          | If set, switches from mock to `gpt-4o-mini`  |

## Spans emitted

| Span name                 | Parent            | Key attributes                                                                        |
|---------------------------|-------------------|---------------------------------------------------------------------------------------|
| `vercel_ai.agent.run`     | _(root)_          | `gen_ai.system`, `gen_ai.request.model`, `agent.tool_calls`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens` |
| `vercel_ai.tool.get_time` | `agent.run`       | `tool.name`, `tool.args`                                                              |

## What the agent does

1. Asks "What time is it right now in UTC?"
2. The model calls the `get_time(timezone="UTC")` tool.
3. The tool returns a hardcoded time string.
4. The model produces a final text answer.
5. Both steps are recorded as OTel spans and exported to zradar.
