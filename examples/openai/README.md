# OpenAI SDK — zradar Example

Shows how to instrument a raw OpenAI SDK tool-calling loop with OpenTelemetry and send traces to zradar.

Both implementations use `client.chat.completions.create()` directly (not the OpenAI Agents SDK). Each one manually manages the tool-calling loop, runs the tool, and injects the result back into the message list before the next API call.

## Quick start

**Python:**

```bash
cd python
uv run example.py
```

**TypeScript:**

```bash
cd typescript
pnpm install && pnpm start
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | _(unset)_ | Real OpenAI key; omit to run with the built-in mock |
| `ZRADAR_ENDPOINT` | `localhost:4317` | OTLP gRPC endpoint for zradar |
| `ZRADAR_API_KEY` | `zk_dev_example` | zradar ingest key sent as `x-zradar-api-key` header |

## Spans emitted

| Span | Attributes |
|------|------------|
| `openai.agent.run` | `gen_ai.system`, `gen_ai.request.model`, `agent.tool_calls`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens` |
| `openai.tool.get_weather` | `tool.name`, `tool.input` |

The `openai.agent.run` span is the parent. One `openai.tool.get_weather` child span is created for each tool call made during the loop.

## How it works

1. Build the initial `messages` array with a system prompt and user question.
2. Call `client.chat.completions.create()` with the `tools` list.
3. If `finish_reason == "tool_calls"`, run each requested tool inside a child OTel span, append the results as `role: "tool"` messages, and loop.
4. If `finish_reason == "stop"`, print the final answer and close the parent span with accumulated token/call counts.

## Mock client

When `OPENAI_API_KEY` is not set, a drop-in mock replaces the real client. The mock returns a fixed tool-call response on the first call and a final answer on the second — letting you run the example and observe the full span tree without any API credentials or network access.
