# LangChain Agent — zradar Example

Runs a single-tool ReAct agent and sends OTel spans to a local zradar instance.
No API key needed — uses `FakeListChatModel` by default.

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
| `OPENAI_API_KEY` | *(unset)* | If set, switches to `ChatOpenAI` (gpt-4o-mini) |

## Spans emitted

| Span name | Parent | Required attributes |
|---|---|---|
| `langchain.agent.run` | *(root)* | `gen_ai.system` (string), `gen_ai.request.model` (string), `agent.tool_calls` (int) |
| `langchain.tool.get_weather` | `langchain.agent.run` | `tool.name` (string), `tool.input` (string) |

## Notes

- The `FakeListChatModel` has two pre-programmed ReAct-format turns: one tool call and one final answer.
- Span IDs are printed to stdout after `force_flush()` so you can cross-reference them in the zradar UI.
- Set `verbose=True` in `AgentExecutor` to see the full ReAct trace on stdout.
