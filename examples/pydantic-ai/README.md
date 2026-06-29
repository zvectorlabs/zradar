# Pydantic-AI + zradar example

Runs a single-agent Q&A task ("What's the capital of France?") using
[pydantic-ai](https://ai.pydantic.dev/) and exports OTel spans to a local
[zradar](https://zradar.dev) instance.

By default the agent uses `TestModel` — no API key required.

## Quickstart

```bash
# 1. Install dependencies (uv recommended)
uv pip install -e .

# 2. Run against mock model (no API key needed)
python agent.py

# 3. (Optional) Run against OpenAI
OPENAI_API_KEY=sk-... python agent.py

# 4. Point at your zradar instance
ZRADAR_ENDPOINT=my-zradar.example.com:4317 \
ZRADAR_API_KEY=zk_prod_xxx \
python agent.py
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `ZRADAR_ENDPOINT` | `localhost:4317` | OTLP/gRPC endpoint for zradar |
| `ZRADAR_API_KEY` | `zk_dev_example` | Bearer token sent in `Authorization` header |
| `OPENAI_API_KEY` | _(unset)_ | Set to use `gpt-4o-mini` instead of `TestModel` |

## Spans emitted

| Span name | Parent | Key attributes |
|---|---|---|
| `pydantic_ai.agent.run` | (root) | `gen_ai.system`, `agent.model`, `agent.result_type`, `agent.answer` |
| `pydantic_ai.tool.lookup_fact` | `pydantic_ai.agent.run` | `tool.name`, `tool.input` |

The `pydantic_ai.tool.*` child span is only emitted when the model actually
calls the tool. With `TestModel` the tool call is triggered only if the model
text mentions the tool; for reliable span generation, the example forces the
question through the tool path in the mock.
