# LlamaIndex ReAct Agent — zradar Example

A minimal [LlamaIndex](https://www.llamaindex.ai/) ReAct agent that sends
OpenTelemetry spans to a zradar-compatible OTLP/gRPC endpoint.

By default the example uses `MockLLM` so it works offline with no API key.
Set `OPENAI_API_KEY` to switch to a real `gpt-4o-mini` model via
`llama-index-llms-openai`.

## Quickstart

```bash
# Install dependencies (uv recommended)
uv pip install -e .

# Run the agent
python agent.py
```

Or with plain pip:

```bash
pip install llama-index-core llama-index-llms-openai \
            opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc
python agent.py
```

### Environment variables

| Variable           | Default            | Description                                         |
|--------------------|--------------------|-----------------------------------------------------|
| `ZRADAR_ENDPOINT`  | `localhost:4317`   | OTLP/gRPC endpoint (no `http://` prefix)            |
| `ZRADAR_API_KEY`   | `zk_dev_example`   | Bearer token sent in the `authorization` header     |
| `OPENAI_API_KEY`   | _(unset)_          | If set, switches from MockLLM to `gpt-4o-mini`      |

## Spans emitted

| Span name                       | Parent            | Key attributes                                             |
|---------------------------------|-------------------|------------------------------------------------------------|
| `llamaindex.agent.run`          | _(root)_          | `gen_ai.system`, `agent.type`, `agent.query`               |
| `llamaindex.tool.search_docs`   | `agent.run`       | `tool.name`, `tool.query`                                  |

## What the agent does

1. Creates a `ReActAgent` with one tool: `search_docs`.
2. Sends the query `"Find information about OpenTelemetry"`.
3. The model calls `search_docs("OpenTelemetry")`.
4. The tool returns a hardcoded description of OTel.
5. Both steps are recorded as OTel spans and exported to zradar.
