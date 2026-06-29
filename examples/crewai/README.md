# CrewAI + zradar example

Runs a two-agent crew (researcher + writer) on the topic "artificial
intelligence" using [CrewAI](https://docs.crewai.com/) and exports OTel spans
to a local [zradar](https://zradar.dev) instance.

By default the agents use `FakeListChatModel` — no API key required.

## Quickstart

```bash
# 1. Install dependencies (uv recommended)
uv pip install -e .

# 2. Run against mock LLM (no API key needed)
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
| `OPENAI_API_KEY` | _(unset)_ | Set to use `gpt-4o-mini` instead of `FakeListChatModel` |

## Spans emitted

| Span name | Parent | Key attributes |
|---|---|---|
| `crewai.crew.run` | (root) | `gen_ai.system`, `crew.agents_count`, `crew.tasks_count`, `crew.final_output_preview` |
| `crewai.agent.researcher` | `crewai.crew.run` | `agent.role`, `agent.goal` |
| `crewai.agent.writer` | `crewai.crew.run` | `agent.role`, `agent.goal` |

## Architecture

```
crewai.crew.run
├── crewai.agent.researcher   (research_task)
└── crewai.agent.writer       (write_task, consumes researcher output)
```

The two agents run sequentially. The researcher gathers facts; the writer
receives those facts in its task description and produces a summary paragraph.
