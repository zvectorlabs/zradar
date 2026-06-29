# Quickstart

Get zradar running locally and see your first AI agent traces in under 5 minutes.

**Prerequisites:** Docker + Docker Compose (or [Rancher Desktop](https://rancherdesktop.io/)).

---

## 1. Clone and start

```bash
git clone https://github.com/zvectorlabs/zradar.git
cd zradar
docker compose up -d
```

This starts two services:

| Service | What it does |
|---------|-------------|
| `postgres` | Control plane — file registry, settings, retention |
| `zradar` | OTLP ingestion (`:4317`) + Admin API (`:8081`) |

Wait for zradar to be healthy (first start compiles the binary, ~2 min):

```bash
curl http://localhost:8081/health
# {"status":"ok"}
```

---

## 2. Your API key

The default dev key is pre-configured in `config.server.toml`:

```
zk_dev_local
```

Every request to the Admin API requires this key as a Bearer token. Every OTLP export uses it as the `Authorization` header.

---

## 3. Send your first trace

### Option A — run a framework example

```bash
# Python (LangChain, OpenAI Agents, PydanticAI, CrewAI, LlamaIndex, Anthropic,
#          Google ADK, OpenAI SDK)
just example langchain

# TypeScript (Vercel AI SDK, Anthropic, Google ADK, OpenAI SDK, Mastra)
just example vercel-ai-sdk
```

Each example runs a mock agent (no API key required) and exports spans to your local zradar.

### Option B — send a span with grpcurl

```bash
# Install: brew install grpcurl  (or go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest)
grpcurl \
  -H "Authorization: Bearer zk_dev_local" \
  -d '{
    "resource_spans": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"string_value": "my-agent"}}]
      },
      "scope_spans": [{
        "spans": [{
          "trace_id": "AAAAAAAAAAAAAAAAAAAAAA==",
          "span_id": "AAAAAAAAAAA=",
          "name": "agent.run",
          "start_time_unix_nano": "1700000000000000000",
          "end_time_unix_nano":   "1700000001000000000",
          "kind": 2
        }]
      }]
    }]
  }' \
  -plaintext localhost:4317 \
  opentelemetry.proto.collector.trace.v1.TraceService/Export
```

### Option C — send via the OTLP HTTP endpoint

```bash
# OTLP/HTTP is also available on :4318
# Most OpenTelemetry SDKs support OTLP/HTTP out of the box.
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer zk_dev_local" \
python my_agent.py
```

---

## 4. Query your data

### List recent spans

```bash
curl -s \
  -H "Authorization: Bearer zk_dev_local" \
  "http://localhost:8081/api/v1/spans?limit=10" | jq .
```

### List traces

```bash
curl -s \
  -H "Authorization: Bearer zk_dev_local" \
  "http://localhost:8081/api/v1/traces?limit=10" | jq .
```

### LLM analytics (token usage, cost, model breakdown)

```bash
curl -s \
  -H "Authorization: Bearer zk_dev_local" \
  "http://localhost:8081/api/v1/analytics/llm" | jq .
```

### Explore with Swagger UI

Open **http://localhost:8081/swagger-ui/** to browse all endpoints interactively. Click "Authorize" and enter `Bearer zk_dev_local`.

---

## 5. Configure for your project

Copy the example config and edit:

```bash
cp config.toml.example config.toml
```

Key fields:

```toml
[[api_keys]]
key        = "zk_live_yourkey"     # replace with a secure random key
tenant_id  = "your-org"
project_id = "your-project"
name       = "production"
```

Set `DATABASE_URL` in your environment (or a `.env` file) if you are not using the Docker Compose default:

```bash
DATABASE_URL=postgres://zradar:dev_password@localhost:5432/zradar
```

---

## 6. Instrument your agent

Point your existing OpenTelemetry setup at zradar:

```bash
# gRPC (recommended)
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer zk_dev_local"

# HTTP
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer zk_dev_local"
```

If you are starting from scratch, pick your framework from the `examples/` directory — each one has a README, a working example, and a `pyproject.toml` / `package.json` ready to go.

| Framework | Language | Directory |
|-----------|----------|-----------|
| LangChain | Python | [`examples/langchain/python/`](../examples/langchain/python/) |
| OpenAI Agents SDK | Python | [`examples/openai-agents/python/`](../examples/openai-agents/python/) |
| PydanticAI | Python | [`examples/pydantic-ai/python/`](../examples/pydantic-ai/python/) |
| CrewAI | Python | [`examples/crewai/python/`](../examples/crewai/python/) |
| LlamaIndex | Python | [`examples/llamaindex/python/`](../examples/llamaindex/python/) |
| Anthropic | Python + TypeScript | [`examples/anthropic/`](../examples/anthropic/) |
| Google ADK | Python + TypeScript | [`examples/google-adk/`](../examples/google-adk/) |
| OpenAI SDK | Python + TypeScript | [`examples/openai/`](../examples/openai/) |
| Vercel AI SDK | TypeScript | [`examples/vercel-ai-sdk/typescript/`](../examples/vercel-ai-sdk/typescript/) |
| Mastra | TypeScript | [`examples/mastra/typescript/`](../examples/mastra/typescript/) |
| NVIDIA NeMo / NIM | YAML (OTel Collector) | [`examples/nemo/`](../examples/nemo/) · [Integration guide](INTEGRATIONS/NEMO.md) |

---

## Stopping and cleaning up

```bash
docker compose down          # stop containers, keep data in ./data/
docker compose down -v       # stop and remove volumes (wipes postgres)
```

> **Note:** trace Parquet files live in `./data/trace-batches/` on your host and survive `docker compose down`. Delete that directory if you want a clean slate.

---

## What's next

- [Architecture Guide](001_ARCHITECTURE_GUIDE.md) — how zradar works internally
- [ROADMAP.md](../ROADMAP.md) — what's coming (MCP, UI, Kubernetes workflows)
- [CONTRIBUTING.md](../CONTRIBUTING.md) — how to add a framework integration or propose a change
