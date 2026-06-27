# NeMo Inference Microservices (NIM) — zradar Integration

This guide covers ingesting NIM and GPU metrics into zradar via the OpenTelemetry
Collector pattern (Phase 3, R3.1–R3.3).

> **Architecture choice.** zradar does **not** ship a built-in Prometheus
> scraper. NIM exposes Prometheus-format `/v1/metrics`; we use the upstream
> OTel Collector to translate Prometheus → OTLP and push to zradar's OTLP/HTTP
> receiver (`:4318`). See `TECH-SPEC-PHASE-3.md` §3 for the decision rationale.

---

## Validated against

| Component | Version |
|-----------|---------|
| NVIDIA NIM LLM | 1.x |
| vLLM (inside NIM) | 0.6+ |
| DCGM-Exporter | 3.x |
| OpenTelemetry Collector (contrib) | v0.95+ |
| zradar | this repo, Phase 1 OTLP/HTTP receiver enabled |

Per `OQ20`, a single version pin is recorded in the header of
[`examples/nemo/otel-collector-nim.yaml`](../../examples/nemo/otel-collector-nim.yaml)
and here. Pin rotation is a deliberate PR.

---

## Authentication (R4.8)

zradar's OTLP receivers (gRPC `:4317`, HTTP `:4318`) authenticate every
incoming request against a static API key passed via the `Authorization`
header. **The header value MUST be prefixed with `Bearer ` (note the
single space).** Raw API keys, `Token` prefixes, or lowercase `bearer`
all return `401 Unauthorized` — there is no defensive parsing on the
server side.

### Correct (✓)

```text
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer zk_live_abc123...
```

Or in YAML for the collector:

```yaml
exporters:
  otlphttp:
    endpoint: ${env:ZRADAR_OTLP_HTTP_URL}
    headers:
      authorization: "Bearer ${env:ZRADAR_API_KEY}"
```

### Common mistakes (✗)

| Wrong form | Why it fails |
|------------|--------------|
| `OTEL_EXPORTER_OTLP_HEADERS=authorization=zk_live_abc123` | Missing `Bearer ` prefix — server returns 401. |
| `OTEL_EXPORTER_OTLP_HEADERS=authorization=Token zk_live_abc123` | Wrong scheme — only `Bearer` is accepted. |
| `OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer zk_live_abc123` | Uppercase header name — OTel SDKs lowercase by spec, but some forwarders preserve case; safer to use lowercase. |
| `OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer  zk_live_abc123` | Two spaces between `Bearer` and the key — the server expects exactly one. |
| Mixing `OTEL_EXPORTER_OTLP_HEADERS` with `OTEL_EXPORTER_OTLP_TRACES_HEADERS` without realizing the latter overrides | Signal-scoped headers take precedence per OTel SDK contract. |

### Where the gotcha bites NeMo users

NeMo Agent Toolkit (NAT) and the NeMo Evaluator both honor
`OTEL_EXPORTER_OTLP_HEADERS`, but their docs sometimes show raw API keys
without the prefix. Always copy the exact form above. If you're seeing
401s, the fastest diagnostic is:

```bash
# Verify what the collector / SDK is actually sending.
echo "$OTEL_EXPORTER_OTLP_HEADERS"
# Should print: authorization=Bearer zk_live_...
```

The server-side audit log records the rejected request with the
`auth.scheme` extracted (or `none`), so check your zradar logs if the
client side looks correct but pushes still 401.

---

## Topology

```text
   ┌─────────────────────────┐    Prometheus scrape
   │ NIM LLM / Retriever     │───────────────┐
   │   :8000/v1/metrics      │               │
   └─────────────────────────┘               ▼
                                  ┌──────────────────────┐
   ┌─────────────────────────┐    │ OTel Collector       │   OTLP/HTTP push
   │ DCGM-Exporter           │───→│  prom receiver       │──────────────┐
   │   :9400/metrics         │    │  batch processor     │              │
   └─────────────────────────┘    │  otlphttp exporter   │              │
                                  └──────────────────────┘              ▼
                                                                ┌──────────────┐
                                                                │ zradar :4318 │
                                                                │   MetricsSvc │
                                                                │   → Parquet  │
                                                                └──────────────┘
```

- **NIM `/v1/metrics`** emits the `vllm:*` surface: TTFT, queue depth,
  KV-cache utilization, token throughput, etc. (LLM-server-level concerns.)
- **DCGM-Exporter** emits the `DCGM_FI_*` surface: GPU device-level
  utilization, memory, temperature, power.

The two surfaces are intentionally separate scrape targets — keep them
separate in dashboards too (`vllm:` panels for request-level concerns,
`DCGM_FI_` panels for hardware concerns).

---

## Deployment

### 1. Run NIM and DCGM-Exporter

Standard NVIDIA helm charts:

```bash
helm install nim-llm nvidia/nim-llm \
    --namespace nim \
    --set image.tag=1.x \
    --set service.port=8000

helm install dcgm-exporter nvidia/dcgm-exporter \
    --namespace nim \
    --set serviceMonitor.enabled=false
```

Both services expose Prometheus-format metrics — NIM at `:8000/v1/metrics`,
DCGM-Exporter at `:9400/metrics`.

### 2. Deploy the OTel Collector

Use the reference config from this repo as your starting point:
[`examples/nemo/otel-collector-nim.yaml`](../../examples/nemo/otel-collector-nim.yaml).

Set the two required environment variables on the collector:

```bash
export ZRADAR_OTLP_HTTP_URL=http://zradar.zradar.svc.cluster.local:4318
export ZRADAR_API_KEY=zk_live_<your-key-with-metrics-ingest>
```

Deploy as a `Deployment` (cluster-wide scrape) or as a `DaemonSet` (per-node
sidecar) depending on your topology. Keep the Bearer token in a Kubernetes
`Secret`, not in the YAML.

### 3. Verify on the zradar side

Within ~30 seconds (the `batch` processor's default `timeout`) you should see
rows for the `vllm:*` and `DCGM_FI_*` metric names:

```bash
curl -H "Authorization: Bearer $ZRADAR_API_KEY" \
     "$ZRADAR_API_URL/api/v1/metrics?metric_name=vllm:e2e_request_latency_seconds&limit=10"
```

If you don't see rows, check the collector's self-telemetry on `:8888/metrics`
and the collector logs (`level: info` by default).

---

## Metric reference

Every metric arrives in zradar's `metric_name` column **byte-for-byte** as
NIM emits it. The colons in `vllm:` names are preserved end-to-end (verified
by the smoke test, AC3.8).

| NIM / DCGM name | zradar `metric_name` | `metric_type` |
|-----------------|----------------------|---------------|
| `vllm:num_requests_running` | `vllm:num_requests_running` | `GAUGE` |
| `vllm:num_requests_waiting` | `vllm:num_requests_waiting` | `GAUGE` |
| `vllm:time_to_first_token_seconds` | same | `HISTOGRAM` |
| `vllm:time_per_output_token_seconds` | same | `HISTOGRAM` |
| `vllm:e2e_request_latency_seconds` | same | `HISTOGRAM` |
| `vllm:kv_cache_usage_perc` | same | `GAUGE` |
| `vllm:prompt_tokens_total` | same | `COUNTER` |
| `vllm:generation_tokens_total` | same | `COUNTER` |
| `vllm:request_success_total` | same | `COUNTER` |
| `DCGM_FI_DEV_GPU_UTIL` | same | `GAUGE` |
| `DCGM_FI_DEV_MEM_COPY_UTIL` | same | `GAUGE` |
| `DCGM_FI_DEV_FB_USED` | same | `GAUGE` |

---

## Recommended panels

Per `OQ22`, zradar does not ship dashboard JSON in Phase 3 (no dashboard CRUD
or UI yet). The 10 panels below are the recommended starting set for an
operator-built Grafana dashboard pointed at zradar's metrics API.

| # | Panel | Source metric | Notes |
|---|-------|--------------|-------|
| 1 | **Time to first token** (p50, p95, p99) | `vllm:time_to_first_token_seconds` | First-token latency from cold queue dispatch. |
| 2 | **End-to-end request latency** (p50, p95, p99) | `vllm:e2e_request_latency_seconds` | Full request duration as seen by NIM. |
| 3 | **Queue depth** | `vllm:num_requests_waiting` | Sustained > 0 = capacity headroom warning. |
| 4 | **Active requests** | `vllm:num_requests_running` | Pair with #3 for queue-vs-active comparison. |
| 5 | **KV-cache utilization** | `vllm:kv_cache_usage_perc` | Above ~0.85 → tokens-per-output latency rises. |
| 6 | **Token throughput** | `rate(vllm:generation_tokens_total[1m])` | Production tokens per second. |
| 7 | **Success rate** | `rate(vllm:request_success_total[1m])` | Pair with HTTP 5xx rate for error budget. |
| 8 | **GPU utilization** | `DCGM_FI_DEV_GPU_UTIL` | Device-level, per `gpu` label. |
| 9 | **GPU memory utilization** | `DCGM_FI_DEV_MEM_COPY_UTIL` | Memory-copy controller busy %. |
| 10 | **GPU framebuffer used** | `DCGM_FI_DEV_FB_USED` | Absolute MiB consumed; pair with KV-cache % to spot OOM risk. |

When zradar's own dashboard CRUD lands (planned in `PRD-FULL-OBSERVABILITY.md`
Phase 4), this table is the blueprint for the shipped JSON.

---

## Trace ↔ Metric correlation

NIM emits **aggregate** `vllm:*` metrics — they are not labeled with
`trace_id`. Per audit H7, this is by design: vLLM has [partial exemplar
support](https://github.com/vllm-project/vllm/issues/4569) but it's not the
default, and NIM's bundled vLLM may or may not surface it.

What you **can** do:

- Click a slow span in the trace UI, note its `start_time` and `duration_ms`.
- Pivot to the metric view filtered by that time window:

  ```bash
  curl "$ZRADAR_API_URL/api/v1/metrics?\
metric_name=vllm:e2e_request_latency_seconds&\
start_time=<span_start>&end_time=<span_start+window>"
  ```

- Compare the population histogram to the single-trace latency — was the
  whole population slow, or just your trace?

What you **cannot** do (yet):

- "Show me NIM's metrics for trace `aaaa`." Aggregate metrics carry no
  trace ID. Wait for vLLM exemplar GA and the OTel collector's exemplar
  preservation path; that work is deferred past Phase 3.

NIM still forwards W3C `traceparent` on inference requests, so if your
backend is OTel-instrumented, backend spans will pick up the parent trace
context. That gives you trace-level visibility into NIM-internal work
without depending on exemplar plumbing.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| No metrics appear in zradar after 1 minute | Collector can't reach zradar | Check collector logs (`level: info`) for `otlphttp` errors; verify `ZRADAR_OTLP_HTTP_URL` is reachable from the collector's pod. |
| `401` on collector → zradar push | Bearer token wrong or missing capability | Verify the API key has `traces:write` / `metrics:write` scope. |
| `vllm:` metric names arrive without colons | Collector relabel config rewrote them | Remove any `metric_relabel_configs` that touch the metric name. |
| Metric type is `HISTOGRAM` but you expected `EXPONENTIALHISTOGRAM` | vLLM ≤ 0.6 emits Prometheus `histogram` (not exponential) | This is correct; zradar's converter currently collapses both shapes to `HISTOGRAM`. |
| `vllm:` metrics arrive but with no data points | NIM hasn't seen any traffic since the last `batch` flush | Send a test inference request; metrics emit on first scrape after traffic. |

---

## References

- [`TECH-SPEC-PHASE-3.md`](../../../zradar-plans/nemo-compatibility/techspec/TECH-SPEC-PHASE-3.md)
- [`examples/nemo/otel-collector-nim.yaml`](../../examples/nemo/otel-collector-nim.yaml)
- [`test_functional/scenarios/test_nim_collector.rs`](../../test_functional/scenarios/test_nim_collector.rs)
- NIM observability: https://docs.nvidia.com/nim/large-language-models/latest/reference/logging-and-observability.html
- vLLM metrics surface: https://docs.vllm.ai/en/stable/usage/metrics/
- DCGM-Exporter: https://github.com/NVIDIA/dcgm-exporter
- OpenTelemetry Collector (contrib): https://github.com/open-telemetry/opentelemetry-collector-releases
