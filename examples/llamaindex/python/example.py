"""
LlamaIndex ReAct agent example — sends OTel spans to a zradar-compatible
OTLP/gRPC endpoint.

Uses MockLLM by default (no API key required).
Set OPENAI_API_KEY to use a real OpenAI model instead.
"""

from __future__ import annotations

import json
import os
from typing import Any

# ---------------------------------------------------------------------------
# OTel setup — must happen before any instrumented code runs
# ---------------------------------------------------------------------------
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import SpanStatusCode

ENDPOINT: str = os.environ.get("ZRADAR_ENDPOINT", "localhost:4317")
API_KEY: str = os.environ.get("ZRADAR_API_KEY", "zk_dev_example")

resource = Resource.create({"service.name": "example-llamaindex"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(
    endpoint=ENDPOINT,
    insecure=True,
    headers=(("authorization", f"Bearer {API_KEY}"),),
)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("llamaindex-example", "0.1.0")

# ---------------------------------------------------------------------------
# LLM selection
# ---------------------------------------------------------------------------
def _build_llm() -> Any:
    if os.environ.get("OPENAI_API_KEY"):
        from llama_index.llms.openai import OpenAI  # type: ignore[import]
        return OpenAI(model="gpt-4o-mini")

    from llama_index.llms.mock import MockLLM  # type: ignore[import]
    return MockLLM(max_tokens=64)


# ---------------------------------------------------------------------------
# Tool
# ---------------------------------------------------------------------------
_TOOL_TRACER_SPAN: dict[str, Any] = {}  # thread-local-ish holder for the active span


def search_docs(query: str) -> str:
    """Search the documentation for the given query and return relevant text."""
    # Instrument the tool execution with a child span.
    # The parent context is inherited automatically because we started the
    # agent span as an active span above in run_agent().
    with tracer.start_as_current_span("llamaindex.tool.search_docs") as span:
        span.set_attribute("tool.name", "search_docs")
        span.set_attribute("tool.query", query)
        # Hardcoded response for the mock example.
        result = (
            "OpenTelemetry (OTel) is a CNCF observability framework that provides "
            "vendor-neutral APIs, SDKs, and tooling for traces, metrics, and logs."
        )
        span.set_status(SpanStatusCode.OK)
    return result


# ---------------------------------------------------------------------------
# Agent run
# ---------------------------------------------------------------------------
def run_agent() -> str:
    from llama_index.core.tools import FunctionTool  # type: ignore[import]
    from llama_index.core.agent import ReActAgent  # type: ignore[import]

    llm = _build_llm()
    search_tool = FunctionTool.from_defaults(fn=search_docs)
    agent = ReActAgent.from_tools([search_tool], llm=llm, verbose=True)

    query = "Find information about OpenTelemetry"

    with tracer.start_as_current_span("llamaindex.agent.run") as agent_span:
        agent_span.set_attribute("gen_ai.system", "llamaindex")
        agent_span.set_attribute("agent.type", "react")
        agent_span.set_attribute("agent.query", query)

        try:
            response = agent.chat(query)
            agent_span.set_status(SpanStatusCode.OK)
            return str(response)
        except Exception as exc:
            agent_span.set_status(SpanStatusCode.ERROR, str(exc))
            raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        answer = run_agent()
        print("\nAgent response:")
        print(answer)
    finally:
        # Flush spans before the process exits.
        provider.shutdown()
