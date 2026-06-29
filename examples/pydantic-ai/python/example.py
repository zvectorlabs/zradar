"""
Pydantic-AI example agent with OTel spans exported to zradar.

Answers "What's the capital of France?" using:
  - TestModel (default, no API key needed) — deterministic mock responses
  - OpenAIModel if OPENAI_API_KEY is set in the environment

OTel spans are exported via OTLP/gRPC to ZRADAR_ENDPOINT (default localhost:4317).
"""

import asyncio
import json
import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# ---------------------------------------------------------------------------
# OTel setup
# ---------------------------------------------------------------------------

ZRADAR_ENDPOINT: str = os.environ.get("ZRADAR_ENDPOINT", "localhost:4317")
ZRADAR_API_KEY: str = os.environ.get("ZRADAR_API_KEY", "zk_dev_example")

resource = Resource.create({"service.name": "example-pydantic-ai"})
provider = TracerProvider(resource=resource)
exporter = OTLPSpanExporter(
    endpoint=ZRADAR_ENDPOINT,
    insecure=True,
    headers=(("authorization", f"Bearer {ZRADAR_API_KEY}"),),
)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

# ---------------------------------------------------------------------------
# Pydantic-AI agent definition
# ---------------------------------------------------------------------------

from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if OPENAI_API_KEY:
    from pydantic_ai.models.openai import OpenAIModel

    model = OpenAIModel("gpt-4o-mini")
    model_name = "gpt-4o-mini"
else:
    # TestModel returns a canned response that references the tool schema.
    # custom_result_text lets us pin a predictable answer string.
    model = TestModel(custom_result_text="The capital of France is Paris.")
    model_name = "test-model"

agent: Agent[None, str] = Agent(
    model,
    system_prompt=(
        "You are a helpful geography assistant. "
        "Use the lookup_fact tool when you need a verified fact."
    ),
)


@agent.tool_plain
def lookup_fact(topic: str) -> str:
    """Return a hardcoded fact about *topic*."""
    facts: dict[str, str] = {
        "france": "France is a country in Western Europe. Its capital is Paris.",
        "paris": "Paris is the capital and largest city of France.",
    }
    key = topic.lower().strip()
    return facts.get(key, f"No fact found for topic: {topic!r}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

QUESTION = "What's the capital of France?"


async def main() -> None:
    with tracer.start_as_current_span(
        "pydantic_ai.agent.run",
        attributes={
            "gen_ai.system": "pydantic-ai",
            "agent.model": model_name,
            "agent.result_type": "str",
        },
    ) as root_span:
        result = await agent.run(QUESTION)

        # Emit a child span for each tool call recorded in the result messages.
        # Pydantic-AI stores tool-call/return pairs in result.all_messages().
        from pydantic_ai.messages import ModelRequest, ToolReturnPart

        for msg in result.all_messages():
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        with tracer.start_as_current_span(
                            f"pydantic_ai.tool.{part.tool_name}",
                            attributes={
                                "tool.name": part.tool_name,
                                # content is the return value (str); input is
                                # not in ToolReturnPart, so we store the content.
                                "tool.input": str(part.content),
                            },
                        ):
                            pass  # span captures the snapshot; work already done

        answer = result.data
        root_span.set_attribute("agent.answer", answer)
        print(f"Question : {QUESTION}")
        print(f"Answer   : {answer}")
        print(f"OTel endpoint : {ZRADAR_ENDPOINT}")


if __name__ == "__main__":
    asyncio.run(main())
