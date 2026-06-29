"""
Anthropic Claude SDK — zradar minimal agent example.

Demonstrates manual OpenTelemetry instrumentation for a tool-use agent loop.
Sends traces to zradar via OTLP/gRPC.

Environment variables:
  ANTHROPIC_API_KEY   — if unset, a mock client is used (no network calls)
  ZRADAR_ENDPOINT     — OTLP gRPC host:port  (default: localhost:4317)
  ZRADAR_API_KEY      — bearer token          (default: zk_dev_example)
"""

from __future__ import annotations

import json
import os
from typing import Any

# ---------------------------------------------------------------------------
# OpenTelemetry setup
# ---------------------------------------------------------------------------
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
import grpc  # type: ignore[import]

_ZRADAR_ENDPOINT: str = os.environ.get("ZRADAR_ENDPOINT", "localhost:4317")
_ZRADAR_API_KEY: str = os.environ.get("ZRADAR_API_KEY", "zk_dev_example")
_MODEL: str = "claude-3-5-haiku-20241022"

resource = Resource.create({"service.name": "zradar-example-anthropic"})

exporter = OTLPSpanExporter(
    endpoint=_ZRADAR_ENDPOINT,
    insecure=True,
    credentials=None,
    headers=(("authorization", f"Bearer {_ZRADAR_API_KEY}"),),
)

provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("zradar.examples.anthropic")

# ---------------------------------------------------------------------------
# Mock Anthropic client (used when ANTHROPIC_API_KEY is absent)
# ---------------------------------------------------------------------------

class _FakeContentBlock:
    """Minimal stand-in for anthropic content blocks."""

    def __init__(self, block_type: str, **kwargs: Any) -> None:
        self.type = block_type
        for k, v in kwargs.items():
            setattr(self, k, v)


class _FakeMessage:
    def __init__(
        self,
        content: list[_FakeContentBlock],
        stop_reason: str,
        usage_input: int = 42,
        usage_output: int = 17,
    ) -> None:
        self.content = content
        self.stop_reason = stop_reason
        self.model = _MODEL

        class _Usage:
            input_tokens = usage_input
            output_tokens = usage_output

        self.usage = _Usage()


class _FakeMessages:
    """Simulates the two-turn conversation: tool_use → final text."""

    def __init__(self) -> None:
        self._call_count = 0

    def create(self, **kwargs: Any) -> _FakeMessage:  # noqa: ARG002
        self._call_count += 1
        if self._call_count == 1:
            # First call: model requests the calculator tool
            return _FakeMessage(
                content=[
                    _FakeContentBlock(
                        "tool_use",
                        id="tool_abc123",
                        name="calculator",
                        input={"expression": "2 + 2"},
                    )
                ],
                stop_reason="tool_use",
            )
        # Second call: model returns a final answer
        return _FakeMessage(
            content=[
                _FakeContentBlock(
                    "text",
                    text="The result of 2 + 2 is 4.",
                )
            ],
            stop_reason="end_turn",
        )


class MockAnthropicClient:
    def __init__(self) -> None:
        self.messages = _FakeMessages()


# ---------------------------------------------------------------------------
# Tool implementation
# ---------------------------------------------------------------------------

_SAFE_EXPRESSION = "2 + 2"


def calculator(expression: str) -> float:
    """Evaluate a simple arithmetic expression.

    Only the hardcoded safe expression is accepted to avoid arbitrary eval.
    """
    if expression.strip() != _SAFE_EXPRESSION:
        raise ValueError(f"Unsupported expression: {expression!r}")
    result: float = eval(_SAFE_EXPRESSION)  # noqa: S307 — controlled input only
    return result


_TOOLS = [
    {
        "name": "calculator",
        "description": "Evaluate a simple arithmetic expression and return the numeric result.",
        "input_schema": {
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "The arithmetic expression to evaluate, e.g. '2 + 2'.",
                }
            },
            "required": ["expression"],
        },
    }
]

# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

def run_agent() -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if api_key:
        import anthropic  # type: ignore[import]

        client = anthropic.Anthropic(api_key=api_key)
        print("[agent] Using real Anthropic client.")
    else:
        client = MockAnthropicClient()  # type: ignore[assignment]
        print("[agent] ANTHROPIC_API_KEY not set — using mock client.")

    messages: list[dict[str, Any]] = [
        {"role": "user", "content": "What is 2 + 2? Use the calculator tool."}
    ]

    tool_call_count = 0
    total_input_tokens = 0
    total_output_tokens = 0
    final_text = ""

    with tracer.start_as_current_span("anthropic.agent.run") as agent_span:
        agent_span.set_attribute("gen_ai.system", "anthropic")
        agent_span.set_attribute("gen_ai.request.model", _MODEL)

        # ---- agent loop ----
        while True:
            response = client.messages.create(
                model=_MODEL,
                max_tokens=1024,
                tools=_TOOLS,
                messages=messages,
            )

            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            # Collect assistant turn
            assistant_content: list[Any] = []

            for block in response.content:
                assistant_content.append(block)

                if block.type == "tool_use":
                    tool_call_count += 1
                    tool_name: str = block.name
                    tool_input: dict[str, Any] = block.input

                    with tracer.start_as_current_span(
                        f"anthropic.tool.{tool_name}"
                    ) as tool_span:
                        tool_span.set_attribute("tool.name", tool_name)
                        tool_span.set_attribute("tool.input", json.dumps(tool_input))

                        tool_result = calculator(tool_input["expression"])
                        print(f"[tool] {tool_name}({tool_input}) = {tool_result}")

                    # Append assistant turn then tool result
                    messages.append({"role": "assistant", "content": assistant_content})
                    messages.append(
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "tool_result",
                                    "tool_use_id": block.id,
                                    "content": str(tool_result),
                                }
                            ],
                        }
                    )
                    assistant_content = []  # reset for next turn

                elif block.type == "text":
                    final_text = block.text

            if response.stop_reason == "end_turn":
                if assistant_content:
                    messages.append(
                        {"role": "assistant", "content": assistant_content}
                    )
                break

        # Record aggregate attributes on the parent span
        agent_span.set_attribute("agent.tool_calls", tool_call_count)
        agent_span.set_attribute("gen_ai.usage.input_tokens", total_input_tokens)
        agent_span.set_attribute("gen_ai.usage.output_tokens", total_output_tokens)

    print(f"\n[agent] Final answer: {final_text}")
    print(
        f"[agent] Stats — tool_calls={tool_call_count}, "
        f"input_tokens={total_input_tokens}, output_tokens={total_output_tokens}"
    )

    # Flush pending spans before the process exits
    provider.force_flush()


if __name__ == "__main__":
    run_agent()
