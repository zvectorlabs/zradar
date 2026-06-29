"""
zradar example: raw OpenAI SDK tool-calling loop with manual OTel instrumentation.

Sends traces to zradar via OTLP/gRPC.

Environment variables:
  OPENAI_API_KEY   — omit to use the built-in mock client
  ZRADAR_ENDPOINT  — OTLP gRPC endpoint (default: localhost:4317)
  ZRADAR_API_KEY   — zradar ingest key   (default: zk_dev_example)
"""

from __future__ import annotations

import json
import os
import types
from typing import Any

# ---------------------------------------------------------------------------
# OpenTelemetry setup
# ---------------------------------------------------------------------------
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

ZRADAR_ENDPOINT: str = os.environ.get("ZRADAR_ENDPOINT", "localhost:4317")
ZRADAR_API_KEY: str = os.environ.get("ZRADAR_API_KEY", "zk_dev_example")

resource = Resource.create(
    {
        "service.name": "zradar-example-openai",
        "service.version": "0.1.0",
    }
)

exporter = OTLPSpanExporter(
    endpoint=ZRADAR_ENDPOINT,
    insecure=True,
    headers={"x-zradar-api-key": ZRADAR_API_KEY},
)

provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("zradar.example.openai")

# ---------------------------------------------------------------------------
# Mock OpenAI client (used when OPENAI_API_KEY is not set)
# ---------------------------------------------------------------------------

_mock_call_counter: int = 0


def _make_tool_call_response() -> Any:
    """First mock response: asks to call get_weather."""
    tool_call = types.SimpleNamespace(
        id="call_mock_001",
        type="function",
        function=types.SimpleNamespace(
            name="get_weather",
            arguments=json.dumps({"city": "London", "unit": "celsius"}),
        ),
    )
    message = types.SimpleNamespace(
        role="assistant",
        content=None,
        tool_calls=[tool_call],
    )
    choice = types.SimpleNamespace(
        index=0,
        message=message,
        finish_reason="tool_calls",
    )
    usage = types.SimpleNamespace(
        prompt_tokens=42,
        completion_tokens=18,
        total_tokens=60,
    )
    return types.SimpleNamespace(
        id="chatcmpl-mock-001",
        object="chat.completion",
        model="gpt-4o-mini",
        choices=[choice],
        usage=usage,
    )


def _make_final_response() -> Any:
    """Second mock response: final answer after tool result injected."""
    message = types.SimpleNamespace(
        role="assistant",
        content="The current weather in London is 15°C with overcast skies.",
        tool_calls=None,
    )
    choice = types.SimpleNamespace(
        index=0,
        message=message,
        finish_reason="stop",
    )
    usage = types.SimpleNamespace(
        prompt_tokens=80,
        completion_tokens=22,
        total_tokens=102,
    )
    return types.SimpleNamespace(
        id="chatcmpl-mock-002",
        object="chat.completion",
        model="gpt-4o-mini",
        choices=[choice],
        usage=usage,
    )


class _MockCompletions:
    @staticmethod
    def create(**kwargs: Any) -> Any:  # noqa: ARG004
        global _mock_call_counter
        _mock_call_counter += 1
        if _mock_call_counter == 1:
            return _make_tool_call_response()
        return _make_final_response()


class _MockChat:
    completions = _MockCompletions()


class MockOpenAIClient:
    chat = _MockChat()


# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Return the current weather for a city.",
            "parameters": {
                "type": "object",
                "properties": {
                    "city": {
                        "type": "string",
                        "description": "City name, e.g. 'London'",
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"],
                        "description": "Temperature unit.",
                    },
                },
                "required": ["city", "unit"],
            },
        },
    }
]


def get_weather(city: str, unit: str) -> str:
    """Hardcoded weather lookup used as the tool implementation."""
    data: dict[str, dict[str, str]] = {
        "london": {"celsius": "15°C, overcast", "fahrenheit": "59°F, overcast"},
        "new york": {"celsius": "22°C, sunny", "fahrenheit": "72°F, sunny"},
        "tokyo": {"celsius": "28°C, humid", "fahrenheit": "82°F, humid"},
    }
    key = city.lower()
    if key in data and unit in data[key]:
        return f"The weather in {city} is {data[key][unit]}."
    return f"Weather data unavailable for {city}."


def dispatch_tool(name: str, arguments: str) -> str:
    """Parse arguments and dispatch to the correct tool function."""
    args: dict[str, Any] = json.loads(arguments)
    if name == "get_weather":
        return get_weather(**args)
    raise ValueError(f"Unknown tool: {name}")


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

MODEL = "gpt-4o-mini"
SYSTEM_PROMPT = "You are a helpful assistant. Use the get_weather tool when asked about weather."
USER_QUESTION = "What is the weather in London in celsius?"


def run_agent() -> None:
    global _mock_call_counter
    _mock_call_counter = 0  # reset for clean test runs

    api_key = os.environ.get("OPENAI_API_KEY")
    if api_key:
        from openai import OpenAI  # type: ignore[import-untyped]

        client: Any = OpenAI(api_key=api_key)
        print("[openai] Using real OpenAI client.")
    else:
        client = MockOpenAIClient()
        print("[openai] OPENAI_API_KEY not set — using mock client.")

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_QUESTION},
    ]

    total_input_tokens = 0
    total_output_tokens = 0
    tool_call_count = 0

    with tracer.start_as_current_span("openai.agent.run") as agent_span:
        agent_span.set_attribute("gen_ai.system", "openai")
        agent_span.set_attribute("gen_ai.request.model", MODEL)

        # ----------------------------------------------------------------
        # Tool-calling loop
        # ----------------------------------------------------------------
        while True:
            response = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
            )

            choice = response.choices[0]
            assistant_message = choice.message

            # Accumulate token usage
            if hasattr(response, "usage") and response.usage:
                total_input_tokens += getattr(response.usage, "prompt_tokens", 0)
                total_output_tokens += getattr(response.usage, "completion_tokens", 0)

            if choice.finish_reason == "tool_calls":
                # Append the assistant's tool-call message
                tool_calls_payload = [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in (assistant_message.tool_calls or [])
                ]
                messages.append(
                    {
                        "role": "assistant",
                        "content": assistant_message.content,
                        "tool_calls": tool_calls_payload,
                    }
                )

                # Execute each tool call inside a child span
                for tc in assistant_message.tool_calls or []:
                    tool_call_count += 1
                    with tracer.start_as_current_span(
                        f"openai.tool.{tc.function.name}"
                    ) as tool_span:
                        tool_span.set_attribute("tool.name", tc.function.name)
                        tool_span.set_attribute("tool.input", tc.function.arguments)

                        result = dispatch_tool(tc.function.name, tc.function.arguments)
                        print(f"[tool] {tc.function.name}({tc.function.arguments}) → {result}")

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": result,
                        }
                    )

            elif choice.finish_reason == "stop":
                final_text = assistant_message.content or ""
                print(f"[agent] Final answer: {final_text}")
                break

            else:
                # Unexpected finish reason — bail out
                print(f"[agent] Unexpected finish_reason={choice.finish_reason!r}, stopping.")
                break

        # ----------------------------------------------------------------
        # Finalise parent span attributes
        # ----------------------------------------------------------------
        agent_span.set_attribute("agent.tool_calls", tool_call_count)
        agent_span.set_attribute("gen_ai.usage.input_tokens", total_input_tokens)
        agent_span.set_attribute("gen_ai.usage.output_tokens", total_output_tokens)

    # Flush pending spans before the process exits
    provider.force_flush()
    print(
        f"[otel] Exported spans to {ZRADAR_ENDPOINT} "
        f"(tool_calls={tool_call_count}, "
        f"input_tokens={total_input_tokens}, "
        f"output_tokens={total_output_tokens})"
    )


if __name__ == "__main__":
    run_agent()
