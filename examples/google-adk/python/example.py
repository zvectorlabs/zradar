"""
Google ADK Exchange Rate Agent — zradar OpenTelemetry example.

Runs against the real Gemini API when GOOGLE_API_KEY (or GOOGLE_GENAI_USE_VERTEXAI=1)
is set; otherwise falls back to InMemoryRunner which replays a scripted response
without any API call.

Usage:
    uv run agent.py
    GOOGLE_API_KEY=your-key uv run agent.py
"""

from __future__ import annotations

import asyncio
import json
import os
import time

# ---------------------------------------------------------------------------
# OpenTelemetry setup
# ---------------------------------------------------------------------------
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

ZRADAR_ENDPOINT = os.environ.get("ZRADAR_ENDPOINT", "localhost:4317")
ZRADAR_API_KEY = os.environ.get("ZRADAR_API_KEY", "zk_dev_example")

resource = Resource.create(
    {
        "service.name": "google-adk-exchange-rate-agent",
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

tracer = trace.get_tracer("zradar.examples.google_adk")

# ---------------------------------------------------------------------------
# Tool definition
# ---------------------------------------------------------------------------

# Hardcoded exchange rates for the mock; extend as needed.
_RATES: dict[tuple[str, str], float] = {
    ("USD", "EUR"): 0.92,
    ("EUR", "USD"): 1.09,
    ("USD", "GBP"): 0.79,
    ("GBP", "USD"): 1.27,
    ("USD", "JPY"): 157.40,
    ("JPY", "USD"): 0.00635,
}


def get_exchange_rate(from_currency: str, to_currency: str) -> str:
    """Return the current exchange rate between two currencies.

    Args:
        from_currency: The ISO 4217 source currency code (e.g. "USD").
        to_currency: The ISO 4217 target currency code (e.g. "EUR").

    Returns:
        A human-readable string describing the exchange rate.
    """
    with tracer.start_as_current_span(
        "google_adk.tool.get_exchange_rate"
    ) as tool_span:
        tool_span.set_attribute("tool.name", "get_exchange_rate")
        tool_span.set_attribute(
            "tool.input",
            json.dumps({"from_currency": from_currency, "to_currency": to_currency}),
        )

        from_currency = from_currency.upper().strip()
        to_currency = to_currency.upper().strip()

        if from_currency == to_currency:
            result = f"1 {from_currency} = 1 {to_currency} (same currency)"
            tool_span.set_attribute("tool.output", result)
            return result

        rate = _RATES.get((from_currency, to_currency))
        if rate is None:
            result = (
                f"Exchange rate for {from_currency}/{to_currency} is not available "
                "in this demo. Try USD/EUR, EUR/USD, USD/GBP, GBP/USD, "
                "USD/JPY, or JPY/USD."
            )
        else:
            result = f"1 {from_currency} = {rate} {to_currency}"

        tool_span.set_attribute("tool.output", result)
        return result


# ---------------------------------------------------------------------------
# Agent construction
# ---------------------------------------------------------------------------

def _build_agent():
    from google.adk.agents import Agent  # type: ignore[import-untyped]

    return Agent(
        name="exchange_rate_agent",
        model="gemini-2.0-flash",
        instruction=(
            "You are a helpful currency assistant. "
            "Use the get_exchange_rate tool to answer questions about exchange rates. "
            "Always call the tool rather than guessing the rate."
        ),
        tools=[get_exchange_rate],
    )


# ---------------------------------------------------------------------------
# Runner selection: real vs. InMemoryRunner
# ---------------------------------------------------------------------------

def _use_real_api() -> bool:
    """Return True when real Gemini credentials are present."""
    return bool(
        os.environ.get("GOOGLE_API_KEY")
        or os.environ.get("GOOGLE_GENAI_USE_VERTEXAI") == "1"
    )


async def _run_with_real_api(agent, user_message: str) -> str:
    """Run the agent against the live Gemini API."""
    from google.adk.runners import Runner  # type: ignore[import-untyped]
    from google.adk.sessions import InMemorySessionService  # type: ignore[import-untyped]
    from google.genai import types as genai_types  # type: ignore[import-untyped]

    session_service = InMemorySessionService()
    runner = Runner(
        agent=agent,
        app_name="zradar_google_adk_example",
        session_service=session_service,
    )

    session = await session_service.create_session(
        app_name="zradar_google_adk_example",
        user_id="demo_user",
    )

    content = genai_types.Content(
        role="user",
        parts=[genai_types.Part(text=user_message)],
    )

    final_response = ""
    async for event in runner.run_async(
        user_id="demo_user",
        session_id=session.id,
        new_message=content,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            final_response = "".join(
                p.text for p in event.content.parts if hasattr(p, "text") and p.text
            )

    return final_response


async def _run_with_in_memory(agent, user_message: str) -> str:
    """Run the agent using InMemoryRunner (no API key required).

    InMemoryRunner uses a scripted / local model that does not call Gemini,
    so it is safe for CI and local demos without credentials.
    """
    from google.adk.runners import InMemoryRunner  # type: ignore[import-untyped]
    from google.genai import types as genai_types  # type: ignore[import-untyped]

    runner = InMemoryRunner(agent=agent)

    session = await runner.session_service.create_session(
        app_name=runner.app_name,
        user_id="demo_user",
    )

    content = genai_types.Content(
        role="user",
        parts=[genai_types.Part(text=user_message)],
    )

    final_response = ""
    async for event in runner.run_async(
        user_id="demo_user",
        session_id=session.id,
        new_message=content,
    ):
        if event.is_final_response() and event.content and event.content.parts:
            final_response = "".join(
                p.text for p in event.content.parts if hasattr(p, "text") and p.text
            )

    # InMemoryRunner may not call the tool automatically — call it directly so
    # the tool span is always emitted in the demo.
    if not final_response:
        tool_result = get_exchange_rate("USD", "EUR")
        final_response = (
            f"[InMemoryRunner demo — tool called directly] {tool_result}"
        )

    return final_response


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    user_message = "What is the exchange rate from USD to EUR?"

    agent = _build_agent()

    with tracer.start_as_current_span("google_adk.agent.run") as agent_span:
        agent_span.set_attribute("gen_ai.system", "google-adk")
        agent_span.set_attribute("gen_ai.request.model", "gemini-2.0-flash")
        agent_span.set_attribute("agent.name", agent.name)
        agent_span.set_attribute("gen_ai.prompt", user_message)

        start = time.perf_counter()
        try:
            if _use_real_api():
                print("Using real Gemini API…")
                response = await _run_with_real_api(agent, user_message)
            else:
                print(
                    "GOOGLE_API_KEY not set — using InMemoryRunner "
                    "(no Gemini API call)."
                )
                response = await _run_with_in_memory(agent, user_message)
        except Exception as exc:
            agent_span.record_exception(exc)
            agent_span.set_status(trace.StatusCode.ERROR, str(exc))
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            agent_span.set_attribute("agent.duration_ms", round(elapsed_ms, 2))

        agent_span.set_attribute("gen_ai.completion", response)

    print(f"\nAgent response: {response}")

    # Flush spans before process exit.
    provider.force_flush()


if __name__ == "__main__":
    asyncio.run(main())
