#!/usr/bin/env python3
"""
OpenAI Agents SDK example instrumented for zradar.

Uses a mock model (no API key required) or a real OpenAI model when
OPENAI_API_KEY is set.

Mock priority:
  1. agents.testing.FakeModel   — SDK-native, pre-programmed responses
  2. Manual simulation          — bypasses Runner, emits identical spans

Spans emitted to ZRADAR_ENDPOINT (default localhost:4317):
  - openai_agents.agent.run         (root)
  - openai_agents.tool.check_stock  (child)
"""

import asyncio
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# ── configuration ────────────────────────────────────────────────────────────
ZRADAR_ENDPOINT = os.getenv("ZRADAR_ENDPOINT", "localhost:4317")
ZRADAR_API_KEY  = os.getenv("ZRADAR_API_KEY",  "zk_dev_example")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY",  "")

AGENT_NAME  = "StockAdvisor"
MOCK_PRICE  = "182.50"
MOCK_TICKER = "AAPL"

# ── OTel setup ────────────────────────────────────────────────────────────────
def setup_telemetry() -> trace.Tracer:
    """Configure the OTLP exporter and return a module tracer."""
    resource = Resource.create({
        "service.name": "openai-agents-example",
        "service.version": "0.1.0",
        "deployment.environment": "development",
    })
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint=ZRADAR_ENDPOINT,
        insecure=True,  # use insecure=False with TLS in production
        headers=(("authorization", f"Bearer {ZRADAR_API_KEY}"),),
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return trace.get_tracer(__name__)


# ── mock model helper ─────────────────────────────────────────────────────────
def _try_build_fake_model():
    """
    Attempt to build an SDK FakeModel pre-loaded with two turns:
      Turn 1 — tool call: check_stock(ticker="AAPL")
      Turn 2 — final text answer

    Returns the configured model object, or None if unavailable.
    """
    # Try both the public and private import paths across SDK versions.
    FakeModel = None
    for module_path in ("agents.testing", "agents._testing"):
        try:
            import importlib
            mod = importlib.import_module(module_path)
            FakeModel = mod.FakeModel
            break
        except (ImportError, AttributeError):
            continue

    if FakeModel is None:
        return None

    try:
        # openai >= 1.50 exposes these under openai.types.responses
        from openai.types.responses import (  # type: ignore[import]
            ResponseFunctionToolCall,
            ResponseOutputMessage,
            ResponseOutputText,
        )

        model = FakeModel()
        model.add_multiple_turn_outputs([
            # Turn 1: instruct the SDK to execute check_stock
            [ResponseFunctionToolCall(
                id="call_001",
                call_id="call_001",
                type="function_call",
                name="check_stock",
                arguments=f'{{"ticker": "{MOCK_TICKER}"}}',
            )],
            # Turn 2: final text reply
            [ResponseOutputMessage(
                id="msg_001",
                type="message",
                role="assistant",
                status="completed",
                content=[ResponseOutputText(
                    type="output_text",
                    text=f"The current price of {MOCK_TICKER} is ${MOCK_PRICE}.",
                    annotations=[],
                )],
            )],
        ])
        return model
    except Exception:
        # Types unavailable in this SDK/openai version — give up gracefully.
        return None


# ── manual simulation (fallback when SDK mock is unavailable) ─────────────────
def _simulate_agent_run(tracer: trace.Tracer) -> str:
    """
    Emit the same span structure that the real Runner would produce,
    without actually invoking the OpenAI API or the agents SDK runner.
    """
    with tracer.start_as_current_span("openai_agents.tool.check_stock") as ts:
        ts.set_attribute("tool.name", "check_stock")
        ts.set_attribute("tool.input.ticker", MOCK_TICKER)
        price_str = f"${MOCK_PRICE}"
    return f"The current price of {MOCK_TICKER} is {price_str}."


# ── agent runner ──────────────────────────────────────────────────────────────
async def run_agent(tracer: trace.Tracer) -> tuple[str, str]:
    """
    Execute the agent (real, SDK-fake, or manual simulation) wrapped in an
    OTel parent span.  Returns (trace_id_hex, span_id_hex).
    """
    from agents import Agent, Runner, function_tool  # type: ignore[import]

    # Define the tool here so its closure captures `tracer`.
    @function_tool
    def check_stock(ticker: str) -> str:
        """Return the current stock price for a ticker symbol."""
        with tracer.start_as_current_span("openai_agents.tool.check_stock") as ts:
            ts.set_attribute("tool.name", "check_stock")
            ts.set_attribute("tool.input.ticker", ticker)
            return f"The current price of {ticker} is ${MOCK_PRICE}."

    with tracer.start_as_current_span("openai_agents.agent.run") as root:
        root.set_attribute("gen_ai.system", "openai-agents")
        root.set_attribute("agent.name", AGENT_NAME)

        if OPENAI_API_KEY:
            # ── real API path ────────────────────────────────────────────────
            agent = Agent(
                name=AGENT_NAME,
                instructions="You are a helpful stock market assistant.",
                tools=[check_stock],
            )
            result = await Runner.run(agent, f"What is the current price of {MOCK_TICKER}?")
            root.set_attribute("agent.output", str(result.final_output))

        else:
            fake_model = _try_build_fake_model()

            if fake_model is not None:
                # ── SDK path with FakeModel ──────────────────────────────────
                agent = Agent(
                    name=AGENT_NAME,
                    instructions="You are a helpful stock market assistant.",
                    model=fake_model,
                    tools=[check_stock],
                )
                result = await Runner.run(agent, f"What is the current price of {MOCK_TICKER}?")
                root.set_attribute("agent.output", str(result.final_output))

            else:
                # ── manual simulation (no SDK runner) ────────────────────────
                output = _simulate_agent_run(tracer)
                root.set_attribute("agent.output", output)

        sctx = root.get_span_context()
        return (
            format(sctx.trace_id, "032x"),
            format(sctx.span_id,  "016x"),
        )


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    tracer = setup_telemetry()

    # Optionally enable the SDK's built-in tracing export (no-op if absent).
    try:
        import agents as _agents_mod  # type: ignore[import]
        if hasattr(_agents_mod, "set_tracing_export_enabled"):
            _agents_mod.set_tracing_export_enabled(True)
    except ImportError:
        pass

    trace_id, span_id = asyncio.run(run_agent(tracer))

    # Flush all buffered spans before the process exits.
    trace.get_tracer_provider().force_flush()

    print(f"trace_id = {trace_id}")
    print(f"span_id  = {span_id}")


if __name__ == "__main__":
    main()
