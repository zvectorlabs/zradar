#!/usr/bin/env python3
"""
LangChain ReAct agent example instrumented for zradar.

Uses FakeListChatModel (no API key required) or ChatOpenAI when
OPENAI_API_KEY is set.

Spans emitted to ZRADAR_ENDPOINT (default localhost:4317):
  - langchain.agent.run          (root)
  - langchain.tool.get_weather   (child)
"""

import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import tool

# ── configuration ────────────────────────────────────────────────────────────
ZRADAR_ENDPOINT = os.getenv("ZRADAR_ENDPOINT", "localhost:4317")
ZRADAR_API_KEY  = os.getenv("ZRADAR_API_KEY",  "zk_dev_example")
OPENAI_API_KEY  = os.getenv("OPENAI_API_KEY",  "")

MODEL_NAME = "gpt-4o-mini" if OPENAI_API_KEY else "fake-list-model"

# ── OTel setup ────────────────────────────────────────────────────────────────
def setup_telemetry() -> trace.Tracer:
    """Configure the OTLP exporter and return a module tracer."""
    resource = Resource.create({
        "service.name": "langchain-agent-example",
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


# Module-level tracer; assigned in main() before any tool invocation.
_tracer = None  # type: trace.Tracer


# ── tool ──────────────────────────────────────────────────────────────────────
@tool
def get_weather(city: str) -> str:
    """Return current weather conditions for a city."""
    with _tracer.start_as_current_span("langchain.tool.get_weather") as span:
        span.set_attribute("tool.name", "get_weather")
        span.set_attribute("tool.input", city)
        return f"The weather in {city} is sunny, 22 °C."


# ── callback ──────────────────────────────────────────────────────────────────
class ToolCallCounter(BaseCallbackHandler):
    """Counts how many times any tool is invoked during the run."""

    def __init__(self) -> None:
        super().__init__()
        self.count: int = 0

    def on_tool_start(self, serialized, input_str, **kwargs) -> None:  # noqa: ARG002
        self.count += 1


# ── LLM factory ───────────────────────────────────────────────────────────────
def build_llm():
    """Return a real or fake chat model depending on environment."""
    if OPENAI_API_KEY:
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=MODEL_NAME, temperature=0)

    from langchain_community.chat_models.fake import FakeListChatModel

    # Two pre-programmed turns in ReAct format:
    #   Turn 1 — decide to call get_weather("London")
    #   Turn 2 — emit the Final Answer
    return FakeListChatModel(
        responses=[
            (
                "Thought: I need to look up the weather for this city.\n"
                "Action: get_weather\n"
                "Action Input: London"
            ),
            (
                "Thought: I now know the final answer.\n"
                "Final Answer: The weather in London is sunny, 22 °C."
            ),
        ]
    )


# ── ReAct prompt (no hub.pull needed) ────────────────────────────────────────
REACT_TEMPLATE = """\
Answer the question as best you can using the available tools.

Tools available:
{tools}

Use EXACTLY this format:
Thought: <your reasoning>
Action: <one of [{tool_names}]>
Action Input: <tool input>
Observation: <tool output>
... (repeat Thought/Action/Action Input/Observation as needed)
Thought: I now know the final answer
Final Answer: <the answer>

Begin!

Question: {input}
{agent_scratchpad}"""


# ── main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    global _tracer
    _tracer = setup_telemetry()

    from langchain.agents import AgentExecutor, create_react_agent

    llm = build_llm()
    prompt = PromptTemplate.from_template(REACT_TEMPLATE)
    agent = create_react_agent(llm, [get_weather], prompt)
    executor = AgentExecutor(
        agent=agent,
        tools=[get_weather],
        verbose=False,
        handle_parsing_errors=True,
        max_iterations=4,
    )

    counter = ToolCallCounter()

    with _tracer.start_as_current_span("langchain.agent.run") as root:
        root.set_attribute("gen_ai.system", "langchain")
        root.set_attribute("gen_ai.request.model", MODEL_NAME)

        result = executor.invoke(
            {"input": "What is the weather in London?"},
            config={"callbacks": [counter]},
        )

        root.set_attribute("agent.tool_calls", counter.count)

        sctx = root.get_span_context()
        trace_id = format(sctx.trace_id, "032x")
        span_id  = format(sctx.span_id,  "016x")

    # Flush all buffered spans before the process exits.
    trace.get_tracer_provider().force_flush()

    print(f"trace_id = {trace_id}")
    print(f"span_id  = {span_id}")
    print(f"output   = {result.get('output', '')}")
    print(f"tool_calls_counted = {counter.count}")


if __name__ == "__main__":
    main()
