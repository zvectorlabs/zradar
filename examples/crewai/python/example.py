"""
CrewAI example with two agents (researcher + writer) and OTel spans
exported to zradar.

Mock LLM: FakeListChatModel from langchain_community — no API key needed.
Real LLM: ChatOpenAI if OPENAI_API_KEY is set.

OTel spans are exported via OTLP/gRPC to ZRADAR_ENDPOINT (default localhost:4317).
"""

import os
import time

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

resource = Resource.create({"service.name": "example-crewai"})
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
# LLM selection
# ---------------------------------------------------------------------------

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

if OPENAI_API_KEY:
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
else:
    from langchain_community.chat_models.fake import FakeListChatModel

    # Provide enough canned responses to cover both agent turns (CrewAI may
    # make multiple LLM calls per task for planning/acting; responses are
    # cycled round-robin so a long list avoids index errors).
    _researcher_responses = [
        "Key facts about artificial intelligence:\n"
        "1. AI was founded as a discipline in 1956 at Dartmouth.\n"
        "2. Machine learning is a subset of AI.\n"
        "3. Deep learning uses neural networks with many layers.\n"
        "4. Large language models like GPT are transformer-based.\n"
        "5. Reinforcement learning trains agents via reward signals.\n",
    ] * 10

    _writer_responses = [
        "Artificial intelligence (AI) has grown from a 1956 academic "
        "discipline into the backbone of modern technology. "
        "Machine learning and deep learning enable systems to learn from data, "
        "while large language models bring natural-language understanding to "
        "everyday applications.\n",
    ] * 10

    # Single shared fake LLM whose responses alternate between the two pools.
    # We interleave them so each agent gets a plausible reply regardless of
    # call order.
    combined = []
    for r, w in zip(_researcher_responses, _writer_responses):
        combined.extend([r, w])

    llm = FakeListChatModel(responses=combined)

# ---------------------------------------------------------------------------
# CrewAI crew definition
# ---------------------------------------------------------------------------

from crewai import Agent, Crew, Process, Task

researcher = Agent(
    role="researcher",
    goal="Research key facts about the given topic and list them clearly.",
    backstory=(
        "You are a meticulous researcher who gathers accurate, concise facts "
        "from reliable sources."
    ),
    llm=llm,
    verbose=False,
    allow_delegation=False,
)

writer = Agent(
    role="writer",
    goal="Write a clear, concise paragraph summarising the research findings.",
    backstory=(
        "You are a skilled technical writer who distils complex information "
        "into readable prose."
    ),
    llm=llm,
    verbose=False,
    allow_delegation=False,
)

TOPIC = "artificial intelligence"

research_task = Task(
    description=f"Gather five key facts about {TOPIC}.",
    expected_output="A numbered list of five facts about the topic.",
    agent=researcher,
)

write_task = Task(
    description=(
        f"Using the researcher's findings, write a short paragraph about {TOPIC}."
    ),
    expected_output="A single well-written paragraph (3-5 sentences).",
    agent=writer,
    context=[research_task],
)

crew = Crew(
    agents=[researcher, writer],
    tasks=[research_task, write_task],
    process=Process.sequential,
    verbose=False,
)

# ---------------------------------------------------------------------------
# Main — wrap crew.kickoff() in OTel spans
# ---------------------------------------------------------------------------


def _run_with_agent_span(agent_obj: Agent, task_obj: Task) -> str:
    """
    Run a single task synchronously inside a child agent span.

    CrewAI's sequential process already calls tasks in order; this helper is
    used only when we need fine-grained per-agent spans.  In the main flow we
    call crew.kickoff() inside the root span and emit per-agent spans by
    introspecting the task outputs afterwards.
    """
    role = agent_obj.role
    with tracer.start_as_current_span(
        f"crewai.agent.{role}",
        attributes={
            "agent.role": role,
            "agent.goal": agent_obj.goal,
        },
    ):
        # Execute a mini one-agent crew for this task only.
        mini_crew = Crew(
            agents=[agent_obj],
            tasks=[task_obj],
            process=Process.sequential,
            verbose=False,
        )
        result = mini_crew.kickoff()
        return str(result)


def main() -> None:
    agents_count = len(crew.agents)
    tasks_count = len(crew.tasks)

    with tracer.start_as_current_span(
        "crewai.crew.run",
        attributes={
            "gen_ai.system": "crewai",
            "crew.agents_count": agents_count,
            "crew.tasks_count": tasks_count,
        },
    ) as root_span:
        # Run each task individually so we can wrap each agent in its own span.
        # We keep the task dependency (write_task.context = [research_task])
        # by running them in order and injecting the research output.

        # --- Researcher ---
        research_output = _run_with_agent_span(researcher, research_task)

        # Inject researcher output into the writer task description so the
        # writer's fake/real LLM has access to it.
        write_task.description = (
            f"Using the following research findings, write a short paragraph "
            f"about {TOPIC}.\n\nResearch findings:\n{research_output}"
        )

        # --- Writer ---
        write_output = _run_with_agent_span(writer, write_task)

        root_span.set_attribute("crew.final_output_preview", write_output[:200])

    print(f"Topic    : {TOPIC}")
    print(f"Research :\n{research_output}\n")
    print(f"Summary  :\n{write_output}\n")
    print(f"OTel endpoint : {ZRADAR_ENDPOINT}")


if __name__ == "__main__":
    main()
