#!/usr/bin/env python3
"""
Example: Send OTLP traces to zradar

This demonstrates how to:
1. Configure OpenTelemetry SDK
2. Authenticate with API key
3. Send traces to zradar
4. Add custom attributes for LLM observability
"""

import time
import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Configuration
ZVRADAR_ENDPOINT = os.getenv("ZVRADAR_ENDPOINT", "localhost:4317")
API_KEY = os.getenv("ZVRADAR_API_KEY", "your-api-key-here")

def setup_telemetry():
    """Configure OpenTelemetry to send traces to zradar"""
    
    # Create resource with service information
    resource = Resource.create({
        "service.name": "example-llm-app",
        "service.version": "1.0.0",
        "deployment.environment": "development",
    })
    
    # Create tracer provider
    provider = TracerProvider(resource=resource)
    
    # Configure OTLP exporter with authentication
    exporter = OTLPSpanExporter(
        endpoint=ZVRADAR_ENDPOINT,
        insecure=True,  # Use False with TLS in production
        headers=(
            ("authorization", f"Bearer {API_KEY}"),
        ),
    )
    
    # Add batch processor
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    
    # Set as global tracer provider
    trace.set_tracer_provider(provider)
    
    print(f"✅ Telemetry configured - sending to {ZVRADAR_ENDPOINT}")
    return trace.get_tracer(__name__)


def simulate_llm_call(tracer):
    """Simulate an LLM API call with rich instrumentation"""
    
    with tracer.start_as_current_span("llm.completion") as span:
        # Standard OTLP attributes
        span.set_attribute("gen_ai.system", "openai")
        span.set_attribute("gen_ai.request.model", "gpt-4")
        span.set_attribute("gen_ai.request.temperature", 0.7)
        span.set_attribute("gen_ai.request.max_tokens", 1000)
        
        # Custom LLM-specific attributes
        span.set_attribute("gen_ai.usage.input_tokens", 45)
        span.set_attribute("gen_ai.usage.output_tokens", 123)
        span.set_attribute("gen_ai.usage.total_tokens", 168)
        
        # Cost tracking
        span.set_attribute("llm.cost.prompt_usd", 0.0014)  # $0.0014
        span.set_attribute("llm.cost.completion_usd", 0.0037)  # $0.0037
        span.set_attribute("llm.cost.total_usd", 0.0051)
        
        # Request/response metadata
        span.set_attribute("gen_ai.response.id", "resp_456def")
        span.set_attribute("gen_ai.response.finish_reasons", "stop")
        
        # Simulate processing time
        time.sleep(0.5)
        
        # Nested span for prompt processing
        with tracer.start_as_current_span("llm.prompt.format") as prompt_span:
            prompt_span.set_attribute("prompt.template", "chat_completion")
            prompt_span.set_attribute("prompt.variables", 3)
            time.sleep(0.1)
        
        # Simulate response processing
        with tracer.start_as_current_span("llm.response.parse") as response_span:
            response_span.set_attribute("response.format", "json")
            response_span.set_attribute("response.valid", True)
            time.sleep(0.05)
        
        print("✅ Simulated LLM call")


def simulate_embedding_call(tracer):
    """Simulate an embedding generation call"""
    
    with tracer.start_as_current_span("llm.embedding") as span:
        span.set_attribute("gen_ai.system", "openai")
        span.set_attribute("gen_ai.request.model", "text-embedding-ada-002")
        span.set_attribute("embedding.dimensions", 1536)
        span.set_attribute("embedding.input.tokens", 20)
        span.set_attribute("llm.cost.total_usd", 0.0002)
        
        time.sleep(0.2)
        print("✅ Simulated embedding call")


def simulate_error_scenario(tracer):
    """Simulate an error case with proper instrumentation"""
    
    with tracer.start_as_current_span("llm.completion.failed") as span:
        span.set_attribute("gen_ai.system", "openai")
        span.set_attribute("gen_ai.request.model", "gpt-4")
        
        try:
            # Simulate an error
            raise Exception("Rate limit exceeded")
        except Exception as e:
            span.set_attribute("error", True)
            span.set_attribute("error.type", type(e).__name__)
            span.set_attribute("error.message", str(e))
            span.record_exception(e)
            print("✅ Simulated error scenario")


def simulate_complex_workflow(tracer):
    """Simulate a complex multi-step LLM workflow"""
    
    with tracer.start_as_current_span("workflow.document_qa") as root_span:
        root_span.set_attribute("workflow.type", "question_answering")
        root_span.set_attribute("workflow.document.id", "doc_789")
        
        # Step 1: Generate embeddings for chunks
        with tracer.start_as_current_span("step.embed_chunks") as embed_span:
            embed_span.set_attribute("chunks.count", 5)
            for i in range(5):
                with tracer.start_as_current_span(f"chunk.embed.{i}") as chunk_span:
                    chunk_span.set_attribute("chunk.index", i)
                    chunk_span.set_attribute("chunk.tokens", 100)
                    time.sleep(0.05)
        
        # Step 2: Query vector database
        with tracer.start_as_current_span("step.vector_search") as search_span:
            search_span.set_attribute("query.embedding.dimensions", 1536)
            search_span.set_attribute("results.count", 3)
            search_span.set_attribute("results.relevance.min", 0.85)
            time.sleep(0.1)
        
        # Step 3: Generate answer
        with tracer.start_as_current_span("step.generate_answer") as answer_span:
            answer_span.set_attribute("gen_ai.system", "openai")
            answer_span.set_attribute("gen_ai.request.model", "gpt-4")
            answer_span.set_attribute("context.chunks", 3)
            answer_span.set_attribute("gen_ai.usage.total_tokens", 500)
            answer_span.set_attribute("llm.cost.total_usd", 0.015)
            time.sleep(0.3)
        
        root_span.set_attribute("workflow.duration_ms", 500)
        root_span.set_attribute("workflow.success", True)
        print("✅ Simulated complex workflow")


def main():
    """Main example runner"""
    print("🚀 zradar OTLP Example - Python\n")
    
    # Check for API key
    if API_KEY == "your-api-key-here":
        print("⚠️  Warning: Using placeholder API key")
        print("   Set ZVRADAR_API_KEY environment variable with your actual key")
        print("   Example: export ZVRADAR_API_KEY=zvr_abc123...\n")
    
    # Setup telemetry
    tracer = setup_telemetry()
    
    print("\nSending example traces...\n")
    
    # Example 1: Simple LLM call
    print("1. Simple LLM completion:")
    simulate_llm_call(tracer)
    
    # Example 2: Embedding generation
    print("\n2. Embedding generation:")
    simulate_embedding_call(tracer)
    
    # Example 3: Error scenario
    print("\n3. Error scenario:")
    simulate_error_scenario(tracer)
    
    # Example 4: Complex workflow
    print("\n4. Complex multi-step workflow:")
    simulate_complex_workflow(tracer)
    
    # Flush all pending spans
    print("\n⏳ Flushing spans to zradar...")
    trace.get_tracer_provider().force_flush()
    
    print("\n✨ Done! Check zradar dashboard to see your traces")
    print(f"   Admin API: http://localhost:8081")
    print(f"   Swagger UI: http://localhost:8081/swagger-ui/")


if __name__ == "__main__":
    main()

