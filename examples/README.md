# zradar Examples

Example client applications demonstrating how to send OTLP telemetry to zradar.

## Quick Start

### Prerequisites

1. **zradar server running:**
   ```bash
   # From project root
   ./scripts/bootstrap.sh
   cargo run --release
   ```

2. **Create an API key:**
   ```bash
   # Login to get JWT token
   TOKEN=$(curl -X POST http://localhost:8080/api/v1/auth/login \
     -H "Content-Type: application/json" \
     -d '{"email":"admin@example.com","password":"changeme123"}' \
     | jq -r '.token')

   # Create organization
   ORG_ID=$(curl -X POST http://localhost:8080/api/v1/organizations \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"name":"my-org","display_name":"My Organization"}' \
     | jq -r '.id')

   # Create project
   PROJECT_ID=$(curl -X POST http://localhost:8080/api/v1/projects \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"organization_id":"'$ORG_ID'","name":"production","display_name":"Production"}' \
     | jq -r '.id')

   # Create API key
   API_KEY=$(curl -X POST http://localhost:8080/api/v1/api-keys \
     -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"project_id":"'$PROJECT_ID'","name":"example-key","description":"Key for examples"}' \
     | jq -r '.key')

   # Save the API key
   echo "export ZVRADAR_API_KEY=$API_KEY" >> ~/.zradar_env
   source ~/.zradar_env
   ```

## Examples

### Python Example

**Features:**
- Simple LLM completion
- Embedding generation
- Error scenarios
- Complex multi-step workflows
- Rich LLM-specific attributes

**Setup:**
```bash
cd examples/python
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**Run:**
```bash
export ZVRADAR_API_KEY=your-key-here
python send_trace.py
```

**What it demonstrates:**
- OpenTelemetry SDK configuration
- Bearer token authentication
- Custom LLM attributes (tokens, costs, model info)
- Nested spans for workflow visibility
- Error instrumentation

### Node.js Example

**Features:**
- OpenAI GPT-4 calls
- Anthropic Claude calls
- RAG (Retrieval Augmented Generation) pipeline
- Retry logic with instrumentation

**Setup:**
```bash
cd examples/nodejs
npm install
```

**Run:**
```bash
export ZVRADAR_API_KEY=your-key-here
node send-trace.js
```

**What it demonstrates:**
- Node.js OpenTelemetry SDK
- Multiple LLM vendors (OpenAI, Anthropic)
- Complex pipeline instrumentation
- Retry pattern instrumentation

## LLM-Specific Attributes

zradar supports rich LLM observability. Here are the recommended attributes:

### Model Information
```javascript
{
  'llm.vendor': 'openai',           // LLM provider
  'llm.model': 'gpt-4',             // Model name
  'llm.temperature': 0.7,           // Temperature setting
  'llm.max_tokens': 1000,           // Max tokens
  'llm.top_p': 0.95,                // Top-p sampling
  'llm.stream': false,              // Streaming enabled?
}
```

### Token Usage
```javascript
{
  'llm.prompt.tokens': 50,          // Input tokens
  'llm.completion.tokens': 150,     // Output tokens
  'llm.total.tokens': 200,          // Total tokens
}
```

### Cost Tracking
```javascript
{
  'llm.cost.input': 0.0015,         // Input cost (USD)
  'llm.cost.output': 0.0045,        // Output cost (USD)
  'llm.cost.total': 0.006,          // Total cost (USD)
}
```

### Response Metadata
```javascript
{
  'llm.request.id': 'req_abc123',   // Request ID
  'llm.response.id': 'resp_def456', // Response ID
  'llm.finish_reason': 'stop',      // Why completion finished
  'llm.stop_reason': 'end_turn',    // Alternative (Anthropic)
}
```

### Embeddings
```javascript
{
  'embedding.dimensions': 1536,      // Vector dimensions
  'embedding.input.tokens': 20,      // Input tokens
}
```

### Workflow Context
```javascript
{
  'workflow.type': 'rag',            // Workflow type
  'workflow.step': 'embedding',      // Current step
  'context.chunks': 5,               // Number of context chunks
  'pipeline.success': true,          // Success flag
}
```

## Advanced Usage

### Custom Instrumentation

**Python:**
```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("custom.operation") as span:
    span.set_attribute("custom.attribute", "value")
    # Your code here
    span.set_attribute("operation.result", "success")
```

**Node.js:**
```javascript
import { trace } from '@opentelemetry/api';

const tracer = trace.getTracer('my-app');

const span = tracer.startSpan('custom.operation');
span.setAttribute('custom.attribute', 'value');
try {
  // Your code here
  span.setAttribute('operation.result', 'success');
} finally {
  span.end();
}
```

### Error Handling

**Python:**
```python
try:
    # Operation that might fail
    result = call_llm()
except Exception as e:
    span.set_attribute("error", True)
    span.set_attribute("error.type", type(e).__name__)
    span.set_attribute("error.message", str(e))
    span.record_exception(e)
    raise
```

**Node.js:**
```javascript
try {
  // Operation that might fail
  await callLLM();
} catch (error) {
  span.setAttributes({
    'error': true,
    'error.type': error.constructor.name,
    'error.message': error.message,
  });
  span.recordException(error);
  throw error;
}
```

### Async Context Propagation

**Python:**
```python
from opentelemetry import context

# Create parent span
with tracer.start_as_current_span("parent") as parent:
    # This automatically propagates to async calls
    await async_operation()
```

**Node.js:**
```javascript
import { context, trace } from '@opentelemetry/api';

const parentSpan = tracer.startSpan('parent');
const ctx = trace.setSpan(context.active(), parentSpan);

// Propagate context to child
const childSpan = tracer.startSpan('child', {}, ctx);
```

## Viewing Your Data

### CLI Queries

```bash
# Check API keys
psql zradar -c "
  SELECT 
    name, 
    created_at,
    last_used_at,
    is_revoked
  FROM api_keys
  ORDER BY created_at DESC;
"
```

### Swagger UI

1. Open http://localhost:8080/swagger-ui/
2. Authenticate with JWT token
3. Try the API endpoints

## Troubleshooting

### "Connection refused"
- Ensure zradar server is running: `cargo run --release`
- Check correct endpoint: default is `localhost:4317`

### "Invalid API key"
- Verify API key is set: `echo $ZVRADAR_API_KEY`
- Check key hasn't been revoked in database
- Ensure Bearer token format: `Bearer your-key-here`

### "No spans appearing"
- Check server logs: Look for OTLP ingestion messages
- Verify batch is flushed: `provider.force_flush()` (Python) or wait for auto-flush
- Query traces through the Admin API

### SSL/TLS Errors
- Examples use `insecure=True` for local development
- In production, set `insecure=False` and use proper TLS

## Next Steps

1. **Integrate into your app:**
   - Copy the SDK setup code
   - Add instrumentation to your LLM calls
   - Deploy to production

2. **Explore the data:**
   - Query traces through the Admin API
   - Build dashboards
   - Set up alerts

3. **Advanced features:**
   - Custom sampling strategies
   - Trace filtering
   - Cost optimization analysis

## Resources

- [OpenTelemetry Python Docs](https://opentelemetry.io/docs/instrumentation/python/)
- [OpenTelemetry JS Docs](https://opentelemetry.io/docs/instrumentation/js/)
- [OTLP Specification](https://opentelemetry.io/docs/specs/otlp/)
- [zradar Documentation](../docs/)

## Contributing

Have an example for another language? PRs welcome!

Planned examples:
- [ ] Go
- [ ] Rust
- [ ] Java
- [ ] Ruby
- [ ] PHP

