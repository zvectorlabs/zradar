#!/usr/bin/env node
/**
 * Example: Send OTLP traces to zradar from Node.js
 * 
 * This demonstrates how to:
 * 1. Configure OpenTelemetry SDK for Node.js
 * 2. Authenticate with API key
 * 3. Send traces to zradar
 * 4. Instrument LLM calls
 */

import { trace } from '@opentelemetry/api';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { Resource } from '@opentelemetry/resources';
import { SemanticResourceAttributes } from '@opentelemetry/semantic-conventions';

// Configuration
const ZVRADAR_ENDPOINT = process.env.ZVRADAR_ENDPOINT || 'localhost:4317';
const API_KEY = process.env.ZVRADAR_API_KEY || 'your-api-key-here';

/**
 * Setup OpenTelemetry SDK
 */
function setupTelemetry() {
  // Create resource with service information
  const resource = new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: 'example-nodejs-app',
    [SemanticResourceAttributes.SERVICE_VERSION]: '1.0.0',
    [SemanticResourceAttributes.DEPLOYMENT_ENVIRONMENT]: 'development',
  });

  // Configure OTLP exporter with authentication
  const traceExporter = new OTLPTraceExporter({
    url: `http://${ZVRADAR_ENDPOINT}`,
    headers: {
      authorization: `Bearer ${API_KEY}`,
    },
  });

  // Create and configure SDK
  const sdk = new NodeSDK({
    resource,
    traceExporter,
  });

  // Start the SDK
  sdk.start();
  console.log(`✅ Telemetry configured - sending to ${ZVRADAR_ENDPOINT}`);

  // Graceful shutdown
  process.on('SIGTERM', () => {
    sdk.shutdown()
      .then(() => console.log('Telemetry terminated'))
      .catch((error) => console.log('Error terminating telemetry', error))
      .finally(() => process.exit(0));
  });

  return trace.getTracer('zradar-example');
}

/**
 * Simulate an OpenAI API call
 */
async function simulateOpenAICall(tracer) {
  const span = tracer.startSpan('llm.openai.completion');
  
  try {
    // Set LLM-specific attributes
    span.setAttributes({
      'llm.vendor': 'openai',
      'llm.model': 'gpt-4',
      'llm.temperature': 0.7,
      'llm.max_tokens': 1000,
      'llm.stream': false,
    });

    // Simulate API latency
    await new Promise(resolve => setTimeout(resolve, 500));

    // Add response metadata
    span.setAttributes({
      'llm.prompt.tokens': 50,
      'llm.completion.tokens': 150,
      'llm.total.tokens': 200,
      'llm.cost.input': 0.0015,
      'llm.cost.output': 0.0045,
      'llm.cost.total': 0.006,
      'llm.finish_reason': 'stop',
    });

    console.log('✅ Simulated OpenAI call');
  } catch (error) {
    span.recordException(error);
    span.setStatus({ code: 2, message: error.message }); // ERROR
    throw error;
  } finally {
    span.end();
  }
}

/**
 * Simulate Anthropic Claude API call
 */
async function simulateClaudeCall(tracer) {
  const span = tracer.startSpan('llm.anthropic.completion');
  
  try {
    span.setAttributes({
      'llm.vendor': 'anthropic',
      'llm.model': 'claude-3-opus-20240229',
      'llm.temperature': 0.8,
      'llm.max_tokens': 2000,
      'llm.top_p': 0.95,
    });

    await new Promise(resolve => setTimeout(resolve, 600));

    span.setAttributes({
      'llm.prompt.tokens': 75,
      'llm.completion.tokens': 200,
      'llm.total.tokens': 275,
      'llm.cost.total': 0.021, // Claude Opus pricing
      'llm.stop_reason': 'end_turn',
    });

    console.log('✅ Simulated Claude call');
  } finally {
    span.end();
  }
}

/**
 * Simulate a RAG (Retrieval Augmented Generation) pipeline
 */
async function simulateRAGPipeline(tracer) {
  const rootSpan = tracer.startSpan('pipeline.rag');
  
  try {
    rootSpan.setAttributes({
      'pipeline.type': 'rag',
      'pipeline.query': 'What is zradar?',
    });

    // Step 1: Generate query embedding
    const embedSpan = tracer.startSpan('step.generate_embedding', {}, trace.setSpan(trace.context.active(), rootSpan));
    try {
      embedSpan.setAttributes({
        'llm.vendor': 'openai',
        'llm.model': 'text-embedding-ada-002',
        'embedding.dimensions': 1536,
        'llm.cost.total': 0.0001,
      });
      await new Promise(resolve => setTimeout(resolve, 150));
    } finally {
      embedSpan.end();
    }

    // Step 2: Vector database search
    const searchSpan = tracer.startSpan('step.vector_search', {}, trace.setSpan(trace.context.active(), rootSpan));
    try {
      searchSpan.setAttributes({
        'db.system': 'pinecone',
        'db.operation': 'query',
        'results.count': 5,
        'results.relevance.threshold': 0.8,
      });
      await new Promise(resolve => setTimeout(resolve, 100));
    } finally {
      searchSpan.end();
    }

    // Step 3: LLM completion with context
    const completionSpan = tracer.startSpan('step.completion', {}, trace.setSpan(trace.context.active(), rootSpan));
    try {
      completionSpan.setAttributes({
        'llm.vendor': 'openai',
        'llm.model': 'gpt-4',
        'context.chunks': 5,
        'llm.total.tokens': 800,
        'llm.cost.total': 0.024,
      });
      await new Promise(resolve => setTimeout(resolve, 700));
    } finally {
      completionSpan.end();
    }

    rootSpan.setAttributes({
      'pipeline.success': true,
      'pipeline.total_cost': 0.0241,
    });

    console.log('✅ Simulated RAG pipeline');
  } finally {
    rootSpan.end();
  }
}

/**
 * Simulate error handling
 */
async function simulateErrorScenario(tracer) {
  const span = tracer.startSpan('llm.call.with_retry');
  
  try {
    span.setAttributes({
      'llm.vendor': 'openai',
      'llm.model': 'gpt-4',
    });

    // Simulate retries
    for (let attempt = 1; attempt <= 3; attempt++) {
      const retrySpan = tracer.startSpan(`attempt.${attempt}`, {}, trace.setSpan(trace.context.active(), span));
      
      try {
        retrySpan.setAttribute('retry.attempt', attempt);
        
        if (attempt < 3) {
          // Simulate failure
          await new Promise(resolve => setTimeout(resolve, 100));
          const error = new Error('Rate limit exceeded');
          retrySpan.recordException(error);
          retrySpan.setAttribute('error', true);
          retrySpan.setAttribute('error.type', 'RateLimitError');
        } else {
          // Success on third attempt
          await new Promise(resolve => setTimeout(resolve, 500));
          retrySpan.setAttribute('success', true);
        }
      } finally {
        retrySpan.end();
      }
    }

    span.setAttributes({
      'retry.total_attempts': 3,
      'retry.success': true,
    });

    console.log('✅ Simulated retry scenario');
  } finally {
    span.end();
  }
}

/**
 * Main function
 */
async function main() {
  console.log('🚀 zradar OTLP Example - Node.js\n');

  // Check for API key
  if (API_KEY === 'your-api-key-here') {
    console.log('⚠️  Warning: Using placeholder API key');
    console.log('   Set ZVRADAR_API_KEY environment variable with your actual key');
    console.log('   Example: export ZVRADAR_API_KEY=zvr_abc123...\n');
  }

  // Setup telemetry
  const tracer = setupTelemetry();

  console.log('\nSending example traces...\n');

  try {
    // Example 1: OpenAI call
    console.log('1. OpenAI GPT-4 completion:');
    await simulateOpenAICall(tracer);

    // Example 2: Anthropic Claude call
    console.log('\n2. Anthropic Claude completion:');
    await simulateClaudeCall(tracer);

    // Example 3: RAG pipeline
    console.log('\n3. RAG pipeline:');
    await simulateRAGPipeline(tracer);

    // Example 4: Error handling with retries
    console.log('\n4. Error handling with retries:');
    await simulateErrorScenario(tracer);

    console.log('\n⏳ Flushing spans to zradar...');
    
    // Give time for spans to be exported
    await new Promise(resolve => setTimeout(resolve, 2000));

    console.log('\n✨ Done! Check zradar dashboard to see your traces');
    console.log('   Admin API: http://localhost:8081');
    console.log('   Swagger UI: http://localhost:8081/swagger-ui/');

  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }

  process.exit(0);
}

// Run if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

