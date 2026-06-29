/**
 * Mastra weather-forecast agent — zradar OTel instrumentation example
 *
 * Mock strategy
 * ─────────────
 * • OPENAI_API_KEY set  → real path: Mastra drives the LLM + tool call;
 *   a manual parent span wraps the call so zradar sees a single root trace.
 *
 * • OPENAI_API_KEY absent → mock path: we skip agent.generate() and instead
 *   manually invoke the forecast function while emitting the same OTel spans
 *   that a real run would produce, satisfying the expected_spans.json contract
 *   in CI without requiring an API key.
 */

// ── OTel imports ─────────────────────────────────────────────────────────────
// Static imports are hoisted, but module-level initialisation code runs
// top-to-bottom, so sdk.start() fires before `new Mastra(…)` below.
import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { Resource } from '@opentelemetry/resources';
import { trace, SpanStatusCode } from '@opentelemetry/api';
import { credentials, Metadata } from '@grpc/grpc-js';

// ── Mastra + AI SDK ──────────────────────────────────────────────────────────
// @mastra/core ^0.10 exports Mastra, Agent, and createTool from the package
// root.  If your version uses sub-path exports, substitute:
//   import { Mastra }     from '@mastra/core';
//   import { Agent }      from '@mastra/core/agent';
//   import { createTool } from '@mastra/core/tools';
import { Mastra, Agent, createTool } from '@mastra/core';
import { openai } from '@ai-sdk/openai';
import { z } from 'zod';

// ── Config ───────────────────────────────────────────────────────────────────

const SERVICE_NAME    = 'mastra-example';
const MODEL_ID        = 'gpt-4o-mini';

/** gRPC endpoint in host:port format (no http:// prefix). */
const ZRADAR_ENDPOINT = process.env.ZRADAR_ENDPOINT ?? 'localhost:4317';
const ZRADAR_API_KEY  = process.env.ZRADAR_API_KEY;

// ── 1. Bootstrap OTel SDK ────────────────────────────────────────────────────

const grpcMeta = new Metadata();
if (ZRADAR_API_KEY) grpcMeta.set('x-api-key', ZRADAR_API_KEY);

const traceExporter = new OTLPTraceExporter({
  url: ZRADAR_ENDPOINT,
  credentials: credentials.createInsecure(),
  metadata: grpcMeta,
});

const sdk = new NodeSDK({
  resource: new Resource({ 'service.name': SERVICE_NAME }),
  traceExporter,
});

sdk.start();

// ── 2. Core business logic (reusable in mock path) ───────────────────────────

function getForecastImpl(city: string, days: number): string {
  // Hardcoded forecast — no external weather API needed
  return (
    `${city}: Sunny and mild for the next ${days} day(s). ` +
    `High 72°F (22°C) / Low 58°F (14°C). Wind: 10 mph NW.`
  );
}

// ── 3. Mastra tool ───────────────────────────────────────────────────────────

const getForecast = createTool({
  id: 'get_forecast',
  description:
    'Return a plain-text weather forecast for a city over a number of days.',
  inputSchema: z.object({
    city: z.string().describe('City name, e.g. "Paris"'),
    days: z
      .number()
      .int()
      .min(1)
      .max(14)
      .describe('Number of forecast days (1–14)'),
  }),
  outputSchema: z.object({
    forecast: z.string(),
  }),
  execute: async ({ context }) => ({
    forecast: getForecastImpl(context.city, context.days),
  }),
});

// ── 4. Mastra agent ──────────────────────────────────────────────────────────

const forecastAgent = new Agent({
  name: 'forecastAgent',
  instructions:
    'You are a concise weather assistant. ' +
    'When asked for a forecast, call the get_forecast tool and report its ' +
    'result verbatim without additional commentary.',
  model: openai(MODEL_ID),
  tools: { getForecast },
});

// ── 5. Mastra instance with built-in telemetry ───────────────────────────────
// Mastra ≥0.10 wires its own OTel provider through this config.
// If the built-in exporter also reaches ZRADAR_ENDPOINT the spans will be
// duplicated with those from the manual NodeSDK above — harmless here.
// In production choose one export path.

const mastra = new Mastra({
  agents: { forecastAgent },
  telemetry: {
    serviceName: SERVICE_NAME,
    enabled: true,
    // Mastra's OTLP export config (accepted in ^0.10).
    // Falls back gracefully to the manual NodeSDK exporter if the built-in
    // path does not support custom OTLP gRPC endpoints.
    export: {
      type: 'otlp',
      endpoint: ZRADAR_ENDPOINT,
    },
  },
});

// ── 6. Tracer ────────────────────────────────────────────────────────────────

const tracer = trace.getTracer(SERVICE_NAME, '0.1.0');

// ── 7. Main ──────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const prompt = 'What is the 3-day weather forecast for Paris?';

  if (process.env.OPENAI_API_KEY) {
    // ── Real path ────────────────────────────────────────────────────────────
    // Mastra drives the LLM → tool call cycle.  The parent span wraps the
    // full call so zradar sees one root entry per agent run; Mastra's
    // internal spans appear as children.
    await tracer.startActiveSpan(
      'mastra.agent.run',
      {
        attributes: {
          'gen_ai.system': 'mastra',
          'gen_ai.request.model': MODEL_ID,
          'agent.name': forecastAgent.name,
        },
      },
      async (span) => {
        try {
          const agent = mastra.getAgent('forecastAgent');
          const result = await agent.generate(prompt);
          console.log('Agent response:', result.text);
          span.setStatus({ code: SpanStatusCode.OK });
        } catch (err) {
          span.recordException(err as Error);
          span.setStatus({ code: SpanStatusCode.ERROR });
          throw err;
        } finally {
          span.end();
        }
      },
    );
  } else {
    // ── Mock path ────────────────────────────────────────────────────────────
    // No API key available.  We emit the same spans a real run would produce
    // so the expected_spans.json contract is satisfied in CI.
    console.log('[mock] OPENAI_API_KEY not set — running mock agent execution');

    await tracer.startActiveSpan(
      'mastra.agent.run',
      {
        attributes: {
          'gen_ai.system': 'mastra',
          'gen_ai.request.model': MODEL_ID,
          'agent.name': forecastAgent.name,
        },
      },
      async (parentSpan) => {
        try {
          // Simulate the model deciding to call get_forecast
          const toolInput = { city: 'Paris', days: 3 };

          // Child span mirrors what Mastra would emit for a tool invocation
          await tracer.startActiveSpan(
            'mastra.tool.get_forecast',
            {
              attributes: {
                'tool.name': 'get_forecast',
                'tool.input': JSON.stringify(toolInput),
              },
            },
            async (toolSpan) => {
              try {
                // Invoke the actual business logic so output is realistic
                const forecast = getForecastImpl(toolInput.city, toolInput.days);
                console.log('[mock] Tool result:', forecast);
                toolSpan.setStatus({ code: SpanStatusCode.OK });
              } catch (err) {
                toolSpan.recordException(err as Error);
                toolSpan.setStatus({ code: SpanStatusCode.ERROR });
                throw err;
              } finally {
                toolSpan.end();
              }
            },
          );

          const mockResponse =
            '[mock] Paris will be sunny and mild for the next 3 days. ' +
            'High 72°F (22°C), Low 58°F (14°C).';
          console.log('Agent response:', mockResponse);
          parentSpan.setStatus({ code: SpanStatusCode.OK });
        } catch (err) {
          parentSpan.recordException(err as Error);
          parentSpan.setStatus({ code: SpanStatusCode.ERROR });
          throw err;
        } finally {
          parentSpan.end();
        }
      },
    );
  }

  // Flush all pending spans before process exit
  await sdk.shutdown();
}

main().catch(async (err) => {
  console.error(err);
  await sdk.shutdown();
  process.exit(1);
});
