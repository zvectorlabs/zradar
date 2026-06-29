/**
 * Anthropic Claude SDK — zradar minimal agent example (TypeScript).
 *
 * Demonstrates manual OpenTelemetry instrumentation for a tool-use agent loop.
 * Sends traces to zradar via OTLP/gRPC.
 *
 * Environment variables:
 *   ANTHROPIC_API_KEY   — if unset, a mock client is used (no network calls)
 *   ZRADAR_ENDPOINT     — OTLP gRPC host:port  (default: localhost:4317)
 *   ZRADAR_API_KEY      — bearer token          (default: zk_dev_example)
 */

import { NodeSDK } from '@opentelemetry/sdk-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { Resource } from '@opentelemetry/resources';
import { SEMRESATTRS_SERVICE_NAME } from '@opentelemetry/semantic-conventions';
import * as grpc from '@grpc/grpc-js';
import { trace, SpanStatusCode, context } from '@opentelemetry/api';

// ---------------------------------------------------------------------------
// OTel setup — must happen before any other imports that patch the SDK
// ---------------------------------------------------------------------------
const zradarEndpoint = process.env.ZRADAR_ENDPOINT ?? 'localhost:4317';
const zradarApiKey = process.env.ZRADAR_API_KEY ?? 'zk_dev_example';

const metadata = new grpc.Metadata();
metadata.add('authorization', `Bearer ${zradarApiKey}`);

const exporter = new OTLPTraceExporter({
  url: zradarEndpoint,
  credentials: grpc.credentials.createInsecure(),
  metadata,
});

const sdk = new NodeSDK({
  resource: new Resource({
    [SEMRESATTRS_SERVICE_NAME]: 'zradar-example-anthropic',
  }),
  traceExporter: exporter,
});

sdk.start();

const tracer = trace.getTracer('zradar.examples.anthropic');

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
const MODEL = 'claude-3-5-haiku-20241022';

interface ToolUseBlock {
  type: 'tool_use';
  id: string;
  name: string;
  input: Record<string, string>;
}

interface TextBlock {
  type: 'text';
  text: string;
}

type ContentBlock = ToolUseBlock | TextBlock;

interface FakeMessage {
  content: ContentBlock[];
  stop_reason: 'tool_use' | 'end_turn';
  model: string;
  usage: { input_tokens: number; output_tokens: number };
}

// ---------------------------------------------------------------------------
// Mock Anthropic client
// ---------------------------------------------------------------------------
let _mockCallCount = 0;

function createMockMessage(): FakeMessage {
  _mockCallCount++;
  if (_mockCallCount === 1) {
    return {
      content: [
        {
          type: 'tool_use',
          id: 'tool_abc123',
          name: 'calculator',
          input: { expression: '2 + 2' },
        },
      ],
      stop_reason: 'tool_use',
      model: MODEL,
      usage: { input_tokens: 42, output_tokens: 17 },
    };
  }
  return {
    content: [{ type: 'text', text: 'The result of 2 + 2 is 4.' }],
    stop_reason: 'end_turn',
    model: MODEL,
    usage: { input_tokens: 55, output_tokens: 12 },
  };
}

// ---------------------------------------------------------------------------
// Tool implementation
// ---------------------------------------------------------------------------
const SAFE_EXPRESSION = '2 + 2';

function calculator(expression: string): number {
  if (expression.trim() !== SAFE_EXPRESSION) {
    throw new Error(`Unsupported expression: ${expression}`);
  }
  // eslint-disable-next-line no-eval
  return eval(SAFE_EXPRESSION) as number;
}

const TOOLS = [
  {
    name: 'calculator',
    description: 'Evaluate a simple arithmetic expression and return the numeric result.',
    input_schema: {
      type: 'object',
      properties: {
        expression: {
          type: 'string',
          description: "The arithmetic expression to evaluate, e.g. '2 + 2'.",
        },
      },
      required: ['expression'],
    },
  },
];

// ---------------------------------------------------------------------------
// Agent loop
// ---------------------------------------------------------------------------
type Message = { role: 'user' | 'assistant'; content: unknown };

async function runAgent(): Promise<void> {
  const apiKey = process.env.ANTHROPIC_API_KEY;

  // Dynamically import Anthropic only when a real key is present
  let anthropicClient: { messages: { create: (opts: unknown) => Promise<FakeMessage> } } | null =
    null;

  if (apiKey) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Anthropic = (await import('@anthropic-ai/sdk')).default;
    anthropicClient = new Anthropic({ apiKey }) as unknown as typeof anthropicClient;
    console.log('[agent] Using real Anthropic client.');
  } else {
    anthropicClient = {
      messages: { create: async () => createMockMessage() },
    };
    console.log('[agent] ANTHROPIC_API_KEY not set — using mock client.');
  }

  const messages: Message[] = [
    { role: 'user', content: 'What is 2 + 2? Use the calculator tool.' },
  ];

  let toolCallCount = 0;
  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  let finalText = '';

  const agentSpan = tracer.startSpan('anthropic.agent.run');
  const ctx = trace.setSpan(context.active(), agentSpan);

  await context.with(ctx, async () => {
    agentSpan.setAttribute('gen_ai.system', 'anthropic');
    agentSpan.setAttribute('gen_ai.request.model', MODEL);

    try {
      // ---- agent loop ----
      while (true) {
        const response = await anthropicClient!.messages.create({
          model: MODEL,
          max_tokens: 1024,
          tools: TOOLS,
          messages,
        });

        totalInputTokens += response.usage.input_tokens;
        totalOutputTokens += response.usage.output_tokens;

        const assistantContent: ContentBlock[] = [];

        for (const block of response.content) {
          assistantContent.push(block);

          if (block.type === 'tool_use') {
            toolCallCount++;
            const toolName = block.name;
            const toolInput = block.input;

            const toolSpan = tracer.startSpan(`anthropic.tool.${toolName}`, undefined, ctx);
            toolSpan.setAttribute('tool.name', toolName);
            toolSpan.setAttribute('tool.input', JSON.stringify(toolInput));

            try {
              const result = calculator(toolInput.expression);
              console.log(`[tool] ${toolName}(${JSON.stringify(toolInput)}) = ${result}`);

              // Append assistant turn then tool result
              messages.push({ role: 'assistant', content: assistantContent.slice() });
              messages.push({
                role: 'user',
                content: [
                  {
                    type: 'tool_result',
                    tool_use_id: block.id,
                    content: String(result),
                  },
                ],
              });
              assistantContent.length = 0; // reset for next turn
            } finally {
              toolSpan.end();
            }
          } else if (block.type === 'text') {
            finalText = block.text;
          }
        }

        if (response.stop_reason === 'end_turn') {
          if (assistantContent.length > 0) {
            messages.push({ role: 'assistant', content: assistantContent });
          }
          break;
        }
      }
    } catch (err) {
      agentSpan.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
      throw err;
    } finally {
      agentSpan.setAttribute('agent.tool_calls', toolCallCount);
      agentSpan.setAttribute('gen_ai.usage.input_tokens', totalInputTokens);
      agentSpan.setAttribute('gen_ai.usage.output_tokens', totalOutputTokens);
      agentSpan.end();
    }
  });

  console.log(`\n[agent] Final answer: ${finalText}`);
  console.log(
    `[agent] Stats — tool_calls=${toolCallCount}, ` +
      `input_tokens=${totalInputTokens}, output_tokens=${totalOutputTokens}`,
  );
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
runAgent()
  .catch((err) => {
    console.error('[agent] Fatal error:', err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await sdk.shutdown();
  });
