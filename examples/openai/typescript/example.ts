/**
 * zradar example: raw OpenAI SDK tool-calling loop with manual OTel instrumentation.
 *
 * Sends traces to zradar via OTLP/gRPC.
 *
 * Environment variables:
 *   OPENAI_API_KEY   — omit to use the built-in mock client
 *   ZRADAR_ENDPOINT  — OTLP gRPC endpoint (default: localhost:4317)
 *   ZRADAR_API_KEY   — zradar ingest key  (default: zk_dev_example)
 */

import OpenAI from "openai";
import type {
  ChatCompletion,
  ChatCompletionMessageParam,
  ChatCompletionTool,
} from "openai/resources/chat/completions";

import { NodeSDK } from "@opentelemetry/sdk-node";
import { Resource } from "@opentelemetry/resources";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { trace, SpanStatusCode } from "@opentelemetry/api";
import * as grpc from "@grpc/grpc-js";

// ---------------------------------------------------------------------------
// OpenTelemetry setup
// ---------------------------------------------------------------------------

const ZRADAR_ENDPOINT = process.env.ZRADAR_ENDPOINT ?? "localhost:4317";
const ZRADAR_API_KEY = process.env.ZRADAR_API_KEY ?? "zk_dev_example";

const exporter = new OTLPTraceExporter({
  url: `http://${ZRADAR_ENDPOINT}`,
  credentials: grpc.credentials.createInsecure(),
  metadata: (() => {
    const meta = new grpc.Metadata();
    meta.add("x-zradar-api-key", ZRADAR_API_KEY);
    return meta;
  })(),
});

const sdk = new NodeSDK({
  resource: new Resource({
    [SemanticResourceAttributes.SERVICE_NAME]: "zradar-example-openai",
    [SemanticResourceAttributes.SERVICE_VERSION]: "0.1.0",
  }),
  traceExporter: exporter,
});

sdk.start();

const tracer = trace.getTracer("zradar.example.openai");

// ---------------------------------------------------------------------------
// Mock OpenAI client (used when OPENAI_API_KEY is not set)
// ---------------------------------------------------------------------------

let mockCallCounter = 0;

function makeToolCallResponse(): ChatCompletion {
  return {
    id: "chatcmpl-mock-001",
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: "gpt-4o-mini",
    choices: [
      {
        index: 0,
        message: {
          role: "assistant",
          content: null,
          refusal: null,
          tool_calls: [
            {
              id: "call_mock_001",
              type: "function",
              function: {
                name: "get_weather",
                arguments: JSON.stringify({ city: "London", unit: "celsius" }),
              },
            },
          ],
        },
        finish_reason: "tool_calls",
        logprobs: null,
      },
    ],
    usage: {
      prompt_tokens: 42,
      completion_tokens: 18,
      total_tokens: 60,
    },
  };
}

function makeFinalResponse(): ChatCompletion {
  return {
    id: "chatcmpl-mock-002",
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: "gpt-4o-mini",
    choices: [
      {
        index: 0,
        message: {
          role: "assistant",
          content: "The current weather in London is 15°C with overcast skies.",
          refusal: null,
          tool_calls: undefined,
        },
        finish_reason: "stop",
        logprobs: null,
      },
    ],
    usage: {
      prompt_tokens: 80,
      completion_tokens: 22,
      total_tokens: 102,
    },
  };
}

// Minimal mock that satisfies the shape expected by our loop.
// It only implements the create method we actually call.
type MinimalOpenAIClient = {
  chat: {
    completions: {
      create(params: {
        model: string;
        messages: ChatCompletionMessageParam[];
        tools: ChatCompletionTool[];
        tool_choice: string;
      }): Promise<ChatCompletion>;
    };
  };
};

const mockClient: MinimalOpenAIClient = {
  chat: {
    completions: {
      async create(_params) {
        mockCallCounter += 1;
        if (mockCallCounter === 1) return makeToolCallResponse();
        return makeFinalResponse();
      },
    },
  },
};

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

const TOOLS: ChatCompletionTool[] = [
  {
    type: "function",
    function: {
      name: "get_weather",
      description: "Return the current weather for a city.",
      parameters: {
        type: "object",
        properties: {
          city: {
            type: "string",
            description: "City name, e.g. 'London'",
          },
          unit: {
            type: "string",
            enum: ["celsius", "fahrenheit"],
            description: "Temperature unit.",
          },
        },
        required: ["city", "unit"],
      },
    },
  },
];

// ---------------------------------------------------------------------------
// Tool implementation
// ---------------------------------------------------------------------------

const weatherData: Record<string, Record<string, string>> = {
  london: { celsius: "15°C, overcast", fahrenheit: "59°F, overcast" },
  "new york": { celsius: "22°C, sunny", fahrenheit: "72°F, sunny" },
  tokyo: { celsius: "28°C, humid", fahrenheit: "82°F, humid" },
};

function getWeather(city: string, unit: string): string {
  const key = city.toLowerCase();
  if (weatherData[key]?.[unit]) {
    return `The weather in ${city} is ${weatherData[key][unit]}.`;
  }
  return `Weather data unavailable for ${city}.`;
}

function dispatchTool(name: string, argumentsJson: string): string {
  const args = JSON.parse(argumentsJson) as Record<string, string>;
  if (name === "get_weather") {
    return getWeather(args["city"] ?? "", args["unit"] ?? "celsius");
  }
  throw new Error(`Unknown tool: ${name}`);
}

// ---------------------------------------------------------------------------
// Agent loop
// ---------------------------------------------------------------------------

const MODEL = "gpt-4o-mini";
const SYSTEM_PROMPT =
  "You are a helpful assistant. Use the get_weather tool when asked about weather.";
const USER_QUESTION = "What is the weather in London in celsius?";

async function runAgent(): Promise<void> {
  mockCallCounter = 0; // reset for clean test runs

  let client: MinimalOpenAIClient;

  if (process.env.OPENAI_API_KEY) {
    client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) as unknown as MinimalOpenAIClient;
    console.log("[openai] Using real OpenAI client.");
  } else {
    client = mockClient;
    console.log("[openai] OPENAI_API_KEY not set — using mock client.");
  }

  const messages: ChatCompletionMessageParam[] = [
    { role: "system", content: SYSTEM_PROMPT },
    { role: "user", content: USER_QUESTION },
  ];

  let totalInputTokens = 0;
  let totalOutputTokens = 0;
  let toolCallCount = 0;

  const agentSpan = tracer.startSpan("openai.agent.run");
  const ctx = trace.setSpan(trace.context ? trace.context : ({} as never), agentSpan);

  // We use a manual context propagation approach so async child spans nest correctly.
  await trace.getTracer("zradar.example.openai").startActiveSpan(
    "openai.agent.run",
    {},
    async (span) => {
      span.setAttribute("gen_ai.system", "openai");
      span.setAttribute("gen_ai.request.model", MODEL);

      try {
        // ---------------------------------------------------------------
        // Tool-calling loop
        // ---------------------------------------------------------------
        while (true) {
          const response = await client.chat.completions.create({
            model: MODEL,
            messages,
            tools: TOOLS,
            tool_choice: "auto",
          });

          const choice = response.choices[0];
          const assistantMessage = choice.message;

          // Accumulate token usage
          if (response.usage) {
            totalInputTokens += response.usage.prompt_tokens ?? 0;
            totalOutputTokens += response.usage.completion_tokens ?? 0;
          }

          if (choice.finish_reason === "tool_calls") {
            // Append the assistant's tool-call turn
            messages.push({
              role: "assistant",
              content: assistantMessage.content ?? null,
              tool_calls: assistantMessage.tool_calls,
            });

            // Execute each tool call inside a child span
            for (const tc of assistantMessage.tool_calls ?? []) {
              toolCallCount += 1;

              await tracer.startActiveSpan(
                `openai.tool.${tc.function.name}`,
                {},
                async (toolSpan) => {
                  toolSpan.setAttribute("tool.name", tc.function.name);
                  toolSpan.setAttribute("tool.input", tc.function.arguments);

                  const result = dispatchTool(
                    tc.function.name,
                    tc.function.arguments
                  );
                  console.log(
                    `[tool] ${tc.function.name}(${tc.function.arguments}) → ${result}`
                  );

                  toolSpan.end();

                  messages.push({
                    role: "tool",
                    tool_call_id: tc.id,
                    content: result,
                  });
                }
              );
            }
          } else if (choice.finish_reason === "stop") {
            const finalText = assistantMessage.content ?? "";
            console.log(`[agent] Final answer: ${finalText}`);
            break;
          } else {
            console.log(
              `[agent] Unexpected finish_reason=${choice.finish_reason}, stopping.`
            );
            break;
          }
        }
      } catch (err) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: String(err),
        });
        throw err;
      } finally {
        // Finalise parent span attributes
        span.setAttribute("agent.tool_calls", toolCallCount);
        span.setAttribute("gen_ai.usage.input_tokens", totalInputTokens);
        span.setAttribute("gen_ai.usage.output_tokens", totalOutputTokens);
        span.end();
      }
    }
  );

  // Flush pending spans before the process exits
  await sdk.shutdown();
  console.log(
    `[otel] Exported spans to ${ZRADAR_ENDPOINT} ` +
      `(tool_calls=${toolCallCount}, ` +
      `input_tokens=${totalInputTokens}, ` +
      `output_tokens=${totalOutputTokens})`
  );
}

runAgent().catch((err) => {
  console.error("[agent] Fatal error:", err);
  process.exit(1);
});
