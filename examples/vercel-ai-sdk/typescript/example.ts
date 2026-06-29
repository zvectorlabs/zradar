import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { Resource } from "@opentelemetry/resources";
import { ATTR_SERVICE_NAME } from "@opentelemetry/semantic-conventions";
import { credentials } from "@grpc/grpc-js";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-node";
import { trace, SpanStatusCode } from "@opentelemetry/api";
import { generateText, tool } from "ai";
import { z } from "zod";

// ---------------------------------------------------------------------------
// OTel setup — must happen before any instrumented code runs
// ---------------------------------------------------------------------------
const ENDPOINT = process.env.ZRADAR_ENDPOINT ?? "localhost:4317";
const API_KEY = process.env.ZRADAR_API_KEY ?? "zk_dev_example";

const exporter = new OTLPTraceExporter({
  url: ENDPOINT,
  credentials: credentials.createInsecure(),
  metadata: (() => {
    const { Metadata } = require("@grpc/grpc-js");
    const meta = new Metadata();
    meta.set("authorization", `Bearer ${API_KEY}`);
    return meta;
  })(),
});

const sdk = new NodeSDK({
  resource: new Resource({
    [ATTR_SERVICE_NAME]: "example-vercel-ai-sdk",
  }),
  spanProcessors: [new BatchSpanProcessor(exporter)],
});

sdk.start();

const tracer = trace.getTracer("vercel-ai-sdk-example", "0.1.0");

// ---------------------------------------------------------------------------
// LLM provider selection
// ---------------------------------------------------------------------------
async function getModel() {
  if (process.env.OPENAI_API_KEY) {
    const { openai } = await import("@ai-sdk/openai");
    return openai("gpt-4o-mini");
  }

  // Mock model: returns a tool call on the first step, then a text response.
  const { MockLanguageModelV1 } = await import("ai/test");
  let callCount = 0;
  return new MockLanguageModelV1({
    doGenerate: async (options) => {
      callCount += 1;
      if (callCount === 1) {
        // First invocation — emit a tool call
        return {
          rawCall: { rawPrompt: options.prompt, rawSettings: {} },
          finishReason: "tool-calls" as const,
          usage: { promptTokens: 42, completionTokens: 18 },
          toolCalls: [
            {
              toolCallType: "function" as const,
              toolCallId: "call_001",
              toolName: "get_time",
              args: JSON.stringify({ timezone: "UTC" }),
            },
          ],
        };
      }
      // Second invocation — return final text
      return {
        rawCall: { rawPrompt: options.prompt, rawSettings: {} },
        finishReason: "stop" as const,
        usage: { promptTokens: 60, completionTokens: 24 },
        text: "The current time in UTC is 12:34:56.",
      };
    },
  });
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------
const getTimeTool = tool({
  description: "Returns the current time in the requested timezone.",
  parameters: z.object({
    timezone: z.string().describe("IANA timezone name, e.g. America/New_York"),
  }),
  execute: async ({ timezone }) => {
    // Hardcoded for the mock example — swap for real logic if desired.
    return `Current time in ${timezone}: 12:34:56`;
  },
});

// ---------------------------------------------------------------------------
// Main agent run
// ---------------------------------------------------------------------------
async function runAgent() {
  const model = await getModel();
  const modelId =
    (model as { modelId?: string }).modelId ?? "mock-language-model";

  return tracer.startActiveSpan("vercel_ai.agent.run", async (agentSpan) => {
    agentSpan.setAttribute("gen_ai.system", "vercel-ai");
    agentSpan.setAttribute("gen_ai.request.model", modelId);

    try {
      let toolCallCount = 0;

      const result = await generateText({
        model: model as Parameters<typeof generateText>[0]["model"],
        prompt: "What time is it right now in UTC?",
        maxSteps: 3,
        tools: { get_time: getTimeTool },
        onStepFinish: async (step) => {
          for (const tc of step.toolCalls ?? []) {
            toolCallCount += 1;
            // Child span per tool call
            const toolSpan = tracer.startSpan("vercel_ai.tool.get_time");
            toolSpan.setAttribute("tool.name", tc.toolName);
            toolSpan.setAttribute("tool.args", JSON.stringify(tc.args));
            toolSpan.end();
          }
        },
      });

      agentSpan.setAttribute("agent.tool_calls", toolCallCount);
      agentSpan.setAttribute(
        "gen_ai.usage.input_tokens",
        result.usage.promptTokens
      );
      agentSpan.setAttribute(
        "gen_ai.usage.output_tokens",
        result.usage.completionTokens
      );

      agentSpan.setStatus({ code: SpanStatusCode.OK });
      agentSpan.end();

      return result;
    } catch (err) {
      agentSpan.setStatus({
        code: SpanStatusCode.ERROR,
        message: String(err),
      });
      agentSpan.end();
      throw err;
    }
  });
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
(async () => {
  try {
    const result = await runAgent();
    console.log("Agent finished.");
    console.log("Text:", result.text);
    console.log("Usage:", result.usage);
  } finally {
    // Flush spans before the process exits.
    await sdk.shutdown();
  }
})();
