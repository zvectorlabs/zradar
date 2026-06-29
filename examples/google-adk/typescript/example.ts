/**
 * Google ADK Exchange Rate Agent — zradar OpenTelemetry example (TypeScript).
 *
 * When GOOGLE_API_KEY is set the agent runs against the real Gemini API.
 * When it is absent the script falls back to a manual span simulation so
 * that the correct OTel shape is always emitted without needing credentials.
 *
 * NOTE: The manual simulation path is a placeholder until the TypeScript ADK
 * SDK stabilises its InMemoryRunner / test utilities (tracked upstream at
 * https://github.com/google/adk-node).
 *
 * Usage:
 *   pnpm start
 *   GOOGLE_API_KEY=your-key pnpm start
 */

import { NodeSDK } from "@opentelemetry/sdk-node";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-grpc";
import { Resource } from "@opentelemetry/resources";
import {
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
} from "@opentelemetry/semantic-conventions";
import {
  trace,
  Tracer,
  SpanStatusCode,
  context,
  SpanKind,
} from "@opentelemetry/api";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-node";

// ---------------------------------------------------------------------------
// OTel bootstrap
// ---------------------------------------------------------------------------

const ZRADAR_ENDPOINT = process.env.ZRADAR_ENDPOINT ?? "localhost:4317";
const ZRADAR_API_KEY = process.env.ZRADAR_API_KEY ?? "zk_dev_example";

const exporter = new OTLPTraceExporter({
  url: `http://${ZRADAR_ENDPOINT}`,
  headers: { "x-zradar-api-key": ZRADAR_API_KEY },
});

const resource = new Resource({
  [ATTR_SERVICE_NAME]: "google-adk-exchange-rate-agent",
  [ATTR_SERVICE_VERSION]: "0.1.0",
});

const sdk = new NodeSDK({
  resource,
  spanProcessors: [new BatchSpanProcessor(exporter)],
});

sdk.start();

const tracer: Tracer = trace.getTracer("zradar.examples.google_adk");

// ---------------------------------------------------------------------------
// Tool: getExchangeRate
// ---------------------------------------------------------------------------

const RATES: Record<string, number> = {
  "USD/EUR": 0.92,
  "EUR/USD": 1.09,
  "USD/GBP": 0.79,
  "GBP/USD": 1.27,
  "USD/JPY": 157.4,
  "JPY/USD": 0.00635,
};

function getExchangeRate(fromCurrency: string, toCurrency: string): string {
  const from = fromCurrency.toUpperCase().trim();
  const to = toCurrency.toUpperCase().trim();

  if (from === to) {
    return `1 ${from} = 1 ${to} (same currency)`;
  }

  const key = `${from}/${to}`;
  const rate = RATES[key];
  if (rate === undefined) {
    return (
      `Exchange rate for ${from}/${to} is not available in this demo. ` +
      "Try USD/EUR, EUR/USD, USD/GBP, GBP/USD, USD/JPY, or JPY/USD."
    );
  }
  return `1 ${from} = ${rate} ${to}`;
}

// ---------------------------------------------------------------------------
// ADK agent runner (real API path)
// ---------------------------------------------------------------------------

async function runWithRealApi(userMessage: string): Promise<string> {
  // Dynamic import so the module is only loaded when the API key is present.
  // The @google/adk package exposes an Agent class and a Runner.
  const { Agent, Runner, InMemorySessionService } = await import("@google/adk");

  const agent = new Agent({
    name: "exchange_rate_agent",
    model: "gemini-2.0-flash",
    instruction:
      "You are a helpful currency assistant. " +
      "Use the getExchangeRate tool to answer questions about exchange rates. " +
      "Always call the tool rather than guessing the rate.",
    tools: [
      {
        name: "getExchangeRate",
        description: "Return the current exchange rate between two currencies.",
        parameters: {
          type: "object",
          properties: {
            fromCurrency: {
              type: "string",
              description: "ISO 4217 source currency code, e.g. USD",
            },
            toCurrency: {
              type: "string",
              description: "ISO 4217 target currency code, e.g. EUR",
            },
          },
          required: ["fromCurrency", "toCurrency"],
        },
        execute: async (params: { fromCurrency: string; toCurrency: string }) => {
          return tracer.startActiveSpan(
            "google_adk.tool.get_exchange_rate",
            { kind: SpanKind.INTERNAL },
            (toolSpan) => {
              toolSpan.setAttribute("tool.name", "getExchangeRate");
              toolSpan.setAttribute(
                "tool.input",
                JSON.stringify(params)
              );
              const result = getExchangeRate(
                params.fromCurrency,
                params.toCurrency
              );
              toolSpan.setAttribute("tool.output", result);
              toolSpan.end();
              return result;
            }
          );
        },
      },
    ],
  });

  const sessionService = new InMemorySessionService();
  const runner = new Runner({
    agent,
    appName: "zradar_google_adk_example",
    sessionService,
  });

  const session = await sessionService.createSession({
    appName: "zradar_google_adk_example",
    userId: "demo_user",
  });

  let finalResponse = "";
  for await (const event of runner.runAsync({
    userId: "demo_user",
    sessionId: session.id,
    newMessage: { role: "user", parts: [{ text: userMessage }] },
  })) {
    if (event.isFinalResponse?.() && event.content?.parts) {
      finalResponse = event.content.parts
        .filter((p: { text?: string }) => p.text)
        .map((p: { text: string }) => p.text)
        .join("");
    }
  }

  return finalResponse || "(no response)";
}

// ---------------------------------------------------------------------------
// Span simulation (no-API-key path)
//
// NOTE: This is a placeholder until @google/adk ships stable InMemoryRunner /
// test utilities. The spans produced here are structurally identical to what
// the real path emits, so zradar ingestion and the expected_spans.json contract
// are both satisfied.
// ---------------------------------------------------------------------------

async function runWithSimulatedSpans(userMessage: string): Promise<string> {
  // Simulate the tool call synchronously, wrapped in its own span.
  const toolResult = await new Promise<string>((resolve) => {
    tracer.startActiveSpan(
      "google_adk.tool.get_exchange_rate",
      { kind: SpanKind.INTERNAL },
      (toolSpan) => {
        const params = { fromCurrency: "USD", toCurrency: "EUR" };
        toolSpan.setAttribute("tool.name", "getExchangeRate");
        toolSpan.setAttribute("tool.input", JSON.stringify(params));
        const result = getExchangeRate(params.fromCurrency, params.toCurrency);
        toolSpan.setAttribute("tool.output", result);
        toolSpan.end();
        resolve(result);
      }
    );
  });

  return (
    `[simulated — no GOOGLE_API_KEY] ` +
    `Agent would have answered: "${toolResult}"`
  );
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const userMessage = "What is the exchange rate from USD to EUR?";
  const useRealApi = Boolean(process.env.GOOGLE_API_KEY);

  const agentSpan = tracer.startSpan("google_adk.agent.run", {
    kind: SpanKind.INTERNAL,
    attributes: {
      "gen_ai.system": "google-adk",
      "gen_ai.request.model": "gemini-2.0-flash",
      "agent.name": "exchange_rate_agent",
      "gen_ai.prompt": userMessage,
    },
  });

  // Run everything inside the agent span's context so child spans are
  // correctly parented.
  const ctx = trace.setSpan(context.active(), agentSpan);

  try {
    let response: string;

    await context.with(ctx, async () => {
      if (useRealApi) {
        console.log("Using real Gemini API…");
        response = await runWithRealApi(userMessage);
      } else {
        console.log(
          "GOOGLE_API_KEY not set — simulating spans " +
            "(no Gemini API call)."
        );
        response = await runWithSimulatedSpans(userMessage);
      }

      agentSpan.setAttribute("gen_ai.completion", response!);
      console.log(`\nAgent response: ${response}`);
    });
  } catch (err) {
    agentSpan.recordException(err as Error);
    agentSpan.setStatus({ code: SpanStatusCode.ERROR, message: String(err) });
    throw err;
  } finally {
    agentSpan.end();
  }

  // Flush before exit.
  await sdk.shutdown();
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
