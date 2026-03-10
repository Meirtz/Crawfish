import * as readline from "node:readline";
import { CrawfishBridgeError, CrawfishUdsClient } from "./client.js";
import {
  JsonRpcErrorObject,
  JsonRpcRequest,
  JsonRpcResponse,
  OpenClawActionInspectParams,
  OpenClawAgentStatusParams,
  OpenClawInboundActionRequest,
} from "./types.js";

export class OpenClawInboundBridge {
  constructor(private readonly client: CrawfishUdsClient) {}

  async handleRequest(request: JsonRpcRequest): Promise<JsonRpcResponse> {
    if (request.jsonrpc !== "2.0") {
      return rpcError(request.id, -32600, "invalid JSON-RPC version");
    }

    try {
      switch (request.method) {
        case "crawfish.action.submit":
          return rpcResult(
            request.id,
            await this.client.submitAction(
              validateSubmitParams(request.params),
            ),
          );
        case "crawfish.action.inspect":
          return rpcResult(
            request.id,
            await this.client.inspectAction(
              validateActionInspectParams(request.params),
            ),
          );
        case "crawfish.action.events":
          return rpcResult(
            request.id,
            await this.client.actionEvents(
              validateActionInspectParams(request.params),
            ),
          );
        case "crawfish.agent.status":
          return rpcResult(
            request.id,
            await this.client.agentStatus(
              validateAgentStatusParams(request.params),
            ),
          );
        default:
          return rpcError(request.id, -32601, `method not found: ${request.method}`);
      }
    } catch (error) {
      if (error instanceof CrawfishBridgeError) {
        return rpcError(request.id, error.code, error.message, error.data);
      }
      if (error instanceof Error) {
        return rpcError(request.id, -32602, error.message);
      }
      return rpcError(request.id, -32000, "unexpected bridge failure");
    }
  }
}

export async function runStdioBridge(bridge: OpenClawInboundBridge): Promise<void> {
  const rl = readline.createInterface({
    input: process.stdin,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) {
      continue;
    }

    let request: JsonRpcRequest;
    try {
      request = JSON.parse(line) as JsonRpcRequest;
    } catch {
      process.stdout.write(
        `${JSON.stringify(rpcError(null, -32700, "invalid JSON payload"))}\n`,
      );
      continue;
    }

    const response = await bridge.handleRequest(request);
    process.stdout.write(`${JSON.stringify(response)}\n`);
  }
}

function validateSubmitParams(value: unknown): OpenClawInboundActionRequest {
  if (!isObject(value)) {
    throw new Error("submit params must be an object");
  }
  if (!isObject(value.caller)) {
    throw new Error("submit params must include caller context");
  }
  requireString(value.target_agent_id, "target_agent_id");
  requireString(value.capability, "capability");
  if (!isObject(value.goal) || typeof value.goal.summary !== "string") {
    throw new Error("goal.summary is required");
  }
  requireString(value.caller.caller_id, "caller.caller_id");
  requireString(value.caller.session_id, "caller.session_id");
  requireString(value.caller.channel_id, "caller.channel_id");
  return value as OpenClawInboundActionRequest;
}

function validateActionInspectParams(value: unknown): OpenClawActionInspectParams {
  if (!isObject(value)) {
    throw new Error("inspect params must be an object");
  }
  requireString(value.actionId, "actionId");
  if (!isObject(value.context) || !isObject(value.context.caller)) {
    throw new Error("inspect params must include context.caller");
  }
  requireString(value.context.caller.caller_id, "context.caller.caller_id");
  requireString(value.context.caller.session_id, "context.caller.session_id");
  requireString(value.context.caller.channel_id, "context.caller.channel_id");
  return value as OpenClawActionInspectParams;
}

function validateAgentStatusParams(value: unknown): OpenClawAgentStatusParams {
  if (!isObject(value)) {
    throw new Error("agent status params must be an object");
  }
  requireString(value.agentId, "agentId");
  if (!isObject(value.context) || !isObject(value.context.caller)) {
    throw new Error("agent status params must include context.caller");
  }
  requireString(value.context.caller.caller_id, "context.caller.caller_id");
  requireString(value.context.caller.session_id, "context.caller.session_id");
  requireString(value.context.caller.channel_id, "context.caller.channel_id");
  return value as OpenClawAgentStatusParams;
}

function requireString(value: unknown, field: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${field} is required`);
  }
}

function isObject(value: unknown): value is Record<string, any> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function rpcResult(id: JsonRpcRequest["id"], result: unknown): JsonRpcResponse {
  return {
    jsonrpc: "2.0",
    id,
    result,
  };
}

function rpcError(
  id: JsonRpcRequest["id"],
  code: number,
  message: string,
  data?: unknown,
): JsonRpcResponse {
  const error: JsonRpcErrorObject = { code, message };
  if (data !== undefined) {
    error.data = data;
  }
  return {
    jsonrpc: "2.0",
    id,
    error,
  };
}
