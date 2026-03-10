import assert from "node:assert/strict";
import fs from "node:fs";
import * as http from "node:http";
import path from "node:path";
import os from "node:os";
import test from "node:test";
import {
  CrawfishUdsClient,
  OpenClawInboundBridge,
  type JsonRpcRequest,
} from "../src/index.js";

async function withUnixServer(
  handler: http.RequestListener,
  run: (socketPath: string) => Promise<void>,
): Promise<void> {
  const socketPath = path.join(
    os.tmpdir(),
    `crawfish-openclaw-${process.pid}-${Date.now()}.sock`,
  );
  await fs.promises.rm(socketPath, { force: true });
  const server = http.createServer(handler);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => resolve());
  });

  try {
    await run(socketPath);
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
    await fs.promises.rm(socketPath, { force: true });
  }
}

test("submit forwards inbound request to crawfish daemon", async () => {
  await withUnixServer(async (req, res) => {
    assert.equal(req.method, "POST");
    assert.equal(req.url, "/v1/inbound/openclaw/actions");
    const body = await readJson(req);
    assert.equal(body.caller.caller_id, "local_gateway");
    assert.equal(body.target_agent_id, "repo_reviewer");
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        action_id: "action-123",
        phase: "accepted",
        requester_id: "local_gateway-session",
        trace_refs: [],
      }),
    );
  }, async (socketPath) => {
    const bridge = new OpenClawInboundBridge(
      new CrawfishUdsClient({ socketPath }),
    );
    const response = await bridge.handleRequest({
      jsonrpc: "2.0",
      id: "1",
      method: "crawfish.action.submit",
      params: {
        caller: {
          caller_id: "local_gateway",
          session_id: "local_gateway-session",
          channel_id: "gateway",
        },
        target_agent_id: "repo_reviewer",
        capability: "repo.review",
        goal: { summary: "review" },
      },
    });
    assert.equal(response.error, undefined);
    assert.deepEqual(response.result, {
      action_id: "action-123",
      phase: "accepted",
      requester_id: "local_gateway-session",
      trace_refs: [],
    });
  });
});

test("inspect and events translate to scoped inbound endpoints", async () => {
  const paths: string[] = [];
  await withUnixServer(async (req, res) => {
    paths.push(req.url ?? "");
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  }, async (socketPath) => {
    const bridge = new OpenClawInboundBridge(
      new CrawfishUdsClient({ socketPath }),
    );
    const context = {
      caller: {
        caller_id: "local_gateway",
        session_id: "local_gateway-session",
        channel_id: "gateway",
      },
    };
    await bridge.handleRequest({
      jsonrpc: "2.0",
      id: "1",
      method: "crawfish.action.inspect",
      params: {
        actionId: "action-1",
        context,
      },
    });
    await bridge.handleRequest({
      jsonrpc: "2.0",
      id: "2",
      method: "crawfish.action.events",
      params: {
        actionId: "action-1",
        context,
      },
    });
    await bridge.handleRequest({
      jsonrpc: "2.0",
      id: "3",
      method: "crawfish.agent.status",
      params: {
        agentId: "repo_reviewer",
        context,
      },
    });
  });

  assert.deepEqual(paths, [
    "/v1/inbound/openclaw/actions/action-1/inspect",
    "/v1/inbound/openclaw/actions/action-1/events",
    "/v1/inbound/openclaw/agents/repo_reviewer/status",
  ]);
});

test("daemon unavailability maps to bounded rpc error", async () => {
  const bridge = new OpenClawInboundBridge(
    new CrawfishUdsClient({
      socketPath: path.join(
        os.tmpdir(),
        `crawfish-missing-${process.pid}-${Date.now()}.sock`,
      ),
    }),
  );
  const response = await bridge.handleRequest({
    jsonrpc: "2.0",
    id: "1",
    method: "crawfish.agent.status",
    params: {
      agentId: "repo_reviewer",
      context: {
        caller: {
          caller_id: "local_gateway",
          session_id: "local_gateway-session",
          channel_id: "gateway",
        },
      },
    },
  });
  assert.equal(response.result, undefined);
  assert.equal(response.error?.code, -32001);
});

test("invalid payload maps to invalid params", async () => {
  const bridge = new OpenClawInboundBridge(
    new CrawfishUdsClient({ socketPath: "/tmp/unused.sock" }),
  );
  const response = await bridge.handleRequest({
    jsonrpc: "2.0",
    id: "1",
    method: "crawfish.action.submit",
    params: {
      caller: {
        caller_id: "local_gateway",
      },
    },
  } as JsonRpcRequest);
  assert.equal(response.result, undefined);
  assert.equal(response.error?.code, -32602);
});

async function readJson(req: http.IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.from(chunk));
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
