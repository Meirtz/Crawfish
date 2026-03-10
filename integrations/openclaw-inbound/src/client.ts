import * as http from "node:http";
import {
  OpenClawAgentStatusParams,
  OpenClawBridgeOptions,
  OpenClawInboundActionRequest,
  OpenClawInspectionContext,
} from "./types.js";

export class CrawfishBridgeError extends Error {
  constructor(
    message: string,
    readonly code: number,
    readonly data?: unknown,
  ) {
    super(message);
    this.name = "CrawfishBridgeError";
  }
}

export class CrawfishUdsClient {
  readonly socketPath: string;

  constructor(options: OpenClawBridgeOptions = {}) {
    this.socketPath =
      options.socketPath ??
      process.env.CRAWFISH_SOCKET_PATH ??
      ".crawfish/run/crawfishd.sock";
  }

  submitAction(request: OpenClawInboundActionRequest): Promise<unknown> {
    return this.request("POST", "/v1/inbound/openclaw/actions", request);
  }

  inspectAction(params: { actionId: string; context: OpenClawInspectionContext }): Promise<unknown> {
    return this.request(
      "POST",
      `/v1/inbound/openclaw/actions/${encodeURIComponent(params.actionId)}/inspect`,
      params.context,
    );
  }

  actionEvents(params: { actionId: string; context: OpenClawInspectionContext }): Promise<unknown> {
    return this.request(
      "POST",
      `/v1/inbound/openclaw/actions/${encodeURIComponent(params.actionId)}/events`,
      params.context,
    );
  }

  agentStatus(params: OpenClawAgentStatusParams): Promise<unknown> {
    return this.request(
      "POST",
      `/v1/inbound/openclaw/agents/${encodeURIComponent(params.agentId)}/status`,
      params.context,
    );
  }

  private request(method: string, path: string, body?: unknown): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const payload = body === undefined ? undefined : JSON.stringify(body);
      const req = http.request(
        {
          socketPath: this.socketPath,
          path,
          method,
          headers:
            payload === undefined
              ? undefined
              : {
                  "content-type": "application/json",
                  "content-length": Buffer.byteLength(payload),
                },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
          res.on("end", () => {
            const text = Buffer.concat(chunks).toString("utf8");
            const parsed = text.length > 0 ? JSON.parse(text) : null;
            if ((res.statusCode ?? 500) >= 200 && (res.statusCode ?? 500) < 300) {
              resolve(parsed);
              return;
            }
            reject(
              new CrawfishBridgeError(
                typeof parsed?.error === "string"
                  ? parsed.error
                  : `daemon request failed with status ${res.statusCode ?? 500}`,
                mapStatusToErrorCode(res.statusCode ?? 500),
                parsed,
              ),
            );
          });
        },
      );

      req.on("error", (error) => {
        reject(
          new CrawfishBridgeError(
            `crawfish daemon unavailable: ${error.message}`,
            -32001,
          ),
        );
      });

      if (payload !== undefined) {
        req.write(payload);
      }
      req.end();
    });
  }
}

function mapStatusToErrorCode(statusCode: number): number {
  if (statusCode === 400) {
    return -32602;
  }
  if (statusCode === 403) {
    return -32003;
  }
  if (statusCode === 404) {
    return -32004;
  }
  return -32000;
}
