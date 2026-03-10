export interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: string | number | null;
  method: string;
  params?: unknown;
}

export interface JsonRpcErrorObject {
  code: number;
  message: string;
  data?: unknown;
}

export interface JsonRpcResponse {
  jsonrpc: "2.0";
  id: string | number | null;
  result?: unknown;
  error?: JsonRpcErrorObject;
}

export interface OpenClawCallerContext {
  caller_id: string;
  session_id: string;
  channel_id: string;
  workspace_root?: string;
  scopes?: string[];
  display_name?: string;
  trace_ids?: Record<string, unknown>;
}

export interface OpenClawInspectionContext {
  caller: OpenClawCallerContext;
}

export interface GoalSpec {
  summary: string;
  details?: string;
}

export interface OpenClawInboundActionRequest {
  caller: OpenClawCallerContext;
  target_agent_id: string;
  capability: string;
  goal: GoalSpec;
  inputs?: Record<string, unknown>;
  contract_overrides?: Record<string, unknown>;
  execution_strategy?: Record<string, unknown>;
  schedule?: Record<string, unknown>;
  data_boundary?: string;
  workspace_write?: boolean;
  secret_access?: boolean;
  mutating?: boolean;
}

export interface OpenClawActionInspectParams {
  actionId: string;
  context: OpenClawInspectionContext;
}

export interface OpenClawAgentStatusParams {
  agentId: string;
  context: OpenClawInspectionContext;
}

export interface OpenClawBridgeOptions {
  socketPath?: string;
}
