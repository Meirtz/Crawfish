use async_trait::async_trait;
use crawfish_core::ExecutionSurface;
use crawfish_types::{
    Action, ActionOutputs, CapabilityDescriptor, CostClass, ExecutorClass, LatencyClass,
    McpServerConfig, McpToolBinding, Mutability, RiskClass,
};
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION};
use serde_json::Value;
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct McpAdapter {
    server_name: String,
    binding: Option<McpToolBinding>,
    server_config: Option<McpServerConfig>,
    client: reqwest::Client,
}

#[derive(Debug, thiserror::Error)]
pub enum McpError {
    #[error("mcp transport is not configured for server {0}")]
    Unconfigured(String),
    #[error("timed out while connecting to mcp server {server}")]
    ConnectTimeout { server: String },
    #[error("timed out waiting for mcp response from {server}")]
    RequestTimeout { server: String },
    #[error("mcp server {server} closed before providing an endpoint event")]
    MissingEndpoint { server: String },
    #[error("mcp server {server} disconnected before completing the request")]
    Disconnected { server: String },
    #[error("mcp tool {tool} is not exposed by server {server}")]
    ToolNotFound { server: String, tool: String },
    #[error("mcp protocol error from {server}: {message}")]
    Protocol { server: String, message: String },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl McpAdapter {
    pub fn new(server_name: impl Into<String>) -> Self {
        Self {
            server_name: server_name.into(),
            binding: None,
            server_config: None,
            client: reqwest::Client::new(),
        }
    }

    pub fn configured(
        server_name: impl Into<String>,
        server_config: McpServerConfig,
        binding: McpToolBinding,
    ) -> anyhow::Result<Self> {
        let server_name = server_name.into();
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(server_config.connect_timeout_ms))
            .default_headers(build_headers(&server_config)?)
            .build()?;
        Ok(Self {
            server_name,
            binding: Some(binding),
            server_config: Some(server_config),
            client,
        })
    }

    pub fn describe_binding(&self, binding: &McpToolBinding) -> CapabilityDescriptor {
        CapabilityDescriptor {
            namespace: format!("mcp.{}", binding.capability),
            verbs: vec!["invoke".to_string()],
            executor_class: ExecutorClass::Agentic,
            mutability: Mutability::ReadOnly,
            risk_class: RiskClass::Medium,
            cost_class: CostClass::Standard,
            latency_class: LatencyClass::Interactive,
            approval_requirements: Vec::new(),
        }
    }

    async fn invoke_remote(&self, action: &Action) -> Result<ActionOutputs, McpError> {
        let binding = self
            .binding
            .clone()
            .ok_or_else(|| McpError::Unconfigured(self.server_name.clone()))?;
        let server_config = self
            .server_config
            .clone()
            .ok_or_else(|| McpError::Unconfigured(self.server_name.clone()))?;
        let mut session = SseSession::connect(
            Arc::new(self.client.clone()),
            self.server_name.clone(),
            server_config.clone(),
        )
        .await?;

        let _ = session
            .request(
                "initialize",
                serde_json::json!({
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "crawfish", "version": env!("CARGO_PKG_VERSION")},
                }),
            )
            .await?;

        let tools = session.request("tools/list", serde_json::json!({})).await?;
        let tool_exists = tools
            .get("tools")
            .and_then(|value| value.as_array())
            .map(|tools| {
                tools.iter().any(|tool| {
                    tool.get("name").and_then(|name| name.as_str()) == Some(binding.tool.as_str())
                })
            })
            .unwrap_or(false);
        if !tool_exists {
            return Err(McpError::ToolNotFound {
                server: self.server_name.clone(),
                tool: binding.tool,
            });
        }

        let result = session
            .request(
                "tools/call",
                serde_json::json!({
                    "name": binding.tool,
                    "arguments": action.inputs,
                }),
            )
            .await?;

        Ok(ActionOutputs {
            summary: Some(extract_call_summary(&result)),
            artifacts: Vec::new(),
            metadata: std::collections::BTreeMap::from([
                (
                    "execution_surface".to_string(),
                    serde_json::json!(self.server_name),
                ),
                ("mcp_tool".to_string(), serde_json::json!(binding.tool)),
                ("mcp_result".to_string(), result),
            ]),
        })
    }
}

#[async_trait]
impl ExecutionSurface for McpAdapter {
    fn name(&self) -> &str {
        &self.server_name
    }

    fn supports(&self, capability: &CapabilityDescriptor) -> bool {
        capability.namespace.starts_with("mcp.")
    }

    async fn run(&self, action: &Action) -> anyhow::Result<ActionOutputs> {
        if self.binding.is_some() && self.server_config.is_some() {
            return self.invoke_remote(action).await.map_err(Into::into);
        }

        Ok(ActionOutputs {
            summary: Some(format!(
                "MCP execution stub for action {} on {}",
                action.id, self.server_name
            )),
            artifacts: Vec::new(),
            metadata: std::collections::BTreeMap::from([(
                "execution_surface".to_string(),
                serde_json::json!(self.server_name),
            )]),
        })
    }
}

fn build_headers(config: &McpServerConfig) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    for (key, value) in &config.headers {
        headers.insert(HeaderName::from_str(key)?, HeaderValue::from_str(value)?);
    }
    if let Some(token_env) = &config.auth_token_env {
        if let Ok(token) = env::var(token_env) {
            let bearer = format!("Bearer {token}");
            headers.insert(AUTHORIZATION, HeaderValue::from_str(&bearer)?);
        }
    }
    Ok(headers)
}

fn extract_call_summary(result: &Value) -> String {
    if let Some(text) = result
        .get("content")
        .and_then(|value| value.as_array())
        .and_then(|items| items.first())
        .and_then(|item| item.get("text"))
        .and_then(|text| text.as_str())
    {
        return text.to_string();
    }

    if let Some(structured) = result.get("structuredContent") {
        return serde_json::to_string_pretty(structured).unwrap_or_else(|_| structured.to_string());
    }

    serde_json::to_string_pretty(result).unwrap_or_else(|_| result.to_string())
}

struct SseSession {
    server_name: String,
    client: Arc<reqwest::Client>,
    post_url: String,
    request_timeout_ms: u64,
    next_id: u64,
    message_rx: mpsc::UnboundedReceiver<Value>,
    _reader: tokio::task::JoinHandle<()>,
}

impl SseSession {
    async fn connect(
        client: Arc<reqwest::Client>,
        server_name: String,
        server_config: McpServerConfig,
    ) -> Result<Self, McpError> {
        let connect_future = client.get(&server_config.url).send();
        let response = tokio::time::timeout(
            Duration::from_millis(server_config.connect_timeout_ms),
            connect_future,
        )
        .await
        .map_err(|_| McpError::ConnectTimeout {
            server: server_name.clone(),
        })?
        .map_err(|error| McpError::Internal(error.into()))?;

        if !response.status().is_success() {
            return Err(McpError::Protocol {
                server: server_name.clone(),
                message: format!("sse handshake failed with {}", response.status()),
            });
        }

        let base_url = response.url().clone();
        let (endpoint_tx, endpoint_rx) = oneshot::channel::<Result<String, McpError>>();
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        let server_for_task = server_name.clone();
        let reader = tokio::spawn(async move {
            read_sse_stream(
                server_for_task,
                base_url,
                response.bytes_stream(),
                endpoint_tx,
                message_tx,
            )
            .await;
        });

        let post_url = tokio::time::timeout(
            Duration::from_millis(server_config.connect_timeout_ms),
            endpoint_rx,
        )
        .await
        .map_err(|_| McpError::MissingEndpoint {
            server: server_name.clone(),
        })?
        .map_err(|_| McpError::Disconnected {
            server: server_name.clone(),
        })??;

        Ok(Self {
            server_name,
            client,
            post_url,
            request_timeout_ms: server_config.request_timeout_ms,
            next_id: 1,
            message_rx,
            _reader: reader,
        })
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, McpError> {
        let id = self.next_id;
        self.next_id += 1;
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.client
            .post(&self.post_url)
            .json(&payload)
            .send()
            .await
            .map_err(|error| McpError::Internal(error.into()))?;

        let response = tokio::time::timeout(
            Duration::from_millis(self.request_timeout_ms),
            self.recv_message_for_id(id),
        )
        .await
        .map_err(|_| McpError::RequestTimeout {
            server: self.server_name.clone(),
        })??;

        if let Some(error) = response.get("error") {
            return Err(McpError::Protocol {
                server: self.server_name.clone(),
                message: error.to_string(),
            });
        }

        response
            .get("result")
            .cloned()
            .ok_or_else(|| McpError::Protocol {
                server: self.server_name.clone(),
                message: "missing result field in MCP response".to_string(),
            })
    }

    async fn recv_message_for_id(&mut self, id: u64) -> Result<Value, McpError> {
        while let Some(message) = self.message_rx.recv().await {
            if message.get("id").and_then(|value| value.as_u64()) == Some(id) {
                return Ok(message);
            }
        }

        Err(McpError::Disconnected {
            server: self.server_name.clone(),
        })
    }
}

async fn read_sse_stream<S>(
    server_name: String,
    base_url: reqwest::Url,
    mut stream: S,
    endpoint_tx: oneshot::Sender<Result<String, McpError>>,
    message_tx: mpsc::UnboundedSender<Value>,
) where
    S: futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin,
{
    let mut endpoint_tx = Some(endpoint_tx);
    let mut buffer = String::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                buffer.push_str(&String::from_utf8_lossy(&bytes));
                let normalized = buffer.replace('\r', "");
                let mut parts = normalized
                    .split("\n\n")
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                if !normalized.ends_with("\n\n") {
                    buffer = parts.pop().unwrap_or_default();
                } else {
                    buffer.clear();
                }

                for block in parts {
                    if let Some(event) = parse_sse_event(&block) {
                        match event.kind.as_deref() {
                            Some("endpoint") => {
                                if let Some(sender) = endpoint_tx.take() {
                                    let resolved = base_url
                                        .join(&event.data)
                                        .map(|url| url.to_string())
                                        .map_err(|error| McpError::Protocol {
                                            server: server_name.clone(),
                                            message: error.to_string(),
                                        });
                                    let _ = sender.send(resolved);
                                }
                            }
                            Some("message") | None => {
                                if let Ok(message) = serde_json::from_str::<Value>(&event.data) {
                                    let _ = message_tx.send(message);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            Err(error) => {
                if let Some(sender) = endpoint_tx.take() {
                    let _ = sender.send(Err(McpError::Protocol {
                        server: server_name.clone(),
                        message: error.to_string(),
                    }));
                }
                return;
            }
        }
    }

    if let Some(sender) = endpoint_tx.take() {
        let _ = sender.send(Err(McpError::MissingEndpoint {
            server: server_name,
        }));
    }
}

struct ParsedSseEvent {
    kind: Option<String>,
    data: String,
}

fn parse_sse_event(block: &str) -> Option<ParsedSseEvent> {
    let mut kind = None;
    let mut data = Vec::new();
    for line in block.lines() {
        if let Some(value) = line.strip_prefix("event:") {
            kind = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start().to_string());
        }
    }

    if data.is_empty() {
        None
    } else {
        Some(ParsedSseEvent {
            kind,
            data: data.join("\n"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        response::sse::{Event, Sse},
        routing::{get, post},
        Json, Router,
    };
    use futures_util::stream;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    struct MockState {
        sessions: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<String>>>>,
        next_session: Arc<AtomicUsize>,
        mode: MockMode,
    }

    #[derive(Clone)]
    enum MockMode {
        Normal,
        SlowCall,
        NoEndpoint,
    }

    #[derive(serde::Deserialize)]
    struct SessionQuery {
        session: String,
    }

    #[tokio::test]
    async fn configured_adapter_invokes_tool_over_sse() {
        let server = spawn_mock_server(MockMode::Normal).await;
        let adapter = McpAdapter::configured(
            "hero_remote",
            McpServerConfig {
                transport: crawfish_types::McpTransportKind::Sse,
                url: format!("{server}/sse"),
                auth_token_env: None,
                headers: std::collections::BTreeMap::new(),
                connect_timeout_ms: 5_000,
                request_timeout_ms: 5_000,
            },
            McpToolBinding {
                capability: "ci.runs.inspect".to_string(),
                server: "hero_remote".to_string(),
                tool: "ci_runs_inspect".to_string(),
                default_scope: vec!["ci:read".to_string()],
            },
        )
        .unwrap();

        let outputs = adapter
            .run(&Action {
                id: "action-1".to_string(),
                target_agent_id: "ci_triage".to_string(),
                requester: crawfish_types::RequesterRef {
                    kind: crawfish_types::RequesterKind::System,
                    id: "test".to_string(),
                },
                initiator_owner: crawfish_types::OwnerRef {
                    kind: crawfish_types::OwnerKind::ServiceAccount,
                    id: "test".to_string(),
                    display_name: None,
                },
                counterparty_refs: Vec::new(),
                goal: crawfish_types::GoalSpec {
                    summary: "inspect ci".to_string(),
                    details: None,
                },
                capability: "ci.triage".to_string(),
                inputs: std::collections::BTreeMap::from([(
                    "run_url".to_string(),
                    serde_json::json!("https://ci.example/run/1"),
                )]),
                contract: crawfish_types::ExecutionContract::default(),
                execution_strategy: None,
                grant_refs: Vec::new(),
                lease_ref: None,
                encounter_ref: None,
                audit_receipt_ref: None,
                data_boundary: "owner_local".to_string(),
                schedule: crawfish_types::ScheduleSpec::default(),
                phase: crawfish_types::ActionPhase::Accepted,
                created_at: "0".to_string(),
                started_at: None,
                finished_at: None,
                checkpoint_ref: None,
                continuity_mode: None,
                degradation_profile: None,
                failure_reason: None,
                failure_code: None,
                selected_executor: None,
                recovery_stage: None,
                lock_detail: None,
                external_refs: Vec::new(),
                outputs: ActionOutputs::default(),
            })
            .await
            .unwrap();

        assert!(outputs
            .summary
            .as_deref()
            .unwrap_or_default()
            .contains("CI run metadata loaded"));
    }

    #[tokio::test]
    async fn configured_adapter_times_out_when_server_does_not_reply() {
        let server = spawn_mock_server(MockMode::SlowCall).await;
        let adapter = McpAdapter::configured(
            "hero_remote",
            McpServerConfig {
                transport: crawfish_types::McpTransportKind::Sse,
                url: format!("{server}/sse"),
                auth_token_env: None,
                headers: std::collections::BTreeMap::new(),
                connect_timeout_ms: 5_000,
                request_timeout_ms: 50,
            },
            McpToolBinding {
                capability: "ci.runs.inspect".to_string(),
                server: "hero_remote".to_string(),
                tool: "ci_runs_inspect".to_string(),
                default_scope: vec!["ci:read".to_string()],
            },
        )
        .unwrap();

        let error = adapter
            .invoke_remote(&sample_action())
            .await
            .expect_err("timeout");
        assert!(matches!(error, McpError::RequestTimeout { .. }));
    }

    #[tokio::test]
    async fn configured_adapter_requires_endpoint_event() {
        let server = spawn_mock_server(MockMode::NoEndpoint).await;
        let adapter = McpAdapter::configured(
            "hero_remote",
            McpServerConfig {
                transport: crawfish_types::McpTransportKind::Sse,
                url: format!("{server}/sse"),
                auth_token_env: None,
                headers: std::collections::BTreeMap::new(),
                connect_timeout_ms: 500,
                request_timeout_ms: 500,
            },
            McpToolBinding {
                capability: "ci.runs.inspect".to_string(),
                server: "hero_remote".to_string(),
                tool: "ci_runs_inspect".to_string(),
                default_scope: vec!["ci:read".to_string()],
            },
        )
        .unwrap();

        let error = adapter
            .invoke_remote(&sample_action())
            .await
            .expect_err("endpoint missing");
        assert!(matches!(error, McpError::MissingEndpoint { .. }));
    }

    fn sample_action() -> Action {
        Action {
            id: "action-1".to_string(),
            target_agent_id: "ci_triage".to_string(),
            requester: crawfish_types::RequesterRef {
                kind: crawfish_types::RequesterKind::System,
                id: "test".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::ServiceAccount,
                id: "test".to_string(),
                display_name: None,
            },
            counterparty_refs: Vec::new(),
            goal: crawfish_types::GoalSpec {
                summary: "inspect ci".to_string(),
                details: None,
            },
            capability: "ci.triage".to_string(),
            inputs: std::collections::BTreeMap::new(),
            contract: crawfish_types::ExecutionContract::default(),
            execution_strategy: None,
            grant_refs: Vec::new(),
            lease_ref: None,
            encounter_ref: None,
            audit_receipt_ref: None,
            data_boundary: "owner_local".to_string(),
            schedule: crawfish_types::ScheduleSpec::default(),
            phase: crawfish_types::ActionPhase::Accepted,
            created_at: "0".to_string(),
            started_at: None,
            finished_at: None,
            checkpoint_ref: None,
            continuity_mode: None,
            degradation_profile: None,
            failure_reason: None,
            failure_code: None,
            selected_executor: None,
            recovery_stage: None,
            lock_detail: None,
            external_refs: Vec::new(),
            outputs: ActionOutputs::default(),
        }
    }

    async fn spawn_mock_server(mode: MockMode) -> String {
        let state = MockState {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session: Arc::new(AtomicUsize::new(1)),
            mode,
        };

        let app = Router::new()
            .route("/sse", get(mock_sse))
            .route("/messages", post(mock_messages))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    async fn mock_sse(
        State(state): State<MockState>,
    ) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
        let session_id = format!(
            "session-{}",
            state.next_session.fetch_add(1, Ordering::SeqCst)
        );
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        state.sessions.lock().await.insert(session_id.clone(), tx);

        let send_endpoint = !matches!(state.mode, MockMode::NoEndpoint);
        let endpoint_session = session_id.clone();
        let initial = stream::once(async move {
            if send_endpoint {
                Ok(Event::default()
                    .event("endpoint")
                    .data(format!("/messages?session={endpoint_session}")))
            } else {
                Ok(Event::default().comment("no endpoint"))
            }
        });
        let rest = stream::unfold(rx, |mut rx| async move {
            rx.recv()
                .await
                .map(|payload| (Ok(Event::default().event("message").data(payload)), rx))
        });
        Sse::new(initial.chain(rest))
    }

    async fn mock_messages(
        State(state): State<MockState>,
        Query(query): Query<SessionQuery>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        let sender = state.sessions.lock().await.get(&query.session).cloned();
        if let Some(sender) = sender {
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            let method = payload
                .get("method")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string();
            let mode = state.mode.clone();
            tokio::spawn(async move {
                if matches!(mode, MockMode::SlowCall) && method == "tools/call" {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }

                let response = match method.as_str() {
                    "initialize" => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {"protocolVersion": "2024-11-05"}
                    }),
                    "tools/list" => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {"tools": [{"name": "ci_runs_inspect"}]}
                    }),
                    "tools/call" => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "content": [{"type": "text", "text": "CI run metadata loaded"}],
                            "structuredContent": {"status": "failed", "provider": "github_actions"}
                        }
                    }),
                    _ => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"message": "unknown method"}
                    }),
                };

                let _ = sender.send(response.to_string());
            });
        }

        Json(serde_json::json!({"accepted": true}))
    }
}
