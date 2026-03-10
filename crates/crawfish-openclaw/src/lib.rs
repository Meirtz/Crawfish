use async_trait::async_trait;
use crawfish_core::{now_timestamp, ExecutionSurface, SurfaceActionEvent, SurfaceExecutionResult};
use crawfish_types::{
    Action, ActionOutputs, ArtifactRef, CapabilityDescriptor, CostClass, ExecutorClass,
    ExternalRef, LatencyClass, Mutability, OpenClawBinding, OpenClawSessionMode,
    OpenClawWorkspacePolicy, PatchPlanArtifact, PatchPlanStep, RiskClass,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};

#[derive(Debug, Clone)]
pub struct OpenClawAdapter {
    binding: OpenClawBinding,
    state_dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenClawError {
    #[error("openclaw auth token env var is missing: {0}")]
    MissingAuthEnv(String),
    #[error("openclaw sticky session mode is not supported in p1b")]
    UnsupportedSessionMode,
    #[error("openclaw_managed workspace mode is not supported in p1b")]
    UnsupportedWorkspaceMode,
    #[error("failed to connect to openclaw gateway: {0}")]
    Connect(String),
    #[error("openclaw protocol error: {0}")]
    Protocol(String),
    #[error("openclaw run failed: {0}")]
    RunFailed(String),
}

impl OpenClawAdapter {
    pub fn new(binding: OpenClawBinding, state_dir: PathBuf) -> Self {
        Self { binding, state_dir }
    }

    pub fn describe_binding(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            namespace: format!("openclaw.{}", self.binding.target_agent),
            verbs: vec!["agent".to_string(), "wait".to_string()],
            executor_class: ExecutorClass::Agentic,
            mutability: Mutability::ProposalOnly,
            risk_class: RiskClass::Medium,
            cost_class: CostClass::Standard,
            latency_class: LatencyClass::LongRunning,
            approval_requirements: Vec::new(),
        }
    }

    async fn invoke_remote(
        &self,
        action: &Action,
    ) -> Result<SurfaceExecutionResult, OpenClawError> {
        self.validate_binding()?;
        let token = env::var(&self.binding.auth_ref)
            .map_err(|_| OpenClawError::MissingAuthEnv(self.binding.auth_ref.clone()))?;
        let mut request = self
            .binding
            .gateway_url
            .clone()
            .into_client_request()
            .map_err(|error| OpenClawError::Connect(error.to_string()))?;
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {token}")
                .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
                .map_err(|error| OpenClawError::Connect(error.to_string()))?,
        );

        let (mut socket, _response) = connect_async(request)
            .await
            .map_err(|error| OpenClawError::Connect(error.to_string()))?;

        let mut next_id = 1u64;
        let mut buffered = Vec::new();
        self.send_request(&mut socket, next_id, "connect", self.connect_params(action))
            .await?;
        let connect_result = self
            .wait_for_response(&mut socket, next_id, None, &mut buffered)
            .await?;
        if connect_result.get("challenge").is_some() {
            return Err(OpenClawError::Protocol(
                "gateway connect challenge is not supported in p1b".to_string(),
            ));
        }
        let session_key = connect_result
            .get("sessionKey")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("crawfish:{}", action.id));

        next_id += 1;
        self.send_request(
            &mut socket,
            next_id,
            "agent",
            self.agent_params(action, &session_key),
        )
        .await?;
        let agent_result = self
            .wait_for_response(&mut socket, next_id, None, &mut buffered)
            .await?;
        let run_id = extract_run_id(&agent_result).ok_or_else(|| {
            OpenClawError::Protocol("agent response did not include runId".to_string())
        })?;

        buffered.push(SurfaceActionEvent {
            event_type: "openclaw_run_started".to_string(),
            payload: json!({
                "run_id": run_id,
                "timestamp": now_timestamp(),
                "gateway_event_kind": "agent",
                "session_key": session_key,
                "target_agent": self.binding.target_agent,
                "raw": agent_result,
            }),
        });

        next_id += 1;
        self.send_request(
            &mut socket,
            next_id,
            "agent.wait",
            json!({
                "runId": run_id,
            }),
        )
        .await?;
        let wait_result = self
            .wait_for_response(&mut socket, next_id, Some(&run_id), &mut buffered)
            .await?;

        let final_text = extract_final_text(&wait_result);
        let final_artifact = patch_plan_artifact_from_result(action, &final_text, &wait_result);
        let json_ref = write_json_artifact(
            &self.state_dir,
            &action.id,
            "patch_plan.json",
            &final_artifact,
        )
        .await
        .map_err(|error| OpenClawError::Protocol(error.to_string()))?;
        let markdown_ref = write_text_artifact(
            &self.state_dir,
            &action.id,
            "patch_plan.md",
            &build_patch_plan_markdown(&final_artifact, action, &final_text),
        )
        .await
        .map_err(|error| OpenClawError::Protocol(error.to_string()))?;

        let terminal_error = extract_terminal_error(&wait_result);
        if let Some(message) = terminal_error {
            buffered.push(SurfaceActionEvent {
                event_type: "openclaw_run_failed".to_string(),
                payload: json!({
                    "run_id": run_id,
                    "timestamp": now_timestamp(),
                    "gateway_event_kind": "agent.wait",
                    "message": message,
                    "raw": wait_result,
                }),
            });
            return Err(OpenClawError::RunFailed(message));
        }

        buffered.push(SurfaceActionEvent {
            event_type: "openclaw_run_completed".to_string(),
            payload: json!({
                "run_id": run_id,
                "timestamp": now_timestamp(),
                "gateway_event_kind": "agent.wait",
                "raw": wait_result,
            }),
        });

        Ok(SurfaceExecutionResult {
            outputs: ActionOutputs {
                summary: Some(format!(
                    "OpenClaw produced a proposal plan for {} target files",
                    final_artifact.target_files.len()
                )),
                artifacts: vec![json_ref, markdown_ref],
                metadata: BTreeMap::from([
                    ("execution_surface".to_string(), json!("openclaw")),
                    ("openclaw_run_id".to_string(), json!(run_id)),
                    ("openclaw_result".to_string(), wait_result),
                ]),
            },
            external_refs: vec![
                ExternalRef {
                    kind: "openclaw.gateway_url".to_string(),
                    value: self.binding.gateway_url.clone(),
                    endpoint: Some(self.binding.gateway_url.clone()),
                },
                ExternalRef {
                    kind: "openclaw.target_agent".to_string(),
                    value: self.binding.target_agent.clone(),
                    endpoint: None,
                },
                ExternalRef {
                    kind: "openclaw.run_id".to_string(),
                    value: run_id,
                    endpoint: None,
                },
                ExternalRef {
                    kind: "openclaw.session_key".to_string(),
                    value: session_key,
                    endpoint: None,
                },
            ],
            events: buffered,
        })
    }

    fn validate_binding(&self) -> Result<(), OpenClawError> {
        if matches!(self.binding.session_mode, OpenClawSessionMode::Sticky) {
            return Err(OpenClawError::UnsupportedSessionMode);
        }
        if matches!(
            self.binding.workspace_policy,
            OpenClawWorkspacePolicy::OpenclawManaged
        ) {
            return Err(OpenClawError::UnsupportedWorkspaceMode);
        }
        Ok(())
    }

    fn connect_params(&self, action: &Action) -> Value {
        json!({
            "client": {
                "name": "crawfish",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "sessionKey": format!("crawfish:{}", action.id),
        })
    }

    fn agent_params(&self, action: &Action, session_key: &str) -> Value {
        let workspace_root = action
            .inputs
            .get("workspace_root")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        json!({
            "agentId": self.binding.target_agent,
            "sessionKey": session_key,
            "message": build_agent_prompt(action),
            "workspaceRoot": workspace_root,
            "idempotencyKey": action.id,
        })
    }

    async fn send_request(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        id: u64,
        method: &str,
        params: Value,
    ) -> Result<(), OpenClawError> {
        let message = json!({
            "type": "req",
            "id": id.to_string(),
            "method": method,
            "params": params,
        });
        socket
            .send(Message::Text(message.to_string().into()))
            .await
            .map_err(|error| OpenClawError::Protocol(error.to_string()))
    }

    async fn wait_for_response(
        &self,
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        expected_id: u64,
        run_id: Option<&str>,
        buffered: &mut Vec<SurfaceActionEvent>,
    ) -> Result<Value, OpenClawError> {
        while let Some(message) = socket.next().await {
            let message = message.map_err(|error| OpenClawError::Protocol(error.to_string()))?;
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text)
                .map_err(|error| OpenClawError::Protocol(error.to_string()))?;
            match value.get("type").and_then(Value::as_str) {
                Some("res")
                    if value.get("id").and_then(Value::as_str)
                        == Some(&expected_id.to_string()) =>
                {
                    if value.get("ok").and_then(Value::as_bool) == Some(false) {
                        return Err(OpenClawError::Protocol(
                            value
                                .get("error")
                                .and_then(|error| error.get("message"))
                                .and_then(Value::as_str)
                                .unwrap_or("gateway returned an error")
                                .to_string(),
                        ));
                    }
                    return Ok(value.get("result").cloned().unwrap_or(Value::Null));
                }
                Some("event") => buffered.push(normalize_event(&value, run_id)),
                _ => {}
            }
        }

        Err(OpenClawError::Protocol(
            "gateway disconnected before the response completed".to_string(),
        ))
    }
}

#[async_trait]
impl ExecutionSurface for OpenClawAdapter {
    fn name(&self) -> &str {
        &self.binding.target_agent
    }

    fn supports(&self, capability: &CapabilityDescriptor) -> bool {
        capability.executor_class == ExecutorClass::Agentic
    }

    async fn run(&self, action: &Action) -> anyhow::Result<SurfaceExecutionResult> {
        self.invoke_remote(action).await.map_err(Into::into)
    }
}

fn normalize_event(value: &Value, run_id: Option<&str>) -> SurfaceActionEvent {
    let gateway_event_kind = value
        .get("event")
        .or_else(|| value.get("kind"))
        .or_else(|| value.pointer("/payload/stream"))
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let normalized_event_type = if gateway_event_kind.contains("assistant") {
        "openclaw_assistant_event"
    } else if gateway_event_kind.contains("tool") {
        "openclaw_tool_event"
    } else {
        "openclaw_lifecycle_event"
    };
    SurfaceActionEvent {
        event_type: normalized_event_type.to_string(),
        payload: json!({
            "run_id": extract_run_id(value).or(run_id.map(ToString::to_string)),
            "timestamp": value.get("timestamp").cloned().unwrap_or_else(|| json!(now_timestamp())),
            "gateway_event_kind": gateway_event_kind,
            "summary": event_summary(value),
            "raw": value,
        }),
    }
}

fn event_summary(value: &Value) -> String {
    if let Some(text) = value.pointer("/payload/text").and_then(Value::as_str) {
        return text.chars().take(160).collect();
    }
    if let Some(text) = value.pointer("/payload/message").and_then(Value::as_str) {
        return text.chars().take(160).collect();
    }
    if let Some(text) = value.get("message").and_then(Value::as_str) {
        return text.chars().take(160).collect();
    }
    serde_json::to_string(value).unwrap_or_else(|_| "openclaw event".to_string())
}

fn extract_run_id(value: &Value) -> Option<String> {
    [
        "/runId",
        "/run_id",
        "/result/runId",
        "/result/run_id",
        "/payload/runId",
        "/payload/run_id",
    ]
    .iter()
    .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
    .map(ToString::to_string)
}

fn extract_terminal_error(value: &Value) -> Option<String> {
    if value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("failed"))
    {
        return Some(
            value
                .get("error")
                .and_then(Value::as_str)
                .or_else(|| value.pointer("/error/message").and_then(Value::as_str))
                .unwrap_or("openclaw run failed")
                .to_string(),
        );
    }
    None
}

fn extract_final_text(value: &Value) -> String {
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return text.to_string();
    }
    if let Some(text) = value.get("message").and_then(Value::as_str) {
        return text.to_string();
    }
    if let Some(text) = value.pointer("/assistant/text").and_then(Value::as_str) {
        return text.to_string();
    }
    if let Some(text) = value.pointer("/content/0/text").and_then(Value::as_str) {
        return text.to_string();
    }
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn build_agent_prompt(action: &Action) -> String {
    let brief = action
        .inputs
        .get("task")
        .or_else(|| action.inputs.get("spec_text"))
        .or_else(|| action.inputs.get("problem_statement"))
        .and_then(Value::as_str)
        .unwrap_or(&action.goal.summary);
    let files = action
        .inputs
        .get("files_of_interest")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default();

    let mut lines = vec![
        "Produce a proposal-only patch plan.".to_string(),
        "Do not apply changes, run mutating tools, or edit files.".to_string(),
        format!("Goal: {}", action.goal.summary),
        format!("Brief: {brief}"),
    ];
    if !files.is_empty() {
        lines.push(format!("Files of interest: {files}"));
    }
    if let Some(workspace_root) = action.inputs.get("workspace_root").and_then(Value::as_str) {
        lines.push(format!("Workspace root: {workspace_root}"));
    }
    lines.push(
        "Return a concise plan with target files, ordered steps, risks, assumptions, and test suggestions.".to_string(),
    );
    lines.join("\n")
}

fn patch_plan_artifact_from_result(
    action: &Action,
    text: &str,
    result: &Value,
) -> PatchPlanArtifact {
    let target_files = action
        .inputs
        .get("files_of_interest")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|files| !files.is_empty())
        .unwrap_or_else(|| extract_file_candidates(text));
    let ordered_steps = extract_steps(text);
    let risks = extract_section_lines(text, "risk");
    let assumptions = extract_section_lines(text, "assumption");
    let test_suggestions = extract_section_lines(text, "test");
    let confidence_summary = result
        .get("confidence")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            text.lines()
                .find(|line| line.to_lowercase().contains("confidence"))
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "medium confidence: OpenClaw returned a proposal plan without an explicit confidence field".to_string());

    PatchPlanArtifact {
        target_files,
        ordered_steps: if ordered_steps.is_empty() {
            vec![PatchPlanStep {
                title: "Review the returned proposal".to_string(),
                detail: text.lines().take(3).collect::<Vec<_>>().join(" "),
            }]
        } else {
            ordered_steps
        },
        risks: if risks.is_empty() {
            vec!["Review the proposed file selection and constraints before any follow-on mutation path.".to_string()]
        } else {
            risks
        },
        assumptions: if assumptions.is_empty() {
            vec![format!(
                "This proposal was generated for action {} and still requires operator review.",
                action.id
            )]
        } else {
            assumptions
        },
        test_suggestions: if test_suggestions.is_empty() {
            vec!["Run deterministic checks and the narrowest relevant tests before applying any patch.".to_string()]
        } else {
            test_suggestions
        },
        confidence_summary,
    }
}

fn extract_file_candidates(text: &str) -> Vec<String> {
    let mut files = text
        .split_whitespace()
        .map(|token| token.trim_matches(|char: char| ",:;`()[]{}".contains(char)))
        .filter(|token| token.contains('/') || token.ends_with(".rs") || token.ends_with(".ts"))
        .filter(|token| !token.starts_with("http"))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    files.truncate(8);
    files
}

fn extract_steps(text: &str) -> Vec<PatchPlanStep> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let step = trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
                .or_else(|| trimmed.split_once(". ").map(|(_, rhs)| rhs))
                .map(str::trim)?;
            if step.is_empty() {
                return None;
            }
            Some(PatchPlanStep {
                title: step.chars().take(48).collect(),
                detail: step.to_string(),
            })
        })
        .take(8)
        .collect()
}

fn extract_section_lines(text: &str, keyword: &str) -> Vec<String> {
    text.lines()
        .map(str::trim)
        .filter(|line| line.to_lowercase().contains(keyword))
        .map(ToString::to_string)
        .take(6)
        .collect()
}

fn build_patch_plan_markdown(artifact: &PatchPlanArtifact, action: &Action, text: &str) -> String {
    let mut lines = vec![
        "# Patch Plan".to_string(),
        String::new(),
        format!("Request: {}", action.goal.summary),
        String::new(),
        "## Target Files".to_string(),
    ];
    if artifact.target_files.is_empty() {
        lines.push(
            "- No explicit target files were extracted from the OpenClaw response.".to_string(),
        );
    } else {
        lines.extend(artifact.target_files.iter().map(|file| format!("- {file}")));
    }
    lines.push(String::new());
    lines.push("## Ordered Steps".to_string());
    lines.extend(
        artifact
            .ordered_steps
            .iter()
            .enumerate()
            .map(|(index, step)| format!("{}. **{}**: {}", index + 1, step.title, step.detail)),
    );
    lines.push(String::new());
    lines.push("## Risks".to_string());
    lines.extend(artifact.risks.iter().map(|risk| format!("- {risk}")));
    lines.push(String::new());
    lines.push("## Assumptions".to_string());
    lines.extend(
        artifact
            .assumptions
            .iter()
            .map(|assumption| format!("- {assumption}")),
    );
    lines.push(String::new());
    lines.push("## Suggested Validation".to_string());
    lines.extend(
        artifact
            .test_suggestions
            .iter()
            .map(|suggestion| format!("- {suggestion}")),
    );
    lines.push(String::new());
    lines.push(format!("Confidence: {}", artifact.confidence_summary));
    lines.push(String::new());
    lines.push("## Raw OpenClaw Summary".to_string());
    lines.push(text.to_string());
    lines.join("\n")
}

async fn write_json_artifact<T: serde::Serialize>(
    state_dir: &Path,
    action_id: &str,
    file_name: &str,
    value: &T,
) -> anyhow::Result<ArtifactRef> {
    let artifacts_dir = state_dir.join("artifacts").join(action_id);
    fs::create_dir_all(&artifacts_dir).await?;
    let path = artifacts_dir.join(file_name);
    fs::write(&path, serde_json::to_vec_pretty(value)?).await?;
    Ok(ArtifactRef {
        kind: infer_artifact_kind(file_name),
        path: path.display().to_string(),
    })
}

async fn write_text_artifact(
    state_dir: &Path,
    action_id: &str,
    file_name: &str,
    contents: &str,
) -> anyhow::Result<ArtifactRef> {
    let artifacts_dir = state_dir.join("artifacts").join(action_id);
    fs::create_dir_all(&artifacts_dir).await?;
    let path = artifacts_dir.join(file_name);
    fs::write(&path, contents).await?;
    Ok(ArtifactRef {
        kind: infer_artifact_kind(file_name),
        path: path.display().to_string(),
    })
}

fn infer_artifact_kind(file_name: &str) -> String {
    file_name
        .strip_suffix(".json")
        .or_else(|| file_name.strip_suffix(".md"))
        .unwrap_or(file_name)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::handshake::server::{Request, Response},
    };

    fn planning_action(workspace_root: &Path) -> Action {
        Action {
            id: "action-1".to_string(),
            target_agent_id: "coding_planner".to_string(),
            requester: crawfish_types::RequesterRef {
                kind: crawfish_types::RequesterKind::User,
                id: "cli".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            counterparty_refs: Vec::new(),
            goal: crawfish_types::GoalSpec {
                summary: "plan a patch".to_string(),
                details: None,
            },
            capability: "coding.patch.plan".to_string(),
            inputs: BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    json!(workspace_root.display().to_string()),
                ),
                ("task".to_string(), json!("Plan a safe validation patch")),
                ("files_of_interest".to_string(), json!(["src/lib.rs"])),
            ]),
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

    #[tokio::test]
    async fn adapter_streams_events_and_emits_artifacts() {
        let dir = tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        fs::create_dir_all(&workspace).await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen_auth = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
        let seen_auth_task = seen_auth.clone();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = accept_hdr_async(stream, move |request: &Request, response: Response| {
                *seen_auth_task.lock().unwrap() = request
                    .headers()
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .map(ToString::to_string);
                Ok(response)
            })
            .await
            .unwrap();
            let (mut sink, mut source) = ws.split();
            while let Some(message) = source.next().await {
                let Message::Text(text) = message.unwrap() else {
                    continue;
                };
                let frame: Value = serde_json::from_str(&text).unwrap();
                let method = frame.get("method").and_then(Value::as_str).unwrap();
                let id = frame.get("id").and_then(Value::as_str).unwrap();
                match method {
                    "connect" => {
                        sink.send(Message::Text(
                            json!({"type":"res","id":id,"ok":true,"result":{"sessionKey":"gateway-session"}})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    }
                    "agent" => {
                        sink.send(Message::Text(
                            json!({"type":"event","event":"assistant","payload":{"stream":"assistant","text":"Plan the Rust change safely"},"runId":"run-1"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                        sink.send(Message::Text(
                            json!({"type":"res","id":id,"ok":true,"result":{"runId":"run-1"}})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    }
                    "agent.wait" => {
                        sink.send(Message::Text(
                            json!({"type":"event","event":"tool","payload":{"stream":"tool","message":"scanned repo metadata"},"runId":"run-1"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                        sink.send(Message::Text(
                            json!({
                                "type":"res",
                                "id":id,
                                "ok":true,
                                "result":{
                                    "status":"completed",
                                    "text":"# Proposed Plan\n1. Inspect `src/lib.rs`\n2. Add validation checks\n3. Update tests\nRisks: config and policy drift\nTest: cargo test --workspace"
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                        break;
                    }
                    other => panic!("unexpected method: {other}"),
                }
            }
        });

        env::set_var("OPENCLAW_TEST_TOKEN", "secret-token");
        let adapter = OpenClawAdapter::new(
            OpenClawBinding {
                gateway_url: format!("ws://{address}"),
                auth_ref: "OPENCLAW_TEST_TOKEN".to_string(),
                target_agent: "coding-planner".to_string(),
                session_mode: OpenClawSessionMode::Ephemeral,
                caller_owner_mapping: crawfish_types::CallerOwnerMapping::Required,
                default_trust_domain: crawfish_types::TrustDomain::SameDeviceForeignOwner,
                required_scopes: vec!["repo:read".to_string()],
                lease_required: false,
                workspace_policy: OpenClawWorkspacePolicy::CrawfishManaged,
            },
            dir.path().join(".crawfish/state"),
        );
        let result = adapter.run(&planning_action(&workspace)).await.unwrap();
        server.await.unwrap();

        assert_eq!(
            seen_auth.lock().unwrap().clone(),
            Some("Bearer secret-token".to_string())
        );
        assert_eq!(result.outputs.artifacts.len(), 2);
        assert!(result
            .external_refs
            .iter()
            .any(|reference| reference.kind == "openclaw.run_id" && reference.value == "run-1"));
        assert!(result
            .events
            .iter()
            .any(|event| event.event_type == "openclaw_assistant_event"));
    }

    #[tokio::test]
    async fn adapter_rejects_unsupported_modes() {
        let dir = tempdir().unwrap();
        let binding = OpenClawBinding {
            gateway_url: "ws://127.0.0.1:1".to_string(),
            auth_ref: "OPENCLAW_TEST_TOKEN".to_string(),
            target_agent: "coding-planner".to_string(),
            session_mode: OpenClawSessionMode::Sticky,
            caller_owner_mapping: crawfish_types::CallerOwnerMapping::Required,
            default_trust_domain: crawfish_types::TrustDomain::SameDeviceForeignOwner,
            required_scopes: Vec::new(),
            lease_required: false,
            workspace_policy: OpenClawWorkspacePolicy::Inherit,
        };
        let adapter = OpenClawAdapter::new(binding, dir.path().join(".crawfish/state"));
        let error = adapter.run(&planning_action(dir.path())).await.unwrap_err();
        assert!(error.to_string().contains("sticky"));
    }
}
