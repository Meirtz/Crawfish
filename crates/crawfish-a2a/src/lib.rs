use async_trait::async_trait;
use crawfish_core::{now_timestamp, ExecutionSurface, SurfaceActionEvent, SurfaceExecutionResult};
use crawfish_types::{
    A2ARemoteAgentBinding, A2AStreamingMode, Action, ActionOutputs, ArtifactRef,
    CapabilityDescriptor, CostClass, ExecutorClass, ExternalRef, LatencyClass, Mutability,
    RiskClass, TaskPlanArtifact, TaskPlanStep, TreatyAuthForwardingMode, TreatyPack,
};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use tokio::{fs, time::Duration};

#[derive(Debug, Clone)]
pub struct A2aAdapter {
    binding: A2ARemoteAgentBinding,
    treaty_pack: TreatyPack,
    state_dir: PathBuf,
    client: Client,
}

#[derive(Debug, thiserror::Error)]
pub enum A2aError {
    #[error("a2a auth token env var is missing: {0}")]
    MissingAuthEnv(String),
    #[error("a2a treaty denied remote delegation: {0}")]
    TreatyDenied(String),
    #[error("failed to fetch or use a2a agent card: {0}")]
    AgentCard(String),
    #[error("failed to connect to a2a endpoint: {0}")]
    Connect(String),
    #[error("a2a protocol error: {0}")]
    Protocol(String),
    #[error("a2a remote task failed: {0}")]
    TaskFailed(String),
}

#[derive(Debug, Clone, Deserialize)]
struct AgentCard {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    skills: Vec<AgentSkill>,
    #[serde(default)]
    capabilities: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct AgentSkill {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

impl A2aAdapter {
    pub fn new(
        binding: A2ARemoteAgentBinding,
        treaty_pack: TreatyPack,
        state_dir: PathBuf,
    ) -> Self {
        Self {
            binding,
            treaty_pack,
            state_dir,
            client: Client::new(),
        }
    }

    pub fn binding(&self) -> &A2ARemoteAgentBinding {
        &self.binding
    }

    pub fn treaty_pack(&self) -> &TreatyPack {
        &self.treaty_pack
    }

    pub fn describe_binding(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            namespace: format!("a2a.{}", self.treaty_pack.remote_principal.id),
            verbs: vec!["message_send".to_string(), "task_poll".to_string()],
            executor_class: ExecutorClass::Agentic,
            mutability: Mutability::ProposalOnly,
            risk_class: RiskClass::Medium,
            cost_class: CostClass::Standard,
            latency_class: LatencyClass::LongRunning,
            approval_requirements: Vec::new(),
        }
    }

    fn validate_binding(&self) -> Result<(), A2aError> {
        if self.binding.allow_in_task_auth {
            return Err(A2aError::TreatyDenied(
                "in-task auth forwarding is not supported in p1i".to_string(),
            ));
        }
        if !matches!(
            self.treaty_pack.allowed_auth_forwarding_mode,
            TreatyAuthForwardingMode::None
        ) {
            return Err(A2aError::TreatyDenied(
                "treaty requires unsupported auth forwarding mode".to_string(),
            ));
        }
        if self.treaty_pack.max_delegation_depth != 1 {
            return Err(A2aError::TreatyDenied(
                "p1i only supports treaty max_delegation_depth = 1".to_string(),
            ));
        }
        if !self
            .treaty_pack
            .allowed_capabilities
            .iter()
            .any(|capability| capability == &self.binding.capability)
        {
            return Err(A2aError::TreatyDenied(format!(
                "treaty {} does not allow capability {}",
                self.treaty_pack.id, self.binding.capability
            )));
        }
        Ok(())
    }

    async fn invoke_remote(&self, action: &Action) -> Result<SurfaceExecutionResult, A2aError> {
        self.validate_binding()?;
        let agent_card = self.fetch_agent_card().await?;
        self.validate_agent_card(&agent_card)?;
        let endpoint = self.agent_endpoint(&agent_card)?;

        if matches!(
            self.binding.streaming_mode,
            A2AStreamingMode::PreferStreaming
        ) {
            match self
                .send_stream_request(action, &endpoint, &agent_card)
                .await
            {
                Ok(result) => return Ok(result),
                Err(A2aError::Protocol(message))
                    if message.contains("method not found")
                        || message.contains("stream unsupported")
                        || message.contains("404") => {}
                Err(error) => return Err(error),
            }
        }

        self.send_polling_request(action, &endpoint, &agent_card)
            .await
    }

    async fn fetch_agent_card(&self) -> Result<AgentCard, A2aError> {
        let mut request = self.client.get(&self.binding.agent_card_url);
        if let Some(token) = self.auth_token()? {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|error| A2aError::AgentCard(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(A2aError::AgentCard(format!(
                "agent card request failed with status {status}"
            )));
        }
        response
            .json::<AgentCard>()
            .await
            .map_err(|error| A2aError::AgentCard(error.to_string()))
    }

    fn validate_agent_card(&self, agent_card: &AgentCard) -> Result<(), A2aError> {
        let remote_principal = agent_card
            .id
            .clone()
            .or_else(|| agent_card.name.clone())
            .ok_or_else(|| {
                A2aError::Protocol("agent card missing principal identity".to_string())
            })?;
        if remote_principal != self.treaty_pack.remote_principal.id {
            return Err(A2aError::TreatyDenied(format!(
                "remote principal mismatch: expected {}, got {}",
                self.treaty_pack.remote_principal.id, remote_principal
            )));
        }
        let supports_capability = agent_card
            .capabilities
            .iter()
            .any(|capability| capability == &self.binding.capability)
            || agent_card.skills.iter().any(|skill| {
                skill.id.as_deref() == Some(self.binding.capability.as_str())
                    || skill.name.as_deref() == Some(self.binding.capability.as_str())
                    || skill.tags.iter().any(|tag| tag == &self.binding.capability)
            });
        if !supports_capability {
            return Err(A2aError::TreatyDenied(format!(
                "remote agent does not advertise capability {}",
                self.binding.capability
            )));
        }
        Ok(())
    }

    fn agent_endpoint(&self, agent_card: &AgentCard) -> Result<String, A2aError> {
        if let Some(url) = &agent_card.url {
            return Ok(url.clone());
        }
        let parsed = reqwest::Url::parse(&self.binding.agent_card_url)
            .map_err(|error| A2aError::Protocol(error.to_string()))?;
        let mut base = parsed;
        base.set_path("/rpc");
        Ok(base.to_string())
    }

    fn auth_token(&self) -> Result<Option<String>, A2aError> {
        match &self.binding.auth_ref {
            Some(variable) => env::var(variable)
                .map(Some)
                .map_err(|_| A2aError::MissingAuthEnv(variable.clone())),
            None => Ok(None),
        }
    }

    async fn rpc(&self, endpoint: &str, method: &str, params: Value) -> Result<Value, A2aError> {
        let request_body = json!({
            "jsonrpc": "2.0",
            "id": format!("crawfish-{}", now_timestamp()),
            "method": method,
            "params": params,
        });
        let mut request = self.client.post(endpoint).json(&request_body);
        if let Some(token) = self.auth_token()? {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await
            .map_err(|error| A2aError::Connect(error.to_string()))?;
        let status = response.status();
        let payload: Value = response
            .json()
            .await
            .map_err(|error| A2aError::Protocol(error.to_string()))?;
        if !status.is_success() {
            return Err(A2aError::Protocol(format!(
                "{status}: {}",
                payload
                    .get("error")
                    .cloned()
                    .unwrap_or_else(|| json!(payload))
            )));
        }
        if let Some(error) = payload.get("error") {
            return Err(A2aError::Protocol(error.to_string()));
        }
        Ok(payload.get("result").cloned().unwrap_or(payload))
    }

    async fn send_stream_request(
        &self,
        action: &Action,
        endpoint: &str,
        agent_card: &AgentCard,
    ) -> Result<SurfaceExecutionResult, A2aError> {
        let result = self
            .rpc(
                endpoint,
                "message/stream",
                self.submit_params(action, agent_card),
            )
            .await?;
        let mut events = normalize_stream_events(&result);
        let remote_task_id = extract_task_id(&result).ok_or_else(|| {
            A2aError::Protocol("stream response did not include task id".to_string())
        })?;
        self.surface_result_from_result(
            action,
            agent_card,
            endpoint,
            &remote_task_id,
            result,
            &mut events,
        )
        .await
    }

    async fn send_polling_request(
        &self,
        action: &Action,
        endpoint: &str,
        agent_card: &AgentCard,
    ) -> Result<SurfaceExecutionResult, A2aError> {
        let initial = self
            .rpc(
                endpoint,
                "message/send",
                self.submit_params(action, agent_card),
            )
            .await?;
        let remote_task_id = extract_task_id(&initial).ok_or_else(|| {
            A2aError::Protocol("send response did not include task id".to_string())
        })?;
        let mut events = vec![SurfaceActionEvent {
            event_type: "a2a_run_started".to_string(),
            payload: json!({
                "timestamp": now_timestamp(),
                "remote_task_id": remote_task_id,
                "gateway_event_kind": "message/send",
            }),
        }];
        let mut result = initial;
        for _ in 0..20 {
            if let Some(state) = extract_task_state(&result) {
                if !matches!(state.as_str(), "submitted" | "working") {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            result = self
                .rpc(endpoint, "tasks/get", json!({ "taskId": remote_task_id }))
                .await?;
            events.push(SurfaceActionEvent {
                event_type: "a2a_task_polled".to_string(),
                payload: json!({
                    "timestamp": now_timestamp(),
                    "remote_task_id": remote_task_id,
                    "state": extract_task_state(&result),
                }),
            });
        }
        self.surface_result_from_result(
            action,
            agent_card,
            endpoint,
            &remote_task_id,
            result,
            &mut events,
        )
        .await
    }

    async fn surface_result_from_result(
        &self,
        action: &Action,
        agent_card: &AgentCard,
        endpoint: &str,
        remote_task_id: &str,
        result: Value,
        events: &mut Vec<SurfaceActionEvent>,
    ) -> Result<SurfaceExecutionResult, A2aError> {
        let remote_principal = agent_card
            .id
            .clone()
            .or_else(|| agent_card.name.clone())
            .unwrap_or_else(|| self.treaty_pack.remote_principal.id.clone());
        let remote_state = extract_task_state(&result).unwrap_or_else(|| "completed".to_string());
        match remote_state.as_str() {
            "input-required" => {
                events.push(SurfaceActionEvent {
                    event_type: "a2a_input_required".to_string(),
                    payload: json!({
                        "timestamp": now_timestamp(),
                        "remote_task_id": remote_task_id,
                        "raw": result,
                    }),
                });
                Ok(SurfaceExecutionResult {
                    outputs: ActionOutputs {
                        summary: Some("remote A2A agent requested more input".to_string()),
                        artifacts: Vec::new(),
                        metadata: BTreeMap::from([
                            ("a2a_remote_state".to_string(), json!("blocked")),
                            ("a2a_task_id".to_string(), json!(remote_task_id)),
                            ("a2a_result".to_string(), result),
                        ]),
                    },
                    external_refs: base_external_refs(
                        &self.binding.agent_card_url,
                        endpoint,
                        &remote_principal,
                        remote_task_id,
                    ),
                    events: events.clone(),
                })
            }
            "auth-required" => {
                events.push(SurfaceActionEvent {
                    event_type: "a2a_auth_required".to_string(),
                    payload: json!({
                        "timestamp": now_timestamp(),
                        "remote_task_id": remote_task_id,
                        "raw": result,
                    }),
                });
                Ok(SurfaceExecutionResult {
                    outputs: ActionOutputs {
                        summary: Some("remote A2A agent requested authorization".to_string()),
                        artifacts: Vec::new(),
                        metadata: BTreeMap::from([
                            ("a2a_remote_state".to_string(), json!("awaiting_approval")),
                            ("a2a_task_id".to_string(), json!(remote_task_id)),
                            ("a2a_result".to_string(), result),
                        ]),
                    },
                    external_refs: base_external_refs(
                        &self.binding.agent_card_url,
                        endpoint,
                        &remote_principal,
                        remote_task_id,
                    ),
                    events: events.clone(),
                })
            }
            "failed" | "rejected" | "canceled" => {
                events.push(SurfaceActionEvent {
                    event_type: "a2a_run_failed".to_string(),
                    payload: json!({
                        "timestamp": now_timestamp(),
                        "remote_task_id": remote_task_id,
                        "state": remote_state,
                        "raw": result,
                    }),
                });
                Ok(SurfaceExecutionResult {
                    outputs: ActionOutputs {
                        summary: Some(extract_terminal_message(&result).unwrap_or_else(|| {
                            format!("remote task entered terminal failure state: {remote_state}")
                        })),
                        artifacts: Vec::new(),
                        metadata: BTreeMap::from([
                            ("a2a_remote_state".to_string(), json!("failed")),
                            ("a2a_task_id".to_string(), json!(remote_task_id)),
                            ("a2a_result".to_string(), result),
                        ]),
                    },
                    external_refs: base_external_refs(
                        &self.binding.agent_card_url,
                        endpoint,
                        &remote_principal,
                        remote_task_id,
                    ),
                    events: events.clone(),
                })
            }
            "submitted" | "working" => Err(A2aError::Protocol(
                "remote task did not reach a terminal or operator-awaiting state".to_string(),
            )),
            _ => {
                let final_text = extract_final_text(&result);
                let artifact = task_plan_artifact_from_text(action, &final_text);
                let json_ref =
                    write_json_artifact(&self.state_dir, &action.id, "task_plan.json", &artifact)
                        .await
                        .map_err(|error| A2aError::Protocol(error.to_string()))?;
                let markdown_ref = write_text_artifact(
                    &self.state_dir,
                    &action.id,
                    "task_plan.md",
                    &build_task_plan_markdown(&artifact, action, &final_text),
                )
                .await
                .map_err(|error| A2aError::Protocol(error.to_string()))?;

                events.push(SurfaceActionEvent {
                    event_type: "a2a_run_completed".to_string(),
                    payload: json!({
                        "timestamp": now_timestamp(),
                        "remote_task_id": remote_task_id,
                        "state": remote_state,
                    }),
                });

                Ok(SurfaceExecutionResult {
                    outputs: ActionOutputs {
                        summary: Some(format!(
                            "remote A2A agent produced a task plan for {} target files",
                            artifact.target_files.len()
                        )),
                        artifacts: vec![json_ref, markdown_ref],
                        metadata: BTreeMap::from([
                            ("a2a_remote_state".to_string(), json!("completed")),
                            ("a2a_task_id".to_string(), json!(remote_task_id)),
                            ("a2a_result".to_string(), result),
                        ]),
                    },
                    external_refs: base_external_refs(
                        &self.binding.agent_card_url,
                        endpoint,
                        &remote_principal,
                        remote_task_id,
                    ),
                    events: events.clone(),
                })
            }
        }
    }

    fn submit_params(&self, action: &Action, agent_card: &AgentCard) -> Value {
        let remote_agent = agent_card
            .id
            .clone()
            .or_else(|| agent_card.name.clone())
            .unwrap_or_else(|| self.treaty_pack.remote_principal.id.clone());
        json!({
            "agentId": remote_agent,
            "message": {
                "role": "user",
                "parts": [
                    {
                        "kind": "text",
                        "text": build_task_plan_prompt(action),
                    }
                ],
            },
            "metadata": {
                "capability": action.capability,
                "crawfish_action_id": action.id,
                "required_scopes": self.binding.required_scopes,
            }
        })
    }
}

#[async_trait]
impl ExecutionSurface for A2aAdapter {
    fn name(&self) -> &str {
        &self.treaty_pack.remote_principal.id
    }

    fn supports(&self, capability: &CapabilityDescriptor) -> bool {
        capability.namespace == self.binding.capability
            || capability
                .verbs
                .iter()
                .any(|verb| verb == &self.binding.capability)
    }

    async fn run(&self, action: &Action) -> anyhow::Result<SurfaceExecutionResult> {
        self.invoke_remote(action)
            .await
            .map_err(anyhow::Error::from)
    }
}

fn base_external_refs(
    agent_card_url: &str,
    endpoint: &str,
    remote_principal: &str,
    remote_task_id: &str,
) -> Vec<ExternalRef> {
    vec![
        ExternalRef {
            kind: "a2a.agent_card_url".to_string(),
            value: agent_card_url.to_string(),
            endpoint: Some(agent_card_url.to_string()),
        },
        ExternalRef {
            kind: "a2a.endpoint".to_string(),
            value: endpoint.to_string(),
            endpoint: Some(endpoint.to_string()),
        },
        ExternalRef {
            kind: "a2a.remote_principal".to_string(),
            value: remote_principal.to_string(),
            endpoint: None,
        },
        ExternalRef {
            kind: "a2a.task_id".to_string(),
            value: remote_task_id.to_string(),
            endpoint: None,
        },
    ]
}

fn normalize_stream_events(result: &Value) -> Vec<SurfaceActionEvent> {
    let remote_task_id = extract_task_id(result).unwrap_or_else(|| "unknown".to_string());
    let mut events = vec![SurfaceActionEvent {
        event_type: "a2a_run_started".to_string(),
        payload: json!({
            "timestamp": now_timestamp(),
            "remote_task_id": remote_task_id,
            "gateway_event_kind": "message/stream",
        }),
    }];
    if let Some(stream_events) = result.get("events").and_then(Value::as_array) {
        for event in stream_events {
            let kind = event
                .get("kind")
                .or_else(|| event.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("stream_event");
            events.push(SurfaceActionEvent {
                event_type: match kind {
                    "lifecycle" | "status" => "a2a_lifecycle_event".to_string(),
                    "assistant" | "message" => "a2a_assistant_event".to_string(),
                    _ => "a2a_stream_event".to_string(),
                },
                payload: json!({
                    "timestamp": now_timestamp(),
                    "remote_task_id": remote_task_id,
                    "kind": kind,
                    "raw": event,
                }),
            });
        }
    }
    events
}

fn extract_task_id(value: &Value) -> Option<String> {
    value
        .pointer("/task/id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("taskId")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
}

fn extract_task_state(value: &Value) -> Option<String> {
    value
        .pointer("/task/status/state")
        .or_else(|| value.pointer("/task/state"))
        .or_else(|| value.pointer("/status/state"))
        .or_else(|| value.get("state"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn extract_terminal_message(value: &Value) -> Option<String> {
    value
        .pointer("/task/status/message")
        .or_else(|| value.pointer("/task/error/message"))
        .or_else(|| value.pointer("/error/message"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn extract_final_text(value: &Value) -> String {
    if let Some(text) = value
        .pointer("/task/result/text")
        .or_else(|| value.pointer("/result/text"))
        .or_else(|| value.pointer("/output/text"))
        .and_then(Value::as_str)
    {
        return text.to_string();
    }

    fn collect_text(value: &Value, output: &mut Vec<String>) {
        match value {
            Value::Array(values) => {
                for entry in values {
                    collect_text(entry, output);
                }
            }
            Value::Object(map) => {
                if let Some(text) = map.get("text").and_then(Value::as_str) {
                    output.push(text.to_string());
                }
                for value in map.values() {
                    collect_text(value, output);
                }
            }
            _ => {}
        }
    }

    let mut chunks = Vec::new();
    collect_text(value, &mut chunks);
    if chunks.is_empty() {
        "Remote A2A planner did not return structured text output.".to_string()
    } else {
        chunks.join("\n\n")
    }
}

fn build_task_plan_prompt(action: &Action) -> String {
    let objective = action
        .inputs
        .get("objective")
        .and_then(Value::as_str)
        .or_else(|| action.inputs.get("task").and_then(Value::as_str))
        .or_else(|| action.inputs.get("spec_text").and_then(Value::as_str))
        .or_else(|| {
            action
                .inputs
                .get("problem_statement")
                .and_then(Value::as_str)
        })
        .unwrap_or("Plan the requested task.");
    let workspace_root = action
        .inputs
        .get("workspace_root")
        .and_then(Value::as_str)
        .unwrap_or(".");
    let context_files = string_array(&action.inputs, "context_files");
    let constraints = string_array(&action.inputs, "constraints");
    let desired_outputs = string_array(&action.inputs, "desired_outputs");
    let background = action
        .inputs
        .get("background")
        .and_then(Value::as_str)
        .unwrap_or("");
    let verification_feedback = action
        .inputs
        .get("verification_feedback")
        .and_then(Value::as_str)
        .unwrap_or("");
    let remote_followup = action
        .inputs
        .get("remote_followup")
        .and_then(Value::as_object);
    let followup_section = remote_followup
        .map(|followup| {
            let requested_evidence = followup
                .get("requested_evidence")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "(none)".to_string());
            let prior_attempts = followup
                .get("prior_remote_attempt_refs")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "(none)".to_string());
            let previous_remote_task = followup
                .get("previous_remote_task_ref")
                .and_then(Value::as_str)
                .unwrap_or("(none)");
            let operator_note = followup
                .get("operator_note")
                .and_then(Value::as_str)
                .unwrap_or("(none)");
            format!(
                "Remote follow-up continuation:\nRequested evidence:\n{requested_evidence}\n\nPrevious remote task:\n{previous_remote_task}\n\nPrior remote attempts:\n{prior_attempts}\n\nOperator note:\n{operator_note}"
            )
        })
        .unwrap_or_else(|| "Remote follow-up continuation:\n(none)".to_string());

    format!(
        "You are helping plan a proposal-only task.\n\nObjective:\n{objective}\n\nWorkspace root:\n{workspace_root}\n\nContext files:\n{}\n\nConstraints:\n{}\n\nDesired outputs:\n{}\n\nBackground:\n{}\n\nVerification feedback:\n{}\n\n{}\n\nReturn a concrete plan with target files, ordered steps, risks, assumptions, test suggestions, and a confidence summary.",
        if context_files.is_empty() {
            "(none)".to_string()
        } else {
            context_files.join(", ")
        },
        if constraints.is_empty() {
            "(none)".to_string()
        } else {
            constraints.join("; ")
        },
        if desired_outputs.is_empty() {
            "(none)".to_string()
        } else {
            desired_outputs.join(", ")
        },
        if background.is_empty() { "(none)" } else { background },
        if verification_feedback.is_empty() {
            "(none)"
        } else {
            verification_feedback
        },
        followup_section
    )
}

fn string_array(inputs: &BTreeMap<String, Value>, key: &str) -> Vec<String> {
    inputs
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn task_plan_artifact_from_text(action: &Action, text: &str) -> TaskPlanArtifact {
    let objective = action
        .inputs
        .get("objective")
        .and_then(Value::as_str)
        .or_else(|| action.inputs.get("task").and_then(Value::as_str))
        .unwrap_or(&action.goal.summary);
    let target_files = string_array(&action.inputs, "context_files");
    let desired_outputs = string_array(&action.inputs, "desired_outputs");
    let mut ordered_steps = text
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start_matches(['-', '*', ' ', '\t']).trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(TaskPlanStep {
                    title: trimmed
                        .split('.')
                        .next()
                        .unwrap_or(trimmed)
                        .chars()
                        .take(48)
                        .collect(),
                    detail: trimmed.to_string(),
                })
            }
        })
        .take(6)
        .collect::<Vec<_>>();
    if ordered_steps.len() < 2 {
        ordered_steps = vec![
            TaskPlanStep {
                title: "Clarify objective".to_string(),
                detail: format!("Confirm scope for: {objective}."),
            },
            TaskPlanStep {
                title: "Draft proposal".to_string(),
                detail: "Produce a concrete, non-mutating execution proposal with tests and risks."
                    .to_string(),
            },
        ];
    }
    TaskPlanArtifact {
        target_files,
        ordered_steps,
        risks: vec!["Remote planning output still requires local verification.".to_string()],
        assumptions: vec!["This capability remains proposal-only in Crawfish.".to_string()],
        test_suggestions: if desired_outputs.is_empty() {
            vec!["Review the plan against expected artifacts and constraints.".to_string()]
        } else {
            desired_outputs
                .into_iter()
                .map(|output| format!("Verify the plan covers desired output: {output}"))
                .collect()
        },
        confidence_summary: truncate_summary(text),
    }
}

fn truncate_summary(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    compact.chars().take(160).collect()
}

fn build_task_plan_markdown(artifact: &TaskPlanArtifact, action: &Action, text: &str) -> String {
    let mut markdown = format!("# Task Plan\n\n## Objective\n\n{}\n", action.goal.summary);
    if !artifact.target_files.is_empty() {
        markdown.push_str("\n## Target Files\n\n");
        for file in &artifact.target_files {
            markdown.push_str(&format!("- {file}\n"));
        }
    }
    markdown.push_str("\n## Ordered Steps\n\n");
    for (index, step) in artifact.ordered_steps.iter().enumerate() {
        markdown.push_str(&format!(
            "{}. **{}** — {}\n",
            index + 1,
            step.title,
            step.detail
        ));
    }
    markdown.push_str("\n## Risks\n\n");
    for risk in &artifact.risks {
        markdown.push_str(&format!("- {risk}\n"));
    }
    markdown.push_str("\n## Assumptions\n\n");
    for assumption in &artifact.assumptions {
        markdown.push_str(&format!("- {assumption}\n"));
    }
    markdown.push_str("\n## Test Suggestions\n\n");
    for suggestion in &artifact.test_suggestions {
        markdown.push_str(&format!("- {suggestion}\n"));
    }
    markdown.push_str(&format!(
        "\n## Confidence Summary\n\n{}\n\n## Raw Remote Output\n\n```\n{}\n```\n",
        artifact.confidence_summary, text
    ));
    markdown
}

async fn write_json_artifact<T: serde::Serialize>(
    state_dir: &Path,
    action_id: &str,
    filename: &str,
    value: &T,
) -> anyhow::Result<ArtifactRef> {
    let artifact_dir = state_dir.join("artifacts").join(action_id);
    fs::create_dir_all(&artifact_dir).await?;
    let path = artifact_dir.join(filename);
    let payload = serde_json::to_vec_pretty(value)?;
    fs::write(&path, payload).await?;
    Ok(ArtifactRef {
        kind: "json".to_string(),
        path: path.display().to_string(),
    })
}

async fn write_text_artifact(
    state_dir: &Path,
    action_id: &str,
    filename: &str,
    value: &str,
) -> anyhow::Result<ArtifactRef> {
    let artifact_dir = state_dir.join("artifacts").join(action_id);
    fs::create_dir_all(&artifact_dir).await?;
    let path = artifact_dir.join(filename);
    fs::write(&path, value).await?;
    Ok(ArtifactRef {
        kind: "markdown".to_string(),
        path: path.display().to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, routing::get, routing::post, Json, Router};
    use serde_json::json;
    use std::net::SocketAddr;
    use tempfile::tempdir;
    use tokio::net::TcpListener;

    #[derive(Clone)]
    struct MockState {
        mode: &'static str,
    }

    #[tokio::test]
    async fn adapter_supports_streaming_path() {
        let address = spawn_mock_server("stream").await;
        let state_dir = tempdir().unwrap();
        let adapter = A2aAdapter::new(
            A2ARemoteAgentBinding {
                capability: "task.plan".to_string(),
                agent_card_url: format!("http://{address}/agent-card.json"),
                auth_ref: Some("A2A_TOKEN".to_string()),
                treaty_pack: "remote_task_planning".to_string(),
                federation_pack: None,
                required_scopes: vec!["planning:read".to_string()],
                streaming_mode: A2AStreamingMode::PreferStreaming,
                allow_in_task_auth: false,
            },
            treaty_pack(format!("http://{address}/agent-card.json")),
            state_dir.path().to_path_buf(),
        );
        env::set_var("A2A_TOKEN", "secret");
        let result = adapter.run(&task_plan_action()).await.unwrap();
        assert_eq!(
            result.outputs.metadata.get("a2a_remote_state"),
            Some(&json!("completed"))
        );
        assert!(result
            .external_refs
            .iter()
            .any(|reference| reference.kind == "a2a.task_id" && reference.value == "task-stream"));
    }

    #[tokio::test]
    async fn adapter_falls_back_to_polling_when_stream_is_unsupported() {
        let address = spawn_mock_server("poll").await;
        let state_dir = tempdir().unwrap();
        let adapter = A2aAdapter::new(
            A2ARemoteAgentBinding {
                capability: "task.plan".to_string(),
                agent_card_url: format!("http://{address}/agent-card.json"),
                auth_ref: None,
                treaty_pack: "remote_task_planning".to_string(),
                federation_pack: None,
                required_scopes: Vec::new(),
                streaming_mode: A2AStreamingMode::PreferStreaming,
                allow_in_task_auth: false,
            },
            treaty_pack(format!("http://{address}/agent-card.json")),
            state_dir.path().to_path_buf(),
        );
        let result = adapter.run(&task_plan_action()).await.unwrap();
        assert_eq!(
            result.outputs.metadata.get("a2a_remote_state"),
            Some(&json!("completed"))
        );
        assert!(result
            .events
            .iter()
            .any(|event| event.event_type == "a2a_task_polled"));
    }

    #[tokio::test]
    async fn adapter_rejects_remote_principal_mismatch() {
        let address = spawn_mock_server("mismatch").await;
        let state_dir = tempdir().unwrap();
        let adapter = A2aAdapter::new(
            A2ARemoteAgentBinding {
                capability: "task.plan".to_string(),
                agent_card_url: format!("http://{address}/agent-card.json"),
                auth_ref: None,
                treaty_pack: "remote_task_planning".to_string(),
                federation_pack: None,
                required_scopes: Vec::new(),
                streaming_mode: A2AStreamingMode::PollOnly,
                allow_in_task_auth: false,
            },
            treaty_pack(format!("http://{address}/agent-card.json")),
            state_dir.path().to_path_buf(),
        );
        let error = adapter.run(&task_plan_action()).await.unwrap_err();
        assert!(error.to_string().contains("remote principal mismatch"));
    }

    async fn spawn_mock_server(mode: &'static str) -> SocketAddr {
        let state = MockState { mode };
        let app = Router::new()
            .route("/agent-card.json", get(agent_card_handler))
            .route("/rpc", post(rpc_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        address
    }

    async fn agent_card_handler(State(state): State<MockState>) -> Json<Value> {
        let name = if state.mode == "mismatch" {
            "other-remote"
        } else {
            "remote-task-planner"
        };
        Json(json!({
            "id": name,
            "name": name,
            "skills": [
                {
                    "id": "task.plan",
                    "tags": ["task.plan"]
                }
            ]
        }))
    }

    async fn rpc_handler(
        State(state): State<MockState>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        let method = payload.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match (state.mode, method) {
            ("stream", "message/stream") => json!({
                "jsonrpc": "2.0",
                "id": "1",
                "result": {
                    "task": {
                        "id": "task-stream",
                        "status": { "state": "completed" },
                        "result": { "text": "Inspect the request\nDraft the proposal\nList validation steps" }
                    },
                    "events": [
                        { "kind": "lifecycle", "state": "working" },
                        { "kind": "assistant", "text": "planning..." }
                    ]
                }
            }),
            ("poll", "message/stream") => json!({
                "jsonrpc": "2.0",
                "id": "1",
                "error": { "code": -32601, "message": "method not found" }
            }),
            ("poll", "message/send") => json!({
                "jsonrpc": "2.0",
                "id": "1",
                "result": {
                    "task": {
                        "id": "task-poll",
                        "status": { "state": "submitted" }
                    }
                }
            }),
            ("poll", "tasks/get") => json!({
                "jsonrpc": "2.0",
                "id": "1",
                "result": {
                    "task": {
                        "id": "task-poll",
                        "status": { "state": "completed" },
                        "result": { "text": "Inspect the objective\nWrite a plan\nReview risks" }
                    }
                }
            }),
            _ => json!({
                "jsonrpc": "2.0",
                "id": "1",
                "error": { "code": -32601, "message": "unsupported method" }
            }),
        };
        Json(result)
    }

    fn treaty_pack(agent_card_url: String) -> TreatyPack {
        TreatyPack {
            id: "remote_task_planning".to_string(),
            title: "Remote task planning".to_string(),
            summary: "Allow one-hop remote task planning.".to_string(),
            local_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            remote_principal: crawfish_types::RemotePrincipalRef {
                kind: crawfish_types::RemotePrincipalKind::Agent,
                id: "remote-task-planner".to_string(),
                display_name: None,
                agent_card_url,
                trust_domain: crawfish_types::TrustDomain::ExternalPartner,
            },
            allowed_capabilities: vec!["task.plan".to_string()],
            allowed_data_scopes: vec![
                "objective".to_string(),
                "workspace_root".to_string(),
                "desired_outputs".to_string(),
            ],
            allowed_artifact_classes: vec![
                "task_plan.json".to_string(),
                "task_plan.md".to_string(),
            ],
            allowed_auth_forwarding_mode: TreatyAuthForwardingMode::None,
            required_checkpoints: vec![
                crawfish_types::OversightCheckpoint::Admission,
                crawfish_types::OversightCheckpoint::PreDispatch,
                crawfish_types::OversightCheckpoint::PostResult,
            ],
            required_result_evidence: vec![
                crawfish_types::TreatyEvidenceRequirement::DelegationReceiptPresent,
                crawfish_types::TreatyEvidenceRequirement::RemoteTaskRefPresent,
                crawfish_types::TreatyEvidenceRequirement::TerminalStateVerified,
                crawfish_types::TreatyEvidenceRequirement::ArtifactClassesAllowed,
                crawfish_types::TreatyEvidenceRequirement::DataScopesAllowed,
            ],
            max_delegation_depth: 1,
            review_policy: "operator_review_queue".to_string(),
            on_scope_violation: crawfish_types::TreatyEscalationMode::Deny,
            on_evidence_gap: crawfish_types::TreatyEscalationMode::ReviewRequired,
            review_queue: true,
            alert_rules: vec!["frontier_gap_detected".to_string()],
            clauses: Vec::new(),
        }
    }

    fn task_plan_action() -> Action {
        Action {
            id: "action-1".to_string(),
            target_agent_id: "task_planner".to_string(),
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
                summary: "Plan a remote task".to_string(),
                details: None,
            },
            capability: "task.plan".to_string(),
            inputs: BTreeMap::from([
                ("objective".to_string(), json!("Plan the next release")),
                ("workspace_root".to_string(), json!(".")),
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
            created_at: now_timestamp(),
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
}
