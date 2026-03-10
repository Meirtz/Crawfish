use async_trait::async_trait;
use crawfish_core::{now_timestamp, ExecutionSurface, SurfaceActionEvent, SurfaceExecutionResult};
use crawfish_types::{
    Action, ActionOutputs, ArtifactRef, CapabilityDescriptor, CostClass, ExecutorClass,
    ExternalRef, LatencyClass, LocalHarnessBinding, LocalHarnessKind, LocalHarnessWorkspacePolicy,
    Mutability, RiskClass, TaskPlanArtifact, TaskPlanStep,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::{fs, process::Command, time::Duration};

#[derive(Debug, Clone)]
pub struct LocalHarnessAdapter {
    binding: LocalHarnessBinding,
    state_dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalHarnessError {
    #[error("local harness binary is missing: {0}")]
    MissingBinary(String),
    #[error("failed to spawn local harness: {0}")]
    Spawn(String),
    #[error("local harness timed out after {0} seconds")]
    Timeout(u64),
    #[error("local harness exited with status {status}: {stderr}")]
    ExitNonZero { status: i32, stderr: String },
    #[error("local harness protocol error: {0}")]
    Protocol(String),
}

impl LocalHarnessAdapter {
    pub fn new(binding: LocalHarnessBinding, state_dir: PathBuf) -> Self {
        Self { binding, state_dir }
    }

    pub fn describe_binding(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            namespace: format!("local_harness.{}", self.name()),
            verbs: vec!["exec".to_string()],
            executor_class: ExecutorClass::Agentic,
            mutability: Mutability::ProposalOnly,
            risk_class: RiskClass::Medium,
            cost_class: CostClass::Standard,
            latency_class: LatencyClass::LongRunning,
            approval_requirements: Vec::new(),
        }
    }

    pub fn binding(&self) -> &LocalHarnessBinding {
        &self.binding
    }

    async fn invoke_local(
        &self,
        action: &Action,
    ) -> Result<SurfaceExecutionResult, LocalHarnessError> {
        let prompt = build_task_plan_prompt(action);
        let workspace_root = action
            .inputs
            .get("workspace_root")
            .and_then(Value::as_str)
            .map(ToString::to_string);

        let mut command = Command::new(&self.binding.command);
        command.kill_on_drop(true);
        command.stdin(Stdio::null());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
        command.env_clear();

        for variable in &self.binding.env_allowlist {
            if let Ok(value) = env::var(variable) {
                command.env(variable, value);
            }
        }

        if matches!(
            self.binding.workspace_policy,
            LocalHarnessWorkspacePolicy::Inherit | LocalHarnessWorkspacePolicy::CrawfishManaged
        ) {
            if let Some(workspace_root) = &workspace_root {
                command.current_dir(workspace_root);
            }
        }

        let mut has_prompt_placeholder = false;
        for argument in &self.binding.args {
            let rendered = render_argument(argument, &prompt, workspace_root.as_deref());
            if argument.contains("{prompt}") {
                has_prompt_placeholder = true;
            }
            command.arg(rendered);
        }
        if !has_prompt_placeholder {
            command.arg(prompt.clone());
        }

        let child = command.spawn().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                LocalHarnessError::MissingBinary(self.binding.command.clone())
            } else {
                LocalHarnessError::Spawn(error.to_string())
            }
        })?;

        let output = tokio::time::timeout(
            Duration::from_secs(self.binding.timeout_seconds),
            child.wait_with_output(),
        )
        .await
        .map_err(|_| LocalHarnessError::Timeout(self.binding.timeout_seconds))?
        .map_err(|error| LocalHarnessError::Spawn(error.to_string()))?;

        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

        let mut events = vec![SurfaceActionEvent {
            event_type: "local_harness_process_started".to_string(),
            payload: json!({
                "timestamp": now_timestamp(),
                "harness": self.name(),
                "command": self.binding.command,
            }),
        }];

        if !stdout.is_empty() {
            events.push(SurfaceActionEvent {
                event_type: "local_harness_stdout".to_string(),
                payload: json!({
                    "timestamp": now_timestamp(),
                    "harness": self.name(),
                    "summary": truncate_summary(&stdout),
                }),
            });
        }

        if !stderr.is_empty() {
            events.push(SurfaceActionEvent {
                event_type: "local_harness_stderr".to_string(),
                payload: json!({
                    "timestamp": now_timestamp(),
                    "harness": self.name(),
                    "summary": truncate_summary(&stderr),
                }),
            });
        }

        if !output.status.success() {
            let status = output.status.code().unwrap_or(-1);
            events.push(SurfaceActionEvent {
                event_type: "local_harness_failed".to_string(),
                payload: json!({
                    "timestamp": now_timestamp(),
                    "harness": self.name(),
                    "status": status,
                    "stderr": stderr,
                }),
            });
            return Err(LocalHarnessError::ExitNonZero { status, stderr });
        }

        if stdout.trim().is_empty() {
            return Err(LocalHarnessError::Protocol(
                "local harness produced no stdout output".to_string(),
            ));
        }

        let artifact = task_plan_artifact_from_text(action, &stdout);
        let json_ref =
            write_json_artifact(&self.state_dir, &action.id, "task_plan.json", &artifact)
                .await
                .map_err(|error| LocalHarnessError::Protocol(error.to_string()))?;
        let markdown_ref = write_text_artifact(
            &self.state_dir,
            &action.id,
            "task_plan.md",
            &build_task_plan_markdown(&artifact, action, &stdout),
        )
        .await
        .map_err(|error| LocalHarnessError::Protocol(error.to_string()))?;

        events.push(SurfaceActionEvent {
            event_type: "local_harness_completed".to_string(),
            payload: json!({
                "timestamp": now_timestamp(),
                "harness": self.name(),
                "artifact_count": 2,
            }),
        });

        Ok(SurfaceExecutionResult {
            outputs: ActionOutputs {
                summary: Some(format!(
                    "{} produced a task plan for {} target files",
                    self.name(),
                    artifact.target_files.len()
                )),
                artifacts: vec![json_ref, markdown_ref],
                metadata: BTreeMap::from([
                    ("execution_surface".to_string(), json!("local_harness")),
                    ("local_harness".to_string(), json!(self.name())),
                ]),
            },
            external_refs: vec![
                ExternalRef {
                    kind: "local_harness.harness".to_string(),
                    value: self.name().to_string(),
                    endpoint: None,
                },
                ExternalRef {
                    kind: "local_harness.command".to_string(),
                    value: self.binding.command.clone(),
                    endpoint: None,
                },
            ],
            events,
        })
    }
}

#[async_trait]
impl ExecutionSurface for LocalHarnessAdapter {
    fn name(&self) -> &str {
        match self.binding.harness {
            LocalHarnessKind::ClaudeCode => "claude_code",
            LocalHarnessKind::Codex => "codex",
        }
    }

    fn supports(&self, capability: &CapabilityDescriptor) -> bool {
        capability.executor_class == ExecutorClass::Agentic
    }

    async fn run(&self, action: &Action) -> anyhow::Result<SurfaceExecutionResult> {
        self.invoke_local(action).await.map_err(Into::into)
    }
}

fn render_argument(template: &str, prompt: &str, workspace_root: Option<&str>) -> String {
    template
        .replace("{prompt}", prompt)
        .replace("{workspace_root}", workspace_root.unwrap_or_default())
}

fn truncate_summary(text: &str) -> String {
    text.chars().take(240).collect()
}

fn build_task_plan_prompt(action: &Action) -> String {
    let objective = action
        .inputs
        .get("objective")
        .or_else(|| action.inputs.get("task"))
        .or_else(|| action.inputs.get("spec_text"))
        .or_else(|| action.inputs.get("problem_statement"))
        .and_then(Value::as_str)
        .unwrap_or(&action.goal.summary);
    let context_files = string_array(action, "context_files");
    let legacy_files = string_array(action, "files_of_interest");
    let files = if context_files.is_empty() {
        legacy_files
    } else {
        context_files
    };
    let constraints = string_array(action, "constraints");
    let desired_outputs = string_array(action, "desired_outputs");
    let background = action
        .inputs
        .get("background")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let verification_feedback = action
        .inputs
        .get("verification_feedback")
        .and_then(Value::as_str)
        .unwrap_or_default();

    let mut lines = vec![
        "Produce a proposal-only task plan.".to_string(),
        "Do not apply changes, edit files, or perform mutating actions.".to_string(),
        format!("Goal: {}", action.goal.summary),
        format!("Objective: {objective}"),
    ];
    if let Some(workspace_root) = action.inputs.get("workspace_root").and_then(Value::as_str) {
        lines.push(format!("Workspace root: {workspace_root}"));
    }
    if !files.is_empty() {
        lines.push(format!("Context files: {}", files.join(", ")));
    }
    if !constraints.is_empty() {
        lines.push(format!("Constraints: {}", constraints.join(", ")));
    }
    if !desired_outputs.is_empty() {
        lines.push(format!("Desired outputs: {}", desired_outputs.join(", ")));
    }
    if !background.trim().is_empty() {
        lines.push(format!("Background: {background}"));
    }
    if !verification_feedback.trim().is_empty() {
        lines.push(format!(
            "Verification feedback to address: {verification_feedback}"
        ));
    }
    lines.push(
        "Return target files, ordered steps, risks, assumptions, and test suggestions.".to_string(),
    );
    lines.join("\n")
}

fn string_array(action: &Action, key: &str) -> Vec<String> {
    action
        .inputs
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn task_plan_artifact_from_text(action: &Action, text: &str) -> TaskPlanArtifact {
    let context_files = string_array(action, "context_files");
    let legacy_files = string_array(action, "files_of_interest");
    let target_files = if context_files.is_empty() {
        if legacy_files.is_empty() {
            extract_file_candidates(text)
        } else {
            legacy_files
        }
    } else {
        context_files
    };
    let ordered_steps = extract_steps(text);
    let risks = extract_section_lines(text, "risk");
    let assumptions = extract_section_lines(text, "assumption");
    let test_suggestions = extract_section_lines(text, "test");
    let confidence_summary = text
        .lines()
        .find(|line| line.to_lowercase().contains("confidence"))
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            format!(
                "medium confidence: {} returned a proposal-only task plan",
                action.target_agent_id
            )
        });

    TaskPlanArtifact {
        target_files,
        ordered_steps: if ordered_steps.is_empty() {
            vec![TaskPlanStep {
                title: "Review the returned proposal".to_string(),
                detail: text.lines().take(3).collect::<Vec<_>>().join(" "),
            }]
        } else {
            ordered_steps
        },
        risks: if risks.is_empty() {
            vec![
                "Review the proposed sequence and constraints before executing follow-on work."
                    .to_string(),
            ]
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
            vec![
                "Run the narrowest deterministic validation before acting on the proposal."
                    .to_string(),
            ]
        } else {
            test_suggestions
        },
        confidence_summary,
    }
}

fn extract_file_candidates(text: &str) -> Vec<String> {
    let mut files = text
        .split_whitespace()
        .map(|token| token.trim_matches(|character: char| ",:;`()[]{}".contains(character)))
        .filter(|token| token.contains('/') || token.ends_with(".rs") || token.ends_with(".ts"))
        .filter(|token| !token.starts_with("http"))
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    files.truncate(8);
    files
}

fn extract_steps(text: &str) -> Vec<TaskPlanStep> {
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
            Some(TaskPlanStep {
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

fn build_task_plan_markdown(artifact: &TaskPlanArtifact, action: &Action, text: &str) -> String {
    let mut lines = vec![
        "# Task Plan".to_string(),
        String::new(),
        format!("Request: {}", action.goal.summary),
        String::new(),
        "## Target Files".to_string(),
    ];
    if artifact.target_files.is_empty() {
        lines
            .push("- No explicit target files were extracted from the harness output.".to_string());
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
    lines.push("## Raw Harness Summary".to_string());
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
    use crawfish_types::{
        ActionPhase, ExecutionContract, GoalSpec, OwnerKind, OwnerRef, RequesterKind, RequesterRef,
        ScheduleSpec,
    };
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    fn planning_action(workspace_root: &Path) -> Action {
        Action {
            id: "action-1".to_string(),
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "cli".to_string(),
            },
            initiator_owner: OwnerRef {
                kind: OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            counterparty_refs: Vec::new(),
            goal: GoalSpec {
                summary: "plan a task".to_string(),
                details: None,
            },
            capability: "task.plan".to_string(),
            inputs: BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    json!(workspace_root.display().to_string()),
                ),
                (
                    "objective".to_string(),
                    json!("Produce an operator-ready task plan"),
                ),
                (
                    "desired_outputs".to_string(),
                    json!(["operator-ready summary"]),
                ),
            ]),
            contract: ExecutionContract::default(),
            execution_strategy: None,
            grant_refs: Vec::new(),
            lease_ref: None,
            encounter_ref: None,
            audit_receipt_ref: None,
            data_boundary: "owner_local".to_string(),
            schedule: ScheduleSpec::default(),
            phase: ActionPhase::Accepted,
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

    async fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, body).await.unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        path
    }

    #[tokio::test]
    async fn adapter_passes_allowlisted_env_and_emits_artifacts() {
        let dir = tempdir().unwrap();
        let script = write_script(
            dir.path(),
            "claude-plan.sh",
            r#"#!/bin/sh
cat <<EOF
- Review the objective against local context.
- Produce the operator-ready summary.
Risk: Environment assumptions may drift.
Assumption: allowed=$ALLOWED_VAR blocked=$BLOCKED_VAR
Test: Validate the operator-ready summary.
Confidence: high confidence after local harness execution
EOF
"#,
        )
        .await;
        env::set_var("ALLOWED_VAR", "safe");
        env::set_var("BLOCKED_VAR", "hidden");

        let adapter = LocalHarnessAdapter::new(
            LocalHarnessBinding {
                capability: "task.plan".to_string(),
                harness: LocalHarnessKind::ClaudeCode,
                command: "sh".to_string(),
                args: vec![script.display().to_string()],
                required_scopes: Vec::new(),
                lease_required: false,
                workspace_policy: LocalHarnessWorkspacePolicy::CrawfishManaged,
                env_allowlist: vec!["ALLOWED_VAR".to_string()],
                timeout_seconds: 5,
            },
            dir.path().to_path_buf(),
        );

        let result = adapter.run(&planning_action(dir.path())).await.unwrap();
        let json_artifact = result
            .outputs
            .artifacts
            .iter()
            .find(|artifact| artifact.path.ends_with("task_plan.json"))
            .unwrap();
        let artifact: TaskPlanArtifact =
            serde_json::from_slice(&fs::read(&json_artifact.path).await.unwrap()).unwrap();
        assert!(artifact
            .assumptions
            .iter()
            .any(|assumption| assumption.contains("allowed=safe")));
        assert!(artifact
            .assumptions
            .iter()
            .all(|assumption| !assumption.contains("blocked=hidden")));
    }

    #[tokio::test]
    async fn adapter_reports_missing_binary() {
        let dir = tempdir().unwrap();
        let adapter = LocalHarnessAdapter::new(
            LocalHarnessBinding {
                capability: "task.plan".to_string(),
                harness: LocalHarnessKind::Codex,
                command: "__missing_local_harness__".to_string(),
                args: vec!["exec".to_string()],
                required_scopes: Vec::new(),
                lease_required: false,
                workspace_policy: LocalHarnessWorkspacePolicy::Inherit,
                env_allowlist: Vec::new(),
                timeout_seconds: 5,
            },
            dir.path().to_path_buf(),
        );

        let error = adapter.run(&planning_action(dir.path())).await.unwrap_err();
        assert!(error
            .downcast_ref::<LocalHarnessError>()
            .is_some_and(|error| matches!(error, LocalHarnessError::MissingBinary(_))));
    }

    #[tokio::test]
    async fn adapter_reports_timeout() {
        let dir = tempdir().unwrap();
        let script = write_script(
            dir.path(),
            "sleepy-plan.sh",
            "#!/bin/sh\nsleep 2\nprintf '%s\n' '- step one'\n",
        )
        .await;
        let adapter = LocalHarnessAdapter::new(
            LocalHarnessBinding {
                capability: "task.plan".to_string(),
                harness: LocalHarnessKind::ClaudeCode,
                command: "sh".to_string(),
                args: vec![script.display().to_string()],
                required_scopes: Vec::new(),
                lease_required: false,
                workspace_policy: LocalHarnessWorkspacePolicy::Inherit,
                env_allowlist: Vec::new(),
                timeout_seconds: 1,
            },
            dir.path().to_path_buf(),
        );

        let error = adapter.run(&planning_action(dir.path())).await.unwrap_err();
        assert!(error
            .downcast_ref::<LocalHarnessError>()
            .is_some_and(|error| matches!(error, LocalHarnessError::Timeout(1))));
    }

    #[tokio::test]
    async fn adapter_reports_nonzero_exit() {
        let dir = tempdir().unwrap();
        let script = write_script(
            dir.path(),
            "failing-plan.sh",
            "#!/bin/sh\necho 'boom' >&2\nexit 7\n",
        )
        .await;
        let adapter = LocalHarnessAdapter::new(
            LocalHarnessBinding {
                capability: "task.plan".to_string(),
                harness: LocalHarnessKind::Codex,
                command: "sh".to_string(),
                args: vec![script.display().to_string()],
                required_scopes: Vec::new(),
                lease_required: false,
                workspace_policy: LocalHarnessWorkspacePolicy::Inherit,
                env_allowlist: Vec::new(),
                timeout_seconds: 5,
            },
            dir.path().to_path_buf(),
        );

        let error = adapter.run(&planning_action(dir.path())).await.unwrap_err();
        assert!(error
            .downcast_ref::<LocalHarnessError>()
            .is_some_and(|error| matches!(
                error,
                LocalHarnessError::ExitNonZero { status: 7, .. }
            )));
    }
}
