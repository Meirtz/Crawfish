use super::*;

pub(crate) fn build_checkpoint(
    action: &Action,
    executor_kind: &str,
    stage: &str,
    artifact_refs: Vec<crawfish_types::ArtifactRef>,
) -> anyhow::Result<DeterministicCheckpoint> {
    Ok(DeterministicCheckpoint {
        executor_kind: executor_kind.to_string(),
        stage: stage.to_string(),
        workspace_root: action
            .inputs
            .get("workspace_root")
            .and_then(Value::as_str)
            .unwrap_or(".")
            .to_string(),
        input_digest: input_digest(&action.inputs)?,
        artifact_refs,
        strategy_state: None,
        last_updated_at: now_timestamp(),
    })
}

pub(crate) fn checkpoint_ref_for_executor(executor_kind: &str) -> String {
    format!("{}-checkpoint", executor_kind.replace('.', "-"))
}

pub(crate) fn input_digest(inputs: &crawfish_types::Metadata) -> anyhow::Result<String> {
    let serialized = serde_json::to_string(inputs)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serialized.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}

pub(crate) fn stable_id(value: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

pub(crate) fn artifact_refs_exist(artifact_refs: &[crawfish_types::ArtifactRef]) -> bool {
    !artifact_refs.is_empty()
        && artifact_refs
            .iter()
            .all(|artifact| Path::new(&artifact.path).exists())
}

pub(crate) fn recovered_outputs_from_checkpoint(
    checkpoint: &DeterministicCheckpoint,
) -> ActionOutputs {
    let mut metadata = std::collections::BTreeMap::from([
        (
            "recovered_from_checkpoint".to_string(),
            serde_json::json!(true),
        ),
        (
            "executor_kind".to_string(),
            serde_json::json!(checkpoint.executor_kind),
        ),
        (
            "input_digest".to_string(),
            serde_json::json!(checkpoint.input_digest),
        ),
    ]);
    if let Some(strategy_state) = &checkpoint.strategy_state {
        metadata.insert(
            "strategy_iteration".to_string(),
            serde_json::json!(strategy_state.iteration),
        );
        if let Some(summary) = &strategy_state.verification_summary {
            metadata.insert(
                "verification_summary".to_string(),
                serde_json::to_value(summary).unwrap_or(serde_json::Value::Null),
            );
        }
    }
    ActionOutputs {
        summary: Some(format!(
            "Recovered outputs from {} checkpoint at stage {}",
            checkpoint.executor_kind, checkpoint.stage
        )),
        artifacts: checkpoint.artifact_refs.clone(),
        metadata,
    }
}

pub(crate) fn has_log_input(action: &Action) -> bool {
    action
        .inputs
        .get("log_text")
        .and_then(Value::as_str)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || action
            .inputs
            .get("log_file")
            .and_then(Value::as_str)
            .map(|path| Path::new(path).is_file())
            .unwrap_or(false)
}

pub(crate) fn mcp_input_external_refs(action: &Action) -> Vec<ExternalRef> {
    action
        .inputs
        .get("mcp_resource_ref")
        .and_then(Value::as_str)
        .map(|value| {
            vec![ExternalRef {
                kind: "mcp_resource".to_string(),
                value: value.to_string(),
                endpoint: None,
            }]
        })
        .unwrap_or_default()
}

pub(crate) fn extract_mcp_log_text(outputs: &ActionOutputs) -> Option<String> {
    let result = outputs.metadata.get("mcp_result")?;
    if let Some(log_text) = result
        .get("structuredContent")
        .and_then(|value| value.get("log_text"))
        .and_then(Value::as_str)
    {
        return Some(log_text.to_string());
    }
    if let Some(log_text) = result
        .get("structuredContent")
        .and_then(|value| value.get("log_excerpt"))
        .and_then(Value::as_str)
    {
        return Some(log_text.to_string());
    }
    if let Some(items) = result.get("content").and_then(Value::as_array) {
        let text = items
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        if !text.trim().is_empty() {
            return Some(text);
        }
    }
    outputs.summary.clone()
}

pub(crate) fn select_continuity_mode(
    preferences: &[ContinuityModeName],
    deterministic_available: bool,
) -> ContinuityModeName {
    for mode in preferences {
        match mode {
            ContinuityModeName::DeterministicOnly if !deterministic_available => continue,
            _ => return mode.clone(),
        }
    }

    if deterministic_available {
        ContinuityModeName::DeterministicOnly
    } else {
        ContinuityModeName::StoreAndForward
    }
}

pub(crate) fn action_requester(id: &str) -> crawfish_types::RequesterRef {
    crawfish_types::RequesterRef {
        kind: crawfish_types::RequesterKind::System,
        id: id.to_string(),
    }
}

pub(crate) fn current_timestamp_seconds() -> u64 {
    now_timestamp().parse::<u64>().unwrap_or_default()
}

pub(crate) fn failure_code_approval_required() -> &'static str {
    "approval_required"
}

pub(crate) fn failure_code_approval_rejected() -> &'static str {
    "approval_rejected"
}

pub(crate) fn failure_code_lease_revoked() -> &'static str {
    "lease_revoked"
}

pub(crate) fn failure_code_lease_expired() -> &'static str {
    "lease_expired"
}

pub(crate) fn failure_code_local_harness_missing_binary() -> &'static str {
    "local_harness_missing_binary"
}

pub(crate) fn failure_code_local_harness_spawn_error() -> &'static str {
    "local_harness_spawn_error"
}

pub(crate) fn failure_code_local_harness_timeout() -> &'static str {
    "local_harness_timeout"
}

pub(crate) fn failure_code_local_harness_exit_nonzero() -> &'static str {
    "local_harness_exit_nonzero"
}

pub(crate) fn failure_code_local_harness_protocol_error() -> &'static str {
    "local_harness_protocol_error"
}

pub(crate) fn failure_code_lock_conflict() -> &'static str {
    "lock_conflict"
}

pub(crate) fn failure_code_openclaw_auth_error() -> &'static str {
    "openclaw_auth_error"
}

pub(crate) fn failure_code_openclaw_connect_error() -> &'static str {
    "openclaw_connect_error"
}

pub(crate) fn failure_code_openclaw_protocol_error() -> &'static str {
    "openclaw_protocol_error"
}

pub(crate) fn failure_code_openclaw_run_failed() -> &'static str {
    "openclaw_run_failed"
}

pub(crate) fn failure_code_openclaw_unsupported_workspace_mode() -> &'static str {
    "openclaw_unsupported_workspace_mode"
}

pub(crate) fn failure_code_openclaw_unsupported_session_mode() -> &'static str {
    "openclaw_unsupported_session_mode"
}

pub(crate) fn failure_code_a2a_auth_error() -> &'static str {
    "a2a_auth_error"
}

pub(crate) fn failure_code_a2a_connect_error() -> &'static str {
    "a2a_connect_error"
}

pub(crate) fn failure_code_a2a_protocol_error() -> &'static str {
    "a2a_protocol_error"
}

pub(crate) fn failure_code_a2a_task_failed() -> &'static str {
    "a2a_task_failed"
}

pub(crate) fn failure_code_treaty_denied() -> &'static str {
    "treaty_denied"
}

pub(crate) fn failure_code_route_unavailable() -> &'static str {
    "route_unavailable"
}

pub(crate) fn failure_code_executor_error() -> &'static str {
    "executor_error"
}

pub(crate) fn failure_code_requeued_after_restart() -> &'static str {
    "requeued_after_restart"
}

pub(crate) fn failure_code_verification_failed() -> &'static str {
    "verification_failed"
}

pub(crate) fn failure_code_verification_budget_exhausted() -> &'static str {
    "verification_budget_exhausted"
}

pub(crate) fn failure_code_verification_spec_invalid() -> &'static str {
    "verification_spec_invalid"
}

pub(crate) fn runtime_enum_to_snake<T: std::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
        .chars()
        .enumerate()
        .fold(String::new(), |mut acc, (index, ch)| {
            if ch.is_ascii_uppercase() {
                if index != 0 {
                    acc.push('_');
                }
                acc.extend(ch.to_lowercase());
            } else {
                acc.push(ch);
            }
            acc
        })
}

pub(crate) fn objective_tokens(objective: &str) -> Vec<String> {
    objective
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.len() >= 4)
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

pub(crate) fn artifact_basename(artifact: &crawfish_types::ArtifactRef) -> String {
    Path::new(&artifact.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&artifact.path)
        .to_string()
}

pub(crate) fn artifact_ref_by_name<'a>(
    action: &'a Action,
    artifact_name: &str,
) -> Option<&'a crawfish_types::ArtifactRef> {
    action.outputs.artifacts.iter().find(|artifact| {
        artifact_basename(artifact) == artifact_name || artifact.path.ends_with(artifact_name)
    })
}

pub(crate) async fn scorecard_target_value(
    action: &Action,
    artifact_name: Option<&str>,
    field_path: Option<&str>,
) -> anyhow::Result<Option<Value>> {
    if let Some(artifact_name) = artifact_name {
        let Some(artifact_ref) = artifact_ref_by_name(action, artifact_name) else {
            return Ok(None);
        };
        let value: Value = load_json_artifact(artifact_ref).await?;
        if let Some(field_path) = field_path {
            return Ok(json_value_at_path(&value, field_path).cloned());
        }
        return Ok(Some(value));
    }

    Ok(field_path
        .and_then(|field_path| metadata_value_at_path(&action.inputs, field_path).cloned()))
}

pub(crate) async fn scorecard_target_text(
    action: &Action,
    artifact_name: Option<&str>,
    field_path: Option<&str>,
) -> anyhow::Result<Option<String>> {
    if let Some(artifact_name) = artifact_name {
        let Some(artifact_ref) = artifact_ref_by_name(action, artifact_name) else {
            return Ok(None);
        };
        if field_path.is_none() {
            return Ok(Some(tokio::fs::read_to_string(&artifact_ref.path).await?));
        }
    }
    let Some(value) = scorecard_target_value(action, artifact_name, field_path).await? else {
        return Ok(None);
    };
    Ok(Some(match value {
        Value::String(text) => text,
        other => serde_json::to_string(&other)?,
    }))
}

pub(crate) async fn scorecard_evidence_summary(
    action: &Action,
    criterion: &ScorecardCriterion,
    interaction_model: &crawfish_types::InteractionModel,
    observed_incidents: &[PolicyIncident],
    passed: bool,
) -> anyhow::Result<String> {
    let target_label = criterion
        .artifact_name
        .clone()
        .unwrap_or_else(|| "inputs".to_string());
    let path_label = criterion
        .field_path
        .as_deref()
        .map(|path| format!(" at `{path}`"))
        .unwrap_or_default();
    let status = if passed { "passed" } else { "failed" };

    let detail = match criterion.kind {
        ScorecardCriterionKind::RegexMatch => criterion
            .regex_pattern
            .as_deref()
            .map(|pattern| format!("pattern `{pattern}`"))
            .unwrap_or_else(|| "regex pattern missing".to_string()),
        ScorecardCriterionKind::NumericThreshold => format!(
            "threshold {:?} {}",
            criterion.numeric_comparison,
            criterion.numeric_threshold.unwrap_or_default()
        ),
        ScorecardCriterionKind::FieldEquals => criterion
            .expected_value
            .as_ref()
            .map(|value| format!("expected {value}"))
            .unwrap_or_else(|| "expected value missing".to_string()),
        ScorecardCriterionKind::JsonSchemaValid => "JSON schema validation".to_string(),
        ScorecardCriterionKind::ListMinLen => {
            format!("minimum length {}", criterion.min_len.unwrap_or(1))
        }
        ScorecardCriterionKind::TokenCoverage => criterion
            .source_path
            .as_deref()
            .map(|path| format!("cover tokens from `{path}`"))
            .unwrap_or_else(|| "token source missing".to_string()),
        ScorecardCriterionKind::CheckpointPassed => criterion
            .checkpoint
            .as_ref()
            .map(|checkpoint| format!("checkpoint `{}`", runtime_enum_to_snake(checkpoint)))
            .unwrap_or_else(|| "checkpoint missing".to_string()),
        ScorecardCriterionKind::IncidentAbsent => criterion
            .incident_code
            .as_deref()
            .map(|code| format!("incident `{code}` absent"))
            .unwrap_or_else(|| format!("{} incidents absent", observed_incidents.len())),
        ScorecardCriterionKind::ExternalRefPresent => criterion
            .external_ref_kind
            .as_deref()
            .map(|kind| format!("external ref `{kind}` present"))
            .unwrap_or_else(|| "external ref kind missing".to_string()),
        ScorecardCriterionKind::InteractionModelIs => criterion
            .interaction_model
            .as_ref()
            .map(|model| format!("interaction model `{}`", runtime_enum_to_snake(model)))
            .unwrap_or_else(|| "interaction model missing".to_string()),
        ScorecardCriterionKind::RemoteOutcomeDispositionIs => criterion
            .remote_outcome_disposition
            .as_ref()
            .map(|disposition| {
                format!(
                    "remote outcome disposition `{}`",
                    runtime_enum_to_snake(disposition)
                )
            })
            .unwrap_or_else(|| "remote outcome disposition missing".to_string()),
        ScorecardCriterionKind::TreatyViolationAbsent => criterion
            .treaty_violation_code
            .as_deref()
            .map(|code| format!("treaty violation `{code}` absent"))
            .unwrap_or_else(|| "no treaty violations present".to_string()),
        ScorecardCriterionKind::ArtifactPresent => "artifact present".to_string(),
        ScorecardCriterionKind::ArtifactAbsent => "artifact absent".to_string(),
        ScorecardCriterionKind::JsonFieldNonempty => "field nonempty".to_string(),
    };

    if matches!(
        criterion.kind,
        ScorecardCriterionKind::ArtifactPresent | ScorecardCriterionKind::ArtifactAbsent
    ) && criterion.artifact_name.is_some()
    {
        return Ok(format!("{status}: {detail} for `{target_label}`"));
    }

    let observed = if matches!(criterion.kind, ScorecardCriterionKind::RegexMatch) {
        scorecard_target_text(
            action,
            criterion.artifact_name.as_deref(),
            criterion.field_path.as_deref(),
        )
        .await?
        .map(|text| format!(" observed {}", compact_json_value(&Value::String(text))))
        .unwrap_or_else(|| " observed <missing>".to_string())
    } else if matches!(criterion.kind, ScorecardCriterionKind::InteractionModelIs) {
        format!(" observed {}", runtime_enum_to_snake(interaction_model))
    } else if matches!(
        criterion.kind,
        ScorecardCriterionKind::RemoteOutcomeDispositionIs
    ) {
        remote_outcome_disposition_for_action(action)
            .as_ref()
            .map(|disposition| format!(" observed {}", runtime_enum_to_snake(disposition)))
            .unwrap_or_else(|| " observed <missing>".to_string())
    } else if matches!(criterion.kind, ScorecardCriterionKind::ExternalRefPresent) {
        criterion
            .external_ref_kind
            .as_deref()
            .map(|kind| {
                let present = action
                    .external_refs
                    .iter()
                    .any(|reference| reference.kind == kind);
                format!(" observed present={present}")
            })
            .unwrap_or_else(|| " observed <missing-kind>".to_string())
    } else if matches!(
        criterion.kind,
        ScorecardCriterionKind::TreatyViolationAbsent
    ) {
        let violations = treaty_violations_for_action(action);
        if let Some(code) = criterion.treaty_violation_code.as_deref() {
            let matched = violations
                .iter()
                .filter(|violation| violation.code == code)
                .map(|violation| violation.summary.clone())
                .collect::<Vec<_>>();
            if matched.is_empty() {
                " observed []".to_string()
            } else {
                format!(" observed {matched:?}")
            }
        } else if violations.is_empty() {
            " observed []".to_string()
        } else {
            format!(
                " observed {:?}",
                violations
                    .iter()
                    .map(|violation| violation.code.clone())
                    .collect::<Vec<_>>()
            )
        }
    } else if matches!(criterion.kind, ScorecardCriterionKind::IncidentAbsent) {
        if let Some(code) = criterion.incident_code.as_deref() {
            let matched = observed_incidents
                .iter()
                .filter(|incident| incident.reason_code == code)
                .map(|incident| incident.summary.clone())
                .collect::<Vec<_>>();
            if matched.is_empty() {
                " observed []".to_string()
            } else {
                format!(" observed {matched:?}")
            }
        } else if observed_incidents.is_empty() {
            " observed []".to_string()
        } else {
            format!(
                " observed {:?}",
                observed_incidents
                    .iter()
                    .map(|incident| incident.reason_code.clone())
                    .collect::<Vec<_>>()
            )
        }
    } else {
        let current = scorecard_target_value(
            action,
            criterion.artifact_name.as_deref(),
            criterion.field_path.as_deref(),
        )
        .await?;
        current
            .as_ref()
            .map(|value| format!(" observed {}", compact_json_value(value)))
            .unwrap_or_else(|| " observed <missing>".to_string())
    };
    Ok(format!(
        "{status}: {detail} on `{target_label}`{path_label}.{observed}"
    ))
}

pub(crate) fn compact_json_value(value: &Value) -> String {
    let raw = match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    let trimmed = raw.trim();
    if trimmed.len() > 120 {
        format!("{}...", &trimmed[..117])
    } else {
        trimmed.to_string()
    }
}

pub(crate) fn json_value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in path.split('.') {
        match current {
            Value::Object(map) => current = map.get(part)?,
            _ => return None,
        }
    }
    Some(current)
}

pub(crate) fn metadata_value_at_path<'a>(metadata: &'a Metadata, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut current = metadata.get(first)?;
    for part in parts {
        current = match current {
            Value::Object(map) => map.get(part)?,
            _ => return None,
        };
    }
    Some(current)
}

pub(crate) fn json_value_is_nonempty(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
        Value::Bool(value) => *value,
        Value::Number(_) => true,
    }
}

pub(crate) fn scorecard_source_tokens(action: &Action, source_path: &str) -> Vec<String> {
    if source_path == "goal_summary" {
        return objective_tokens(&action.goal.summary);
    }
    metadata_value_at_path(&action.inputs, source_path)
        .map(scorecard_value_tokens)
        .unwrap_or_default()
}

pub(crate) fn scorecard_value_tokens(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => objective_tokens(text),
        Value::Array(items) => items.iter().flat_map(scorecard_value_tokens).collect(),
        other => objective_tokens(&other.to_string()),
    }
}

pub(crate) fn lease_failure_code(reason: &str) -> &'static str {
    if reason.contains("revoked") {
        failure_code_lease_revoked()
    } else if reason.contains("expired") {
        failure_code_lease_expired()
    } else {
        failure_code_approval_required()
    }
}

pub(crate) fn local_harness_failure_code(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<LocalHarnessError>() {
        return match error {
            LocalHarnessError::MissingBinary(_) => failure_code_local_harness_missing_binary(),
            LocalHarnessError::Spawn(_) => failure_code_local_harness_spawn_error(),
            LocalHarnessError::Timeout(_) => failure_code_local_harness_timeout(),
            LocalHarnessError::ExitNonZero { .. } => failure_code_local_harness_exit_nonzero(),
            LocalHarnessError::Protocol(_) => failure_code_local_harness_protocol_error(),
        };
    }
    failure_code_route_unavailable()
}

pub(crate) fn openclaw_failure_code(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<OpenClawError>() {
        return match error {
            OpenClawError::MissingAuthEnv(_) => failure_code_openclaw_auth_error(),
            OpenClawError::UnsupportedSessionMode => {
                failure_code_openclaw_unsupported_session_mode()
            }
            OpenClawError::UnsupportedWorkspaceMode => {
                failure_code_openclaw_unsupported_workspace_mode()
            }
            OpenClawError::Connect(_) => failure_code_openclaw_connect_error(),
            OpenClawError::Protocol(_) => failure_code_openclaw_protocol_error(),
            OpenClawError::RunFailed(_) => failure_code_openclaw_run_failed(),
        };
    }
    failure_code_route_unavailable()
}

pub(crate) fn a2a_failure_code(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<A2aError>() {
        return match error {
            A2aError::MissingAuthEnv(_) => failure_code_a2a_auth_error(),
            A2aError::TreatyDenied(_) => failure_code_treaty_denied(),
            A2aError::AgentCard(_) | A2aError::Connect(_) => failure_code_a2a_connect_error(),
            A2aError::Protocol(_) => failure_code_a2a_protocol_error(),
            A2aError::TaskFailed(_) => failure_code_a2a_task_failed(),
        };
    }
    failure_code_route_unavailable()
}

pub(crate) fn next_fallback_label(
    deterministic_fallback: bool,
    fallback: &'static str,
) -> &'static str {
    if deterministic_fallback {
        fallback
    } else {
        "continuity"
    }
}

pub(crate) fn set_action_failed(action: &mut Action, code: &str, reason: String) {
    action.phase = ActionPhase::Failed;
    action.finished_at = Some(now_timestamp());
    action.failure_reason = Some(reason);
    action.failure_code = Some(code.to_string());
}

pub(crate) fn set_action_blocked(action: &mut Action, code: &str, reason: String) {
    action.phase = if code == "a2a_auth_required" {
        ActionPhase::AwaitingApproval
    } else {
        ActionPhase::Blocked
    };
    action.finished_at = None;
    action.failure_reason = Some(reason);
    action.failure_code = Some(code.to_string());
}

pub(crate) fn selected_executor_from_external_refs(
    external_refs: &[ExternalRef],
) -> Option<String> {
    external_ref_value(external_refs, "a2a.remote_principal")
        .map(|principal| format!("a2a.{principal}"))
        .or_else(|| {
            external_ref_value(external_refs, "openclaw.target_agent")
                .map(|agent| format!("openclaw.{agent}"))
        })
        .or_else(|| {
            external_ref_value(external_refs, "local_harness.harness")
                .map(|harness| format!("local_harness.{harness}"))
        })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct WorkspaceLockRecord {
    pub(crate) workspace_root: String,
    pub(crate) owner_action_id: String,
    pub(crate) acquired_at: String,
}

pub(crate) struct WorkspaceLockAcquisition {
    pub(crate) lock_path: PathBuf,
    pub(crate) detail: crawfish_types::WorkspaceLockDetail,
}

pub(crate) enum WorkspaceLockAttempt {
    Acquired(WorkspaceLockAcquisition),
    Conflict(crawfish_types::WorkspaceLockDetail),
}

pub(crate) fn is_remote_harness_executor(executor: &str) -> bool {
    executor.starts_with("openclaw.") || executor.starts_with("local_harness.")
}

pub(crate) fn is_remote_agent_executor(executor: &str) -> bool {
    executor.starts_with("a2a.")
}

pub(crate) fn merge_external_refs(
    mut lhs: Vec<ExternalRef>,
    rhs: Vec<ExternalRef>,
) -> Vec<ExternalRef> {
    for reference in rhs {
        let exists = lhs.iter().any(|candidate| {
            candidate.kind == reference.kind
                && candidate.value == reference.value
                && candidate.endpoint == reference.endpoint
        });
        if !exists {
            lhs.push(reference);
        }
    }
    lhs
}

pub(crate) fn merge_artifact_refs(
    mut lhs: Vec<crawfish_types::ArtifactRef>,
    rhs: Vec<crawfish_types::ArtifactRef>,
) -> Vec<crawfish_types::ArtifactRef> {
    for artifact in rhs {
        let exists = lhs
            .iter()
            .any(|candidate| candidate.kind == artifact.kind && candidate.path == artifact.path);
        if !exists {
            lhs.push(artifact);
        }
    }
    lhs
}

pub(crate) async fn verify_task_plan_outputs(
    action: &Action,
    outputs: &ActionOutputs,
    iteration: u32,
    feedback_policy: &FeedbackPolicy,
) -> anyhow::Result<TaskPlanVerificationResult> {
    let json_artifact = outputs
        .artifacts
        .iter()
        .find(|artifact| artifact.path.ends_with("task_plan.json"))
        .cloned();
    let markdown_artifact = outputs
        .artifacts
        .iter()
        .find(|artifact| artifact.path.ends_with("task_plan.md"))
        .cloned();

    let mut failures = Vec::new();
    if json_artifact.is_none() {
        failures.push("missing task_plan.json artifact".to_string());
    }
    if markdown_artifact.is_none() {
        failures.push("missing task_plan.md artifact".to_string());
    }

    let artifact = if let Some(json_artifact) = &json_artifact {
        Some(load_json_artifact::<crawfish_types::TaskPlanArtifact>(json_artifact).await?)
    } else {
        None
    };
    let markdown = if let Some(markdown_artifact) = &markdown_artifact {
        Some(tokio::fs::read_to_string(&markdown_artifact.path).await?)
    } else {
        None
    };

    if let Some(artifact) = &artifact {
        if artifact.ordered_steps.len() < 2 {
            failures.push("task plan must contain at least two ordered steps".to_string());
        }
        if artifact.risks.is_empty() {
            failures.push("task plan must include at least one risk".to_string());
        }
        if artifact.assumptions.is_empty() {
            failures.push("task plan must include at least one assumption".to_string());
        }
        if artifact.confidence_summary.trim().is_empty() {
            failures.push("task plan must include a confidence summary".to_string());
        }
    }

    let mut combined_text = String::new();
    if let Some(artifact) = &artifact {
        combined_text.push_str(&serde_json::to_string(artifact)?);
        combined_text.push('\n');
    }
    if let Some(markdown) = &markdown {
        combined_text.push_str(markdown);
    }
    let lowered = combined_text.to_lowercase();

    if let Some(objective) = action.inputs.get("objective").and_then(Value::as_str) {
        let missing_tokens = extract_key_tokens(objective)
            .into_iter()
            .filter(|token| !lowered.contains(token))
            .collect::<Vec<_>>();
        if !missing_tokens.is_empty() {
            failures.push(format!(
                "task plan does not sufficiently cover objective tokens: {}",
                missing_tokens.join(", ")
            ));
        }
    }

    let missing_outputs = metadata_string_array(&action.inputs, "desired_outputs")
        .into_iter()
        .filter(|output| !lowered.contains(&output.to_lowercase()))
        .collect::<Vec<_>>();
    if !missing_outputs.is_empty() {
        failures.push(format!(
            "task plan does not cover desired outputs: {}",
            missing_outputs.join(", ")
        ));
    }

    if failures.is_empty() {
        return Ok(TaskPlanVerificationResult {
            passed: true,
            summary: VerificationSummary {
                status: VerificationStatus::Passed,
                iterations_completed: iteration,
                last_feedback: None,
                last_failure_code: None,
            },
            feedback: None,
        });
    }

    let feedback = build_task_plan_feedback(feedback_policy, &failures);
    Ok(TaskPlanVerificationResult {
        passed: false,
        summary: VerificationSummary {
            status: VerificationStatus::Failed,
            iterations_completed: iteration,
            last_feedback: Some(feedback.clone()),
            last_failure_code: Some(failure_code_verification_failed().to_string()),
        },
        feedback: Some(feedback),
    })
}

pub(crate) fn build_task_plan_feedback(policy: &FeedbackPolicy, failures: &[String]) -> String {
    let report = failures.join("; ");
    match policy {
        FeedbackPolicy::InjectReason => {
            format!("Address the following verification gaps: {report}")
        }
        FeedbackPolicy::AppendReport => {
            format!("Verification report:\n- {}", failures.join("\n- "))
        }
        FeedbackPolicy::Handoff => {
            format!("Verification did not pass and needs explicit operator review: {report}")
        }
    }
}

pub(crate) fn extract_key_tokens(text: &str) -> Vec<String> {
    const STOPWORDS: &[&str] = &[
        "about", "after", "around", "before", "build", "change", "changes", "check", "checks",
        "ensure", "from", "into", "plan", "safe", "task", "that", "the", "this", "with",
    ];

    let mut tokens = text
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter_map(|token| {
            let lowered = token.trim().to_lowercase();
            if lowered.len() < 4 || STOPWORDS.contains(&lowered.as_str()) {
                return None;
            }
            Some(lowered)
        })
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    tokens.truncate(3);
    tokens
}

pub(crate) fn metadata_string_array(metadata: &crawfish_types::Metadata, key: &str) -> Vec<String> {
    metadata
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

impl Supervisor {
    pub(crate) async fn write_checkpoint_for_action(
        &self,
        action: &mut Action,
        checkpoint: &DeterministicCheckpoint,
    ) -> anyhow::Result<()> {
        let checkpoint_ref = checkpoint_ref_for_executor(&checkpoint.executor_kind);
        let payload = serde_json::to_vec_pretty(checkpoint)?;
        self.store
            .put_checkpoint(&action.id, &checkpoint_ref, &payload)
            .await?;
        action.checkpoint_ref = Some(checkpoint_ref);
        action.recovery_stage = Some(checkpoint.stage.clone());
        self.store.upsert_action(action).await?;
        Ok(())
    }

    pub(crate) async fn load_deterministic_checkpoint(
        &self,
        action: &Action,
    ) -> anyhow::Result<Option<DeterministicCheckpoint>> {
        let Some(bytes) = self.store.get_checkpoint(&action.id).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    pub(crate) async fn run_deterministic_executor<E>(
        &self,
        action: &mut Action,
        executor_kind: &str,
        running_stage: &str,
        external_refs: Vec<ExternalRef>,
        executor: &E,
    ) -> anyhow::Result<ExecutionOutcome>
    where
        E: DeterministicExecutor,
    {
        let digest = input_digest(&action.inputs)?;
        if let Some(checkpoint) = self.load_deterministic_checkpoint(action).await? {
            if checkpoint.executor_kind == executor_kind
                && checkpoint.input_digest == digest
                && checkpoint.stage == "completed"
                && artifact_refs_exist(&checkpoint.artifact_refs)
            {
                action.selected_executor = Some(executor_kind.to_string());
                action.recovery_stage = Some(checkpoint.stage.clone());
                action.external_refs = external_refs.clone();
                return Ok(ExecutionOutcome::Completed {
                    outputs: recovered_outputs_from_checkpoint(&checkpoint),
                    selected_executor: executor_kind.to_string(),
                    checkpoint: Some(checkpoint),
                    external_refs,
                    surface_events: Vec::new(),
                });
            }
        }

        let running_checkpoint =
            build_checkpoint(action, executor_kind, running_stage, Vec::new())?;
        self.write_checkpoint_for_action(action, &running_checkpoint)
            .await?;
        self.store
            .append_action_event(
                &action.id,
                "checkpointed",
                serde_json::json!({
                    "stage": running_checkpoint.stage,
                    "checkpoint_ref": action.checkpoint_ref,
                }),
            )
            .await?;

        let outputs = executor.execute(action).await?;
        let completed_checkpoint = build_checkpoint(
            action,
            executor_kind,
            "completed",
            outputs.artifacts.clone(),
        )?;

        Ok(ExecutionOutcome::Completed {
            outputs,
            selected_executor: executor_kind.to_string(),
            checkpoint: Some(completed_checkpoint),
            external_refs,
            surface_events: Vec::new(),
        })
    }

    pub(crate) async fn execute_task_plan(
        &self,
        action: &mut Action,
        manifest: &AgentManifest,
    ) -> anyhow::Result<ExecutionOutcome> {
        match action
            .execution_strategy
            .as_ref()
            .map(|strategy| strategy.mode.clone())
            .unwrap_or(ExecutionStrategyMode::SinglePass)
        {
            ExecutionStrategyMode::VerifyLoop => {
                let strategy = action
                    .execution_strategy
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("verify_loop requires an execution strategy"))?;
                self.execute_task_plan_verify_loop(action, manifest, &strategy)
                    .await
            }
            ExecutionStrategyMode::SinglePass => {
                self.execute_task_plan_single_pass(action, manifest).await
            }
        }
    }

    pub(crate) async fn execute_task_plan_verify_loop(
        &self,
        action: &mut Action,
        manifest: &AgentManifest,
        strategy: &ExecutionStrategy,
    ) -> anyhow::Result<ExecutionOutcome> {
        if strategy
            .verification_spec
            .as_ref()
            .map(|spec| !spec.checks.is_empty())
            .unwrap_or(false)
        {
            return Ok(ExecutionOutcome::Failed {
                reason: "task.plan verify_loop only supports built-in verification checks in alpha"
                    .to_string(),
                failure_code: failure_code_verification_spec_invalid().to_string(),
                outputs: ActionOutputs::default(),
                checkpoint: None,
                external_refs: Vec::new(),
                surface_events: Vec::new(),
            });
        }

        let max_iterations = strategy
            .stop_budget
            .as_ref()
            .map(|budget| budget.max_iterations)
            .unwrap_or(3)
            .max(1);
        let on_failure = strategy
            .verification_spec
            .as_ref()
            .map(|spec| spec.on_failure.clone())
            .unwrap_or(VerifyLoopFailureMode::RetryWithFeedback);

        let mut start_iteration = 1;
        let mut carried_feedback = None;
        let mut previous_artifact_refs = Vec::new();
        let mut aggregated_external_refs = action.external_refs.clone();

        if let Some(checkpoint) = self.load_deterministic_checkpoint(action).await? {
            if let Some(strategy_state) = checkpoint.strategy_state.clone() {
                aggregated_external_refs =
                    merge_external_refs(aggregated_external_refs, action.external_refs.clone());
                match (
                    checkpoint.stage.as_str(),
                    strategy_state
                        .verification_summary
                        .as_ref()
                        .map(|summary| summary.status.clone()),
                ) {
                    ("completed", Some(VerificationStatus::Passed))
                        if artifact_refs_exist(&checkpoint.artifact_refs) =>
                    {
                        action.selected_executor = Some(checkpoint.executor_kind.clone());
                        action.recovery_stage = Some(checkpoint.stage.clone());
                        action.external_refs = aggregated_external_refs.clone();
                        return Ok(ExecutionOutcome::Completed {
                            outputs: recovered_outputs_from_checkpoint(&checkpoint),
                            selected_executor: checkpoint.executor_kind.clone(),
                            checkpoint: Some(checkpoint),
                            external_refs: aggregated_external_refs,
                            surface_events: Vec::new(),
                        });
                    }
                    ("completed", Some(VerificationStatus::Failed)) => {
                        start_iteration = strategy_state.iteration.saturating_add(1);
                        carried_feedback = strategy_state.verification_feedback.clone();
                        previous_artifact_refs = merge_artifact_refs(
                            strategy_state.previous_artifact_refs.clone(),
                            checkpoint.artifact_refs.clone(),
                        );
                    }
                    ("verification_failed", Some(VerificationStatus::Failed)) => {
                        start_iteration = strategy_state.iteration.saturating_add(1);
                        carried_feedback = strategy_state.verification_feedback.clone();
                        previous_artifact_refs = merge_artifact_refs(
                            strategy_state.previous_artifact_refs.clone(),
                            checkpoint.artifact_refs.clone(),
                        );
                    }
                    ("planning", _) | ("completed", None) => {
                        start_iteration = strategy_state.iteration.max(1);
                        carried_feedback = strategy_state.verification_feedback.clone();
                        previous_artifact_refs = merge_artifact_refs(
                            strategy_state.previous_artifact_refs.clone(),
                            checkpoint.artifact_refs.clone(),
                        );
                    }
                    (
                        "verification_budget_exhausted",
                        Some(VerificationStatus::BudgetExhausted),
                    ) => {
                        start_iteration = strategy_state.iteration.saturating_add(1);
                        carried_feedback = strategy_state.verification_feedback.clone();
                        previous_artifact_refs = merge_artifact_refs(
                            strategy_state.previous_artifact_refs.clone(),
                            checkpoint.artifact_refs.clone(),
                        );
                    }
                    _ => {}
                }
            }
        }

        if start_iteration > max_iterations {
            let summary = VerificationSummary {
                status: VerificationStatus::BudgetExhausted,
                iterations_completed: max_iterations,
                last_feedback: carried_feedback.clone(),
                last_failure_code: Some(failure_code_verification_budget_exhausted().to_string()),
            };
            let mut outputs = ActionOutputs {
                summary: Some(
                    "Verification budget exhausted before a fresh iteration could start"
                        .to_string(),
                ),
                artifacts: previous_artifact_refs.clone(),
                ..ActionOutputs::default()
            };
            outputs.metadata.insert(
                "strategy_mode".to_string(),
                serde_json::json!("verify_loop"),
            );
            outputs.metadata.insert(
                "verification_summary".to_string(),
                serde_json::to_value(&summary)?,
            );
            let mut checkpoint = build_checkpoint(
                action,
                action
                    .selected_executor
                    .as_deref()
                    .unwrap_or("verify_loop.task_plan"),
                "verification_budget_exhausted",
                previous_artifact_refs,
            )?;
            checkpoint.strategy_state = Some(StrategyCheckpointState {
                mode: ExecutionStrategyMode::VerifyLoop,
                iteration: max_iterations,
                verification_feedback: carried_feedback.clone(),
                previous_artifact_refs: Vec::new(),
                verification_summary: Some(summary),
            });
            return Ok(ExecutionOutcome::Failed {
                reason: carried_feedback
                    .unwrap_or_else(|| "task.plan exhausted its verification budget".to_string()),
                failure_code: failure_code_verification_budget_exhausted().to_string(),
                outputs,
                checkpoint: Some(checkpoint),
                external_refs: aggregated_external_refs,
                surface_events: Vec::new(),
            });
        }

        for iteration in start_iteration..=max_iterations {
            self.store
                .append_action_event(
                    &action.id,
                    "verify_loop_iteration_started",
                    serde_json::json!({
                        "iteration": iteration,
                        "max_iterations": max_iterations,
                        "strategy_mode": "verify_loop",
                        "feedback_present": carried_feedback.is_some(),
                    }),
                )
                .await?;

            let mut iteration_action = action.clone();
            if let Some(feedback) = &carried_feedback {
                iteration_action.inputs.insert(
                    "verification_feedback".to_string(),
                    serde_json::json!(feedback),
                );
            } else {
                iteration_action.inputs.remove("verification_feedback");
            }

            let outcome = self
                .execute_task_plan_single_pass(&mut iteration_action, manifest)
                .await?;

            match outcome {
                ExecutionOutcome::Completed {
                    mut outputs,
                    selected_executor,
                    checkpoint,
                    external_refs,
                    surface_events,
                } => {
                    aggregated_external_refs =
                        merge_external_refs(aggregated_external_refs, external_refs);
                    for event in surface_events {
                        self.store
                            .append_action_event(&action.id, &event.event_type, event.payload)
                            .await?;
                    }

                    let verification = verify_task_plan_outputs(
                        &iteration_action,
                        &outputs,
                        iteration,
                        &strategy.feedback_policy,
                    )
                    .await?;
                    let mut checkpoint = checkpoint.unwrap_or(build_checkpoint(
                        &iteration_action,
                        &selected_executor,
                        "completed",
                        outputs.artifacts.clone(),
                    )?);

                    checkpoint.stage = if verification.passed {
                        "completed".to_string()
                    } else {
                        "verification_failed".to_string()
                    };
                    checkpoint.strategy_state = Some(StrategyCheckpointState {
                        mode: ExecutionStrategyMode::VerifyLoop,
                        iteration,
                        verification_feedback: verification.feedback.clone(),
                        previous_artifact_refs: previous_artifact_refs.clone(),
                        verification_summary: Some(verification.summary.clone()),
                    });
                    self.write_checkpoint_for_action(action, &checkpoint)
                        .await?;

                    self.store
                        .append_action_event(
                            &action.id,
                            "verify_loop_iteration_completed",
                            serde_json::json!({
                                "iteration": iteration,
                                "selected_executor": selected_executor,
                                "status": if verification.passed { "passed" } else { "failed" },
                            }),
                        )
                        .await?;

                    if verification.passed {
                        self.record_verification_evaluation(
                            action,
                            iteration,
                            &verification.summary,
                            verification.feedback.as_ref(),
                        )
                        .await?;
                        self.store
                            .append_action_event(
                                &action.id,
                                "verification_passed",
                                serde_json::json!({
                                    "iteration": iteration,
                                    "summary": verification.summary.clone(),
                                }),
                            )
                            .await?;
                        outputs.metadata.insert(
                            "strategy_mode".to_string(),
                            serde_json::json!("verify_loop"),
                        );
                        outputs.metadata.insert(
                            "strategy_iteration".to_string(),
                            serde_json::json!(iteration),
                        );
                        outputs.metadata.insert(
                            "verification_summary".to_string(),
                            serde_json::to_value(
                                checkpoint
                                    .strategy_state
                                    .as_ref()
                                    .and_then(|state| state.verification_summary.clone())
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("missing verification summary")
                                    })?,
                            )?,
                        );
                        return Ok(ExecutionOutcome::Completed {
                            outputs,
                            selected_executor,
                            checkpoint: Some(checkpoint),
                            external_refs: aggregated_external_refs,
                            surface_events: Vec::new(),
                        });
                    }

                    self.store
                        .append_action_event(
                            &action.id,
                            "verification_failed",
                            serde_json::json!({
                                "iteration": iteration,
                                "summary": verification.summary.clone(),
                                "feedback": verification.feedback.clone(),
                            }),
                        )
                        .await?;
                    self.record_verification_evaluation(
                        action,
                        iteration,
                        &verification.summary,
                        verification.feedback.as_ref(),
                    )
                    .await?;

                    previous_artifact_refs =
                        merge_artifact_refs(previous_artifact_refs, outputs.artifacts.clone());
                    carried_feedback = verification.feedback.clone();

                    match on_failure {
                        VerifyLoopFailureMode::RetryWithFeedback if iteration < max_iterations => {
                            continue;
                        }
                        VerifyLoopFailureMode::HumanHandoff => {
                            return Ok(ExecutionOutcome::Blocked {
                                reason: carried_feedback.clone().unwrap_or_else(|| {
                                    "task.plan requires human handoff after verification failed"
                                        .to_string()
                                }),
                                failure_code: failure_code_verification_failed().to_string(),
                                continuity_mode: Some(ContinuityModeName::HumanHandoff),
                                outputs,
                                external_refs: aggregated_external_refs,
                                surface_events: Vec::new(),
                            });
                        }
                        VerifyLoopFailureMode::Fail => {
                            outputs.metadata.insert(
                                "strategy_mode".to_string(),
                                serde_json::json!("verify_loop"),
                            );
                            outputs.metadata.insert(
                                "strategy_iteration".to_string(),
                                serde_json::json!(iteration),
                            );
                            outputs.metadata.insert(
                                "verification_summary".to_string(),
                                serde_json::to_value(
                                    checkpoint
                                        .strategy_state
                                        .as_ref()
                                        .and_then(|state| state.verification_summary.clone())
                                        .ok_or_else(|| {
                                            anyhow::anyhow!("missing verification summary")
                                        })?,
                                )?,
                            );
                            return Ok(ExecutionOutcome::Failed {
                                reason: carried_feedback.clone().unwrap_or_else(|| {
                                    "task.plan failed deterministic verification".to_string()
                                }),
                                failure_code: failure_code_verification_failed().to_string(),
                                outputs,
                                checkpoint: Some(checkpoint),
                                external_refs: aggregated_external_refs,
                                surface_events: Vec::new(),
                            });
                        }
                        VerifyLoopFailureMode::RetryWithFeedback => {}
                    }
                }
                ExecutionOutcome::Blocked {
                    reason,
                    failure_code,
                    continuity_mode,
                    outputs,
                    external_refs,
                    surface_events,
                } => {
                    return Ok(ExecutionOutcome::Blocked {
                        reason,
                        failure_code,
                        continuity_mode,
                        outputs,
                        external_refs: merge_external_refs(aggregated_external_refs, external_refs),
                        surface_events,
                    });
                }
                ExecutionOutcome::Failed {
                    reason,
                    failure_code,
                    outputs,
                    checkpoint,
                    external_refs,
                    surface_events,
                } => {
                    return Ok(ExecutionOutcome::Failed {
                        reason,
                        failure_code,
                        outputs,
                        checkpoint,
                        external_refs: merge_external_refs(aggregated_external_refs, external_refs),
                        surface_events,
                    });
                }
            }
        }

        let summary = VerificationSummary {
            status: VerificationStatus::BudgetExhausted,
            iterations_completed: max_iterations,
            last_feedback: carried_feedback.clone(),
            last_failure_code: Some(failure_code_verification_budget_exhausted().to_string()),
        };
        self.store
            .append_action_event(
                &action.id,
                "verification_budget_exhausted",
                serde_json::json!({
                    "iterations_completed": max_iterations,
                    "summary": summary,
                }),
            )
            .await?;
        self.record_verification_evaluation(
            action,
            max_iterations,
            &summary,
            carried_feedback.as_ref(),
        )
        .await?;
        let mut outputs = ActionOutputs {
            summary: Some("task.plan exhausted its verification budget".to_string()),
            artifacts: previous_artifact_refs.clone(),
            ..ActionOutputs::default()
        };
        outputs.metadata.insert(
            "strategy_mode".to_string(),
            serde_json::json!("verify_loop"),
        );
        outputs.metadata.insert(
            "strategy_iteration".to_string(),
            serde_json::json!(max_iterations),
        );
        outputs.metadata.insert(
            "verification_summary".to_string(),
            serde_json::to_value(&summary)?,
        );
        let mut checkpoint = build_checkpoint(
            action,
            action
                .selected_executor
                .as_deref()
                .unwrap_or("verify_loop.task_plan"),
            "verification_budget_exhausted",
            previous_artifact_refs,
        )?;
        checkpoint.strategy_state = Some(StrategyCheckpointState {
            mode: ExecutionStrategyMode::VerifyLoop,
            iteration: max_iterations,
            verification_feedback: carried_feedback.clone(),
            previous_artifact_refs: Vec::new(),
            verification_summary: Some(summary),
        });
        Ok(ExecutionOutcome::Failed {
            reason: carried_feedback
                .unwrap_or_else(|| "task.plan exhausted its verification budget".to_string()),
            failure_code: failure_code_verification_budget_exhausted().to_string(),
            outputs,
            checkpoint: Some(checkpoint),
            external_refs: aggregated_external_refs,
            surface_events: Vec::new(),
        })
    }

    pub(crate) async fn execute_task_plan_single_pass(
        &self,
        action: &mut Action,
        manifest: &AgentManifest,
    ) -> anyhow::Result<ExecutionOutcome> {
        let has_active_followup = active_remote_followup_ref_for_action(action).is_some();
        let deterministic_fallback = !has_active_followup
            && action
                .contract
                .execution
                .fallback_chain
                .iter()
                .any(|route| route == "deterministic");
        let mut attempted_agentic_route = false;
        let mut last_reason: Option<String> = None;
        let mut last_external_refs = Vec::new();
        let preferred_routes = if has_active_followup {
            vec!["a2a".to_string()]
        } else {
            action.contract.execution.preferred_harnesses.clone()
        };

        for route in preferred_routes {
            match route.as_str() {
                "claude_code" | "codex" => {
                    attempted_agentic_route = true;
                    let harness = if route == "claude_code" {
                        LocalHarnessKind::ClaudeCode
                    } else {
                        LocalHarnessKind::Codex
                    };
                    match self.resolve_local_harness_adapter(manifest, harness)? {
                        Some((adapter, base_refs)) => {
                            if adapter.binding().lease_required {
                                self.ensure_required_lease_valid(action, &base_refs).await?;
                            }
                            match adapter.run(action).await {
                                Ok(result) => {
                                    return Ok(ExecutionOutcome::Completed {
                                        outputs: result.outputs,
                                        selected_executor: format!(
                                            "local_harness.{}",
                                            adapter.name()
                                        ),
                                        checkpoint: None,
                                        external_refs: merge_external_refs(
                                            base_refs,
                                            result.external_refs,
                                        ),
                                        surface_events: result.events,
                                    });
                                }
                                Err(error) => {
                                    let reason = error.to_string();
                                    self.store
                                        .append_action_event(
                                            &action.id,
                                            "route_degraded",
                                            serde_json::json!({
                                                "selected_surface": route,
                                                "reason": reason,
                                                "code": local_harness_failure_code(&error),
                                                "fallback": next_fallback_label(deterministic_fallback, "deterministic"),
                                            }),
                                        )
                                        .await?;
                                    last_reason = Some(error.to_string());
                                    last_external_refs = base_refs;
                                }
                            }
                        }
                        None => {
                            self.store
                                .append_action_event(
                                    &action.id,
                                    "route_degraded",
                                    serde_json::json!({
                                        "selected_surface": route,
                                        "reason": format!("no local harness binding is configured for {route}"),
                                        "code": failure_code_route_unavailable(),
                                        "fallback": next_fallback_label(deterministic_fallback, "deterministic"),
                                    }),
                                )
                                .await?;
                        }
                    }
                }
                "a2a" => {
                    attempted_agentic_route = true;
                    match self.resolve_a2a_adapter(manifest) {
                        Ok(Some((adapter, mut base_refs))) => {
                            let treaty_decision = match self.compile_treaty_decision(
                                action,
                                adapter.binding(),
                                adapter.treaty_pack(),
                            ) {
                                Ok(decision) => decision,
                                Err(error) => {
                                    let reason = error.to_string();
                                    self.store
                                        .append_action_event(
                                            &action.id,
                                            "route_degraded",
                                            serde_json::json!({
                                                "selected_surface": "a2a",
                                                "reason": reason,
                                                "code": failure_code_treaty_denied(),
                                                "fallback": next_fallback_label(deterministic_fallback, "openclaw"),
                                            }),
                                        )
                                        .await?;
                                    self.store
                                        .insert_policy_incident(&PolicyIncident {
                                            id: Uuid::new_v4().to_string(),
                                            action_id: action.id.clone(),
                                            doctrine_pack_id: "swarm_frontier_v1".to_string(),
                                            jurisdiction: JurisdictionClass::ExternalUnknown,
                                            reason_code: "treaty_denied".to_string(),
                                            summary: reason.clone(),
                                            severity: PolicyIncidentSeverity::Critical,
                                            checkpoint: Some(OversightCheckpoint::PreDispatch),
                                            created_at: now_timestamp(),
                                        })
                                        .await?;
                                    last_reason = Some(reason);
                                    last_external_refs = base_refs;
                                    continue;
                                }
                            };
                            let federation_pack = match self
                                .resolve_federation_pack(adapter.binding(), adapter.treaty_pack())
                            {
                                Ok(pack) => pack,
                                Err(error) => {
                                    let reason = error.to_string();
                                    self.store
                                        .append_action_event(
                                            &action.id,
                                            "route_degraded",
                                            serde_json::json!({
                                                "selected_surface": "a2a",
                                                "reason": reason,
                                                "code": "frontier_enforcement_gap",
                                                "fallback": next_fallback_label(deterministic_fallback, "openclaw"),
                                            }),
                                        )
                                        .await?;
                                    self.store
                                        .insert_policy_incident(&PolicyIncident {
                                            id: Uuid::new_v4().to_string(),
                                            action_id: action.id.clone(),
                                            doctrine_pack_id: "remote_agent_treaty_v1".to_string(),
                                            jurisdiction: JurisdictionClass::ExternalUnknown,
                                            reason_code: "frontier_enforcement_gap".to_string(),
                                            summary: reason.clone(),
                                            severity: PolicyIncidentSeverity::Critical,
                                            checkpoint: Some(OversightCheckpoint::PreDispatch),
                                            created_at: now_timestamp(),
                                        })
                                        .await?;
                                    last_reason = Some(reason);
                                    last_external_refs = base_refs;
                                    continue;
                                }
                            };
                            let federation_decision = match self.compile_federation_decision(
                                action,
                                &treaty_decision,
                                &federation_pack,
                            ) {
                                Ok(decision) => decision,
                                Err(error) => {
                                    let reason = error.to_string();
                                    self.store
                                        .append_action_event(
                                            &action.id,
                                            "route_degraded",
                                            serde_json::json!({
                                                "selected_surface": "a2a",
                                                "reason": reason,
                                                "code": "frontier_enforcement_gap",
                                                "fallback": next_fallback_label(deterministic_fallback, "openclaw"),
                                            }),
                                        )
                                        .await?;
                                    self.store
                                        .insert_policy_incident(&PolicyIncident {
                                            id: Uuid::new_v4().to_string(),
                                            action_id: action.id.clone(),
                                            doctrine_pack_id: "remote_agent_treaty_v1".to_string(),
                                            jurisdiction: JurisdictionClass::ExternalUnknown,
                                            reason_code: "frontier_enforcement_gap".to_string(),
                                            summary: reason.clone(),
                                            severity: PolicyIncidentSeverity::Critical,
                                            checkpoint: Some(OversightCheckpoint::PreDispatch),
                                            created_at: now_timestamp(),
                                        })
                                        .await?;
                                    last_reason = Some(reason);
                                    last_external_refs = base_refs;
                                    continue;
                                }
                            };
                            let mut receipt = crawfish_types::DelegationReceipt {
                                id: Uuid::new_v4().to_string(),
                                action_id: action.id.clone(),
                                treaty_pack_id: treaty_decision.treaty_pack_id.clone(),
                                remote_principal: treaty_decision.remote_principal.clone(),
                                capability: action.capability.clone(),
                                requested_scopes: treaty_decision.requested_scopes.clone(),
                                delegated_data_scopes: treaty_decision
                                    .delegated_data_scopes
                                    .clone(),
                                decision: crawfish_types::DelegationDecision::Allowed,
                                remote_agent_card_url: adapter.binding().agent_card_url.clone(),
                                remote_task_ref: None,
                                delegation_depth: Some(treaty_decision.delegation_depth),
                                created_at: now_timestamp(),
                            };
                            self.store.insert_delegation_receipt(&receipt).await?;
                            base_refs.push(ExternalRef {
                                kind: "a2a.treaty_pack".to_string(),
                                value: adapter.treaty_pack().id.clone(),
                                endpoint: None,
                            });
                            base_refs.push(ExternalRef {
                                kind: "a2a.delegation_receipt".to_string(),
                                value: receipt.id.clone(),
                                endpoint: None,
                            });
                            base_refs.push(ExternalRef {
                                kind: "a2a.delegation_depth".to_string(),
                                value: treaty_decision.delegation_depth.to_string(),
                                endpoint: None,
                            });
                            base_refs.push(ExternalRef {
                                kind: "a2a.federation_pack".to_string(),
                                value: federation_pack.id.clone(),
                                endpoint: None,
                            });
                            let followup_request_ref =
                                active_remote_followup_ref_for_action(action);
                            let attempt_created_at = now_timestamp();
                            let mut attempt_record = RemoteAttemptRecord {
                                id: Uuid::new_v4().to_string(),
                                action_id: action.id.clone(),
                                attempt: self
                                    .store
                                    .list_remote_attempt_records(&action.id)
                                    .await?
                                    .len() as u32
                                    + 1,
                                capability: action.capability.clone(),
                                interaction_model: Some(
                                    crawfish_types::InteractionModel::RemoteAgent,
                                ),
                                executor: Some("a2a".to_string()),
                                remote_principal: Some(treaty_decision.remote_principal.clone()),
                                treaty_pack_id: Some(treaty_decision.treaty_pack_id.clone()),
                                federation_pack_id: Some(federation_pack.id.clone()),
                                remote_task_ref: None,
                                remote_evidence_ref: None,
                                followup_request_ref: followup_request_ref.clone(),
                                created_at: attempt_created_at,
                                completed_at: None,
                            };
                            self.store
                                .upsert_remote_attempt_record(&attempt_record)
                                .await?;
                            base_refs.push(ExternalRef {
                                kind: "a2a.remote_attempt".to_string(),
                                value: attempt_record.id.clone(),
                                endpoint: None,
                            });
                            match adapter.run(action).await {
                                Ok(mut result) => {
                                    let merged_external_refs =
                                        merge_external_refs(base_refs, result.external_refs);
                                    attempt_record.remote_task_ref =
                                        external_ref_value(&merged_external_refs, "a2a.task_id");
                                    attempt_record.completed_at = Some(now_timestamp());
                                    self.store
                                        .upsert_remote_attempt_record(&attempt_record)
                                        .await?;
                                    receipt.remote_task_ref =
                                        external_ref_value(&merged_external_refs, "a2a.task_id");
                                    self.store.insert_delegation_receipt(&receipt).await?;
                                    match result
                                        .outputs
                                        .metadata
                                        .get("a2a_remote_state")
                                        .and_then(Value::as_str)
                                        .unwrap_or("completed")
                                    {
                                        "blocked" => {
                                            let mut decision = federation_decision.clone();
                                            decision.remote_state_disposition =
                                                Some(federation_pack.blocked_remote_policy.clone());
                                            let mut outputs = result.outputs;
                                            set_federation_result_metadata(
                                                &mut outputs,
                                                &decision,
                                                None,
                                                decision.remote_state_disposition.as_ref(),
                                                None,
                                            );
                                            let mut surface_events = result.events;
                                            surface_events.push(
                                                crawfish_core::SurfaceActionEvent {
                                                    event_type: "federation_state_escalated"
                                                        .to_string(),
                                                    payload: serde_json::json!({
                                                        "timestamp": now_timestamp(),
                                                        "federation_pack_id": federation_pack.id.clone(),
                                                        "remote_state": "input-required",
                                                        "disposition": runtime_enum_to_snake(
                                                            decision
                                                                .remote_state_disposition
                                                                .as_ref()
                                                                .expect("remote state disposition"),
                                                        ),
                                                    }),
                                                },
                                            );
                                            let reason =
                                                outputs.summary.clone().unwrap_or_else(|| {
                                                    "remote A2A agent requested additional input"
                                                        .to_string()
                                                });
                                            return match decision
                                                .remote_state_disposition
                                                .clone()
                                                .unwrap_or(RemoteStateDisposition::Blocked)
                                            {
                                                RemoteStateDisposition::Blocked => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_input_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::AwaitingApproval => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_auth_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Failed => {
                                                    Ok(ExecutionOutcome::Failed {
                                                        reason,
                                                        failure_code: "remote_state_escalated"
                                                            .to_string(),
                                                        outputs,
                                                        checkpoint: None,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Running => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_input_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                            };
                                        }
                                        "awaiting_approval" => {
                                            let mut decision = federation_decision.clone();
                                            decision.remote_state_disposition =
                                                Some(federation_pack.auth_required_policy.clone());
                                            let mut outputs = result.outputs;
                                            set_federation_result_metadata(
                                                &mut outputs,
                                                &decision,
                                                None,
                                                decision.remote_state_disposition.as_ref(),
                                                None,
                                            );
                                            let mut surface_events = result.events;
                                            surface_events.push(
                                                crawfish_core::SurfaceActionEvent {
                                                    event_type: "federation_state_escalated"
                                                        .to_string(),
                                                    payload: serde_json::json!({
                                                        "timestamp": now_timestamp(),
                                                        "federation_pack_id": federation_pack.id.clone(),
                                                        "remote_state": "auth-required",
                                                        "disposition": runtime_enum_to_snake(
                                                            decision
                                                                .remote_state_disposition
                                                                .as_ref()
                                                                .expect("remote state disposition"),
                                                        ),
                                                    }),
                                                },
                                            );
                                            let reason =
                                                outputs.summary.clone().unwrap_or_else(|| {
                                                    "remote A2A agent requested authorization"
                                                        .to_string()
                                                });
                                            return match decision
                                                .remote_state_disposition
                                                .clone()
                                                .unwrap_or(RemoteStateDisposition::AwaitingApproval)
                                            {
                                                RemoteStateDisposition::AwaitingApproval => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_auth_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Blocked => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_input_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Failed => {
                                                    Ok(ExecutionOutcome::Failed {
                                                        reason,
                                                        failure_code: "remote_state_escalated"
                                                            .to_string(),
                                                        outputs,
                                                        checkpoint: None,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Running => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_auth_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                            };
                                        }
                                        "failed" => {
                                            let mut decision = federation_decision.clone();
                                            decision.remote_state_disposition =
                                                Some(federation_pack.remote_failure_policy.clone());
                                            let mut outputs = result.outputs;
                                            set_federation_result_metadata(
                                                &mut outputs,
                                                &decision,
                                                None,
                                                decision.remote_state_disposition.as_ref(),
                                                None,
                                            );
                                            let mut surface_events = result.events;
                                            surface_events.push(
                                                crawfish_core::SurfaceActionEvent {
                                                    event_type: "federation_state_escalated"
                                                        .to_string(),
                                                    payload: serde_json::json!({
                                                        "timestamp": now_timestamp(),
                                                        "federation_pack_id": federation_pack.id.clone(),
                                                        "remote_state": "failed",
                                                        "disposition": runtime_enum_to_snake(
                                                            decision
                                                                .remote_state_disposition
                                                                .as_ref()
                                                                .expect("remote state disposition"),
                                                        ),
                                                    }),
                                                },
                                            );
                                            let reason =
                                                outputs.summary.clone().unwrap_or_else(|| {
                                                    "remote A2A task failed".to_string()
                                                });
                                            return match decision
                                                .remote_state_disposition
                                                .clone()
                                                .unwrap_or(RemoteStateDisposition::Failed)
                                            {
                                                RemoteStateDisposition::Failed => {
                                                    Ok(ExecutionOutcome::Failed {
                                                        reason,
                                                        failure_code: failure_code_a2a_task_failed(
                                                        )
                                                        .to_string(),
                                                        outputs,
                                                        checkpoint: None,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Blocked => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_input_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::AwaitingApproval => {
                                                    Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: "a2a_auth_required"
                                                            .to_string(),
                                                        continuity_mode: None,
                                                        outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                                RemoteStateDisposition::Running => {
                                                    Ok(ExecutionOutcome::Failed {
                                                        reason,
                                                        failure_code: failure_code_a2a_task_failed(
                                                        )
                                                        .to_string(),
                                                        outputs,
                                                        checkpoint: None,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    })
                                                }
                                            };
                                        }
                                        _ => {
                                            let (disposition, evidence_status, violations) = self
                                                .evaluate_federation_post_result(
                                                    &result.outputs,
                                                    &receipt,
                                                    &treaty_decision,
                                                    adapter.treaty_pack(),
                                                    &federation_pack,
                                                );
                                            let mut decision = federation_decision.clone();
                                            decision.remote_evidence_status =
                                                Some(evidence_status.clone());
                                            decision.remote_result_acceptance = Some(match disposition {
                                                crawfish_types::RemoteOutcomeDisposition::Accepted => {
                                                    RemoteResultAcceptance::Accepted
                                                }
                                                crawfish_types::RemoteOutcomeDisposition::ReviewRequired => {
                                                    RemoteResultAcceptance::ReviewRequired
                                                }
                                                crawfish_types::RemoteOutcomeDisposition::Rejected => {
                                                    RemoteResultAcceptance::Rejected
                                                }
                                            });
                                            set_treaty_result_metadata(
                                                &mut result.outputs,
                                                &disposition,
                                                &violations,
                                            );
                                            set_federation_result_metadata(
                                                &mut result.outputs,
                                                &decision,
                                                Some(&evidence_status),
                                                None,
                                                decision.remote_result_acceptance.as_ref(),
                                            );
                                            let mut surface_events = result.events.clone();
                                            surface_events.push(crawfish_core::SurfaceActionEvent {
                                                event_type: "treaty_post_result_assessed".to_string(),
                                                payload: serde_json::json!({
                                                    "timestamp": now_timestamp(),
                                                    "disposition": runtime_enum_to_snake(&disposition),
                                                    "violation_count": violations.len(),
                                                    "treaty_pack_id": treaty_decision.treaty_pack_id,
                                                }),
                                            });
                                            surface_events.push(crawfish_core::SurfaceActionEvent {
                                                event_type: "federation_post_result_assessed".to_string(),
                                                payload: serde_json::json!({
                                                    "timestamp": now_timestamp(),
                                                    "federation_pack_id": federation_pack.id.clone(),
                                                    "remote_evidence_status": runtime_enum_to_snake(&evidence_status),
                                                    "remote_result_acceptance": runtime_enum_to_snake(
                                                        decision
                                                            .remote_result_acceptance
                                                            .as_ref()
                                                            .expect("remote result acceptance"),
                                                    ),
                                                    "violation_count": violations.len(),
                                                }),
                                            });
                                            match disposition {
                                                crawfish_types::RemoteOutcomeDisposition::Accepted => {
                                                    return Ok(ExecutionOutcome::Completed {
                                                        outputs: result.outputs,
                                                        selected_executor: format!(
                                                            "a2a.{}",
                                                            adapter.name()
                                                        ),
                                                        checkpoint: None,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    });
                                                }
                                                crawfish_types::RemoteOutcomeDisposition::ReviewRequired => {
                                                    let reason = violations
                                                        .first()
                                                        .map(|violation| violation.summary.clone())
                                                        .unwrap_or_else(|| {
                                                            "remote result requires treaty review before acceptance".to_string()
                                                        });
                                                    let code = violations
                                                        .first()
                                                        .map(|violation| violation.code.clone())
                                                        .unwrap_or_else(|| {
                                                            "frontier_enforcement_gap".to_string()
                                                        });
                                                    return Ok(ExecutionOutcome::Blocked {
                                                        reason,
                                                        failure_code: code,
                                                        continuity_mode: None,
                                                        outputs: result.outputs,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    });
                                                }
                                                crawfish_types::RemoteOutcomeDisposition::Rejected => {
                                                    let reason = violations
                                                        .first()
                                                        .map(|violation| violation.summary.clone())
                                                        .unwrap_or_else(|| {
                                                            "remote result was rejected by treaty governance".to_string()
                                                        });
                                                    let code = violations
                                                        .first()
                                                        .map(|violation| violation.code.clone())
                                                        .unwrap_or_else(|| "treaty_scope_violation".to_string());
                                                    return Ok(ExecutionOutcome::Failed {
                                                        reason,
                                                        failure_code: code,
                                                        outputs: result.outputs,
                                                        checkpoint: None,
                                                        external_refs: merged_external_refs,
                                                        surface_events,
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(error) => {
                                    let reason = error.to_string();
                                    let code = a2a_failure_code(&error);
                                    attempt_record.remote_task_ref =
                                        external_ref_value(&base_refs, "a2a.task_id");
                                    attempt_record.completed_at = Some(now_timestamp());
                                    self.store
                                        .upsert_remote_attempt_record(&attempt_record)
                                        .await?;
                                    self.store
                                        .append_action_event(
                                            &action.id,
                                            "route_degraded",
                                            serde_json::json!({
                                                "selected_surface": "a2a",
                                                "reason": reason,
                                                "code": code,
                                                "fallback": next_fallback_label(deterministic_fallback, "openclaw"),
                                            }),
                                        )
                                        .await?;
                                    if code == failure_code_treaty_denied() {
                                        self.store
                                            .insert_policy_incident(&PolicyIncident {
                                                id: Uuid::new_v4().to_string(),
                                                action_id: action.id.clone(),
                                                doctrine_pack_id: "swarm_frontier_v1".to_string(),
                                                jurisdiction: JurisdictionClass::ExternalUnknown,
                                                reason_code: "treaty_denied".to_string(),
                                                summary: error.to_string(),
                                                severity: PolicyIncidentSeverity::Critical,
                                                checkpoint: Some(OversightCheckpoint::PreDispatch),
                                                created_at: now_timestamp(),
                                            })
                                            .await?;
                                    }
                                    last_reason = Some(error.to_string());
                                    last_external_refs = base_refs;
                                }
                            }
                        }
                        Ok(None) => {
                            self.store
                                .append_action_event(
                                    &action.id,
                                    "route_degraded",
                                    serde_json::json!({
                                        "selected_surface": "a2a",
                                        "reason": "no A2A binding is configured for task.plan",
                                        "code": failure_code_route_unavailable(),
                                        "fallback": next_fallback_label(deterministic_fallback, "openclaw"),
                                    }),
                                )
                                .await?;
                        }
                        Err(error) => {
                            let code = failure_code_treaty_denied();
                            self.store
                                .append_action_event(
                                    &action.id,
                                    "route_degraded",
                                    serde_json::json!({
                                        "selected_surface": "a2a",
                                        "reason": error.to_string(),
                                        "code": code,
                                        "fallback": next_fallback_label(deterministic_fallback, "openclaw"),
                                    }),
                                )
                                .await?;
                            self.store
                                .insert_policy_incident(&PolicyIncident {
                                    id: Uuid::new_v4().to_string(),
                                    action_id: action.id.clone(),
                                    doctrine_pack_id: "swarm_frontier_v1".to_string(),
                                    jurisdiction: JurisdictionClass::ExternalUnknown,
                                    reason_code: "treaty_denied".to_string(),
                                    summary: error.to_string(),
                                    severity: PolicyIncidentSeverity::Critical,
                                    checkpoint: Some(OversightCheckpoint::PreDispatch),
                                    created_at: now_timestamp(),
                                })
                                .await?;
                            last_reason = Some(error.to_string());
                        }
                    }
                }
                "openclaw" => {
                    attempted_agentic_route = true;
                    match self.resolve_openclaw_adapter(manifest)? {
                        Some((adapter, base_refs)) => {
                            if adapter.binding().lease_required {
                                self.ensure_required_lease_valid(action, &base_refs).await?;
                            }
                            match adapter.run(action).await {
                                Ok(result) => {
                                    return Ok(ExecutionOutcome::Completed {
                                        outputs: result.outputs,
                                        selected_executor: format!("openclaw.{}", adapter.name()),
                                        checkpoint: None,
                                        external_refs: merge_external_refs(
                                            base_refs,
                                            result.external_refs,
                                        ),
                                        surface_events: result.events,
                                    });
                                }
                                Err(error) => {
                                    let reason = error.to_string();
                                    self.store
                                        .append_action_event(
                                            &action.id,
                                            "route_degraded",
                                            serde_json::json!({
                                                "selected_surface": "openclaw",
                                                "reason": reason,
                                                "code": openclaw_failure_code(&error),
                                                "fallback": next_fallback_label(deterministic_fallback, "deterministic"),
                                            }),
                                        )
                                        .await?;
                                    last_reason = Some(error.to_string());
                                    last_external_refs = base_refs;
                                }
                            }
                        }
                        None => {
                            self.store
                                .append_action_event(
                                    &action.id,
                                    "route_degraded",
                                    serde_json::json!({
                                        "selected_surface": "openclaw",
                                        "reason": "no OpenClaw binding is configured for task.plan",
                                        "code": failure_code_route_unavailable(),
                                        "fallback": next_fallback_label(deterministic_fallback, "deterministic"),
                                    }),
                                )
                                .await?;
                        }
                    }
                }
                _ => {}
            }
        }

        if deterministic_fallback {
            let executor = TaskPlannerDeterministicExecutor::new(self.state_dir());
            let mut outcome = self
                .run_deterministic_executor(
                    action,
                    "deterministic.task_plan",
                    "planning",
                    last_external_refs.clone(),
                    &executor,
                )
                .await?;
            if attempted_agentic_route {
                if let ExecutionOutcome::Completed { surface_events, .. } = &mut outcome {
                    surface_events.push(crawfish_core::SurfaceActionEvent {
                        event_type: "continuity_selected".to_string(),
                        payload: serde_json::json!({
                            "selected_surface": "deterministic",
                            "reason": last_reason.unwrap_or_else(|| "agentic route unavailable".to_string()),
                            "continuity_mode": "deterministic_only",
                        }),
                    });
                }
            }
            return Ok(outcome);
        }

        Ok(self.continuity_blocked_outcome(
            action,
            last_reason.unwrap_or_else(|| {
                "no supported task.plan execution route is configured".to_string()
            }),
            false,
            last_external_refs,
        ))
    }

    pub(crate) async fn ensure_required_lease_valid(
        &self,
        action: &Action,
        external_refs: &[ExternalRef],
    ) -> anyhow::Result<()> {
        let Some(_) = action.lease_ref else {
            let route = external_refs
                .iter()
                .find(|reference| {
                    reference.kind == "local_harness.harness"
                        || reference.kind == "openclaw.target_agent"
                })
                .map(|reference| reference.value.clone())
                .unwrap_or_else(|| "requested surface".to_string());
            anyhow::bail!("dispatch route requires an active capability lease: {route}");
        };
        self.ensure_pre_execution_lease_valid(action).await
    }

    pub(crate) fn continuity_blocked_outcome(
        &self,
        action: &Action,
        reason: impl Into<String>,
        deterministic_available: bool,
        external_refs: Vec<ExternalRef>,
    ) -> ExecutionOutcome {
        let reason = reason.into();
        let continuity_mode = select_continuity_mode(
            &action.contract.recovery.continuity_preference,
            deterministic_available,
        );
        let mut outputs = ActionOutputs {
            summary: Some(format!(
                "Action {} entered continuity mode {:?}: {}",
                action.id, continuity_mode, reason
            )),
            ..ActionOutputs::default()
        };
        outputs.metadata.insert(
            "continuity_mode".to_string(),
            serde_json::json!(format!("{continuity_mode:?}").to_lowercase()),
        );
        outputs.metadata.insert(
            "route_failure".to_string(),
            serde_json::json!(reason.clone()),
        );
        ExecutionOutcome::Blocked {
            reason,
            failure_code: failure_code_route_unavailable().to_string(),
            continuity_mode: Some(continuity_mode),
            outputs,
            external_refs,
            surface_events: Vec::new(),
        }
    }

    pub(crate) async fn ensure_repo_index_for_workspace(
        &self,
        workspace_root: &str,
    ) -> anyhow::Result<(
        crawfish_types::ArtifactRef,
        crawfish_types::RepoIndexArtifact,
    )> {
        if let Some(action) = self
            .store
            .latest_completed_action("repo_indexer", "repo.index")
            .await?
        {
            if action
                .outputs
                .metadata
                .get("workspace_root")
                .and_then(|value| value.as_str())
                == Some(workspace_root)
            {
                if let Some(artifact_ref) = action.outputs.artifacts.first() {
                    let artifact =
                        load_json_artifact::<crawfish_types::RepoIndexArtifact>(artifact_ref)
                            .await?;
                    return Ok((artifact_ref.clone(), artifact));
                }
            }
        }

        let bootstrap_action = Action {
            id: format!("inline-index-{}", Uuid::new_v4()),
            target_agent_id: "repo_indexer".to_string(),
            requester: action_requester("system"),
            initiator_owner: self.synthetic_owner(),
            counterparty_refs: Vec::new(),
            goal: crawfish_types::GoalSpec {
                summary: "inline repo index bootstrap".to_string(),
                details: None,
            },
            capability: "repo.index".to_string(),
            inputs: std::collections::BTreeMap::from([(
                "workspace_root".to_string(),
                serde_json::json!(workspace_root),
            )]),
            contract: self.config.contracts.org_defaults.clone(),
            execution_strategy: None,
            grant_refs: Vec::new(),
            lease_ref: None,
            encounter_ref: None,
            audit_receipt_ref: None,
            data_boundary: "owner_local".to_string(),
            schedule: crawfish_types::ScheduleSpec::default(),
            phase: ActionPhase::Running,
            created_at: now_timestamp(),
            started_at: Some(now_timestamp()),
            finished_at: None,
            checkpoint_ref: None,
            continuity_mode: None,
            degradation_profile: None,
            failure_reason: None,
            failure_code: None,
            selected_executor: Some("deterministic.repo_index".to_string()),
            recovery_stage: None,
            lock_detail: None,
            external_refs: Vec::new(),
            outputs: ActionOutputs::default(),
        };

        let executor = RepoIndexerDeterministicExecutor::new(self.state_dir());
        let outputs = executor.execute(&bootstrap_action).await?;
        let artifact_ref = outputs
            .artifacts
            .first()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("repo.index did not emit an artifact"))?;
        let artifact =
            load_json_artifact::<crawfish_types::RepoIndexArtifact>(&artifact_ref).await?;
        Ok((artifact_ref, artifact))
    }

    pub(crate) async fn process_action_queue_once(&self) -> anyhow::Result<()> {
        if self.store.is_draining().await? {
            return Ok(());
        }

        self.expire_awaiting_approval_actions().await?;

        while let Some(action) = self.store.claim_next_accepted_action().await? {
            self.process_claimed_action(action).await?;
        }

        Ok(())
    }

    pub(crate) async fn process_claimed_action(&self, mut action: Action) -> anyhow::Result<()> {
        let manifest = self
            .store
            .get_agent_manifest(&action.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target agent not found: {}", action.target_agent_id))?;
        let lifecycle = self
            .store
            .get_lifecycle_record(&action.target_agent_id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("lifecycle record missing for {}", action.target_agent_id)
            })?;

        if matches!(
            lifecycle.observed_state,
            AgentState::Inactive
                | AgentState::Draining
                | AgentState::Failed
                | AgentState::Finalized
        ) {
            set_action_blocked(
                &mut action,
                failure_code_route_unavailable(),
                format!(
                    "target agent {} is not executable in state {:?}",
                    manifest.id, lifecycle.observed_state
                ),
            );
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "blocked",
                    serde_json::json!({
                        "reason": action.failure_reason,
                        "code": action.failure_code,
                    }),
                )
                .await?;
            return Ok(());
        }

        if let Err(error) = self.ensure_pre_execution_lease_valid(&action).await {
            let reason = error.to_string();
            set_action_failed(&mut action, lease_failure_code(&reason), reason.clone());
            if let Some(encounter_ref) = &action.encounter_ref {
                if let Some(mut encounter) = self.store.get_encounter(encounter_ref).await? {
                    encounter.state = EncounterState::Denied;
                    self.store.insert_encounter(&encounter).await?;
                }
                let receipt = self
                    .emit_audit_receipt(
                        encounter_ref,
                        action.grant_refs.clone(),
                        action.lease_ref.clone(),
                        AuditOutcome::Denied,
                        reason.clone(),
                        None,
                    )
                    .await?;
                action.audit_receipt_ref = Some(receipt.id);
            }
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "failed",
                    serde_json::json!({
                        "reason": reason,
                        "code": action.failure_code,
                        "finished_at": action.finished_at
                    }),
                )
                .await?;
            return Ok(());
        }

        match self.execute_action(&mut action, &manifest).await {
            Ok(ExecutionOutcome::Completed {
                outputs,
                selected_executor,
                checkpoint,
                external_refs,
                surface_events,
            }) => {
                action.phase = ActionPhase::Completed;
                action.outputs = outputs;
                action.finished_at = Some(now_timestamp());
                action.failure_reason = None;
                action.failure_code = None;
                action.selected_executor = Some(selected_executor);
                action.external_refs = external_refs;
                if let Some(checkpoint) = checkpoint {
                    self.write_checkpoint_for_action(&mut action, &checkpoint)
                        .await?;
                }
                self.store.upsert_action(&action).await?;
                for event in surface_events {
                    self.store
                        .append_action_event(&action.id, &event.event_type, event.payload)
                        .await?;
                }
                self.store
                    .append_action_event(
                        &action.id,
                        "completed",
                        serde_json::json!({
                            "finished_at": action.finished_at,
                            "checkpoint_ref": action.checkpoint_ref,
                            "recovery_stage": action.recovery_stage,
                        }),
                    )
                    .await?;
                self.postprocess_terminal_action(&mut action).await?;
            }
            Ok(ExecutionOutcome::Blocked {
                reason,
                failure_code,
                continuity_mode,
                outputs,
                external_refs,
                surface_events,
            }) => {
                set_action_blocked(&mut action, &failure_code, reason.clone());
                action.continuity_mode = continuity_mode;
                action.outputs = outputs;
                action.selected_executor = selected_executor_from_external_refs(&external_refs);
                action.external_refs = external_refs;
                self.store.upsert_action(&action).await?;
                for event in surface_events {
                    self.store
                        .append_action_event(&action.id, &event.event_type, event.payload)
                        .await?;
                }
                self.store
                    .append_action_event(
                        &action.id,
                        "blocked",
                        serde_json::json!({
                            "reason": reason,
                            "code": action.failure_code,
                            "continuity_mode": action.continuity_mode.as_ref().map(|mode| format!("{mode:?}").to_lowercase())
                        }),
                    )
                    .await?;
                self.postprocess_terminal_action(&mut action).await?;
            }
            Ok(ExecutionOutcome::Failed {
                reason,
                failure_code,
                outputs,
                checkpoint,
                external_refs,
                surface_events,
            }) => {
                set_action_failed(&mut action, &failure_code, reason.clone());
                action.outputs = outputs;
                action.selected_executor = selected_executor_from_external_refs(&external_refs);
                action.external_refs = external_refs;
                if let Some(checkpoint) = checkpoint {
                    self.write_checkpoint_for_action(&mut action, &checkpoint)
                        .await?;
                }
                self.store.upsert_action(&action).await?;
                for event in surface_events {
                    self.store
                        .append_action_event(&action.id, &event.event_type, event.payload)
                        .await?;
                }
                self.store
                    .append_action_event(
                        &action.id,
                        "failed",
                        serde_json::json!({
                            "reason": reason,
                            "code": action.failure_code,
                            "checkpoint_ref": action.checkpoint_ref,
                            "recovery_stage": action.recovery_stage,
                            "finished_at": action.finished_at
                        }),
                    )
                    .await?;
                self.postprocess_terminal_action(&mut action).await?;
            }
            Err(error) => {
                let reason = error.to_string();
                set_action_failed(&mut action, failure_code_executor_error(), reason.clone());
                self.store.upsert_action(&action).await?;
                self.store
                    .append_action_event(
                        &action.id,
                        "failed",
                        serde_json::json!({
                            "reason": reason,
                            "code": action.failure_code,
                            "finished_at": action.finished_at
                        }),
                    )
                    .await?;
                self.postprocess_terminal_action(&mut action).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn execute_action(
        &self,
        action: &mut Action,
        manifest: &AgentManifest,
    ) -> anyhow::Result<ExecutionOutcome> {
        if action.capability == "repo.index" {
            let executor = RepoIndexerDeterministicExecutor::new(self.state_dir());
            return self
                .run_deterministic_executor(
                    action,
                    "deterministic.repo_index",
                    "scanning",
                    Vec::new(),
                    &executor,
                )
                .await;
        }

        if action.capability == "repo.review" {
            let workspace_root = required_input_string(action, "workspace_root")?;
            let (repo_index_ref, repo_index) = self
                .ensure_repo_index_for_workspace(&workspace_root)
                .await?;
            let executor = RepoReviewerDeterministicExecutor::new(
                self.state_dir(),
                repo_index,
                Some(repo_index_ref),
            );
            return self
                .run_deterministic_executor(
                    action,
                    "deterministic.repo_review",
                    "reviewing",
                    Vec::new(),
                    &executor,
                )
                .await;
        }

        if action.capability == "ci.triage" {
            if !has_log_input(action) && action.inputs.contains_key("mcp_resource_ref") {
                let (adapter, external_refs) = match self.resolve_mcp_adapter(manifest, action) {
                    Ok(Some(binding)) => binding,
                    Ok(None) => {
                        return Ok(self.continuity_blocked_outcome(
                            action,
                            "no MCP adapter is configured for ci.triage",
                            false,
                            mcp_input_external_refs(action),
                        ));
                    }
                    Err(error) => {
                        return Ok(self.continuity_blocked_outcome(
                            action,
                            error.to_string(),
                            false,
                            mcp_input_external_refs(action),
                        ));
                    }
                };

                match adapter.run(action).await {
                    Ok(remote_result) => {
                        let mut derived_action = action.clone();
                        let log_text =
                            extract_mcp_log_text(&remote_result.outputs).ok_or_else(|| {
                            anyhow::anyhow!(
                                "mcp result did not contain log_text, log_excerpt, or textual content"
                            )
                        })?;
                        derived_action
                            .inputs
                            .insert("log_text".to_string(), serde_json::json!(log_text));
                        let executor = CiTriageDeterministicExecutor::new(self.state_dir());
                        let mut outcome = self
                            .run_deterministic_executor(
                                &mut derived_action,
                                "deterministic.ci_triage",
                                "classifying",
                                merge_external_refs(
                                    external_refs.clone(),
                                    remote_result.external_refs.clone(),
                                ),
                                &executor,
                            )
                            .await?;
                        if let ExecutionOutcome::Completed {
                            outputs,
                            checkpoint,
                            external_refs: refs,
                            surface_events,
                            ..
                        } = &mut outcome
                        {
                            outputs.metadata.insert(
                                "mcp_summary".to_string(),
                                serde_json::json!(remote_result.outputs.summary.clone()),
                            );
                            outputs.metadata.insert(
                                "mcp_result".to_string(),
                                remote_result
                                    .outputs
                                    .metadata
                                    .get("mcp_result")
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null),
                            );
                            *refs = merge_external_refs(
                                external_refs.clone(),
                                remote_result.external_refs.clone(),
                            );
                            surface_events.extend(remote_result.events.clone());
                            if let Some(checkpoint) = checkpoint {
                                checkpoint.last_updated_at = now_timestamp();
                            }
                        }
                        return Ok(outcome);
                    }
                    Err(error) => {
                        return Ok(self.continuity_blocked_outcome(
                            action,
                            error.to_string(),
                            false,
                            external_refs,
                        ));
                    }
                }
            }

            let executor = CiTriageDeterministicExecutor::new(self.state_dir());
            return self
                .run_deterministic_executor(
                    action,
                    "deterministic.ci_triage",
                    "classifying",
                    Vec::new(),
                    &executor,
                )
                .await;
        }

        if action.capability == "workspace.patch.apply" {
            let acquired_lock = match self.try_acquire_workspace_lock(action, manifest).await? {
                Some(WorkspaceLockAttempt::Acquired(acquisition)) => {
                    action.lock_detail = Some(acquisition.detail.clone());
                    self.store.upsert_action(action).await?;
                    self.store
                        .append_action_event(
                            &action.id,
                            "lock_acquired",
                            serde_json::json!({
                                "lock_detail": action.lock_detail.clone(),
                            }),
                        )
                        .await?;
                    Some(acquisition)
                }
                Some(WorkspaceLockAttempt::Conflict(detail)) => {
                    action.lock_detail = Some(detail.clone());
                    return Ok(ExecutionOutcome::Blocked {
                        reason: format!(
                            "workspace lock is held by {}",
                            detail
                                .owner_action_id
                                .clone()
                                .unwrap_or_else(|| "another action".to_string())
                        ),
                        failure_code: failure_code_lock_conflict().to_string(),
                        continuity_mode: None,
                        outputs: ActionOutputs {
                            summary: Some("Mutation action blocked by workspace lock".to_string()),
                            artifacts: Vec::new(),
                            metadata: std::collections::BTreeMap::from([(
                                "lock_path".to_string(),
                                serde_json::json!(detail.lock_path),
                            )]),
                        },
                        external_refs: Vec::new(),
                        surface_events: Vec::new(),
                    });
                }
                None => None,
            };

            if let Err(error) = self.ensure_pre_execution_lease_valid(action).await {
                if let Some(acquisition) = &acquired_lock {
                    self.release_workspace_lock(action, &acquisition.lock_path)
                        .await?;
                    action.lock_detail = Some(crawfish_types::WorkspaceLockDetail {
                        status: "released".to_string(),
                        ..acquisition.detail.clone()
                    });
                }
                return Err(error);
            }

            let executor = WorkspacePatchApplyDeterministicExecutor::new(self.state_dir());
            let outcome = self
                .run_deterministic_executor(
                    action,
                    "deterministic.workspace_patch_apply",
                    "applying",
                    Vec::new(),
                    &executor,
                )
                .await;
            if let Some(acquisition) = &acquired_lock {
                self.release_workspace_lock(action, &acquisition.lock_path)
                    .await?;
                action.lock_detail = Some(crawfish_types::WorkspaceLockDetail {
                    status: "released".to_string(),
                    ..acquisition.detail.clone()
                });
                self.store.upsert_action(action).await?;
                self.store
                    .append_action_event(
                        &action.id,
                        "lock_released",
                        serde_json::json!({
                            "lock_detail": action.lock_detail.clone(),
                        }),
                    )
                    .await?;
            }
            return outcome;
        }

        if action.capability == "incident.enrich" {
            let executor = IncidentEnricherDeterministicExecutor::new(self.state_dir());
            return self
                .run_deterministic_executor(
                    action,
                    "deterministic.incident_enrich",
                    "enriching",
                    Vec::new(),
                    &executor,
                )
                .await;
        }

        if is_task_plan_capability(&action.capability) {
            return self.execute_task_plan(action, manifest).await;
        }

        if let Some((adapter, external_refs)) = self.resolve_mcp_adapter(manifest, action)? {
            match adapter.run(action).await {
                Ok(result) => {
                    return Ok(ExecutionOutcome::Completed {
                        outputs: result.outputs,
                        selected_executor: format!("mcp.{}", adapter.name()),
                        checkpoint: None,
                        external_refs: merge_external_refs(external_refs, result.external_refs),
                        surface_events: result.events,
                    });
                }
                Err(error) => {
                    return Ok(self.continuity_blocked_outcome(
                        action,
                        error.to_string(),
                        false,
                        external_refs,
                    ));
                }
            }
        }

        Ok(self.continuity_blocked_outcome(
            action,
            "no execution surface was available",
            false,
            Vec::new(),
        ))
    }

    pub(crate) fn lock_file_path(&self, workspace_root: &str) -> PathBuf {
        self.state_dir()
            .join("locks")
            .join(format!("workspace-{}.lock", stable_id(workspace_root)))
    }

    pub(crate) async fn try_acquire_workspace_lock(
        &self,
        action: &Action,
        manifest: &AgentManifest,
    ) -> anyhow::Result<Option<WorkspaceLockAttempt>> {
        if action.capability != "workspace.patch.apply"
            || !matches!(
                manifest.workspace_policy.lock_mode,
                crawfish_types::WorkspaceLockMode::File
            )
        {
            return Ok(None);
        }

        let workspace_root = required_input_string(action, "workspace_root")?;
        let lock_path = self.lock_file_path(&workspace_root);
        if let Some(parent) = lock_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        self.try_acquire_workspace_lock_path(action, workspace_root, lock_path, true)
            .await
            .map(Some)
    }

    pub(crate) async fn try_acquire_workspace_lock_path(
        &self,
        action: &Action,
        workspace_root: String,
        lock_path: PathBuf,
        retry_stale: bool,
    ) -> anyhow::Result<WorkspaceLockAttempt> {
        let record = WorkspaceLockRecord {
            workspace_root: workspace_root.clone(),
            owner_action_id: action.id.clone(),
            acquired_at: now_timestamp(),
        };
        let serialized = serde_json::to_vec_pretty(&record)?;

        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .await
        {
            Ok(mut file) => {
                file.write_all(&serialized).await?;
                file.flush().await?;
                Ok(WorkspaceLockAttempt::Acquired(WorkspaceLockAcquisition {
                    lock_path: lock_path.clone(),
                    detail: crawfish_types::WorkspaceLockDetail {
                        mode: crawfish_types::WorkspaceLockMode::File,
                        scope: workspace_root,
                        lock_path: lock_path.display().to_string(),
                        status: "acquired".to_string(),
                        owner_action_id: Some(action.id.clone()),
                        acquired_at: Some(record.acquired_at),
                    },
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let contents = tokio::fs::read_to_string(&lock_path).await.ok();
                let existing = contents
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<WorkspaceLockRecord>(value).ok());

                if let Some(existing) = existing {
                    if existing.owner_action_id == action.id {
                        return Ok(WorkspaceLockAttempt::Acquired(WorkspaceLockAcquisition {
                            lock_path: lock_path.clone(),
                            detail: crawfish_types::WorkspaceLockDetail {
                                mode: crawfish_types::WorkspaceLockMode::File,
                                scope: workspace_root,
                                lock_path: lock_path.display().to_string(),
                                status: "acquired".to_string(),
                                owner_action_id: Some(action.id.clone()),
                                acquired_at: Some(existing.acquired_at),
                            },
                        }));
                    }

                    if retry_stale && self.is_stale_lock_owner(&existing.owner_action_id).await? {
                        let _ = tokio::fs::remove_file(&lock_path).await;
                        return Box::pin(self.try_acquire_workspace_lock_path(
                            action,
                            workspace_root,
                            lock_path,
                            false,
                        ))
                        .await;
                    }

                    return Ok(WorkspaceLockAttempt::Conflict(
                        crawfish_types::WorkspaceLockDetail {
                            mode: crawfish_types::WorkspaceLockMode::File,
                            scope: workspace_root,
                            lock_path: lock_path.display().to_string(),
                            status: "conflicted".to_string(),
                            owner_action_id: Some(existing.owner_action_id),
                            acquired_at: Some(existing.acquired_at),
                        },
                    ));
                }

                if retry_stale {
                    let _ = tokio::fs::remove_file(&lock_path).await;
                    return Box::pin(self.try_acquire_workspace_lock_path(
                        action,
                        workspace_root,
                        lock_path,
                        false,
                    ))
                    .await;
                }

                Ok(WorkspaceLockAttempt::Conflict(
                    crawfish_types::WorkspaceLockDetail {
                        mode: crawfish_types::WorkspaceLockMode::File,
                        scope: workspace_root,
                        lock_path: lock_path.display().to_string(),
                        status: "conflicted".to_string(),
                        owner_action_id: None,
                        acquired_at: None,
                    },
                ))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) async fn is_stale_lock_owner(&self, owner_action_id: &str) -> anyhow::Result<bool> {
        let action = self.store.get_action(owner_action_id).await?;
        Ok(match action {
            Some(action) => matches!(
                action.phase,
                ActionPhase::Completed | ActionPhase::Failed | ActionPhase::Expired
            ),
            None => true,
        })
    }

    pub(crate) async fn release_workspace_lock(
        &self,
        action: &Action,
        lock_path: &Path,
    ) -> anyhow::Result<()> {
        let contents = match tokio::fs::read_to_string(lock_path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let existing: WorkspaceLockRecord = serde_json::from_str(&contents)?;
        if existing.owner_action_id == action.id {
            tokio::fs::remove_file(lock_path).await?;
        }
        Ok(())
    }
}
