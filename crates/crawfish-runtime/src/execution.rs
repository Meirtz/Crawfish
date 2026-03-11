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
