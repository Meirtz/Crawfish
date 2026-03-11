use super::*;

pub(crate) fn delegation_depth_for_action(action: &Action) -> Option<u32> {
    external_ref_value(&action.external_refs, "a2a.delegation_depth")
        .and_then(|value| value.parse::<u32>().ok())
        .or_else(|| {
            if matches!(
                interaction_model_for_action(action, None),
                crawfish_types::InteractionModel::RemoteAgent
            ) {
                Some(1)
            } else {
                None
            }
        })
}

pub(crate) fn federation_pack_id_for_action(action: &Action) -> Option<String> {
    external_ref_value(&action.external_refs, "a2a.federation_pack")
}

pub(crate) fn remote_followup_context(action: &Action) -> Option<&Value> {
    action.inputs.get("remote_followup")
}

pub(crate) fn active_remote_followup_ref_for_action(action: &Action) -> Option<String> {
    remote_followup_context(action)
        .and_then(Value::as_object)
        .and_then(|value| value.get("request_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

pub(crate) fn clear_remote_followup_context(action: &mut Action) {
    action.inputs.remove("remote_followup");
}

pub(crate) fn strategy_iteration_for_action(action: &Action) -> u32 {
    action
        .outputs
        .metadata
        .get("strategy_iteration")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

pub(crate) fn remote_evidence_status_for_action(action: &Action) -> Option<RemoteEvidenceStatus> {
    action
        .outputs
        .metadata
        .get("federation_remote_evidence_status")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn remote_state_disposition_for_action(
    action: &Action,
) -> Option<RemoteStateDisposition> {
    action
        .outputs
        .metadata
        .get("federation_remote_state_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn remote_review_disposition_for_action(
    action: &Action,
) -> Option<RemoteReviewDisposition> {
    action
        .outputs
        .metadata
        .get("remote_review_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn federation_decision_for_action(action: &Action) -> Option<FederationDecision> {
    action
        .outputs
        .metadata
        .get("federation_decision")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn remote_outcome_disposition_for_action(
    action: &Action,
) -> Option<crawfish_types::RemoteOutcomeDisposition> {
    action
        .outputs
        .metadata
        .get("treaty_remote_outcome_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

pub(crate) fn treaty_violations_for_action(
    action: &Action,
) -> Vec<crawfish_types::TreatyViolation> {
    action
        .outputs
        .metadata
        .get("treaty_violations")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

pub(crate) fn set_remote_review_disposition_metadata(
    outputs: &mut ActionOutputs,
    disposition: &RemoteReviewDisposition,
) {
    outputs.metadata.insert(
        "remote_review_disposition".to_string(),
        serde_json::to_value(disposition).unwrap_or(Value::Null),
    );
}

pub(crate) fn remote_review_reason_for_action(
    action: &Action,
    policy_incidents: &[PolicyIncident],
) -> Option<RemoteReviewReason> {
    if matches!(
        remote_evidence_status_for_action(action),
        Some(RemoteEvidenceStatus::ScopeViolation)
    ) || treaty_violations_for_action(action)
        .iter()
        .any(|violation| violation.code == "treaty_scope_violation")
    {
        return Some(RemoteReviewReason::ScopeViolation);
    }
    if matches!(
        remote_evidence_status_for_action(action),
        Some(RemoteEvidenceStatus::MissingRequiredEvidence)
    ) || policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_enforcement_gap")
    {
        return Some(RemoteReviewReason::EvidenceGap);
    }
    if matches!(
        remote_state_disposition_for_action(action),
        Some(RemoteStateDisposition::Blocked | RemoteStateDisposition::AwaitingApproval)
    ) || policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "remote_state_escalated")
    {
        return Some(RemoteReviewReason::RemoteStateEscalated);
    }
    if matches!(
        remote_outcome_disposition_for_action(action),
        Some(RemoteOutcomeDisposition::ReviewRequired)
    ) {
        return Some(RemoteReviewReason::ResultReviewRequired);
    }
    None
}

pub(crate) fn task_plan_delegated_data_scopes(action: &Action) -> Vec<String> {
    let mut scopes = Vec::new();
    for key in [
        "objective",
        "workspace_root",
        "context_files",
        "constraints",
        "desired_outputs",
        "background",
        "verification_feedback",
        "base_ref",
        "head_ref",
    ] {
        if action.inputs.contains_key(key) {
            scopes.push(key.to_string());
        }
    }
    scopes
}

pub(crate) fn set_treaty_result_metadata(
    outputs: &mut ActionOutputs,
    disposition: &crawfish_types::RemoteOutcomeDisposition,
    violations: &[crawfish_types::TreatyViolation],
) {
    outputs.metadata.insert(
        "treaty_remote_outcome_disposition".to_string(),
        serde_json::to_value(disposition).unwrap_or(Value::Null),
    );
    outputs.metadata.insert(
        "treaty_violations".to_string(),
        serde_json::to_value(violations).unwrap_or_else(|_| Value::Array(Vec::new())),
    );
}

pub(crate) fn set_federation_result_metadata(
    outputs: &mut ActionOutputs,
    decision: &FederationDecision,
    evidence_status: Option<&RemoteEvidenceStatus>,
    state_disposition: Option<&RemoteStateDisposition>,
    result_acceptance: Option<&RemoteResultAcceptance>,
) {
    outputs.metadata.insert(
        "federation_decision".to_string(),
        serde_json::to_value(decision).unwrap_or(Value::Null),
    );
    if let Some(evidence_status) = evidence_status {
        outputs.metadata.insert(
            "federation_remote_evidence_status".to_string(),
            serde_json::to_value(evidence_status).unwrap_or(Value::Null),
        );
    }
    if let Some(state_disposition) = state_disposition {
        outputs.metadata.insert(
            "federation_remote_state_disposition".to_string(),
            serde_json::to_value(state_disposition).unwrap_or(Value::Null),
        );
    }
    if let Some(result_acceptance) = result_acceptance {
        outputs.metadata.insert(
            "federation_remote_result_acceptance".to_string(),
            serde_json::to_value(result_acceptance).unwrap_or(Value::Null),
        );
    }
}
