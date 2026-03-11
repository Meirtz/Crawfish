use super::*;

pub(crate) fn evaluation_required_for_action(action: &Action) -> bool {
    matches!(
        action.capability.as_str(),
        "task.plan" | "coding.patch.plan" | "repo.review" | "incident.enrich"
    ) || action.contract.quality.evaluation_hook.is_some()
        || action.contract.quality.evaluation_profile.is_some()
}

pub(crate) fn legacy_evaluation_hook_profile_name(hook: &str) -> Option<&'static str> {
    match hook {
        "operator_review_queue" => Some("task_plan_default"),
        "deterministic_scorecard" => Some("repo_review_default"),
        _ => None,
    }
}

pub(crate) fn builtin_evaluation_profiles() -> BTreeMap<String, EvaluationProfile> {
    BTreeMap::from([
        (
            "task_plan_default".to_string(),
            EvaluationProfile {
                scorecard: "task_plan_scorecard".to_string(),
                review_queue: true,
                alert_rules: vec![
                    "evaluation_attention_required".to_string(),
                    "frontier_gap_detected".to_string(),
                ],
                dataset_name: Some("task_plan_dataset".to_string()),
                dataset_capture: true,
                post_result_required: true,
            },
        ),
        (
            "repo_review_default".to_string(),
            EvaluationProfile {
                scorecard: "repo_review_scorecard".to_string(),
                review_queue: true,
                alert_rules: vec![
                    "evaluation_attention_required".to_string(),
                    "frontier_gap_detected".to_string(),
                ],
                dataset_name: Some("repo_review_dataset".to_string()),
                dataset_capture: true,
                post_result_required: true,
            },
        ),
        (
            "task_plan_remote_default".to_string(),
            EvaluationProfile {
                scorecard: "task_plan_remote_scorecard".to_string(),
                review_queue: true,
                alert_rules: vec![
                    "evaluation_attention_required".to_string(),
                    "frontier_gap_detected".to_string(),
                ],
                dataset_name: Some("task_plan_dataset".to_string()),
                dataset_capture: true,
                post_result_required: true,
            },
        ),
        (
            "incident_enrich_default".to_string(),
            EvaluationProfile {
                scorecard: "incident_enrich_scorecard".to_string(),
                review_queue: true,
                alert_rules: vec![
                    "evaluation_attention_required".to_string(),
                    "frontier_gap_detected".to_string(),
                ],
                dataset_name: Some("incident_enrich_dataset".to_string()),
                dataset_capture: true,
                post_result_required: true,
            },
        ),
    ])
}

pub(crate) fn builtin_scorecards() -> BTreeMap<String, ScorecardSpec> {
    BTreeMap::from([
        (
            "task_plan_scorecard".to_string(),
            ScorecardSpec {
                id: "task_plan_scorecard".to_string(),
                title: "Task plan default scorecard".to_string(),
                criteria: vec![
                    ScorecardCriterion {
                        id: "artifact_json".to_string(),
                        title: "task_plan.json present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "artifact_markdown".to_string(),
                        title: "task_plan.md present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("task_plan.md".to_string()),
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "ordered_steps".to_string(),
                        title: "ordered_steps has enough entries".to_string(),
                        kind: ScorecardCriterionKind::ListMinLen,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: Some("ordered_steps".to_string()),
                        source_path: None,
                        min_len: Some(2),
                        checkpoint: None,
                        incident_code: None,
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "risks".to_string(),
                        title: "risks populated".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: Some("risks".to_string()),
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "assumptions".to_string(),
                        title: "assumptions populated".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: Some("assumptions".to_string()),
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "confidence_summary".to_string(),
                        title: "confidence summary populated".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: Some("confidence_summary".to_string()),
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "task_plan_schema".to_string(),
                        title: "task plan JSON matches the expected schema".to_string(),
                        kind: ScorecardCriterionKind::JsonSchemaValid,
                        artifact_name: Some("task_plan.json".to_string()),
                        json_schema: Some(serde_json::json!({
                            "type": "object",
                            "required": ["ordered_steps", "risks", "assumptions", "confidence_summary"],
                            "properties": {
                                "ordered_steps": {"type": "array"},
                                "risks": {"type": "array"},
                                "assumptions": {"type": "array"},
                                "confidence_summary": {"type": "string"}
                            }
                        })),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "task_plan_heading".to_string(),
                        title: "task plan markdown keeps the expected heading".to_string(),
                        kind: ScorecardCriterionKind::RegexMatch,
                        artifact_name: Some("task_plan.md".to_string()),
                        regex_pattern: Some(r"(?m)^# Task Plan$".to_string()),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "objective_coverage".to_string(),
                        title: "objective tokens covered".to_string(),
                        kind: ScorecardCriterionKind::TokenCoverage,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: None,
                        source_path: Some("inputs.objective".to_string()),
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "desired_outputs".to_string(),
                        title: "desired outputs covered".to_string(),
                        kind: ScorecardCriterionKind::TokenCoverage,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: None,
                        source_path: Some("inputs.desired_outputs".to_string()),
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "pre_dispatch".to_string(),
                        title: "pre-dispatch checkpoint passed".to_string(),
                        kind: ScorecardCriterionKind::CheckpointPassed,
                        artifact_name: None,
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: Some(OversightCheckpoint::PreDispatch),
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                ],
                minimum_score: Some(0.6),
                needs_review_below: Some(1.0),
            },
        ),
        (
            "task_plan_remote_scorecard".to_string(),
            ScorecardSpec {
                id: "task_plan_remote_scorecard".to_string(),
                title: "Task plan remote-agent scorecard".to_string(),
                criteria: vec![
                    ScorecardCriterion {
                        id: "interaction_model_remote_agent".to_string(),
                        title: "interaction model is remote_agent".to_string(),
                        kind: ScorecardCriterionKind::InteractionModelIs,
                        interaction_model: Some(crawfish_types::InteractionModel::RemoteAgent),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "delegation_receipt_present".to_string(),
                        title: "delegation receipt external ref present".to_string(),
                        kind: ScorecardCriterionKind::ExternalRefPresent,
                        external_ref_kind: Some("a2a.delegation_receipt".to_string()),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "remote_task_ref_present".to_string(),
                        title: "remote task ref present".to_string(),
                        kind: ScorecardCriterionKind::ExternalRefPresent,
                        external_ref_kind: Some("a2a.task_id".to_string()),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "remote_outcome_accepted".to_string(),
                        title: "remote outcome accepted".to_string(),
                        kind: ScorecardCriterionKind::RemoteOutcomeDispositionIs,
                        remote_outcome_disposition: Some(
                            crawfish_types::RemoteOutcomeDisposition::Accepted,
                        ),
                        weight: 3,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "no_treaty_violations".to_string(),
                        title: "no treaty violations present".to_string(),
                        kind: ScorecardCriterionKind::TreatyViolationAbsent,
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "no_treaty_scope_violation".to_string(),
                        title: "treaty scope violation absent".to_string(),
                        kind: ScorecardCriterionKind::TreatyViolationAbsent,
                        treaty_violation_code: Some("treaty_scope_violation".to_string()),
                        weight: 3,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "no_frontier_gap_violation".to_string(),
                        title: "frontier enforcement gap absent".to_string(),
                        kind: ScorecardCriterionKind::TreatyViolationAbsent,
                        treaty_violation_code: Some("frontier_enforcement_gap".to_string()),
                        weight: 3,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "artifact_json".to_string(),
                        title: "task_plan.json present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("task_plan.json".to_string()),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "artifact_markdown".to_string(),
                        title: "task_plan.md present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("task_plan.md".to_string()),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "ordered_steps".to_string(),
                        title: "ordered_steps has enough entries".to_string(),
                        kind: ScorecardCriterionKind::ListMinLen,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: Some("ordered_steps".to_string()),
                        min_len: Some(2),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "confidence_summary".to_string(),
                        title: "confidence summary populated".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("task_plan.json".to_string()),
                        field_path: Some("confidence_summary".to_string()),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "task_plan_schema".to_string(),
                        title: "task plan JSON matches the expected schema".to_string(),
                        kind: ScorecardCriterionKind::JsonSchemaValid,
                        artifact_name: Some("task_plan.json".to_string()),
                        json_schema: Some(serde_json::json!({
                            "type": "object",
                            "required": ["ordered_steps", "risks", "assumptions", "confidence_summary"],
                            "properties": {
                                "ordered_steps": {"type": "array"},
                                "risks": {"type": "array"},
                                "assumptions": {"type": "array"},
                                "confidence_summary": {"type": "string"}
                            }
                        })),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "objective_coverage".to_string(),
                        title: "objective tokens covered".to_string(),
                        kind: ScorecardCriterionKind::TokenCoverage,
                        artifact_name: Some("task_plan.json".to_string()),
                        source_path: Some("inputs.objective".to_string()),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "pre_dispatch".to_string(),
                        title: "pre-dispatch checkpoint passed".to_string(),
                        kind: ScorecardCriterionKind::CheckpointPassed,
                        checkpoint: Some(OversightCheckpoint::PreDispatch),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                ],
                minimum_score: Some(0.75),
                needs_review_below: Some(1.0),
            },
        ),
        (
            "repo_review_scorecard".to_string(),
            ScorecardSpec {
                id: "repo_review_scorecard".to_string(),
                title: "Repo review default scorecard".to_string(),
                criteria: vec![
                    ScorecardCriterion {
                        id: "findings_json".to_string(),
                        title: "review_findings.json present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("review_findings.json".to_string()),
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "summary_md".to_string(),
                        title: "review_summary.md present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("review_summary.md".to_string()),
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "changed_files".to_string(),
                        title: "changed files captured".to_string(),
                        kind: ScorecardCriterionKind::ListMinLen,
                        artifact_name: Some("review_findings.json".to_string()),
                        field_path: Some("changed_files".to_string()),
                        source_path: None,
                        min_len: Some(1),
                        checkpoint: None,
                        incident_code: None,
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "findings".to_string(),
                        title: "findings present".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("review_findings.json".to_string()),
                        field_path: Some("findings".to_string()),
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "review_schema".to_string(),
                        title: "review findings JSON matches the expected schema".to_string(),
                        kind: ScorecardCriterionKind::JsonSchemaValid,
                        artifact_name: Some("review_findings.json".to_string()),
                        json_schema: Some(serde_json::json!({
                            "type": "object",
                            "required": ["risk_level", "changed_files", "findings"],
                            "properties": {
                                "risk_level": {"type": "string"},
                                "changed_files": {"type": "array"},
                                "findings": {"type": "array"}
                            }
                        })),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "review_heading".to_string(),
                        title: "review markdown keeps the expected heading".to_string(),
                        kind: ScorecardCriterionKind::RegexMatch,
                        artifact_name: Some("review_summary.md".to_string()),
                        regex_pattern: Some(r"(?m)^# Review Summary$".to_string()),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "pre_dispatch".to_string(),
                        title: "pre-dispatch checkpoint passed".to_string(),
                        kind: ScorecardCriterionKind::CheckpointPassed,
                        artifact_name: None,
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: Some(OversightCheckpoint::PreDispatch),
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                ],
                minimum_score: Some(0.5),
                needs_review_below: Some(1.0),
            },
        ),
        (
            "incident_enrich_scorecard".to_string(),
            ScorecardSpec {
                id: "incident_enrich_scorecard".to_string(),
                title: "Incident enrich default scorecard".to_string(),
                criteria: vec![
                    ScorecardCriterion {
                        id: "enrichment_json".to_string(),
                        title: "incident_enrichment.json present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("incident_enrichment.json".to_string()),
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "summary_md".to_string(),
                        title: "incident_summary.md present".to_string(),
                        kind: ScorecardCriterionKind::ArtifactPresent,
                        artifact_name: Some("incident_summary.md".to_string()),
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "blast_radius".to_string(),
                        title: "blast radius captured".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("incident_enrichment.json".to_string()),
                        field_path: Some("probable_blast_radius".to_string()),
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "next_steps".to_string(),
                        title: "next steps present".to_string(),
                        kind: ScorecardCriterionKind::JsonFieldNonempty,
                        artifact_name: Some("incident_enrichment.json".to_string()),
                        field_path: Some("next_steps".to_string()),
                        source_path: None,
                        min_len: None,
                        checkpoint: None,
                        incident_code: None,
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "incident_schema".to_string(),
                        title: "incident enrichment JSON matches the expected schema".to_string(),
                        kind: ScorecardCriterionKind::JsonSchemaValid,
                        artifact_name: Some("incident_enrichment.json".to_string()),
                        json_schema: Some(serde_json::json!({
                            "type": "object",
                            "required": ["probable_blast_radius", "error_signatures", "repeated_symptoms", "next_steps"],
                            "properties": {
                                "probable_blast_radius": {"type": "array"},
                                "error_signatures": {"type": "array"},
                                "repeated_symptoms": {"type": "array"},
                                "next_steps": {"type": "array"}
                            }
                        })),
                        weight: 2,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "incident_heading".to_string(),
                        title: "incident markdown keeps the expected heading".to_string(),
                        kind: ScorecardCriterionKind::RegexMatch,
                        artifact_name: Some("incident_summary.md".to_string()),
                        regex_pattern: Some(r"(?m)^# Incident Summary$".to_string()),
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                    ScorecardCriterion {
                        id: "pre_dispatch".to_string(),
                        title: "pre-dispatch checkpoint passed".to_string(),
                        kind: ScorecardCriterionKind::CheckpointPassed,
                        artifact_name: None,
                        field_path: None,
                        source_path: None,
                        min_len: None,
                        checkpoint: Some(OversightCheckpoint::PreDispatch),
                        incident_code: None,
                        weight: 1,
                        ..ScorecardCriterion::default()
                    },
                ],
                minimum_score: Some(0.5),
                needs_review_below: Some(1.0),
            },
        ),
    ])
}

pub(crate) fn builtin_evaluation_datasets() -> BTreeMap<String, EvaluationDataset> {
    BTreeMap::from([
        (
            "task_plan_dataset".to_string(),
            EvaluationDataset {
                capability: "task.plan".to_string(),
                title: Some("Task planning replay set".to_string()),
                auto_capture: true,
            },
        ),
        (
            "repo_review_dataset".to_string(),
            EvaluationDataset {
                capability: "repo.review".to_string(),
                title: Some("Repo review replay set".to_string()),
                auto_capture: true,
            },
        ),
        (
            "incident_enrich_dataset".to_string(),
            EvaluationDataset {
                capability: "incident.enrich".to_string(),
                title: Some("Incident enrichment replay set".to_string()),
                auto_capture: true,
            },
        ),
    ])
}

pub(crate) fn default_alert_rule_frontier_gap() -> AlertRule {
    AlertRule {
        id: "frontier_gap_detected".to_string(),
        name: "Frontier gap detected".to_string(),
        trigger: "policy_incident".to_string(),
        severity: "warning".to_string(),
    }
}

pub(crate) fn default_alert_rule_evaluation_attention() -> AlertRule {
    AlertRule {
        id: "evaluation_attention_required".to_string(),
        name: "Evaluation attention required".to_string(),
        trigger: "evaluation_attention".to_string(),
        severity: "info".to_string(),
    }
}

pub(crate) fn builtin_alert_rules() -> BTreeMap<String, AlertRule> {
    BTreeMap::from([
        (
            "frontier_gap_detected".to_string(),
            default_alert_rule_frontier_gap(),
        ),
        (
            "evaluation_attention_required".to_string(),
            default_alert_rule_evaluation_attention(),
        ),
    ])
}

pub(crate) fn builtin_pairwise_profiles() -> BTreeMap<String, PairwiseProfile> {
    BTreeMap::from([(
        "task_plan_pairwise_default".to_string(),
        PairwiseProfile {
            capability: "task.plan".to_string(),
            score_margin: 0.1,
            review_queue: true,
            review_priority: "medium".to_string(),
            low_confidence_threshold: 0.85,
            regression_loss_rate_threshold: 0.3,
            needs_review_rate_threshold: 0.25,
        },
    )])
}

pub(crate) fn builtin_profile_name_for_action(action: &Action) -> Option<&'static str> {
    match action.capability.as_str() {
        "task.plan" | "coding.patch.plan"
            if matches!(
                interaction_model_for_action(action, None),
                crawfish_types::InteractionModel::RemoteAgent
            ) =>
        {
            Some("task_plan_remote_default")
        }
        "task.plan" | "coding.patch.plan" => Some("task_plan_default"),
        "repo.review" => Some("repo_review_default"),
        "incident.enrich" => Some("incident_enrich_default"),
        _ => None,
    }
}

pub(crate) fn builtin_pairwise_profile_name_for_capability(
    capability: &str,
) -> Option<&'static str> {
    match capability {
        "task.plan" | "coding.patch.plan" => Some("task_plan_pairwise_default"),
        _ => None,
    }
}

pub(crate) fn replay_routes_for_executor(executor: &str) -> (Vec<String>, Vec<String>) {
    match executor {
        "deterministic" | "deterministic.task_plan" => {
            (Vec::new(), vec!["deterministic".to_string()])
        }
        "claude_code" | "local_harness.claude_code" => {
            (vec!["claude_code".to_string()], Vec::new())
        }
        "codex" | "local_harness.codex" => (vec!["codex".to_string()], Vec::new()),
        "openclaw" => (vec!["openclaw".to_string()], Vec::new()),
        "a2a" => (vec!["a2a".to_string()], Vec::new()),
        other if other.starts_with("openclaw.") => (vec!["openclaw".to_string()], Vec::new()),
        other if other.starts_with("a2a.") => (vec!["a2a".to_string()], Vec::new()),
        other => (vec![other.to_string()], Vec::new()),
    }
}

pub(crate) fn alert_rule_matches(
    rule: &AlertRule,
    evaluation: Option<&EvaluationRecord>,
    incidents: &[PolicyIncident],
) -> bool {
    match rule.trigger.as_str() {
        "policy_incident" => incidents.iter().any(|incident| {
            matches!(
                incident.severity,
                PolicyIncidentSeverity::Warning | PolicyIncidentSeverity::Critical
            )
        }),
        "evaluation_attention" => evaluation
            .map(|evaluation| {
                matches!(
                    evaluation.status,
                    EvaluationStatus::Failed | EvaluationStatus::NeedsReview
                )
            })
            .unwrap_or(false),
        "evaluation_failed" => evaluation
            .map(|evaluation| matches!(evaluation.status, EvaluationStatus::Failed))
            .unwrap_or(false),
        _ => false,
    }
}

pub(crate) fn alert_summary_for_rule(
    rule: &AlertRule,
    evaluation: Option<&EvaluationRecord>,
    incidents: &[PolicyIncident],
) -> String {
    match rule.trigger.as_str() {
        "policy_incident" => incidents
            .iter()
            .find(|incident| {
                matches!(
                    incident.severity,
                    PolicyIncidentSeverity::Warning | PolicyIncidentSeverity::Critical
                )
            })
            .map(|incident| incident.summary.clone())
            .unwrap_or_else(|| rule.name.clone()),
        "evaluation_attention" | "evaluation_failed" => evaluation
            .map(|evaluation| evaluation.summary.clone())
            .unwrap_or_else(|| rule.name.clone()),
        _ => rule.name.clone(),
    }
}

pub(crate) fn action_phase_name(phase: &ActionPhase) -> &'static str {
    match phase {
        ActionPhase::Accepted => "accepted",
        ActionPhase::Running => "running",
        ActionPhase::Blocked => "blocked",
        ActionPhase::AwaitingApproval => "awaiting_approval",
        ActionPhase::Cancelling => "cancelling",
        ActionPhase::Completed => "completed",
        ActionPhase::Failed => "failed",
        ActionPhase::Expired => "expired",
    }
}

pub(crate) fn agent_state_name(state: &AgentState) -> &'static str {
    match state {
        AgentState::Unconfigured => "unconfigured",
        AgentState::Configuring => "configuring",
        AgentState::Inactive => "inactive",
        AgentState::Activating => "activating",
        AgentState::Active => "active",
        AgentState::Degraded => "degraded",
        AgentState::Draining => "draining",
        AgentState::Failed => "failed",
        AgentState::Finalized => "finalized",
    }
}

pub(crate) fn health_status_name(status: &HealthStatus) -> &'static str {
    match status {
        HealthStatus::Unknown => "unknown",
        HealthStatus::Healthy => "healthy",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Unhealthy => "unhealthy",
    }
}

pub(crate) fn degraded_profile_name(profile: &DegradedProfileName) -> &'static str {
    match profile {
        DegradedProfileName::ReadOnly => "read_only",
        DegradedProfileName::DependencyIsolation => "dependency_isolation",
        DegradedProfileName::BudgetGuard => "budget_guard",
        DegradedProfileName::ProviderFailover => "provider_failover",
    }
}

pub(crate) fn continuity_mode_name(mode: &ContinuityModeName) -> &'static str {
    match mode {
        ContinuityModeName::DeterministicOnly => "deterministic_only",
        ContinuityModeName::StoreAndForward => "store_and_forward",
        ContinuityModeName::HumanHandoff => "human_handoff",
        ContinuityModeName::Suspended => "suspended",
    }
}

pub(crate) fn build_pairwise_case_result(
    pairwise_run_id: &str,
    dataset_case: &DatasetCase,
    left: &ExperimentCaseResult,
    right: &ExperimentCaseResult,
    profile: &PairwiseProfile,
) -> PairwiseCaseResult {
    let (outcome, reason_code, summary) =
        if left.treaty_violation_count < right.treaty_violation_count {
            (
                PairwiseOutcome::LeftWins,
                "fewer_treaty_violations".to_string(),
                "left executor produced fewer treaty-governance violations".to_string(),
            )
        } else if right.treaty_violation_count < left.treaty_violation_count {
            (
                PairwiseOutcome::RightWins,
                "fewer_treaty_violations".to_string(),
                "right executor produced fewer treaty-governance violations".to_string(),
            )
        } else if left.policy_incident_count < right.policy_incident_count {
            (
                PairwiseOutcome::LeftWins,
                "fewer_policy_incidents".to_string(),
                "left executor produced fewer doctrine/policy incidents".to_string(),
            )
        } else if right.policy_incident_count < left.policy_incident_count {
            (
                PairwiseOutcome::RightWins,
                "fewer_policy_incidents".to_string(),
                "right executor produced fewer doctrine/policy incidents".to_string(),
            )
        } else if left.status != right.status {
            if matches!(left.status, ExperimentCaseStatus::Passed) {
                (
                    PairwiseOutcome::LeftWins,
                    "successful_status".to_string(),
                    "left executor succeeded while right failed".to_string(),
                )
            } else {
                (
                    PairwiseOutcome::RightWins,
                    "successful_status".to_string(),
                    "right executor succeeded while left failed".to_string(),
                )
            }
        } else if let (Some(left_score), Some(right_score)) = (left.score, right.score) {
            if left.policy_incident_count != right.policy_incident_count
                && (left_score - right_score).abs() > profile.score_margin
                && ((left.policy_incident_count > right.policy_incident_count
                    && left_score > right_score)
                    || (right.policy_incident_count > left.policy_incident_count
                        && right_score > left_score))
            {
                (
                    PairwiseOutcome::NeedsReview,
                    "signal_conflict".to_string(),
                    "doctrine and evaluation signals conflict across executors".to_string(),
                )
            } else if (left_score - right_score).abs() > profile.score_margin {
                if left_score > right_score {
                    (
                        PairwiseOutcome::LeftWins,
                        "higher_normalized_score".to_string(),
                        "left executor achieved the higher normalized score".to_string(),
                    )
                } else {
                    (
                        PairwiseOutcome::RightWins,
                        "higher_normalized_score".to_string(),
                        "right executor achieved the higher normalized score".to_string(),
                    )
                }
            } else {
                (
                    PairwiseOutcome::NeedsReview,
                    "score_margin_needs_review".to_string(),
                    "score delta stayed within the pairwise review margin".to_string(),
                )
            }
        } else {
            (
                PairwiseOutcome::NeedsReview,
                "insufficient_score_evidence".to_string(),
                "pairwise comparison lacked sufficient score evidence".to_string(),
            )
        };

    PairwiseCaseResult {
        id: Uuid::new_v4().to_string(),
        pairwise_run_id: pairwise_run_id.to_string(),
        dataset_case_id: dataset_case.id.clone(),
        outcome,
        summary,
        reason_code,
        left_case_result_ref: left.id.clone(),
        right_case_result_ref: right.id.clone(),
        left_score: left.score,
        right_score: right.score,
        review_queue_item_ref: None,
        feedback_note_id: None,
        review_resolution: None,
        created_at: now_timestamp(),
    }
}

pub(crate) fn maybe_enqueue_pairwise_review_item(
    dataset_case: &DatasetCase,
    profile: &PairwiseProfile,
    left_executor: &str,
    right_executor: &str,
    left: &ExperimentCaseResult,
    right: &ExperimentCaseResult,
    result: &PairwiseCaseResult,
) -> Option<ReviewQueueItem> {
    if !profile.review_queue {
        return None;
    }

    let low_confidence = left
        .score
        .zip(right.score)
        .map(|(left_score, right_score)| {
            left_score < profile.low_confidence_threshold
                && right_score < profile.low_confidence_threshold
        })
        .unwrap_or(false)
        || matches!(
            left.remote_review_disposition,
            Some(RemoteReviewDisposition::Pending | RemoteReviewDisposition::NeedsFollowup)
        )
        || matches!(
            right.remote_review_disposition,
            Some(RemoteReviewDisposition::Pending | RemoteReviewDisposition::NeedsFollowup)
        );
    let signal_conflict = result.reason_code == "signal_conflict";
    let within_margin = result.reason_code == "score_margin_needs_review";
    let should_queue = matches!(result.outcome, PairwiseOutcome::NeedsReview)
        || signal_conflict
        || (low_confidence
            && matches!(left.evaluation_status, Some(EvaluationStatus::Passed))
                != matches!(right.evaluation_status, Some(EvaluationStatus::Passed)));
    if !should_queue {
        return None;
    }

    let reason_code = if signal_conflict {
        "pairwise_signal_conflict".to_string()
    } else if low_confidence {
        "pairwise_low_confidence".to_string()
    } else if within_margin {
        "pairwise_margin_needs_review".to_string()
    } else {
        "pairwise_needs_review".to_string()
    };

    Some(ReviewQueueItem {
        id: Uuid::new_v4().to_string(),
        action_id: dataset_case.source_action_id.clone(),
        source: "pairwise_compare".to_string(),
        kind: ReviewQueueKind::PairwiseEval,
        status: ReviewQueueStatus::Open,
        priority: profile.review_priority.clone(),
        reason_code,
        summary: format!(
            "Compare {} vs {} for dataset case {}",
            left_executor, right_executor, dataset_case.id
        ),
        treaty_pack_id: dataset_case.treaty_pack_id.clone(),
        federation_pack_id: dataset_case.federation_pack_id.clone(),
        remote_evidence_status: dataset_case.remote_evidence_status.clone(),
        remote_evidence_ref: dataset_case.remote_evidence_ref.clone(),
        remote_task_ref: dataset_case.remote_task_ref.clone(),
        remote_review_disposition: dataset_case.remote_review_disposition.clone(),
        remote_followup_ref: dataset_case.remote_followup_refs.last().cloned(),
        evaluation_ref: None,
        dataset_case_ref: Some(dataset_case.id.clone()),
        pairwise_run_ref: Some(result.pairwise_run_id.clone()),
        pairwise_case_ref: Some(result.id.clone()),
        left_case_result_ref: Some(left.id.clone()),
        right_case_result_ref: Some(right.id.clone()),
        created_at: now_timestamp(),
        resolved_at: None,
        resolution: None,
    })
}
