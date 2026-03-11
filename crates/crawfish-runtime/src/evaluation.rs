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

impl Supervisor {
    pub(crate) async fn execute_evaluation_run_internal(
        &self,
        request: StartEvaluationRunRequest,
    ) -> anyhow::Result<ExperimentRunDetailResponse> {
        let datasets = self.evaluation_datasets();
        let dataset = datasets
            .get(&request.dataset)
            .ok_or_else(|| anyhow::anyhow!("dataset not found: {}", request.dataset))?;
        let cases = self.store.list_dataset_cases(&request.dataset).await?;
        let run = ExperimentRun {
            id: Uuid::new_v4().to_string(),
            dataset_name: request.dataset.clone(),
            executor: request.executor.clone(),
            strategy_mode: ExecutionStrategyMode::SinglePass,
            allow_fallback: false,
            status: ExperimentRunStatus::Running,
            total_cases: cases.len() as u32,
            completed_cases: 0,
            started_at: Some(now_timestamp()),
            finished_at: None,
            created_at: now_timestamp(),
            summary: Some(format!(
                "Replaying {} {} cases against {}",
                cases.len(),
                dataset.capability,
                request.executor
            )),
        };
        self.store.insert_experiment_run(&run).await?;

        let mut completed = 0_u32;
        let mut failed = 0_u32;
        let mut results = Vec::new();
        for case in cases {
            let result = self.run_experiment_case(&run, &case).await?;
            if matches!(result.status, ExperimentCaseStatus::Failed) {
                failed = failed.saturating_add(1);
            }
            completed = completed.saturating_add(1);
            self.store.insert_experiment_case_result(&result).await?;
            results.push(result);
            let mut updated = run.clone();
            updated.completed_cases = completed;
            updated.status = ExperimentRunStatus::Running;
            self.store.update_experiment_run(&updated).await?;
        }

        let mut completed_run = run.clone();
        completed_run.completed_cases = completed;
        completed_run.finished_at = Some(now_timestamp());
        completed_run.status = if failed > 0 {
            ExperimentRunStatus::Failed
        } else {
            ExperimentRunStatus::Completed
        };
        completed_run.summary = Some(format!("{completed} cases replayed, {failed} failed"));
        self.store.update_experiment_run(&completed_run).await?;
        Ok(ExperimentRunDetailResponse {
            run: completed_run,
            cases: results,
        })
    }

    pub(crate) async fn execute_pairwise_evaluation_run_internal(
        &self,
        request: StartPairwiseEvaluationRunRequest,
    ) -> anyhow::Result<PairwiseExperimentRunDetailResponse> {
        let datasets = self.evaluation_datasets();
        let dataset = datasets
            .get(&request.dataset)
            .ok_or_else(|| anyhow::anyhow!("dataset not found: {}", request.dataset))?
            .clone();
        let resolved_profile = self
            .resolve_pairwise_profile(&dataset, request.profile.as_deref())?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "pairwise profile not found for dataset {} capability {}",
                    request.dataset,
                    dataset.capability
                )
            })?;
        let dataset_cases = self.store.list_dataset_cases(&request.dataset).await?;

        let left = self
            .execute_evaluation_run_internal(StartEvaluationRunRequest {
                dataset: request.dataset.clone(),
                executor: request.left_executor.clone(),
            })
            .await?;
        let right = self
            .execute_evaluation_run_internal(StartEvaluationRunRequest {
                dataset: request.dataset.clone(),
                executor: request.right_executor.clone(),
            })
            .await?;

        let mut run = PairwiseExperimentRun {
            id: Uuid::new_v4().to_string(),
            dataset_name: request.dataset.clone(),
            capability: dataset.capability.clone(),
            profile_name: resolved_profile.name.clone(),
            left_executor: request.left_executor.clone(),
            right_executor: request.right_executor.clone(),
            left_run_id: left.run.id.clone(),
            right_run_id: right.run.id.clone(),
            status: PairwiseExperimentRunStatus::Running,
            total_cases: dataset_cases.len() as u32,
            completed_cases: 0,
            left_wins: 0,
            right_wins: 0,
            needs_review_cases: 0,
            triggered_alert_rules: Vec::new(),
            alert_summaries: Vec::new(),
            started_at: Some(now_timestamp()),
            finished_at: None,
            created_at: now_timestamp(),
            summary: Some(format!(
                "Comparing {} against {} on {} {} cases",
                request.left_executor,
                request.right_executor,
                dataset_cases.len(),
                dataset.capability
            )),
        };
        self.store.insert_pairwise_experiment_run(&run).await?;

        let left_results: BTreeMap<_, _> = left
            .cases
            .into_iter()
            .map(|result| (result.dataset_case_id.clone(), result))
            .collect();
        let right_results: BTreeMap<_, _> = right
            .cases
            .into_iter()
            .map(|result| (result.dataset_case_id.clone(), result))
            .collect();

        let mut pairwise_results = Vec::new();
        for dataset_case in &dataset_cases {
            let left_result = left_results
                .get(&dataset_case.id)
                .ok_or_else(|| anyhow::anyhow!("missing left case result for {}", dataset_case.id))?
                .clone();
            let right_result = right_results
                .get(&dataset_case.id)
                .ok_or_else(|| {
                    anyhow::anyhow!("missing right case result for {}", dataset_case.id)
                })?
                .clone();

            let mut result = build_pairwise_case_result(
                &run.id,
                dataset_case,
                &left_result,
                &right_result,
                &resolved_profile.profile,
            );

            let review_item = maybe_enqueue_pairwise_review_item(
                dataset_case,
                &resolved_profile.profile,
                &request.left_executor,
                &request.right_executor,
                &left_result,
                &right_result,
                &result,
            );
            if let Some(item) = review_item {
                result.review_queue_item_ref = Some(item.id.clone());
                self.store.insert_review_queue_item(&item).await?;
            }

            match result.outcome {
                PairwiseOutcome::LeftWins => run.left_wins = run.left_wins.saturating_add(1),
                PairwiseOutcome::RightWins => run.right_wins = run.right_wins.saturating_add(1),
                PairwiseOutcome::NeedsReview => {
                    run.needs_review_cases = run.needs_review_cases.saturating_add(1)
                }
            }
            run.completed_cases = run.completed_cases.saturating_add(1);
            self.store.insert_pairwise_case_result(&result).await?;
            pairwise_results.push(result);
            self.store.update_pairwise_experiment_run(&run).await?;
        }

        let total = run.total_cases.max(1) as f64;
        let left_win_rate = f64::from(run.left_wins) / total;
        let needs_review_rate = f64::from(run.needs_review_cases) / total;
        if left_win_rate > resolved_profile.profile.regression_loss_rate_threshold {
            run.triggered_alert_rules
                .push("comparison_regression".to_string());
            run.alert_summaries.push(format!(
                "candidate {} lost to baseline {} in {:.0}% of cases",
                run.right_executor,
                run.left_executor,
                left_win_rate * 100.0
            ));
        }
        if needs_review_rate > resolved_profile.profile.needs_review_rate_threshold {
            run.triggered_alert_rules
                .push("comparison_attention_required".to_string());
            run.alert_summaries.push(format!(
                "{:.0}% of pairwise cases require human review",
                needs_review_rate * 100.0
            ));
        }

        run.status = PairwiseExperimentRunStatus::Completed;
        run.finished_at = Some(now_timestamp());
        run.summary = Some(format!(
            "{} left wins, {} right wins, {} need review",
            run.left_wins, run.right_wins, run.needs_review_cases
        ));
        self.store.update_pairwise_experiment_run(&run).await?;

        Ok(PairwiseExperimentRunDetailResponse {
            run,
            cases: pairwise_results,
        })
    }
}

impl Supervisor {
    pub(crate) async fn postprocess_terminal_action(
        &self,
        action: &mut Action,
    ) -> anyhow::Result<()> {
        let encounter = match action.encounter_ref.as_deref() {
            Some(encounter_ref) => self.store.get_encounter(encounter_ref).await?,
            None => None,
        };
        let interaction_model = interaction_model_for_action(action, encounter.as_ref());
        let doctrine = default_doctrine_pack(
            action,
            &interaction_model,
            jurisdiction_class_for_action(action, encounter.as_ref()),
        );
        let resolved_profile = self.resolve_evaluation_profile(action)?;
        let preliminary_checkpoint_status =
            checkpoint_status_for_action(action, &doctrine, true, None, resolved_profile.is_some());
        let preliminary_incidents = self.policy_incidents_for_action(
            action,
            resolved_profile.as_ref(),
            None,
            &preliminary_checkpoint_status,
        );
        let mut evaluations = self
            .evaluate_action_outputs(
                action,
                resolved_profile.as_ref(),
                &doctrine,
                &interaction_model,
                &preliminary_checkpoint_status,
                &preliminary_incidents,
            )
            .await?;
        let incidents = self.policy_incidents_for_action(
            action,
            resolved_profile.as_ref(),
            evaluations.last(),
            &preliminary_checkpoint_status,
        );
        for incident in &incidents {
            self.store.insert_policy_incident(incident).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "policy_incident_recorded",
                    serde_json::json!({
                        "incident_id": incident.id,
                        "reason_code": incident.reason_code,
                        "severity": format!("{:?}", incident.severity).to_lowercase(),
                    }),
                )
                .await?;
        }

        let remote_evidence_bundle = self
            .build_remote_evidence_bundle_for_action(
                action,
                &interaction_model,
                &preliminary_checkpoint_status,
                &incidents,
            )
            .await?;
        if let Some(bundle) = &remote_evidence_bundle {
            self.store.insert_remote_evidence_bundle(bundle).await?;
            if let Some(attempt_ref) = &bundle.remote_attempt_ref {
                if let Some(mut attempt) = self.store.get_remote_attempt_record(attempt_ref).await?
                {
                    attempt.remote_evidence_ref = Some(bundle.id.clone());
                    if attempt.completed_at.is_none() {
                        attempt.completed_at = Some(now_timestamp());
                    }
                    self.store.upsert_remote_attempt_record(&attempt).await?;
                }
            }
            self.store
                .append_action_event(
                    &action.id,
                    "remote_evidence_bundle_recorded",
                    serde_json::json!({
                        "bundle_id": bundle.id,
                        "attempt": bundle.attempt,
                        "remote_task_ref": bundle.remote_task_ref,
                        "remote_review_disposition": bundle
                            .remote_review_disposition
                            .as_ref()
                            .map(runtime_enum_to_snake),
                    }),
                )
                .await?;
        }

        for evaluation in &mut evaluations {
            if let Some(bundle) = &remote_evidence_bundle {
                evaluation.remote_evidence_ref = Some(bundle.id.clone());
                evaluation.remote_attempt_ref = bundle.remote_attempt_ref.clone();
                evaluation.remote_followup_ref = bundle.followup_request_ref.clone();
                evaluation.remote_review_disposition = bundle.remote_review_disposition.clone();
            }
            self.store.insert_evaluation(evaluation).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "evaluation_recorded",
                    serde_json::json!({
                        "evaluation_id": evaluation.id,
                        "evaluator": evaluation.evaluator,
                        "status": format!("{:?}", evaluation.status).to_lowercase(),
                    }),
                )
                .await?;
        }

        let checkpoint_status = checkpoint_status_for_action(
            action,
            &doctrine,
            true,
            evaluations.last(),
            resolved_profile.is_some(),
        );
        let trace = self
            .build_trace_bundle_for_action(
                action,
                &doctrine,
                &interaction_model,
                &checkpoint_status,
                &incidents,
                remote_evidence_bundle.as_ref(),
            )
            .await?;
        self.store.put_trace_bundle(&trace).await?;
        self.store
            .append_action_event(
                &action.id,
                "trace_bundle_recorded",
                serde_json::json!({
                    "trace_id": trace.id,
                    "event_count": trace.events.len(),
                }),
            )
            .await?;

        let dataset_case = self
            .maybe_capture_dataset_case(
                action,
                resolved_profile.as_ref(),
                &trace,
                evaluations.as_slice(),
            )
            .await?;
        if let Some(case) = &dataset_case {
            self.store.insert_dataset_case(case).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "dataset_case_captured",
                    serde_json::json!({
                        "dataset_case_id": case.id,
                        "dataset_name": case.dataset_name,
                    }),
                )
                .await?;
        }

        if let Some(item) = self
            .maybe_enqueue_review_item(
                action,
                resolved_profile.as_ref(),
                evaluations.last(),
                &incidents,
                remote_evidence_bundle.as_ref(),
                dataset_case.as_ref(),
            )
            .await?
        {
            self.store.insert_review_queue_item(&item).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "review_queue_item_created",
                    serde_json::json!({
                        "review_id": item.id,
                        "source": item.source,
                        "status": format!("{:?}", item.status).to_lowercase(),
                        "priority": item.priority,
                        "reason_code": item.reason_code,
                    }),
                )
                .await?;
        }

        for alert in self.build_alert_events(
            action,
            resolved_profile.as_ref(),
            evaluations.last(),
            &incidents,
            remote_evidence_bundle.as_ref(),
        ) {
            self.store.insert_alert_event(&alert).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "alert_triggered",
                    serde_json::json!({
                        "rule_id": alert.rule_id,
                        "summary": alert.summary,
                        "severity": alert.severity,
                    }),
                )
                .await?;
        }

        if let Some(followup) = self.close_active_remote_followup_request(action).await? {
            self.store
                .append_action_event(
                    &action.id,
                    "remote_followup_closed",
                    serde_json::json!({
                        "followup_request_id": followup.id,
                        "status": runtime_enum_to_snake(&followup.status),
                    }),
                )
                .await?;
            clear_remote_followup_context(action);
            self.store.upsert_action(action).await?;
        }

        Ok(())
    }

    pub(crate) async fn build_trace_bundle_for_action(
        &self,
        action: &Action,
        doctrine: &DoctrinePack,
        interaction_model: &crawfish_types::InteractionModel,
        checkpoint_status: &[CheckpointStatus],
        incidents: &[PolicyIncident],
        remote_evidence_bundle: Option<&RemoteEvidenceBundle>,
    ) -> anyhow::Result<TraceBundle> {
        let events = self.store.list_action_events(&action.id).await?;
        let delegation_receipt_ref =
            external_ref_value(&action.external_refs, "a2a.delegation_receipt");
        let delegation_receipt = if let Some(receipt_ref) = delegation_receipt_ref.as_deref() {
            self.store.get_delegation_receipt(receipt_ref).await?
        } else {
            None
        };
        let remote_attempts = self.store.list_remote_attempt_records(&action.id).await?;
        let remote_followups = self.store.list_remote_followup_requests(&action.id).await?;
        let federation_pack_id = federation_pack_id_for_action(action);
        let trace = TraceBundle {
            id: format!("trace-{}", action.id),
            action_id: action.id.clone(),
            capability: action.capability.clone(),
            goal_summary: action.goal.summary.clone(),
            interaction_model: Some(interaction_model.clone()),
            jurisdiction_class: Some(doctrine.jurisdiction.clone()),
            doctrine_summary: Some(doctrine.clone()),
            checkpoint_status: checkpoint_status.to_vec(),
            selected_executor: action.selected_executor.clone(),
            inputs: action.inputs.clone(),
            artifact_refs: action.outputs.artifacts.clone(),
            external_refs: action.external_refs.clone(),
            events: events
                .into_iter()
                .map(|event| {
                    BTreeMap::from([
                        (
                            "event_type".to_string(),
                            serde_json::json!(event.event_type),
                        ),
                        ("payload".to_string(), event.payload),
                        (
                            "created_at".to_string(),
                            serde_json::json!(event.created_at),
                        ),
                    ])
                })
                .collect(),
            verification_summary: action
                .outputs
                .metadata
                .get("verification_summary")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            enforcement_records: checkpoint_status
                .iter()
                .cloned()
                .map(|status| crawfish_types::EnforcementRecord {
                    id: format!(
                        "enforcement-{}-{}",
                        action.id,
                        runtime_enum_to_snake(&status.checkpoint)
                    ),
                    action_id: action.id.clone(),
                    checkpoint: status.checkpoint,
                    outcome: status.outcome,
                    reason: status
                        .reason
                        .unwrap_or_else(|| "no reason supplied".to_string()),
                    created_at: now_timestamp(),
                })
                .collect(),
            policy_incidents: incidents.to_vec(),
            remote_principal: delegation_receipt
                .as_ref()
                .map(|receipt| receipt.remote_principal.clone()),
            treaty_pack_id: external_ref_value(&action.external_refs, "a2a.treaty_pack"),
            federation_pack_id: federation_pack_id.clone(),
            federation_decision: federation_decision_for_action(action),
            delegation_receipt_ref,
            remote_evidence_ref: remote_evidence_bundle.map(|bundle| bundle.id.clone()),
            remote_attempt_refs: remote_attempts
                .iter()
                .map(|attempt| attempt.id.clone())
                .collect(),
            remote_followup_refs: remote_followups
                .iter()
                .map(|followup| followup.id.clone())
                .collect(),
            remote_task_ref: external_ref_value(&action.external_refs, "a2a.task_id"),
            remote_outcome_disposition: remote_outcome_disposition_for_action(action),
            remote_evidence_status: remote_evidence_status_for_action(action),
            remote_review_disposition: remote_evidence_bundle
                .and_then(|bundle| bundle.remote_review_disposition.clone()),
            remote_state_disposition: remote_state_disposition_for_action(action),
            treaty_violations: treaty_violations_for_action(action),
            delegation_depth: delegation_depth_for_action(action),
            created_at: now_timestamp(),
        };
        Ok(trace)
    }

    pub(crate) async fn build_remote_evidence_bundle_for_action(
        &self,
        action: &Action,
        interaction_model: &InteractionModel,
        checkpoint_status: &[CheckpointStatus],
        incidents: &[PolicyIncident],
    ) -> anyhow::Result<Option<RemoteEvidenceBundle>> {
        if !matches!(interaction_model, InteractionModel::RemoteAgent) {
            return Ok(None);
        }

        let delegation_receipt_ref =
            external_ref_value(&action.external_refs, "a2a.delegation_receipt");
        let delegation_receipt = if let Some(receipt_ref) = delegation_receipt_ref.as_deref() {
            self.store.get_delegation_receipt(receipt_ref).await?
        } else {
            None
        };
        let treaty_pack_id = external_ref_value(&action.external_refs, "a2a.treaty_pack");
        let remote_task_ref = external_ref_value(&action.external_refs, "a2a.task_id");
        let remote_attempts = self.store.list_remote_attempt_records(&action.id).await?;
        let latest_remote_attempt = remote_attempts.last().cloned();
        let remote_terminal_state = action
            .outputs
            .metadata
            .get("a2a_remote_state")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let remote_artifact_manifest = action
            .outputs
            .artifacts
            .iter()
            .map(artifact_basename)
            .collect::<Vec<_>>();
        let remote_data_scopes = task_plan_delegated_data_scopes(action);
        let remote_evidence_status = remote_evidence_status_for_action(action);
        let remote_outcome_disposition = remote_outcome_disposition_for_action(action);
        let remote_review_disposition =
            remote_review_disposition_for_action(action).or_else(|| {
                if matches!(
                    remote_outcome_disposition,
                    Some(RemoteOutcomeDisposition::ReviewRequired)
                ) || matches!(
                    remote_state_disposition_for_action(action),
                    Some(RemoteStateDisposition::Blocked)
                ) {
                    Some(RemoteReviewDisposition::Pending)
                } else {
                    None
                }
            });
        let remote_review_reason = remote_review_reason_for_action(action, incidents);

        let mut evidence_items = checkpoint_status
            .iter()
            .map(|status| RemoteEvidenceItem {
                id: format!("checkpoint-{}", runtime_enum_to_snake(&status.checkpoint)),
                kind: "checkpoint".to_string(),
                summary: status
                    .reason
                    .clone()
                    .unwrap_or_else(|| "checkpoint evaluated".to_string()),
                checkpoint: Some(status.checkpoint.clone()),
                satisfied: !status.required || matches!(status.outcome, CheckpointOutcome::Passed),
                source_ref: None,
                detail: Some(runtime_enum_to_snake(&status.outcome)),
            })
            .collect::<Vec<_>>();

        evidence_items.push(RemoteEvidenceItem {
            id: "delegation_receipt_present".to_string(),
            kind: "delegation_receipt".to_string(),
            summary: if delegation_receipt_ref.is_some() {
                "delegation receipt is present".to_string()
            } else {
                "delegation receipt is missing".to_string()
            },
            checkpoint: Some(OversightCheckpoint::PostResult),
            satisfied: delegation_receipt_ref.is_some(),
            source_ref: delegation_receipt_ref.clone(),
            detail: None,
        });
        evidence_items.push(RemoteEvidenceItem {
            id: "remote_task_ref_present".to_string(),
            kind: "remote_task_ref".to_string(),
            summary: if remote_task_ref.is_some() {
                "remote task reference is present".to_string()
            } else {
                "remote task reference is missing".to_string()
            },
            checkpoint: Some(OversightCheckpoint::PostResult),
            satisfied: remote_task_ref.is_some(),
            source_ref: remote_task_ref.clone(),
            detail: None,
        });
        evidence_items.push(RemoteEvidenceItem {
            id: "remote_terminal_state_verified".to_string(),
            kind: "terminal_state".to_string(),
            summary: remote_terminal_state
                .clone()
                .map(|state| format!("remote terminal state recorded as {state}"))
                .unwrap_or_else(|| "remote terminal state could not be proven".to_string()),
            checkpoint: Some(OversightCheckpoint::PostResult),
            satisfied: remote_terminal_state.is_some()
                && action.outputs.metadata.contains_key("a2a_result"),
            source_ref: remote_task_ref.clone(),
            detail: None,
        });
        evidence_items.push(RemoteEvidenceItem {
            id: "artifact_classes_allowed".to_string(),
            kind: "artifact_scope".to_string(),
            summary: if treaty_violations_for_action(action)
                .iter()
                .any(|violation| violation.code == "treaty_scope_violation")
            {
                "remote artifact manifest crossed treaty allowance".to_string()
            } else {
                "remote artifact manifest stayed within treaty allowance".to_string()
            },
            checkpoint: Some(OversightCheckpoint::PostResult),
            satisfied: !treaty_violations_for_action(action)
                .iter()
                .any(|violation| violation.code == "treaty_scope_violation"),
            source_ref: None,
            detail: Some(remote_artifact_manifest.join(",")),
        });
        evidence_items.push(RemoteEvidenceItem {
            id: "data_scopes_allowed".to_string(),
            kind: "data_scope".to_string(),
            summary: if matches!(
                remote_evidence_status,
                Some(RemoteEvidenceStatus::ScopeViolation)
            ) {
                "remote data scope crossed treaty allowance".to_string()
            } else {
                "remote data scope stayed within treaty allowance".to_string()
            },
            checkpoint: Some(OversightCheckpoint::PostResult),
            satisfied: !matches!(
                remote_evidence_status,
                Some(RemoteEvidenceStatus::ScopeViolation)
            ),
            source_ref: None,
            detail: Some(remote_data_scopes.join(",")),
        });

        Ok(Some(RemoteEvidenceBundle {
            id: format!(
                "remote-evidence-{}-attempt-{}",
                action.id,
                latest_remote_attempt
                    .as_ref()
                    .map(|attempt| attempt.attempt)
                    .unwrap_or_else(|| strategy_iteration_for_action(action))
            ),
            action_id: action.id.clone(),
            attempt: latest_remote_attempt
                .as_ref()
                .map(|attempt| attempt.attempt)
                .unwrap_or_else(|| strategy_iteration_for_action(action)),
            remote_attempt_ref: latest_remote_attempt
                .as_ref()
                .map(|attempt| attempt.id.clone()),
            interaction_model: interaction_model.clone(),
            treaty_pack_id,
            federation_pack_id: federation_pack_id_for_action(action),
            remote_principal: delegation_receipt
                .as_ref()
                .map(|receipt| receipt.remote_principal.clone()),
            delegation_receipt_ref,
            remote_task_ref,
            remote_terminal_state,
            remote_artifact_manifest,
            remote_data_scopes,
            checkpoint_status: checkpoint_status.to_vec(),
            evidence_items,
            policy_incidents: incidents.to_vec(),
            treaty_violations: treaty_violations_for_action(action),
            remote_evidence_status,
            remote_outcome_disposition,
            remote_review_disposition,
            remote_review_reason,
            followup_request_ref: latest_remote_attempt
                .as_ref()
                .and_then(|attempt| attempt.followup_request_ref.clone()),
            created_at: now_timestamp(),
        }))
    }

    pub(crate) async fn evaluate_action_outputs(
        &self,
        action: &Action,
        profile: Option<&ResolvedEvaluationProfile>,
        doctrine: &DoctrinePack,
        interaction_model: &crawfish_types::InteractionModel,
        checkpoint_status: &[CheckpointStatus],
        observed_incidents: &[PolicyIncident],
    ) -> anyhow::Result<Vec<EvaluationRecord>> {
        let Some(profile) = profile else {
            return Ok(Vec::new());
        };

        let outcome = self
            .score_action_outputs(
                action,
                profile,
                doctrine,
                interaction_model,
                checkpoint_status,
                observed_incidents,
            )
            .await?;
        let latest_remote_attempt = self
            .store
            .list_remote_attempt_records(&action.id)
            .await?
            .into_iter()
            .last();
        Ok(vec![EvaluationRecord {
            id: Uuid::new_v4().to_string(),
            action_id: action.id.clone(),
            evaluator: profile.name.clone(),
            status: outcome.status,
            score: Some(outcome.score),
            summary: outcome.summary,
            findings: outcome.findings,
            criterion_results: outcome.criterion_results,
            interaction_model: Some(interaction_model.clone()),
            remote_outcome_disposition: remote_outcome_disposition_for_action(action),
            treaty_violation_count: treaty_violations_for_action(action).len() as u32,
            federation_pack_id: federation_pack_id_for_action(action),
            remote_evidence_status: remote_evidence_status_for_action(action),
            remote_evidence_ref: None,
            remote_attempt_ref: latest_remote_attempt
                .as_ref()
                .map(|attempt| attempt.id.clone()),
            remote_followup_ref: latest_remote_attempt
                .as_ref()
                .and_then(|attempt| attempt.followup_request_ref.clone()),
            remote_review_disposition: remote_review_disposition_for_action(action),
            feedback_note_id: None,
            created_at: now_timestamp(),
        }])
    }

    pub(crate) async fn score_action_outputs(
        &self,
        action: &Action,
        profile: &ResolvedEvaluationProfile,
        doctrine: &DoctrinePack,
        interaction_model: &crawfish_types::InteractionModel,
        checkpoint_status: &[CheckpointStatus],
        observed_incidents: &[PolicyIncident],
    ) -> anyhow::Result<ScorecardOutcome> {
        let total_weight: u32 = profile
            .scorecard
            .criteria
            .iter()
            .map(|criterion| criterion.weight.max(1))
            .sum();
        if total_weight == 0 {
            anyhow::bail!("scorecard {} has no criteria", profile.scorecard.id);
        }

        let mut passed_weight = 0_u32;
        let mut findings = Vec::new();
        let mut criterion_results = Vec::new();
        for criterion in &profile.scorecard.criteria {
            let criterion_result = self
                .scorecard_criterion_result(
                    action,
                    doctrine,
                    interaction_model,
                    checkpoint_status,
                    observed_incidents,
                    criterion,
                )
                .await?;
            if criterion_result.passed {
                passed_weight = passed_weight.saturating_add(criterion.weight.max(1));
            } else {
                findings.push(format!("{} failed", criterion.title));
            }
            criterion_results.push(criterion_result);
        }

        let score = f64::from(passed_weight) / f64::from(total_weight);
        let minimum_score = profile.scorecard.minimum_score.unwrap_or(0.5);
        let needs_review_below = profile.scorecard.needs_review_below.unwrap_or(1.0);
        let status = if score < minimum_score {
            EvaluationStatus::Failed
        } else if score < needs_review_below {
            EvaluationStatus::NeedsReview
        } else {
            EvaluationStatus::Passed
        };

        Ok(ScorecardOutcome {
            status,
            score,
            summary: format!("Deterministic scorecard for {}", action.capability),
            findings,
            criterion_results,
        })
    }

    pub(crate) async fn maybe_enqueue_review_item(
        &self,
        action: &Action,
        profile: Option<&ResolvedEvaluationProfile>,
        evaluation: Option<&EvaluationRecord>,
        incidents: &[PolicyIncident],
        remote_evidence_bundle: Option<&RemoteEvidenceBundle>,
        dataset_case: Option<&DatasetCase>,
    ) -> anyhow::Result<Option<ReviewQueueItem>> {
        let treaty_pack = external_ref_value(&action.external_refs, "a2a.treaty_pack")
            .and_then(|treaty_id| self.config.treaties.packs.get(&treaty_id).cloned());
        let federation_pack = federation_pack_id_for_action(action)
            .and_then(|pack_id| self.resolve_federation_pack_by_id(&pack_id, treaty_pack.as_ref()));
        let remote_outcome_disposition = remote_outcome_disposition_for_action(action);
        let remote_evidence_status = remote_evidence_status_for_action(action);
        let remote_review_disposition = remote_evidence_bundle
            .and_then(|bundle| bundle.remote_review_disposition.clone())
            .or_else(|| remote_review_disposition_for_action(action));
        let remote_review_required = matches!(
            remote_review_disposition,
            Some(RemoteReviewDisposition::Pending | RemoteReviewDisposition::NeedsFollowup)
        );
        let should_queue = profile
            .map(|profile| profile.profile.review_queue)
            .unwrap_or(false)
            || federation_pack
                .as_ref()
                .map(|pack| pack.review_defaults.enabled)
                .unwrap_or(false)
            || (treaty_pack
                .as_ref()
                .map(|treaty| treaty.review_queue)
                .unwrap_or(false)
                && remote_review_required)
            || evaluation
                .map(|evaluation| {
                    matches!(
                        evaluation.status,
                        EvaluationStatus::NeedsReview | EvaluationStatus::Failed
                    )
                })
                .unwrap_or(false)
            || incidents.iter().any(|incident| {
                matches!(
                    incident.severity,
                    PolicyIncidentSeverity::Warning | PolicyIncidentSeverity::Critical
                )
            });

        if !should_queue {
            return Ok(None);
        }

        let high_priority = incidents
            .iter()
            .any(|incident| matches!(incident.severity, PolicyIncidentSeverity::Critical))
            || evaluation
                .map(|evaluation| matches!(evaluation.status, EvaluationStatus::Failed))
                .unwrap_or(false)
            || matches!(
                remote_evidence_status,
                Some(RemoteEvidenceStatus::ScopeViolation)
            )
            || (treaty_pack
                .as_ref()
                .map(|treaty| treaty.review_queue)
                .unwrap_or(false)
                && remote_review_required);
        let priority = if high_priority {
            "high".to_string()
        } else {
            "medium".to_string()
        };

        let reason_code = if incidents
            .iter()
            .any(|incident| incident.reason_code == "unresolved_evaluation_profile")
        {
            "enforcement_gap".to_string()
        } else if evaluation
            .map(|evaluation| matches!(evaluation.status, EvaluationStatus::NeedsReview))
            .unwrap_or(false)
        {
            "needs_review".to_string()
        } else if evaluation
            .map(|evaluation| matches!(evaluation.status, EvaluationStatus::Failed))
            .unwrap_or(false)
        {
            "evaluation_failed".to_string()
        } else if matches!(
            remote_outcome_disposition,
            Some(crawfish_types::RemoteOutcomeDisposition::ReviewRequired)
        ) {
            "treaty_review_required".to_string()
        } else {
            "policy_incident".to_string()
        };

        let kind = if remote_review_required {
            ReviewQueueKind::RemoteResultReview
        } else {
            ReviewQueueKind::ActionEval
        };

        let summary = if kind == ReviewQueueKind::RemoteResultReview {
            match remote_evidence_bundle.and_then(|bundle| bundle.remote_review_reason.clone()) {
                Some(RemoteReviewReason::EvidenceGap) => {
                    "remote outcome requires review because required evidence is incomplete"
                        .to_string()
                }
                Some(RemoteReviewReason::ScopeViolation) => {
                    "remote outcome requires review because treaty scope evidence is violated"
                        .to_string()
                }
                Some(RemoteReviewReason::RemoteStateEscalated) => {
                    "remote state was escalated and requires operator review".to_string()
                }
                Some(RemoteReviewReason::ResultReviewRequired) => {
                    "remote result requires operator review before it can be admitted".to_string()
                }
                Some(RemoteReviewReason::Unknown) => {
                    "remote outcome requires operator review".to_string()
                }
                None => "remote result requires operator review".to_string(),
            }
        } else {
            evaluation
                .map(|evaluation| evaluation.summary.clone())
                .or_else(|| incidents.first().map(|incident| incident.summary.clone()))
                .unwrap_or_else(|| "operator review required".to_string())
        };

        Ok(Some(ReviewQueueItem {
            id: Uuid::new_v4().to_string(),
            action_id: action.id.clone(),
            source: profile
                .map(|profile| profile.name.clone())
                .unwrap_or_else(|| "policy_incident".to_string()),
            kind,
            status: ReviewQueueStatus::Open,
            priority,
            reason_code,
            summary,
            treaty_pack_id: treaty_pack.as_ref().map(|treaty| treaty.id.clone()),
            federation_pack_id: federation_pack.map(|pack| pack.id),
            remote_evidence_status,
            remote_evidence_ref: remote_evidence_bundle.map(|bundle| bundle.id.clone()),
            remote_followup_ref: remote_evidence_bundle
                .and_then(|bundle| bundle.followup_request_ref.clone()),
            remote_task_ref: external_ref_value(&action.external_refs, "a2a.task_id"),
            remote_review_disposition,
            evaluation_ref: evaluation.map(|evaluation| evaluation.id.clone()),
            dataset_case_ref: dataset_case.map(|case| case.id.clone()),
            pairwise_run_ref: None,
            pairwise_case_ref: None,
            left_case_result_ref: None,
            right_case_result_ref: None,
            created_at: now_timestamp(),
            resolved_at: None,
            resolution: None,
        }))
    }

    pub(crate) fn policy_incidents_for_action(
        &self,
        action: &Action,
        profile: Option<&ResolvedEvaluationProfile>,
        evaluation: Option<&EvaluationRecord>,
        checkpoint_status: &[CheckpointStatus],
    ) -> Vec<PolicyIncident> {
        let interaction_model = interaction_model_for_action(action, None);
        let doctrine = default_doctrine_pack(
            action,
            &interaction_model,
            jurisdiction_class_for_action(action, None),
        );
        let mut incidents = Vec::new();
        let treaty_pack = external_ref_value(&action.external_refs, "a2a.treaty_pack")
            .and_then(|treaty_id| self.config.treaties.packs.get(&treaty_id).cloned());
        let federation_pack = federation_pack_id_for_action(action)
            .and_then(|pack_id| self.resolve_federation_pack_by_id(&pack_id, treaty_pack.as_ref()));
        let treaty_violations = treaty_violations_for_action(action);
        let remote_state_disposition = remote_state_disposition_for_action(action);
        let remote_evidence_status = remote_evidence_status_for_action(action);
        if action.capability == "workspace.patch.apply" {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "frontier_gap_mutation_post_result_review".to_string(),
                summary:
                    "Mutation completed without evaluation-spine review; doctrine is ahead of enforcement."
                        .to_string(),
                severity: crawfish_types::PolicyIncidentSeverity::Warning,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        if matches!(
            interaction_model,
            crawfish_types::InteractionModel::RemoteAgent
        ) {
            if external_ref_value(&action.external_refs, "a2a.treaty_pack").is_none() {
                incidents.push(PolicyIncident {
                    id: Uuid::new_v4().to_string(),
                    action_id: action.id.clone(),
                    doctrine_pack_id: doctrine.id.clone(),
                    jurisdiction: doctrine.jurisdiction.clone(),
                    reason_code: "frontier_gap_remote_treaty".to_string(),
                    summary: "remote agent delegation executed without durable treaty evidence"
                        .to_string(),
                    severity: PolicyIncidentSeverity::Critical,
                    checkpoint: Some(crawfish_types::OversightCheckpoint::PreDispatch),
                    created_at: now_timestamp(),
                });
            }
            if federation_pack_id_for_action(action).is_none() {
                incidents.push(PolicyIncident {
                    id: Uuid::new_v4().to_string(),
                    action_id: action.id.clone(),
                    doctrine_pack_id: doctrine.id.clone(),
                    jurisdiction: doctrine.jurisdiction.clone(),
                    reason_code: "frontier_gap_remote_federation_pack".to_string(),
                    summary:
                        "remote agent delegation completed without a durable federation governance pack reference"
                            .to_string(),
                    severity: PolicyIncidentSeverity::Critical,
                    checkpoint: Some(crawfish_types::OversightCheckpoint::PreDispatch),
                    created_at: now_timestamp(),
                });
            }
            if external_ref_value(&action.external_refs, "a2a.delegation_receipt").is_none() {
                incidents.push(PolicyIncident {
                    id: Uuid::new_v4().to_string(),
                    action_id: action.id.clone(),
                    doctrine_pack_id: doctrine.id.clone(),
                    jurisdiction: doctrine.jurisdiction.clone(),
                    reason_code: "frontier_gap_remote_delegation_receipt".to_string(),
                    summary:
                        "remote agent delegation completed without a durable delegation receipt"
                            .to_string(),
                    severity: PolicyIncidentSeverity::Critical,
                    checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                    created_at: now_timestamp(),
                });
            }
        }
        if matches!(
            remote_state_disposition,
            Some(RemoteStateDisposition::Blocked | RemoteStateDisposition::AwaitingApproval)
        ) {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "remote_state_escalated".to_string(),
                summary: federation_pack
                    .as_ref()
                    .map(|pack| {
                        format!(
                            "remote state was escalated under federation pack {}",
                            pack.id
                        )
                    })
                    .unwrap_or_else(|| {
                        "remote state was escalated by the control plane".to_string()
                    }),
                severity: PolicyIncidentSeverity::Warning,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        for violation in &treaty_violations {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: violation.code.clone(),
                summary: violation.summary.clone(),
                severity: if violation.code == "treaty_scope_violation" {
                    PolicyIncidentSeverity::Critical
                } else {
                    PolicyIncidentSeverity::Warning
                },
                checkpoint: violation.checkpoint.clone(),
                created_at: now_timestamp(),
            });
        }
        if matches!(
            remote_evidence_status,
            Some(RemoteEvidenceStatus::ScopeViolation)
        ) && !treaty_violations
            .iter()
            .any(|violation| violation.code == "treaty_scope_violation")
        {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "treaty_scope_violation".to_string(),
                summary:
                    "remote result crossed a scope or artifact boundary outside treaty allowance"
                        .to_string(),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        if let Some(hook) = action.contract.quality.evaluation_hook.as_deref() {
            if legacy_evaluation_hook_profile_name(hook).is_none() {
                incidents.push(PolicyIncident {
                    id: Uuid::new_v4().to_string(),
                    action_id: action.id.clone(),
                    doctrine_pack_id: doctrine.id.clone(),
                    jurisdiction: doctrine.jurisdiction.clone(),
                    reason_code: "unsupported_evaluation_hook".to_string(),
                    summary: format!(
                        "evaluation_hook `{hook}` is deprecated and could not be normalized into a named evaluation profile"
                    ),
                    severity: PolicyIncidentSeverity::Critical,
                    checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                    created_at: now_timestamp(),
                });
            }
        }
        if evaluation_required_for_action(action) && profile.is_none() {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "unresolved_evaluation_profile".to_string(),
                summary: "post-result evaluation is required but no resolvable evaluation profile was available".to_string(),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        if let Some(evaluation) = evaluation {
            if matches!(evaluation.status, EvaluationStatus::Failed) {
                incidents.push(PolicyIncident {
                    id: Uuid::new_v4().to_string(),
                    action_id: action.id.clone(),
                    doctrine_pack_id: doctrine.id.clone(),
                    jurisdiction: doctrine.jurisdiction.clone(),
                    reason_code: "evaluation_failed".to_string(),
                    summary: evaluation.summary.clone(),
                    severity: PolicyIncidentSeverity::Warning,
                    checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                    created_at: now_timestamp(),
                });
            }
        }
        if checkpoint_status.iter().any(|status| {
            status.checkpoint == crawfish_types::OversightCheckpoint::PostResult
                && status.required
                && matches!(status.outcome, CheckpointOutcome::Failed)
        }) {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "post_result_checkpoint_failed".to_string(),
                summary: "post-result checkpoint could not be proven with current evidence"
                    .to_string(),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        if matches!(
            remote_outcome_disposition_for_action(action),
            Some(crawfish_types::RemoteOutcomeDisposition::ReviewRequired)
        ) {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: if matches!(
                    remote_evidence_status,
                    Some(RemoteEvidenceStatus::MissingRequiredEvidence)
                ) {
                    "frontier_enforcement_gap".to_string()
                } else {
                    "remote_state_escalated".to_string()
                },
                summary: federation_pack
                    .as_ref()
                    .map(|pack| {
                        format!(
                            "remote result under federation pack {} requires operator review before acceptance",
                            pack.id
                        )
                    })
                    .unwrap_or_else(|| {
                        "remote result requires operator review before acceptance".to_string()
                    }),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        if matches!(
            remote_outcome_disposition_for_action(action),
            Some(crawfish_types::RemoteOutcomeDisposition::Rejected)
        ) {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "remote_result_rejected".to_string(),
                summary: federation_pack
                    .as_ref()
                    .map(|pack| {
                        format!("remote result was rejected by federation pack {}", pack.id)
                    })
                    .unwrap_or_else(|| {
                        "remote result was rejected by frontier governance".to_string()
                    }),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                created_at: now_timestamp(),
            });
        }
        let has_required_checkpoint_gap = checkpoint_status
            .iter()
            .any(|status| status.required && !matches!(status.outcome, CheckpointOutcome::Passed));
        let has_frontier_specific_gap = incidents
            .iter()
            .any(|incident| incident.reason_code.starts_with("frontier_gap_"));
        if interaction_model_is_frontier(&interaction_model)
            && (has_required_checkpoint_gap || has_frontier_specific_gap)
        {
            incidents.push(PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: doctrine.id.clone(),
                jurisdiction: doctrine.jurisdiction.clone(),
                reason_code: "frontier_enforcement_gap".to_string(),
                summary:
                    "frontier governance required explicit checkpoint evidence, but one or more required checkpoints could not be proven."
                        .to_string(),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: checkpoint_status
                    .iter()
                    .find(|status| status.required && !matches!(status.outcome, CheckpointOutcome::Passed))
                    .map(|status| status.checkpoint.clone())
                    .or_else(|| incidents.iter().find_map(|incident| incident.checkpoint.clone())),
                created_at: now_timestamp(),
            });
        }
        incidents
    }

    pub(crate) fn build_alert_events(
        &self,
        action: &Action,
        profile: Option<&ResolvedEvaluationProfile>,
        evaluation: Option<&EvaluationRecord>,
        incidents: &[PolicyIncident],
        remote_evidence_bundle: Option<&RemoteEvidenceBundle>,
    ) -> Vec<AlertEvent> {
        let federation_pack_id = federation_pack_id_for_action(action);
        let remote_evidence_status = remote_evidence_status_for_action(action);
        let mut configured_rules = profile
            .map(|profile| profile.alert_rules.clone())
            .unwrap_or_default();
        if let Some(treaty_pack) = external_ref_value(&action.external_refs, "a2a.treaty_pack")
            .and_then(|treaty_id| self.config.treaties.packs.get(&treaty_id).cloned())
        {
            for rule_id in treaty_pack.alert_rules {
                if let Some(rule) = self.evaluation_alert_rules().get(&rule_id).cloned() {
                    configured_rules.push(rule);
                }
            }
        }
        if let Some(federation_pack) = federation_pack_id
            .as_ref()
            .and_then(|pack_id| self.resolve_federation_pack_by_id(pack_id, None))
        {
            for rule_id in federation_pack.alert_defaults.rules {
                if let Some(rule) = self.evaluation_alert_rules().get(&rule_id).cloned() {
                    configured_rules.push(rule);
                }
            }
        }
        if !configured_rules
            .iter()
            .any(|rule| rule.id == "frontier_gap_detected")
        {
            configured_rules.push(default_alert_rule_frontier_gap());
        }
        if !configured_rules
            .iter()
            .any(|rule| rule.id == "evaluation_attention_required")
        {
            configured_rules.push(default_alert_rule_evaluation_attention());
        }
        configured_rules
            .into_iter()
            .filter(|rule| alert_rule_matches(rule, evaluation, incidents))
            .map(|rule| AlertEvent {
                id: Uuid::new_v4().to_string(),
                rule_id: rule.id.clone(),
                action_id: action.id.clone(),
                severity: rule.severity.clone(),
                summary: alert_summary_for_rule(&rule, evaluation, incidents),
                federation_pack_id: federation_pack_id.clone(),
                remote_evidence_status: remote_evidence_status.clone(),
                remote_evidence_ref: remote_evidence_bundle.map(|bundle| bundle.id.clone()),
                remote_review_disposition: remote_evidence_bundle
                    .and_then(|bundle| bundle.remote_review_disposition.clone()),
                created_at: now_timestamp(),
                acknowledged_at: None,
                acknowledged_by: None,
            })
            .collect()
    }

    pub(crate) fn evaluation_profiles(&self) -> BTreeMap<String, EvaluationProfile> {
        let mut profiles = builtin_evaluation_profiles();
        profiles.extend(self.config.evaluation.profiles.clone());
        profiles
    }

    pub(crate) fn evaluation_scorecards(&self) -> BTreeMap<String, ScorecardSpec> {
        let mut scorecards = builtin_scorecards();
        scorecards.extend(self.config.evaluation.scorecards.clone());
        scorecards
    }

    pub(crate) fn evaluation_datasets(&self) -> BTreeMap<String, EvaluationDataset> {
        let mut datasets = builtin_evaluation_datasets();
        datasets.extend(self.config.evaluation.datasets.clone());
        datasets
    }

    pub(crate) fn evaluation_alert_rules(&self) -> BTreeMap<String, AlertRule> {
        let mut rules = builtin_alert_rules();
        rules.extend(self.config.evaluation.alerts.clone());
        rules
    }

    pub(crate) fn evaluation_pairwise_profiles(&self) -> BTreeMap<String, PairwiseProfile> {
        let mut profiles = builtin_pairwise_profiles();
        profiles.extend(self.config.evaluation.pairwise_profiles.clone());
        profiles
    }

    pub(crate) fn resolve_evaluation_profile(
        &self,
        action: &Action,
    ) -> anyhow::Result<Option<ResolvedEvaluationProfile>> {
        let requested_name =
            if let Some(profile) = action.contract.quality.evaluation_profile.clone() {
                Some(profile)
            } else if let Some(hook) = action.contract.quality.evaluation_hook.as_deref() {
                legacy_evaluation_hook_profile_name(hook).map(ToString::to_string)
            } else {
                builtin_profile_name_for_action(action).map(ToString::to_string)
            };

        let Some(profile_name) = requested_name else {
            return Ok(None);
        };

        let profiles = self.evaluation_profiles();
        let Some(profile) = profiles.get(&profile_name).cloned() else {
            return Ok(None);
        };
        let scorecards = self.evaluation_scorecards();
        let Some(scorecard) = scorecards.get(&profile.scorecard).cloned() else {
            return Ok(None);
        };
        let datasets = self.evaluation_datasets();
        let dataset = profile.dataset_name.as_ref().and_then(|name| {
            datasets
                .get(name)
                .cloned()
                .map(|dataset| (name.clone(), dataset))
        });
        let alerts = self.evaluation_alert_rules();
        let alert_rules = profile
            .alert_rules
            .iter()
            .filter_map(|name| alerts.get(name).cloned())
            .collect();

        Ok(Some(ResolvedEvaluationProfile {
            name: profile_name,
            profile,
            scorecard,
            dataset,
            alert_rules,
        }))
    }

    pub(crate) fn resolve_pairwise_profile(
        &self,
        dataset: &EvaluationDataset,
        requested_name: Option<&str>,
    ) -> anyhow::Result<Option<ResolvedPairwiseProfile>> {
        let profile_name = if let Some(name) = requested_name {
            name.to_string()
        } else if let Some(name) = builtin_pairwise_profile_name_for_capability(&dataset.capability)
        {
            name.to_string()
        } else {
            return Ok(None);
        };

        let profiles = self.evaluation_pairwise_profiles();
        let Some(profile) = profiles.get(&profile_name).cloned() else {
            return Ok(None);
        };
        if profile.capability != dataset.capability {
            anyhow::bail!(
                "pairwise profile {} targets {}, not {}",
                profile_name,
                profile.capability,
                dataset.capability
            );
        }
        Ok(Some(ResolvedPairwiseProfile {
            name: profile_name,
            profile,
        }))
    }

    pub(crate) async fn scorecard_criterion_result(
        &self,
        action: &Action,
        _doctrine: &DoctrinePack,
        interaction_model: &crawfish_types::InteractionModel,
        checkpoint_status: &[CheckpointStatus],
        observed_incidents: &[PolicyIncident],
        criterion: &ScorecardCriterion,
    ) -> anyhow::Result<crawfish_types::EvaluationCriterionResult> {
        let passed = match criterion.kind {
            ScorecardCriterionKind::ArtifactPresent => criterion
                .artifact_name
                .as_deref()
                .and_then(|name| artifact_ref_by_name(action, name))
                .is_some(),
            ScorecardCriterionKind::ArtifactAbsent => criterion
                .artifact_name
                .as_deref()
                .map(|name| artifact_ref_by_name(action, name).is_none())
                .unwrap_or(false),
            ScorecardCriterionKind::JsonFieldNonempty => {
                let Some(target) = scorecard_target_value(
                    action,
                    criterion.artifact_name.as_deref(),
                    criterion.field_path.as_deref(),
                )
                .await?
                else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target value missing".to_string(),
                    });
                };
                json_value_is_nonempty(&target)
            }
            ScorecardCriterionKind::JsonSchemaValid => {
                let Some(target) = scorecard_target_value(
                    action,
                    criterion.artifact_name.as_deref(),
                    criterion.field_path.as_deref(),
                )
                .await?
                else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target value missing".to_string(),
                    });
                };
                let Some(schema) = criterion.json_schema.as_ref() else {
                    anyhow::bail!(
                        "json_schema_valid criterion {} missing json_schema",
                        criterion.id
                    );
                };
                validator_for(schema)?.is_valid(&target)
            }
            ScorecardCriterionKind::ListMinLen => {
                let Some(target) = scorecard_target_value(
                    action,
                    criterion.artifact_name.as_deref(),
                    criterion.field_path.as_deref(),
                )
                .await?
                else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target value missing".to_string(),
                    });
                };
                target
                    .as_array()
                    .map(|items| items.len() >= criterion.min_len.unwrap_or(1) as usize)
                    .unwrap_or(false)
            }
            ScorecardCriterionKind::RegexMatch => {
                let Some(target_text) = scorecard_target_text(
                    action,
                    criterion.artifact_name.as_deref(),
                    criterion.field_path.as_deref(),
                )
                .await?
                else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target text missing".to_string(),
                    });
                };
                let Some(pattern) = criterion.regex_pattern.as_deref() else {
                    anyhow::bail!(
                        "regex_match criterion {} missing regex_pattern",
                        criterion.id
                    );
                };
                Regex::new(pattern)?.is_match(&target_text)
            }
            ScorecardCriterionKind::NumericThreshold => {
                let Some(target) = scorecard_target_value(
                    action,
                    criterion.artifact_name.as_deref(),
                    criterion.field_path.as_deref(),
                )
                .await?
                else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target value missing".to_string(),
                    });
                };
                let Some(number) = target.as_f64() else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target value was not numeric".to_string(),
                    });
                };
                let threshold = criterion.numeric_threshold.unwrap_or_default();
                match criterion
                    .numeric_comparison
                    .clone()
                    .unwrap_or(NumericComparison::GreaterThanOrEqual)
                {
                    NumericComparison::GreaterThan => number > threshold,
                    NumericComparison::GreaterThanOrEqual => number >= threshold,
                    NumericComparison::LessThan => number < threshold,
                    NumericComparison::LessThanOrEqual => number <= threshold,
                    NumericComparison::Equal => (number - threshold).abs() < f64::EPSILON,
                }
            }
            ScorecardCriterionKind::FieldEquals => {
                let Some(target) = scorecard_target_value(
                    action,
                    criterion.artifact_name.as_deref(),
                    criterion.field_path.as_deref(),
                )
                .await?
                else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "target value missing".to_string(),
                    });
                };
                let Some(expected) = criterion.expected_value.as_ref() else {
                    anyhow::bail!(
                        "field_equals criterion {} missing expected_value",
                        criterion.id
                    );
                };
                target == *expected
            }
            ScorecardCriterionKind::TokenCoverage => {
                let Some(source_path) = criterion.source_path.as_deref() else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "source_path missing".to_string(),
                    });
                };
                let source_tokens = scorecard_source_tokens(action, source_path);
                if source_tokens.is_empty() {
                    true
                } else {
                    let Some(target_text) = scorecard_target_text(
                        action,
                        criterion.artifact_name.as_deref(),
                        criterion.field_path.as_deref(),
                    )
                    .await?
                    else {
                        return Ok(crawfish_types::EvaluationCriterionResult {
                            criterion_id: criterion.id.clone(),
                            passed: false,
                            score_contribution: 0.0,
                            evidence_summary: "target text missing".to_string(),
                        });
                    };
                    let target_text = target_text.to_ascii_lowercase();
                    source_tokens
                        .into_iter()
                        .all(|token| target_text.contains(&token))
                }
            }
            ScorecardCriterionKind::CheckpointPassed => {
                let Some(checkpoint) = criterion.checkpoint.as_ref() else {
                    return Ok(crawfish_types::EvaluationCriterionResult {
                        criterion_id: criterion.id.clone(),
                        passed: false,
                        score_contribution: 0.0,
                        evidence_summary: "checkpoint missing".to_string(),
                    });
                };
                checkpoint_status.iter().any(|status| {
                    &status.checkpoint == checkpoint
                        && matches!(status.outcome, CheckpointOutcome::Passed)
                })
            }
            ScorecardCriterionKind::IncidentAbsent => {
                if let Some(code) = criterion.incident_code.as_deref() {
                    !observed_incidents
                        .iter()
                        .any(|incident| incident.reason_code == code)
                } else {
                    observed_incidents.is_empty()
                }
            }
            ScorecardCriterionKind::ExternalRefPresent => criterion
                .external_ref_kind
                .as_deref()
                .map(|kind| {
                    action
                        .external_refs
                        .iter()
                        .any(|reference| reference.kind == kind)
                })
                .unwrap_or(false),
            ScorecardCriterionKind::InteractionModelIs => criterion
                .interaction_model
                .as_ref()
                .map(|expected| expected == interaction_model)
                .unwrap_or(false),
            ScorecardCriterionKind::RemoteOutcomeDispositionIs => criterion
                .remote_outcome_disposition
                .as_ref()
                .map(|expected| {
                    remote_outcome_disposition_for_action(action)
                        .as_ref()
                        .map(|actual| actual == expected)
                        .unwrap_or(false)
                })
                .unwrap_or(false),
            ScorecardCriterionKind::TreatyViolationAbsent => {
                let violations = treaty_violations_for_action(action);
                if let Some(code) = criterion.treaty_violation_code.as_deref() {
                    !violations.iter().any(|violation| violation.code == code)
                } else {
                    violations.is_empty()
                }
            }
        };

        Ok(crawfish_types::EvaluationCriterionResult {
            criterion_id: criterion.id.clone(),
            passed,
            score_contribution: if passed {
                f64::from(criterion.weight.max(1))
            } else {
                0.0
            },
            evidence_summary: scorecard_evidence_summary(
                action,
                criterion,
                interaction_model,
                observed_incidents,
                passed,
            )
            .await?,
        })
    }

    pub(crate) async fn maybe_capture_dataset_case(
        &self,
        action: &Action,
        profile: Option<&ResolvedEvaluationProfile>,
        trace: &TraceBundle,
        evaluations: &[EvaluationRecord],
    ) -> anyhow::Result<Option<DatasetCase>> {
        let Some(profile) = profile else {
            return Ok(None);
        };
        if !profile.profile.dataset_capture {
            return Ok(None);
        }
        let Some((dataset_name, dataset)) = profile.dataset.as_ref() else {
            return Ok(None);
        };
        if !dataset.auto_capture {
            return Ok(None);
        }

        Ok(Some(DatasetCase {
            id: Uuid::new_v4().to_string(),
            dataset_name: dataset_name.clone(),
            capability: action.capability.clone(),
            goal_summary: action.goal.summary.clone(),
            interaction_model: trace.interaction_model.clone(),
            normalized_inputs: action.inputs.clone(),
            expected_artifacts: action
                .outputs
                .artifacts
                .iter()
                .map(artifact_basename)
                .collect(),
            expected_output_signals: metadata_string_array(&action.inputs, "desired_outputs"),
            source_action_id: action.id.clone(),
            jurisdiction_class: trace.jurisdiction_class.clone(),
            doctrine_summary: trace.doctrine_summary.clone(),
            checkpoint_status: trace.checkpoint_status.clone(),
            policy_incidents: trace.policy_incidents.clone(),
            remote_principal: trace.remote_principal.clone(),
            treaty_pack_id: trace.treaty_pack_id.clone(),
            federation_pack_id: trace.federation_pack_id.clone(),
            federation_decision: trace.federation_decision.clone(),
            delegation_receipt_ref: trace.delegation_receipt_ref.clone(),
            remote_task_ref: trace.remote_task_ref.clone(),
            remote_outcome_disposition: trace.remote_outcome_disposition.clone(),
            remote_evidence_status: trace.remote_evidence_status.clone(),
            remote_evidence_ref: trace.remote_evidence_ref.clone(),
            remote_attempt_refs: trace.remote_attempt_refs.clone(),
            remote_followup_refs: trace.remote_followup_refs.clone(),
            remote_review_disposition: trace.remote_review_disposition.clone(),
            remote_state_disposition: trace.remote_state_disposition.clone(),
            treaty_violations: trace.treaty_violations.clone(),
            delegation_depth: trace.delegation_depth,
            verification_summary: trace.verification_summary.clone(),
            evaluation_refs: evaluations
                .iter()
                .map(|evaluation| evaluation.id.clone())
                .collect(),
            created_at: now_timestamp(),
        }))
    }

    pub(crate) async fn run_experiment_case(
        &self,
        run: &ExperimentRun,
        case: &DatasetCase,
    ) -> anyhow::Result<ExperimentCaseResult> {
        let source_action = self
            .store
            .get_action(&case.source_action_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source action not found: {}", case.source_action_id))?;
        let manifest = self
            .store
            .get_agent_manifest(&source_action.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", source_action.target_agent_id))?;

        let mut action = source_action.clone();
        action.id = format!("experiment-{}", Uuid::new_v4());
        action.phase = ActionPhase::Running;
        action.created_at = now_timestamp();
        action.started_at = Some(now_timestamp());
        action.finished_at = None;
        action.checkpoint_ref = None;
        action.selected_executor = None;
        action.recovery_stage = None;
        action.external_refs = Vec::new();
        action.outputs = ActionOutputs::default();
        action.continuity_mode = None;
        action.degradation_profile = None;
        action.failure_reason = None;
        action.failure_code = None;
        action.inputs = case.normalized_inputs.clone();
        action.execution_strategy = Some(ExecutionStrategy {
            mode: ExecutionStrategyMode::SinglePass,
            verification_spec: None,
            stop_budget: None,
            feedback_policy: crawfish_types::FeedbackPolicy::default(),
        });
        let (preferred_harnesses, fallback_chain) = replay_routes_for_executor(&run.executor);
        action.contract.execution.fallback_chain = fallback_chain;
        action.contract.execution.preferred_harnesses = preferred_harnesses;

        let outcome = match case.capability.as_str() {
            "task.plan" => {
                self.execute_task_plan_single_pass(&mut action, &manifest)
                    .await?
            }
            "repo.review" if run.executor == "deterministic" => {
                let workspace_root = required_input_string(&action, "workspace_root")?;
                let (repo_index_ref, repo_index) = self
                    .ensure_repo_index_for_workspace(&workspace_root)
                    .await?;
                let executor = RepoReviewerDeterministicExecutor::new(
                    self.state_dir(),
                    repo_index,
                    Some(repo_index_ref),
                );
                match executor.execute(&action).await {
                    Ok(outputs) => ExecutionOutcome::Completed {
                        outputs,
                        selected_executor: "deterministic.repo_review".to_string(),
                        checkpoint: None,
                        external_refs: Vec::new(),
                        surface_events: Vec::new(),
                    },
                    Err(error) => ExecutionOutcome::Failed {
                        reason: error.to_string(),
                        failure_code: failure_code_executor_error().to_string(),
                        outputs: ActionOutputs::default(),
                        checkpoint: None,
                        external_refs: Vec::new(),
                        surface_events: Vec::new(),
                    },
                }
            }
            "incident.enrich" if run.executor == "deterministic" => {
                let executor = IncidentEnricherDeterministicExecutor::new(self.state_dir());
                match executor.execute(&action).await {
                    Ok(outputs) => ExecutionOutcome::Completed {
                        outputs,
                        selected_executor: "deterministic.incident_enrich".to_string(),
                        checkpoint: None,
                        external_refs: Vec::new(),
                        surface_events: Vec::new(),
                    },
                    Err(error) => ExecutionOutcome::Failed {
                        reason: error.to_string(),
                        failure_code: failure_code_executor_error().to_string(),
                        outputs: ActionOutputs::default(),
                        checkpoint: None,
                        external_refs: Vec::new(),
                        surface_events: Vec::new(),
                    },
                }
            }
            _ => ExecutionOutcome::Failed {
                reason: format!(
                    "executor {} does not support replay for {}",
                    run.executor, case.capability
                ),
                failure_code: failure_code_route_unavailable().to_string(),
                outputs: ActionOutputs::default(),
                checkpoint: None,
                external_refs: Vec::new(),
                surface_events: Vec::new(),
            },
        };

        match outcome {
            ExecutionOutcome::Completed {
                outputs,
                selected_executor,
                external_refs,
                ..
            } => {
                action.phase = ActionPhase::Completed;
                action.finished_at = Some(now_timestamp());
                action.outputs = outputs.clone();
                action.selected_executor = Some(selected_executor.clone());
                action.external_refs = external_refs.clone();
                let interaction_model = interaction_model_for_action(&action, None);
                let doctrine = default_doctrine_pack(
                    &action,
                    &interaction_model,
                    jurisdiction_class_for_action(&action, None),
                );
                let resolved_profile = self.resolve_evaluation_profile(&action)?;
                let checkpoint_status = checkpoint_status_for_action(
                    &action,
                    &doctrine,
                    false,
                    None,
                    resolved_profile.is_some(),
                );
                let preliminary_incidents = self.policy_incidents_for_action(
                    &action,
                    resolved_profile.as_ref(),
                    None,
                    &checkpoint_status,
                );
                let evaluation = self
                    .evaluate_action_outputs(
                        &action,
                        resolved_profile.as_ref(),
                        &doctrine,
                        &interaction_model,
                        &checkpoint_status,
                        &preliminary_incidents,
                    )
                    .await?
                    .into_iter()
                    .last();
                let incidents = self.policy_incidents_for_action(
                    &action,
                    resolved_profile.as_ref(),
                    evaluation.as_ref(),
                    &checkpoint_status,
                );
                let latest_remote_attempt = self
                    .store
                    .list_remote_attempt_records(&action.id)
                    .await?
                    .into_iter()
                    .last();
                let latest_remote_evidence = self
                    .store
                    .list_remote_evidence_bundles(&action.id)
                    .await?
                    .into_iter()
                    .last();
                Ok(ExperimentCaseResult {
                    id: Uuid::new_v4().to_string(),
                    run_id: run.id.clone(),
                    dataset_case_id: case.id.clone(),
                    capability: case.capability.clone(),
                    status: match evaluation.as_ref().map(|evaluation| &evaluation.status) {
                        Some(EvaluationStatus::Failed) => ExperimentCaseStatus::Failed,
                        _ => ExperimentCaseStatus::Passed,
                    },
                    selected_executor: Some(selected_executor),
                    evaluation_status: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.status.clone()),
                    score: evaluation.as_ref().and_then(|evaluation| evaluation.score),
                    summary: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.summary.clone())
                        .unwrap_or_else(|| "experiment run completed".to_string()),
                    findings: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.findings.clone())
                        .unwrap_or_default(),
                    criterion_results: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.criterion_results.clone())
                        .unwrap_or_default(),
                    artifact_refs: outputs.artifacts,
                    external_refs,
                    policy_incident_count: incidents.len() as u32,
                    interaction_model: Some(interaction_model),
                    remote_outcome_disposition: remote_outcome_disposition_for_action(&action),
                    treaty_violation_count: treaty_violations_for_action(&action).len() as u32,
                    federation_pack_id: federation_pack_id_for_action(&action),
                    remote_evidence_status: remote_evidence_status_for_action(&action),
                    remote_evidence_ref: latest_remote_evidence
                        .as_ref()
                        .map(|bundle| bundle.id.clone()),
                    remote_review_disposition: remote_review_disposition_for_action(&action),
                    remote_attempt_ref: latest_remote_attempt
                        .as_ref()
                        .map(|attempt| attempt.id.clone()),
                    remote_followup_ref: latest_remote_attempt
                        .as_ref()
                        .and_then(|attempt| attempt.followup_request_ref.clone()),
                    failure_code: None,
                    created_at: now_timestamp(),
                })
            }
            ExecutionOutcome::Blocked {
                reason,
                failure_code,
                outputs,
                external_refs,
                ..
            }
            | ExecutionOutcome::Failed {
                reason,
                failure_code,
                outputs,
                external_refs,
                ..
            } => {
                action.phase = if failure_code == "a2a_auth_required" {
                    ActionPhase::AwaitingApproval
                } else if failure_code == "a2a_input_required" {
                    ActionPhase::Blocked
                } else {
                    ActionPhase::Failed
                };
                action.finished_at = if matches!(
                    action.phase,
                    ActionPhase::Blocked | ActionPhase::AwaitingApproval
                ) {
                    None
                } else {
                    Some(now_timestamp())
                };
                action.failure_reason = Some(reason.clone());
                action.failure_code = Some(failure_code.clone());
                action.outputs = outputs.clone();
                action.external_refs = external_refs.clone();
                action.selected_executor = action
                    .selected_executor
                    .clone()
                    .or_else(|| selected_executor_from_external_refs(&external_refs));
                let interaction_model = interaction_model_for_action(&action, None);
                let doctrine = default_doctrine_pack(
                    &action,
                    &interaction_model,
                    jurisdiction_class_for_action(&action, None),
                );
                let resolved_profile = self.resolve_evaluation_profile(&action)?;
                let checkpoint_status = checkpoint_status_for_action(
                    &action,
                    &doctrine,
                    false,
                    None,
                    resolved_profile.is_some(),
                );
                let preliminary_incidents = self.policy_incidents_for_action(
                    &action,
                    resolved_profile.as_ref(),
                    None,
                    &checkpoint_status,
                );
                let evaluation = self
                    .evaluate_action_outputs(
                        &action,
                        resolved_profile.as_ref(),
                        &doctrine,
                        &interaction_model,
                        &checkpoint_status,
                        &preliminary_incidents,
                    )
                    .await?
                    .into_iter()
                    .last();
                let incidents = self.policy_incidents_for_action(
                    &action,
                    resolved_profile.as_ref(),
                    evaluation.as_ref(),
                    &checkpoint_status,
                );
                let latest_remote_attempt = self
                    .store
                    .list_remote_attempt_records(&action.id)
                    .await?
                    .into_iter()
                    .last();
                let latest_remote_evidence = self
                    .store
                    .list_remote_evidence_bundles(&action.id)
                    .await?
                    .into_iter()
                    .last();

                Ok(ExperimentCaseResult {
                    id: Uuid::new_v4().to_string(),
                    run_id: run.id.clone(),
                    dataset_case_id: case.id.clone(),
                    capability: case.capability.clone(),
                    status: ExperimentCaseStatus::Failed,
                    selected_executor: action.selected_executor.clone(),
                    evaluation_status: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.status.clone()),
                    score: evaluation.as_ref().and_then(|evaluation| evaluation.score),
                    summary: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.summary.clone())
                        .unwrap_or(reason),
                    findings: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.findings.clone())
                        .unwrap_or_default(),
                    criterion_results: evaluation
                        .as_ref()
                        .map(|evaluation| evaluation.criterion_results.clone())
                        .unwrap_or_default(),
                    artifact_refs: outputs.artifacts,
                    external_refs,
                    policy_incident_count: incidents.len() as u32,
                    interaction_model: Some(interaction_model),
                    remote_outcome_disposition: remote_outcome_disposition_for_action(&action),
                    treaty_violation_count: treaty_violations_for_action(&action).len() as u32,
                    federation_pack_id: federation_pack_id_for_action(&action),
                    remote_evidence_status: remote_evidence_status_for_action(&action),
                    remote_evidence_ref: latest_remote_evidence
                        .as_ref()
                        .map(|bundle| bundle.id.clone()),
                    remote_review_disposition: remote_review_disposition_for_action(&action),
                    remote_attempt_ref: latest_remote_attempt
                        .as_ref()
                        .map(|attempt| attempt.id.clone()),
                    remote_followup_ref: latest_remote_attempt
                        .as_ref()
                        .and_then(|attempt| attempt.followup_request_ref.clone()),
                    failure_code: Some(failure_code),
                    created_at: now_timestamp(),
                })
            }
        }
    }

    pub(crate) async fn record_verification_evaluation(
        &self,
        action: &Action,
        iteration: u32,
        summary: &VerificationSummary,
        feedback: Option<&String>,
    ) -> anyhow::Result<()> {
        let latest_remote_attempt = self
            .store
            .list_remote_attempt_records(&action.id)
            .await?
            .into_iter()
            .last();
        let evaluation = EvaluationRecord {
            id: Uuid::new_v4().to_string(),
            action_id: action.id.clone(),
            evaluator: "verify_loop".to_string(),
            status: match summary.status {
                VerificationStatus::Passed => crawfish_types::EvaluationStatus::Passed,
                VerificationStatus::Failed | VerificationStatus::BudgetExhausted => {
                    crawfish_types::EvaluationStatus::Failed
                }
            },
            score: None,
            summary: format!(
                "verify_loop iteration {iteration} {}",
                runtime_enum_to_snake(&summary.status)
            ),
            findings: feedback.into_iter().cloned().collect(),
            criterion_results: Vec::new(),
            interaction_model: Some(interaction_model_for_action(action, None)),
            remote_outcome_disposition: remote_outcome_disposition_for_action(action),
            treaty_violation_count: treaty_violations_for_action(action).len() as u32,
            federation_pack_id: federation_pack_id_for_action(action),
            remote_evidence_status: remote_evidence_status_for_action(action),
            remote_evidence_ref: None,
            remote_review_disposition: remote_review_disposition_for_action(action),
            remote_attempt_ref: latest_remote_attempt
                .as_ref()
                .map(|attempt| attempt.id.clone()),
            remote_followup_ref: latest_remote_attempt
                .as_ref()
                .and_then(|attempt| attempt.followup_request_ref.clone()),
            feedback_note_id: None,
            created_at: now_timestamp(),
        };
        self.store.insert_evaluation(&evaluation).await?;
        self.store
            .append_action_event(
                &action.id,
                "evaluation_recorded",
                serde_json::json!({
                    "evaluation_id": evaluation.id,
                    "evaluator": evaluation.evaluator,
                    "status": format!("{:?}", evaluation.status).to_lowercase(),
                    "iteration": iteration,
                }),
            )
            .await?;
        if !matches!(summary.status, VerificationStatus::Passed) {
            self.store
                .append_action_event(
                    &action.id,
                    "alert_triggered",
                    serde_json::json!({
                        "rule_id": "verification_attention_required",
                        "name": "Verification attention required",
                        "trigger": "verify_loop",
                        "severity": "info",
                    }),
                )
                .await?;
        }
        Ok(())
    }
}
