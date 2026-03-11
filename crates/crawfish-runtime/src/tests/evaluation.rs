use super::support::*;
use super::*;

#[tokio::test]
async fn task_plan_remote_actions_use_remote_evaluation_profile_and_dataset_metadata() {
    let dir = tempdir().unwrap();
    let a2a_url = spawn_runtime_a2a_server(RuntimeA2aMode::StreamingCompleted).await;
    std::env::set_var("A2A_REMOTE_TOKEN", "remote-token");
    let manifest = local_task_planner_manifest(
        "__missing_claude__",
        "__missing_codex__",
        "ws://127.0.0.1:9/unavailable",
    )
    .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
    let config = include_str!("../../../../examples/experimental/remote-swarm/Crawfish.toml")
        .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
    let supervisor =
        build_supervisor_with_task_planner_manifest_and_config(dir.path(), manifest, config, None)
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a remote-agent evaluation path with treaty evidence",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        detail.evaluation_profile.as_deref(),
        Some("task_plan_remote_default")
    );
    assert_eq!(
        detail.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
    );

    let evaluations = supervisor
        .list_action_evaluations(&submitted.action_id)
        .await
        .unwrap();
    let latest = evaluations
        .evaluations
        .iter()
        .rev()
        .find(|evaluation| evaluation.evaluator == "task_plan_remote_default")
        .expect("remote task-plan evaluation");
    assert_eq!(
        latest.interaction_model,
        Some(crawfish_types::InteractionModel::RemoteAgent)
    );
    assert_eq!(
        latest.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
    );
    assert_eq!(latest.treaty_violation_count, 0);
    assert!(latest
        .criterion_results
        .iter()
        .any(
            |criterion| criterion.criterion_id == "interaction_model_remote_agent"
                && criterion.passed
        ));
    assert!(latest
        .criterion_results
        .iter()
        .any(|criterion| criterion.criterion_id == "remote_outcome_accepted" && criterion.passed));
    assert!(latest
        .criterion_results
        .iter()
        .any(|criterion| criterion.criterion_id == "no_treaty_violations" && criterion.passed));

    let dataset = supervisor
        .get_evaluation_dataset("task_plan_dataset")
        .await
        .unwrap()
        .expect("task plan dataset");
    let case = dataset
        .cases
        .iter()
        .find(|case| case.source_action_id == submitted.action_id)
        .expect("captured dataset case");
    assert_eq!(
        case.interaction_model,
        Some(crawfish_types::InteractionModel::RemoteAgent)
    );
    assert_eq!(
        case.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
    );
    assert_eq!(case.treaty_pack_id.as_deref(), Some("remote_task_planning"));
    assert!(case.treaty_violations.is_empty());
}

#[tokio::test]
async fn task_plan_remote_evidence_gap_fails_remote_scorecard() {
    let dir = tempdir().unwrap();
    let a2a_url = spawn_runtime_a2a_server(RuntimeA2aMode::StreamingMissingTaskRef).await;
    std::env::set_var("A2A_REMOTE_TOKEN", "remote-token");
    let manifest = local_task_planner_manifest(
        "__missing_claude__",
        "__missing_codex__",
        "ws://127.0.0.1:9/unavailable",
    )
    .replace(
        "preferred_harnesses = [\"claude_code\", \"codex\", \"a2a\", \"openclaw\"]",
        "preferred_harnesses = [\"a2a\"]",
    )
    .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
    let config = include_str!("../../../../examples/experimental/remote-swarm/Crawfish.toml")
        .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
    let supervisor =
        build_supervisor_with_task_planner_manifest_and_config(dir.path(), manifest, config, None)
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a remote task that should trigger a frontier evidence gap",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        detail.evaluation_profile.as_deref(),
        Some("task_plan_remote_default")
    );
    assert_eq!(detail.action.phase, ActionPhase::Blocked);
    assert_eq!(
        detail.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::ReviewRequired)
    );

    let evaluations = supervisor
        .list_action_evaluations(&submitted.action_id)
        .await
        .unwrap();
    let latest = evaluations
        .evaluations
        .iter()
        .rev()
        .find(|evaluation| evaluation.evaluator == "task_plan_remote_default")
        .expect("remote task-plan evaluation");
    assert!(matches!(latest.status, EvaluationStatus::Failed));
    assert_eq!(latest.treaty_violation_count, 1);
    assert!(latest
        .criterion_results
        .iter()
        .any(|criterion| criterion.criterion_id == "remote_outcome_accepted" && !criterion.passed));
    assert!(latest
        .criterion_results
        .iter()
        .any(
            |criterion| criterion.criterion_id == "no_frontier_gap_violation" && !criterion.passed
        ));
}

#[tokio::test]
async fn task_plan_emits_trace_evaluations_and_review_queue_items() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a rollout investigation for the local swarm runtime",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.interaction_model,
        Some(crawfish_types::InteractionModel::SameOwnerSwarm)
    );
    assert_eq!(
        detail.jurisdiction_class,
        Some(JurisdictionClass::SameOwnerLocal)
    );
    assert!(detail.doctrine_summary.is_some());
    assert!(detail
        .checkpoint_status
        .iter()
        .any(|status| status.checkpoint == crawfish_types::OversightCheckpoint::PostResult));
    assert!(detail.latest_evaluation.is_some());

    let (handle, socket_path) = spawn_api_server(Arc::clone(&supervisor)).await;

    let (status, trace_payload) = get_uds_json(
        &socket_path,
        &format!("/v1/actions/{}/trace", submitted.action_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(trace_payload["trace"]["action_id"], submitted.action_id);
    assert_eq!(
        trace_payload["trace"]["interaction_model"],
        "same_owner_swarm"
    );
    assert!(trace_payload["trace"]["enforcement_records"]
        .as_array()
        .unwrap()
        .iter()
        .any(|record| record["checkpoint"] == "post_result"));

    let (status, eval_payload) = get_uds_json(
        &socket_path,
        &format!("/v1/actions/{}/evaluations", submitted.action_id),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!eval_payload["evaluations"].as_array().unwrap().is_empty());

    let (status, review_payload) = get_uds_json(&socket_path, "/v1/review-queue").await;
    assert_eq!(status, StatusCode::OK);
    assert!(review_payload["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["action_id"] == submitted.action_id));

    handle.abort();
}

#[tokio::test]
async fn review_queue_resolution_creates_feedback_note() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a deterministic incident response drill",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let review_item = supervisor
        .list_review_queue()
        .await
        .unwrap()
        .items
        .into_iter()
        .find(|item| item.action_id == submitted.action_id)
        .expect("review queue item");

    let resolved = supervisor
        .resolve_review_queue_item(
            &review_item.id,
            ResolveReviewQueueItemRequest {
                resolver_ref: "operator-1".to_string(),
                resolution: "approved_for_followup".to_string(),
                note: Some("Keep this plan as the baseline for the next run.".to_string()),
            },
        )
        .await
        .unwrap();
    assert_eq!(resolved.item.status, ReviewQueueStatus::Resolved);

    let evaluations = supervisor
        .list_action_evaluations(&submitted.action_id)
        .await
        .unwrap();
    let feedback_id = evaluations
        .evaluations
        .iter()
        .find_map(|evaluation| evaluation.feedback_note_id.clone())
        .expect("feedback note id");
    let feedback = supervisor
        .store()
        .get_feedback_note(&feedback_id)
        .await
        .unwrap()
        .expect("feedback note");
    assert_eq!(feedback.action_id, submitted.action_id);
    assert!(feedback.body.contains("baseline"));
}

#[tokio::test]
async fn task_plan_dataset_capture_and_replay_are_operator_visible() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a doctrine-aware evaluation rollout for the local swarm",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let datasets = supervisor.list_evaluation_datasets().await.unwrap();
    let dataset = datasets
        .datasets
        .iter()
        .find(|dataset| dataset.name == "task_plan_dataset")
        .expect("task plan dataset");
    assert!(dataset.case_count >= 1);

    let detail = supervisor
        .get_evaluation_dataset("task_plan_dataset")
        .await
        .unwrap()
        .expect("dataset detail");
    assert!(detail.cases.iter().all(|case| {
        case.interaction_model == Some(crawfish_types::InteractionModel::SameOwnerSwarm)
    }));
    let dataset_case_ids: std::collections::BTreeSet<String> =
        detail.cases.iter().map(|case| case.id.clone()).collect();
    assert!(detail
        .cases
        .iter()
        .any(|case| case.source_action_id == submitted.action_id));

    let review_count_before = supervisor.list_review_queue().await.unwrap().items.len();
    let run = supervisor
        .start_evaluation_run(StartEvaluationRunRequest {
            dataset: "task_plan_dataset".to_string(),
            executor: "deterministic".to_string(),
        })
        .await
        .unwrap();
    assert!(matches!(
        run.run.status,
        ExperimentRunStatus::Completed | ExperimentRunStatus::Failed
    ));

    let run_detail = supervisor
        .get_evaluation_run(&run.run.id)
        .await
        .unwrap()
        .expect("experiment run detail");
    assert_eq!(run_detail.run.dataset_name, "task_plan_dataset");
    assert!(!run_detail.cases.is_empty());
    assert!(run_detail
        .cases
        .iter()
        .all(|case| dataset_case_ids.contains(&case.dataset_case_id)));

    let review_count_after = supervisor.list_review_queue().await.unwrap().items.len();
    assert_eq!(review_count_before, review_count_after);
}

#[tokio::test]
async fn richer_task_plan_evaluation_persists_criterion_evidence() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan an evaluation rollout with doctrine checkpoints and artifact coverage",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let evaluations = supervisor
        .list_action_evaluations(&submitted.action_id)
        .await
        .unwrap();
    let latest = evaluations
        .evaluations
        .iter()
        .rev()
        .find(|evaluation| !evaluation.criterion_results.is_empty())
        .expect("latest scorecard evaluation");
    assert!(!latest.criterion_results.is_empty());
    assert!(latest
        .criterion_results
        .iter()
        .any(|criterion| criterion.criterion_id == "task_plan_schema" && criterion.passed));
    assert!(latest
        .criterion_results
        .iter()
        .any(|criterion| criterion.criterion_id == "task_plan_heading"));
    assert!(latest
        .criterion_results
        .iter()
        .all(|criterion| !criterion.evidence_summary.trim().is_empty()));
}

#[tokio::test]
async fn pairwise_compare_creates_review_items_and_feedback_lineage() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a pairwise evaluation flow for the local swarm",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let review_count_before = supervisor.list_review_queue().await.unwrap().items.len();
    let started = supervisor
        .start_pairwise_evaluation_run(StartPairwiseEvaluationRunRequest {
            dataset: "task_plan_dataset".to_string(),
            left_executor: "deterministic".to_string(),
            right_executor: "deterministic".to_string(),
            profile: None,
        })
        .await
        .unwrap();
    let pairwise = supervisor
        .get_pairwise_evaluation_run(&started.run.id)
        .await
        .unwrap()
        .expect("pairwise detail");
    assert_eq!(pairwise.run.status, PairwiseExperimentRunStatus::Completed);
    assert!(!pairwise.cases.is_empty());
    assert!(pairwise
        .cases
        .iter()
        .all(|case| case.outcome == PairwiseOutcome::NeedsReview));

    let review_queue = supervisor.list_review_queue().await.unwrap();
    assert!(review_queue.items.len() > review_count_before);
    let pairwise_item = review_queue
        .items
        .iter()
        .find(|item| item.kind == ReviewQueueKind::PairwiseEval)
        .expect("pairwise review item");
    assert!(pairwise_item.pairwise_run_ref.is_some());
    assert!(pairwise_item.pairwise_case_ref.is_some());

    let resolved = supervisor
        .resolve_review_queue_item(
            &pairwise_item.id,
            ResolveReviewQueueItemRequest {
                resolver_ref: "operator-2".to_string(),
                resolution: "prefer_left".to_string(),
                note: Some("Left executor stays the baseline.".to_string()),
            },
        )
        .await
        .unwrap();
    assert_eq!(resolved.item.status, ReviewQueueStatus::Resolved);

    let pairwise_case = supervisor
        .store()
        .get_pairwise_case_result(
            pairwise_item
                .pairwise_case_ref
                .as_deref()
                .expect("pairwise case ref"),
        )
        .await
        .unwrap()
        .expect("pairwise case");
    assert_eq!(
        pairwise_case.review_resolution.as_deref(),
        Some("prefer_left")
    );
    let feedback_id = pairwise_case
        .feedback_note_id
        .as_deref()
        .expect("feedback note id");
    let feedback = supervisor
        .store()
        .get_feedback_note(feedback_id)
        .await
        .unwrap()
        .expect("feedback note");
    assert_eq!(
        feedback.pairwise_case_result_ref.as_deref(),
        Some(pairwise_case.id.as_str())
    );
}

#[tokio::test]
async fn pairwise_compare_flags_regression_without_emitting_production_alert_events() {
    let dir = tempdir().unwrap();
    let claude_script = write_executable_script(
        dir.path(),
        "claude-compare.sh",
        r#"#!/bin/sh
cat <<'EOF'
- Review the task objective and the relevant context files.
- Produce a rollout checklist and the operator handoff notes.
Risk: local executor drift may still require human review.
Assumption: the task remains proposal-only.
Test: verify the desired outputs appear in the plan.
Confidence: high confidence with the rollout checklist included
EOF
"#,
    )
    .await;
    let codex_script = write_executable_script(
        dir.path(),
        "codex-compare.sh",
        r#"#!/bin/sh
cat <<'EOF'
- Sketch a rough outline.
Risk: the proposal may omit the requested outputs.
Confidence: low confidence
EOF
"#,
    )
    .await;
    let manifest = local_task_planner_manifest(
        &claude_script.display().to_string(),
        &codex_script.display().to_string(),
        "ws://127.0.0.1:9988/gateway",
    );
    let supervisor = build_supervisor_with_task_planner_manifest(dir.path(), manifest, None)
        .await
        .unwrap();

    supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Plan a regression-sensitive executor comparison for the swarm",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let alert_count_before = supervisor.list_alerts().await.unwrap().alerts.len();
    let started = supervisor
        .start_pairwise_evaluation_run(StartPairwiseEvaluationRunRequest {
            dataset: "task_plan_dataset".to_string(),
            left_executor: "local_harness.claude_code".to_string(),
            right_executor: "local_harness.codex".to_string(),
            profile: None,
        })
        .await
        .unwrap();
    let pairwise = supervisor
        .get_pairwise_evaluation_run(&started.run.id)
        .await
        .unwrap()
        .expect("pairwise detail");
    assert_eq!(pairwise.run.status, PairwiseExperimentRunStatus::Completed);
    assert_eq!(
        pairwise.run.left_wins, pairwise.run.total_cases,
        "pairwise detail: {pairwise:?}",
    );
    assert!(
        pairwise
            .run
            .triggered_alert_rules
            .iter()
            .any(|rule| rule == "comparison_regression"),
        "pairwise run: {:?}",
        pairwise.run
    );
    assert!(pairwise
        .cases
        .iter()
        .all(|case| case.outcome == PairwiseOutcome::LeftWins));

    let alert_count_after = supervisor.list_alerts().await.unwrap().alerts.len();
    assert_eq!(alert_count_before, alert_count_after);
}

#[tokio::test]
async fn unresolved_evaluation_profile_creates_alert_that_can_be_acknowledged() {
    let dir = tempdir().unwrap();
    tokio::fs::create_dir_all(dir.path().join("src"))
        .await
        .unwrap();
    tokio::fs::write(
        dir.path().join("src/lib.rs"),
        "pub fn value() -> u32 { 42 }\n",
    )
    .await
    .unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "Review this repo change".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "changed_files".to_string(),
                    serde_json::json!(["src/lib.rs"]),
                ),
            ]),
            contract_overrides: Some(ExecutionContractPatch {
                quality: crawfish_core::QualityPolicyPatch {
                    evaluation_profile: Some(Some("missing_profile".to_string())),
                    ..Default::default()
                },
                ..Default::default()
            }),
            execution_strategy: None,
            schedule: None,
            counterparty_refs: Vec::new(),
            data_boundary: None,
            workspace_write: false,
            secret_access: false,
            mutating: false,
        })
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let alerts = supervisor.list_alerts().await.unwrap();
    let alert = alerts
        .alerts
        .iter()
        .find(|alert| alert.action_id == submitted.action_id)
        .expect("alert event");
    assert!(matches!(alert.severity.as_str(), "warning" | "critical"));
    assert!(alert.summary.contains("frontier") || alert.summary.contains("evaluation"));

    let ack = supervisor
        .acknowledge_alert(
            &alert.id,
            AcknowledgeAlertRequest {
                actor: "operator-1".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(ack.alert.id, alert.id);
    assert_eq!(ack.alert.acknowledged_by.as_deref(), Some("operator-1"));
    assert!(ack.alert.acknowledged_at.is_some());
}

#[tokio::test]
async fn unsupported_evaluation_hook_creates_frontier_gap_but_preserves_terminal_result() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "Review this repo change".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "changed_files".to_string(),
                    serde_json::json!(["src/lib.rs"]),
                ),
            ]),
            contract_overrides: Some(ExecutionContractPatch {
                quality: crawfish_core::QualityPolicyPatch {
                    evaluation_hook: Some(Some("rubric_scorecard".to_string())),
                    ..Default::default()
                },
                ..Default::default()
            }),
            execution_strategy: None,
            schedule: None,
            counterparty_refs: Vec::new(),
            data_boundary: None,
            workspace_write: false,
            secret_access: false,
            mutating: false,
        })
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(detail.terminal_code.as_deref(), None);
    assert!(detail
        .policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "unsupported_evaluation_hook"));
    assert!(detail
        .policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_enforcement_gap"));
    assert!(detail.latest_evaluation.is_some());
}
