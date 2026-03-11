use super::support::*;
use super::*;

#[tokio::test]
async fn task_plan_routes_to_openclaw_and_streams_events() {
    let dir = tempdir().unwrap();
    let gateway_url = spawn_mock_openclaw_gateway().await;
    std::env::set_var("OPENCLAW_GATEWAY_TOKEN", "test-token");
    let supervisor = build_supervisor_with_openclaw_gateway(dir.path(), &gateway_url)
        .await
        .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "task.plan".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "plan a task".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "objective".to_string(),
                    serde_json::json!("Add validation checks around the repo indexing path"),
                ),
                (
                    "files_of_interest".to_string(),
                    serde_json::json!(["src/lib.rs"]),
                ),
                (
                    "desired_outputs".to_string(),
                    serde_json::json!(["rollout checklist"]),
                ),
            ]),
            contract_overrides: None,
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
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("openclaw.task-planner")
    );
    assert_eq!(
        detail.interaction_model,
        Some(crawfish_types::InteractionModel::RemoteHarness)
    );
    assert_eq!(
        detail.strategy_mode,
        Some(ExecutionStrategyMode::VerifyLoop)
    );
    assert_eq!(detail.strategy_iteration, Some(2));
    assert_eq!(
        detail
            .verification_summary
            .as_ref()
            .map(|summary| summary.status.clone()),
        Some(VerificationStatus::Passed)
    );
    assert!(detail
        .external_refs
        .iter()
        .any(|reference| reference.kind == "openclaw.run_id" && reference.value == "run-1"));
    assert!(detail
        .external_refs
        .iter()
        .any(|reference| reference.kind == "openclaw.run_id" && reference.value == "run-2"));
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verify_loop_iteration_started"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verify_loop_iteration_completed"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verification_failed"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verification_passed"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "openclaw_run_started"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "openclaw_assistant_event"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "openclaw_run_completed"));
}

#[tokio::test]
async fn task_plan_prefers_local_claude_harness_before_openclaw() {
    let dir = tempdir().unwrap();
    let gateway_url = spawn_mock_openclaw_gateway().await;
    let claude_script = write_executable_script(
        dir.path(),
        "claude-plan.sh",
        r#"#!/bin/sh
PROMPT="$1"
if printf '%s' "$PROMPT" | grep -q "Verification feedback"; then
  cat <<'EOF'
- Review the objective against the current context files.
- Produce the rollout checklist and operator handoff.
Risk: Verification coverage may still miss an environment-specific edge case.
Assumption: The task planner can prepare a rollout checklist from the local workspace.
Test: Validate the rollout checklist against the desired outputs.
Confidence: high confidence after verification feedback
EOF
else
  cat <<'EOF'
- Draft an initial task outline.
Risk: Initial pass may omit desired outputs.
Confidence: low confidence on the first pass
EOF
fi
"#,
    )
    .await;
    std::env::set_var("OPENCLAW_GATEWAY_TOKEN", "test-token");
    let manifest = local_task_planner_manifest(
        &claude_script.display().to_string(),
        "__missing_codex__",
        &gateway_url,
    );
    let supervisor =
        build_supervisor_with_task_planner_manifest(dir.path(), manifest, Some(&gateway_url))
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "task.plan".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "plan a task".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "objective".to_string(),
                    serde_json::json!("Prepare a rollout checklist for the task planner"),
                ),
                (
                    "desired_outputs".to_string(),
                    serde_json::json!(["rollout checklist"]),
                ),
            ]),
            contract_overrides: None,
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
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("local_harness.claude_code")
    );
    assert_eq!(
        detail.interaction_model,
        Some(crawfish_types::InteractionModel::RemoteHarness)
    );
    assert_eq!(detail.strategy_iteration, Some(2));
    assert!(detail
        .external_refs
        .iter()
        .any(|reference| reference.kind == "local_harness.harness"
            && reference.value == "claude_code"));
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "local_harness_process_started"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verification_failed"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verification_passed"));
    assert!(!events
        .events
        .iter()
        .any(|event| event.event_type == "openclaw_run_started"));
}

#[tokio::test]
async fn task_plan_falls_back_from_missing_claude_to_local_codex() {
    let dir = tempdir().unwrap();
    let gateway_url = spawn_mock_openclaw_gateway().await;
    let codex_script = write_executable_script(
        dir.path(),
        "codex-plan.sh",
        r#"#!/bin/sh
cat <<'EOF'
- Review the objective and gather relevant context.
- Produce the requested rollout checklist and summary.
Risk: The proposal may still need operator review before follow-on work.
Assumption: The current workspace is representative of the intended task.
Test: Validate the rollout checklist against the desired outputs.
Confidence: medium confidence after local codex planning
EOF
"#,
    )
    .await;
    std::env::set_var("OPENCLAW_GATEWAY_TOKEN", "test-token");
    let manifest = local_task_planner_manifest(
        "__missing_claude__",
        &codex_script.display().to_string(),
        &gateway_url,
    );
    let supervisor =
        build_supervisor_with_task_planner_manifest(dir.path(), manifest, Some(&gateway_url))
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "task.plan".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "plan a task".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "objective".to_string(),
                    serde_json::json!("Produce a rollout checklist for the local harness path"),
                ),
                (
                    "desired_outputs".to_string(),
                    serde_json::json!(["rollout checklist"]),
                ),
            ]),
            contract_overrides: None,
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
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("local_harness.codex")
    );
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events.events.iter().any(|event| {
        event.event_type == "route_degraded"
            && event.payload.get("code").and_then(Value::as_str)
                == Some("local_harness_missing_binary")
    }));
}

#[tokio::test]
async fn task_plan_falls_back_to_openclaw_after_local_harness_failures() {
    let dir = tempdir().unwrap();
    let gateway_url = spawn_mock_openclaw_gateway().await;
    std::env::set_var("OPENCLAW_GATEWAY_TOKEN", "test-token");
    let manifest =
        local_task_planner_manifest("__missing_claude__", "__missing_codex__", &gateway_url);
    let supervisor =
        build_supervisor_with_task_planner_manifest(dir.path(), manifest, Some(&gateway_url))
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "task.plan".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "plan a task".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "objective".to_string(),
                    serde_json::json!("Add validation checks around the repo indexing path"),
                ),
                (
                    "desired_outputs".to_string(),
                    serde_json::json!(["rollout checklist"]),
                ),
            ]),
            contract_overrides: None,
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
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("openclaw.task-planner")
    );
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(
        events
            .events
            .iter()
            .filter(|event| {
                event.event_type == "route_degraded"
                    && event.payload.get("code").and_then(Value::as_str)
                        == Some("local_harness_missing_binary")
            })
            .count()
            >= 2
    );
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "openclaw_run_started"));
}

#[tokio::test]
async fn task_plan_falls_back_to_deterministic_when_gateway_is_unavailable() {
    let dir = tempdir().unwrap();
    std::env::set_var("OPENCLAW_GATEWAY_TOKEN", "test-token");
    let supervisor =
        build_supervisor_with_openclaw_gateway(dir.path(), "ws://127.0.0.1:9/unavailable")
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "task.plan".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "plan a task".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                (
                    "objective".to_string(),
                    serde_json::json!("Plan a safe fallback-only task flow"),
                ),
                (
                    "desired_outputs".to_string(),
                    serde_json::json!(["fallback checklist"]),
                ),
            ]),
            contract_overrides: None,
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
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("deterministic.task_plan")
    );
    assert_eq!(
        detail.strategy_mode,
        Some(ExecutionStrategyMode::VerifyLoop)
    );
    assert_eq!(detail.strategy_iteration, Some(1));
    assert_eq!(
        detail
            .verification_summary
            .as_ref()
            .map(|summary| summary.status.clone()),
        Some(VerificationStatus::Passed)
    );
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "route_degraded"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "continuity_selected"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verification_passed"));
}

#[tokio::test]
async fn ci_triage_completes_with_direct_logs() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "ci_triage".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "ci.triage".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "triage CI logs".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([(
                "log_text".to_string(),
                serde_json::json!("error: test failed, to rerun pass `cargo test`"),
            )]),
            contract_overrides: None,
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
    let artifact = tokio::fs::read_to_string(&detail.artifact_refs[0].path)
        .await
        .unwrap();
    let triage: CiTriageArtifact = serde_json::from_str(&artifact).unwrap();
    assert_eq!(triage.family, crawfish_types::CiFailureFamily::Test);
}

#[tokio::test]
async fn incident_enrich_completes_with_local_inputs() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();
    let manifest_path = dir.path().join("services.toml");
    tokio::fs::write(
        &manifest_path,
        r#"[services.api]
depends_on = ["db"]

[services.web]
depends_on = ["api"]

[services.db]
depends_on = []
"#,
    )
    .await
    .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "incident_enricher".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "incident.enrich".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "enrich incident".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                ("service_name".to_string(), serde_json::json!("api")),
                (
                    "log_text".to_string(),
                    serde_json::json!(
                        "ERROR timeout contacting db\n503 service unavailable from api\n"
                    ),
                ),
                (
                    "service_manifest_file".to_string(),
                    serde_json::json!(manifest_path.display().to_string()),
                ),
            ]),
            contract_overrides: None,
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
    let artifact = tokio::fs::read_to_string(&detail.artifact_refs[0].path)
        .await
        .unwrap();
    let enrichment: crawfish_types::IncidentEnrichmentArtifact =
        serde_json::from_str(&artifact).unwrap();
    assert!(enrichment
        .probable_blast_radius
        .contains(&"api".to_string()));
    assert!(enrichment
        .probable_blast_radius
        .contains(&"web".to_string()));
}

#[tokio::test]
async fn ci_triage_can_fetch_logs_via_mcp() {
    let dir = tempdir().unwrap();
    let mcp_url = spawn_runtime_mcp_server(
        "error: test failed, to rerun pass `cargo test`\nfailures:\n    tests::smoke\n",
    )
    .await;
    let supervisor = build_supervisor_with_mcp(dir.path(), &mcp_url)
        .await
        .unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "ci_triage".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "ci.triage".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "triage remote CI logs".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([(
                "mcp_resource_ref".to_string(),
                serde_json::json!("ci://runs/1/logs"),
            )]),
            contract_overrides: None,
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
    assert!(detail
        .external_refs
        .iter()
        .any(|external| external.kind == "mcp_server"));
    assert!(detail
        .external_refs
        .iter()
        .any(|external| external.kind == "mcp_resource"));
    assert!(detail.action.outputs.metadata.contains_key("mcp_result"));
    let artifact = tokio::fs::read_to_string(&detail.artifact_refs[0].path)
        .await
        .unwrap();
    let triage: CiTriageArtifact = serde_json::from_str(&artifact).unwrap();
    assert_eq!(triage.family, crawfish_types::CiFailureFamily::Test);
}

#[tokio::test]
async fn running_action_is_requeued_after_restart() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let mut action = Action {
        id: "running-action".to_string(),
        target_agent_id: "repo_indexer".to_string(),
        requester: RequesterRef {
            kind: RequesterKind::User,
            id: "operator".to_string(),
        },
        initiator_owner: crawfish_types::OwnerRef {
            kind: crawfish_types::OwnerKind::Human,
            id: "local-dev".to_string(),
            display_name: None,
        },
        counterparty_refs: Vec::new(),
        goal: crawfish_types::GoalSpec {
            summary: "index repository".to_string(),
            details: None,
        },
        capability: "repo.index".to_string(),
        inputs: std::collections::BTreeMap::from([(
            "workspace_root".to_string(),
            serde_json::json!(dir.path().display().to_string()),
        )]),
        contract: supervisor.config().contracts.org_defaults.clone(),
        execution_strategy: None,
        grant_refs: Vec::new(),
        lease_ref: None,
        encounter_ref: None,
        audit_receipt_ref: None,
        data_boundary: "owner_local".to_string(),
        schedule: Default::default(),
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
    let checkpoint =
        build_checkpoint(&action, "deterministic.repo_index", "scanning", Vec::new()).unwrap();
    let checkpoint_ref = checkpoint_ref_for_executor(&checkpoint.executor_kind);
    supervisor
        .store()
        .put_checkpoint(
            &action.id,
            &checkpoint_ref,
            &serde_json::to_vec_pretty(&checkpoint).unwrap(),
        )
        .await
        .unwrap();
    action.checkpoint_ref = Some(checkpoint_ref);
    supervisor.store().upsert_action(&action).await.unwrap();

    supervisor.run_once().await.unwrap();

    let detail = supervisor
        .inspect_action("running-action")
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(detail.recovery_stage.as_deref(), Some("completed"));
    assert!(detail.action.checkpoint_ref.is_some());
}

#[tokio::test]
async fn running_openclaw_action_records_abandoned_run_on_restart() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let action = Action {
        id: "openclaw-running-action".to_string(),
        target_agent_id: "task_planner".to_string(),
        requester: RequesterRef {
            kind: RequesterKind::User,
            id: "operator".to_string(),
        },
        initiator_owner: crawfish_types::OwnerRef {
            kind: crawfish_types::OwnerKind::Human,
            id: "local-dev".to_string(),
            display_name: None,
        },
        counterparty_refs: Vec::new(),
        goal: crawfish_types::GoalSpec {
            summary: "plan task".to_string(),
            details: None,
        },
        capability: "task.plan".to_string(),
        inputs: std::collections::BTreeMap::from([
            (
                "workspace_root".to_string(),
                serde_json::json!(dir.path().display().to_string()),
            ),
            ("objective".to_string(), serde_json::json!("Plan a task")),
        ]),
        contract: supervisor.config().contracts.org_defaults.clone(),
        execution_strategy: None,
        grant_refs: Vec::new(),
        lease_ref: None,
        encounter_ref: None,
        audit_receipt_ref: None,
        data_boundary: "owner_local".to_string(),
        schedule: Default::default(),
        phase: ActionPhase::Running,
        created_at: now_timestamp(),
        started_at: Some(now_timestamp()),
        finished_at: None,
        checkpoint_ref: None,
        continuity_mode: None,
        degradation_profile: None,
        failure_reason: None,
        failure_code: None,
        selected_executor: Some("openclaw.task-planner".to_string()),
        recovery_stage: None,
        lock_detail: None,
        external_refs: vec![ExternalRef {
            kind: "openclaw.run_id".to_string(),
            value: "run-xyz".to_string(),
            endpoint: None,
        }],
        outputs: ActionOutputs::default(),
    };
    supervisor.store().upsert_action(&action).await.unwrap();

    let restarted = Supervisor::from_config_path(&dir.path().join("Crawfish.toml"))
        .await
        .unwrap();
    restarted.run_once().await.unwrap();

    let events = restarted
        .list_action_events("openclaw-running-action")
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "openclaw_run_abandoned"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "recovered"));
}

#[tokio::test]
async fn verify_loop_action_recovers_iteration_lineage_after_restart() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let mut action = Action {
        id: "verify-loop-running-action".to_string(),
        target_agent_id: "task_planner".to_string(),
        requester: RequesterRef {
            kind: RequesterKind::User,
            id: "operator".to_string(),
        },
        initiator_owner: crawfish_types::OwnerRef {
            kind: crawfish_types::OwnerKind::Human,
            id: "local-dev".to_string(),
            display_name: None,
        },
        counterparty_refs: Vec::new(),
        goal: crawfish_types::GoalSpec {
            summary: "plan task".to_string(),
            details: None,
        },
        capability: "task.plan".to_string(),
        inputs: std::collections::BTreeMap::from([
            (
                "workspace_root".to_string(),
                serde_json::json!(dir.path().display().to_string()),
            ),
            (
                "objective".to_string(),
                serde_json::json!("Plan the repo indexing rollout"),
            ),
            (
                "desired_outputs".to_string(),
                serde_json::json!(["rollout checklist"]),
            ),
        ]),
        contract: supervisor.config().contracts.org_defaults.clone(),
        execution_strategy: Some(crawfish_types::ExecutionStrategy {
            mode: ExecutionStrategyMode::VerifyLoop,
            verification_spec: Some(crawfish_types::VerificationSpec {
                checks: Vec::new(),
                require_all: true,
                on_failure: crawfish_types::VerifyLoopFailureMode::RetryWithFeedback,
            }),
            stop_budget: Some(crawfish_types::StopBudget {
                max_iterations: 3,
                max_cost_usd: None,
                max_elapsed_ms: None,
            }),
            feedback_policy: FeedbackPolicy::InjectReason,
        }),
        grant_refs: Vec::new(),
        lease_ref: None,
        encounter_ref: None,
        audit_receipt_ref: None,
        data_boundary: "owner_local".to_string(),
        schedule: Default::default(),
        phase: ActionPhase::Running,
        created_at: now_timestamp(),
        started_at: Some(now_timestamp()),
        finished_at: None,
        checkpoint_ref: None,
        continuity_mode: None,
        degradation_profile: None,
        failure_reason: None,
        failure_code: None,
        selected_executor: Some("deterministic.task_plan".to_string()),
        recovery_stage: None,
        lock_detail: None,
        external_refs: Vec::new(),
        outputs: ActionOutputs::default(),
    };
    let mut checkpoint = build_checkpoint(
        &action,
        "deterministic.task_plan",
        "verification_failed",
        Vec::new(),
    )
    .unwrap();
    checkpoint.strategy_state = Some(StrategyCheckpointState {
        mode: ExecutionStrategyMode::VerifyLoop,
        iteration: 1,
        verification_feedback: Some(
            "Address the following verification gaps: task plan must cover rollout checklist"
                .to_string(),
        ),
        previous_artifact_refs: Vec::new(),
        verification_summary: Some(VerificationSummary {
            status: VerificationStatus::Failed,
            iterations_completed: 1,
            last_feedback: Some(
                "Address the following verification gaps: task plan must cover rollout checklist"
                    .to_string(),
            ),
            last_failure_code: Some(failure_code_verification_failed().to_string()),
        }),
    });
    let checkpoint_ref = checkpoint_ref_for_executor(&checkpoint.executor_kind);
    supervisor
        .store()
        .put_checkpoint(
            &action.id,
            &checkpoint_ref,
            &serde_json::to_vec_pretty(&checkpoint).unwrap(),
        )
        .await
        .unwrap();
    action.checkpoint_ref = Some(checkpoint_ref);
    supervisor.store().upsert_action(&action).await.unwrap();

    let restarted = Supervisor::from_config_path(&dir.path().join("Crawfish.toml"))
        .await
        .unwrap();
    restarted.run_once().await.unwrap();

    let detail = restarted
        .inspect_action("verify-loop-running-action")
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.strategy_mode,
        Some(ExecutionStrategyMode::VerifyLoop)
    );
    assert_eq!(detail.strategy_iteration, Some(2));
    assert_eq!(
        detail
            .verification_summary
            .as_ref()
            .map(|summary| summary.status.clone()),
        Some(VerificationStatus::Passed)
    );

    let events = restarted
        .list_action_events("verify-loop-running-action")
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "recovered"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verify_loop_iteration_started"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "verification_passed"));
}

#[tokio::test]
async fn action_events_surface_operator_timeline() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "timeline.txt",
                    "op": "create",
                    "contents": "timeline\n"
                }
            ]),
            None,
        ))
        .await
        .unwrap();

    supervisor
        .approve_action(
            &submitted.action_id,
            ApproveActionRequest {
                approver_ref: "local-dev".to_string(),
                note: None,
            },
        )
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    let event_types = events
        .events
        .iter()
        .map(|event| event.event_type.as_str())
        .collect::<Vec<_>>();
    assert!(event_types.contains(&"awaiting_approval"));
    assert!(event_types.contains(&"approved"));
    assert!(event_types.contains(&"running"));
    assert!(event_types.contains(&"completed"));
}
