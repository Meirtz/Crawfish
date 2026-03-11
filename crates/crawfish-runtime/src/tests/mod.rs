use super::*;
use axum::{
    extract::{Query, State as AxumState},
    response::sse::{Event, Sse},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use crawfish_core::CheckpointStore;
use crawfish_types::{CiTriageArtifact, RequesterKind, RequesterRef};
use futures_util::{stream, SinkExt, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::Client;
use hyperlocal::UnixClientExt;
use std::collections::HashMap;
use std::convert::Infallible;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request as WsRequest, Response as WsResponse},
        Message as WsMessage,
    },
};

mod support;
use support::*;

#[tokio::test]
async fn submit_action_completes_deterministically() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "review pull request".to_string(),
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
        .artifact_refs
        .iter()
        .any(|artifact| artifact.kind == "repo_index"));
    assert!(detail
        .artifact_refs
        .iter()
        .any(|artifact| artifact.kind == "review_summary"));
    assert!(detail.action.checkpoint_ref.is_some());
}

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
async fn task_plan_routes_to_a2a_and_surfaces_treaty_lineage() {
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
            "Plan a treaty-governed remote task rollout",
        ))
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
        Some("a2a.remote-task-planner")
    );
    assert_eq!(
        detail.interaction_model,
        Some(crawfish_types::InteractionModel::RemoteAgent)
    );
    assert_eq!(detail.remote_task_ref.as_deref(), Some("remote-task-1"));
    assert_eq!(
        detail
            .remote_principal
            .as_ref()
            .map(|principal| principal.id.as_str()),
        Some("remote-task-planner")
    );
    assert_eq!(
        detail
            .treaty_summary
            .as_ref()
            .map(|treaty| treaty.id.as_str()),
        Some("remote_task_planning")
    );
    assert!(detail.delegation_receipt_ref.is_some());
    assert!(detail
        .external_refs
        .iter()
        .any(|reference| reference.kind == "a2a.task_id" && reference.value == "remote-task-1"));
    let trace = supervisor
        .store()
        .get_trace_bundle(&submitted.action_id)
        .await
        .unwrap()
        .expect("trace bundle");
    assert_eq!(
        trace.interaction_model,
        Some(crawfish_types::InteractionModel::RemoteAgent)
    );
    assert_eq!(
        trace.treaty_pack_id.as_deref(),
        Some("remote_task_planning")
    );
    assert_eq!(trace.remote_task_ref.as_deref(), Some("remote-task-1"));
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "a2a_run_started"));
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "a2a_run_completed"));
}

#[tokio::test]
async fn task_plan_without_treaty_is_denied_before_remote_dispatch() {
    let dir = tempdir().unwrap();
    let manifest = local_task_planner_manifest(
        "__missing_claude__",
        "__missing_codex__",
        "ws://127.0.0.1:9/unavailable",
    )
    .replace(
        "preferred_harnesses = [\"claude_code\", \"codex\", \"a2a\", \"openclaw\"]",
        "preferred_harnesses = [\"a2a\"]",
    )
    .replace(
        "fallback_chain = [\"deterministic\"]",
        "fallback_chain = []",
    )
    .replace(
        "treaty_pack = \"remote_task_planning\"",
        "treaty_pack = \"missing_treaty\"",
    );
    let config =
        include_str!("../../../../examples/experimental/remote-swarm/Crawfish.toml").to_string();
    let supervisor =
        build_supervisor_with_task_planner_manifest_and_config(dir.path(), manifest, config, None)
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Attempt remote delegation without a treaty",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Blocked);
    assert!(detail.delegation_receipt_ref.is_none());
    assert!(detail.remote_task_ref.is_none());
    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events.events.iter().any(|event| {
        event.event_type == "route_degraded"
            && event.payload.get("code").and_then(Value::as_str) == Some("treaty_denied")
    }));
    assert!(detail.policy_incidents.iter().any(|incident| {
        incident.reason_code == "treaty_denied"
            || incident.reason_code == "frontier_enforcement_gap"
    }));
}

#[tokio::test]
async fn task_plan_a2a_input_required_maps_to_blocked() {
    let dir = tempdir().unwrap();
    let a2a_url = spawn_runtime_a2a_server(RuntimeA2aMode::InputRequired).await;
    std::env::set_var("A2A_REMOTE_TOKEN", "remote-token");
    let manifest = local_task_planner_manifest(
        "__missing_claude__",
        "__missing_codex__",
        "ws://127.0.0.1:9/unavailable",
    )
    .replace(
        "preferred_harnesses = [\"claude_code\", \"codex\", \"a2a\", \"openclaw\"]",
        "preferred_harnesses = [\"a2a\", \"openclaw\"]",
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
            "Request input-required remote planning",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Blocked);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("a2a.remote-task-planner")
    );
    assert_eq!(detail.remote_task_ref.as_deref(), Some("remote-task-1"));
    assert_eq!(
        detail.action.failure_code.as_deref(),
        Some("a2a_input_required")
    );
}

#[tokio::test]
async fn task_plan_a2a_auth_required_maps_to_awaiting_approval() {
    let dir = tempdir().unwrap();
    let a2a_url = spawn_runtime_a2a_server(RuntimeA2aMode::AuthRequired).await;
    std::env::set_var("A2A_REMOTE_TOKEN", "remote-token");
    let manifest = local_task_planner_manifest(
        "__missing_claude__",
        "__missing_codex__",
        "ws://127.0.0.1:9/unavailable",
    )
    .replace(
        "preferred_harnesses = [\"claude_code\", \"codex\", \"a2a\", \"openclaw\"]",
        "preferred_harnesses = [\"a2a\", \"openclaw\"]",
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
            "Request auth-required remote planning",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::AwaitingApproval);
    assert_eq!(
        detail.selected_executor.as_deref(),
        Some("a2a.remote-task-planner")
    );
    assert_eq!(detail.remote_task_ref.as_deref(), Some("remote-task-1"));
    assert_eq!(
        detail.action.failure_code.as_deref(),
        Some("a2a_auth_required")
    );
}

#[tokio::test]
async fn task_plan_a2a_scope_violation_rejects_remote_result() {
    let dir = tempdir().unwrap();
    let a2a_url = spawn_runtime_a2a_server(RuntimeA2aMode::StreamingCompleted).await;
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
        .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url)
        .replace(
            "allowed_artifact_classes = [\"task_plan.json\", \"task_plan.md\"]",
            "allowed_artifact_classes = [\"task_plan.json\"]",
        );
    let supervisor =
        build_supervisor_with_task_planner_manifest_and_config(dir.path(), manifest, config, None)
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Trigger treaty scope violation for remote artifact classes",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Failed);
    assert_eq!(
        detail.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::Rejected)
    );
    assert!(detail
        .treaty_violations
        .iter()
        .any(|violation| violation.code == "treaty_scope_violation"));
    assert!(detail
        .policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "treaty_scope_violation"));
}

#[tokio::test]
async fn task_plan_a2a_missing_result_evidence_requires_review() {
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
            "Trigger treaty evidence gap for remote planning",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Blocked);
    assert_eq!(
        detail.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::ReviewRequired)
    );
    assert_eq!(detail.delegation_depth, Some(1));
    assert!(detail
        .treaty_violations
        .iter()
        .any(|violation| violation.code == "frontier_enforcement_gap"));
    assert!(detail
        .policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_enforcement_gap"));
    let trace = supervisor
        .store()
        .get_trace_bundle(&submitted.action_id)
        .await
        .unwrap()
        .expect("trace bundle");
    assert_eq!(
        trace.remote_outcome_disposition,
        Some(crawfish_types::RemoteOutcomeDisposition::ReviewRequired)
    );
    assert!(trace
        .treaty_violations
        .iter()
        .any(|violation| violation.code == "frontier_enforcement_gap"));
}

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
async fn task_plan_remote_review_required_creates_remote_evidence_bundle_and_queue_item() {
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
            "Trigger remote result review with missing evidence",
        ))
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let remote_evidence = supervisor
        .get_action_remote_evidence(&submitted.action_id)
        .await
        .unwrap()
        .expect("remote evidence response");
    assert_eq!(remote_evidence.bundles.len(), 1);
    assert_eq!(
        remote_evidence.bundles[0].remote_review_disposition,
        Some(RemoteReviewDisposition::Pending)
    );
    assert_eq!(
        remote_evidence.bundles[0].remote_review_reason,
        Some(RemoteReviewReason::EvidenceGap)
    );

    let review_queue = supervisor.list_review_queue().await.unwrap();
    let review_item = review_queue
        .items
        .iter()
        .find(|item| {
            item.action_id == submitted.action_id
                && item.kind == ReviewQueueKind::RemoteResultReview
        })
        .expect("remote review queue item");
    assert_eq!(
        review_item.remote_evidence_ref.as_deref(),
        Some(remote_evidence.bundles[0].id.as_str())
    );

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        detail.remote_evidence_ref.as_deref(),
        Some(remote_evidence.bundles[0].id.as_str())
    );
    assert_eq!(
        detail.remote_review_disposition,
        Some(RemoteReviewDisposition::Pending)
    );
    assert_eq!(
        detail.pending_remote_review_ref,
        Some(review_item.id.clone())
    );
}

#[tokio::test]
async fn accepting_remote_result_review_completes_blocked_action() {
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
            "Trigger remote result review and then accept it",
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
        .find(|item| {
            item.action_id == submitted.action_id
                && item.kind == ReviewQueueKind::RemoteResultReview
                && item.status == ReviewQueueStatus::Open
        })
        .expect("remote review item");

    supervisor
        .resolve_review_queue_item(
            &review_item.id,
            ResolveReviewQueueItemRequest {
                resolver_ref: "operator".to_string(),
                resolution: "accept_result".to_string(),
                note: Some("evidence reviewed manually".to_string()),
            },
        )
        .await
        .unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert_eq!(
        detail.remote_review_disposition,
        Some(RemoteReviewDisposition::Accepted)
    );
    assert!(detail.pending_remote_review_ref.is_none());
}

#[tokio::test]
async fn remote_result_review_needs_followup_keeps_action_blocked() {
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
            "Trigger remote result review and request follow-up",
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
        .find(|item| {
            item.action_id == submitted.action_id
                && item.kind == ReviewQueueKind::RemoteResultReview
                && item.status == ReviewQueueStatus::Open
        })
        .expect("remote review item");

    supervisor
        .resolve_review_queue_item(
            &review_item.id,
            ResolveReviewQueueItemRequest {
                resolver_ref: "operator".to_string(),
                resolution: "needs_followup".to_string(),
                note: Some("remote result still needs admissibility evidence".to_string()),
            },
        )
        .await
        .unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.phase, ActionPhase::Blocked);
    assert_eq!(
        detail.remote_review_disposition,
        Some(RemoteReviewDisposition::NeedsFollowup)
    );
    assert!(detail.pending_remote_review_ref.is_none());
    let followups = supervisor
        .get_action_remote_followups(&submitted.action_id)
        .await
        .unwrap()
        .expect("remote followups");
    assert_eq!(followups.followups.len(), 1);
    assert_eq!(followups.followups[0].status, RemoteFollowupStatus::Open);
    assert_eq!(
        followups.followups[0].operator_note.as_deref(),
        Some("remote result still needs admissibility evidence")
    );
    let review_queue = supervisor.list_review_queue().await.unwrap();
    assert!(!review_queue.items.iter().any(|item| {
        item.action_id == submitted.action_id
            && item.kind == ReviewQueueKind::RemoteResultReview
            && item.status == ReviewQueueStatus::Open
    }));
}

#[tokio::test]
async fn dispatching_remote_followup_creates_fresh_attempt_on_same_action() {
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
            "Trigger remote follow-up redispatch on the same action",
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
        .find(|item| {
            item.action_id == submitted.action_id
                && item.kind == ReviewQueueKind::RemoteResultReview
                && item.status == ReviewQueueStatus::Open
        })
        .expect("remote review item");
    supervisor
        .resolve_review_queue_item(
            &review_item.id,
            ResolveReviewQueueItemRequest {
                resolver_ref: "operator".to_string(),
                resolution: "needs_followup".to_string(),
                note: Some("request clearer remote evidence".to_string()),
            },
        )
        .await
        .unwrap();

    let followups = supervisor
        .get_action_remote_followups(&submitted.action_id)
        .await
        .unwrap()
        .expect("remote followups");
    assert_eq!(followups.attempts.len(), 1);
    let followup = followups
        .followups
        .iter()
        .find(|followup| followup.status == RemoteFollowupStatus::Open)
        .cloned()
        .expect("open followup request");

    supervisor
        .dispatch_remote_followup(
            &submitted.action_id,
            &followup.id,
            DispatchRemoteFollowupRequest {
                dispatcher_ref: "operator".to_string(),
                note: Some("retry with admissible evidence".to_string()),
            },
        )
        .await
        .unwrap();
    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(detail.action.id, submitted.action_id);
    assert_eq!(detail.remote_attempt_count, 2);
    assert!(detail.latest_remote_attempt_ref.is_some());

    let followups = supervisor
        .get_action_remote_followups(&submitted.action_id)
        .await
        .unwrap()
        .expect("remote followups");
    assert_eq!(followups.attempts.len(), 2);
    assert!(followups
        .attempts
        .iter()
        .any(|attempt| attempt.followup_request_ref.as_deref() == Some(followup.id.as_str())));
    assert!(
        followups
            .followups
            .iter()
            .any(|request| request.id == followup.id
                && request.status == RemoteFollowupStatus::Closed)
    );

    let events = supervisor
        .list_action_events(&submitted.action_id)
        .await
        .unwrap();
    assert!(events
        .events
        .iter()
        .any(|event| event.event_type == "remote_followup_dispatched"));
}

#[tokio::test]
async fn federation_pack_can_disable_followup_dispatch() {
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
        .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url)
        .replace("followup_allowed = true", "followup_allowed = false");
    let supervisor =
        build_supervisor_with_task_planner_manifest_and_config(dir.path(), manifest, config, None)
            .await
            .unwrap();

    let submitted = supervisor
        .submit_action(task_plan_request(
            dir.path(),
            "Trigger a remote follow-up and deny redispatch by pack policy",
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
        .find(|item| {
            item.action_id == submitted.action_id
                && item.kind == ReviewQueueKind::RemoteResultReview
                && item.status == ReviewQueueStatus::Open
        })
        .expect("remote review item");
    let error = supervisor
        .resolve_review_queue_item(
            &review_item.id,
            ResolveReviewQueueItemRequest {
                resolver_ref: "operator".to_string(),
                resolution: "needs_followup".to_string(),
                note: Some("should be denied by federation pack".to_string()),
            },
        )
        .await
        .expect_err("follow-up resolution should be denied");
    assert!(error
        .to_string()
        .contains("does not allow remote follow-up dispatch"));
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
async fn submit_action_rejects_invalid_inputs() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let error = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "local-dev".to_string(),
                display_name: None,
            },
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "review pull request".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([(
                "workspace_root".to_string(),
                serde_json::json!(dir.path().display().to_string()),
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
        .expect_err("request should be rejected");

    assert!(error
        .to_string()
        .starts_with("invalid action request: repo.review requires"));
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
async fn foreign_owner_mutation_is_denied_by_default() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let error = supervisor
        .submit_action(SubmitActionRequest {
            target_agent_id: "repo_reviewer".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: crawfish_types::OwnerRef {
                kind: crawfish_types::OwnerKind::Human,
                id: "foreign-owner".to_string(),
                display_name: None,
            },
            capability: "repo.review".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "attempt write".to_string(),
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
            contract_overrides: None,
            execution_strategy: None,
            schedule: None,
            counterparty_refs: vec![CounterpartyRef {
                agent_id: None,
                session_id: Some("cli".to_string()),
                owner: crawfish_types::OwnerRef {
                    kind: crawfish_types::OwnerKind::Human,
                    id: "foreign-owner".to_string(),
                    display_name: None,
                },
                trust_domain: TrustDomain::SameDeviceForeignOwner,
            }],
            data_boundary: None,
            workspace_write: true,
            secret_access: false,
            mutating: true,
        })
        .await
        .expect_err("foreign mutation should be denied");

    assert!(error.to_string().contains("denied"));
}

#[test]
fn intra_runtime_agent_handoff_derives_context_split_interaction_model() {
    let action = Action {
        id: "context-split".to_string(),
        target_agent_id: "task_planner".to_string(),
        requester: RequesterRef {
            kind: RequesterKind::Agent,
            id: "delegator-agent".to_string(),
        },
        initiator_owner: local_owner("local-dev"),
        counterparty_refs: Vec::new(),
        goal: crawfish_types::GoalSpec {
            summary: "delegate planning subtask".to_string(),
            details: None,
        },
        capability: "task.plan".to_string(),
        inputs: Metadata::default(),
        contract: crawfish_types::ExecutionContract::default(),
        execution_strategy: None,
        grant_refs: Vec::new(),
        lease_ref: None,
        encounter_ref: None,
        audit_receipt_ref: None,
        data_boundary: "owner_local".to_string(),
        schedule: crawfish_types::ScheduleSpec::default(),
        phase: ActionPhase::Accepted,
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
    };

    assert_eq!(
        interaction_model_for_action(&action, None),
        crawfish_types::InteractionModel::ContextSplit
    );
}

#[tokio::test]
async fn policy_validate_is_dry_run_only() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let before_encounters: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM encounters")
        .fetch_one(supervisor.store().pool())
        .await
        .unwrap();
    let before_receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_receipts")
        .fetch_one(supervisor.store().pool())
        .await
        .unwrap();

    let response = supervisor
        .validate_policy_request(PolicyValidationRequest {
            target_agent_id: "repo_reviewer".to_string(),
            caller: CounterpartyRef {
                agent_id: None,
                session_id: Some("cli".to_string()),
                owner: local_owner("foreign-user"),
                trust_domain: TrustDomain::SameDeviceForeignOwner,
            },
            capability: "repo.review".to_string(),
            workspace_write: true,
            secret_access: false,
            mutating: true,
        })
        .await
        .unwrap();

    assert_eq!(response.disposition, "deny");

    let after_encounters: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM encounters")
        .fetch_one(supervisor.store().pool())
        .await
        .unwrap();
    let after_receipts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_receipts")
        .fetch_one(supervisor.store().pool())
        .await
        .unwrap();

    assert_eq!(before_encounters, after_encounters);
    assert_eq!(before_receipts, after_receipts);
}

#[tokio::test]
async fn same_owner_read_only_action_issues_grant_and_lease() {
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
                summary: "review pull request".to_string(),
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

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Accepted);
    assert_eq!(detail.grant_details.len(), 1);
    assert!(detail.lease_detail.is_some());
    assert_eq!(
        detail.encounter.expect("encounter").state,
        EncounterState::Leased
    );
}

#[tokio::test]
async fn mutation_action_requires_approval_then_can_be_approved() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "approved.txt",
                    "op": "create",
                    "contents": "approved\n"
                }
            ]),
            None,
        ))
        .await
        .unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::AwaitingApproval);
    assert!(detail.grant_details.is_empty());
    assert!(detail.lease_detail.is_none());

    let approved = supervisor
        .approve_action(
            &submitted.action_id,
            ApproveActionRequest {
                approver_ref: "local-dev".to_string(),
                note: Some("approved for local mutation".to_string()),
            },
        )
        .await
        .unwrap();
    assert_eq!(approved.phase, "accepted");

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Accepted);
    assert_eq!(detail.grant_details.len(), 1);
    assert!(detail.lease_detail.is_some());
    assert_eq!(
        detail.latest_audit_receipt.expect("audit").outcome,
        AuditOutcome::Allowed
    );
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

#[tokio::test]
async fn rejecting_mutation_action_marks_it_failed() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "rejected.txt",
                    "op": "create",
                    "contents": "rejected\n"
                }
            ]),
            None,
        ))
        .await
        .unwrap();

    let rejected = supervisor
        .reject_action(
            &submitted.action_id,
            RejectActionRequest {
                approver_ref: "local-dev".to_string(),
                reason: "operator rejected".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(rejected.phase, "failed");

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Failed);
    assert_eq!(
        detail.latest_audit_receipt.expect("audit").outcome,
        AuditOutcome::Denied
    );
}

#[tokio::test]
async fn revoking_lease_fails_pending_mutation_action() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "revoked.txt",
                    "op": "create",
                    "contents": "revoked\n"
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
    let lease_id = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail")
        .lease_detail
        .expect("lease detail")
        .id;

    let response = supervisor
        .revoke_lease(
            &lease_id,
            RevokeLeaseRequest {
                revoker_ref: "local-dev".to_string(),
                reason: "operator revoked".to_string(),
            },
        )
        .await
        .unwrap();
    assert_eq!(response.status, "revoked");

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Failed);
    assert!(detail
        .action
        .failure_reason
        .expect("failure reason")
        .contains("lease revoked"));
    assert_eq!(
        detail.encounter.expect("encounter").state,
        EncounterState::Revoked
    );
}

#[tokio::test]
async fn awaiting_approval_action_expires_at_deadline() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "expired.txt",
                    "op": "create",
                    "contents": "expired\n"
                }
            ]),
            Some(0),
        ))
        .await
        .unwrap();

    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Expired);
    assert_eq!(
        detail.latest_audit_receipt.expect("audit").outcome,
        AuditOutcome::Expired
    );
}

#[tokio::test]
async fn workspace_mutation_executes_after_approval() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "applied.txt",
                    "op": "create",
                    "contents": "applied\n"
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

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert!(dir.path().join("applied.txt").exists());
    assert!(detail
        .artifact_refs
        .iter()
        .any(|artifact| artifact.kind == "workspace_apply_result"));
}

#[tokio::test]
async fn workspace_lock_conflict_blocks_second_mutation() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();
    supervisor
        .store()
        .upsert_action(&Action {
            id: "other-action".to_string(),
            target_agent_id: "workspace_editor".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::System,
                id: "test".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            counterparty_refs: Vec::new(),
            goal: crawfish_types::GoalSpec {
                summary: "hold workspace lock".to_string(),
                details: None,
            },
            capability: "workspace.patch.apply".to_string(),
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.path().display().to_string()),
                ),
                ("edits".to_string(), serde_json::json!([])),
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
            selected_executor: None,
            recovery_stage: None,
            lock_detail: None,
            external_refs: Vec::new(),
            outputs: ActionOutputs::default(),
        })
        .await
        .unwrap();
    let lock_path = supervisor.lock_file_path(&dir.path().display().to_string());
    if let Some(parent) = lock_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    tokio::fs::write(
        &lock_path,
        serde_json::to_vec_pretty(&WorkspaceLockRecord {
            workspace_root: dir.path().display().to_string(),
            owner_action_id: "other-action".to_string(),
            acquired_at: now_timestamp(),
        })
        .unwrap(),
    )
    .await
    .unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "blocked.txt",
                    "op": "create",
                    "contents": "blocked\n"
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

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Blocked);
    assert_eq!(detail.terminal_code.as_deref(), Some("lock_conflict"));
    assert_eq!(
        detail
            .lock_detail
            .as_ref()
            .and_then(|lock| lock.owner_action_id.as_deref()),
        Some("other-action")
    );
}

#[tokio::test]
async fn expired_lease_fails_mutation_before_commit() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "expired-lease.txt",
                    "op": "create",
                    "contents": "should not apply\n"
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

    let lease_id = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail")
        .lease_detail
        .expect("lease detail")
        .id;
    let mut lease = supervisor
        .store()
        .get_capability_lease(&lease_id)
        .await
        .unwrap()
        .expect("lease");
    lease.expires_at = "1".to_string();
    supervisor
        .store()
        .upsert_capability_lease(&lease)
        .await
        .unwrap();

    supervisor.process_action_queue_once().await.unwrap();

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Failed);
    assert_eq!(detail.terminal_code.as_deref(), Some("lease_expired"));
    assert!(!dir.path().join("expired-lease.txt").exists());
}

#[tokio::test]
async fn openclaw_inbound_submit_inspect_and_events_work_for_mapped_caller() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor_with_openclaw(dir.path()).await.unwrap();
    let (handle, socket_path) = spawn_api_server(Arc::clone(&supervisor)).await;

    let submit_request = OpenClawInboundActionRequest {
        caller: OpenClawCallerContext {
            workspace_root: Some(dir.path().display().to_string()),
            ..openclaw_caller("local_gateway")
        },
        target_agent_id: "repo_reviewer".to_string(),
        capability: "repo.review".to_string(),
        goal: crawfish_types::GoalSpec {
            summary: "review from openclaw".to_string(),
            details: None,
        },
        inputs: std::collections::BTreeMap::from([(
            "changed_files".to_string(),
            serde_json::json!(["src/lib.rs"]),
        )]),
        contract_overrides: None,
        execution_strategy: None,
        schedule: None,
        data_boundary: None,
        workspace_write: false,
        secret_access: false,
        mutating: false,
    };

    let (status, submitted) = post_uds_json(
        &socket_path,
        "/v1/inbound/openclaw/actions",
        &submit_request,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let action_id = submitted["action_id"].as_str().unwrap().to_string();

    let (status, inspected) = post_uds_json(
        &socket_path,
        &format!("/v1/inbound/openclaw/actions/{action_id}/inspect"),
        &OpenClawInspectionContext {
            caller: openclaw_caller("local_gateway"),
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(inspected["action"]["id"], action_id);
    assert_eq!(inspected["interaction_model"], "same_device_multi_owner");
    assert!(inspected["external_refs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|reference| reference["kind"] == "openclaw.session_id"));

    let (status, events) = post_uds_json(
        &socket_path,
        &format!("/v1/inbound/openclaw/actions/{action_id}/events"),
        &OpenClawInspectionContext {
            caller: openclaw_caller("local_gateway"),
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(events["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event_type"] == "openclaw_inbound"));

    let (status, agent_status) = post_uds_json(
        &socket_path,
        "/v1/inbound/openclaw/agents/repo_reviewer/status",
        &OpenClawInspectionContext {
            caller: openclaw_caller("local_gateway"),
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(agent_status["agent_id"], "repo_reviewer");
    assert_eq!(agent_status["observed_state"], "active");

    handle.abort();
}

#[tokio::test]
async fn openclaw_inbound_rejects_unmapped_caller_before_action_creation() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor_with_openclaw(dir.path()).await.unwrap();
    let (handle, socket_path) = spawn_api_server(Arc::clone(&supervisor)).await;

    let submit_request = OpenClawInboundActionRequest {
        caller: OpenClawCallerContext {
            workspace_root: Some(dir.path().display().to_string()),
            ..openclaw_caller("unknown_gateway")
        },
        target_agent_id: "repo_reviewer".to_string(),
        capability: "repo.review".to_string(),
        goal: crawfish_types::GoalSpec {
            summary: "review from unknown gateway".to_string(),
            details: None,
        },
        inputs: std::collections::BTreeMap::from([(
            "changed_files".to_string(),
            serde_json::json!(["src/lib.rs"]),
        )]),
        contract_overrides: None,
        execution_strategy: None,
        schedule: None,
        data_boundary: None,
        workspace_write: false,
        secret_access: false,
        mutating: false,
    };

    let (status, body) = post_uds_json(
        &socket_path,
        "/v1/inbound/openclaw/actions",
        &submit_request,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body["error"].as_str().unwrap().contains("not mapped"));
    assert!(supervisor
        .store()
        .list_actions_by_phase(None)
        .await
        .unwrap()
        .is_empty());

    handle.abort();
}

#[tokio::test]
async fn openclaw_inbound_denies_foreign_owner_mutation() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor_with_openclaw(dir.path()).await.unwrap();
    let (handle, socket_path) = spawn_api_server(Arc::clone(&supervisor)).await;

    let submit_request = OpenClawInboundActionRequest {
        caller: OpenClawCallerContext {
            workspace_root: Some(dir.path().display().to_string()),
            ..openclaw_caller("foreign_gateway")
        },
        target_agent_id: "workspace_editor".to_string(),
        capability: "workspace.patch.apply".to_string(),
        goal: crawfish_types::GoalSpec {
            summary: "foreign patch attempt".to_string(),
            details: None,
        },
        inputs: std::collections::BTreeMap::from([(
            "edits".to_string(),
            serde_json::json!([{ "path": "denied.txt", "op": "create", "contents": "nope\n" }]),
        )]),
        contract_overrides: None,
        execution_strategy: None,
        schedule: None,
        data_boundary: None,
        workspace_write: true,
        secret_access: false,
        mutating: true,
    };

    let (status, body) = post_uds_json(
        &socket_path,
        "/v1/inbound/openclaw/actions",
        &submit_request,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body["error"].as_str().unwrap().contains("denied"));
    assert!(supervisor
        .store()
        .list_actions_by_phase(None)
        .await
        .unwrap()
        .is_empty());

    handle.abort();
}

#[tokio::test]
async fn openclaw_inbound_blocks_foreign_action_inspection() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor_with_openclaw(dir.path()).await.unwrap();
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
                summary: "local review".to_string(),
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
            contract_overrides: None,
            execution_strategy: None,
            schedule: None,
            counterparty_refs: vec![CounterpartyRef {
                agent_id: None,
                session_id: Some("local-session".to_string()),
                owner: local_owner("local-dev"),
                trust_domain: TrustDomain::SameOwnerLocal,
            }],
            data_boundary: None,
            workspace_write: false,
            secret_access: false,
            mutating: false,
        })
        .await
        .unwrap();
    let (handle, socket_path) = spawn_api_server(Arc::clone(&supervisor)).await;

    let (status, body) = post_uds_json(
        &socket_path,
        &format!(
            "/v1/inbound/openclaw/actions/{}/inspect",
            submitted.action_id
        ),
        &OpenClawInspectionContext {
            caller: openclaw_caller("foreign_gateway"),
        },
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body["error"].as_str().unwrap().contains("cannot inspect"));

    handle.abort();
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

#[tokio::test]
async fn workspace_mutation_surfaces_frontier_gap_incident() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();

    let submitted = supervisor
        .submit_action(workspace_patch_request(
            dir.path(),
            serde_json::json!([
                {
                    "path": "frontier.txt",
                    "op": "create",
                    "contents": "frontier\n"
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

    let detail = supervisor
        .inspect_action(&submitted.action_id)
        .await
        .unwrap()
        .expect("action detail");
    assert_eq!(detail.action.phase, ActionPhase::Completed);
    assert!(detail
        .policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_gap_mutation_post_result_review"));
    assert!(detail
        .policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_enforcement_gap"));
    assert!(detail
        .checkpoint_status
        .iter()
        .any(|status| status.checkpoint == crawfish_types::OversightCheckpoint::PreMutation));
}

#[tokio::test]
async fn health_endpoint_works_over_uds() {
    let dir = tempdir().unwrap();
    let supervisor = build_supervisor(dir.path()).await.unwrap();
    let socket_path = supervisor.config().socket_path(supervisor.root());
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await.unwrap();
    }
    if socket_path.exists() {
        tokio::fs::remove_file(&socket_path).await.unwrap();
    }
    let listener = UnixListener::bind(&socket_path).unwrap();
    let app = api_router(Arc::clone(&supervisor));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
    let uri: Uri = hyperlocal::Uri::new(&socket_path, "/v1/health").into();
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let response = client.request(request).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: HealthResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload.status, "ok");

    handle.abort();
}
