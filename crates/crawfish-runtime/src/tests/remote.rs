use super::support::*;
use super::*;

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
