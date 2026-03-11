use super::support::*;
use super::*;

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
