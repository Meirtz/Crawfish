use super::*;

impl Supervisor {
    pub async fn from_config_path(path: &Path) -> anyhow::Result<Self> {
        let root = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let config = CrawfishConfig::load(path)?;
        let state_dir = config.state_dir(&root);
        let sqlite_path = config.sqlite_path(&root);
        let store = SqliteStore::connect(&sqlite_path, &state_dir).await?;
        Ok(Self {
            root,
            config,
            store,
        })
    }

    pub async fn run_once(&self) -> anyhow::Result<()> {
        for manifest in self.load_manifests()? {
            self.store.upsert_agent_manifest(&manifest).await?;
            let record = self.reconcile_manifest(&manifest).await?;
            self.store.upsert_lifecycle_record(&record).await?;
        }

        self.recover_running_actions().await?;
        self.process_action_queue_once().await?;
        info!("reconciled manifests and processed local action queue");
        Ok(())
    }

    pub async fn run_until_signal(self: Arc<Self>) -> anyhow::Result<()> {
        self.run_once().await?;
        let socket_path = self.config.socket_path(&self.root);
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if socket_path.exists() {
            tokio::fs::remove_file(&socket_path).await?;
        }

        let listener = UnixListener::bind(&socket_path)?;
        let app = api_router(Arc::clone(&self));
        let reconcile = tokio::spawn({
            let supervisor = Arc::clone(&self);
            async move {
                loop {
                    if let Err(error) = supervisor.run_once().await {
                        error!("reconcile loop failed: {error:#}");
                    }
                    sleep(Duration::from_millis(
                        supervisor.config.runtime.reconcile_interval_ms,
                    ))
                    .await;
                }
            }
        });

        let server = axum::serve(listener, app).with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        });
        let result = server.await;
        reconcile.abort();
        if socket_path.exists() {
            let _ = tokio::fs::remove_file(&socket_path).await;
        }
        result?;
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &CrawfishConfig {
        &self.config
    }

    pub fn store(&self) -> &SqliteStore {
        &self.store
    }

    pub(crate) fn state_dir(&self) -> PathBuf {
        self.config.state_dir(&self.root)
    }

    pub(crate) async fn recover_running_actions(&self) -> anyhow::Result<()> {
        for mut action in self.store.list_actions_by_phase(Some("running")).await? {
            let abandoned_openclaw_run = action
                .external_refs
                .iter()
                .find(|reference| reference.kind == "openclaw.run_id")
                .map(|reference| reference.value.clone());
            let recovery_stage = match self.load_deterministic_checkpoint(&action).await? {
                Some(checkpoint) => {
                    action.checkpoint_ref =
                        Some(checkpoint_ref_for_executor(&checkpoint.executor_kind));
                    Some(checkpoint.stage)
                }
                None => Some("requeued_after_restart".to_string()),
            };
            action.phase = ActionPhase::Accepted;
            action.started_at = None;
            action.finished_at = None;
            action.recovery_stage = recovery_stage.clone();
            action.failure_code = Some(failure_code_requeued_after_restart().to_string());
            self.store.upsert_action(&action).await?;
            if let Some(run_id) = abandoned_openclaw_run {
                self.store
                    .append_action_event(
                        &action.id,
                        "openclaw_run_abandoned",
                        serde_json::json!({
                            "run_id": run_id,
                            "reason": "daemon restart requeued running action",
                        }),
                    )
                    .await?;
            }
            self.store
                .append_action_event(
                    &action.id,
                    "recovered",
                    serde_json::json!({
                        "phase": "accepted",
                        "code": failure_code_requeued_after_restart(),
                        "recovery_stage": recovery_stage,
                        "reason": "daemon restart requeued running action",
                    }),
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn expire_awaiting_approval_actions(&self) -> anyhow::Result<()> {
        let now = current_timestamp_seconds();
        for mut action in self
            .store
            .list_actions_by_phase(Some("awaiting_approval"))
            .await?
        {
            let Some(deadline_ms) = action.contract.delivery.deadline_ms else {
                continue;
            };
            let created_at = action.created_at.parse::<u64>().unwrap_or_default();
            if now.saturating_sub(created_at) * 1000 < deadline_ms {
                continue;
            }

            action.phase = ActionPhase::Expired;
            action.finished_at = Some(now.to_string());
            action.failure_reason = Some("approval expired before deadline".to_string());
            action.failure_code = Some(failure_code_approval_required().to_string());
            if let Some(encounter_ref) = &action.encounter_ref {
                if let Some(mut encounter) = self.store.get_encounter(encounter_ref).await? {
                    encounter.state = EncounterState::Expired;
                    self.store.insert_encounter(&encounter).await?;
                }
                let receipt = self
                    .emit_audit_receipt(
                        encounter_ref,
                        action.grant_refs.clone(),
                        action.lease_ref.clone(),
                        AuditOutcome::Expired,
                        "approval expired before deadline".to_string(),
                        None,
                    )
                    .await?;
                action.audit_receipt_ref = Some(receipt.id.clone());
            }
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "expired",
                    serde_json::json!({
                        "reason": action.failure_reason,
                        "code": action.failure_code,
                        "finished_at": action.finished_at,
                    }),
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) fn validate_submit_action_request(
        &self,
        manifest: &AgentManifest,
        request: &SubmitActionRequest,
    ) -> anyhow::Result<()> {
        if !manifest
            .capabilities
            .iter()
            .any(|cap| cap == &request.capability)
        {
            anyhow::bail!(
                "invalid action request: capability {} is not exposed by {}",
                request.capability,
                manifest.id
            );
        }

        match request.capability.as_str() {
            "repo.index" => {
                let workspace_root = request
                    .inputs
                    .get("workspace_root")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid action request: repo.index requires workspace_root"
                        )
                    })?;
                let workspace_path = Path::new(workspace_root);
                if !workspace_path.is_dir() {
                    anyhow::bail!(
                        "invalid action request: workspace_root must be an existing directory"
                    );
                }
            }
            "repo.review" => {
                let workspace_root = request
                    .inputs
                    .get("workspace_root")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid action request: repo.review requires workspace_root"
                        )
                    })?;
                if !Path::new(workspace_root).is_dir() {
                    anyhow::bail!(
                        "invalid action request: workspace_root must be an existing directory"
                    );
                }

                let has_diff_text = request
                    .inputs
                    .get("diff_text")
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false);
                let has_diff_file = request
                    .inputs
                    .get("diff_file")
                    .and_then(Value::as_str)
                    .map(|path| Path::new(path).is_file())
                    .unwrap_or(false);
                let has_changed_files = request
                    .inputs
                    .get("changed_files")
                    .and_then(Value::as_array)
                    .map(|files| !files.is_empty())
                    .unwrap_or(false);

                if !(has_diff_text || has_diff_file || has_changed_files) {
                    anyhow::bail!(
                        "invalid action request: repo.review requires diff_text, diff_file, or changed_files"
                    );
                }
            }
            "ci.triage" => {
                let has_log_text = request
                    .inputs
                    .get("log_text")
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false);
                let has_log_file = request
                    .inputs
                    .get("log_file")
                    .and_then(Value::as_str)
                    .map(|path| Path::new(path).is_file())
                    .unwrap_or(false);
                let has_mcp_resource_ref = request
                    .inputs
                    .get("mcp_resource_ref")
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false);

                if !(has_log_text || has_log_file || has_mcp_resource_ref) {
                    anyhow::bail!(
                        "invalid action request: ci.triage requires log_text, log_file, or mcp_resource_ref"
                    );
                }
            }
            "incident.enrich" => {
                let has_log_text = request
                    .inputs
                    .get("log_text")
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false);
                let has_log_file = request
                    .inputs
                    .get("log_file")
                    .and_then(Value::as_str)
                    .map(|path| Path::new(path).is_file())
                    .unwrap_or(false);
                let has_service_manifest = request
                    .inputs
                    .get("service_manifest_file")
                    .and_then(Value::as_str)
                    .map(|path| Path::new(path).is_file())
                    .unwrap_or(false);

                if !(has_log_text || has_log_file || has_service_manifest) {
                    anyhow::bail!(
                        "invalid action request: incident.enrich requires log_text, log_file, or service_manifest_file"
                    );
                }
            }
            capability if is_task_plan_capability(capability) => {
                if let Some(workspace_root) =
                    request.inputs.get("workspace_root").and_then(Value::as_str)
                {
                    if !Path::new(workspace_root).is_dir() {
                        anyhow::bail!(
                            "invalid action request: workspace_root must be an existing directory"
                        );
                    }
                }

                let has_objective = request
                    .inputs
                    .get("objective")
                    .and_then(Value::as_str)
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false);
                if !has_objective {
                    anyhow::bail!(
                        "invalid action request: task.plan requires objective, task, spec_text, or problem_statement"
                    );
                }
            }
            "workspace.patch.apply" => {
                let workspace_root = request
                    .inputs
                    .get("workspace_root")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid action request: workspace.patch.apply requires workspace_root"
                        )
                    })?;
                if !Path::new(workspace_root).is_dir() {
                    anyhow::bail!(
                        "invalid action request: workspace_root must be an existing directory"
                    );
                }

                let edits_value = request.inputs.get("edits").cloned().ok_or_else(|| {
                    anyhow::anyhow!("invalid action request: workspace.patch.apply requires edits")
                })?;
                let edits: Vec<WorkspaceEdit> = serde_json::from_value(edits_value)
                    .map_err(|error| {
                        anyhow::anyhow!(
                            "invalid action request: workspace.patch.apply edits are invalid: {error}"
                        )
                    })?;
                if edits.is_empty() {
                    anyhow::bail!(
                        "invalid action request: workspace.patch.apply requires at least one edit"
                    );
                }
                for edit in edits {
                    if edit.path.trim().is_empty() {
                        anyhow::bail!(
                            "invalid action request: workspace.patch.apply edit path cannot be empty"
                        );
                    }
                    match edit.op {
                        WorkspaceEditOp::Create => {
                            if edit.contents.is_none() {
                                anyhow::bail!(
                                    "invalid action request: create edits require contents"
                                );
                            }
                        }
                        WorkspaceEditOp::Replace => {
                            if edit.contents.is_none() {
                                anyhow::bail!(
                                    "invalid action request: replace edits require contents"
                                );
                            }
                            if edit.expected_sha256.is_none() {
                                anyhow::bail!(
                                    "invalid action request: replace edits require expected_sha256"
                                );
                            }
                        }
                        WorkspaceEditOp::Delete => {
                            if edit.contents.is_some() {
                                anyhow::bail!(
                                    "invalid action request: delete edits must not include contents"
                                );
                            }
                            if edit.expected_sha256.is_none() {
                                anyhow::bail!(
                                    "invalid action request: delete edits require expected_sha256"
                                );
                            }
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

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

    pub(crate) fn resolve_mcp_adapter(
        &self,
        manifest: &AgentManifest,
        action: &Action,
    ) -> anyhow::Result<Option<(McpAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::Mcp(binding) => Some(binding.clone()),
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let server = self
            .config
            .mcp
            .servers
            .get(&binding.server)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("mcp server {} is not configured", binding.server))?;
        let adapter =
            McpAdapter::configured(binding.server.clone(), server.clone(), binding.clone())?;
        let mut external_refs = vec![
            ExternalRef {
                kind: "mcp_server".to_string(),
                value: binding.server.clone(),
                endpoint: Some(server.url),
            },
            ExternalRef {
                kind: "mcp_tool".to_string(),
                value: binding.tool,
                endpoint: None,
            },
        ];
        if let Some(resource_ref) = action
            .inputs
            .get("mcp_resource_ref")
            .and_then(Value::as_str)
        {
            external_refs.push(ExternalRef {
                kind: "mcp_resource".to_string(),
                value: resource_ref.to_string(),
                endpoint: None,
            });
        }
        Ok(Some((adapter, external_refs)))
    }

    pub(crate) fn resolve_openclaw_adapter(
        &self,
        manifest: &AgentManifest,
    ) -> anyhow::Result<Option<(OpenClawAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::Openclaw(binding) => Some(binding.clone()),
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let external_refs = vec![
            ExternalRef {
                kind: "openclaw.gateway_url".to_string(),
                value: binding.gateway_url.clone(),
                endpoint: Some(binding.gateway_url.clone()),
            },
            ExternalRef {
                kind: "openclaw.target_agent".to_string(),
                value: binding.target_agent.clone(),
                endpoint: None,
            },
        ];
        Ok(Some((
            OpenClawAdapter::new(binding, self.state_dir()),
            external_refs,
        )))
    }

    pub(crate) fn resolve_local_harness_adapter(
        &self,
        manifest: &AgentManifest,
        harness: LocalHarnessKind,
    ) -> anyhow::Result<Option<(LocalHarnessAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::LocalHarness(binding)
                if binding.capability == "task.plan" && binding.harness == harness =>
            {
                Some(binding.clone())
            }
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let harness_name = match binding.harness {
            LocalHarnessKind::ClaudeCode => "claude_code",
            LocalHarnessKind::Codex => "codex",
        };
        let external_refs = vec![
            ExternalRef {
                kind: "local_harness.harness".to_string(),
                value: harness_name.to_string(),
                endpoint: None,
            },
            ExternalRef {
                kind: "local_harness.command".to_string(),
                value: binding.command.clone(),
                endpoint: None,
            },
        ];
        Ok(Some((
            LocalHarnessAdapter::new(binding, self.state_dir()),
            external_refs,
        )))
    }

    pub(crate) fn resolve_a2a_adapter(
        &self,
        manifest: &AgentManifest,
    ) -> anyhow::Result<Option<(A2aAdapter, Vec<ExternalRef>)>> {
        let binding = manifest.adapters.iter().find_map(|binding| match binding {
            AdapterBinding::A2a(binding) if binding.capability == "task.plan" => {
                Some(binding.clone())
            }
            _ => None,
        });
        let Some(binding) = binding else {
            return Ok(None);
        };

        let treaty_pack = self
            .config
            .treaties
            .packs
            .get(&binding.treaty_pack)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "treaty pack {} is not configured for remote delegation",
                    binding.treaty_pack
                )
            })?;
        let external_refs = vec![
            ExternalRef {
                kind: "a2a.agent_card_url".to_string(),
                value: binding.agent_card_url.clone(),
                endpoint: Some(binding.agent_card_url.clone()),
            },
            ExternalRef {
                kind: "a2a.remote_principal".to_string(),
                value: treaty_pack.remote_principal.id.clone(),
                endpoint: None,
            },
        ];
        Ok(Some((
            A2aAdapter::new(binding, treaty_pack, self.state_dir()),
            external_refs,
        )))
    }

    pub(crate) fn builtin_federation_pack(
        &self,
        treaty_pack: &crawfish_types::TreatyPack,
    ) -> FederationPack {
        FederationPack {
            id: "remote_task_plan_default".to_string(),
            title: "Remote task.plan federation pack".to_string(),
            summary: "Default remote-agent governance pack for proposal-only task planning over a treaty-governed frontier.".to_string(),
            treaty_pack_id: treaty_pack.id.clone(),
            review_defaults: crawfish_types::FederationReviewDefaults {
                enabled: treaty_pack.review_queue,
                priority: "high".to_string(),
            },
            alert_defaults: crawfish_types::FederationAlertDefaults {
                rules: treaty_pack.alert_rules.clone(),
            },
            required_remote_evidence: treaty_pack.required_result_evidence.clone(),
            result_acceptance_policy: RemoteResultAcceptance::Accepted,
            scope_violation_policy: match treaty_pack.on_scope_violation {
                crawfish_types::TreatyEscalationMode::Deny => RemoteResultAcceptance::Rejected,
                crawfish_types::TreatyEscalationMode::ReviewRequired => {
                    RemoteResultAcceptance::ReviewRequired
                }
            },
            evidence_gap_policy: match treaty_pack.on_evidence_gap {
                crawfish_types::TreatyEscalationMode::Deny => RemoteResultAcceptance::Rejected,
                crawfish_types::TreatyEscalationMode::ReviewRequired => {
                    RemoteResultAcceptance::ReviewRequired
                }
            },
            blocked_remote_policy: RemoteStateDisposition::Blocked,
            auth_required_policy: RemoteStateDisposition::AwaitingApproval,
            remote_failure_policy: RemoteStateDisposition::Failed,
            required_checkpoints: treaty_pack.required_checkpoints.clone(),
            followup_allowed: true,
            max_followup_attempts: 2,
            followup_review_priority: "high".to_string(),
            max_delegation_depth: treaty_pack.max_delegation_depth,
        }
    }

    pub(crate) fn resolve_federation_pack(
        &self,
        binding: &crawfish_types::A2ARemoteAgentBinding,
        treaty_pack: &crawfish_types::TreatyPack,
    ) -> anyhow::Result<FederationPack> {
        match binding.federation_pack.as_deref() {
            Some(pack_id) => self
                .config
                .federation
                .packs
                .get(pack_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("federation pack {pack_id} is not configured")),
            None => Ok(self.builtin_federation_pack(treaty_pack)),
        }
    }

    pub(crate) fn compile_treaty_decision(
        &self,
        action: &Action,
        binding: &crawfish_types::A2ARemoteAgentBinding,
        treaty_pack: &crawfish_types::TreatyPack,
    ) -> anyhow::Result<crawfish_types::TreatyDecision> {
        if action.initiator_owner.kind != treaty_pack.local_owner.kind
            || action.initiator_owner.id != treaty_pack.local_owner.id
        {
            anyhow::bail!(
                "treaty {} requires local owner {}:{}, got {}:{}",
                treaty_pack.id,
                runtime_enum_to_snake(&treaty_pack.local_owner.kind),
                treaty_pack.local_owner.id,
                runtime_enum_to_snake(&action.initiator_owner.kind),
                action.initiator_owner.id
            );
        }
        if !treaty_pack
            .allowed_capabilities
            .iter()
            .any(|capability| capability == &binding.capability)
        {
            anyhow::bail!(
                "treaty {} does not allow capability {}",
                treaty_pack.id,
                binding.capability
            );
        }
        if treaty_pack.max_delegation_depth != 1 {
            anyhow::bail!(
                "treaty {} exceeds supported delegation depth {}",
                treaty_pack.id,
                treaty_pack.max_delegation_depth
            );
        }

        let delegated_data_scopes = task_plan_delegated_data_scopes(action);
        let out_of_scope = delegated_data_scopes
            .iter()
            .filter(|scope| {
                !treaty_pack
                    .allowed_data_scopes
                    .iter()
                    .any(|allowed| allowed == *scope)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !out_of_scope.is_empty() {
            anyhow::bail!(
                "treaty {} does not allow delegated data scopes: {}",
                treaty_pack.id,
                out_of_scope.join(", ")
            );
        }

        Ok(crawfish_types::TreatyDecision {
            treaty_pack_id: treaty_pack.id.clone(),
            remote_principal: treaty_pack.remote_principal.clone(),
            capability: binding.capability.clone(),
            requested_scopes: binding.required_scopes.clone(),
            delegated_data_scopes,
            required_checkpoints: treaty_pack.required_checkpoints.clone(),
            required_result_evidence: treaty_pack.required_result_evidence.clone(),
            delegation_depth: 1,
            on_scope_violation: treaty_pack.on_scope_violation.clone(),
            on_evidence_gap: treaty_pack.on_evidence_gap.clone(),
            review_queue: treaty_pack.review_queue,
            alert_rules: treaty_pack.alert_rules.clone(),
        })
    }

    pub(crate) fn compile_federation_decision(
        &self,
        action: &Action,
        treaty_decision: &crawfish_types::TreatyDecision,
        federation_pack: &FederationPack,
    ) -> anyhow::Result<FederationDecision> {
        if federation_pack.treaty_pack_id != treaty_decision.treaty_pack_id {
            anyhow::bail!(
                "federation pack {} is bound to treaty {}, not {}",
                federation_pack.id,
                federation_pack.treaty_pack_id,
                treaty_decision.treaty_pack_id
            );
        }
        if federation_pack.max_delegation_depth < treaty_decision.delegation_depth {
            anyhow::bail!(
                "federation pack {} does not allow delegation depth {}",
                federation_pack.id,
                treaty_decision.delegation_depth
            );
        }
        Ok(FederationDecision {
            federation_pack_id: federation_pack.id.clone(),
            treaty_pack_id: treaty_decision.treaty_pack_id.clone(),
            remote_principal: treaty_decision.remote_principal.clone(),
            capability: action.capability.clone(),
            required_checkpoints: federation_pack.required_checkpoints.clone(),
            required_remote_evidence: federation_pack.required_remote_evidence.clone(),
            delegation_depth: treaty_decision.delegation_depth,
            escalation: crawfish_types::RemoteEscalationPolicy {
                result_acceptance_policy: federation_pack.result_acceptance_policy.clone(),
                scope_violation_policy: federation_pack.scope_violation_policy.clone(),
                evidence_gap_policy: federation_pack.evidence_gap_policy.clone(),
                blocked_remote_policy: federation_pack.blocked_remote_policy.clone(),
                auth_required_policy: federation_pack.auth_required_policy.clone(),
                remote_failure_policy: federation_pack.remote_failure_policy.clone(),
            },
            review_defaults: federation_pack.review_defaults.clone(),
            alert_defaults: federation_pack.alert_defaults.clone(),
            remote_state_disposition: None,
            remote_evidence_status: None,
            remote_result_acceptance: None,
            summary: format!(
                "{} interprets remote task.plan results for treaty {} at delegation depth {}",
                federation_pack.id,
                treaty_decision.treaty_pack_id,
                treaty_decision.delegation_depth
            ),
        })
    }

    pub(crate) fn evaluate_federation_post_result(
        &self,
        outputs: &ActionOutputs,
        receipt: &crawfish_types::DelegationReceipt,
        _decision: &crawfish_types::TreatyDecision,
        treaty_pack: &crawfish_types::TreatyPack,
        federation_pack: &FederationPack,
    ) -> (
        crawfish_types::RemoteOutcomeDisposition,
        RemoteEvidenceStatus,
        Vec<crawfish_types::TreatyViolation>,
    ) {
        let mut violations = Vec::new();
        for requirement in &federation_pack.required_remote_evidence {
            match requirement {
                crawfish_types::TreatyEvidenceRequirement::DelegationReceiptPresent => {
                    if receipt.id.is_empty() {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "frontier_enforcement_gap".to_string(),
                            summary: "delegation receipt is missing".to_string(),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some("delegation_receipt".to_string()),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::RemoteTaskRefPresent => {
                    if receipt
                        .remote_task_ref
                        .as_deref()
                        .unwrap_or_default()
                        .is_empty()
                    {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "frontier_enforcement_gap".to_string(),
                            summary: "remote task reference is missing".to_string(),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some("remote_task_ref".to_string()),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::TerminalStateVerified => {
                    let verified = outputs
                        .metadata
                        .get("a2a_remote_state")
                        .and_then(Value::as_str)
                        .map(|state| matches!(state, "completed" | "failed"))
                        .unwrap_or(false)
                        && outputs.metadata.contains_key("a2a_result");
                    if !verified {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "frontier_enforcement_gap".to_string(),
                            summary:
                                "remote terminal state could not be proven from returned evidence"
                                    .to_string(),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some("terminal_state".to_string()),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::ArtifactClassesAllowed => {
                    let disallowed = outputs
                        .artifacts
                        .iter()
                        .map(artifact_basename)
                        .filter(|artifact| {
                            !treaty_pack
                                .allowed_artifact_classes
                                .iter()
                                .any(|allowed| allowed == artifact)
                        })
                        .collect::<Vec<_>>();
                    for artifact in disallowed {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "treaty_scope_violation".to_string(),
                            summary: format!(
                                "remote result returned disallowed artifact class {artifact}"
                            ),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some(artifact),
                        });
                    }
                }
                crawfish_types::TreatyEvidenceRequirement::DataScopesAllowed => {
                    let disallowed = receipt
                        .delegated_data_scopes
                        .iter()
                        .filter(|scope| {
                            !treaty_pack
                                .allowed_data_scopes
                                .iter()
                                .any(|allowed| allowed == *scope)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    for scope in disallowed {
                        violations.push(crawfish_types::TreatyViolation {
                            code: "treaty_scope_violation".to_string(),
                            summary: format!(
                                "remote delegation used a data scope outside treaty allowance: {scope}"
                            ),
                            checkpoint: Some(crawfish_types::OversightCheckpoint::PostResult),
                            subject: Some(scope),
                        });
                    }
                }
            }
        }

        let evidence_status = if violations
            .iter()
            .any(|violation| violation.code == "treaty_scope_violation")
        {
            RemoteEvidenceStatus::ScopeViolation
        } else if violations
            .iter()
            .any(|violation| violation.code == "frontier_enforcement_gap")
        {
            RemoteEvidenceStatus::MissingRequiredEvidence
        } else {
            RemoteEvidenceStatus::Satisfied
        };

        let disposition = match evidence_status {
            RemoteEvidenceStatus::ScopeViolation => match federation_pack.scope_violation_policy {
                RemoteResultAcceptance::Accepted => {
                    crawfish_types::RemoteOutcomeDisposition::Accepted
                }
                RemoteResultAcceptance::ReviewRequired => {
                    crawfish_types::RemoteOutcomeDisposition::ReviewRequired
                }
                RemoteResultAcceptance::Rejected => {
                    crawfish_types::RemoteOutcomeDisposition::Rejected
                }
            },
            RemoteEvidenceStatus::MissingRequiredEvidence => {
                match federation_pack.evidence_gap_policy {
                    RemoteResultAcceptance::Accepted => {
                        crawfish_types::RemoteOutcomeDisposition::Accepted
                    }
                    RemoteResultAcceptance::ReviewRequired => {
                        crawfish_types::RemoteOutcomeDisposition::ReviewRequired
                    }
                    RemoteResultAcceptance::Rejected => {
                        crawfish_types::RemoteOutcomeDisposition::Rejected
                    }
                }
            }
            _ => match federation_pack.result_acceptance_policy {
                RemoteResultAcceptance::Accepted => {
                    crawfish_types::RemoteOutcomeDisposition::Accepted
                }
                RemoteResultAcceptance::ReviewRequired => {
                    crawfish_types::RemoteOutcomeDisposition::ReviewRequired
                }
                RemoteResultAcceptance::Rejected => {
                    crawfish_types::RemoteOutcomeDisposition::Rejected
                }
            },
        };

        (disposition, evidence_status, violations)
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

    pub(crate) fn synthetic_owner(&self) -> crawfish_types::OwnerRef {
        crawfish_types::OwnerRef {
            kind: crawfish_types::OwnerKind::ServiceAccount,
            id: "crawfishd".to_string(),
            display_name: Some("Crawfish Daemon".to_string()),
        }
    }

    pub(crate) async fn reconcile_manifest(
        &self,
        manifest: &AgentManifest,
    ) -> anyhow::Result<LifecycleRecord> {
        let is_draining = self.store.is_draining().await?;
        if is_draining {
            return Ok(LifecycleRecord {
                agent_id: manifest.id.clone(),
                desired_state: AgentState::Inactive,
                observed_state: AgentState::Inactive,
                health: HealthStatus::Healthy,
                transition_reason: Some("admin drain is active".to_string()),
                last_transition_at: now_timestamp(),
                degradation_profile: None,
                continuity_mode: None,
                failure_count: 0,
            });
        }

        let dependency_missing = manifest
            .dependencies
            .iter()
            .any(|dependency| !self.manifest_exists(dependency));

        let compiled = compile_execution_plan(
            &self.config.contracts.org_defaults,
            &manifest.contract_defaults,
            &ExecutionContractPatch::default(),
            &manifest.strategy_defaults,
            manifest
                .capabilities
                .first()
                .map(String::as_str)
                .unwrap_or("default"),
            None,
        )?;

        let (observed_state, health, degradation_profile) = if dependency_missing {
            (
                AgentState::Degraded,
                HealthStatus::Degraded,
                Some(DegradedProfileName::DependencyIsolation),
            )
        } else {
            (AgentState::Active, HealthStatus::Healthy, None)
        };

        let continuity_mode = if compiled.contract.execution.preferred_harnesses.is_empty() {
            Some(ContinuityModeName::DeterministicOnly)
        } else {
            None
        };

        Ok(LifecycleRecord {
            agent_id: manifest.id.clone(),
            desired_state: AgentState::Active,
            observed_state,
            health,
            transition_reason: if dependency_missing {
                Some("dependency missing during reconcile".to_string())
            } else {
                Some("reconciled successfully".to_string())
            },
            last_transition_at: now_timestamp(),
            degradation_profile,
            continuity_mode,
            failure_count: 0,
        })
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

    pub(crate) fn load_manifests(&self) -> anyhow::Result<Vec<AgentManifest>> {
        let manifests_dir = self.config.manifest_dir(&self.root);
        let mut entries = fs::read_dir(manifests_dir)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.path());

        let mut manifests = Vec::new();
        for entry in entries {
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("toml") {
                continue;
            }
            let contents = fs::read_to_string(entry.path())?;
            let manifest: AgentManifest = toml::from_str(&contents)?;
            manifests.push(manifest);
        }
        Ok(manifests)
    }

    pub(crate) fn manifest_exists(&self, dependency: &str) -> bool {
        let path = self
            .config
            .manifest_dir(&self.root)
            .join(format!("{dependency}.toml"));
        path.exists()
    }

    pub(crate) fn authorize(
        &self,
        manifest: &AgentManifest,
        request: &EncounterRequest,
    ) -> EncounterDecision {
        authorize_encounter(
            &GovernanceContext {
                system_defaults: self.config.governance.system_defaults.clone(),
                owner_policy: neutral_policy(),
                trust_domain_defaults: trust_domain_defaults(request.caller.trust_domain.clone()),
                manifest_policy: owner_policy_for_manifest(manifest),
            },
            request,
        )
    }

    pub(crate) fn resolve_openclaw_caller(
        &self,
        target_owner: &OwnerRef,
        caller: &OpenClawCallerContext,
    ) -> Result<OpenClawResolvedCaller, RuntimeError> {
        let inbound = &self.config.openclaw.inbound;
        if !inbound.enabled {
            return Err(RuntimeError::Forbidden(
                "openclaw inbound is disabled".to_string(),
            ));
        }

        let allowed = inbound.allowed_callers.get(&caller.caller_id);
        let (owner, trust_domain, allowed_scopes) = match (allowed, &inbound.caller_owner_mapping) {
            (Some(configured), _) => {
                let owner = OwnerRef {
                    kind: configured.owner_kind.clone(),
                    id: configured.owner_id.clone(),
                    display_name: configured.display_name.clone(),
                };
                let trust_domain = configured.trust_domain.clone().unwrap_or_else(|| {
                    if owner == *target_owner {
                        TrustDomain::SameOwnerLocal
                    } else {
                        inbound.default_trust_domain.clone()
                    }
                });
                (owner, trust_domain, configured.allowed_scopes.clone())
            }
            (None, CallerOwnerMapping::Required) => {
                return Err(RuntimeError::Forbidden(format!(
                    "openclaw caller is not mapped: {}",
                    caller.caller_id
                )));
            }
            (None, CallerOwnerMapping::BestEffort) => (
                OwnerRef {
                    kind: OwnerKind::ServiceAccount,
                    id: caller.caller_id.clone(),
                    display_name: caller.display_name.clone(),
                },
                inbound.default_trust_domain.clone(),
                Vec::new(),
            ),
        };

        if !allowed_scopes.is_empty()
            && caller
                .scopes
                .iter()
                .any(|scope| !allowed_scopes.iter().any(|allowed| allowed == scope))
        {
            return Err(RuntimeError::Forbidden(format!(
                "openclaw caller requested scopes outside its allowlist: {}",
                caller.caller_id
            )));
        }

        Ok(OpenClawResolvedCaller {
            caller_id: caller.caller_id.clone(),
            counterparty: CounterpartyRef {
                agent_id: None,
                session_id: Some(caller.session_id.clone()),
                owner,
                trust_domain,
            },
            requester_id: caller.session_id.clone(),
            effective_scopes: caller.scopes.clone(),
        })
    }

    pub(crate) fn openclaw_external_refs(
        &self,
        caller: &OpenClawCallerContext,
        effective_scopes: &[String],
    ) -> Vec<ExternalRef> {
        let mut refs = vec![
            ExternalRef {
                kind: "openclaw.caller_id".to_string(),
                value: caller.caller_id.clone(),
                endpoint: None,
            },
            ExternalRef {
                kind: "openclaw.session_id".to_string(),
                value: caller.session_id.clone(),
                endpoint: None,
            },
            ExternalRef {
                kind: "openclaw.channel_id".to_string(),
                value: caller.channel_id.clone(),
                endpoint: None,
            },
        ];

        if let Some(workspace_root) = &caller.workspace_root {
            refs.push(ExternalRef {
                kind: "openclaw.workspace_root".to_string(),
                value: workspace_root.clone(),
                endpoint: None,
            });
        }

        refs.extend(effective_scopes.iter().cloned().map(|scope| ExternalRef {
            kind: "openclaw.scope".to_string(),
            value: scope,
            endpoint: None,
        }));

        refs.extend(caller.trace_ids.iter().map(|(key, value)| ExternalRef {
            kind: format!("openclaw.trace.{key}"),
            value: value.to_string(),
            endpoint: None,
        }));

        refs
    }

    pub(crate) fn action_visible_to_openclaw(
        &self,
        caller: &OpenClawResolvedCaller,
        action: &Action,
    ) -> bool {
        if action.initiator_owner == caller.counterparty.owner {
            return true;
        }

        if action.requester.id == caller.requester_id {
            return true;
        }

        if action.counterparty_refs.iter().any(|counterparty| {
            counterparty.owner == caller.counterparty.owner
                && counterparty.session_id.as_deref() == caller.counterparty.session_id.as_deref()
        }) {
            return true;
        }

        action.external_refs.iter().any(|reference| {
            (reference.kind == "openclaw.caller_id" && reference.value == caller.caller_id)
                || (reference.kind == "openclaw.session_id"
                    && Some(reference.value.as_str()) == caller.counterparty.session_id.as_deref())
        })
    }

    pub(crate) fn agent_visible_to_openclaw(
        &self,
        caller: &OpenClawResolvedCaller,
        manifest: &AgentManifest,
    ) -> bool {
        if manifest.owner == caller.counterparty.owner {
            return true;
        }

        !matches!(
            owner_policy_for_manifest(manifest).capability_visibility,
            CapabilityVisibility::Private | CapabilityVisibility::OwnerOnly
        )
    }

    pub(crate) async fn preflight_submission(
        &self,
        request: &SubmitActionRequest,
    ) -> anyhow::Result<(
        AgentManifest,
        CompiledExecutionPlan,
        EncounterRequest,
        EncounterDecision,
        bool,
    )> {
        let manifest = self
            .store
            .get_agent_manifest(&request.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", request.target_agent_id))?;
        self.validate_submit_action_request(&manifest, request)?;

        let compiled = compile_execution_plan(
            &self.config.contracts.org_defaults,
            &manifest.contract_defaults,
            &request.contract_overrides.clone().unwrap_or_default(),
            &manifest.strategy_defaults,
            &request.capability,
            request.execution_strategy.clone(),
        )
        .map_err(|error| anyhow::anyhow!("invalid action request: {error}"))?;

        let caller = request
            .counterparty_refs
            .first()
            .cloned()
            .unwrap_or_else(|| CounterpartyRef {
                agent_id: None,
                session_id: Some("local".to_string()),
                owner: request.initiator_owner.clone(),
                trust_domain: TrustDomain::SameOwnerLocal,
            });
        let encounter_request = EncounterRequest {
            caller,
            target_agent_id: request.target_agent_id.clone(),
            target_owner: manifest.owner.clone(),
            requested_capabilities: vec![request.capability.clone()],
            requests_workspace_write: request.workspace_write,
            requests_secret_access: request.secret_access,
            requests_mutating_capability: request.mutating,
        };
        let decision = self.authorize(&manifest, &encounter_request);
        let requires_approval = self.action_requires_approval(
            request,
            &manifest,
            &request.capability,
            &compiled.contract.safety.approval_policy,
        );

        Ok((
            manifest,
            compiled,
            encounter_request,
            decision,
            requires_approval,
        ))
    }

    pub(crate) fn action_requires_approval(
        &self,
        request: &SubmitActionRequest,
        manifest: &AgentManifest,
        capability: &str,
        approval_policy: &ApprovalPolicy,
    ) -> bool {
        if capability == "workspace.patch.apply" {
            return true;
        }

        if request.workspace_write || request.secret_access || request.mutating {
            return !matches!(approval_policy, ApprovalPolicy::None)
                || matches!(
                    manifest.workspace_policy.write_mode,
                    crawfish_types::WorkspaceWriteMode::ApprovalGated
                );
        }

        matches!(approval_policy, ApprovalPolicy::Always)
    }

    pub(crate) async fn create_encounter(
        &self,
        manifest: &AgentManifest,
        request: &EncounterRequest,
        _decision: &EncounterDecision,
        state: EncounterState,
    ) -> anyhow::Result<EncounterRecord> {
        let encounter = EncounterRecord {
            id: Uuid::new_v4().to_string(),
            initiator_ref: request.caller.clone(),
            target_agent_id: request.target_agent_id.clone(),
            target_owner: manifest.owner.clone(),
            trust_domain: request.caller.trust_domain.clone(),
            requested_capabilities: request.requested_capabilities.clone(),
            applied_policy_source: "system>owner>trust-domain>manifest".to_string(),
            state,
            grant_refs: Vec::new(),
            lease_ref: None,
            created_at: now_timestamp(),
        };
        self.store.insert_encounter(&encounter).await?;
        Ok(encounter)
    }

    pub(crate) async fn emit_audit_receipt(
        &self,
        encounter_ref: &str,
        grant_refs: Vec<String>,
        lease_ref: Option<String>,
        outcome: AuditOutcome,
        reason: String,
        approver_ref: Option<String>,
    ) -> anyhow::Result<AuditReceipt> {
        let receipt = AuditReceipt {
            id: Uuid::new_v4().to_string(),
            encounter_ref: encounter_ref.to_string(),
            grant_refs,
            lease_ref,
            outcome,
            reason,
            approver_ref,
            emitted_at: now_timestamp(),
        };
        self.store.insert_audit_receipt(&receipt).await?;
        Ok(receipt)
    }

    pub(crate) fn approval_expiry_for_action(&self, action: &Action) -> String {
        let base = action.created_at.parse::<u64>().unwrap_or_default();
        let deadline = action.contract.delivery.deadline_ms.unwrap_or(900_000);
        (base.saturating_add(deadline / 1000)).to_string()
    }

    pub(crate) async fn issue_grant_and_lease(
        &self,
        action: &Action,
        manifest: &AgentManifest,
        encounter: &mut EncounterRecord,
        approver_ref: Option<String>,
        reason: String,
    ) -> anyhow::Result<(ConsentGrant, CapabilityLease, AuditReceipt)> {
        let expires_at = self.approval_expiry_for_action(action);
        let grant = ConsentGrant {
            id: Uuid::new_v4().to_string(),
            grantor: manifest.owner.clone(),
            grantee: action.initiator_owner.clone(),
            purpose: action.goal.summary.clone(),
            scope: vec![action.capability.clone()],
            issued_at: now_timestamp(),
            expires_at: expires_at.clone(),
            revocable: true,
            approver_ref: approver_ref.clone(),
        };
        self.store.upsert_consent_grant(&grant).await?;

        let lease = CapabilityLease {
            id: Uuid::new_v4().to_string(),
            grant_ref: grant.id.clone(),
            lessor: manifest.owner.clone(),
            lessee: action.initiator_owner.clone(),
            capability_refs: vec![action.capability.clone()],
            scope: if action.contract.safety.tool_scope.is_empty() {
                vec![action.capability.clone()]
            } else {
                action.contract.safety.tool_scope.clone()
            },
            issued_at: now_timestamp(),
            expires_at,
            revocation_reason: None,
            audit_receipt_ref: String::new(),
        };
        self.store.upsert_capability_lease(&lease).await?;

        encounter.state = EncounterState::Leased;
        encounter.grant_refs = vec![grant.id.clone()];
        encounter.lease_ref = Some(lease.id.clone());
        self.store.insert_encounter(encounter).await?;

        let receipt = self
            .emit_audit_receipt(
                &encounter.id,
                vec![grant.id.clone()],
                Some(lease.id.clone()),
                AuditOutcome::Allowed,
                reason,
                approver_ref,
            )
            .await?;

        let mut persisted_lease = lease;
        persisted_lease.audit_receipt_ref = receipt.id.clone();
        self.store.upsert_capability_lease(&persisted_lease).await?;

        Ok((grant, persisted_lease, receipt))
    }

    pub(crate) async fn ensure_pre_execution_lease_valid(
        &self,
        action: &Action,
    ) -> anyhow::Result<()> {
        let Some(lease_ref) = &action.lease_ref else {
            if action.capability == "workspace.patch.apply" {
                anyhow::bail!("mutation action requires an active capability lease");
            }
            return Ok(());
        };
        let lease = self
            .store
            .get_capability_lease(lease_ref)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capability lease not found: {lease_ref}"))?;
        if lease.revocation_reason.is_some() {
            anyhow::bail!("capability lease {} has been revoked", lease.id);
        }
        let now = current_timestamp_seconds();
        let expires_at = lease.expires_at.parse::<u64>().unwrap_or_default();
        if expires_at > 0 && now >= expires_at {
            anyhow::bail!("capability lease {} has expired", lease.id);
        }
        Ok(())
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

    pub(crate) async fn submit_openclaw_action(
        &self,
        request: OpenClawInboundActionRequest,
    ) -> Result<OpenClawInboundActionResponse, RuntimeError> {
        let manifest = self
            .store
            .get_agent_manifest(&request.target_agent_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("agent not found: {}", request.target_agent_id))
            })?;
        let caller = self.resolve_openclaw_caller(&manifest.owner, &request.caller)?;

        let mut inputs = request.inputs;
        if let Some(workspace_root) = &request.caller.workspace_root {
            inputs
                .entry("workspace_root".to_string())
                .or_insert_with(|| Value::String(workspace_root.clone()));
        }

        let submit_request = normalize_submit_request(SubmitActionRequest {
            target_agent_id: request.target_agent_id,
            requester: crawfish_types::RequesterRef {
                kind: crawfish_types::RequesterKind::Session,
                id: caller.requester_id.clone(),
            },
            initiator_owner: caller.counterparty.owner.clone(),
            capability: request.capability,
            goal: request.goal,
            inputs,
            contract_overrides: request.contract_overrides,
            execution_strategy: request.execution_strategy,
            schedule: request.schedule,
            counterparty_refs: vec![caller.counterparty.clone()],
            data_boundary: request.data_boundary,
            workspace_write: request.workspace_write,
            secret_access: request.secret_access,
            mutating: request.mutating,
        });

        let (_, _, _, decision, _) = self
            .preflight_submission(&submit_request)
            .await
            .map_err(map_submit_error)?;
        if matches!(decision.disposition, EncounterDisposition::Deny) {
            return Err(RuntimeError::Forbidden(decision.reason));
        }

        let submitted = self
            .submit_action(submit_request)
            .await
            .map_err(map_submit_error)?;
        let mut action = self
            .store
            .get_action(&submitted.action_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| {
                RuntimeError::NotFound(format!("action not found: {}", submitted.action_id))
            })?;
        let trace_refs = self.openclaw_external_refs(&request.caller, &caller.effective_scopes);
        action.external_refs.extend(trace_refs.clone());
        self.store
            .upsert_action(&action)
            .await
            .map_err(RuntimeError::Internal)?;
        self.store
            .append_action_event(
                &action.id,
                "openclaw_inbound",
                serde_json::json!({
                    "caller_id": request.caller.caller_id,
                    "session_id": request.caller.session_id,
                    "channel_id": request.caller.channel_id,
                    "scopes": caller.effective_scopes,
                    "trace_ids": request.caller.trace_ids,
                }),
            )
            .await
            .map_err(RuntimeError::Internal)?;

        Ok(OpenClawInboundActionResponse {
            action_id: action.id,
            phase: submitted.phase,
            requester_id: caller.requester_id,
            trace_refs,
        })
    }

    pub(crate) async fn inspect_openclaw_action(
        &self,
        action_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<ActionDetail, RuntimeError> {
        let action = self
            .store
            .get_action(action_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {action_id}")))?;
        let caller = self.resolve_openclaw_caller(&action.initiator_owner, &context.caller)?;
        if !self.action_visible_to_openclaw(&caller, &action) {
            return Err(RuntimeError::Forbidden(format!(
                "openclaw caller cannot inspect action: {action_id}"
            )));
        }

        self.inspect_action(action_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {action_id}")))
    }

    pub(crate) async fn list_openclaw_action_events(
        &self,
        action_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<ActionEventsResponse, RuntimeError> {
        self.inspect_openclaw_action(action_id, context).await?;
        self.list_action_events(action_id)
            .await
            .map_err(RuntimeError::Internal)
    }

    pub(crate) async fn inspect_openclaw_agent_status(
        &self,
        agent_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<OpenClawAgentStatusResponse, RuntimeError> {
        let manifest = self
            .store
            .get_agent_manifest(agent_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("agent not found: {agent_id}")))?;
        let caller = self.resolve_openclaw_caller(&manifest.owner, &context.caller)?;
        if !self.agent_visible_to_openclaw(&caller, &manifest) {
            return Err(RuntimeError::Forbidden(format!(
                "openclaw caller cannot inspect agent: {agent_id}"
            )));
        }
        let detail = self
            .inspect_agent(agent_id)
            .await
            .map_err(RuntimeError::Internal)?
            .ok_or_else(|| RuntimeError::NotFound(format!("agent not found: {agent_id}")))?;
        Ok(OpenClawAgentStatusResponse {
            agent_id: detail.lifecycle.agent_id,
            desired_state: agent_state_name(&detail.lifecycle.desired_state).to_string(),
            observed_state: agent_state_name(&detail.lifecycle.observed_state).to_string(),
            health: health_status_name(&detail.lifecycle.health).to_string(),
            transition_reason: detail.lifecycle.transition_reason,
            last_transition_at: detail.lifecycle.last_transition_at,
            degradation_profile: detail
                .lifecycle
                .degradation_profile
                .as_ref()
                .map(degraded_profile_name)
                .map(str::to_string),
            continuity_mode: detail
                .lifecycle
                .continuity_mode
                .as_ref()
                .map(continuity_mode_name)
                .map(str::to_string),
        })
    }
}

impl Supervisor {
    pub(crate) async fn sync_remote_review_state(
        &self,
        action: &Action,
        remote_evidence_ref: Option<&str>,
        disposition: &RemoteReviewDisposition,
    ) -> anyhow::Result<()> {
        if let Some(bundle_id) = remote_evidence_ref {
            if let Some(mut bundle) = self.store.get_remote_evidence_bundle(bundle_id).await? {
                bundle.remote_review_disposition = Some(disposition.clone());
                self.store.insert_remote_evidence_bundle(&bundle).await?;
            }
        }

        if let Some(mut trace) = self.store.get_trace_bundle(&action.id).await? {
            trace.remote_review_disposition = Some(disposition.clone());
            self.store.put_trace_bundle(&trace).await?;
        }

        Ok(())
    }

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

    pub(crate) fn builtin_federation_pack_template(&self) -> FederationPack {
        FederationPack {
            id: "remote_task_plan_default".to_string(),
            title: "Remote task.plan federation pack".to_string(),
            summary: "Built-in remote governance pack for proposal-only task planning over a treaty-governed frontier.".to_string(),
            treaty_pack_id: "<binding treaty>".to_string(),
            review_defaults: crawfish_types::FederationReviewDefaults {
                enabled: true,
                priority: "high".to_string(),
            },
            alert_defaults: crawfish_types::FederationAlertDefaults {
                rules: vec!["frontier_gap_detected".to_string()],
            },
            required_remote_evidence: vec![
                crawfish_types::TreatyEvidenceRequirement::DelegationReceiptPresent,
                crawfish_types::TreatyEvidenceRequirement::RemoteTaskRefPresent,
                crawfish_types::TreatyEvidenceRequirement::TerminalStateVerified,
                crawfish_types::TreatyEvidenceRequirement::ArtifactClassesAllowed,
                crawfish_types::TreatyEvidenceRequirement::DataScopesAllowed,
            ],
            result_acceptance_policy: RemoteResultAcceptance::Accepted,
            scope_violation_policy: RemoteResultAcceptance::Rejected,
            evidence_gap_policy: RemoteResultAcceptance::ReviewRequired,
            blocked_remote_policy: RemoteStateDisposition::Blocked,
            auth_required_policy: RemoteStateDisposition::AwaitingApproval,
            remote_failure_policy: RemoteStateDisposition::Failed,
            required_checkpoints: vec![
                OversightCheckpoint::Admission,
                OversightCheckpoint::PreDispatch,
                OversightCheckpoint::PostResult,
            ],
            followup_allowed: true,
            max_followup_attempts: 2,
            followup_review_priority: "high".to_string(),
            max_delegation_depth: 1,
        }
    }

    pub(crate) fn resolve_federation_pack_by_id(
        &self,
        pack_id: &str,
        treaty_pack: Option<&crawfish_types::TreatyPack>,
    ) -> Option<FederationPack> {
        if let Some(pack) = self.config.federation.packs.get(pack_id).cloned() {
            return Some(pack);
        }
        if pack_id == "remote_task_plan_default" {
            return Some(match treaty_pack {
                Some(treaty_pack) => self.builtin_federation_pack(treaty_pack),
                None => self.builtin_federation_pack_template(),
            });
        }
        None
    }

    pub(crate) fn remote_followup_reasons_for_bundle(
        &self,
        bundle: &RemoteEvidenceBundle,
    ) -> Vec<RemoteFollowupReason> {
        let mut reasons = Vec::new();
        if matches!(
            bundle.remote_evidence_status,
            Some(RemoteEvidenceStatus::MissingRequiredEvidence)
        ) {
            reasons.push(RemoteFollowupReason::MissingTreatyEvidence);
        }
        if bundle.evidence_items.iter().any(|item| {
            !item.satisfied && matches!(item.checkpoint, Some(OversightCheckpoint::PostResult))
        }) {
            reasons.push(RemoteFollowupReason::PostResultCheckpointGap);
        }
        if matches!(
            bundle.remote_evidence_status,
            Some(RemoteEvidenceStatus::ScopeViolation)
        ) {
            reasons.push(RemoteFollowupReason::ScopeDataAmbiguity);
        }
        if bundle
            .evidence_items
            .iter()
            .any(|item| item.id == "artifact_classes_allowed" && !item.satisfied)
        {
            reasons.push(RemoteFollowupReason::ArtifactAdmissibilityAmbiguity);
        }
        if reasons.is_empty() {
            reasons.push(RemoteFollowupReason::OperatorRequestedClarification);
        }
        let mut deduped = Vec::new();
        for reason in reasons {
            if !deduped.contains(&reason) {
                deduped.push(reason);
            }
        }
        deduped
    }

    pub(crate) fn requested_evidence_for_bundle(
        &self,
        bundle: &RemoteEvidenceBundle,
    ) -> Vec<String> {
        let mut requested = bundle
            .evidence_items
            .iter()
            .filter(|item| !item.satisfied)
            .map(|item| item.summary.clone())
            .collect::<Vec<_>>();
        requested.extend(
            bundle
                .policy_incidents
                .iter()
                .filter(|incident| {
                    matches!(
                        incident.reason_code.as_str(),
                        "frontier_enforcement_gap" | "treaty_scope_violation"
                    )
                })
                .map(|incident| incident.summary.clone()),
        );
        requested.sort();
        requested.dedup();
        if requested.is_empty() {
            requested.push("provide admissibility evidence for the remote result".to_string());
        }
        requested
    }

    pub(crate) async fn create_remote_followup_request(
        &self,
        action: &Action,
        bundle: &RemoteEvidenceBundle,
        reason_code: String,
        operator_note: Option<String>,
    ) -> anyhow::Result<RemoteFollowupRequest> {
        let treaty_pack_id = bundle
            .treaty_pack_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("remote follow-up requires treaty pack lineage"))?;
        let federation_pack_id = bundle
            .federation_pack_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("remote follow-up requires federation pack lineage"))?;
        let remote_principal = bundle
            .remote_principal
            .clone()
            .ok_or_else(|| anyhow::anyhow!("remote follow-up requires remote principal lineage"))?;
        let timestamp = now_timestamp();
        let request = RemoteFollowupRequest {
            id: Uuid::new_v4().to_string(),
            action_id: action.id.clone(),
            remote_evidence_ref: bundle.id.clone(),
            treaty_pack_id,
            federation_pack_id,
            remote_principal,
            remote_task_ref: bundle.remote_task_ref.clone(),
            reason_code,
            reasons: self.remote_followup_reasons_for_bundle(bundle),
            requested_evidence: self.requested_evidence_for_bundle(bundle),
            operator_note,
            status: RemoteFollowupStatus::Open,
            dispatched_at: None,
            dispatched_by: None,
            closed_at: None,
            superseded_by: None,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        self.store.upsert_remote_followup_request(&request).await?;
        Ok(request)
    }

    pub(crate) async fn close_active_remote_followup_request(
        &self,
        action: &Action,
    ) -> anyhow::Result<Option<RemoteFollowupRequest>> {
        let Some(followup_id) = active_remote_followup_ref_for_action(action) else {
            return Ok(None);
        };
        let Some(mut followup) = self.store.get_remote_followup_request(&followup_id).await? else {
            return Ok(None);
        };
        if matches!(
            followup.status,
            RemoteFollowupStatus::Open | RemoteFollowupStatus::Dispatched
        ) {
            followup.status = RemoteFollowupStatus::Closed;
            followup.closed_at = Some(now_timestamp());
            followup.updated_at = now_timestamp();
            self.store.upsert_remote_followup_request(&followup).await?;
        }
        Ok(Some(followup))
    }

    pub(crate) async fn list_federation_packs(&self) -> anyhow::Result<FederationPackListResponse> {
        let mut packs = self
            .config
            .federation
            .packs
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if !packs
            .iter()
            .any(|pack| pack.id == "remote_task_plan_default")
        {
            packs.push(self.builtin_federation_pack_template());
        }
        Ok(FederationPackListResponse { packs })
    }

    pub(crate) async fn get_federation_pack(
        &self,
        pack_id: &str,
    ) -> anyhow::Result<Option<FederationPackDetailResponse>> {
        Ok(self
            .resolve_federation_pack_by_id(pack_id, None)
            .map(|pack| FederationPackDetailResponse { pack }))
    }
}

#[async_trait::async_trait]
impl SupervisorControl for Supervisor {
    async fn list_status(&self) -> anyhow::Result<SwarmStatusResponse> {
        Ok(SwarmStatusResponse {
            agents: self.store.list_lifecycle_records().await?,
            queue: self.store.queue_summary().await?,
        })
    }

    async fn list_actions(&self, phase: Option<&str>) -> anyhow::Result<ActionListResponse> {
        let actions = self
            .store
            .list_actions_by_phase(phase)
            .await?
            .into_iter()
            .map(|action| ActionSummary {
                id: action.id,
                target_agent_id: action.target_agent_id,
                capability: action.capability,
                phase: action_phase_name(&action.phase).to_string(),
                created_at: action.created_at,
                failure_reason: action.failure_reason,
                encounter_ref: action.encounter_ref,
                lease_ref: action.lease_ref,
            })
            .collect();
        Ok(ActionListResponse { actions })
    }

    async fn list_action_events(&self, action_id: &str) -> anyhow::Result<ActionEventsResponse> {
        Ok(ActionEventsResponse {
            events: self.store.list_action_events(action_id).await?,
        })
    }

    async fn get_action_trace(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<ActionTraceResponse>> {
        Ok(self
            .store
            .get_trace_bundle(action_id)
            .await?
            .map(|trace| ActionTraceResponse { trace }))
    }

    async fn list_action_evaluations(
        &self,
        action_id: &str,
    ) -> anyhow::Result<ActionEvaluationsResponse> {
        Ok(ActionEvaluationsResponse {
            evaluations: self.store.list_evaluations(action_id).await?,
        })
    }

    async fn get_action_remote_evidence(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<ActionRemoteEvidenceResponse>> {
        if self.store.get_action(action_id).await?.is_none() {
            return Ok(None);
        }
        Ok(Some(ActionRemoteEvidenceResponse {
            bundles: self.store.list_remote_evidence_bundles(action_id).await?,
        }))
    }

    async fn get_action_remote_followups(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<ActionRemoteFollowupsResponse>> {
        if self.store.get_action(action_id).await?.is_none() {
            return Ok(None);
        }
        Ok(Some(ActionRemoteFollowupsResponse {
            followups: self.store.list_remote_followup_requests(action_id).await?,
            attempts: self.store.list_remote_attempt_records(action_id).await?,
        }))
    }

    async fn list_review_queue(&self) -> anyhow::Result<ReviewQueueResponse> {
        Ok(ReviewQueueResponse {
            items: self.store.list_review_queue_items().await?,
        })
    }

    async fn list_evaluation_datasets(&self) -> anyhow::Result<EvaluationDatasetsResponse> {
        let datasets = self.evaluation_datasets();
        let mut items = Vec::new();
        for (name, config) in datasets {
            let case_count = self.store.list_dataset_cases(&name).await?.len() as u64;
            items.push(crawfish_core::EvaluationDatasetSummary {
                name,
                capability: config.capability,
                title: config.title,
                auto_capture: config.auto_capture,
                case_count,
            });
        }
        Ok(EvaluationDatasetsResponse { datasets: items })
    }

    async fn get_evaluation_dataset(
        &self,
        dataset_name: &str,
    ) -> anyhow::Result<Option<EvaluationDatasetDetailResponse>> {
        let datasets = self.evaluation_datasets();
        let Some(config) = datasets.get(dataset_name).cloned() else {
            return Ok(None);
        };
        let cases = self.store.list_dataset_cases(dataset_name).await?;
        Ok(Some(EvaluationDatasetDetailResponse {
            name: dataset_name.to_string(),
            config,
            cases,
        }))
    }

    async fn start_evaluation_run(
        &self,
        request: StartEvaluationRunRequest,
    ) -> anyhow::Result<StartEvaluationRunResponse> {
        let detail = self.execute_evaluation_run_internal(request).await?;
        Ok(StartEvaluationRunResponse { run: detail.run })
    }

    async fn get_evaluation_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<ExperimentRunDetailResponse>> {
        let Some(run) = self.store.get_experiment_run(run_id).await? else {
            return Ok(None);
        };
        let cases = self.store.list_experiment_case_results(run_id).await?;
        Ok(Some(ExperimentRunDetailResponse { run, cases }))
    }

    async fn start_pairwise_evaluation_run(
        &self,
        request: StartPairwiseEvaluationRunRequest,
    ) -> anyhow::Result<StartPairwiseEvaluationRunResponse> {
        let detail = self
            .execute_pairwise_evaluation_run_internal(request)
            .await?;
        Ok(StartPairwiseEvaluationRunResponse { run: detail.run })
    }

    async fn get_pairwise_evaluation_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<PairwiseExperimentRunDetailResponse>> {
        let Some(run) = self.store.get_pairwise_experiment_run(run_id).await? else {
            return Ok(None);
        };
        let cases = self.store.list_pairwise_case_results(run_id).await?;
        Ok(Some(PairwiseExperimentRunDetailResponse { run, cases }))
    }

    async fn list_alerts(&self) -> anyhow::Result<AlertListResponse> {
        Ok(AlertListResponse {
            alerts: self.store.list_alert_events().await?,
        })
    }

    async fn list_treaties(&self) -> anyhow::Result<TreatyListResponse> {
        Ok(TreatyListResponse {
            treaties: self.config.treaties.packs.values().cloned().collect(),
        })
    }

    async fn get_treaty(&self, treaty_id: &str) -> anyhow::Result<Option<TreatyDetailResponse>> {
        Ok(self
            .config
            .treaties
            .packs
            .get(treaty_id)
            .cloned()
            .map(|treaty| TreatyDetailResponse { treaty }))
    }

    async fn dispatch_remote_followup(
        &self,
        action_id: &str,
        followup_id: &str,
        request: DispatchRemoteFollowupRequest,
    ) -> anyhow::Result<DispatchRemoteFollowupResponse> {
        let mut action = self
            .store
            .get_action(action_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("action not found: {action_id}"))?;
        let mut followup = self
            .store
            .get_remote_followup_request(followup_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("remote follow-up request not found: {followup_id}"))?;
        if followup.action_id != action_id {
            anyhow::bail!(
                "remote follow-up request {followup_id} does not belong to action {action_id}"
            );
        }
        if !matches!(followup.status, RemoteFollowupStatus::Open) {
            anyhow::bail!("remote follow-up request {followup_id} is not open");
        }
        if !matches!(action.phase, ActionPhase::Blocked) {
            anyhow::bail!("action {action_id} is not blocked for remote follow-up");
        }

        let manifest = self
            .store
            .get_agent_manifest(&action.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", action.target_agent_id))?;
        let Some((adapter, _)) = self.resolve_a2a_adapter(&manifest)? else {
            let incident = PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: "remote_agent_treaty_v1".to_string(),
                jurisdiction: JurisdictionClass::ExternalUnknown,
                reason_code: "followup_dispatch_denied".to_string(),
                summary: "remote follow-up dispatch requires an A2A binding".to_string(),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(OversightCheckpoint::PreDispatch),
                created_at: now_timestamp(),
            };
            self.store.insert_policy_incident(&incident).await?;
            anyhow::bail!("remote follow-up dispatch requires an A2A binding");
        };

        let treaty_pack = adapter.treaty_pack().clone();
        let federation_pack = self.resolve_federation_pack(adapter.binding(), &treaty_pack)?;
        let denial_summary = if treaty_pack.id != followup.treaty_pack_id {
            Some(format!(
                "follow-up treaty mismatch: expected {}, got {}",
                followup.treaty_pack_id, treaty_pack.id
            ))
        } else if treaty_pack.remote_principal != followup.remote_principal {
            Some("follow-up remote principal no longer matches treaty binding".to_string())
        } else if federation_pack.id != followup.federation_pack_id {
            Some(format!(
                "follow-up federation pack mismatch: expected {}, got {}",
                followup.federation_pack_id, federation_pack.id
            ))
        } else if !federation_pack.followup_allowed {
            Some(format!(
                "federation pack {} does not allow remote follow-up dispatch",
                federation_pack.id
            ))
        } else {
            None
        };
        if let Some(summary) = denial_summary {
            let incident = PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: "remote_agent_treaty_v1".to_string(),
                jurisdiction: JurisdictionClass::ExternalUnknown,
                reason_code: "followup_dispatch_denied".to_string(),
                summary: summary.clone(),
                severity: PolicyIncidentSeverity::Critical,
                checkpoint: Some(OversightCheckpoint::PreDispatch),
                created_at: now_timestamp(),
            };
            self.store.insert_policy_incident(&incident).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "policy_incident_recorded",
                    serde_json::json!({
                        "incident_id": incident.id,
                        "reason_code": incident.reason_code,
                        "severity": "critical",
                    }),
                )
                .await?;
            anyhow::bail!(summary);
        }

        let followup_attempt_count = self
            .store
            .list_remote_attempt_records(action_id)
            .await?
            .into_iter()
            .filter(|attempt| attempt.followup_request_ref.is_some())
            .count() as u32;
        if followup_attempt_count >= federation_pack.max_followup_attempts {
            let summary = format!(
                "remote follow-up dispatch exceeds max_followup_attempts ({})",
                federation_pack.max_followup_attempts
            );
            let incident = PolicyIncident {
                id: Uuid::new_v4().to_string(),
                action_id: action.id.clone(),
                doctrine_pack_id: "remote_agent_treaty_v1".to_string(),
                jurisdiction: JurisdictionClass::ExternalUnknown,
                reason_code: "followup_attempt_limit_exceeded".to_string(),
                summary: summary.clone(),
                severity: PolicyIncidentSeverity::Warning,
                checkpoint: Some(OversightCheckpoint::PreDispatch),
                created_at: now_timestamp(),
            };
            self.store.insert_policy_incident(&incident).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "policy_incident_recorded",
                    serde_json::json!({
                        "incident_id": incident.id,
                        "reason_code": incident.reason_code,
                        "severity": "warning",
                    }),
                )
                .await?;
            anyhow::bail!(summary);
        }

        self.compile_treaty_decision(&action, adapter.binding(), &treaty_pack)
            .map_err(|error| anyhow::anyhow!("followup_dispatch_denied: {error}"))?;

        let prior_attempts = self.store.list_remote_attempt_records(action_id).await?;
        let prior_evidence = self.store.list_remote_evidence_bundles(action_id).await?;
        for mut request_to_supersede in self.store.list_remote_followup_requests(action_id).await? {
            if request_to_supersede.id != followup.id
                && matches!(request_to_supersede.status, RemoteFollowupStatus::Open)
            {
                request_to_supersede.status = RemoteFollowupStatus::Superseded;
                request_to_supersede.updated_at = now_timestamp();
                request_to_supersede.superseded_by = Some(followup.id.clone());
                self.store
                    .upsert_remote_followup_request(&request_to_supersede)
                    .await?;
            }
        }

        let dispatched_at = now_timestamp();
        followup.status = RemoteFollowupStatus::Dispatched;
        followup.dispatched_at = Some(dispatched_at.clone());
        followup.dispatched_by = Some(request.dispatcher_ref.clone());
        if let Some(note) = request.note.clone() {
            followup.operator_note = Some(note);
        }
        followup.updated_at = dispatched_at;
        self.store.upsert_remote_followup_request(&followup).await?;

        action.phase = ActionPhase::Accepted;
        action.started_at = None;
        action.finished_at = None;
        action.failure_reason = None;
        action.failure_code = None;
        action.checkpoint_ref = None;
        action.selected_executor = None;
        action.recovery_stage = None;
        action.continuity_mode = None;
        action.degradation_profile = None;
        action.lock_detail = None;
        action.external_refs = Vec::new();
        action.outputs = ActionOutputs::default();
        action.inputs.insert(
            "remote_followup".to_string(),
            serde_json::json!({
                "request_id": followup.id,
                "reason_code": followup.reason_code,
                "requested_evidence": followup.requested_evidence,
                "operator_note": followup.operator_note,
                "prior_remote_attempt_refs": prior_attempts
                    .iter()
                    .map(|attempt| attempt.id.clone())
                    .collect::<Vec<_>>(),
                "prior_remote_evidence_refs": prior_evidence
                    .iter()
                    .map(|bundle| bundle.id.clone())
                    .collect::<Vec<_>>(),
                "previous_remote_task_ref": followup.remote_task_ref,
            }),
        );
        self.store.upsert_action(&action).await?;
        self.store
            .append_action_event(
                &action.id,
                "remote_followup_dispatched",
                serde_json::json!({
                    "followup_request_id": followup.id,
                    "dispatcher_ref": request.dispatcher_ref,
                    "requested_evidence": followup.requested_evidence,
                }),
            )
            .await?;

        Ok(DispatchRemoteFollowupResponse {
            action: SubmittedAction {
                action_id: action.id,
                phase: "accepted".to_string(),
            },
            followup,
        })
    }

    async fn acknowledge_alert(
        &self,
        alert_id: &str,
        request: AcknowledgeAlertRequest,
    ) -> anyhow::Result<AcknowledgeAlertResponse> {
        let mut alert = self
            .store
            .list_alert_events()
            .await?
            .into_iter()
            .find(|alert| alert.id == alert_id)
            .ok_or_else(|| anyhow::anyhow!("alert not found: {alert_id}"))?;
        alert.acknowledged_at = Some(now_timestamp());
        alert.acknowledged_by = Some(request.actor);
        self.store.acknowledge_alert_event(&alert).await?;
        Ok(AcknowledgeAlertResponse { alert })
    }

    async fn resolve_review_queue_item(
        &self,
        review_id: &str,
        request: ResolveReviewQueueItemRequest,
    ) -> anyhow::Result<ResolveReviewQueueItemResponse> {
        let mut item = self
            .store
            .list_review_queue_items()
            .await?
            .into_iter()
            .find(|item| item.id == review_id)
            .ok_or_else(|| anyhow::anyhow!("review item not found: {review_id}"))?;
        item.status = ReviewQueueStatus::Resolved;
        item.resolved_at = Some(now_timestamp());
        item.resolution = Some(request.resolution.clone());
        if item.kind == ReviewQueueKind::RemoteResultReview {
            let mut action = self
                .store
                .get_action(&item.action_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("action not found: {}", item.action_id))?;
            match request.resolution.as_str() {
                "accept_result" => {
                    if !matches!(
                        remote_outcome_disposition_for_action(&action),
                        Some(RemoteOutcomeDisposition::ReviewRequired)
                    ) {
                        anyhow::bail!(
                            "accept_result is only valid for remote outcomes requiring review"
                        );
                    }
                    action.phase = ActionPhase::Completed;
                    action.finished_at = Some(now_timestamp());
                    action.failure_reason = None;
                    action.failure_code = None;
                    set_remote_review_disposition_metadata(
                        &mut action.outputs,
                        &RemoteReviewDisposition::Accepted,
                    );
                    item.remote_review_disposition = Some(RemoteReviewDisposition::Accepted);
                    self.sync_remote_review_state(
                        &action,
                        item.remote_evidence_ref.as_deref(),
                        &RemoteReviewDisposition::Accepted,
                    )
                    .await?;
                    self.store.upsert_action(&action).await?;
                    self.store
                        .append_action_event(
                            &action.id,
                            "remote_review_resolved",
                            serde_json::json!({
                                "review_id": item.id,
                                "resolution": "accept_result",
                            }),
                        )
                        .await?;
                }
                "reject_result" => {
                    set_action_failed(
                        &mut action,
                        "remote_result_rejected",
                        "remote result rejected during operator review".to_string(),
                    );
                    action.finished_at = Some(now_timestamp());
                    set_remote_review_disposition_metadata(
                        &mut action.outputs,
                        &RemoteReviewDisposition::Rejected,
                    );
                    item.remote_review_disposition = Some(RemoteReviewDisposition::Rejected);
                    self.sync_remote_review_state(
                        &action,
                        item.remote_evidence_ref.as_deref(),
                        &RemoteReviewDisposition::Rejected,
                    )
                    .await?;
                    self.store.upsert_action(&action).await?;
                    self.store
                        .append_action_event(
                            &action.id,
                            "remote_review_resolved",
                            serde_json::json!({
                                "review_id": item.id,
                                "resolution": "reject_result",
                            }),
                        )
                        .await?;
                }
                "needs_followup" => {
                    let treaty_summary =
                        external_ref_value(&action.external_refs, "a2a.treaty_pack").and_then(
                            |treaty_id| self.config.treaties.packs.get(&treaty_id).cloned(),
                        );
                    if let Some(federation_pack_id) = federation_pack_id_for_action(&action) {
                        if let Some(federation_pack) = self.resolve_federation_pack_by_id(
                            &federation_pack_id,
                            treaty_summary.as_ref(),
                        ) {
                            if !federation_pack.followup_allowed {
                                anyhow::bail!(
                                    "federation pack {} does not allow remote follow-up dispatch",
                                    federation_pack.id
                                );
                            }
                        }
                    }
                    action.phase = ActionPhase::Blocked;
                    action.finished_at = None;
                    action.failure_reason =
                        Some("remote result requires follow-up review".to_string());
                    action.failure_code = Some("remote_review_followup".to_string());
                    set_remote_review_disposition_metadata(
                        &mut action.outputs,
                        &RemoteReviewDisposition::NeedsFollowup,
                    );
                    item.remote_review_disposition = Some(RemoteReviewDisposition::NeedsFollowup);
                    self.sync_remote_review_state(
                        &action,
                        item.remote_evidence_ref.as_deref(),
                        &RemoteReviewDisposition::NeedsFollowup,
                    )
                    .await?;
                    self.store.upsert_action(&action).await?;
                    self.store
                        .append_action_event(
                            &action.id,
                            "remote_review_resolved",
                            serde_json::json!({
                                "review_id": item.id,
                                "resolution": "needs_followup",
                            }),
                        )
                        .await?;
                    let remote_evidence_ref =
                        item.remote_evidence_ref.clone().ok_or_else(|| {
                            anyhow::anyhow!("needs_followup requires remote evidence lineage")
                        })?;
                    let remote_bundle = self
                        .store
                        .get_remote_evidence_bundle(&remote_evidence_ref)
                        .await?
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "remote evidence bundle not found: {remote_evidence_ref}"
                            )
                        })?;
                    let followup = self
                        .create_remote_followup_request(
                            &action,
                            &remote_bundle,
                            "remote_result_followup".to_string(),
                            request.note.clone(),
                        )
                        .await?;
                    self.store
                        .append_action_event(
                            &action.id,
                            "remote_followup_requested",
                            serde_json::json!({
                                "followup_request_id": followup.id,
                                "remote_evidence_ref": followup.remote_evidence_ref,
                                "reason_code": followup.reason_code,
                            }),
                        )
                        .await?;
                }
                other => anyhow::bail!("unsupported remote review resolution: {other}"),
            }
        }
        self.store.resolve_review_queue_item(&item).await?;
        if let Some(pairwise_case_ref) = &item.pairwise_case_ref {
            if let Some(mut pairwise_case) = self
                .store
                .get_pairwise_case_result(pairwise_case_ref)
                .await?
            {
                pairwise_case.review_resolution = item.resolution.clone();
                self.store
                    .update_pairwise_case_result(&pairwise_case)
                    .await?;
            }
        }
        let feedback_body = request.note.clone().or_else(|| {
            if item.kind == ReviewQueueKind::RemoteResultReview
                && request.resolution == "needs_followup"
            {
                Some("remote result requires follow-up review".to_string())
            } else {
                None
            }
        });
        if let Some(note) = feedback_body {
            let feedback = FeedbackNote {
                id: Uuid::new_v4().to_string(),
                action_id: item.action_id.clone(),
                source: request.resolver_ref.clone(),
                body: note,
                pairwise_case_result_ref: item.pairwise_case_ref.clone(),
                created_at: now_timestamp(),
            };
            self.store.insert_feedback_note(&feedback).await?;
            if let Some(pairwise_case_ref) = &item.pairwise_case_ref {
                if let Some(mut pairwise_case) = self
                    .store
                    .get_pairwise_case_result(pairwise_case_ref)
                    .await?
                {
                    pairwise_case.feedback_note_id = Some(feedback.id.clone());
                    pairwise_case.review_resolution = item.resolution.clone();
                    self.store
                        .update_pairwise_case_result(&pairwise_case)
                        .await?;
                }
            }
            if let Some(evaluation_id) = &item.evaluation_ref {
                if let Some(mut evaluation) = self
                    .store
                    .list_evaluations(&item.action_id)
                    .await?
                    .into_iter()
                    .find(|evaluation| &evaluation.id == evaluation_id)
                {
                    evaluation.feedback_note_id = Some(feedback.id.clone());
                    self.store.insert_evaluation(&evaluation).await?;
                }
            }
        }
        Ok(ResolveReviewQueueItemResponse { item })
    }

    async fn inspect_agent(&self, agent_id: &str) -> anyhow::Result<Option<AgentDetail>> {
        let manifest = self.store.get_agent_manifest(agent_id).await?;
        let lifecycle = self.store.get_lifecycle_record(agent_id).await?;
        Ok(match (manifest, lifecycle) {
            (Some(manifest), Some(lifecycle)) => Some(AgentDetail {
                manifest,
                lifecycle,
            }),
            _ => None,
        })
    }

    async fn inspect_action(&self, action_id: &str) -> anyhow::Result<Option<ActionDetail>> {
        let Some(action) = self.store.get_action(action_id).await? else {
            return Ok(None);
        };
        let checkpoint = self.load_deterministic_checkpoint(&action).await?;
        let encounter = if let Some(encounter_ref) = &action.encounter_ref {
            self.store.get_encounter(encounter_ref).await?
        } else {
            None
        };
        let audit_receipt = if let Some(receipt_ref) = &action.audit_receipt_ref {
            self.store.get_audit_receipt(receipt_ref).await?
        } else {
            None
        };
        let grant_details = self.store.list_consent_grants(&action.grant_refs).await?;
        let lease_detail = if let Some(lease_ref) = &action.lease_ref {
            self.store.get_capability_lease(lease_ref).await?
        } else {
            None
        };
        let strategy_mode = action
            .execution_strategy
            .as_ref()
            .map(|strategy| strategy.mode.clone());
        let strategy_iteration = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.strategy_state.as_ref())
            .map(|state| state.iteration);
        let verification_summary = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.strategy_state.as_ref())
            .and_then(|state| state.verification_summary.clone())
            .or_else(|| {
                action
                    .outputs
                    .metadata
                    .get("verification_summary")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
            });
        let stored_policy_incidents = self.store.list_policy_incidents(action_id).await?;
        let latest_evaluation = self
            .store
            .list_evaluations(action_id)
            .await?
            .into_iter()
            .last();
        let interaction_model = Some(interaction_model_for_action(&action, encounter.as_ref()));
        let jurisdiction_class = Some(jurisdiction_class_for_action(&action, encounter.as_ref()));
        let doctrine_summary = Some(default_doctrine_pack(
            &action,
            interaction_model.as_ref().expect("interaction model"),
            jurisdiction_class.clone().expect("jurisdiction class"),
        ));
        let trace_bundle = self.store.get_trace_bundle(action_id).await?;
        let review_queue_items = self
            .store
            .list_review_queue_items()
            .await?
            .into_iter()
            .filter(|item| item.action_id == action.id)
            .collect::<Vec<_>>();
        let alert_events = self
            .store
            .list_alert_events()
            .await?
            .into_iter()
            .filter(|alert| alert.action_id == action.id)
            .collect::<Vec<_>>();
        let resolved_profile = self.resolve_evaluation_profile(&action)?;
        let checkpoint_status = checkpoint_status_for_action(
            &action,
            doctrine_summary.as_ref().expect("doctrine summary"),
            trace_bundle.is_some(),
            latest_evaluation.as_ref(),
            resolved_profile.is_some(),
        );
        let mut policy_incidents = stored_policy_incidents;
        let derived_policy_incidents = self.policy_incidents_for_action(
            &action,
            resolved_profile.as_ref(),
            latest_evaluation.as_ref(),
            &checkpoint_status,
        );
        for incident in derived_policy_incidents {
            let exists = policy_incidents.iter().any(|existing| {
                existing.reason_code == incident.reason_code
                    && existing.summary == incident.summary
                    && existing.checkpoint == incident.checkpoint
            });
            if !exists {
                policy_incidents.push(incident);
            }
        }
        let delegation_receipt_ref =
            external_ref_value(&action.external_refs, "a2a.delegation_receipt");
        let delegation_receipt = if let Some(receipt_ref) = delegation_receipt_ref.as_deref() {
            self.store.get_delegation_receipt(receipt_ref).await?
        } else {
            None
        };
        let treaty_summary = external_ref_value(&action.external_refs, "a2a.treaty_pack")
            .and_then(|treaty_id| self.config.treaties.packs.get(&treaty_id).cloned());
        let treaty_pack_id = treaty_summary
            .as_ref()
            .map(|treaty| treaty.id.clone())
            .or_else(|| external_ref_value(&action.external_refs, "a2a.treaty_pack"));
        let federation_pack_id = federation_pack_id_for_action(&action);
        let federation_summary = federation_pack_id.as_ref().and_then(|pack_id| {
            self.resolve_federation_pack_by_id(pack_id, treaty_summary.as_ref())
        });
        let federation_decision = federation_decision_for_action(&action);
        let remote_task_ref = external_ref_value(&action.external_refs, "a2a.task_id");
        let remote_principal = delegation_receipt
            .as_ref()
            .map(|receipt| receipt.remote_principal.clone());
        let remote_evidence_bundles = self.store.list_remote_evidence_bundles(action_id).await?;
        let remote_followups = self.store.list_remote_followup_requests(action_id).await?;
        let remote_attempts = self.store.list_remote_attempt_records(action_id).await?;
        let latest_remote_evidence = remote_evidence_bundles.last().cloned();
        let pending_remote_review_ref = review_queue_items
            .iter()
            .find(|item| {
                item.kind == ReviewQueueKind::RemoteResultReview
                    && item.status == ReviewQueueStatus::Open
            })
            .map(|item| item.id.clone());
        let active_remote_followup_ref = remote_followups
            .iter()
            .find(|followup| followup.status == RemoteFollowupStatus::Open)
            .map(|followup| followup.id.clone());
        Ok(Some(ActionDetail {
            artifact_refs: action.outputs.artifacts.clone(),
            selected_executor: action.selected_executor.clone(),
            recovery_stage: action.recovery_stage.clone(),
            external_refs: action.external_refs.clone(),
            strategy_mode,
            strategy_iteration,
            verification_summary,
            grant_details,
            lease_detail,
            blocked_reason: if matches!(action.phase, ActionPhase::Blocked) {
                action.failure_reason.clone()
            } else {
                None
            },
            terminal_code: action.failure_code.clone(),
            lock_detail: action.lock_detail.clone(),
            interaction_model,
            jurisdiction_class,
            doctrine_summary,
            checkpoint_status,
            policy_incidents,
            latest_evaluation,
            evaluation_profile: resolved_profile.map(|profile| profile.name),
            trace_ref: trace_bundle.as_ref().map(|trace| trace.id.clone()),
            review_queue_refs: review_queue_items
                .iter()
                .map(|item| item.id.clone())
                .collect(),
            alert_refs: alert_events.iter().map(|alert| alert.id.clone()).collect(),
            remote_principal,
            treaty_pack_id,
            treaty_summary,
            federation_pack_id,
            federation_summary,
            federation_decision,
            delegation_receipt_ref,
            remote_evidence_ref: latest_remote_evidence
                .as_ref()
                .map(|bundle| bundle.id.clone()),
            remote_task_ref,
            remote_outcome_disposition: remote_outcome_disposition_for_action(&action),
            remote_evidence_status: remote_evidence_status_for_action(&action),
            remote_review_disposition: remote_review_disposition_for_action(&action).or_else(
                || {
                    latest_remote_evidence
                        .as_ref()
                        .and_then(|bundle| bundle.remote_review_disposition.clone())
                },
            ),
            pending_remote_review_ref,
            remote_followup_refs: remote_followups
                .iter()
                .map(|followup| followup.id.clone())
                .collect(),
            active_remote_followup_ref,
            remote_attempt_count: remote_attempts.len() as u32,
            latest_remote_attempt_ref: remote_attempts.last().map(|attempt| attempt.id.clone()),
            remote_state_disposition: remote_state_disposition_for_action(&action),
            treaty_violations: treaty_violations_for_action(&action),
            delegation_depth: delegation_depth_for_action(&action),
            delegation_receipt,
            action,
            encounter,
            latest_audit_receipt: audit_receipt,
        }))
    }

    async fn submit_action(&self, request: SubmitActionRequest) -> anyhow::Result<SubmittedAction> {
        let request = normalize_submit_request(request);
        let (manifest, compiled, encounter_request, decision, requires_approval) =
            self.preflight_submission(&request).await?;
        let encounter_state = if matches!(decision.disposition, EncounterDisposition::Deny) {
            EncounterState::Denied
        } else if requires_approval
            || matches!(decision.disposition, EncounterDisposition::AwaitConsent)
        {
            EncounterState::AwaitingConsent
        } else {
            EncounterState::Leased
        };
        if matches!(decision.disposition, EncounterDisposition::Deny) {
            anyhow::bail!(decision.reason);
        }
        let mut encounter = self
            .create_encounter(&manifest, &encounter_request, &decision, encounter_state)
            .await?;

        let created_at = now_timestamp();
        let encounter_id = encounter.id.clone();
        let mut action = Action {
            id: Uuid::new_v4().to_string(),
            target_agent_id: request.target_agent_id,
            requester: request.requester,
            initiator_owner: request.initiator_owner,
            counterparty_refs: request.counterparty_refs,
            goal: request.goal,
            capability: request.capability,
            inputs: request.inputs,
            contract: compiled.contract,
            execution_strategy: compiled.strategy,
            grant_refs: Vec::new(),
            lease_ref: None,
            encounter_ref: Some(encounter_id),
            audit_receipt_ref: None,
            data_boundary: request.data_boundary.unwrap_or_else(|| {
                manifest
                    .default_data_boundaries
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "owner_local".to_string())
            }),
            schedule: request.schedule.unwrap_or_default(),
            phase: if requires_approval
                || matches!(decision.disposition, EncounterDisposition::AwaitConsent)
            {
                ActionPhase::AwaitingApproval
            } else {
                ActionPhase::Accepted
            },
            created_at,
            started_at: None,
            finished_at: None,
            checkpoint_ref: None,
            continuity_mode: None,
            degradation_profile: None,
            failure_reason: None,
            failure_code: if requires_approval
                || matches!(decision.disposition, EncounterDisposition::AwaitConsent)
            {
                Some(failure_code_approval_required().to_string())
            } else {
                None
            },
            selected_executor: None,
            recovery_stage: None,
            lock_detail: None,
            external_refs: Vec::new(),
            outputs: ActionOutputs::default(),
        };

        if matches!(action.phase, ActionPhase::Accepted) {
            let (grant, lease, receipt) = self
                .issue_grant_and_lease(
                    &action,
                    &manifest,
                    &mut encounter,
                    None,
                    decision.reason.clone(),
                )
                .await?;
            action.grant_refs = vec![grant.id];
            action.lease_ref = Some(lease.id);
            action.audit_receipt_ref = Some(receipt.id);
        }

        self.store.upsert_action(&action).await?;
        self.store
            .append_action_event(
                &action.id,
                if matches!(action.phase, ActionPhase::AwaitingApproval) {
                    "awaiting_approval"
                } else {
                    "accepted"
                },
                serde_json::json!({
                    "phase": match action.phase {
                        ActionPhase::AwaitingApproval => "awaiting_approval",
                        _ => "accepted",
                    },
                    "code": action.failure_code,
                    "target_agent_id": action.target_agent_id,
                    "encounter_ref": action.encounter_ref,
                    "audit_receipt_ref": action.audit_receipt_ref,
                    "grant_refs": action.grant_refs,
                    "lease_ref": action.lease_ref,
                }),
            )
            .await?;

        Ok(SubmittedAction {
            action_id: action.id,
            phase: match action.phase {
                ActionPhase::AwaitingApproval => "awaiting_approval".to_string(),
                _ => "accepted".to_string(),
            },
        })
    }

    async fn approve_action(
        &self,
        action_id: &str,
        request: ApproveActionRequest,
    ) -> anyhow::Result<SubmittedAction> {
        let mut action = self
            .store
            .get_action(action_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("action not found: {action_id}"))?;
        if !matches!(action.phase, ActionPhase::AwaitingApproval) {
            anyhow::bail!("action {action_id} is not awaiting approval");
        }
        let encounter_ref = action
            .encounter_ref
            .clone()
            .ok_or_else(|| anyhow::anyhow!("action {action_id} is missing encounter_ref"))?;
        let mut encounter = self
            .store
            .get_encounter(&encounter_ref)
            .await?
            .ok_or_else(|| anyhow::anyhow!("encounter not found: {encounter_ref}"))?;
        let manifest = self
            .store
            .get_agent_manifest(&action.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", action.target_agent_id))?;

        let reason = request
            .note
            .as_ref()
            .map(|note| format!("action approved: {note}"))
            .unwrap_or_else(|| "action approved by operator".to_string());
        let (grant, lease, receipt) = self
            .issue_grant_and_lease(
                &action,
                &manifest,
                &mut encounter,
                Some(request.approver_ref),
                reason,
            )
            .await?;

        action.grant_refs = vec![grant.id];
        action.lease_ref = Some(lease.id);
        action.audit_receipt_ref = Some(receipt.id);
        action.phase = ActionPhase::Accepted;
        action.failure_reason = None;
        action.failure_code = None;
        self.store.upsert_action(&action).await?;
        self.store
            .append_action_event(
                &action.id,
                "approved",
                serde_json::json!({
                    "phase": "accepted",
                    "code": serde_json::Value::Null,
                    "grant_refs": action.grant_refs,
                    "lease_ref": action.lease_ref,
                }),
            )
            .await?;

        Ok(SubmittedAction {
            action_id: action.id,
            phase: "accepted".to_string(),
        })
    }

    async fn reject_action(
        &self,
        action_id: &str,
        request: RejectActionRequest,
    ) -> anyhow::Result<SubmittedAction> {
        let mut action = self
            .store
            .get_action(action_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("action not found: {action_id}"))?;
        if !matches!(action.phase, ActionPhase::AwaitingApproval) {
            anyhow::bail!("action {action_id} is not awaiting approval");
        }
        let encounter_ref = action
            .encounter_ref
            .clone()
            .ok_or_else(|| anyhow::anyhow!("action {action_id} is missing encounter_ref"))?;
        if let Some(mut encounter) = self.store.get_encounter(&encounter_ref).await? {
            encounter.state = EncounterState::Denied;
            self.store.insert_encounter(&encounter).await?;
        }
        let receipt = self
            .emit_audit_receipt(
                &encounter_ref,
                action.grant_refs.clone(),
                action.lease_ref.clone(),
                AuditOutcome::Denied,
                request.reason.clone(),
                Some(request.approver_ref),
            )
            .await?;
        set_action_failed(
            &mut action,
            failure_code_approval_rejected(),
            "approval rejected".to_string(),
        );
        action.audit_receipt_ref = Some(receipt.id);
        self.store.upsert_action(&action).await?;
        self.store
            .append_action_event(
                &action.id,
                "rejected",
                serde_json::json!({
                    "phase": "failed",
                    "code": action.failure_code,
                    "reason": action.failure_reason,
                    "finished_at": action.finished_at,
                }),
            )
            .await?;

        Ok(SubmittedAction {
            action_id: action.id,
            phase: "failed".to_string(),
        })
    }

    async fn revoke_lease(
        &self,
        lease_id: &str,
        request: RevokeLeaseRequest,
    ) -> anyhow::Result<AdminActionResponse> {
        let mut lease = self
            .store
            .get_capability_lease(lease_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("capability lease not found: {lease_id}"))?;
        lease.revocation_reason = Some(request.reason.clone());
        self.store.upsert_capability_lease(&lease).await?;

        for mut action in self.store.list_actions_by_phase(None).await? {
            if action.lease_ref.as_deref() != Some(lease_id) {
                continue;
            }
            if matches!(
                action.phase,
                ActionPhase::Completed | ActionPhase::Failed | ActionPhase::Expired
            ) {
                continue;
            }

            if let Some(encounter_ref) = &action.encounter_ref {
                if let Some(mut encounter) = self.store.get_encounter(encounter_ref).await? {
                    encounter.state = EncounterState::Revoked;
                    self.store.insert_encounter(&encounter).await?;
                }
                let receipt = self
                    .emit_audit_receipt(
                        encounter_ref,
                        action.grant_refs.clone(),
                        Some(lease.id.clone()),
                        AuditOutcome::Revoked,
                        request.reason.clone(),
                        Some(request.revoker_ref.clone()),
                    )
                    .await?;
                action.audit_receipt_ref = Some(receipt.id);
            }
            set_action_failed(
                &mut action,
                failure_code_lease_revoked(),
                format!("lease revoked: {}", request.reason),
            );
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "revoked",
                    serde_json::json!({
                        "phase": "failed",
                        "lease_ref": lease.id,
                        "code": action.failure_code,
                        "reason": action.failure_reason,
                    }),
                )
                .await?;
        }

        Ok(AdminActionResponse {
            status: "revoked".to_string(),
        })
    }

    async fn validate_policy_request(
        &self,
        request: PolicyValidationRequest,
    ) -> anyhow::Result<PolicyValidationResponse> {
        let manifest = self
            .store
            .get_agent_manifest(&request.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", request.target_agent_id))?;
        let encounter_request = EncounterRequest {
            caller: request.caller.clone(),
            target_agent_id: request.target_agent_id.clone(),
            target_owner: manifest.owner.clone(),
            requested_capabilities: vec![request.capability.clone()],
            requests_workspace_write: request.workspace_write,
            requests_secret_access: request.secret_access,
            requests_mutating_capability: request.mutating,
        };
        let decision = self.authorize(&manifest, &encounter_request);

        Ok(PolicyValidationResponse {
            disposition: format!("{:?}", decision.disposition).to_lowercase(),
            reason: decision.reason,
            trust_domain: request.caller.trust_domain,
            target_agent_id: request.target_agent_id,
        })
    }

    async fn drain(&self) -> anyhow::Result<()> {
        self.store.set_admin_mode_draining(true).await?;
        for mut record in self.store.list_lifecycle_records().await? {
            record.desired_state = AgentState::Inactive;
            record.observed_state = AgentState::Inactive;
            record.transition_reason = Some("operator drain".to_string());
            record.last_transition_at = now_timestamp();
            self.store.upsert_lifecycle_record(&record).await?;
        }
        Ok(())
    }

    async fn resume(&self) -> anyhow::Result<()> {
        self.store.set_admin_mode_draining(false).await?;
        self.run_once().await
    }
}
