use super::*;

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
