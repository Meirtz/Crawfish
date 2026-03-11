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
}
