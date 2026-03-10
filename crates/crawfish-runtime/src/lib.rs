mod hero;

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use crawfish_core::{
    authorize_encounter, compile_execution_plan, neutral_policy, now_timestamp,
    owner_policy_for_manifest, ActionDetail, ActionEventsResponse, ActionListResponse, ActionStore,
    ActionSummary, AdminActionResponse, AgentDetail, ApproveActionRequest, CheckpointStore,
    CrawfishConfig, DeterministicExecutor, EncounterDecision, EncounterDisposition,
    EncounterRequest, ExecutionContractPatch, ExecutionSurface, FleetStatusResponse,
    GovernanceContext, HealthResponse, PolicyValidationRequest, PolicyValidationResponse,
    RejectActionRequest, RevokeLeaseRequest, SubmitActionRequest, SubmittedAction,
    SupervisorControl,
};
use crawfish_mcp::McpAdapter;
use crawfish_store_sqlite::SqliteStore;
use crawfish_types::{
    Action, ActionOutputs, ActionPhase, AdapterBinding, AgentManifest, AgentState, ApprovalPolicy,
    AuditOutcome, AuditReceipt, CapabilityDescriptor, CapabilityLease, ConsentGrant,
    ContinuityModeName, CounterpartyRef, DegradedProfileName, DeterministicCheckpoint,
    EncounterRecord, EncounterState, ExternalRef, HealthStatus, LifecycleRecord, Mutability,
    TrustDomain, WorkspaceEdit, WorkspaceEditOp,
};
use hero::{
    load_json_artifact, required_input_string, CiTriageDeterministicExecutor,
    IncidentEnricherDeterministicExecutor, RepoIndexerDeterministicExecutor,
    RepoReviewerDeterministicExecutor, WorkspacePatchApplyDeterministicExecutor,
};
use serde_json::Value;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::time::{sleep, Duration};
use tracing::{error, info};
use uuid::Uuid;

pub struct Supervisor {
    root: PathBuf,
    config: CrawfishConfig,
    store: SqliteStore,
}

#[derive(Debug, thiserror::Error)]
enum RuntimeError {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Forbidden(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug, serde::Deserialize)]
struct ActionListQuery {
    phase: Option<String>,
}

impl IntoResponse for RuntimeError {
    fn into_response(self) -> Response {
        match self {
            Self::NotFound(message) => {
                (StatusCode::NOT_FOUND, Json(error_body(message))).into_response()
            }
            Self::BadRequest(message) => {
                (StatusCode::BAD_REQUEST, Json(error_body(message))).into_response()
            }
            Self::Forbidden(message) => {
                (StatusCode::FORBIDDEN, Json(error_body(message))).into_response()
            }
            Self::Internal(error) => {
                error!("internal runtime error: {error:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(error_body(error.to_string())),
                )
                    .into_response()
            }
        }
    }
}

#[derive(Debug)]
enum ExecutionOutcome {
    Completed {
        outputs: ActionOutputs,
        selected_executor: String,
        checkpoint: Option<DeterministicCheckpoint>,
        external_refs: Vec<ExternalRef>,
    },
    Blocked {
        reason: String,
        continuity_mode: ContinuityModeName,
        outputs: ActionOutputs,
        external_refs: Vec<ExternalRef>,
    },
}

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

    fn state_dir(&self) -> PathBuf {
        self.config.state_dir(&self.root)
    }

    async fn recover_running_actions(&self) -> anyhow::Result<()> {
        for mut action in self.store.list_actions_by_phase(Some("running")).await? {
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
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "recovered",
                    serde_json::json!({
                        "phase": "accepted",
                        "recovery_stage": recovery_stage,
                        "reason": "daemon restart requeued running action",
                    }),
                )
                .await?;
        }
        Ok(())
    }

    async fn expire_awaiting_approval_actions(&self) -> anyhow::Result<()> {
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
                        "finished_at": action.finished_at,
                    }),
                )
                .await?;
        }
        Ok(())
    }

    fn validate_submit_action_request(
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

    async fn write_checkpoint_for_action(
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

    async fn load_deterministic_checkpoint(
        &self,
        action: &Action,
    ) -> anyhow::Result<Option<DeterministicCheckpoint>> {
        let Some(bytes) = self.store.get_checkpoint(&action.id).await? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn run_deterministic_executor<E>(
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
        })
    }

    fn resolve_mcp_adapter(
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

    fn continuity_blocked_outcome(
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
            continuity_mode,
            outputs,
            external_refs,
        }
    }

    async fn ensure_repo_index_for_workspace(
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
            selected_executor: Some("deterministic.repo_index".to_string()),
            recovery_stage: None,
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

    fn synthetic_owner(&self) -> crawfish_types::OwnerRef {
        crawfish_types::OwnerRef {
            kind: crawfish_types::OwnerKind::ServiceAccount,
            id: "crawfishd".to_string(),
            display_name: Some("Crawfish Daemon".to_string()),
        }
    }

    async fn reconcile_manifest(
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

    async fn process_action_queue_once(&self) -> anyhow::Result<()> {
        if self.store.is_draining().await? {
            return Ok(());
        }

        self.expire_awaiting_approval_actions().await?;

        while let Some(action) = self.store.claim_next_accepted_action().await? {
            self.process_claimed_action(action).await?;
        }

        Ok(())
    }

    async fn process_claimed_action(&self, mut action: Action) -> anyhow::Result<()> {
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
            action.phase = ActionPhase::Blocked;
            action.failure_reason = Some(format!(
                "target agent {} is not executable in state {:?}",
                manifest.id, lifecycle.observed_state
            ));
            action.finished_at = None;
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "blocked",
                    serde_json::json!({"reason": action.failure_reason}),
                )
                .await?;
            return Ok(());
        }

        if let Err(error) = self.ensure_pre_execution_lease_valid(&action).await {
            let reason = error.to_string();
            action.phase = ActionPhase::Failed;
            action.finished_at = Some(now_timestamp());
            action.failure_reason = Some(reason.clone());
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
                    serde_json::json!({"reason": reason, "finished_at": action.finished_at}),
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
            }) => {
                action.phase = ActionPhase::Completed;
                action.outputs = outputs;
                action.finished_at = Some(now_timestamp());
                action.failure_reason = None;
                action.selected_executor = Some(selected_executor);
                action.external_refs = external_refs;
                if let Some(checkpoint) = checkpoint {
                    self.write_checkpoint_for_action(&mut action, &checkpoint)
                        .await?;
                }
                self.store.upsert_action(&action).await?;
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
            }
            Ok(ExecutionOutcome::Blocked {
                reason,
                continuity_mode,
                outputs,
                external_refs,
            }) => {
                action.phase = ActionPhase::Blocked;
                action.continuity_mode = Some(continuity_mode);
                action.failure_reason = Some(reason.clone());
                action.outputs = outputs;
                action.external_refs = external_refs;
                self.store.upsert_action(&action).await?;
                self.store
                    .append_action_event(
                        &action.id,
                        "blocked",
                        serde_json::json!({
                            "reason": reason,
                            "continuity_mode": action.continuity_mode.as_ref().map(|mode| format!("{mode:?}").to_lowercase())
                        }),
                    )
                    .await?;
            }
            Err(error) => {
                let reason = error.to_string();
                action.phase = ActionPhase::Failed;
                action.finished_at = Some(now_timestamp());
                action.failure_reason = Some(reason.clone());
                self.store.upsert_action(&action).await?;
                self.store
                    .append_action_event(
                        &action.id,
                        "failed",
                        serde_json::json!({"reason": reason, "finished_at": action.finished_at}),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn execute_action(
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
                    Ok(remote_outputs) => {
                        let mut derived_action = action.clone();
                        let log_text = extract_mcp_log_text(&remote_outputs).ok_or_else(|| {
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
                                external_refs.clone(),
                                &executor,
                            )
                            .await?;
                        if let ExecutionOutcome::Completed {
                            outputs,
                            checkpoint,
                            external_refs: refs,
                            ..
                        } = &mut outcome
                        {
                            outputs.metadata.insert(
                                "mcp_summary".to_string(),
                                serde_json::json!(remote_outputs.summary.clone()),
                            );
                            outputs.metadata.insert(
                                "mcp_result".to_string(),
                                remote_outputs
                                    .metadata
                                    .get("mcp_result")
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null),
                            );
                            *refs = external_refs;
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
            let executor = WorkspacePatchApplyDeterministicExecutor::new(self.state_dir());
            return self
                .run_deterministic_executor(
                    action,
                    "deterministic.workspace_patch_apply",
                    "applying",
                    Vec::new(),
                    &executor,
                )
                .await;
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

        if let Some((adapter, external_refs)) = self.resolve_mcp_adapter(manifest, action)? {
            match adapter.run(action).await {
                Ok(outputs) => {
                    return Ok(ExecutionOutcome::Completed {
                        outputs,
                        selected_executor: format!("mcp.{}", adapter.name()),
                        checkpoint: None,
                        external_refs,
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

    fn load_manifests(&self) -> anyhow::Result<Vec<AgentManifest>> {
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

    fn manifest_exists(&self, dependency: &str) -> bool {
        let path = self
            .config
            .manifest_dir(&self.root)
            .join(format!("{dependency}.toml"));
        path.exists()
    }

    fn authorize(&self, manifest: &AgentManifest, request: &EncounterRequest) -> EncounterDecision {
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

    fn action_requires_approval(
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

    async fn create_encounter(
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

    async fn emit_audit_receipt(
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

    fn approval_expiry_for_action(&self, action: &Action) -> String {
        let base = action.created_at.parse::<u64>().unwrap_or_default();
        let deadline = action.contract.delivery.deadline_ms.unwrap_or(900_000);
        (base.saturating_add(deadline / 1000)).to_string()
    }

    async fn issue_grant_and_lease(
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

    async fn ensure_pre_execution_lease_valid(&self, action: &Action) -> anyhow::Result<()> {
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
}

fn build_checkpoint(
    action: &Action,
    executor_kind: &str,
    stage: &str,
    artifact_refs: Vec<crawfish_types::ArtifactRef>,
) -> anyhow::Result<DeterministicCheckpoint> {
    Ok(DeterministicCheckpoint {
        executor_kind: executor_kind.to_string(),
        stage: stage.to_string(),
        workspace_root: action
            .inputs
            .get("workspace_root")
            .and_then(Value::as_str)
            .unwrap_or(".")
            .to_string(),
        input_digest: input_digest(&action.inputs)?,
        artifact_refs,
        last_updated_at: now_timestamp(),
    })
}

fn checkpoint_ref_for_executor(executor_kind: &str) -> String {
    format!("{}-checkpoint", executor_kind.replace('.', "-"))
}

fn input_digest(inputs: &crawfish_types::Metadata) -> anyhow::Result<String> {
    let serialized = serde_json::to_string(inputs)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    serialized.hash(&mut hasher);
    Ok(format!("{:016x}", hasher.finish()))
}

fn artifact_refs_exist(artifact_refs: &[crawfish_types::ArtifactRef]) -> bool {
    !artifact_refs.is_empty()
        && artifact_refs
            .iter()
            .all(|artifact| Path::new(&artifact.path).exists())
}

fn recovered_outputs_from_checkpoint(checkpoint: &DeterministicCheckpoint) -> ActionOutputs {
    ActionOutputs {
        summary: Some(format!(
            "Recovered outputs from {} checkpoint at stage {}",
            checkpoint.executor_kind, checkpoint.stage
        )),
        artifacts: checkpoint.artifact_refs.clone(),
        metadata: std::collections::BTreeMap::from([
            (
                "recovered_from_checkpoint".to_string(),
                serde_json::json!(true),
            ),
            (
                "executor_kind".to_string(),
                serde_json::json!(checkpoint.executor_kind),
            ),
            (
                "input_digest".to_string(),
                serde_json::json!(checkpoint.input_digest),
            ),
        ]),
    }
}

fn has_log_input(action: &Action) -> bool {
    action
        .inputs
        .get("log_text")
        .and_then(Value::as_str)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
        || action
            .inputs
            .get("log_file")
            .and_then(Value::as_str)
            .map(|path| Path::new(path).is_file())
            .unwrap_or(false)
}

fn mcp_input_external_refs(action: &Action) -> Vec<ExternalRef> {
    action
        .inputs
        .get("mcp_resource_ref")
        .and_then(Value::as_str)
        .map(|value| {
            vec![ExternalRef {
                kind: "mcp_resource".to_string(),
                value: value.to_string(),
                endpoint: None,
            }]
        })
        .unwrap_or_default()
}

fn extract_mcp_log_text(outputs: &ActionOutputs) -> Option<String> {
    let result = outputs.metadata.get("mcp_result")?;
    if let Some(log_text) = result
        .get("structuredContent")
        .and_then(|value| value.get("log_text"))
        .and_then(Value::as_str)
    {
        return Some(log_text.to_string());
    }
    if let Some(log_text) = result
        .get("structuredContent")
        .and_then(|value| value.get("log_excerpt"))
        .and_then(Value::as_str)
    {
        return Some(log_text.to_string());
    }
    if let Some(items) = result.get("content").and_then(Value::as_array) {
        let text = items
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        if !text.trim().is_empty() {
            return Some(text);
        }
    }
    outputs.summary.clone()
}

fn select_continuity_mode(
    preferences: &[ContinuityModeName],
    deterministic_available: bool,
) -> ContinuityModeName {
    for mode in preferences {
        match mode {
            ContinuityModeName::DeterministicOnly if !deterministic_available => continue,
            _ => return mode.clone(),
        }
    }

    if deterministic_available {
        ContinuityModeName::DeterministicOnly
    } else {
        ContinuityModeName::StoreAndForward
    }
}

fn action_requester(id: &str) -> crawfish_types::RequesterRef {
    crawfish_types::RequesterRef {
        kind: crawfish_types::RequesterKind::System,
        id: id.to_string(),
    }
}

fn current_timestamp_seconds() -> u64 {
    now_timestamp().parse::<u64>().unwrap_or_default()
}

fn action_phase_name(phase: &ActionPhase) -> &'static str {
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

#[async_trait::async_trait]
impl SupervisorControl for Supervisor {
    async fn list_status(&self) -> anyhow::Result<FleetStatusResponse> {
        Ok(FleetStatusResponse {
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
        Ok(Some(ActionDetail {
            artifact_refs: action.outputs.artifacts.clone(),
            selected_executor: action.selected_executor.clone(),
            recovery_stage: action.recovery_stage.clone(),
            external_refs: action.external_refs.clone(),
            grant_details,
            lease_detail,
            action,
            encounter,
            latest_audit_receipt: audit_receipt,
        }))
    }

    async fn submit_action(&self, request: SubmitActionRequest) -> anyhow::Result<SubmittedAction> {
        let manifest = self
            .store
            .get_agent_manifest(&request.target_agent_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("agent not found: {}", request.target_agent_id))?;
        self.validate_submit_action_request(&manifest, &request)?;

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
            &request,
            &manifest,
            &request.capability,
            &compiled.contract.safety.approval_policy,
        );
        let encounter_state = if matches!(decision.disposition, EncounterDisposition::Deny) {
            EncounterState::Denied
        } else if requires_approval
            || matches!(decision.disposition, EncounterDisposition::AwaitConsent)
        {
            EncounterState::AwaitingConsent
        } else {
            EncounterState::Leased
        };
        let mut encounter = self
            .create_encounter(&manifest, &encounter_request, &decision, encounter_state)
            .await?;
        if matches!(decision.disposition, EncounterDisposition::Deny) {
            let receipt = self
                .emit_audit_receipt(
                    &encounter.id,
                    Vec::new(),
                    None,
                    AuditOutcome::Denied,
                    decision.reason.clone(),
                    None,
                )
                .await?;
            let _ = receipt;
            anyhow::bail!(decision.reason);
        }

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
            selected_executor: None,
            recovery_stage: None,
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
        self.store.upsert_action(&action).await?;
        self.store
            .append_action_event(
                &action.id,
                "approved",
                serde_json::json!({
                    "phase": "accepted",
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
        action.phase = ActionPhase::Failed;
        action.finished_at = Some(now_timestamp());
        action.failure_reason = Some("approval rejected".to_string());
        action.audit_receipt_ref = Some(receipt.id);
        self.store.upsert_action(&action).await?;
        self.store
            .append_action_event(
                &action.id,
                "rejected",
                serde_json::json!({
                    "phase": "failed",
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
            action.phase = ActionPhase::Failed;
            action.finished_at = Some(now_timestamp());
            action.failure_reason = Some(format!("lease revoked: {}", request.reason));
            self.store.upsert_action(&action).await?;
            self.store
                .append_action_event(
                    &action.id,
                    "revoked",
                    serde_json::json!({
                        "phase": "failed",
                        "lease_ref": lease.id,
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

fn trust_domain_defaults(trust_domain: TrustDomain) -> crawfish_types::EncounterPolicy {
    let mut policy = crawfish_types::EncounterPolicy {
        default_disposition: crawfish_types::DefaultDisposition::AllowWithLease,
        capability_visibility: crawfish_types::CapabilityVisibility::OwnerOnly,
        data_boundary: crawfish_types::DataBoundaryPolicy::OwnerOnly,
        tool_boundary: crawfish_types::ToolBoundaryPolicy::NoCrossOwnerMutation,
        workspace_boundary: crawfish_types::WorkspaceBoundaryPolicy::Isolated,
        network_boundary: crawfish_types::NetworkBoundaryPolicy::LocalOnly,
        human_approval_requirements: Vec::new(),
    };

    match trust_domain {
        TrustDomain::SameOwnerLocal => {}
        TrustDomain::SameDeviceForeignOwner => {
            policy.default_disposition = crawfish_types::DefaultDisposition::RequireConsent;
            policy.workspace_boundary = crawfish_types::WorkspaceBoundaryPolicy::LeaseScoped;
            policy.data_boundary = crawfish_types::DataBoundaryPolicy::LeaseScoped;
            policy.network_boundary = crawfish_types::NetworkBoundaryPolicy::LeasedEgress;
        }
        TrustDomain::InternalOrg | TrustDomain::ExternalPartner => {
            policy.default_disposition = crawfish_types::DefaultDisposition::RequireConsent;
            policy.workspace_boundary = crawfish_types::WorkspaceBoundaryPolicy::LeaseScoped;
            policy.data_boundary = crawfish_types::DataBoundaryPolicy::LeaseScoped;
        }
        TrustDomain::PublicUnknown => {
            policy.default_disposition = crawfish_types::DefaultDisposition::Deny;
        }
    }

    policy
}

pub fn summarize_capabilities(manifest: &AgentManifest) -> Vec<CapabilityDescriptor> {
    manifest
        .capabilities
        .iter()
        .map(|capability| CapabilityDescriptor {
            namespace: capability.clone(),
            verbs: vec!["run".to_string()],
            executor_class: crawfish_types::ExecutorClass::Hybrid,
            mutability: if capability.contains("patch") || capability.contains("write") {
                Mutability::Mutating
            } else {
                Mutability::ReadOnly
            },
            risk_class: crawfish_types::RiskClass::Medium,
            cost_class: crawfish_types::CostClass::Standard,
            latency_class: crawfish_types::LatencyClass::Background,
            approval_requirements: Vec::new(),
        })
        .collect()
}

fn api_router(supervisor: Arc<Supervisor>) -> Router {
    Router::new()
        .route("/v1/health", get(health_handler))
        .route("/v1/agents", get(list_agents_handler))
        .route("/v1/agents/{id}", get(agent_detail_handler))
        .route(
            "/v1/actions",
            get(list_actions_handler).post(submit_action_handler),
        )
        .route("/v1/actions/{id}", get(action_detail_handler))
        .route("/v1/actions/{id}/events", get(action_events_handler))
        .route("/v1/actions/{id}/approve", post(approve_action_handler))
        .route("/v1/actions/{id}/reject", post(reject_action_handler))
        .route("/v1/leases/{id}/revoke", post(revoke_lease_handler))
        .route("/v1/admin/drain", post(drain_handler))
        .route("/v1/admin/resume", post(resume_handler))
        .route("/v1/policy/validate", post(policy_validate_handler))
        .with_state(supervisor)
}

async fn health_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<HealthResponse>, RuntimeError> {
    Ok(Json(HealthResponse {
        status: "ok".to_string(),
        socket_path: supervisor
            .config()
            .socket_path(supervisor.root())
            .display()
            .to_string(),
    }))
}

async fn list_agents_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<FleetStatusResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_status()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn agent_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<AgentDetail>, RuntimeError> {
    supervisor
        .inspect_agent(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("agent not found: {id}")))
}

async fn action_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionDetail>, RuntimeError> {
    supervisor
        .inspect_action(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

async fn action_events_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionEventsResponse>, RuntimeError> {
    if supervisor
        .store()
        .get_action(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .is_none()
    {
        return Err(RuntimeError::NotFound(format!("action not found: {id}")));
    }

    Ok(Json(
        supervisor
            .list_action_events(&id)
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn list_actions_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Query(query): Query<ActionListQuery>,
) -> Result<Json<ActionListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_actions(query.phase.as_deref())
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn submit_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<SubmitActionRequest>,
) -> Result<Json<SubmittedAction>, RuntimeError> {
    match supervisor.submit_action(request).await {
        Ok(submitted) => Ok(Json(submitted)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("agent not found:") {
                Err(RuntimeError::NotFound(message))
            } else if message.starts_with("invalid action request:") {
                Err(RuntimeError::BadRequest(message))
            } else if message.contains("denied") || message.contains("consent") {
                Err(RuntimeError::Forbidden(message))
            } else {
                Err(RuntimeError::Internal(error))
            }
        }
    }
}

async fn approve_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ApproveActionRequest>,
) -> Result<Json<SubmittedAction>, RuntimeError> {
    match supervisor.approve_action(&id, request).await {
        Ok(submitted) => Ok(Json(submitted)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("action not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn reject_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<RejectActionRequest>,
) -> Result<Json<SubmittedAction>, RuntimeError> {
    match supervisor.reject_action(&id, request).await {
        Ok(submitted) => Ok(Json(submitted)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("action not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn revoke_lease_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<RevokeLeaseRequest>,
) -> Result<Json<AdminActionResponse>, RuntimeError> {
    match supervisor.revoke_lease(&id, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("capability lease not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn drain_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<AdminActionResponse>, RuntimeError> {
    supervisor.drain().await.map_err(RuntimeError::Internal)?;
    Ok(Json(AdminActionResponse {
        status: "draining".to_string(),
    }))
}

async fn resume_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<AdminActionResponse>, RuntimeError> {
    supervisor.resume().await.map_err(RuntimeError::Internal)?;
    Ok(Json(AdminActionResponse {
        status: "active".to_string(),
    }))
}

async fn policy_validate_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<PolicyValidationRequest>,
) -> Result<Json<PolicyValidationResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .validate_policy_request(request)
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

fn error_body(message: String) -> serde_json::Value {
    serde_json::json!({ "error": message })
}

#[cfg(test)]
mod tests {
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
    use futures_util::{stream, StreamExt};
    use http_body_util::{BodyExt, Full};
    use hyper::{Method, Request, Uri};
    use hyper_util::client::legacy::Client;
    use hyperlocal::UnixClientExt;
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, Mutex};

    async fn build_supervisor_with_config(
        dir: &Path,
        config_contents: String,
    ) -> anyhow::Result<Arc<Supervisor>> {
        tokio::fs::create_dir_all(dir.join("agents")).await?;
        tokio::fs::create_dir_all(dir.join(".crawfish/state")).await?;
        tokio::fs::create_dir_all(dir.join(".crawfish/run")).await?;
        tokio::fs::create_dir_all(dir.join("src")).await?;
        tokio::fs::create_dir_all(dir.join("tests")).await?;
        tokio::fs::write(dir.join("Crawfish.toml"), config_contents).await?;
        tokio::fs::write(
            dir.join("src/lib.rs"),
            "pub fn value() -> u32 { 42 } // TODO follow up\n",
        )
        .await?;
        tokio::fs::write(dir.join("tests/lib_test.rs"), "#[test] fn smoke() {}\n").await?;
        for agent in [
            "repo_indexer",
            "repo_reviewer",
            "ci_triage",
            "incident_enricher",
            "workspace_editor",
        ] {
            tokio::fs::write(
                dir.join(format!("agents/{agent}.toml")),
                std::fs::read_to_string(format!(
                    "{}/../../examples/hero-fleet/agents/{agent}.toml",
                    env!("CARGO_MANIFEST_DIR")
                ))?,
            )
            .await?;
        }
        let supervisor = Arc::new(Supervisor::from_config_path(&dir.join("Crawfish.toml")).await?);
        supervisor.run_once().await?;
        Ok(supervisor)
    }

    async fn build_supervisor(dir: &Path) -> anyhow::Result<Arc<Supervisor>> {
        build_supervisor_with_config(
            dir,
            include_str!("../../../examples/hero-fleet/Crawfish.toml").to_string(),
        )
        .await
    }

    async fn build_supervisor_with_mcp(
        dir: &Path,
        mcp_url: &str,
    ) -> anyhow::Result<Arc<Supervisor>> {
        let config = include_str!("../../../examples/hero-fleet/Crawfish.toml")
            .replace("http://127.0.0.1:8877/sse", mcp_url);
        build_supervisor_with_config(dir, config).await
    }

    fn local_owner(id: &str) -> crawfish_types::OwnerRef {
        crawfish_types::OwnerRef {
            kind: crawfish_types::OwnerKind::Human,
            id: id.to_string(),
            display_name: None,
        }
    }

    fn workspace_patch_request(
        dir: &Path,
        edits: Value,
        deadline_ms: Option<u64>,
    ) -> SubmitActionRequest {
        SubmitActionRequest {
            target_agent_id: "workspace_editor".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "workspace.patch.apply".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: "apply local patch".to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.display().to_string()),
                ),
                ("edits".to_string(), edits),
            ]),
            contract_overrides: deadline_ms.map(|deadline_ms| ExecutionContractPatch {
                delivery: crawfish_core::DeliveryContractPatch {
                    deadline_ms: Some(deadline_ms),
                    freshness_ttl_ms: None,
                    required_ack: None,
                    liveliness_window_ms: None,
                },
                ..ExecutionContractPatch::default()
            }),
            execution_strategy: None,
            schedule: None,
            counterparty_refs: Vec::new(),
            data_boundary: None,
            workspace_write: true,
            secret_access: false,
            mutating: true,
        }
    }

    #[derive(Clone)]
    struct RuntimeMcpState {
        sessions: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<String>>>>,
        next_session: Arc<AtomicUsize>,
        log_text: String,
    }

    #[derive(serde::Deserialize)]
    struct SessionQuery {
        session: String,
    }

    async fn spawn_runtime_mcp_server(log_text: &str) -> String {
        let state = RuntimeMcpState {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session: Arc::new(AtomicUsize::new(1)),
            log_text: log_text.to_string(),
        };

        let app = Router::new()
            .route("/sse", get(runtime_mock_sse))
            .route("/messages", post(runtime_mock_messages))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/sse")
    }

    async fn runtime_mock_sse(
        AxumState(state): AxumState<RuntimeMcpState>,
    ) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
        let session_id = format!(
            "session-{}",
            state.next_session.fetch_add(1, Ordering::SeqCst)
        );
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        state.sessions.lock().await.insert(session_id.clone(), tx);

        let initial = stream::once(async move {
            Ok(Event::default()
                .event("endpoint")
                .data(format!("/messages?session={session_id}")))
        });
        let rest = stream::unfold(rx, |mut rx| async move {
            rx.recv()
                .await
                .map(|payload| (Ok(Event::default().event("message").data(payload)), rx))
        });
        Sse::new(initial.chain(rest))
    }

    async fn runtime_mock_messages(
        AxumState(state): AxumState<RuntimeMcpState>,
        Query(query): Query<SessionQuery>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        let sender = state.sessions.lock().await.get(&query.session).cloned();
        if let Some(sender) = sender {
            let id = payload.get("id").cloned().unwrap_or(Value::Null);
            let method = payload
                .get("method")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let log_text = state.log_text.clone();
            tokio::spawn(async move {
                let response = match method.as_str() {
                    "initialize" => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {"protocolVersion": "2024-11-05"}
                    }),
                    "tools/list" => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {"tools": [{"name": "ci_runs_inspect"}]}
                    }),
                    "tools/call" => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": {
                            "content": [{"type": "text", "text": "remote CI logs fetched"}],
                            "structuredContent": {
                                "provider": "github_actions",
                                "log_text": log_text
                            }
                        }
                    }),
                    _ => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"message": "unknown method"}
                    }),
                };
                let _ = sender.send(response.to_string());
            });
        }

        Json(serde_json::json!({"accepted": true}))
    }

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
            selected_executor: Some("deterministic.repo_index".to_string()),
            recovery_stage: None,
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
}
