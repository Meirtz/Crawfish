mod hero;

use axum::{
    extract::{Path as AxumPath, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use crawfish_a2a::{A2aAdapter, A2aError};
use crawfish_core::{
    authorize_encounter, compile_execution_plan, neutral_policy, now_timestamp,
    owner_policy_for_manifest, AcknowledgeAlertRequest, AcknowledgeAlertResponse, ActionDetail,
    ActionEvaluationsResponse, ActionEventsResponse, ActionListResponse,
    ActionRemoteEvidenceResponse, ActionRemoteFollowupsResponse, ActionStore, ActionSummary,
    ActionTraceResponse, AdminActionResponse, AgentDetail, AlertListResponse, ApproveActionRequest,
    CheckpointStore, CompiledExecutionPlan, CrawfishConfig, DeterministicExecutor,
    DispatchRemoteFollowupRequest, DispatchRemoteFollowupResponse, EncounterDecision,
    EncounterDisposition, EncounterRequest, EvaluationDatasetDetailResponse,
    EvaluationDatasetsResponse, ExecutionContractPatch, ExecutionSurface,
    ExperimentRunDetailResponse, FederationPackDetailResponse, FederationPackListResponse,
    GovernanceContext, HealthResponse, OpenClawAgentStatusResponse, OpenClawCallerContext,
    OpenClawInboundActionRequest, OpenClawInboundActionResponse, OpenClawInspectionContext,
    PairwiseExperimentRunDetailResponse, PolicyValidationRequest, PolicyValidationResponse,
    RejectActionRequest, ResolveReviewQueueItemRequest, ResolveReviewQueueItemResponse,
    ReviewQueueResponse, RevokeLeaseRequest, StartEvaluationRunRequest, StartEvaluationRunResponse,
    StartPairwiseEvaluationRunRequest, StartPairwiseEvaluationRunResponse, SubmitActionRequest,
    SubmittedAction, SupervisorControl, SwarmStatusResponse, TreatyDetailResponse,
    TreatyListResponse,
};
use crawfish_harness_local::{LocalHarnessAdapter, LocalHarnessError};
use crawfish_mcp::McpAdapter;
use crawfish_openclaw::{OpenClawAdapter, OpenClawError};
use crawfish_store_sqlite::SqliteStore;
use crawfish_types::{
    Action, ActionOutputs, ActionPhase, AdapterBinding, AgentManifest, AgentState, AlertEvent,
    AlertRule, ApprovalPolicy, AuditOutcome, AuditReceipt, CallerOwnerMapping,
    CapabilityDescriptor, CapabilityLease, CapabilityVisibility, CheckpointOutcome,
    CheckpointStatus, ConsentGrant, ContinuityModeName, CounterpartyRef, DatasetCase,
    DegradedProfileName, DeterministicCheckpoint, DoctrinePack, EncounterRecord, EncounterState,
    EvaluationDataset, EvaluationProfile, EvaluationRecord, EvaluationStatus, ExecutionStrategy,
    ExecutionStrategyMode, ExperimentCaseResult, ExperimentCaseStatus, ExperimentRun,
    ExperimentRunStatus, ExternalRef, FederationDecision, FederationPack, FeedbackNote,
    FeedbackPolicy, HealthStatus, InteractionModel, JurisdictionClass, LifecycleRecord,
    LocalHarnessKind, Metadata, Mutability, NumericComparison, OversightCheckpoint, OwnerKind,
    OwnerRef, PairwiseCaseResult, PairwiseExperimentRun, PairwiseExperimentRunStatus,
    PairwiseOutcome, PairwiseProfile, PolicyIncident, PolicyIncidentSeverity, RemoteAttemptRecord,
    RemoteEvidenceBundle, RemoteEvidenceItem, RemoteEvidenceStatus, RemoteFollowupReason,
    RemoteFollowupRequest, RemoteFollowupStatus, RemoteOutcomeDisposition, RemoteResultAcceptance,
    RemoteReviewDisposition, RemoteReviewReason, RemoteStateDisposition, RequesterKind,
    ReviewQueueItem, ReviewQueueKind, ReviewQueueStatus, ScorecardCriterion,
    ScorecardCriterionKind, ScorecardSpec, StrategyCheckpointState, TraceBundle, TrustDomain,
    VerificationStatus, VerificationSummary, VerifyLoopFailureMode, WorkspaceEdit, WorkspaceEditOp,
};
use hero::{
    load_json_artifact, required_input_string, CiTriageDeterministicExecutor,
    IncidentEnricherDeterministicExecutor, RepoIndexerDeterministicExecutor,
    RepoReviewerDeterministicExecutor, TaskPlannerDeterministicExecutor,
    WorkspacePatchApplyDeterministicExecutor,
};
use jsonschema::validator_for;
use regex::Regex;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::net::UnixListener;
use tokio::time::{sleep, Duration};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::{error, info, warn};
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

#[derive(Debug, serde::Deserialize)]
struct ReviewQueueQuery {
    kind: Option<String>,
}

#[derive(Debug, Clone)]
struct OpenClawResolvedCaller {
    caller_id: String,
    counterparty: CounterpartyRef,
    requester_id: String,
    effective_scopes: Vec<String>,
}

#[derive(Debug, Clone)]
struct ResolvedEvaluationProfile {
    name: String,
    profile: EvaluationProfile,
    scorecard: ScorecardSpec,
    dataset: Option<(String, EvaluationDataset)>,
    alert_rules: Vec<AlertRule>,
}

#[derive(Debug, Clone)]
struct ResolvedPairwiseProfile {
    name: String,
    profile: PairwiseProfile,
}

#[derive(Debug, Clone)]
struct ScorecardOutcome {
    status: EvaluationStatus,
    score: f64,
    summary: String,
    findings: Vec<String>,
    criterion_results: Vec<crawfish_types::EvaluationCriterionResult>,
}

fn is_task_plan_capability(capability: &str) -> bool {
    matches!(capability, "task.plan" | "coding.patch.plan")
}

fn normalize_task_plan_inputs(inputs: &mut BTreeMap<String, Value>) -> bool {
    let mut normalized = false;

    let has_objective = inputs
        .get("objective")
        .and_then(Value::as_str)
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false);
    if !has_objective {
        for legacy_key in ["task", "spec_text", "problem_statement"] {
            if let Some(value) = inputs.get(legacy_key).cloned() {
                if value
                    .as_str()
                    .map(|text| !text.trim().is_empty())
                    .unwrap_or(false)
                {
                    inputs.insert("objective".to_string(), value);
                    normalized = true;
                    break;
                }
            }
        }
    }

    if !inputs.contains_key("context_files") {
        if let Some(value) = inputs.get("files_of_interest").cloned() {
            if value
                .as_array()
                .map(|entries| !entries.is_empty())
                .unwrap_or(false)
            {
                inputs.insert("context_files".to_string(), value);
                normalized = true;
            }
        }
    }

    normalized
}

fn normalize_submit_request(mut request: SubmitActionRequest) -> SubmitActionRequest {
    let mut normalized_capability = None;
    if request.capability == "coding.patch.plan" {
        normalized_capability = Some("task.plan".to_string());
        request.capability = "task.plan".to_string();
    }

    if is_task_plan_capability(&request.capability)
        && normalize_task_plan_inputs(&mut request.inputs)
    {
        warn!("normalized deprecated task planning input keys to objective");
    }

    if normalized_capability.is_some() {
        warn!("normalized deprecated capability coding.patch.plan to task.plan");
    }

    request
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
        surface_events: Vec<crawfish_core::SurfaceActionEvent>,
    },
    Blocked {
        reason: String,
        failure_code: String,
        continuity_mode: Option<ContinuityModeName>,
        outputs: ActionOutputs,
        external_refs: Vec<ExternalRef>,
        surface_events: Vec<crawfish_core::SurfaceActionEvent>,
    },
    Failed {
        reason: String,
        failure_code: String,
        outputs: ActionOutputs,
        checkpoint: Option<DeterministicCheckpoint>,
        external_refs: Vec<ExternalRef>,
        surface_events: Vec<crawfish_core::SurfaceActionEvent>,
    },
}

#[derive(Debug, Clone)]
struct TaskPlanVerificationResult {
    passed: bool,
    summary: VerificationSummary,
    feedback: Option<String>,
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

    async fn postprocess_terminal_action(&self, action: &mut Action) -> anyhow::Result<()> {
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

    async fn build_trace_bundle_for_action(
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

    async fn build_remote_evidence_bundle_for_action(
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

    async fn evaluate_action_outputs(
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

    async fn score_action_outputs(
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

    async fn maybe_enqueue_review_item(
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

    fn policy_incidents_for_action(
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

    fn build_alert_events(
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

    fn evaluation_profiles(&self) -> BTreeMap<String, EvaluationProfile> {
        let mut profiles = builtin_evaluation_profiles();
        profiles.extend(self.config.evaluation.profiles.clone());
        profiles
    }

    fn evaluation_scorecards(&self) -> BTreeMap<String, ScorecardSpec> {
        let mut scorecards = builtin_scorecards();
        scorecards.extend(self.config.evaluation.scorecards.clone());
        scorecards
    }

    fn evaluation_datasets(&self) -> BTreeMap<String, EvaluationDataset> {
        let mut datasets = builtin_evaluation_datasets();
        datasets.extend(self.config.evaluation.datasets.clone());
        datasets
    }

    fn evaluation_alert_rules(&self) -> BTreeMap<String, AlertRule> {
        let mut rules = builtin_alert_rules();
        rules.extend(self.config.evaluation.alerts.clone());
        rules
    }

    fn evaluation_pairwise_profiles(&self) -> BTreeMap<String, PairwiseProfile> {
        let mut profiles = builtin_pairwise_profiles();
        profiles.extend(self.config.evaluation.pairwise_profiles.clone());
        profiles
    }

    fn resolve_evaluation_profile(
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

    fn resolve_pairwise_profile(
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

    async fn scorecard_criterion_result(
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

    async fn maybe_capture_dataset_case(
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

    async fn run_experiment_case(
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

    async fn record_verification_evaluation(
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

    async fn execute_task_plan(
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

    async fn execute_task_plan_verify_loop(
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

    async fn execute_task_plan_single_pass(
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

    fn resolve_openclaw_adapter(
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

    fn resolve_local_harness_adapter(
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

    fn resolve_a2a_adapter(
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

    fn builtin_federation_pack(&self, treaty_pack: &crawfish_types::TreatyPack) -> FederationPack {
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

    fn resolve_federation_pack(
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

    fn compile_treaty_decision(
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

    fn compile_federation_decision(
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

    fn evaluate_federation_post_result(
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

    async fn ensure_required_lease_valid(
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
            failure_code: failure_code_route_unavailable().to_string(),
            continuity_mode: Some(continuity_mode),
            outputs,
            external_refs,
            surface_events: Vec::new(),
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

    fn resolve_openclaw_caller(
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

    fn openclaw_external_refs(
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

    fn action_visible_to_openclaw(&self, caller: &OpenClawResolvedCaller, action: &Action) -> bool {
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

    fn agent_visible_to_openclaw(
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

    async fn preflight_submission(
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

    fn lock_file_path(&self, workspace_root: &str) -> PathBuf {
        self.state_dir()
            .join("locks")
            .join(format!("workspace-{}.lock", stable_id(workspace_root)))
    }

    async fn try_acquire_workspace_lock(
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

    async fn try_acquire_workspace_lock_path(
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

    async fn is_stale_lock_owner(&self, owner_action_id: &str) -> anyhow::Result<bool> {
        let action = self.store.get_action(owner_action_id).await?;
        Ok(match action {
            Some(action) => matches!(
                action.phase,
                ActionPhase::Completed | ActionPhase::Failed | ActionPhase::Expired
            ),
            None => true,
        })
    }

    async fn release_workspace_lock(
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

    async fn submit_openclaw_action(
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

    async fn inspect_openclaw_action(
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

    async fn list_openclaw_action_events(
        &self,
        action_id: &str,
        context: OpenClawInspectionContext,
    ) -> Result<ActionEventsResponse, RuntimeError> {
        self.inspect_openclaw_action(action_id, context).await?;
        self.list_action_events(action_id)
            .await
            .map_err(RuntimeError::Internal)
    }

    async fn inspect_openclaw_agent_status(
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
        strategy_state: None,
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

fn stable_id(value: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn artifact_refs_exist(artifact_refs: &[crawfish_types::ArtifactRef]) -> bool {
    !artifact_refs.is_empty()
        && artifact_refs
            .iter()
            .all(|artifact| Path::new(&artifact.path).exists())
}

fn recovered_outputs_from_checkpoint(checkpoint: &DeterministicCheckpoint) -> ActionOutputs {
    let mut metadata = std::collections::BTreeMap::from([
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
    ]);
    if let Some(strategy_state) = &checkpoint.strategy_state {
        metadata.insert(
            "strategy_iteration".to_string(),
            serde_json::json!(strategy_state.iteration),
        );
        if let Some(summary) = &strategy_state.verification_summary {
            metadata.insert(
                "verification_summary".to_string(),
                serde_json::to_value(summary).unwrap_or(serde_json::Value::Null),
            );
        }
    }
    ActionOutputs {
        summary: Some(format!(
            "Recovered outputs from {} checkpoint at stage {}",
            checkpoint.executor_kind, checkpoint.stage
        )),
        artifacts: checkpoint.artifact_refs.clone(),
        metadata,
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

fn failure_code_approval_required() -> &'static str {
    "approval_required"
}

fn failure_code_approval_rejected() -> &'static str {
    "approval_rejected"
}

fn failure_code_lease_revoked() -> &'static str {
    "lease_revoked"
}

fn failure_code_lease_expired() -> &'static str {
    "lease_expired"
}

fn failure_code_local_harness_missing_binary() -> &'static str {
    "local_harness_missing_binary"
}

fn failure_code_local_harness_spawn_error() -> &'static str {
    "local_harness_spawn_error"
}

fn failure_code_local_harness_timeout() -> &'static str {
    "local_harness_timeout"
}

fn failure_code_local_harness_exit_nonzero() -> &'static str {
    "local_harness_exit_nonzero"
}

fn failure_code_local_harness_protocol_error() -> &'static str {
    "local_harness_protocol_error"
}

fn failure_code_lock_conflict() -> &'static str {
    "lock_conflict"
}

fn failure_code_openclaw_auth_error() -> &'static str {
    "openclaw_auth_error"
}

fn failure_code_openclaw_connect_error() -> &'static str {
    "openclaw_connect_error"
}

fn failure_code_openclaw_protocol_error() -> &'static str {
    "openclaw_protocol_error"
}

fn failure_code_openclaw_run_failed() -> &'static str {
    "openclaw_run_failed"
}

fn failure_code_openclaw_unsupported_workspace_mode() -> &'static str {
    "openclaw_unsupported_workspace_mode"
}

fn failure_code_openclaw_unsupported_session_mode() -> &'static str {
    "openclaw_unsupported_session_mode"
}

fn failure_code_a2a_auth_error() -> &'static str {
    "a2a_auth_error"
}

fn failure_code_a2a_connect_error() -> &'static str {
    "a2a_connect_error"
}

fn failure_code_a2a_protocol_error() -> &'static str {
    "a2a_protocol_error"
}

fn failure_code_a2a_task_failed() -> &'static str {
    "a2a_task_failed"
}

fn failure_code_treaty_denied() -> &'static str {
    "treaty_denied"
}

fn failure_code_route_unavailable() -> &'static str {
    "route_unavailable"
}

fn failure_code_executor_error() -> &'static str {
    "executor_error"
}

fn failure_code_requeued_after_restart() -> &'static str {
    "requeued_after_restart"
}

fn failure_code_verification_failed() -> &'static str {
    "verification_failed"
}

fn failure_code_verification_budget_exhausted() -> &'static str {
    "verification_budget_exhausted"
}

fn failure_code_verification_spec_invalid() -> &'static str {
    "verification_spec_invalid"
}

fn runtime_enum_to_snake<T: std::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
        .chars()
        .enumerate()
        .fold(String::new(), |mut acc, (index, ch)| {
            if ch.is_ascii_uppercase() {
                if index != 0 {
                    acc.push('_');
                }
                acc.extend(ch.to_lowercase());
            } else {
                acc.push(ch);
            }
            acc
        })
}

fn objective_tokens(objective: &str) -> Vec<String> {
    objective
        .split(|character: char| !character.is_alphanumeric())
        .filter(|token| token.len() >= 4)
        .map(|token| token.to_ascii_lowercase())
        .collect()
}

fn artifact_basename(artifact: &crawfish_types::ArtifactRef) -> String {
    Path::new(&artifact.path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(&artifact.path)
        .to_string()
}

fn artifact_ref_by_name<'a>(
    action: &'a Action,
    artifact_name: &str,
) -> Option<&'a crawfish_types::ArtifactRef> {
    action.outputs.artifacts.iter().find(|artifact| {
        artifact_basename(artifact) == artifact_name || artifact.path.ends_with(artifact_name)
    })
}

async fn scorecard_target_value(
    action: &Action,
    artifact_name: Option<&str>,
    field_path: Option<&str>,
) -> anyhow::Result<Option<Value>> {
    if let Some(artifact_name) = artifact_name {
        let Some(artifact_ref) = artifact_ref_by_name(action, artifact_name) else {
            return Ok(None);
        };
        let value: Value = load_json_artifact(artifact_ref).await?;
        if let Some(field_path) = field_path {
            return Ok(json_value_at_path(&value, field_path).cloned());
        }
        return Ok(Some(value));
    }

    Ok(field_path
        .and_then(|field_path| metadata_value_at_path(&action.inputs, field_path).cloned()))
}

async fn scorecard_target_text(
    action: &Action,
    artifact_name: Option<&str>,
    field_path: Option<&str>,
) -> anyhow::Result<Option<String>> {
    if let Some(artifact_name) = artifact_name {
        let Some(artifact_ref) = artifact_ref_by_name(action, artifact_name) else {
            return Ok(None);
        };
        if field_path.is_none() {
            return Ok(Some(tokio::fs::read_to_string(&artifact_ref.path).await?));
        }
    }
    let Some(value) = scorecard_target_value(action, artifact_name, field_path).await? else {
        return Ok(None);
    };
    Ok(Some(match value {
        Value::String(text) => text,
        other => serde_json::to_string(&other)?,
    }))
}

async fn scorecard_evidence_summary(
    action: &Action,
    criterion: &ScorecardCriterion,
    interaction_model: &crawfish_types::InteractionModel,
    observed_incidents: &[PolicyIncident],
    passed: bool,
) -> anyhow::Result<String> {
    let target_label = criterion
        .artifact_name
        .clone()
        .unwrap_or_else(|| "inputs".to_string());
    let path_label = criterion
        .field_path
        .as_deref()
        .map(|path| format!(" at `{path}`"))
        .unwrap_or_default();
    let status = if passed { "passed" } else { "failed" };

    let detail = match criterion.kind {
        ScorecardCriterionKind::RegexMatch => criterion
            .regex_pattern
            .as_deref()
            .map(|pattern| format!("pattern `{pattern}`"))
            .unwrap_or_else(|| "regex pattern missing".to_string()),
        ScorecardCriterionKind::NumericThreshold => format!(
            "threshold {:?} {}",
            criterion.numeric_comparison,
            criterion.numeric_threshold.unwrap_or_default()
        ),
        ScorecardCriterionKind::FieldEquals => criterion
            .expected_value
            .as_ref()
            .map(|value| format!("expected {value}"))
            .unwrap_or_else(|| "expected value missing".to_string()),
        ScorecardCriterionKind::JsonSchemaValid => "JSON schema validation".to_string(),
        ScorecardCriterionKind::ListMinLen => {
            format!("minimum length {}", criterion.min_len.unwrap_or(1))
        }
        ScorecardCriterionKind::TokenCoverage => criterion
            .source_path
            .as_deref()
            .map(|path| format!("cover tokens from `{path}`"))
            .unwrap_or_else(|| "token source missing".to_string()),
        ScorecardCriterionKind::CheckpointPassed => criterion
            .checkpoint
            .as_ref()
            .map(|checkpoint| format!("checkpoint `{}`", runtime_enum_to_snake(checkpoint)))
            .unwrap_or_else(|| "checkpoint missing".to_string()),
        ScorecardCriterionKind::IncidentAbsent => criterion
            .incident_code
            .as_deref()
            .map(|code| format!("incident `{code}` absent"))
            .unwrap_or_else(|| format!("{} incidents absent", observed_incidents.len())),
        ScorecardCriterionKind::ExternalRefPresent => criterion
            .external_ref_kind
            .as_deref()
            .map(|kind| format!("external ref `{kind}` present"))
            .unwrap_or_else(|| "external ref kind missing".to_string()),
        ScorecardCriterionKind::InteractionModelIs => criterion
            .interaction_model
            .as_ref()
            .map(|model| format!("interaction model `{}`", runtime_enum_to_snake(model)))
            .unwrap_or_else(|| "interaction model missing".to_string()),
        ScorecardCriterionKind::RemoteOutcomeDispositionIs => criterion
            .remote_outcome_disposition
            .as_ref()
            .map(|disposition| {
                format!(
                    "remote outcome disposition `{}`",
                    runtime_enum_to_snake(disposition)
                )
            })
            .unwrap_or_else(|| "remote outcome disposition missing".to_string()),
        ScorecardCriterionKind::TreatyViolationAbsent => criterion
            .treaty_violation_code
            .as_deref()
            .map(|code| format!("treaty violation `{code}` absent"))
            .unwrap_or_else(|| "no treaty violations present".to_string()),
        ScorecardCriterionKind::ArtifactPresent => "artifact present".to_string(),
        ScorecardCriterionKind::ArtifactAbsent => "artifact absent".to_string(),
        ScorecardCriterionKind::JsonFieldNonempty => "field nonempty".to_string(),
    };

    if matches!(
        criterion.kind,
        ScorecardCriterionKind::ArtifactPresent | ScorecardCriterionKind::ArtifactAbsent
    ) && criterion.artifact_name.is_some()
    {
        return Ok(format!("{status}: {detail} for `{target_label}`"));
    }

    let observed = if matches!(criterion.kind, ScorecardCriterionKind::RegexMatch) {
        scorecard_target_text(
            action,
            criterion.artifact_name.as_deref(),
            criterion.field_path.as_deref(),
        )
        .await?
        .map(|text| format!(" observed {}", compact_json_value(&Value::String(text))))
        .unwrap_or_else(|| " observed <missing>".to_string())
    } else if matches!(criterion.kind, ScorecardCriterionKind::InteractionModelIs) {
        format!(" observed {}", runtime_enum_to_snake(interaction_model))
    } else if matches!(
        criterion.kind,
        ScorecardCriterionKind::RemoteOutcomeDispositionIs
    ) {
        remote_outcome_disposition_for_action(action)
            .as_ref()
            .map(|disposition| format!(" observed {}", runtime_enum_to_snake(disposition)))
            .unwrap_or_else(|| " observed <missing>".to_string())
    } else if matches!(criterion.kind, ScorecardCriterionKind::ExternalRefPresent) {
        criterion
            .external_ref_kind
            .as_deref()
            .map(|kind| {
                let present = action
                    .external_refs
                    .iter()
                    .any(|reference| reference.kind == kind);
                format!(" observed present={present}")
            })
            .unwrap_or_else(|| " observed <missing-kind>".to_string())
    } else if matches!(
        criterion.kind,
        ScorecardCriterionKind::TreatyViolationAbsent
    ) {
        let violations = treaty_violations_for_action(action);
        if let Some(code) = criterion.treaty_violation_code.as_deref() {
            let matched = violations
                .iter()
                .filter(|violation| violation.code == code)
                .map(|violation| violation.summary.clone())
                .collect::<Vec<_>>();
            if matched.is_empty() {
                " observed []".to_string()
            } else {
                format!(" observed {matched:?}")
            }
        } else if violations.is_empty() {
            " observed []".to_string()
        } else {
            format!(
                " observed {:?}",
                violations
                    .iter()
                    .map(|violation| violation.code.clone())
                    .collect::<Vec<_>>()
            )
        }
    } else if matches!(criterion.kind, ScorecardCriterionKind::IncidentAbsent) {
        if let Some(code) = criterion.incident_code.as_deref() {
            let matched = observed_incidents
                .iter()
                .filter(|incident| incident.reason_code == code)
                .map(|incident| incident.summary.clone())
                .collect::<Vec<_>>();
            if matched.is_empty() {
                " observed []".to_string()
            } else {
                format!(" observed {matched:?}")
            }
        } else if observed_incidents.is_empty() {
            " observed []".to_string()
        } else {
            format!(
                " observed {:?}",
                observed_incidents
                    .iter()
                    .map(|incident| incident.reason_code.clone())
                    .collect::<Vec<_>>()
            )
        }
    } else {
        let current = scorecard_target_value(
            action,
            criterion.artifact_name.as_deref(),
            criterion.field_path.as_deref(),
        )
        .await?;
        current
            .as_ref()
            .map(|value| format!(" observed {}", compact_json_value(value)))
            .unwrap_or_else(|| " observed <missing>".to_string())
    };
    Ok(format!(
        "{status}: {detail} on `{target_label}`{path_label}.{observed}"
    ))
}

fn compact_json_value(value: &Value) -> String {
    let raw = match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    let trimmed = raw.trim();
    if trimmed.len() > 120 {
        format!("{}...", &trimmed[..117])
    } else {
        trimmed.to_string()
    }
}

fn json_value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in path.split('.') {
        match current {
            Value::Object(map) => current = map.get(part)?,
            _ => return None,
        }
    }
    Some(current)
}

fn metadata_value_at_path<'a>(metadata: &'a Metadata, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut current = metadata.get(first)?;
    for part in parts {
        current = match current {
            Value::Object(map) => map.get(part)?,
            _ => return None,
        };
    }
    Some(current)
}

fn json_value_is_nonempty(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(text) => !text.trim().is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
        Value::Bool(value) => *value,
        Value::Number(_) => true,
    }
}

fn scorecard_source_tokens(action: &Action, source_path: &str) -> Vec<String> {
    if source_path == "goal_summary" {
        return objective_tokens(&action.goal.summary);
    }
    metadata_value_at_path(&action.inputs, source_path)
        .map(scorecard_value_tokens)
        .unwrap_or_default()
}

fn scorecard_value_tokens(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => objective_tokens(text),
        Value::Array(items) => items.iter().flat_map(scorecard_value_tokens).collect(),
        other => objective_tokens(&other.to_string()),
    }
}

fn lease_failure_code(reason: &str) -> &'static str {
    if reason.contains("revoked") {
        failure_code_lease_revoked()
    } else if reason.contains("expired") {
        failure_code_lease_expired()
    } else {
        failure_code_approval_required()
    }
}

fn local_harness_failure_code(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<LocalHarnessError>() {
        return match error {
            LocalHarnessError::MissingBinary(_) => failure_code_local_harness_missing_binary(),
            LocalHarnessError::Spawn(_) => failure_code_local_harness_spawn_error(),
            LocalHarnessError::Timeout(_) => failure_code_local_harness_timeout(),
            LocalHarnessError::ExitNonZero { .. } => failure_code_local_harness_exit_nonzero(),
            LocalHarnessError::Protocol(_) => failure_code_local_harness_protocol_error(),
        };
    }
    failure_code_route_unavailable()
}

fn openclaw_failure_code(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<OpenClawError>() {
        return match error {
            OpenClawError::MissingAuthEnv(_) => failure_code_openclaw_auth_error(),
            OpenClawError::UnsupportedSessionMode => {
                failure_code_openclaw_unsupported_session_mode()
            }
            OpenClawError::UnsupportedWorkspaceMode => {
                failure_code_openclaw_unsupported_workspace_mode()
            }
            OpenClawError::Connect(_) => failure_code_openclaw_connect_error(),
            OpenClawError::Protocol(_) => failure_code_openclaw_protocol_error(),
            OpenClawError::RunFailed(_) => failure_code_openclaw_run_failed(),
        };
    }
    failure_code_route_unavailable()
}

fn a2a_failure_code(error: &anyhow::Error) -> &'static str {
    if let Some(error) = error.downcast_ref::<A2aError>() {
        return match error {
            A2aError::MissingAuthEnv(_) => failure_code_a2a_auth_error(),
            A2aError::TreatyDenied(_) => failure_code_treaty_denied(),
            A2aError::AgentCard(_) | A2aError::Connect(_) => failure_code_a2a_connect_error(),
            A2aError::Protocol(_) => failure_code_a2a_protocol_error(),
            A2aError::TaskFailed(_) => failure_code_a2a_task_failed(),
        };
    }
    failure_code_route_unavailable()
}

fn next_fallback_label(deterministic_fallback: bool, fallback: &'static str) -> &'static str {
    if deterministic_fallback {
        fallback
    } else {
        "continuity"
    }
}

fn set_action_failed(action: &mut Action, code: &str, reason: String) {
    action.phase = ActionPhase::Failed;
    action.finished_at = Some(now_timestamp());
    action.failure_reason = Some(reason);
    action.failure_code = Some(code.to_string());
}

fn set_action_blocked(action: &mut Action, code: &str, reason: String) {
    action.phase = if code == "a2a_auth_required" {
        ActionPhase::AwaitingApproval
    } else {
        ActionPhase::Blocked
    };
    action.finished_at = None;
    action.failure_reason = Some(reason);
    action.failure_code = Some(code.to_string());
}

fn selected_executor_from_external_refs(external_refs: &[ExternalRef]) -> Option<String> {
    external_ref_value(external_refs, "a2a.remote_principal")
        .map(|principal| format!("a2a.{principal}"))
        .or_else(|| {
            external_ref_value(external_refs, "openclaw.target_agent")
                .map(|agent| format!("openclaw.{agent}"))
        })
        .or_else(|| {
            external_ref_value(external_refs, "local_harness.harness")
                .map(|harness| format!("local_harness.{harness}"))
        })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WorkspaceLockRecord {
    workspace_root: String,
    owner_action_id: String,
    acquired_at: String,
}

struct WorkspaceLockAcquisition {
    lock_path: PathBuf,
    detail: crawfish_types::WorkspaceLockDetail,
}

enum WorkspaceLockAttempt {
    Acquired(WorkspaceLockAcquisition),
    Conflict(crawfish_types::WorkspaceLockDetail),
}

fn is_remote_harness_executor(executor: &str) -> bool {
    executor.starts_with("openclaw.") || executor.starts_with("local_harness.")
}

fn is_remote_agent_executor(executor: &str) -> bool {
    executor.starts_with("a2a.")
}

fn jurisdiction_class_for_action(
    action: &Action,
    encounter: Option<&EncounterRecord>,
) -> JurisdictionClass {
    if action
        .selected_executor
        .as_deref()
        .map(is_remote_harness_executor)
        .unwrap_or(false)
    {
        return JurisdictionClass::RemoteHarness;
    }

    match encounter.map(|encounter| &encounter.trust_domain) {
        Some(TrustDomain::SameOwnerLocal) => JurisdictionClass::SameOwnerLocal,
        Some(TrustDomain::SameDeviceForeignOwner) => JurisdictionClass::SameDeviceForeignOwner,
        Some(_) => JurisdictionClass::ExternalUnknown,
        None => match action
            .counterparty_refs
            .first()
            .map(|counterparty| &counterparty.trust_domain)
        {
            Some(TrustDomain::SameOwnerLocal) => JurisdictionClass::SameOwnerLocal,
            Some(TrustDomain::SameDeviceForeignOwner) => JurisdictionClass::SameDeviceForeignOwner,
            Some(_) => JurisdictionClass::ExternalUnknown,
            None => JurisdictionClass::ExternalUnknown,
        },
    }
}

fn interaction_model_for_action(
    action: &Action,
    encounter: Option<&EncounterRecord>,
) -> crawfish_types::InteractionModel {
    if action
        .selected_executor
        .as_deref()
        .map(is_remote_agent_executor)
        .unwrap_or(false)
    {
        return crawfish_types::InteractionModel::RemoteAgent;
    }

    if action
        .selected_executor
        .as_deref()
        .map(is_remote_harness_executor)
        .unwrap_or(false)
    {
        return crawfish_types::InteractionModel::RemoteHarness;
    }

    match encounter.map(|encounter| &encounter.trust_domain) {
        Some(TrustDomain::SameOwnerLocal) => crawfish_types::InteractionModel::SameOwnerSwarm,
        Some(TrustDomain::SameDeviceForeignOwner) => {
            crawfish_types::InteractionModel::SameDeviceMultiOwner
        }
        Some(_) => crawfish_types::InteractionModel::ExternalUnknown,
        None if matches!(
            action
                .counterparty_refs
                .first()
                .map(|counterparty| &counterparty.trust_domain),
            Some(TrustDomain::SameOwnerLocal)
        ) =>
        {
            crawfish_types::InteractionModel::SameOwnerSwarm
        }
        None if matches!(
            action
                .counterparty_refs
                .first()
                .map(|counterparty| &counterparty.trust_domain),
            Some(TrustDomain::SameDeviceForeignOwner)
        ) =>
        {
            crawfish_types::InteractionModel::SameDeviceMultiOwner
        }
        None if matches!(action.requester.kind, RequesterKind::Agent)
            && action.counterparty_refs.is_empty() =>
        {
            crawfish_types::InteractionModel::ContextSplit
        }
        None if action.initiator_owner.kind == OwnerKind::ServiceAccount
            && action.counterparty_refs.is_empty()
            && !action
                .external_refs
                .iter()
                .any(|reference| reference.kind.starts_with("openclaw.")) =>
        {
            crawfish_types::InteractionModel::ContextSplit
        }
        None => crawfish_types::InteractionModel::ExternalUnknown,
    }
}

fn interaction_model_is_frontier(interaction_model: &crawfish_types::InteractionModel) -> bool {
    !matches!(
        interaction_model,
        crawfish_types::InteractionModel::ContextSplit
    )
}

fn external_ref_value(external_refs: &[ExternalRef], kind: &str) -> Option<String> {
    external_refs
        .iter()
        .find(|reference| reference.kind == kind)
        .map(|reference| reference.value.clone())
}

fn delegation_depth_for_action(action: &Action) -> Option<u32> {
    external_ref_value(&action.external_refs, "a2a.delegation_depth")
        .and_then(|value| value.parse::<u32>().ok())
        .or_else(|| {
            if matches!(
                interaction_model_for_action(action, None),
                crawfish_types::InteractionModel::RemoteAgent
            ) {
                Some(1)
            } else {
                None
            }
        })
}

fn federation_pack_id_for_action(action: &Action) -> Option<String> {
    external_ref_value(&action.external_refs, "a2a.federation_pack")
}

fn remote_followup_context(action: &Action) -> Option<&Value> {
    action.inputs.get("remote_followup")
}

fn active_remote_followup_ref_for_action(action: &Action) -> Option<String> {
    remote_followup_context(action)
        .and_then(Value::as_object)
        .and_then(|value| value.get("request_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn clear_remote_followup_context(action: &mut Action) {
    action.inputs.remove("remote_followup");
}

fn strategy_iteration_for_action(action: &Action) -> u32 {
    action
        .outputs
        .metadata
        .get("strategy_iteration")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(1)
}

fn remote_evidence_status_for_action(action: &Action) -> Option<RemoteEvidenceStatus> {
    action
        .outputs
        .metadata
        .get("federation_remote_evidence_status")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn remote_state_disposition_for_action(action: &Action) -> Option<RemoteStateDisposition> {
    action
        .outputs
        .metadata
        .get("federation_remote_state_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn remote_review_disposition_for_action(action: &Action) -> Option<RemoteReviewDisposition> {
    action
        .outputs
        .metadata
        .get("remote_review_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn federation_decision_for_action(action: &Action) -> Option<FederationDecision> {
    action
        .outputs
        .metadata
        .get("federation_decision")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn remote_outcome_disposition_for_action(
    action: &Action,
) -> Option<crawfish_types::RemoteOutcomeDisposition> {
    action
        .outputs
        .metadata
        .get("treaty_remote_outcome_disposition")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn treaty_violations_for_action(action: &Action) -> Vec<crawfish_types::TreatyViolation> {
    action
        .outputs
        .metadata
        .get("treaty_violations")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

fn set_remote_review_disposition_metadata(
    outputs: &mut ActionOutputs,
    disposition: &RemoteReviewDisposition,
) {
    outputs.metadata.insert(
        "remote_review_disposition".to_string(),
        serde_json::to_value(disposition).unwrap_or(Value::Null),
    );
}

fn remote_review_reason_for_action(
    action: &Action,
    policy_incidents: &[PolicyIncident],
) -> Option<RemoteReviewReason> {
    if matches!(
        remote_evidence_status_for_action(action),
        Some(RemoteEvidenceStatus::ScopeViolation)
    ) || treaty_violations_for_action(action)
        .iter()
        .any(|violation| violation.code == "treaty_scope_violation")
    {
        return Some(RemoteReviewReason::ScopeViolation);
    }
    if matches!(
        remote_evidence_status_for_action(action),
        Some(RemoteEvidenceStatus::MissingRequiredEvidence)
    ) || policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "frontier_enforcement_gap")
    {
        return Some(RemoteReviewReason::EvidenceGap);
    }
    if matches!(
        remote_state_disposition_for_action(action),
        Some(RemoteStateDisposition::Blocked | RemoteStateDisposition::AwaitingApproval)
    ) || policy_incidents
        .iter()
        .any(|incident| incident.reason_code == "remote_state_escalated")
    {
        return Some(RemoteReviewReason::RemoteStateEscalated);
    }
    if matches!(
        remote_outcome_disposition_for_action(action),
        Some(RemoteOutcomeDisposition::ReviewRequired)
    ) {
        return Some(RemoteReviewReason::ResultReviewRequired);
    }
    None
}

fn task_plan_delegated_data_scopes(action: &Action) -> Vec<String> {
    let mut scopes = Vec::new();
    for key in [
        "objective",
        "workspace_root",
        "context_files",
        "constraints",
        "desired_outputs",
        "background",
        "verification_feedback",
        "base_ref",
        "head_ref",
    ] {
        if action.inputs.contains_key(key) {
            scopes.push(key.to_string());
        }
    }
    scopes
}

fn set_treaty_result_metadata(
    outputs: &mut ActionOutputs,
    disposition: &crawfish_types::RemoteOutcomeDisposition,
    violations: &[crawfish_types::TreatyViolation],
) {
    outputs.metadata.insert(
        "treaty_remote_outcome_disposition".to_string(),
        serde_json::to_value(disposition).unwrap_or(Value::Null),
    );
    outputs.metadata.insert(
        "treaty_violations".to_string(),
        serde_json::to_value(violations).unwrap_or_else(|_| Value::Array(Vec::new())),
    );
}

fn set_federation_result_metadata(
    outputs: &mut ActionOutputs,
    decision: &FederationDecision,
    evidence_status: Option<&RemoteEvidenceStatus>,
    state_disposition: Option<&RemoteStateDisposition>,
    result_acceptance: Option<&RemoteResultAcceptance>,
) {
    outputs.metadata.insert(
        "federation_decision".to_string(),
        serde_json::to_value(decision).unwrap_or(Value::Null),
    );
    if let Some(evidence_status) = evidence_status {
        outputs.metadata.insert(
            "federation_remote_evidence_status".to_string(),
            serde_json::to_value(evidence_status).unwrap_or(Value::Null),
        );
    }
    if let Some(state_disposition) = state_disposition {
        outputs.metadata.insert(
            "federation_remote_state_disposition".to_string(),
            serde_json::to_value(state_disposition).unwrap_or(Value::Null),
        );
    }
    if let Some(result_acceptance) = result_acceptance {
        outputs.metadata.insert(
            "federation_remote_result_acceptance".to_string(),
            serde_json::to_value(result_acceptance).unwrap_or(Value::Null),
        );
    }
}

fn default_doctrine_pack(
    action: &Action,
    interaction_model: &crawfish_types::InteractionModel,
    jurisdiction: JurisdictionClass,
) -> DoctrinePack {
    let mut rules = vec![crawfish_types::DoctrineRule {
        id: "results_need_evidence".to_string(),
        title: "Results need evidence".to_string(),
        summary: "Terminal outputs require traceable evidence and, when configured, evaluation."
            .to_string(),
        required_checkpoints: vec![crawfish_types::OversightCheckpoint::PostResult],
    }];

    if interaction_model_is_frontier(interaction_model) {
        rules.insert(
            0,
            crawfish_types::DoctrineRule {
                id: "explicit_jurisdiction".to_string(),
                title: "Explicit jurisdiction before action".to_string(),
                summary: "Authority must be classified before execution begins.".to_string(),
                required_checkpoints: vec![crawfish_types::OversightCheckpoint::Admission],
            },
        );
        rules.insert(
            1,
            crawfish_types::DoctrineRule {
                id: "dispatch_under_control".to_string(),
                title: "Dispatch under control".to_string(),
                summary:
                    "Execution surfaces are selected by the control plane, not by ambient trust."
                        .to_string(),
                required_checkpoints: vec![crawfish_types::OversightCheckpoint::PreDispatch],
            },
        );
        if matches!(
            interaction_model,
            crawfish_types::InteractionModel::RemoteAgent
        ) {
            rules.insert(
                2,
                crawfish_types::DoctrineRule {
                    id: "treaty_before_remote_delegation".to_string(),
                    title: "Remote delegation requires a treaty".to_string(),
                    summary: "Remote agent delegation must prove treaty scope and delegation evidence before dispatch and after results return.".to_string(),
                    required_checkpoints: vec![
                        crawfish_types::OversightCheckpoint::Admission,
                        crawfish_types::OversightCheckpoint::PreDispatch,
                        crawfish_types::OversightCheckpoint::PostResult,
                    ],
                },
            );
        }
    } else {
        rules.insert(
            0,
            crawfish_types::DoctrineRule {
                id: "context_split_coordination".to_string(),
                title: "Context split still needs evidence".to_string(),
                summary:
                    "Role-split or handoff-style sub-agents still need bounded dispatch and inspectable results."
                        .to_string(),
                required_checkpoints: vec![crawfish_types::OversightCheckpoint::PreDispatch],
            },
        );
    }

    if action.capability == "workspace.patch.apply" {
        rules.push(crawfish_types::DoctrineRule {
            id: "mutations_need_gate".to_string(),
            title: "Mutations need an enforceable gate".to_string(),
            summary: "Mutation must pass an explicit pre-mutation gate before write commit."
                .to_string(),
            required_checkpoints: vec![crawfish_types::OversightCheckpoint::PreMutation],
        });
    }

    let (id, title, summary) = if matches!(
        interaction_model,
        crawfish_types::InteractionModel::RemoteAgent
    ) {
        (
            "remote_agent_treaty_v1",
            "Remote agent treaty doctrine",
            "Remote agents are not just another harness; delegation requires treaty scope, checkpoint evidence, and inspectable lineage.",
        )
    } else if interaction_model_is_frontier(interaction_model) {
        (
            "swarm_frontier_v1",
            "Swarm frontier doctrine",
            "Constitutions do not enforce themselves; frontier encounters require runtime checkpoints and evidence.",
        )
    } else {
        (
            "context_split_coordination_v1",
            "Context-split coordination doctrine",
            "Role-split multi-agent patterns still need bounded dispatch and evidence, but they are not frontier governance by default.",
        )
    };

    DoctrinePack {
        id: id.to_string(),
        title: title.to_string(),
        summary: summary.to_string(),
        jurisdiction,
        rules,
    }
}

fn checkpoint_status_for_action(
    action: &Action,
    doctrine: &DoctrinePack,
    has_trace_bundle: bool,
    latest_evaluation: Option<&EvaluationRecord>,
    profile_resolved: bool,
) -> Vec<CheckpointStatus> {
    use crawfish_types::{CheckpointOutcome, OversightCheckpoint};
    let interaction_model = interaction_model_for_action(action, None);
    let has_remote_treaty = external_ref_value(&action.external_refs, "a2a.treaty_pack").is_some();
    let has_remote_receipt =
        external_ref_value(&action.external_refs, "a2a.delegation_receipt").is_some();
    let remote_outcome_disposition = remote_outcome_disposition_for_action(action);

    let requires = |checkpoint: OversightCheckpoint| {
        doctrine
            .rules
            .iter()
            .any(|rule| rule.required_checkpoints.contains(&checkpoint))
    };

    vec![
        CheckpointStatus {
            checkpoint: OversightCheckpoint::Admission,
            required: requires(OversightCheckpoint::Admission),
            outcome: CheckpointOutcome::Passed,
            reason: Some("action entered the control plane through admission".to_string()),
        },
        CheckpointStatus {
            checkpoint: OversightCheckpoint::PreDispatch,
            required: requires(OversightCheckpoint::PreDispatch),
            outcome: if action.selected_executor.is_some()
                && (!matches!(
                    interaction_model,
                    crawfish_types::InteractionModel::RemoteAgent
                ) || has_remote_treaty)
                || matches!(
                    action.phase,
                    ActionPhase::Completed | ActionPhase::Failed | ActionPhase::Blocked
                ) {
                CheckpointOutcome::Passed
            } else {
                CheckpointOutcome::Pending
            },
            reason: action
                .selected_executor
                .as_ref()
                .map(|executor| format!("executor selected: {executor}")),
        },
        CheckpointStatus {
            checkpoint: OversightCheckpoint::PreMutation,
            required: requires(OversightCheckpoint::PreMutation),
            outcome: if action.capability != "workspace.patch.apply" {
                CheckpointOutcome::Skipped
            } else if action.lock_detail.is_some() || action.phase == ActionPhase::Completed {
                CheckpointOutcome::Passed
            } else if action.phase == ActionPhase::AwaitingApproval {
                CheckpointOutcome::Pending
            } else {
                CheckpointOutcome::Failed
            },
            reason: if action.capability != "workspace.patch.apply" {
                Some("capability is proposal-only".to_string())
            } else {
                action.failure_reason.clone()
            },
        },
        CheckpointStatus {
            checkpoint: OversightCheckpoint::PostResult,
            required: requires(OversightCheckpoint::PostResult),
            outcome: if !matches!(
                action.phase,
                ActionPhase::Completed
                    | ActionPhase::Failed
                    | ActionPhase::Blocked
                    | ActionPhase::Expired
            ) {
                CheckpointOutcome::Pending
            } else if has_trace_bundle
                && (!evaluation_required_for_action(action)
                    || (profile_resolved && latest_evaluation.is_some()))
                && (!matches!(
                    interaction_model,
                    crawfish_types::InteractionModel::RemoteAgent
                ) || (has_remote_receipt
                    && matches!(
                        remote_outcome_disposition,
                        Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
                    )))
            {
                CheckpointOutcome::Passed
            } else {
                CheckpointOutcome::Failed
            },
            reason: if !has_trace_bundle {
                Some("trace bundle not available".to_string())
            } else if evaluation_required_for_action(action) && !profile_resolved {
                Some("evaluation profile required but unresolved".to_string())
            } else if evaluation_required_for_action(action) && latest_evaluation.is_none() {
                Some("evaluation required but missing".to_string())
            } else if matches!(
                interaction_model,
                crawfish_types::InteractionModel::RemoteAgent
            ) && !has_remote_receipt
            {
                Some("remote delegation receipt is missing".to_string())
            } else if matches!(
                interaction_model,
                crawfish_types::InteractionModel::RemoteAgent
            ) && !matches!(
                remote_outcome_disposition,
                Some(crawfish_types::RemoteOutcomeDisposition::Accepted)
            ) {
                Some("remote outcome did not satisfy treaty post-result governance".to_string())
            } else {
                Some("terminal evidence present".to_string())
            },
        },
    ]
}

fn evaluation_required_for_action(action: &Action) -> bool {
    matches!(
        action.capability.as_str(),
        "task.plan" | "coding.patch.plan" | "repo.review" | "incident.enrich"
    ) || action.contract.quality.evaluation_hook.is_some()
        || action.contract.quality.evaluation_profile.is_some()
}

fn legacy_evaluation_hook_profile_name(hook: &str) -> Option<&'static str> {
    match hook {
        "operator_review_queue" => Some("task_plan_default"),
        "deterministic_scorecard" => Some("repo_review_default"),
        _ => None,
    }
}

fn builtin_evaluation_profiles() -> BTreeMap<String, EvaluationProfile> {
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

fn builtin_scorecards() -> BTreeMap<String, ScorecardSpec> {
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

fn builtin_evaluation_datasets() -> BTreeMap<String, EvaluationDataset> {
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

fn default_alert_rule_frontier_gap() -> AlertRule {
    AlertRule {
        id: "frontier_gap_detected".to_string(),
        name: "Frontier gap detected".to_string(),
        trigger: "policy_incident".to_string(),
        severity: "warning".to_string(),
    }
}

fn default_alert_rule_evaluation_attention() -> AlertRule {
    AlertRule {
        id: "evaluation_attention_required".to_string(),
        name: "Evaluation attention required".to_string(),
        trigger: "evaluation_attention".to_string(),
        severity: "info".to_string(),
    }
}

fn builtin_alert_rules() -> BTreeMap<String, AlertRule> {
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

fn builtin_pairwise_profiles() -> BTreeMap<String, PairwiseProfile> {
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

fn builtin_profile_name_for_action(action: &Action) -> Option<&'static str> {
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

fn builtin_pairwise_profile_name_for_capability(capability: &str) -> Option<&'static str> {
    match capability {
        "task.plan" | "coding.patch.plan" => Some("task_plan_pairwise_default"),
        _ => None,
    }
}

fn replay_routes_for_executor(executor: &str) -> (Vec<String>, Vec<String>) {
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

fn alert_rule_matches(
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

fn alert_summary_for_rule(
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

fn agent_state_name(state: &AgentState) -> &'static str {
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

fn health_status_name(status: &HealthStatus) -> &'static str {
    match status {
        HealthStatus::Unknown => "unknown",
        HealthStatus::Healthy => "healthy",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Unhealthy => "unhealthy",
    }
}

fn degraded_profile_name(profile: &DegradedProfileName) -> &'static str {
    match profile {
        DegradedProfileName::ReadOnly => "read_only",
        DegradedProfileName::DependencyIsolation => "dependency_isolation",
        DegradedProfileName::BudgetGuard => "budget_guard",
        DegradedProfileName::ProviderFailover => "provider_failover",
    }
}

fn continuity_mode_name(mode: &ContinuityModeName) -> &'static str {
    match mode {
        ContinuityModeName::DeterministicOnly => "deterministic_only",
        ContinuityModeName::StoreAndForward => "store_and_forward",
        ContinuityModeName::HumanHandoff => "human_handoff",
        ContinuityModeName::Suspended => "suspended",
    }
}

fn build_pairwise_case_result(
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

fn maybe_enqueue_pairwise_review_item(
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
    async fn sync_remote_review_state(
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

    async fn execute_evaluation_run_internal(
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

    async fn execute_pairwise_evaluation_run_internal(
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

    fn builtin_federation_pack_template(&self) -> FederationPack {
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

    fn resolve_federation_pack_by_id(
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

    fn remote_followup_reasons_for_bundle(
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

    fn requested_evidence_for_bundle(&self, bundle: &RemoteEvidenceBundle) -> Vec<String> {
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

    async fn create_remote_followup_request(
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

    async fn close_active_remote_followup_request(
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

    async fn list_federation_packs(&self) -> anyhow::Result<FederationPackListResponse> {
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

    async fn get_federation_pack(
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

fn merge_external_refs(mut lhs: Vec<ExternalRef>, rhs: Vec<ExternalRef>) -> Vec<ExternalRef> {
    for reference in rhs {
        let exists = lhs.iter().any(|candidate| {
            candidate.kind == reference.kind
                && candidate.value == reference.value
                && candidate.endpoint == reference.endpoint
        });
        if !exists {
            lhs.push(reference);
        }
    }
    lhs
}

fn merge_artifact_refs(
    mut lhs: Vec<crawfish_types::ArtifactRef>,
    rhs: Vec<crawfish_types::ArtifactRef>,
) -> Vec<crawfish_types::ArtifactRef> {
    for artifact in rhs {
        let exists = lhs
            .iter()
            .any(|candidate| candidate.kind == artifact.kind && candidate.path == artifact.path);
        if !exists {
            lhs.push(artifact);
        }
    }
    lhs
}

async fn verify_task_plan_outputs(
    action: &Action,
    outputs: &ActionOutputs,
    iteration: u32,
    feedback_policy: &FeedbackPolicy,
) -> anyhow::Result<TaskPlanVerificationResult> {
    let json_artifact = outputs
        .artifacts
        .iter()
        .find(|artifact| artifact.path.ends_with("task_plan.json"))
        .cloned();
    let markdown_artifact = outputs
        .artifacts
        .iter()
        .find(|artifact| artifact.path.ends_with("task_plan.md"))
        .cloned();

    let mut failures = Vec::new();
    if json_artifact.is_none() {
        failures.push("missing task_plan.json artifact".to_string());
    }
    if markdown_artifact.is_none() {
        failures.push("missing task_plan.md artifact".to_string());
    }

    let artifact = if let Some(json_artifact) = &json_artifact {
        Some(load_json_artifact::<crawfish_types::TaskPlanArtifact>(json_artifact).await?)
    } else {
        None
    };
    let markdown = if let Some(markdown_artifact) = &markdown_artifact {
        Some(tokio::fs::read_to_string(&markdown_artifact.path).await?)
    } else {
        None
    };

    if let Some(artifact) = &artifact {
        if artifact.ordered_steps.len() < 2 {
            failures.push("task plan must contain at least two ordered steps".to_string());
        }
        if artifact.risks.is_empty() {
            failures.push("task plan must include at least one risk".to_string());
        }
        if artifact.assumptions.is_empty() {
            failures.push("task plan must include at least one assumption".to_string());
        }
        if artifact.confidence_summary.trim().is_empty() {
            failures.push("task plan must include a confidence summary".to_string());
        }
    }

    let mut combined_text = String::new();
    if let Some(artifact) = &artifact {
        combined_text.push_str(&serde_json::to_string(artifact)?);
        combined_text.push('\n');
    }
    if let Some(markdown) = &markdown {
        combined_text.push_str(markdown);
    }
    let lowered = combined_text.to_lowercase();

    if let Some(objective) = action.inputs.get("objective").and_then(Value::as_str) {
        let missing_tokens = extract_key_tokens(objective)
            .into_iter()
            .filter(|token| !lowered.contains(token))
            .collect::<Vec<_>>();
        if !missing_tokens.is_empty() {
            failures.push(format!(
                "task plan does not sufficiently cover objective tokens: {}",
                missing_tokens.join(", ")
            ));
        }
    }

    let missing_outputs = metadata_string_array(&action.inputs, "desired_outputs")
        .into_iter()
        .filter(|output| !lowered.contains(&output.to_lowercase()))
        .collect::<Vec<_>>();
    if !missing_outputs.is_empty() {
        failures.push(format!(
            "task plan does not cover desired outputs: {}",
            missing_outputs.join(", ")
        ));
    }

    if failures.is_empty() {
        return Ok(TaskPlanVerificationResult {
            passed: true,
            summary: VerificationSummary {
                status: VerificationStatus::Passed,
                iterations_completed: iteration,
                last_feedback: None,
                last_failure_code: None,
            },
            feedback: None,
        });
    }

    let feedback = build_task_plan_feedback(feedback_policy, &failures);
    Ok(TaskPlanVerificationResult {
        passed: false,
        summary: VerificationSummary {
            status: VerificationStatus::Failed,
            iterations_completed: iteration,
            last_feedback: Some(feedback.clone()),
            last_failure_code: Some(failure_code_verification_failed().to_string()),
        },
        feedback: Some(feedback),
    })
}

fn build_task_plan_feedback(policy: &FeedbackPolicy, failures: &[String]) -> String {
    let report = failures.join("; ");
    match policy {
        FeedbackPolicy::InjectReason => {
            format!("Address the following verification gaps: {report}")
        }
        FeedbackPolicy::AppendReport => {
            format!("Verification report:\n- {}", failures.join("\n- "))
        }
        FeedbackPolicy::Handoff => {
            format!("Verification did not pass and needs explicit operator review: {report}")
        }
    }
}

fn extract_key_tokens(text: &str) -> Vec<String> {
    const STOPWORDS: &[&str] = &[
        "about", "after", "around", "before", "build", "change", "changes", "check", "checks",
        "ensure", "from", "into", "plan", "safe", "task", "that", "the", "this", "with",
    ];

    let mut tokens = text
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter_map(|token| {
            let lowered = token.trim().to_lowercase();
            if lowered.len() < 4 || STOPWORDS.contains(&lowered.as_str()) {
                return None;
            }
            Some(lowered)
        })
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    tokens.truncate(3);
    tokens
}

fn metadata_string_array(metadata: &crawfish_types::Metadata, key: &str) -> Vec<String> {
    metadata
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
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
        .route(
            "/v1/actions/{id}/remote-evidence",
            get(action_remote_evidence_handler),
        )
        .route(
            "/v1/actions/{id}/remote-followups",
            get(action_remote_followups_handler),
        )
        .route(
            "/v1/actions/{id}/remote-followups/{followup_id}/dispatch",
            post(action_remote_followup_dispatch_handler),
        )
        .route("/v1/actions/{id}/trace", get(action_trace_handler))
        .route(
            "/v1/actions/{id}/evaluations",
            get(action_evaluations_handler),
        )
        .route("/v1/actions/{id}/approve", post(approve_action_handler))
        .route("/v1/actions/{id}/reject", post(reject_action_handler))
        .route("/v1/leases/{id}/revoke", post(revoke_lease_handler))
        .route("/v1/review-queue", get(review_queue_handler))
        .route(
            "/v1/review-queue/{id}/resolve",
            post(resolve_review_queue_item_handler),
        )
        .route("/v1/evaluation/datasets", get(evaluation_datasets_handler))
        .route(
            "/v1/evaluation/datasets/{name}",
            get(evaluation_dataset_detail_handler),
        )
        .route("/v1/evaluation/runs", post(start_evaluation_run_handler))
        .route(
            "/v1/evaluation/runs/{id}",
            get(evaluation_run_detail_handler),
        )
        .route(
            "/v1/evaluation/compare",
            post(start_pairwise_evaluation_run_handler),
        )
        .route(
            "/v1/evaluation/compare/{id}",
            get(pairwise_evaluation_run_detail_handler),
        )
        .route("/v1/alerts", get(alert_list_handler))
        .route("/v1/alerts/{id}/ack", post(alert_ack_handler))
        .route("/v1/treaties", get(treaty_list_handler))
        .route("/v1/treaties/{id}", get(treaty_detail_handler))
        .route("/v1/federation/packs", get(federation_list_handler))
        .route("/v1/federation/packs/{id}", get(federation_detail_handler))
        .route(
            "/v1/inbound/openclaw/actions",
            post(openclaw_submit_action_handler),
        )
        .route(
            "/v1/inbound/openclaw/actions/{id}/inspect",
            post(openclaw_action_detail_handler),
        )
        .route(
            "/v1/inbound/openclaw/actions/{id}/events",
            post(openclaw_action_events_handler),
        )
        .route(
            "/v1/inbound/openclaw/agents/{id}/status",
            post(openclaw_agent_status_handler),
        )
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
) -> Result<Json<SwarmStatusResponse>, RuntimeError> {
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

async fn action_remote_evidence_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionRemoteEvidenceResponse>, RuntimeError> {
    supervisor
        .get_action_remote_evidence(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

async fn action_remote_followups_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionRemoteFollowupsResponse>, RuntimeError> {
    supervisor
        .get_action_remote_followups(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

async fn action_remote_followup_dispatch_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath((id, followup_id)): AxumPath<(String, String)>,
    Json(request): Json<DispatchRemoteFollowupRequest>,
) -> Result<Json<DispatchRemoteFollowupResponse>, RuntimeError> {
    match supervisor
        .dispatch_remote_followup(&id, &followup_id, request)
        .await
    {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.contains("not found") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn action_trace_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionTraceResponse>, RuntimeError> {
    supervisor
        .get_action_trace(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("action not found: {id}")))
}

async fn action_evaluations_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ActionEvaluationsResponse>, RuntimeError> {
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
            .list_action_evaluations(&id)
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
        Err(error) => Err(map_submit_error(error)),
    }
}

async fn openclaw_submit_action_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<OpenClawInboundActionRequest>,
) -> Result<Json<OpenClawInboundActionResponse>, RuntimeError> {
    supervisor.submit_openclaw_action(request).await.map(Json)
}

async fn openclaw_action_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(context): Json<OpenClawInspectionContext>,
) -> Result<Json<ActionDetail>, RuntimeError> {
    supervisor
        .inspect_openclaw_action(&id, context)
        .await
        .map(Json)
}

async fn openclaw_action_events_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(context): Json<OpenClawInspectionContext>,
) -> Result<Json<ActionEventsResponse>, RuntimeError> {
    supervisor
        .list_openclaw_action_events(&id, context)
        .await
        .map(Json)
}

async fn openclaw_agent_status_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(context): Json<OpenClawInspectionContext>,
) -> Result<Json<OpenClawAgentStatusResponse>, RuntimeError> {
    supervisor
        .inspect_openclaw_agent_status(&id, context)
        .await
        .map(Json)
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

async fn review_queue_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Query(query): Query<ReviewQueueQuery>,
) -> Result<Json<ReviewQueueResponse>, RuntimeError> {
    let mut response = supervisor
        .list_review_queue()
        .await
        .map_err(RuntimeError::Internal)?;
    if let Some(kind) = query.kind.as_deref() {
        response.items.retain(|item| match kind {
            "action" | "action_eval" => item.kind == ReviewQueueKind::ActionEval,
            "pairwise" | "pairwise_eval" => item.kind == ReviewQueueKind::PairwiseEval,
            "remote" | "remote_result_review" => item.kind == ReviewQueueKind::RemoteResultReview,
            _ => true,
        });
    }
    Ok(Json(response))
}

async fn resolve_review_queue_item_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<ResolveReviewQueueItemRequest>,
) -> Result<Json<ResolveReviewQueueItemResponse>, RuntimeError> {
    match supervisor.resolve_review_queue_item(&id, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("review queue item not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn evaluation_datasets_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<EvaluationDatasetsResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_evaluation_datasets()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn evaluation_dataset_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<EvaluationDatasetDetailResponse>, RuntimeError> {
    supervisor
        .get_evaluation_dataset(&name)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("dataset not found: {name}")))
}

async fn start_evaluation_run_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<StartEvaluationRunRequest>,
) -> Result<Json<StartEvaluationRunResponse>, RuntimeError> {
    match supervisor.start_evaluation_run(request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("dataset not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn evaluation_run_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ExperimentRunDetailResponse>, RuntimeError> {
    supervisor
        .get_evaluation_run(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("evaluation run not found: {id}")))
}

async fn start_pairwise_evaluation_run_handler(
    State(supervisor): State<Arc<Supervisor>>,
    Json(request): Json<StartPairwiseEvaluationRunRequest>,
) -> Result<Json<StartPairwiseEvaluationRunResponse>, RuntimeError> {
    match supervisor.start_pairwise_evaluation_run(request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("dataset not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn pairwise_evaluation_run_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<PairwiseExperimentRunDetailResponse>, RuntimeError> {
    supervisor
        .get_pairwise_evaluation_run(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("pairwise evaluation run not found: {id}")))
}

async fn alert_list_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<AlertListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_alerts()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn alert_ack_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
    Json(request): Json<AcknowledgeAlertRequest>,
) -> Result<Json<AcknowledgeAlertResponse>, RuntimeError> {
    match supervisor.acknowledge_alert(&id, request).await {
        Ok(response) => Ok(Json(response)),
        Err(error) => {
            let message = error.to_string();
            if message.starts_with("alert not found:") {
                Err(RuntimeError::NotFound(message))
            } else {
                Err(RuntimeError::BadRequest(message))
            }
        }
    }
}

async fn treaty_list_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<TreatyListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_treaties()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn treaty_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<TreatyDetailResponse>, RuntimeError> {
    supervisor
        .get_treaty(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("treaty not found: {id}")))
}

async fn federation_list_handler(
    State(supervisor): State<Arc<Supervisor>>,
) -> Result<Json<FederationPackListResponse>, RuntimeError> {
    Ok(Json(
        supervisor
            .list_federation_packs()
            .await
            .map_err(RuntimeError::Internal)?,
    ))
}

async fn federation_detail_handler(
    State(supervisor): State<Arc<Supervisor>>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<FederationPackDetailResponse>, RuntimeError> {
    supervisor
        .get_federation_pack(&id)
        .await
        .map_err(RuntimeError::Internal)?
        .map(Json)
        .ok_or_else(|| RuntimeError::NotFound(format!("federation pack not found: {id}")))
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

fn map_submit_error(error: anyhow::Error) -> RuntimeError {
    let message = error.to_string();
    if message.starts_with("agent not found:") {
        RuntimeError::NotFound(message)
    } else if message.starts_with("invalid action request:") {
        RuntimeError::BadRequest(message)
    } else if message.contains("denied") || message.contains("consent") {
        RuntimeError::Forbidden(message)
    } else {
        RuntimeError::Internal(error)
    }
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

    async fn build_supervisor_with_config(
        dir: &Path,
        config_contents: String,
    ) -> anyhow::Result<Arc<Supervisor>> {
        build_supervisor_with_config_and_openclaw_gateway(dir, config_contents, None).await
    }

    async fn build_supervisor_with_config_and_openclaw_gateway(
        dir: &Path,
        config_contents: String,
        openclaw_gateway_url: Option<&str>,
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
            "task_planner",
            "workspace_editor",
        ] {
            let mut manifest = std::fs::read_to_string(format!(
                "{}/../../examples/hero-swarm/agents/{agent}.toml",
                env!("CARGO_MANIFEST_DIR")
            ))?;
            if agent == "task_planner" {
                manifest = manifest.replace(
                    "command = \"claude\"",
                    "command = \"__test_missing_claude__\"",
                );
                manifest = manifest.replace(
                    "command = \"codex\"",
                    "command = \"__test_missing_codex__\"",
                );
                if let Some(gateway_url) = openclaw_gateway_url {
                    manifest = manifest.replace("ws://127.0.0.1:9988/gateway", gateway_url);
                }
            }
            tokio::fs::write(dir.join(format!("agents/{agent}.toml")), manifest).await?;
        }
        let supervisor = Arc::new(Supervisor::from_config_path(&dir.join("Crawfish.toml")).await?);
        supervisor.run_once().await?;
        Ok(supervisor)
    }

    async fn build_supervisor_with_task_planner_manifest(
        dir: &Path,
        task_planner_manifest: String,
        openclaw_gateway_url: Option<&str>,
    ) -> anyhow::Result<Arc<Supervisor>> {
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml").to_string();
        build_supervisor_with_task_planner_manifest_and_config(
            dir,
            task_planner_manifest,
            config,
            openclaw_gateway_url,
        )
        .await
    }

    async fn build_supervisor_with_task_planner_manifest_and_config(
        dir: &Path,
        task_planner_manifest: String,
        config_contents: String,
        openclaw_gateway_url: Option<&str>,
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
            let manifest = std::fs::read_to_string(format!(
                "{}/../../examples/hero-swarm/agents/{agent}.toml",
                env!("CARGO_MANIFEST_DIR")
            ))?;
            tokio::fs::write(dir.join(format!("agents/{agent}.toml")), manifest).await?;
        }
        let manifest = if let Some(gateway_url) = openclaw_gateway_url {
            task_planner_manifest.replace("ws://127.0.0.1:9988/gateway", gateway_url)
        } else {
            task_planner_manifest
        };
        tokio::fs::write(dir.join("agents/task_planner.toml"), manifest).await?;
        let supervisor = Arc::new(Supervisor::from_config_path(&dir.join("Crawfish.toml")).await?);
        supervisor.run_once().await?;
        Ok(supervisor)
    }

    async fn build_supervisor(dir: &Path) -> anyhow::Result<Arc<Supervisor>> {
        build_supervisor_with_config(
            dir,
            include_str!("../../../examples/hero-swarm/Crawfish.toml").to_string(),
        )
        .await
    }

    async fn build_supervisor_with_mcp(
        dir: &Path,
        mcp_url: &str,
    ) -> anyhow::Result<Arc<Supervisor>> {
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:8877/sse", mcp_url);
        build_supervisor_with_config(dir, config).await
    }

    async fn build_supervisor_with_openclaw(dir: &Path) -> anyhow::Result<Arc<Supervisor>> {
        build_supervisor_with_config(
            dir,
            include_str!("../../../examples/hero-swarm/Crawfish.toml").to_string(),
        )
        .await
    }

    async fn build_supervisor_with_openclaw_gateway(
        dir: &Path,
        gateway_url: &str,
    ) -> anyhow::Result<Arc<Supervisor>> {
        build_supervisor_with_config_and_openclaw_gateway(
            dir,
            include_str!("../../../examples/hero-swarm/Crawfish.toml").to_string(),
            Some(gateway_url),
        )
        .await
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

    fn openclaw_caller(caller_id: &str) -> OpenClawCallerContext {
        OpenClawCallerContext {
            caller_id: caller_id.to_string(),
            session_id: format!("{caller_id}-session"),
            channel_id: "gateway".to_string(),
            workspace_root: None,
            scopes: vec!["crawfish.read".to_string(), "crawfish.submit".to_string()],
            display_name: None,
            trace_ids: crawfish_types::Metadata::default(),
        }
    }

    fn task_plan_request(dir: &Path, objective: &str) -> SubmitActionRequest {
        SubmitActionRequest {
            target_agent_id: "task_planner".to_string(),
            requester: RequesterRef {
                kind: RequesterKind::User,
                id: "operator".to_string(),
            },
            initiator_owner: local_owner("local-dev"),
            capability: "task.plan".to_string(),
            goal: crawfish_types::GoalSpec {
                summary: objective.to_string(),
                details: None,
            },
            inputs: std::collections::BTreeMap::from([
                ("objective".to_string(), serde_json::json!(objective)),
                (
                    "workspace_root".to_string(),
                    serde_json::json!(dir.display().to_string()),
                ),
                (
                    "desired_outputs".to_string(),
                    serde_json::json!(["plan", "risks"]),
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
        }
    }

    async fn write_executable_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        tokio::fs::write(&path, body).await.unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        path
    }

    fn local_task_planner_manifest(
        claude_command: &str,
        codex_command: &str,
        openclaw_gateway_url: &str,
    ) -> String {
        format!(
            r#"id = "task_planner"
role = "task_planner"
trust_domain = "same_owner_local"
capabilities = ["task.plan"]
exposed_capabilities = ["task.plan"]
default_data_boundaries = ["owner_local"]

[owner]
kind = "human"
id = "local-dev"
display_name = "Local Developer"

[contract_defaults.execution]
preferred_harnesses = ["claude_code", "codex", "a2a", "openclaw"]
fallback_chain = ["deterministic"]

[contract_defaults.safety]
approval_policy = "on_mutation"
mutation_mode = "proposal_only"

[strategy_defaults."task.plan"]
mode = "verify_loop"
feedback_policy = "inject_reason"

[strategy_defaults."task.plan".verification_spec]
require_all = true
on_failure = "retry_with_feedback"
checks = []

[strategy_defaults."task.plan".stop_budget]
max_iterations = 3

[[adapters]]
adapter = "local_harness"
capability = "task.plan"
harness = "claude_code"
command = "{claude_command}"
args = []
required_scopes = ["planning:read", "planning:propose"]
lease_required = false
workspace_policy = "crawfish_managed"
env_allowlist = ["PATH", "HOME", "CODEX_HOME", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"]
timeout_seconds = 5

[[adapters]]
adapter = "local_harness"
capability = "task.plan"
harness = "codex"
command = "{codex_command}"
args = ["exec", "--skip-git-repo-check"]
required_scopes = ["planning:read", "planning:propose"]
lease_required = false
workspace_policy = "crawfish_managed"
env_allowlist = ["PATH", "HOME", "CODEX_HOME", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"]
timeout_seconds = 5

[[adapters]]
adapter = "openclaw"
gateway_url = "{openclaw_gateway_url}"
auth_ref = "OPENCLAW_GATEWAY_TOKEN"
target_agent = "task-planner"
session_mode = "ephemeral"
caller_owner_mapping = "required"
default_trust_domain = "same_device_foreign_owner"
required_scopes = ["planning:read", "planning:propose"]
lease_required = false
workspace_policy = "crawfish_managed"

[[adapters]]
adapter = "a2a"
capability = "task.plan"
agent_card_url = "http://127.0.0.1:7788/agent-card.json"
auth_ref = "A2A_REMOTE_TOKEN"
treaty_pack = "remote_task_planning"
required_scopes = ["planning:read", "planning:propose"]
streaming_mode = "prefer_streaming"
allow_in_task_auth = false
"#
        )
    }

    async fn spawn_api_server(
        supervisor: Arc<Supervisor>,
    ) -> (tokio::task::JoinHandle<()>, PathBuf) {
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
        (handle, socket_path)
    }

    async fn post_uds_json<T: serde::Serialize>(
        socket_path: &Path,
        endpoint: &str,
        payload: &T,
    ) -> (StatusCode, Value) {
        let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
        let uri: Uri = hyperlocal::Uri::new(socket_path, endpoint).into();
        let request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(serde_json::to_vec(payload).unwrap())))
            .unwrap();
        let response = client.request(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    async fn get_uds_json(socket_path: &Path, endpoint: &str) -> (StatusCode, Value) {
        let client: Client<hyperlocal::UnixConnector, Full<Bytes>> = Client::unix();
        let uri: Uri = hyperlocal::Uri::new(socket_path, endpoint).into();
        let request = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = client.request(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json = serde_json::from_slice(&body).unwrap();
        (status, json)
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

    async fn spawn_mock_openclaw_gateway() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let run_counter = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let run_counter = Arc::clone(&run_counter);
                tokio::spawn(async move {
                    let ws =
                        accept_hdr_async(stream, |_request: &WsRequest, response: WsResponse| {
                            Ok(response)
                        })
                        .await
                        .unwrap();
                    let (mut sink, mut source) = ws.split();
                    let mut active_run_id: Option<String> = None;
                    let mut last_prompt = String::new();
                    while let Some(message) = source.next().await {
                        let WsMessage::Text(text) = message.unwrap() else {
                            continue;
                        };
                        let frame: Value = serde_json::from_str(&text).unwrap();
                        let method = frame.get("method").and_then(Value::as_str).unwrap();
                        let id = frame.get("id").and_then(Value::as_str).unwrap();
                        match method {
                            "connect" => {
                                sink.send(WsMessage::Text(
                                    serde_json::json!({
                                        "type":"res",
                                        "id": id,
                                        "ok": true,
                                        "result": {"sessionKey":"gateway-session"}
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            }
                            "agent" => {
                                let attempt = run_counter.fetch_add(1, Ordering::SeqCst) + 1;
                                let run_id = format!("run-{attempt}");
                                last_prompt = frame
                                    .pointer("/params/message")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string();
                                active_run_id = Some(run_id.clone());
                                sink.send(WsMessage::Text(
                                    serde_json::json!({
                                        "type":"event",
                                        "event":"assistant",
                                        "runId":run_id,
                                        "payload":{
                                            "stream":"assistant",
                                            "text": format!("OpenClaw planning attempt {attempt}")
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                                sink.send(WsMessage::Text(
                                    serde_json::json!({
                                        "type":"res",
                                        "id": id,
                                        "ok": true,
                                        "result": {"runId": active_run_id}
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                            }
                            "agent.wait" => {
                                let run_id = active_run_id
                                    .clone()
                                    .expect("agent.wait should follow agent");
                                let has_feedback =
                                    last_prompt.contains("Verification feedback to address:");
                                sink.send(WsMessage::Text(
                                    serde_json::json!({
                                        "type":"event",
                                        "event":"tool",
                                        "runId":run_id,
                                        "payload":{
                                            "stream":"tool",
                                            "message":"read target files and shape proposal"
                                        }
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                                let result = if has_feedback {
                                    serde_json::json!({
                                        "status":"completed",
                                        "confidence":"High confidence once the checklist and rollout notes are included.",
                                        "text":"# Task Plan\n1. Inspect `src/lib.rs` and the repo indexing boundary.\n2. Add validation checks around the indexing path and capture a rollout checklist.\n3. Update tests and document the rollout checklist.\nRisks: config drift can hide indexing regressions.\nAssumptions: the rollout checklist can stay proposal-only for this task.\nTest: cargo test --workspace\nChecklist: include rollout checklist in the final proposal."
                                    })
                                } else {
                                    serde_json::json!({
                                        "status":"completed",
                                        "text":"# Task Plan\n1. Inspect `src/lib.rs`.\n2. Add validation checks around the repo indexing path.\nRisks: config drift.\nTest: cargo test --workspace"
                                    })
                                };
                                sink.send(WsMessage::Text(
                                    serde_json::json!({
                                        "type":"res",
                                        "id": id,
                                        "ok": true,
                                        "result": result
                                    })
                                    .to_string()
                                    .into(),
                                ))
                                .await
                                .unwrap();
                                break;
                            }
                            other => panic!("unexpected gateway method: {other}"),
                        }
                    }
                });
            }
        });
        format!("ws://{address}")
    }

    #[derive(Clone, Copy)]
    enum RuntimeA2aMode {
        StreamingCompleted,
        InputRequired,
        AuthRequired,
        StreamingMissingTaskRef,
    }

    #[derive(Clone)]
    struct RuntimeA2aState {
        mode: RuntimeA2aMode,
    }

    async fn spawn_runtime_a2a_server(mode: RuntimeA2aMode) -> String {
        let app = Router::new()
            .route("/agent-card.json", get(runtime_a2a_agent_card))
            .route("/rpc", post(runtime_a2a_rpc))
            .with_state(RuntimeA2aState { mode });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}/agent-card.json")
    }

    async fn runtime_a2a_agent_card(
        AxumState(_state): AxumState<RuntimeA2aState>,
        request: Request<axum::body::Body>,
    ) -> Json<Value> {
        let host = request
            .headers()
            .get("host")
            .and_then(|header| header.to_str().ok())
            .unwrap_or("127.0.0.1:0");
        Json(serde_json::json!({
            "id": "remote-task-planner",
            "name": "remote-task-planner",
            "url": format!("http://{host}/rpc"),
            "capabilities": ["task.plan"],
            "skills": [{"id": "task.plan", "name": "task.plan", "tags": ["task.plan"]}]
        }))
    }

    async fn runtime_a2a_rpc(
        AxumState(state): AxumState<RuntimeA2aState>,
        Json(payload): Json<Value>,
    ) -> Json<Value> {
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let id = payload.get("id").cloned().unwrap_or(Value::Null);
        let response = match (state.mode, method) {
            (RuntimeA2aMode::StreamingCompleted, "message/stream") => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "task": {
                        "id": "remote-task-1",
                        "status": { "state": "completed" },
                        "result": {
                            "text": "# Task Plan\n1. Inspect the objective and the context files.\n2. Produce a proposal-only plan with ordered steps and rollout notes.\nRisks: remote coordination can drift from local assumptions.\nAssumptions: the plan remains proposal-only.\nTest: verify the proposal covers desired outputs."
                        }
                    },
                    "events": [
                        { "kind": "lifecycle", "state": "working" },
                        { "kind": "assistant", "text": "Remote planner is preparing a task plan." }
                    ]
                }
            }),
            (RuntimeA2aMode::StreamingMissingTaskRef, "message/stream") => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "task": {
                        "id": "",
                        "status": { "state": "completed" },
                        "result": {
                            "text": "# Task Plan\n1. Inspect the objective and the context files.\n2. Produce a proposal-only plan with ordered steps and rollout notes.\nRisks: remote coordination can drift from local assumptions.\nAssumptions: the plan remains proposal-only.\nTest: verify the proposal covers desired outputs."
                        }
                    },
                    "events": [
                        { "kind": "lifecycle", "state": "working" },
                        { "kind": "assistant", "text": "Remote planner completed without a durable task id." }
                    ]
                }
            }),
            (RuntimeA2aMode::InputRequired | RuntimeA2aMode::AuthRequired, "message/stream") => {
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "message": "stream unsupported" }
                })
            }
            (_, "message/send") => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "task": {
                        "id": "remote-task-1",
                        "status": { "state": "submitted" }
                    }
                }
            }),
            (RuntimeA2aMode::InputRequired, "tasks/get") => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "task": {
                        "id": "remote-task-1",
                        "status": {
                            "state": "input-required",
                            "message": "Remote planner needs more input."
                        }
                    }
                }
            }),
            (RuntimeA2aMode::AuthRequired, "tasks/get") => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "task": {
                        "id": "remote-task-1",
                        "status": {
                            "state": "auth-required",
                            "message": "Remote planner requires authorization."
                        }
                    }
                }
            }),
            (_, "tasks/get") => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "task": {
                        "id": "remote-task-1",
                        "status": {
                            "state": "completed"
                        },
                        "result": {
                            "text": "# Task Plan\n1. Inspect the objective and the context files.\n2. Produce a proposal-only plan with ordered steps and rollout notes.\nRisks: remote coordination can drift from local assumptions.\nAssumptions: the plan remains proposal-only.\nTest: verify the proposal covers desired outputs."
                        }
                    }
                }
            }),
            (_, other) => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "message": format!("unexpected method: {other}") }
            }),
        };
        Json(response)
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        assert!(
            detail
                .external_refs
                .iter()
                .any(|reference| reference.kind == "a2a.task_id"
                    && reference.value == "remote-task-1")
        );
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml").to_string();
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url)
            .replace(
                "allowed_artifact_classes = [\"task_plan.json\", \"task_plan.md\"]",
                "allowed_artifact_classes = [\"task_plan.json\"]",
            );
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
            .any(
                |criterion| criterion.criterion_id == "remote_outcome_accepted" && criterion.passed
            ));
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
            .any(
                |criterion| criterion.criterion_id == "remote_outcome_accepted"
                    && !criterion.passed
            ));
        assert!(latest
            .criterion_results
            .iter()
            .any(
                |criterion| criterion.criterion_id == "no_frontier_gap_violation"
                    && !criterion.passed
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url);
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
        assert!(followups
            .followups
            .iter()
            .any(|request| request.id == followup.id
                && request.status == RemoteFollowupStatus::Closed));

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
        let config = include_str!("../../../examples/hero-swarm/Crawfish.toml")
            .replace("http://127.0.0.1:7788/agent-card.json", &a2a_url)
            .replace("followup_allowed = true", "followup_allowed = false");
        let supervisor = build_supervisor_with_task_planner_manifest_and_config(
            dir.path(),
            manifest,
            config,
            None,
        )
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
}
