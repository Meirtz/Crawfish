mod actions;
mod api;
mod evaluation;
mod execution;
mod governance;
mod hero;
mod remote;
mod supervisor;
#[cfg(test)]
mod tests;

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

#[allow(unused_imports)]
use actions::*;
#[allow(unused_imports)]
use api::*;
#[allow(unused_imports)]
use evaluation::*;
#[allow(unused_imports)]
use execution::*;
#[allow(unused_imports)]
use governance::*;
#[allow(unused_imports)]
use remote::*;
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
