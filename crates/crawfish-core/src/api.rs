use crate::ExecutionContractPatch;
use crawfish_types::{
    Action, AgentManifest, AlertEvent, ArtifactRef, AuditReceipt, CapabilityLease,
    CheckpointStatus, ConsentGrant, CounterpartyRef, DatasetCase, DelegationReceipt, DoctrinePack,
    EncounterRecord, EvaluationDataset, EvaluationRecord, ExecutionStrategy, ExecutionStrategyMode,
    ExperimentCaseResult, ExperimentRun, ExternalRef, FederationDecision, FederationPack, GoalSpec,
    InteractionModel, JurisdictionClass, LifecycleRecord, Metadata, OwnerRef, PairwiseCaseResult,
    PairwiseExperimentRun, PolicyIncident, RemoteEvidenceStatus, RemoteOutcomeDisposition,
    RemotePrincipalRef, RemoteStateDisposition, RequesterRef, ReviewQueueItem, ScheduleSpec,
    TraceBundle, TreatyPack, TreatyViolation, TrustDomain, VerificationSummary,
    WorkspaceLockDetail,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub status: String,
    pub socket_path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueSummary {
    pub accepted: u64,
    pub running: u64,
    pub blocked: u64,
    pub awaiting_approval: u64,
    pub completed: u64,
    pub failed: u64,
    pub expired: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SwarmStatusResponse {
    pub agents: Vec<LifecycleRecord>,
    pub queue: QueueSummary,
}

pub type FleetStatusResponse = SwarmStatusResponse;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentDetail {
    pub manifest: AgentManifest,
    pub lifecycle: LifecycleRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionDetail {
    pub action: Action,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_stage: Option<String>,
    #[serde(default)]
    pub external_refs: Vec<ExternalRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_mode: Option<ExecutionStrategyMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_iteration: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<VerificationSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter: Option<EncounterRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_audit_receipt: Option<AuditReceipt>,
    #[serde(default)]
    pub grant_details: Vec<ConsentGrant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_detail: Option<CapabilityLease>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_detail: Option<WorkspaceLockDetail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_model: Option<InteractionModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jurisdiction_class: Option<JurisdictionClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doctrine_summary: Option<DoctrinePack>,
    #[serde(default)]
    pub checkpoint_status: Vec<CheckpointStatus>,
    #[serde(default)]
    pub policy_incidents: Vec<PolicyIncident>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_evaluation: Option<EvaluationRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_ref: Option<String>,
    #[serde(default)]
    pub review_queue_refs: Vec<String>,
    #[serde(default)]
    pub alert_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_principal: Option<RemotePrincipalRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_summary: Option<TreatyPack>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_summary: Option<FederationPack>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_decision: Option<FederationDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_receipt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_state_disposition: Option<RemoteStateDisposition>,
    #[serde(default)]
    pub treaty_violations: Vec<TreatyViolation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_depth: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_receipt: Option<DelegationReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionSummary {
    pub id: String,
    pub target_agent_id: String,
    pub capability: String,
    pub phase: String,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionListResponse {
    pub actions: Vec<ActionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionEventRecord {
    pub id: i64,
    pub action_id: String,
    pub event_type: String,
    pub payload: Value,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionEventsResponse {
    pub events: Vec<ActionEventRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionEvaluationsResponse {
    pub evaluations: Vec<EvaluationRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionTraceResponse {
    pub trace: TraceBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewQueueResponse {
    pub items: Vec<ReviewQueueItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvaluationDatasetSummary {
    pub name: String,
    pub capability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub auto_capture: bool,
    pub case_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvaluationDatasetsResponse {
    pub datasets: Vec<EvaluationDatasetSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationDatasetDetailResponse {
    pub name: String,
    pub config: EvaluationDataset,
    #[serde(default)]
    pub cases: Vec<DatasetCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartEvaluationRunRequest {
    pub dataset: String,
    pub executor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StartEvaluationRunResponse {
    pub run: ExperimentRun,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentRunDetailResponse {
    pub run: ExperimentRun,
    #[serde(default)]
    pub cases: Vec<ExperimentCaseResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StartPairwiseEvaluationRunRequest {
    pub dataset: String,
    pub left_executor: String,
    pub right_executor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StartPairwiseEvaluationRunResponse {
    pub run: PairwiseExperimentRun,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairwiseExperimentRunDetailResponse {
    pub run: PairwiseExperimentRun,
    #[serde(default)]
    pub cases: Vec<PairwiseCaseResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertListResponse {
    pub alerts: Vec<AlertEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreatyListResponse {
    pub treaties: Vec<TreatyPack>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreatyDetailResponse {
    pub treaty: TreatyPack,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederationPackListResponse {
    pub packs: Vec<FederationPack>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederationPackDetailResponse {
    pub pack: FederationPack,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcknowledgeAlertRequest {
    pub actor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcknowledgeAlertResponse {
    pub alert: AlertEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolveReviewQueueItemRequest {
    pub resolver_ref: String,
    pub resolution: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolveReviewQueueItemResponse {
    pub item: ReviewQueueItem,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SubmitActionRequest {
    pub target_agent_id: String,
    pub requester: RequesterRef,
    pub initiator_owner: OwnerRef,
    pub capability: String,
    pub goal: GoalSpec,
    #[serde(default)]
    pub inputs: Metadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_overrides: Option<ExecutionContractPatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_strategy: Option<ExecutionStrategy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleSpec>,
    #[serde(default)]
    pub counterparty_refs: Vec<CounterpartyRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_boundary: Option<String>,
    #[serde(default)]
    pub workspace_write: bool,
    #[serde(default)]
    pub secret_access: bool,
    #[serde(default)]
    pub mutating: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmittedAction {
    pub action_id: String,
    pub phase: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawCallerContext {
    pub caller_id: String,
    pub session_id: String,
    pub channel_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_root: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default)]
    pub trace_ids: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpenClawInboundActionRequest {
    pub caller: OpenClawCallerContext,
    pub target_agent_id: String,
    pub capability: String,
    pub goal: GoalSpec,
    #[serde(default)]
    pub inputs: Metadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_overrides: Option<ExecutionContractPatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_strategy: Option<ExecutionStrategy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<ScheduleSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_boundary: Option<String>,
    #[serde(default)]
    pub workspace_write: bool,
    #[serde(default)]
    pub secret_access: bool,
    #[serde(default)]
    pub mutating: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawInboundActionResponse {
    pub action_id: String,
    pub phase: String,
    pub requester_id: String,
    #[serde(default)]
    pub trace_refs: Vec<ExternalRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawInspectionContext {
    pub caller: OpenClawCallerContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawAgentStatusResponse {
    pub agent_id: String,
    pub desired_state: String,
    pub observed_state: String,
    pub health: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition_reason: Option<String>,
    pub last_transition_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degradation_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApproveActionRequest {
    pub approver_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RejectActionRequest {
    pub approver_ref: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevokeLeaseRequest {
    pub revoker_ref: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyValidationRequest {
    pub target_agent_id: String,
    pub caller: CounterpartyRef,
    pub capability: String,
    #[serde(default)]
    pub workspace_write: bool,
    #[serde(default)]
    pub secret_access: bool,
    #[serde(default)]
    pub mutating: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyValidationResponse {
    pub disposition: String,
    pub reason: String,
    pub trust_domain: TrustDomain,
    pub target_agent_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminActionResponse {
    pub status: String,
}
