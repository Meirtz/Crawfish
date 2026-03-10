use crate::ExecutionContractPatch;
use crawfish_types::{
    Action, AgentManifest, AuditReceipt, CounterpartyRef, EncounterRecord, ExecutionStrategy,
    GoalSpec, LifecycleRecord, Metadata, OwnerRef, RequesterRef, ScheduleSpec, TrustDomain,
};
use serde::{Deserialize, Serialize};

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
pub struct FleetStatusResponse {
    pub agents: Vec<LifecycleRecord>,
    pub queue: QueueSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentDetail {
    pub manifest: AgentManifest,
    pub lifecycle: LifecycleRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActionDetail {
    pub action: Action,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter: Option<EncounterRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_receipt: Option<AuditReceipt>,
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
