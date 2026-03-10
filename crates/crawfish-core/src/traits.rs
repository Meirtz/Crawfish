use async_trait::async_trait;
use crawfish_types::{Action, ActionOutputs, AgentManifest, CapabilityDescriptor, ExternalRef};
use serde_json::Value;

#[async_trait]
pub trait ActionStore: Send + Sync {
    async fn upsert_action(&self, action: &Action) -> anyhow::Result<()>;
    async fn append_action_event(
        &self,
        action_id: &str,
        event_type: &str,
        payload: serde_json::Value,
    ) -> anyhow::Result<()>;
    async fn get_action(&self, action_id: &str) -> anyhow::Result<Option<Action>>;
    async fn list_action_events(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<crate::ActionEventRecord>>;
    async fn list_actions_by_phase(&self, phase: Option<&str>) -> anyhow::Result<Vec<Action>>;
    async fn claim_next_accepted_action(&self) -> anyhow::Result<Option<Action>>;
    async fn queue_summary(&self) -> anyhow::Result<crate::QueueSummary>;
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn put_checkpoint(
        &self,
        action_id: &str,
        checkpoint_ref: &str,
        payload: &[u8],
    ) -> anyhow::Result<()>;
    async fn get_checkpoint(&self, action_id: &str) -> anyhow::Result<Option<Vec<u8>>>;
}

pub trait PolicyEngine: Send + Sync {
    fn compile(
        &self,
        agent: &AgentManifest,
        capability: &str,
    ) -> anyhow::Result<crate::CompiledExecutionPlan>;
}

pub trait GovernanceEngine: Send + Sync {
    fn authorize(&self, request: &crate::EncounterRequest) -> crate::EncounterDecision;
}

#[async_trait]
pub trait ExecutionSurface: Send + Sync {
    fn name(&self) -> &str;
    fn supports(&self, capability: &CapabilityDescriptor) -> bool;
    async fn run(&self, action: &Action) -> anyhow::Result<SurfaceExecutionResult>;
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SurfaceExecutionResult {
    pub outputs: ActionOutputs,
    pub external_refs: Vec<ExternalRef>,
    pub events: Vec<SurfaceActionEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceActionEvent {
    pub event_type: String,
    pub payload: Value,
}

#[async_trait]
pub trait DeterministicExecutor: Send + Sync {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs>;
}

#[async_trait]
pub trait SupervisorControl: Send + Sync {
    async fn list_status(&self) -> anyhow::Result<crate::FleetStatusResponse>;
    async fn list_actions(&self, phase: Option<&str>) -> anyhow::Result<crate::ActionListResponse>;
    async fn list_action_events(
        &self,
        action_id: &str,
    ) -> anyhow::Result<crate::ActionEventsResponse>;
    async fn inspect_agent(&self, agent_id: &str) -> anyhow::Result<Option<crate::AgentDetail>>;
    async fn inspect_action(&self, action_id: &str) -> anyhow::Result<Option<crate::ActionDetail>>;
    async fn submit_action(
        &self,
        request: crate::SubmitActionRequest,
    ) -> anyhow::Result<crate::SubmittedAction>;
    async fn approve_action(
        &self,
        action_id: &str,
        request: crate::ApproveActionRequest,
    ) -> anyhow::Result<crate::SubmittedAction>;
    async fn reject_action(
        &self,
        action_id: &str,
        request: crate::RejectActionRequest,
    ) -> anyhow::Result<crate::SubmittedAction>;
    async fn revoke_lease(
        &self,
        lease_id: &str,
        request: crate::RevokeLeaseRequest,
    ) -> anyhow::Result<crate::AdminActionResponse>;
    async fn validate_policy_request(
        &self,
        request: crate::PolicyValidationRequest,
    ) -> anyhow::Result<crate::PolicyValidationResponse>;
    async fn drain(&self) -> anyhow::Result<()>;
    async fn resume(&self) -> anyhow::Result<()>;
}
