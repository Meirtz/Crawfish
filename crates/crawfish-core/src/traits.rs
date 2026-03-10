use async_trait::async_trait;
use crawfish_types::{
    Action, ActionOutputs, AgentManifest, AuditReceipt, CapabilityDescriptor, EncounterRecord,
    LifecycleRecord,
};

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
    async fn run(&self, action: &Action) -> anyhow::Result<ActionOutputs>;
}

#[async_trait]
pub trait DeterministicExecutor: Send + Sync {
    async fn execute(&self, action: &Action) -> anyhow::Result<ActionOutputs>;
}

#[async_trait]
pub trait SupervisorControl: Send + Sync {
    async fn list_status(&self) -> anyhow::Result<Vec<LifecycleRecord>>;
    async fn inspect_agent(
        &self,
        agent_id: &str,
    ) -> anyhow::Result<Option<(AgentManifest, LifecycleRecord)>>;
    async fn inspect_action(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<(Action, Option<EncounterRecord>, Option<AuditReceipt>)>>;
    async fn drain(&self) -> anyhow::Result<()>;
    async fn resume(&self) -> anyhow::Result<()>;
}
