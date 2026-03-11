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
    async fn put_trace_bundle(&self, bundle: &crawfish_types::TraceBundle) -> anyhow::Result<()>;
    async fn get_trace_bundle(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<crawfish_types::TraceBundle>>;
    async fn insert_evaluation(
        &self,
        evaluation: &crawfish_types::EvaluationRecord,
    ) -> anyhow::Result<()>;
    async fn list_evaluations(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<crawfish_types::EvaluationRecord>>;
    async fn insert_review_queue_item(
        &self,
        item: &crawfish_types::ReviewQueueItem,
    ) -> anyhow::Result<()>;
    async fn list_review_queue_items(&self)
        -> anyhow::Result<Vec<crawfish_types::ReviewQueueItem>>;
    async fn resolve_review_queue_item(
        &self,
        item: &crawfish_types::ReviewQueueItem,
    ) -> anyhow::Result<()>;
    async fn insert_feedback_note(&self, note: &crawfish_types::FeedbackNote)
        -> anyhow::Result<()>;
    async fn get_feedback_note(
        &self,
        note_id: &str,
    ) -> anyhow::Result<Option<crawfish_types::FeedbackNote>>;
    async fn insert_policy_incident(
        &self,
        incident: &crawfish_types::PolicyIncident,
    ) -> anyhow::Result<()>;
    async fn list_policy_incidents(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Vec<crawfish_types::PolicyIncident>>;
    async fn insert_dataset_case(&self, case: &crawfish_types::DatasetCase) -> anyhow::Result<()>;
    async fn list_dataset_cases(
        &self,
        dataset_name: &str,
    ) -> anyhow::Result<Vec<crawfish_types::DatasetCase>>;
    async fn insert_experiment_run(
        &self,
        run: &crawfish_types::ExperimentRun,
    ) -> anyhow::Result<()>;
    async fn get_experiment_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<crawfish_types::ExperimentRun>>;
    async fn update_experiment_run(
        &self,
        run: &crawfish_types::ExperimentRun,
    ) -> anyhow::Result<()>;
    async fn insert_experiment_case_result(
        &self,
        result: &crawfish_types::ExperimentCaseResult,
    ) -> anyhow::Result<()>;
    async fn list_experiment_case_results(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<crawfish_types::ExperimentCaseResult>>;
    async fn insert_pairwise_experiment_run(
        &self,
        run: &crawfish_types::PairwiseExperimentRun,
    ) -> anyhow::Result<()>;
    async fn get_pairwise_experiment_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<crawfish_types::PairwiseExperimentRun>>;
    async fn update_pairwise_experiment_run(
        &self,
        run: &crawfish_types::PairwiseExperimentRun,
    ) -> anyhow::Result<()>;
    async fn insert_pairwise_case_result(
        &self,
        result: &crawfish_types::PairwiseCaseResult,
    ) -> anyhow::Result<()>;
    async fn get_pairwise_case_result(
        &self,
        case_result_id: &str,
    ) -> anyhow::Result<Option<crawfish_types::PairwiseCaseResult>>;
    async fn update_pairwise_case_result(
        &self,
        result: &crawfish_types::PairwiseCaseResult,
    ) -> anyhow::Result<()>;
    async fn list_pairwise_case_results(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<crawfish_types::PairwiseCaseResult>>;
    async fn insert_alert_event(&self, alert: &crawfish_types::AlertEvent) -> anyhow::Result<()>;
    async fn list_alert_events(&self) -> anyhow::Result<Vec<crawfish_types::AlertEvent>>;
    async fn acknowledge_alert_event(
        &self,
        alert: &crawfish_types::AlertEvent,
    ) -> anyhow::Result<()>;
    async fn insert_delegation_receipt(
        &self,
        receipt: &crawfish_types::DelegationReceipt,
    ) -> anyhow::Result<()>;
    async fn get_delegation_receipt(
        &self,
        receipt_id: &str,
    ) -> anyhow::Result<Option<crawfish_types::DelegationReceipt>>;
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
    async fn list_status(&self) -> anyhow::Result<crate::SwarmStatusResponse>;
    async fn list_actions(&self, phase: Option<&str>) -> anyhow::Result<crate::ActionListResponse>;
    async fn list_action_events(
        &self,
        action_id: &str,
    ) -> anyhow::Result<crate::ActionEventsResponse>;
    async fn get_action_trace(
        &self,
        action_id: &str,
    ) -> anyhow::Result<Option<crate::ActionTraceResponse>>;
    async fn list_action_evaluations(
        &self,
        action_id: &str,
    ) -> anyhow::Result<crate::ActionEvaluationsResponse>;
    async fn list_review_queue(&self) -> anyhow::Result<crate::ReviewQueueResponse>;
    async fn resolve_review_queue_item(
        &self,
        review_id: &str,
        request: crate::ResolveReviewQueueItemRequest,
    ) -> anyhow::Result<crate::ResolveReviewQueueItemResponse>;
    async fn list_evaluation_datasets(&self) -> anyhow::Result<crate::EvaluationDatasetsResponse>;
    async fn get_evaluation_dataset(
        &self,
        dataset_name: &str,
    ) -> anyhow::Result<Option<crate::EvaluationDatasetDetailResponse>>;
    async fn start_evaluation_run(
        &self,
        request: crate::StartEvaluationRunRequest,
    ) -> anyhow::Result<crate::StartEvaluationRunResponse>;
    async fn get_evaluation_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<crate::ExperimentRunDetailResponse>>;
    async fn start_pairwise_evaluation_run(
        &self,
        request: crate::StartPairwiseEvaluationRunRequest,
    ) -> anyhow::Result<crate::StartPairwiseEvaluationRunResponse>;
    async fn get_pairwise_evaluation_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<crate::PairwiseExperimentRunDetailResponse>>;
    async fn list_alerts(&self) -> anyhow::Result<crate::AlertListResponse>;
    async fn acknowledge_alert(
        &self,
        alert_id: &str,
        request: crate::AcknowledgeAlertRequest,
    ) -> anyhow::Result<crate::AcknowledgeAlertResponse>;
    async fn list_treaties(&self) -> anyhow::Result<crate::TreatyListResponse>;
    async fn get_treaty(
        &self,
        treaty_id: &str,
    ) -> anyhow::Result<Option<crate::TreatyDetailResponse>>;
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
