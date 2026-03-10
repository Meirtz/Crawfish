use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Metadata = BTreeMap<String, serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnerKind {
    Human,
    Team,
    Org,
    ServiceAccount,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerRef {
    pub kind: OwnerKind,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustDomain {
    SameOwnerLocal,
    SameDeviceForeignOwner,
    InternalOrg,
    ExternalPartner,
    PublicUnknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Unconfigured,
    Configuring,
    Inactive,
    Activating,
    Active,
    Degraded,
    Draining,
    Failed,
    Finalized,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Unknown,
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionPhase {
    Accepted,
    Running,
    Blocked,
    AwaitingApproval,
    Cancelling,
    Completed,
    Failed,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorClass {
    Deterministic,
    Agentic,
    Hybrid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mutability {
    ReadOnly,
    ProposalOnly,
    Mutating,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskClass {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CostClass {
    Cheap,
    Standard,
    Expensive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LatencyClass {
    Interactive,
    Background,
    LongRunning,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DegradedProfileName {
    ReadOnly,
    DependencyIsolation,
    BudgetGuard,
    ProviderFailover,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityModeName {
    DeterministicOnly,
    StoreAndForward,
    HumanHandoff,
    Suspended,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EncounterState {
    Discovered,
    Classified,
    PolicyChecked,
    AwaitingConsent,
    Granted,
    Leased,
    Active,
    Denied,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApprovalRequirement {
    AnyHuman,
    OwnerConsent,
    NamedApprover { approver_id: String },
    TicketReference { system: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalPolicy {
    None,
    OnMutation,
    Always,
}

impl Default for ApprovalPolicy {
    fn default() -> Self {
        Self::OnMutation
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MutationMode {
    ReadOnly,
    ProposalOnly,
    ApprovalGated,
    Autonomous,
}

impl Default for MutationMode {
    fn default() -> Self {
        Self::ProposalOnly
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SecretPolicy {
    None,
    AdapterScoped,
    ActionScoped,
}

impl Default for SecretPolicy {
    fn default() -> Self {
        Self::AdapterScoped
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FallbackBehavior {
    Degrade,
    DeterministicOnly,
    StoreAndForward,
    HumanHandoff,
    Fail,
}

impl Default for FallbackBehavior {
    fn default() -> Self {
        Self::Degrade
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Resumability {
    StatelessReplay,
    CheckpointResume,
    NonResumable,
}

impl Default for Resumability {
    fn default() -> Self {
        Self::CheckpointResume
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointInterval {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_turns: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_clock_ms: Option<u64>,
}

impl Default for CheckpointInterval {
    fn default() -> Self {
        Self {
            model_turns: Some(1),
            wall_clock_ms: Some(30_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HumanHandoffPolicy {
    pub enabled: bool,
    #[serde(default)]
    pub include_context_bundle: bool,
}

impl Default for HumanHandoffPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            include_context_bundle: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeadLetterPolicy {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

impl Default for DeadLetterPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            queue: Some("dead_letters".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryContract {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub freshness_ttl_ms: Option<u64>,
    #[serde(default)]
    pub required_ack: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liveliness_window_ms: Option<u64>,
}

impl Default for DeliveryContract {
    fn default() -> Self {
        Self {
            deadline_ms: Some(300_000),
            freshness_ttl_ms: Some(60_000),
            required_ack: true,
            liveliness_window_ms: Some(30_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_class: Option<String>,
    #[serde(default)]
    pub preferred_harnesses: Vec<String>,
    #[serde(default)]
    pub fallback_chain: Vec<String>,
    #[serde(default)]
    pub retry_budget: u32,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
            max_cost_usd: Some(5.0),
            max_tokens: Some(64_000),
            model_class: Some("standard".to_string()),
            preferred_harnesses: vec!["mcp".to_string()],
            fallback_chain: vec!["deterministic".to_string()],
            retry_budget: 1,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafetyPolicy {
    #[serde(default)]
    pub tool_scope: Vec<String>,
    #[serde(default)]
    pub approval_policy: ApprovalPolicy,
    #[serde(default)]
    pub mutation_mode: MutationMode,
    #[serde(default)]
    pub data_zone: String,
    #[serde(default)]
    pub secret_policy: SecretPolicy,
}

impl Default for SafetyPolicy {
    fn default() -> Self {
        Self {
            tool_scope: Vec::new(),
            approval_policy: ApprovalPolicy::OnMutation,
            mutation_mode: MutationMode::ProposalOnly,
            data_zone: "owner_local".to_string(),
            secret_policy: SecretPolicy::AdapterScoped,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QualityPolicy {
    #[serde(default)]
    pub quality_class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_hook: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_confidence: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub human_review_rule: Option<String>,
}

impl Default for QualityPolicy {
    fn default() -> Self {
        Self {
            quality_class: "standard".to_string(),
            evaluation_hook: None,
            minimum_confidence: Some(0.6),
            human_review_rule: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryPolicy {
    #[serde(default)]
    pub checkpoint_interval: CheckpointInterval,
    #[serde(default)]
    pub resumability: Resumability,
    #[serde(default)]
    pub fallback_behavior: FallbackBehavior,
    #[serde(default)]
    pub continuity_preference: Vec<ContinuityModeName>,
    #[serde(default)]
    pub deterministic_fallbacks: Vec<String>,
    #[serde(default)]
    pub human_handoff_policy: HumanHandoffPolicy,
    #[serde(default)]
    pub dead_letter_policy: DeadLetterPolicy,
}

impl Default for RecoveryPolicy {
    fn default() -> Self {
        Self {
            checkpoint_interval: CheckpointInterval::default(),
            resumability: Resumability::CheckpointResume,
            fallback_behavior: FallbackBehavior::Degrade,
            continuity_preference: vec![
                ContinuityModeName::DeterministicOnly,
                ContinuityModeName::StoreAndForward,
                ContinuityModeName::HumanHandoff,
            ],
            deterministic_fallbacks: vec!["local.rule_engine".to_string()],
            human_handoff_policy: HumanHandoffPolicy::default(),
            dead_letter_policy: DeadLetterPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ExecutionContract {
    #[serde(default)]
    pub delivery: DeliveryContract,
    #[serde(default)]
    pub execution: ExecutionPolicy,
    #[serde(default)]
    pub safety: SafetyPolicy,
    #[serde(default)]
    pub quality: QualityPolicy,
    #[serde(default)]
    pub recovery: RecoveryPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityDescriptor {
    pub namespace: String,
    #[serde(default)]
    pub verbs: Vec<String>,
    pub executor_class: ExecutorClass,
    pub mutability: Mutability,
    pub risk_class: RiskClass,
    pub cost_class: CostClass,
    pub latency_class: LatencyClass,
    #[serde(default)]
    pub approval_requirements: Vec<ApprovalRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeProfile {
    #[serde(default)]
    pub concurrency_group: ConcurrencyGroup,
    #[serde(default)]
    pub max_parallel_actions: u32,
    #[serde(default)]
    pub memory_scope: MemoryScope,
}

impl Default for RuntimeProfile {
    fn default() -> Self {
        Self {
            concurrency_group: ConcurrencyGroup::Exclusive,
            max_parallel_actions: 1,
            memory_scope: MemoryScope::Workspace,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConcurrencyGroup {
    Exclusive,
    Reentrant,
}

impl Default for ConcurrencyGroup {
    fn default() -> Self {
        Self::Exclusive
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    Ephemeral,
    Session,
    Workspace,
}

impl Default for MemoryScope {
    fn default() -> Self {
        Self::Workspace
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthProbe {
    None,
    Heartbeat { interval_seconds: u32 },
}

impl Default for HealthProbe {
    fn default() -> Self {
        Self::Heartbeat {
            interval_seconds: 15,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecyclePolicy {
    #[serde(default)]
    pub heartbeat_seconds: u32,
    #[serde(default)]
    pub activate_timeout_seconds: u32,
    #[serde(default)]
    pub degrade_after_failures: u32,
    #[serde(default)]
    pub drain_timeout_seconds: u32,
    #[serde(default)]
    pub allowed_degraded_profiles: Vec<DegradedProfileName>,
    #[serde(default)]
    pub health_probe: HealthProbe,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            heartbeat_seconds: 15,
            activate_timeout_seconds: 30,
            degrade_after_failures: 1,
            drain_timeout_seconds: 30,
            allowed_degraded_profiles: vec![
                DegradedProfileName::ReadOnly,
                DegradedProfileName::DependencyIsolation,
                DegradedProfileName::BudgetGuard,
            ],
            health_probe: HealthProbe::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceIsolation {
    None,
    PerAgent,
    PerAction,
}

impl Default for WorkspaceIsolation {
    fn default() -> Self {
        Self::PerAgent
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceLockMode {
    None,
    File,
    Branch,
}

impl Default for WorkspaceLockMode {
    fn default() -> Self {
        Self::Branch
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceWriteMode {
    ReadOnly,
    ApprovalGated,
    Autonomous,
}

impl Default for WorkspaceWriteMode {
    fn default() -> Self {
        Self::ApprovalGated
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspacePolicy {
    #[serde(default)]
    pub isolation: WorkspaceIsolation,
    #[serde(default)]
    pub lock_mode: WorkspaceLockMode,
    #[serde(default)]
    pub write_mode: WorkspaceWriteMode,
}

impl Default for WorkspacePolicy {
    fn default() -> Self {
        Self {
            isolation: WorkspaceIsolation::PerAgent,
            lock_mode: WorkspaceLockMode::Branch,
            write_mode: WorkspaceWriteMode::ApprovalGated,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityVisibility {
    Private,
    OwnerOnly,
    EncounterScoped,
    Discoverable,
}

impl Default for CapabilityVisibility {
    fn default() -> Self {
        Self::OwnerOnly
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataBoundaryPolicy {
    OwnerOnly,
    Redacted,
    LeaseScoped,
}

impl Default for DataBoundaryPolicy {
    fn default() -> Self {
        Self::OwnerOnly
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolBoundaryPolicy {
    NoCrossOwnerMutation,
    LeaseScoped,
    ApprovalRequired,
}

impl Default for ToolBoundaryPolicy {
    fn default() -> Self {
        Self::NoCrossOwnerMutation
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceBoundaryPolicy {
    Isolated,
    ReadShared,
    LeaseScoped,
}

impl Default for WorkspaceBoundaryPolicy {
    fn default() -> Self {
        Self::Isolated
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NetworkBoundaryPolicy {
    LocalOnly,
    Allowlisted,
    LeasedEgress,
}

impl Default for NetworkBoundaryPolicy {
    fn default() -> Self {
        Self::LocalOnly
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DefaultDisposition {
    Deny,
    RequireConsent,
    AllowWithLease,
}

impl Default for DefaultDisposition {
    fn default() -> Self {
        Self::Deny
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncounterPolicy {
    #[serde(default)]
    pub default_disposition: DefaultDisposition,
    #[serde(default)]
    pub capability_visibility: CapabilityVisibility,
    #[serde(default)]
    pub data_boundary: DataBoundaryPolicy,
    #[serde(default)]
    pub tool_boundary: ToolBoundaryPolicy,
    #[serde(default)]
    pub workspace_boundary: WorkspaceBoundaryPolicy,
    #[serde(default)]
    pub network_boundary: NetworkBoundaryPolicy,
    #[serde(default)]
    pub human_approval_requirements: Vec<ApprovalRequirement>,
}

impl Default for EncounterPolicy {
    fn default() -> Self {
        Self {
            default_disposition: DefaultDisposition::Deny,
            capability_visibility: CapabilityVisibility::OwnerOnly,
            data_boundary: DataBoundaryPolicy::OwnerOnly,
            tool_boundary: ToolBoundaryPolicy::NoCrossOwnerMutation,
            workspace_boundary: WorkspaceBoundaryPolicy::Isolated,
            network_boundary: NetworkBoundaryPolicy::LocalOnly,
            human_approval_requirements: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CounterpartyRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub owner: OwnerRef,
    pub trust_domain: TrustDomain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RequesterKind {
    User,
    Agent,
    Session,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequesterRef {
    pub kind: RequesterKind,
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GoalSpec {
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionPriority {
    Low,
    Normal,
    High,
}

impl Default for ActionPriority {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleSpec {
    #[serde(default)]
    pub priority: ActionPriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<String>,
}

impl Default for ScheduleSpec {
    fn default() -> Self {
        Self {
            priority: ActionPriority::Normal,
            not_before: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ActionOutputs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "adapter", rename_all = "snake_case")]
pub enum AdapterBinding {
    Mcp(McpToolBinding),
    Openclaw(OpenClawBinding),
    Acp(AcpHarnessBinding),
    A2a(A2ARemoteAgentBinding),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpToolBinding {
    pub capability: String,
    #[serde(default)]
    pub default_scope: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpenClawSessionMode {
    Ephemeral,
    Sticky,
}

impl Default for OpenClawSessionMode {
    fn default() -> Self {
        Self::Ephemeral
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CallerOwnerMapping {
    Required,
    BestEffort,
}

impl Default for CallerOwnerMapping {
    fn default() -> Self {
        Self::Required
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OpenClawWorkspacePolicy {
    Inherit,
    OpenclawManaged,
    CrawfishManaged,
}

impl Default for OpenClawWorkspacePolicy {
    fn default() -> Self {
        Self::Inherit
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenClawBinding {
    pub gateway_url: String,
    pub auth_ref: String,
    pub target_agent: String,
    pub session_mode: OpenClawSessionMode,
    pub caller_owner_mapping: CallerOwnerMapping,
    pub default_trust_domain: TrustDomain,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub lease_required: bool,
    pub workspace_policy: OpenClawWorkspacePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcpSessionMode {
    Ephemeral,
    Persistent,
}

impl Default for AcpSessionMode {
    fn default() -> Self {
        Self::Ephemeral
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcpHarnessBinding {
    pub harness: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub session_mode: AcpSessionMode,
    #[serde(default)]
    pub default_scope: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct A2ARemoteAgentBinding {
    pub capability: String,
    pub endpoint: String,
    pub trust_domain: TrustDomain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStrategyMode {
    SinglePass,
    VerifyLoop,
}

impl Default for ExecutionStrategyMode {
    fn default() -> Self {
        Self::SinglePass
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackPolicy {
    InjectReason,
    AppendReport,
    Handoff,
}

impl Default for FeedbackPolicy {
    fn default() -> Self {
        Self::InjectReason
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionStrategy {
    pub mode: ExecutionStrategyMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_spec: Option<VerificationSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_budget: Option<StopBudget>,
    #[serde(default)]
    pub feedback_policy: FeedbackPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VerificationSpec {
    #[serde(default)]
    pub checks: Vec<VerificationCheck>,
    #[serde(default)]
    pub require_all: bool,
    #[serde(default)]
    pub on_failure: VerifyLoopFailureMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VerifyLoopFailureMode {
    RetryWithFeedback,
    HumanHandoff,
    Fail,
}

impl Default for VerifyLoopFailureMode {
    fn default() -> Self {
        Self::RetryWithFeedback
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StopBudget {
    pub max_iterations: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_elapsed_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VerificationCheck {
    CommandExit {
        command: Vec<String>,
        cwd: Option<String>,
    },
    FileExists {
        path: String,
    },
    FilePattern {
        path: String,
        pattern: String,
    },
    TestSuite {
        command: Vec<String>,
        cwd: Option<String>,
    },
    Lint {
        command: Vec<String>,
        cwd: Option<String>,
    },
    SchemaValidate {
        schema_ref: String,
        target_ref: String,
    },
    ApprovalGate {
        policy_ref: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleRecord {
    pub agent_id: String,
    pub desired_state: AgentState,
    pub observed_state: AgentState,
    pub health: HealthStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition_reason: Option<String>,
    pub last_transition_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degradation_profile: Option<DegradedProfileName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_mode: Option<ContinuityModeName>,
    #[serde(default)]
    pub failure_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentManifest {
    pub id: String,
    pub owner: OwnerRef,
    pub trust_domain: TrustDomain,
    pub role: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub exposed_capabilities: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub runtime: RuntimeProfile,
    #[serde(default)]
    pub lifecycle: LifecyclePolicy,
    #[serde(default)]
    pub encounter_policy: EncounterPolicy,
    #[serde(default)]
    pub contract_defaults: ExecutionContract,
    #[serde(default)]
    pub adapters: Vec<AdapterBinding>,
    #[serde(default)]
    pub workspace_policy: WorkspacePolicy,
    #[serde(default)]
    pub default_data_boundaries: Vec<String>,
    #[serde(default)]
    pub strategy_defaults: BTreeMap<String, ExecutionStrategy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Action {
    pub id: String,
    pub requester: RequesterRef,
    pub initiator_owner: OwnerRef,
    #[serde(default)]
    pub counterparty_refs: Vec<CounterpartyRef>,
    pub goal: GoalSpec,
    pub capability: String,
    #[serde(default)]
    pub inputs: Metadata,
    #[serde(default)]
    pub contract: ExecutionContract,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_strategy: Option<ExecutionStrategy>,
    #[serde(default)]
    pub grant_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ref: Option<String>,
    #[serde(default)]
    pub data_boundary: String,
    #[serde(default)]
    pub schedule: ScheduleSpec,
    pub phase: ActionPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_ref: Option<String>,
    #[serde(default)]
    pub outputs: ActionOutputs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncounterRecord {
    pub id: String,
    pub initiator_ref: CounterpartyRef,
    pub target_agent_id: String,
    pub target_owner: OwnerRef,
    pub trust_domain: TrustDomain,
    #[serde(default)]
    pub requested_capabilities: Vec<String>,
    pub applied_policy_source: String,
    pub state: EncounterState,
    #[serde(default)]
    pub grant_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ref: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsentGrant {
    pub id: String,
    pub grantor: OwnerRef,
    pub grantee: OwnerRef,
    pub purpose: String,
    #[serde(default)]
    pub scope: Vec<String>,
    pub issued_at: String,
    pub expires_at: String,
    pub revocable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityLease {
    pub id: String,
    pub grant_ref: String,
    pub lessor: OwnerRef,
    pub lessee: OwnerRef,
    #[serde(default)]
    pub capability_refs: Vec<String>,
    #[serde(default)]
    pub scope: Vec<String>,
    pub issued_at: String,
    pub expires_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revocation_reason: Option<String>,
    pub audit_receipt_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Allowed,
    Denied,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditReceipt {
    pub id: String,
    pub encounter_ref: String,
    #[serde(default)]
    pub grant_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ref: Option<String>,
    pub outcome: AuditOutcome,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver_ref: Option<String>,
    pub emitted_at: String,
}
