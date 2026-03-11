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
    pub evaluation_profile: Option<String>,
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
            evaluation_profile: None,
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
    pub artifacts: Vec<ArtifactRef>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArtifactRef {
    pub kind: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceEditOp {
    Create,
    Replace,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceEdit {
    pub path: String,
    pub op: WorkspaceEditOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contents: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceRejectedEdit {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceApplyResult {
    #[serde(default)]
    pub applied: Vec<String>,
    #[serde(default)]
    pub rejected: Vec<WorkspaceRejectedEdit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceLockDetail {
    pub mode: WorkspaceLockMode,
    pub scope: String,
    pub lock_path: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_action_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acquired_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalRef {
    pub kind: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "adapter", rename_all = "snake_case")]
pub enum AdapterBinding {
    Mcp(McpToolBinding),
    LocalHarness(LocalHarnessBinding),
    Openclaw(OpenClawBinding),
    Acp(AcpHarnessBinding),
    A2a(A2ARemoteAgentBinding),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpToolBinding {
    pub capability: String,
    pub server: String,
    pub tool: String,
    #[serde(default)]
    pub default_scope: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportKind {
    Sse,
}

impl Default for McpTransportKind {
    fn default() -> Self {
        Self::Sse
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpServerConfig {
    #[serde(default)]
    pub transport: McpTransportKind,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token_env: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default = "default_mcp_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_mcp_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

fn default_mcp_connect_timeout_ms() -> u64 {
    5_000
}

fn default_mcp_request_timeout_ms() -> u64 {
    15_000
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalHarnessKind {
    ClaudeCode,
    Codex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LocalHarnessWorkspacePolicy {
    Inherit,
    CrawfishManaged,
}

impl Default for LocalHarnessWorkspacePolicy {
    fn default() -> Self {
        Self::Inherit
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalHarnessBinding {
    pub capability: String,
    pub harness: LocalHarnessKind,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub lease_required: bool,
    #[serde(default)]
    pub workspace_policy: LocalHarnessWorkspacePolicy,
    #[serde(default)]
    pub env_allowlist: Vec<String>,
    #[serde(default = "default_local_harness_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_local_harness_timeout_seconds() -> u64 {
    90
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
    #[serde(alias = "endpoint")]
    pub agent_card_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_ref: Option<String>,
    pub treaty_pack: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack: Option<String>,
    #[serde(default)]
    pub required_scopes: Vec<String>,
    #[serde(default)]
    pub streaming_mode: A2AStreamingMode,
    #[serde(default)]
    pub allow_in_task_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum A2AStreamingMode {
    PreferStreaming,
    PollOnly,
}

impl Default for A2AStreamingMode {
    fn default() -> Self {
        Self::PreferStreaming
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TreatyAuthForwardingMode {
    None,
    InTaskBearer,
}

impl Default for TreatyAuthForwardingMode {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemotePrincipalKind {
    Agent,
    Service,
    Unknown,
}

impl Default for RemotePrincipalKind {
    fn default() -> Self {
        Self::Agent
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemotePrincipalRef {
    #[serde(default)]
    pub kind: RemotePrincipalKind,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub agent_card_url: String,
    pub trust_domain: TrustDomain,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TreatyEscalationMode {
    Deny,
    ReviewRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TreatyEvidenceRequirement {
    DelegationReceiptPresent,
    RemoteTaskRefPresent,
    TerminalStateVerified,
    ArtifactClassesAllowed,
    DataScopesAllowed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreatyViolation {
    pub code: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<OversightCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteOutcomeDisposition {
    Accepted,
    ReviewRequired,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteStateDisposition {
    Running,
    Blocked,
    AwaitingApproval,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteEvidenceStatus {
    Pending,
    Satisfied,
    MissingRequiredEvidence,
    ScopeViolation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteResultAcceptance {
    Accepted,
    ReviewRequired,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteReviewDisposition {
    Pending,
    Accepted,
    Rejected,
    NeedsFollowup,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteReviewResolution {
    AcceptResult,
    RejectResult,
    NeedsFollowup,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RemoteReviewReason {
    RemoteStateEscalated,
    EvidenceGap,
    ScopeViolation,
    ResultReviewRequired,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederationReviewDefaults {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_federation_review_priority")]
    pub priority: String,
}

fn default_federation_review_priority() -> String {
    "high".to_string()
}

impl Default for FederationReviewDefaults {
    fn default() -> Self {
        Self {
            enabled: true,
            priority: default_federation_review_priority(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct FederationAlertDefaults {
    #[serde(default)]
    pub rules: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteEscalationPolicy {
    pub blocked_remote_policy: RemoteStateDisposition,
    pub auth_required_policy: RemoteStateDisposition,
    pub remote_failure_policy: RemoteStateDisposition,
    pub result_acceptance_policy: RemoteResultAcceptance,
    pub scope_violation_policy: RemoteResultAcceptance,
    pub evidence_gap_policy: RemoteResultAcceptance,
}

impl Default for RemoteEscalationPolicy {
    fn default() -> Self {
        Self {
            blocked_remote_policy: RemoteStateDisposition::Blocked,
            auth_required_policy: RemoteStateDisposition::AwaitingApproval,
            remote_failure_policy: RemoteStateDisposition::Failed,
            result_acceptance_policy: RemoteResultAcceptance::Accepted,
            scope_violation_policy: RemoteResultAcceptance::Rejected,
            evidence_gap_policy: RemoteResultAcceptance::ReviewRequired,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreatyClause {
    pub id: String,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub required_checkpoints: Vec<OversightCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreatyPack {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub local_owner: OwnerRef,
    pub remote_principal: RemotePrincipalRef,
    #[serde(default)]
    pub allowed_capabilities: Vec<String>,
    #[serde(default)]
    pub allowed_data_scopes: Vec<String>,
    #[serde(default)]
    pub allowed_artifact_classes: Vec<String>,
    #[serde(default)]
    pub allowed_auth_forwarding_mode: TreatyAuthForwardingMode,
    #[serde(default)]
    pub required_checkpoints: Vec<OversightCheckpoint>,
    #[serde(default = "default_treaty_required_result_evidence")]
    pub required_result_evidence: Vec<TreatyEvidenceRequirement>,
    pub max_delegation_depth: u32,
    pub review_policy: String,
    #[serde(default = "default_treaty_scope_violation_mode")]
    pub on_scope_violation: TreatyEscalationMode,
    #[serde(default = "default_treaty_evidence_gap_mode")]
    pub on_evidence_gap: TreatyEscalationMode,
    #[serde(default = "default_true")]
    pub review_queue: bool,
    #[serde(default)]
    pub alert_rules: Vec<String>,
    #[serde(default)]
    pub clauses: Vec<TreatyClause>,
}

fn default_treaty_required_result_evidence() -> Vec<TreatyEvidenceRequirement> {
    vec![
        TreatyEvidenceRequirement::DelegationReceiptPresent,
        TreatyEvidenceRequirement::RemoteTaskRefPresent,
        TreatyEvidenceRequirement::TerminalStateVerified,
        TreatyEvidenceRequirement::ArtifactClassesAllowed,
        TreatyEvidenceRequirement::DataScopesAllowed,
    ]
}

fn default_treaty_scope_violation_mode() -> TreatyEscalationMode {
    TreatyEscalationMode::Deny
}

fn default_treaty_evidence_gap_mode() -> TreatyEscalationMode {
    TreatyEscalationMode::ReviewRequired
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreatyDecision {
    pub treaty_pack_id: String,
    pub remote_principal: RemotePrincipalRef,
    pub capability: String,
    #[serde(default)]
    pub requested_scopes: Vec<String>,
    #[serde(default)]
    pub delegated_data_scopes: Vec<String>,
    #[serde(default)]
    pub required_checkpoints: Vec<OversightCheckpoint>,
    #[serde(default)]
    pub required_result_evidence: Vec<TreatyEvidenceRequirement>,
    pub delegation_depth: u32,
    pub on_scope_violation: TreatyEscalationMode,
    pub on_evidence_gap: TreatyEscalationMode,
    pub review_queue: bool,
    #[serde(default)]
    pub alert_rules: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederationPack {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub treaty_pack_id: String,
    #[serde(default)]
    pub review_defaults: FederationReviewDefaults,
    #[serde(default)]
    pub alert_defaults: FederationAlertDefaults,
    #[serde(default = "default_treaty_required_result_evidence")]
    pub required_remote_evidence: Vec<TreatyEvidenceRequirement>,
    #[serde(default = "default_remote_result_acceptance_policy")]
    pub result_acceptance_policy: RemoteResultAcceptance,
    #[serde(default = "default_scope_violation_policy")]
    pub scope_violation_policy: RemoteResultAcceptance,
    #[serde(default = "default_evidence_gap_policy")]
    pub evidence_gap_policy: RemoteResultAcceptance,
    #[serde(default = "default_blocked_remote_policy")]
    pub blocked_remote_policy: RemoteStateDisposition,
    #[serde(default = "default_auth_required_policy")]
    pub auth_required_policy: RemoteStateDisposition,
    #[serde(default = "default_remote_failure_policy")]
    pub remote_failure_policy: RemoteStateDisposition,
    #[serde(default = "default_treaty_required_checkpoints")]
    pub required_checkpoints: Vec<OversightCheckpoint>,
    pub max_delegation_depth: u32,
}

fn default_remote_result_acceptance_policy() -> RemoteResultAcceptance {
    RemoteResultAcceptance::Accepted
}

fn default_scope_violation_policy() -> RemoteResultAcceptance {
    RemoteResultAcceptance::Rejected
}

fn default_evidence_gap_policy() -> RemoteResultAcceptance {
    RemoteResultAcceptance::ReviewRequired
}

fn default_blocked_remote_policy() -> RemoteStateDisposition {
    RemoteStateDisposition::Blocked
}

fn default_auth_required_policy() -> RemoteStateDisposition {
    RemoteStateDisposition::AwaitingApproval
}

fn default_remote_failure_policy() -> RemoteStateDisposition {
    RemoteStateDisposition::Failed
}

fn default_treaty_required_checkpoints() -> Vec<OversightCheckpoint> {
    vec![
        OversightCheckpoint::Admission,
        OversightCheckpoint::PreDispatch,
        OversightCheckpoint::PostResult,
    ]
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FederationDecision {
    pub federation_pack_id: String,
    pub treaty_pack_id: String,
    pub remote_principal: RemotePrincipalRef,
    pub capability: String,
    #[serde(default)]
    pub required_checkpoints: Vec<OversightCheckpoint>,
    #[serde(default)]
    pub required_remote_evidence: Vec<TreatyEvidenceRequirement>,
    pub delegation_depth: u32,
    pub escalation: RemoteEscalationPolicy,
    #[serde(default)]
    pub review_defaults: FederationReviewDefaults,
    #[serde(default)]
    pub alert_defaults: FederationAlertDefaults,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_state_disposition: Option<RemoteStateDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_result_acceptance: Option<RemoteResultAcceptance>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DelegationDecision {
    Allowed,
    Denied,
    StoreAndForward,
    HumanHandoff,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationReceipt {
    pub id: String,
    pub action_id: String,
    pub treaty_pack_id: String,
    pub remote_principal: RemotePrincipalRef,
    pub capability: String,
    #[serde(default)]
    pub requested_scopes: Vec<String>,
    #[serde(default)]
    pub delegated_data_scopes: Vec<String>,
    pub decision: DelegationDecision,
    pub remote_agent_card_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_depth: Option<u32>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteEvidenceItem {
    pub id: String,
    pub kind: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<OversightCheckpoint>,
    pub satisfied: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteEvidenceBundle {
    pub id: String,
    pub action_id: String,
    pub attempt: u32,
    pub interaction_model: InteractionModel,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_principal: Option<RemotePrincipalRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_receipt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_terminal_state: Option<String>,
    #[serde(default)]
    pub remote_artifact_manifest: Vec<String>,
    #[serde(default)]
    pub remote_data_scopes: Vec<String>,
    #[serde(default)]
    pub checkpoint_status: Vec<CheckpointStatus>,
    #[serde(default)]
    pub evidence_items: Vec<RemoteEvidenceItem>,
    #[serde(default)]
    pub policy_incidents: Vec<PolicyIncident>,
    #[serde(default)]
    pub treaty_violations: Vec<TreatyViolation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_reason: Option<RemoteReviewReason>,
    pub created_at: String,
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
pub enum VerificationStatus {
    Passed,
    Failed,
    BudgetExhausted,
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
pub struct VerificationSummary {
    pub status: VerificationStatus,
    pub iterations_completed: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_feedback: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyCheckpointState {
    pub mode: ExecutionStrategyMode,
    pub iteration: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_feedback: Option<String>,
    #[serde(default)]
    pub previous_artifact_refs: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<VerificationSummary>,
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
    pub target_agent_id: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encounter_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audit_receipt_ref: Option<String>,
    #[serde(default)]
    pub data_boundary: String,
    #[serde(default)]
    pub schedule: ScheduleSpec,
    pub phase: ActionPhase,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_mode: Option<ContinuityModeName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub degradation_profile: Option<DegradedProfileName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_detail: Option<WorkspaceLockDetail>,
    #[serde(default)]
    pub external_refs: Vec<ExternalRef>,
    #[serde(default)]
    pub outputs: ActionOutputs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JurisdictionClass {
    SameOwnerLocal,
    SameDeviceForeignOwner,
    RemoteHarness,
    ExternalUnknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InteractionModel {
    ContextSplit,
    SameOwnerSwarm,
    SameDeviceMultiOwner,
    RemoteHarness,
    RemoteAgent,
    ExternalUnknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OversightCheckpoint {
    Admission,
    PreDispatch,
    PreMutation,
    PostResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointOutcome {
    Pending,
    Passed,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointStatus {
    pub checkpoint: OversightCheckpoint,
    pub required: bool,
    pub outcome: CheckpointOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctrineRule {
    pub id: String,
    pub title: String,
    pub summary: String,
    #[serde(default)]
    pub required_checkpoints: Vec<OversightCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctrinePack {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub jurisdiction: JurisdictionClass,
    #[serde(default)]
    pub rules: Vec<DoctrineRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnforcementRecord {
    pub id: String,
    pub action_id: String,
    pub checkpoint: OversightCheckpoint,
    pub outcome: CheckpointOutcome,
    pub reason: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolicyIncidentSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyIncident {
    pub id: String,
    pub action_id: String,
    pub doctrine_pack_id: String,
    pub jurisdiction: JurisdictionClass,
    #[serde(alias = "code")]
    pub reason_code: String,
    pub summary: String,
    pub severity: PolicyIncidentSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<OversightCheckpoint>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceBundle {
    pub id: String,
    pub action_id: String,
    pub capability: String,
    pub goal_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_model: Option<InteractionModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jurisdiction_class: Option<JurisdictionClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doctrine_summary: Option<DoctrinePack>,
    #[serde(default)]
    pub checkpoint_status: Vec<CheckpointStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_executor: Option<String>,
    #[serde(default)]
    pub inputs: Metadata,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub external_refs: Vec<ExternalRef>,
    #[serde(default)]
    pub events: Vec<Metadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<VerificationSummary>,
    #[serde(default)]
    pub enforcement_records: Vec<EnforcementRecord>,
    #[serde(default)]
    pub policy_incidents: Vec<PolicyIncident>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_principal: Option<RemotePrincipalRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_decision: Option<FederationDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_receipt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_state_disposition: Option<RemoteStateDisposition>,
    #[serde(default)]
    pub treaty_violations: Vec<TreatyViolation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_depth: Option<u32>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationStatus {
    Passed,
    Failed,
    NeedsReview,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationRecord {
    pub id: String,
    pub action_id: String,
    pub evaluator: String,
    pub status: EvaluationStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<String>,
    #[serde(default)]
    pub criterion_results: Vec<EvaluationCriterionResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_model: Option<InteractionModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default)]
    pub treaty_violation_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback_note_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluationCriterionResult {
    pub criterion_id: String,
    pub passed: bool,
    pub score_contribution: f64,
    pub evidence_summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewQueueStatus {
    Open,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewQueueKind {
    ActionEval,
    PairwiseEval,
    RemoteResultReview,
}

impl Default for ReviewQueueKind {
    fn default() -> Self {
        Self::ActionEval
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewQueueItem {
    pub id: String,
    pub action_id: String,
    pub source: String,
    #[serde(default)]
    pub kind: ReviewQueueKind,
    pub status: ReviewQueueStatus,
    pub priority: String,
    pub reason_code: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_case_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairwise_run_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairwise_case_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_case_result_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_case_result_ref: Option<String>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeedbackNote {
    pub id: String,
    pub action_id: String,
    pub source: String,
    pub body: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pairwise_case_result_ref: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub trigger: String,
    pub severity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NumericComparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScorecardCriterionKind {
    ArtifactPresent,
    ArtifactAbsent,
    JsonFieldNonempty,
    JsonSchemaValid,
    ListMinLen,
    RegexMatch,
    NumericThreshold,
    FieldEquals,
    TokenCoverage,
    CheckpointPassed,
    IncidentAbsent,
    ExternalRefPresent,
    InteractionModelIs,
    RemoteOutcomeDispositionIs,
    TreatyViolationAbsent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScorecardCriterion {
    pub id: String,
    pub title: String,
    pub kind: ScorecardCriterionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_len: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<OversightCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incident_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex_pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_value: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_threshold: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_comparison: Option<NumericComparison>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub json_schema: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ref_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_model: Option<InteractionModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_violation_code: Option<String>,
    #[serde(default = "default_scorecard_weight")]
    pub weight: u32,
}

impl Default for ScorecardCriterion {
    fn default() -> Self {
        Self {
            id: String::new(),
            title: String::new(),
            kind: ScorecardCriterionKind::ArtifactPresent,
            artifact_name: None,
            field_path: None,
            source_path: None,
            min_len: None,
            checkpoint: None,
            incident_code: None,
            regex_pattern: None,
            expected_value: None,
            numeric_threshold: None,
            numeric_comparison: None,
            json_schema: None,
            external_ref_kind: None,
            interaction_model: None,
            remote_outcome_disposition: None,
            treaty_violation_code: None,
            weight: default_scorecard_weight(),
        }
    }
}

fn default_scorecard_weight() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScorecardSpec {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub criteria: Vec<ScorecardCriterion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub needs_review_below: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvaluationProfile {
    pub scorecard: String,
    #[serde(default)]
    pub review_queue: bool,
    #[serde(default)]
    pub alert_rules: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_name: Option<String>,
    #[serde(default)]
    pub dataset_capture: bool,
    #[serde(default)]
    pub post_result_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairwiseProfile {
    pub capability: String,
    pub score_margin: f64,
    #[serde(default)]
    pub review_queue: bool,
    #[serde(default = "default_pairwise_priority")]
    pub review_priority: String,
    #[serde(default = "default_pairwise_low_confidence_threshold")]
    pub low_confidence_threshold: f64,
    #[serde(default = "default_pairwise_regression_loss_rate_threshold")]
    pub regression_loss_rate_threshold: f64,
    #[serde(default = "default_pairwise_needs_review_rate_threshold")]
    pub needs_review_rate_threshold: f64,
}

fn default_pairwise_priority() -> String {
    "medium".to_string()
}

fn default_pairwise_low_confidence_threshold() -> f64 {
    0.85
}

fn default_pairwise_regression_loss_rate_threshold() -> f64 {
    0.3
}

fn default_pairwise_needs_review_rate_threshold() -> f64 {
    0.25
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvaluationDataset {
    pub capability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default)]
    pub auto_capture: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatasetCase {
    pub id: String,
    pub dataset_name: String,
    pub capability: String,
    pub goal_summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_model: Option<InteractionModel>,
    #[serde(default)]
    pub normalized_inputs: Metadata,
    #[serde(default)]
    pub expected_artifacts: Vec<String>,
    #[serde(default)]
    pub expected_output_signals: Vec<String>,
    pub source_action_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jurisdiction_class: Option<JurisdictionClass>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doctrine_summary: Option<DoctrinePack>,
    #[serde(default)]
    pub checkpoint_status: Vec<CheckpointStatus>,
    #[serde(default)]
    pub policy_incidents: Vec<PolicyIncident>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_principal: Option<RemotePrincipalRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub treaty_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_decision: Option<FederationDecision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_receipt_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_state_disposition: Option<RemoteStateDisposition>,
    #[serde(default)]
    pub treaty_violations: Vec<TreatyViolation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_depth: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<VerificationSummary>,
    #[serde(default)]
    pub evaluation_refs: Vec<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentRunStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentCaseStatus {
    Passed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentRun {
    pub id: String,
    pub dataset_name: String,
    pub executor: String,
    pub strategy_mode: ExecutionStrategyMode,
    pub allow_fallback: bool,
    pub status: ExperimentRunStatus,
    pub total_cases: u32,
    pub completed_cases: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExperimentCaseResult {
    pub id: String,
    pub run_id: String,
    pub dataset_case_id: String,
    pub capability: String,
    pub status: ExperimentCaseStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation_status: Option<EvaluationStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    pub summary: String,
    #[serde(default)]
    pub findings: Vec<String>,
    #[serde(default)]
    pub criterion_results: Vec<EvaluationCriterionResult>,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub external_refs: Vec<ExternalRef>,
    #[serde(default)]
    pub policy_incident_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interaction_model: Option<InteractionModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_outcome_disposition: Option<RemoteOutcomeDisposition>,
    #[serde(default)]
    pub treaty_violation_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairwiseOutcome {
    LeftWins,
    RightWins,
    NeedsReview,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PairwiseExperimentRunStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairwiseExperimentRun {
    pub id: String,
    pub dataset_name: String,
    pub capability: String,
    pub profile_name: String,
    pub left_executor: String,
    pub right_executor: String,
    pub left_run_id: String,
    pub right_run_id: String,
    pub status: PairwiseExperimentRunStatus,
    pub total_cases: u32,
    pub completed_cases: u32,
    pub left_wins: u32,
    pub right_wins: u32,
    pub needs_review_cases: u32,
    #[serde(default)]
    pub triggered_alert_rules: Vec<String>,
    #[serde(default)]
    pub alert_summaries: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PairwiseCaseResult {
    pub id: String,
    pub pairwise_run_id: String,
    pub dataset_case_id: String,
    pub outcome: PairwiseOutcome,
    pub summary: String,
    pub reason_code: String,
    pub left_case_result_ref: String,
    pub right_case_result_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_score: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_queue_item_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback_note_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_resolution: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AlertEvent {
    pub id: String,
    pub rule_id: String,
    pub action_id: String,
    pub severity: String,
    pub summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub federation_pack_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_status: Option<RemoteEvidenceStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_review_disposition: Option<RemoteReviewDisposition>,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterministicCheckpoint {
    pub executor_kind: String,
    pub stage: String,
    pub workspace_root: String,
    pub input_digest: String,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_state: Option<StrategyCheckpointState>,
    pub last_updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RepoIndexArtifact {
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub languages: BTreeMap<String, u64>,
    #[serde(default)]
    pub test_files: Vec<String>,
    #[serde(default)]
    pub test_file_map: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub owners: BTreeMap<String, Vec<String>>,
    pub ownership_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewRiskLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReviewFinding {
    pub title: String,
    pub detail: String,
    pub severity: String,
    #[serde(default)]
    pub files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewFindingsArtifact {
    pub risk_level: ReviewRiskLevel,
    #[serde(default)]
    pub changed_files: Vec<String>,
    #[serde(default)]
    pub findings: Vec<ReviewFinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CiFailureFamily {
    Test,
    Lint,
    Typecheck,
    Build,
    DependencyInstall,
    InfraTransient,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CiTriageArtifact {
    pub family: CiFailureFamily,
    pub summary: String,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncidentEnrichmentArtifact {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alert_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_url: Option<String>,
    #[serde(default)]
    pub probable_blast_radius: Vec<String>,
    #[serde(default)]
    pub error_signatures: Vec<String>,
    #[serde(default)]
    pub repeated_symptoms: Vec<String>,
    #[serde(default)]
    pub next_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskPlanStep {
    pub title: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskPlanArtifact {
    #[serde(default)]
    pub target_files: Vec<String>,
    #[serde(default)]
    pub ordered_steps: Vec<TaskPlanStep>,
    #[serde(default)]
    pub risks: Vec<String>,
    #[serde(default)]
    pub assumptions: Vec<String>,
    #[serde(default)]
    pub test_suggestions: Vec<String>,
    pub confidence_summary: String,
}

pub type PatchPlanStep = TaskPlanStep;
pub type PatchPlanArtifact = TaskPlanArtifact;

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
